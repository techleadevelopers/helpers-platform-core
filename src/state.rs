use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{postgres::PgPoolOptions, PgPool};
use tokio::sync::{broadcast, Mutex as AsyncMutex};

use crate::{
    config::Config,
    services::{email::EmailService, event_bus::EventBus},
};

// Schema changes are applied through SQLx's versioned migration ledger.  The
// legacy idempotent runtime checks below remain only as a compatibility bridge
// for databases created by earlier preview builds.
static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

#[derive(Clone)]
pub struct AppState {
    pub config: Config,
    pub db: PgPool,
    pub started_at: DateTime<Utc>,
    pub chat_channels: Arc<AsyncMutex<HashMap<String, broadcast::Sender<ChatEvent>>>>,
    pub rescue_tx: broadcast::Sender<RescueEvent>,
    pub feed_tx: broadcast::Sender<FeedEvent>,
    pub email: EmailService,
    pub event_bus: EventBus,
    pub redis: Option<redis::Client>,
    pub rate_limiter: Arc<Mutex<HashMap<String, Vec<Instant>>>>,
}

impl AppState {
    pub async fn new(config: Config) -> anyhow::Result<Self> {
        let db = PgPoolOptions::new()
            .max_connections(config.database_max_connections)
            .min_connections(config.database_min_connections)
            .acquire_timeout(Duration::from_secs(5))
            .connect_lazy(&config.database_url)?;
        MIGRATOR.run(&db).await?;
        ensure_runtime_schema(&db, config.postgis_enabled).await?;

        let chat_channels = Arc::new(AsyncMutex::new(HashMap::new()));
        let (rescue_tx, _) = broadcast::channel(4096);
        let (feed_tx, _) = broadcast::channel(4096);
        let email = EmailService::new(config.clone());
        let event_bus = EventBus::connect(&config).await?;
        if config.process_role.starts_realtime_bridge() {
            event_bus.spawn_bridge(chat_channels.clone(), rescue_tx.clone(), feed_tx.clone());
        }
        let redis = if config.app_env == "test" {
            None
        } else {
            Some(redis::Client::open(config.redis_url.as_str())?)
        };
        if config.process_role.starts_push_worker() {
            crate::services::push_worker::spawn(config.clone(), db.clone());
        }
        if config.process_role.starts_fanout_worker() {
            crate::services::rescue_fanout::spawn(config.rescue_fanout_worker_enabled, db.clone());
        }

        let state = Self {
            config,
            db,
            started_at: Utc::now(),
            chat_channels,
            rescue_tx,
            feed_tx,
            email,
            event_bus,
            redis,
            rate_limiter: Arc::new(Mutex::new(HashMap::new())),
        };
        if state.config.process_role.starts_geocode_worker() {
            crate::services::geocoding_worker::spawn(state.clone());
        }
        Ok(state)
    }
}

async fn ensure_runtime_schema(db: &PgPool, postgis_enabled: bool) -> anyhow::Result<()> {
    let base_statements = [
        r#"
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'moderation_status') THEN
                CREATE TYPE moderation_status AS ENUM ('queued', 'approved', 'rejected', 'needs_review');
            END IF;
        END
        $$;
        "#,
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS avatar_url text;",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS deleted_at timestamptz;",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS anonymized_at timestamptz;",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS retention_delete_after timestamptz;",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS gender text;",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS cep text;",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS street text;",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS number text;",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS complement text;",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS neighborhood text;",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS city text;",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS state text;",
        "ALTER TABLE users DROP CONSTRAINT IF EXISTS users_gender_check;",
        "ALTER TABLE users ADD CONSTRAINT users_gender_check CHECK (gender IS NULL OR gender IN ('male', 'female'));",
        "ALTER TABLE ong_profiles ADD COLUMN IF NOT EXISTS cep text;",
        "ALTER TABLE ong_profiles ADD COLUMN IF NOT EXISTS street text;",
        "ALTER TABLE ong_profiles ADD COLUMN IF NOT EXISTS number text;",
        "ALTER TABLE ong_profiles ADD COLUMN IF NOT EXISTS complement text;",
        "ALTER TABLE ong_profiles ADD COLUMN IF NOT EXISTS neighborhood text;",
        "ALTER TABLE ong_profiles ADD COLUMN IF NOT EXISTS foundation_year integer;",
        "ALTER TABLE posts ADD COLUMN IF NOT EXISTS tags text[] NOT NULL DEFAULT '{}';",
        "ALTER TABLE posts ADD COLUMN IF NOT EXISTS urgent boolean NOT NULL DEFAULT false;",
        "ALTER TABLE posts ADD COLUMN IF NOT EXISTS rescue_status text NOT NULL DEFAULT 'open';",
        "ALTER TABLE posts ADD COLUMN IF NOT EXISTS resolved_at timestamptz;",
        "ALTER TABLE posts DROP CONSTRAINT IF EXISTS posts_rescue_status_check;",
        "ALTER TABLE posts ADD CONSTRAINT posts_rescue_status_check CHECK (rescue_status IN ('open', 'active', 'resolved', 'cancelled'));",
        "ALTER TABLE posts ADD COLUMN IF NOT EXISTS text_only boolean NOT NULL DEFAULT false;",
        "ALTER TABLE posts ADD COLUMN IF NOT EXISTS likes_count integer NOT NULL DEFAULT 0 CHECK (likes_count >= 0);",
        "ALTER TABLE posts ADD COLUMN IF NOT EXISTS comments_count integer NOT NULL DEFAULT 0 CHECK (comments_count >= 0);",
        "ALTER TABLE posts ADD COLUMN IF NOT EXISTS shares_count integer NOT NULL DEFAULT 0 CHECK (shares_count >= 0);",
        r#"
        CREATE TABLE IF NOT EXISTS post_shares (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          post_id uuid NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
          user_id uuid REFERENCES users(id) ON DELETE SET NULL,
          channel text NOT NULL DEFAULT 'system_share',
          created_at timestamptz NOT NULL DEFAULT now()
        );
        "#,
        "CREATE INDEX IF NOT EXISTS post_shares_post_created_idx ON post_shares (post_id, created_at DESC);",
        "CREATE INDEX IF NOT EXISTS post_shares_user_created_idx ON post_shares (user_id, created_at DESC) WHERE user_id IS NOT NULL;",
        "ALTER TABLE posts ADD COLUMN IF NOT EXISTS moderation_status moderation_status NOT NULL DEFAULT 'approved';",
        "ALTER TABLE posts ALTER COLUMN moderation_status SET DEFAULT 'approved';",
        "UPDATE posts SET moderation_status = 'approved' WHERE moderation_status IN ('queued', 'needs_review');",
        "ALTER TABLE posts DROP CONSTRAINT IF EXISTS posts_no_pending_visibility;",
        "ALTER TABLE posts ADD CONSTRAINT posts_no_pending_visibility CHECK (moderation_status NOT IN ('queued', 'needs_review'));",
        "ALTER TABLE posts ADD COLUMN IF NOT EXISTS fraud_risk smallint NOT NULL DEFAULT 0 CHECK (fraud_risk BETWEEN 0 AND 100);",
        "ALTER TABLE posts ADD COLUMN IF NOT EXISTS idempotency_key text;",
        "CREATE UNIQUE INDEX IF NOT EXISTS posts_author_idempotency_idx ON posts (author_id, idempotency_key) WHERE idempotency_key IS NOT NULL;",
        "ALTER TABLE posts ADD COLUMN IF NOT EXISTS geo_status text NOT NULL DEFAULT 'unavailable';",
        "ALTER TABLE posts ADD COLUMN IF NOT EXISTS geo_source text;",
        "ALTER TABLE posts ADD COLUMN IF NOT EXISTS geo_provider text;",
        "ALTER TABLE posts ADD COLUMN IF NOT EXISTS geo_confidence double precision;",
        "ALTER TABLE posts ADD COLUMN IF NOT EXISTS geo_resolved_at timestamptz;",
        "ALTER TABLE posts ADD COLUMN IF NOT EXISTS route_public boolean NOT NULL DEFAULT false;",
        "ALTER TABLE posts DROP CONSTRAINT IF EXISTS posts_geo_status_check;",
        "ALTER TABLE posts ADD CONSTRAINT posts_geo_status_check CHECK (geo_status IN ('unavailable', 'pending', 'confirmed', 'failed'));",
        "ALTER TABLE posts DROP CONSTRAINT IF EXISTS posts_geo_source_check;",
        "ALTER TABLE posts ADD CONSTRAINT posts_geo_source_check CHECK (geo_source IS NULL OR geo_source IN ('gps_confirmed', 'address_geocoded'));",
        r#"
        CREATE TABLE IF NOT EXISTS post_geocode_jobs (
          post_id uuid PRIMARY KEY REFERENCES posts(id) ON DELETE CASCADE,
          address_label text NOT NULL,
          status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'completed', 'failed')),
          attempts integer NOT NULL DEFAULT 0,
          last_error text,
          next_run_at timestamptz NOT NULL DEFAULT now(),
          created_at timestamptz NOT NULL DEFAULT now(),
          updated_at timestamptz NOT NULL DEFAULT now()
        );
        "#,
        "CREATE INDEX IF NOT EXISTS post_geocode_jobs_due_idx ON post_geocode_jobs (status, next_run_at) WHERE status IN ('pending', 'processing');",
        "CREATE INDEX IF NOT EXISTS posts_moderation_created_idx ON posts (moderation_status, created_at DESC);",
        "CREATE INDEX IF NOT EXISTS posts_author_created_idx ON posts (author_id, created_at DESC);",
        "CREATE INDEX IF NOT EXISTS posts_operational_feed_idx ON posts (moderation_status, urgent, rescue_status, created_at DESC);",
        "ALTER TABLE chat_rooms ADD COLUMN IF NOT EXISTS requester_id uuid REFERENCES users(id) ON DELETE CASCADE;",
        "CREATE UNIQUE INDEX IF NOT EXISTS chat_rooms_private_post_requester_idx ON chat_rooms (post_id, requester_id) WHERE post_id IS NOT NULL AND requester_id IS NOT NULL;",
        "ALTER TABLE chat_rooms ADD COLUMN IF NOT EXISTS direct_pair_key text;",
        "CREATE UNIQUE INDEX IF NOT EXISTS chat_rooms_direct_pair_idx ON chat_rooms (direct_pair_key) WHERE direct_pair_key IS NOT NULL;",
        r#"
        CREATE TABLE IF NOT EXISTS chat_room_members (
          room_id uuid NOT NULL REFERENCES chat_rooms(id) ON DELETE CASCADE,
          user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          last_read_at timestamptz,
          joined_at timestamptz NOT NULL DEFAULT now(),
          PRIMARY KEY (room_id, user_id)
        );
        "#,
        "CREATE INDEX IF NOT EXISTS chat_room_members_user_idx ON chat_room_members (user_id, room_id);",
        "ALTER TABLE chat_messages ADD COLUMN IF NOT EXISTS idempotency_key text;",
        "CREATE UNIQUE INDEX IF NOT EXISTS chat_messages_sender_idempotency_idx ON chat_messages (room_id, sender_id, idempotency_key) WHERE idempotency_key IS NOT NULL;",
        r#"
        CREATE TABLE IF NOT EXISTS chat_ws_tickets (
          token_hash text PRIMARY KEY,
          room_id uuid NOT NULL REFERENCES chat_rooms(id) ON DELETE CASCADE,
          user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          expires_at timestamptz NOT NULL,
          consumed_at timestamptz,
          created_at timestamptz NOT NULL DEFAULT now()
        );
        "#,
        "CREATE INDEX IF NOT EXISTS chat_ws_tickets_expiry_idx ON chat_ws_tickets (expires_at);",
        r#"
        CREATE TABLE IF NOT EXISTS chat_user_blocks (
          blocker_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          blocked_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          created_at timestamptz NOT NULL DEFAULT now(),
          PRIMARY KEY (blocker_id, blocked_id),
          CHECK (blocker_id <> blocked_id)
        );
        "#,
        "CREATE INDEX IF NOT EXISTS chat_user_blocks_blocked_idx ON chat_user_blocks (blocked_id, blocker_id);",
        r#"
        CREATE TABLE IF NOT EXISTS media_upload_intents (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          user_id uuid REFERENCES users(id) ON DELETE SET NULL,
          provider text NOT NULL DEFAULT 'cloudinary',
          resource_type text NOT NULL CHECK (resource_type IN ('image', 'video')),
          object_key text NOT NULL UNIQUE,
          file_name text NOT NULL,
          content_type text NOT NULL,
          size_bytes bigint NOT NULL CHECK (size_bytes > 0),
          checksum_sha256 text,
          upload_url text NOT NULL,
          public_url text NOT NULL,
          expires_at timestamptz NOT NULL,
          consumed_at timestamptz,
          created_at timestamptz NOT NULL DEFAULT now()
        );
        "#,
        "CREATE INDEX IF NOT EXISTS media_upload_intents_user_idx ON media_upload_intents (user_id, created_at DESC);",
        "CREATE INDEX IF NOT EXISTS media_upload_intents_user_active_idx ON media_upload_intents (user_id, expires_at DESC) WHERE consumed_at IS NULL;",
        r#"
        CREATE TABLE IF NOT EXISTS audit_events (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          actor_user_id uuid REFERENCES users(id) ON DELETE SET NULL,
          action text NOT NULL,
          target_type text,
          target_id text,
          metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
          created_at timestamptz NOT NULL DEFAULT now()
        );
        "#,
        "CREATE INDEX IF NOT EXISTS audit_events_actor_created_idx ON audit_events (actor_user_id, created_at DESC);",
        "CREATE INDEX IF NOT EXISTS audit_events_action_created_idx ON audit_events (action, created_at DESC);",
        r#"
        CREATE TABLE IF NOT EXISTS push_subscriptions (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          push_token text NOT NULL UNIQUE,
          platform text NOT NULL CHECK (platform IN ('ios', 'android', 'expo', 'web')),
          lat double precision NOT NULL CHECK (lat BETWEEN -90 AND 90),
          lng double precision NOT NULL CHECK (lng BETWEEN -180 AND 180),
          radius_km double precision NOT NULL DEFAULT 8 CHECK (radius_km BETWEEN 0.03 AND 50),
          critical_alerts boolean NOT NULL DEFAULT false,
          updated_at timestamptz NOT NULL DEFAULT now(),
          created_at timestamptz NOT NULL DEFAULT now()
        );
        "#,
        "CREATE INDEX IF NOT EXISTS push_subscriptions_user_idx ON push_subscriptions (user_id, updated_at DESC);",
        "CREATE INDEX IF NOT EXISTS push_subscriptions_location_idx ON push_subscriptions (lat, lng);",
        "ALTER TABLE push_subscriptions ADD COLUMN IF NOT EXISTS invalidated_at timestamptz;",
        "ALTER TABLE push_subscriptions ADD COLUMN IF NOT EXISTS last_delivery_error text;",
        r#"
        CREATE TABLE IF NOT EXISTS notification_events (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          user_id uuid REFERENCES users(id) ON DELETE CASCADE,
          kind text NOT NULL,
          title text NOT NULL,
          body text NOT NULL,
          post_id text,
          image_url text,
          distance_km double precision,
          critical boolean NOT NULL DEFAULT false,
          deeplink text,
          dedupe_key text,
          ttl_seconds integer,
          category text,
          payload jsonb NOT NULL DEFAULT '{}'::jsonb,
          read_at timestamptz,
          acked_at timestamptz,
          created_at timestamptz NOT NULL DEFAULT now()
        );
        "#,
        "CREATE INDEX IF NOT EXISTS notification_events_user_created_idx ON notification_events (user_id, created_at DESC);",
        "CREATE INDEX IF NOT EXISTS notification_events_user_kind_created_idx ON notification_events (user_id, kind, created_at DESC);",
        "CREATE INDEX IF NOT EXISTS notification_events_post_kind_created_idx ON notification_events (post_id, kind, created_at DESC);",
        "CREATE INDEX IF NOT EXISTS notification_events_dedupe_idx ON notification_events (dedupe_key, user_id);",
        "CREATE UNIQUE INDEX IF NOT EXISTS notification_events_user_dedupe_unique_idx ON notification_events (dedupe_key, user_id) WHERE dedupe_key IS NOT NULL AND user_id IS NOT NULL;",
        r#"
        CREATE TABLE IF NOT EXISTS push_delivery_jobs (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          notification_event_id uuid REFERENCES notification_events(id) ON DELETE CASCADE,
          user_id uuid REFERENCES users(id) ON DELETE CASCADE,
          push_token text NOT NULL,
          platform text NOT NULL,
          payload jsonb NOT NULL,
          status text NOT NULL DEFAULT 'queued' CHECK (status IN ('queued', 'provider_accepted', 'delivered', 'failed', 'dead_letter')),
          attempts integer NOT NULL DEFAULT 0,
          next_attempt_at timestamptz NOT NULL DEFAULT now(),
          last_error text,
          created_at timestamptz NOT NULL DEFAULT now(),
          updated_at timestamptz NOT NULL DEFAULT now()
        );
        "#,
        "CREATE INDEX IF NOT EXISTS push_delivery_jobs_status_next_idx ON push_delivery_jobs (status, next_attempt_at);",
        "CREATE INDEX IF NOT EXISTS push_delivery_jobs_due_created_idx ON push_delivery_jobs (status, next_attempt_at, created_at ASC) WHERE status IN ('queued', 'failed');",
        "ALTER TABLE push_delivery_jobs ADD COLUMN IF NOT EXISTS provider_response jsonb;",
        "ALTER TABLE push_delivery_jobs ADD COLUMN IF NOT EXISTS provider_ticket_id text;",
        "ALTER TABLE push_delivery_jobs ADD COLUMN IF NOT EXISTS provider_accepted_at timestamptz;",
        "ALTER TABLE push_delivery_jobs ADD COLUMN IF NOT EXISTS receipt_status text;",
        "ALTER TABLE push_delivery_jobs ADD COLUMN IF NOT EXISTS receipt_checked_at timestamptz;",
        "ALTER TABLE push_delivery_jobs ADD COLUMN IF NOT EXISTS receipt_response jsonb;",
        "ALTER TABLE push_delivery_jobs ADD COLUMN IF NOT EXISTS delivered_at timestamptz;",
        "ALTER TABLE push_delivery_jobs DROP CONSTRAINT IF EXISTS push_delivery_jobs_status_check;",
        "ALTER TABLE push_delivery_jobs ADD CONSTRAINT push_delivery_jobs_status_check CHECK (status IN ('queued', 'provider_accepted', 'delivered', 'failed', 'dead_letter'));",
        "CREATE INDEX IF NOT EXISTS push_delivery_jobs_receipt_due_idx ON push_delivery_jobs (status, provider_accepted_at) WHERE status = 'provider_accepted' AND provider_ticket_id IS NOT NULL;",
        "CREATE INDEX IF NOT EXISTS push_subscriptions_active_location_idx ON push_subscriptions (updated_at DESC, lat, lng) WHERE invalidated_at IS NULL;",
        r#"
        CREATE TABLE IF NOT EXISTS user_ong_follows (
          user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          ong_id uuid NOT NULL REFERENCES ong_profiles(id) ON DELETE CASCADE,
          active boolean NOT NULL DEFAULT true,
          created_at timestamptz NOT NULL DEFAULT now(),
          updated_at timestamptz NOT NULL DEFAULT now(),
          PRIMARY KEY (user_id, ong_id)
        );
        "#,
        "CREATE INDEX IF NOT EXISTS user_ong_follows_ong_idx ON user_ong_follows (ong_id, active);",
        r#"
        CREATE TABLE IF NOT EXISTS user_follows (
          follower_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          followed_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          active boolean NOT NULL DEFAULT true,
          created_at timestamptz NOT NULL DEFAULT now(),
          updated_at timestamptz NOT NULL DEFAULT now(),
          PRIMARY KEY (follower_id, followed_id),
          CHECK (follower_id <> followed_id)
        );
        "#,
        "CREATE INDEX IF NOT EXISTS user_follows_followed_idx ON user_follows (followed_id, active);",
        r#"
        CREATE TABLE IF NOT EXISTS post_media (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          post_id uuid NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
          provider text NOT NULL DEFAULT 'cloudinary',
          resource_type text NOT NULL DEFAULT 'image' CHECK (resource_type IN ('image', 'video')),
          object_key text NOT NULL,
          public_url text NOT NULL,
          content_type text NOT NULL,
          width integer CHECK (width IS NULL OR width > 0),
          height integer CHECK (height IS NULL OR height > 0),
          size_bytes bigint CHECK (size_bytes IS NULL OR size_bytes > 0),
          checksum_sha256 text,
          sort_order smallint NOT NULL DEFAULT 0 CHECK (sort_order >= 0),
          moderation_status moderation_status NOT NULL DEFAULT 'approved',
          moderation_labels text[] NOT NULL DEFAULT '{}',
          created_at timestamptz NOT NULL DEFAULT now()
        );
        "#,
        "ALTER TABLE post_media ALTER COLUMN moderation_status SET DEFAULT 'approved';",
        "UPDATE post_media SET moderation_status = 'approved' WHERE moderation_status IN ('queued', 'needs_review');",
        "ALTER TABLE post_media DROP CONSTRAINT IF EXISTS post_media_no_pending_visibility;",
        "ALTER TABLE post_media ADD CONSTRAINT post_media_no_pending_visibility CHECK (moderation_status NOT IN ('queued', 'needs_review'));",
        "CREATE INDEX IF NOT EXISTS post_media_post_idx ON post_media (post_id, sort_order);",
        "CREATE INDEX IF NOT EXISTS post_media_moderation_idx ON post_media (moderation_status, created_at);",
        r#"
        CREATE TABLE IF NOT EXISTS post_likes (
          post_id uuid NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
          user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          created_at timestamptz NOT NULL DEFAULT now(),
          PRIMARY KEY (post_id, user_id)
        );
        "#,
        r#"
        CREATE TABLE IF NOT EXISTS post_comments (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          post_id uuid NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
          user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          body text NOT NULL CHECK (char_length(body) BETWEEN 1 AND 2000),
          moderation_status text NOT NULL DEFAULT 'visible_monitored',
          created_at timestamptz NOT NULL DEFAULT now()
        );
        "#,
        "CREATE INDEX IF NOT EXISTS post_comments_post_created_idx ON post_comments (post_id, created_at DESC);",
        r#"
        CREATE TABLE IF NOT EXISTS post_reports (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          post_id uuid NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
          reporter_user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          reason text NOT NULL,
          details text,
          severity text NOT NULL DEFAULT 'normal',
          status text NOT NULL DEFAULT 'queued_review',
          created_at timestamptz NOT NULL DEFAULT now()
        );
        "#,
        "CREATE INDEX IF NOT EXISTS post_reports_status_created_idx ON post_reports (status, created_at);",
        r#"
        CREATE TABLE IF NOT EXISTS support_tickets (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          user_id uuid REFERENCES users(id) ON DELETE SET NULL,
          subject text NOT NULL CHECK (char_length(subject) BETWEEN 1 AND 160),
          status text NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'pending', 'resolved', 'closed')),
          category text NOT NULL DEFAULT 'OTHER',
          severity text NOT NULL DEFAULT 'MEDIUM',
          created_at timestamptz NOT NULL DEFAULT now(),
          updated_at timestamptz NOT NULL DEFAULT now()
        );
        "#,
        "ALTER TABLE support_tickets ADD COLUMN IF NOT EXISTS user_id uuid REFERENCES users(id) ON DELETE SET NULL;",
        "CREATE INDEX IF NOT EXISTS support_tickets_status_created_idx ON support_tickets (status, created_at DESC);",
        "CREATE INDEX IF NOT EXISTS support_tickets_user_created_idx ON support_tickets (user_id, created_at DESC) WHERE user_id IS NOT NULL;",
        r#"
        CREATE TABLE IF NOT EXISTS support_ticket_messages (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          ticket_id uuid NOT NULL REFERENCES support_tickets(id) ON DELETE CASCADE,
          body text NOT NULL CHECK (char_length(body) BETWEEN 1 AND 4000),
          author_type text NOT NULL DEFAULT 'user' CHECK (author_type IN ('user', 'support', 'system')),
          created_at timestamptz NOT NULL DEFAULT now()
        );
        "#,
        "CREATE INDEX IF NOT EXISTS support_ticket_messages_ticket_created_idx ON support_ticket_messages (ticket_id, created_at ASC);",
        r#"
        CREATE TABLE IF NOT EXISTS rescue_sessions (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          post_id uuid NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
          reporter_user_id uuid REFERENCES users(id) ON DELETE SET NULL,
          status text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'ended')),
          lat double precision NOT NULL CHECK (lat BETWEEN -90 AND 90),
          lng double precision NOT NULL CHECK (lng BETWEEN -180 AND 180),
          accuracy double precision CHECK (accuracy IS NULL OR accuracy >= 0),
          created_at timestamptz NOT NULL DEFAULT now(),
          updated_at timestamptz NOT NULL DEFAULT now(),
          ended_at timestamptz
        );
        "#,
        r#"
        CREATE TABLE IF NOT EXISTS rescue_location_points (
          id bigserial PRIMARY KEY,
          rescue_id uuid NOT NULL REFERENCES rescue_sessions(id) ON DELETE CASCADE,
          lat double precision NOT NULL CHECK (lat BETWEEN -90 AND 90),
          lng double precision NOT NULL CHECK (lng BETWEEN -180 AND 180),
          accuracy double precision CHECK (accuracy IS NULL OR accuracy >= 0),
          recorded_at timestamptz NOT NULL DEFAULT now()
        );
        "#,
        r#"
        CREATE TABLE IF NOT EXISTS rescue_incidents (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          rescue_id uuid NOT NULL REFERENCES rescue_sessions(id) ON DELETE CASCADE,
          description text NOT NULL CHECK (char_length(description) BETWEEN 1 AND 2000),
          attachments jsonb NOT NULL DEFAULT '[]'::jsonb,
          status text NOT NULL DEFAULT 'queued_review',
          created_at timestamptz NOT NULL DEFAULT now()
        );
        "#,
        r#"
        CREATE TABLE IF NOT EXISTS rescue_events (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          rescue_id uuid NOT NULL REFERENCES rescue_sessions(id) ON DELETE CASCADE,
          post_id uuid NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
          type text NOT NULL,
          actor_id uuid REFERENCES users(id) ON DELETE SET NULL,
          message text,
          metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
          created_at timestamptz NOT NULL DEFAULT now()
        );
        "#,
        "CREATE INDEX IF NOT EXISTS rescue_events_rescue_created_idx ON rescue_events (rescue_id, created_at ASC);",
        "CREATE INDEX IF NOT EXISTS rescue_events_post_created_idx ON rescue_events (post_id, created_at ASC);",
        "CREATE INDEX IF NOT EXISTS rescue_events_type_idx ON rescue_events (type);",
        r#"
        CREATE TABLE IF NOT EXISTS rescue_responses (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          rescue_session_id uuid REFERENCES rescue_sessions(id) ON DELETE CASCADE,
          post_id uuid NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
          user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          action text NOT NULL DEFAULT 'going' CHECK (action IN ('going', 'remote_support', 'unavailable')),
          status text NOT NULL DEFAULT 'confirmed' CHECK (status IN ('confirmed', 'cancelled', 'arrived')),
          lat double precision CHECK (lat IS NULL OR lat BETWEEN -90 AND 90),
          lng double precision CHECK (lng IS NULL OR lng BETWEEN -180 AND 180),
          eta_seconds integer CHECK (eta_seconds IS NULL OR eta_seconds >= 0),
          created_at timestamptz NOT NULL DEFAULT now(),
          updated_at timestamptz NOT NULL DEFAULT now(),
          UNIQUE (post_id, user_id, action)
        );
        "#,
        "CREATE INDEX IF NOT EXISTS rescue_responses_post_status_idx ON rescue_responses (post_id, status, created_at DESC);",
        "CREATE INDEX IF NOT EXISTS rescue_responses_rescue_status_idx ON rescue_responses (rescue_session_id, status, created_at DESC);",
        "CREATE INDEX IF NOT EXISTS rescue_responses_status_created_post_idx ON rescue_responses (status, created_at DESC, post_id);",
        r#"
        CREATE TABLE IF NOT EXISTS rescue_final_reports (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          rescue_id uuid REFERENCES rescue_sessions(id) ON DELETE SET NULL,
          post_id uuid NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
          status text NOT NULL,
          summary text NOT NULL,
          public_update text NOT NULL,
          generated_by_ai boolean NOT NULL DEFAULT false,
          publication_status text NOT NULL DEFAULT 'pending_approval',
          rejection_reason text,
          approved_by uuid REFERENCES users(id) ON DELETE SET NULL,
          approved_at timestamptz,
          rejected_by uuid REFERENCES users(id) ON DELETE SET NULL,
          rejected_at timestamptz,
          created_by uuid REFERENCES users(id) ON DELETE SET NULL,
          updated_by uuid REFERENCES users(id) ON DELETE SET NULL,
          admin_notes text,
          ai_model text,
          ai_latency_ms integer,
          ai_cost_cents integer,
          prompt_version text,
          schema_version text NOT NULL DEFAULT '1.0.0',
          created_at timestamptz NOT NULL DEFAULT now(),
          updated_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT rescue_final_reports_status_check CHECK (status IN ('rescued', 'not_found', 'died', 'referred', 'cancelled', 'false_alarm')),
          CONSTRAINT rescue_final_reports_publication_status_check CHECK (publication_status IN ('draft', 'pending_approval', 'published', 'rejected')),
          CONSTRAINT rescue_final_reports_rejected_reason_check CHECK (publication_status <> 'rejected' OR rejection_reason IS NOT NULL),
          CONSTRAINT rescue_final_reports_post_unique UNIQUE (post_id)
        );
        "#,
        "CREATE INDEX IF NOT EXISTS rescue_final_reports_rescue_id_idx ON rescue_final_reports (rescue_id);",
        "CREATE INDEX IF NOT EXISTS rescue_final_reports_post_id_idx ON rescue_final_reports (post_id);",
        "CREATE INDEX IF NOT EXISTS rescue_final_reports_publication_status_idx ON rescue_final_reports (publication_status);",
        "CREATE INDEX IF NOT EXISTS rescue_final_reports_publication_status_status_post_idx ON rescue_final_reports (publication_status, status, post_id);",
        "CREATE INDEX IF NOT EXISTS rescue_final_reports_approved_at_idx ON rescue_final_reports (approved_at);",
        r#"
        CREATE TABLE IF NOT EXISTS rescue_fanout_states (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          post_id uuid NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
          rescue_session_id uuid REFERENCES rescue_sessions(id) ON DELETE SET NULL,
          current_phase integer NOT NULL DEFAULT 1 CHECK (current_phase BETWEEN 1 AND 20),
          status text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'paused', 'completed', 'cancelled', 'exhausted')),
          last_radius_km numeric NOT NULL,
          next_run_at timestamptz NOT NULL,
          confirmed_count integer NOT NULL DEFAULT 0 CHECK (confirmed_count >= 0),
          arrived_count integer NOT NULL DEFAULT 0 CHECK (arrived_count >= 0),
          attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
          created_at timestamptz NOT NULL DEFAULT now(),
          updated_at timestamptz NOT NULL DEFAULT now(),
          completed_at timestamptz,
          UNIQUE (post_id)
        );
        "#,
        "CREATE INDEX IF NOT EXISTS rescue_fanout_states_due_idx ON rescue_fanout_states (status, next_run_at) WHERE status = 'active';",
        "CREATE INDEX IF NOT EXISTS rescue_fanout_states_post_idx ON rescue_fanout_states (post_id);",
        r#"
        CREATE TABLE IF NOT EXISTS rescue_fanout_attempts (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          fanout_state_id uuid NOT NULL REFERENCES rescue_fanout_states(id) ON DELETE CASCADE,
          post_id uuid NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
          phase integer NOT NULL CHECK (phase BETWEEN 1 AND 20),
          radius_km numeric NOT NULL,
          candidate_count integer NOT NULL DEFAULT 0 CHECK (candidate_count >= 0),
          push_jobs_created integer NOT NULL DEFAULT 0 CHECK (push_jobs_created >= 0),
          confirmed_count_at_run integer NOT NULL DEFAULT 0 CHECK (confirmed_count_at_run >= 0),
          reason text NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now()
        );
        "#,
        "CREATE INDEX IF NOT EXISTS rescue_fanout_attempts_state_created_idx ON rescue_fanout_attempts (fanout_state_id, created_at DESC);",
        "ALTER TABLE rescue_fanout_states DROP CONSTRAINT IF EXISTS rescue_fanout_states_current_phase_check;",
        "ALTER TABLE rescue_fanout_states ADD CONSTRAINT rescue_fanout_states_current_phase_check CHECK (current_phase BETWEEN 1 AND 20);",
        "ALTER TABLE rescue_fanout_attempts DROP CONSTRAINT IF EXISTS rescue_fanout_attempts_phase_check;",
        "ALTER TABLE rescue_fanout_attempts ADD CONSTRAINT rescue_fanout_attempts_phase_check CHECK (phase BETWEEN 1 AND 20);",
        r#"
        CREATE TABLE IF NOT EXISTS rescue_specialist_providers (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          user_id uuid REFERENCES users(id) ON DELETE SET NULL,
          name text NOT NULL,
          provider_type text NOT NULL CHECK (provider_type IN ('ong', 'cetas', 'ibama', 'environmental_police', 'fire_department', 'vet', 'rural_rescue', 'wildlife_rescue', 'marine_rescue', 'independent')),
          animal_scopes text[] NOT NULL DEFAULT ARRAY['general']::text[],
          city text,
          state_code text,
          lat double precision CHECK (lat IS NULL OR lat BETWEEN -90 AND 90),
          lng double precision CHECK (lng IS NULL OR lng BETWEEN -180 AND 180),
          service_radius_km numeric NOT NULL DEFAULT 50 CHECK (service_radius_km BETWEEN 1 AND 1000),
          phone text,
          email text,
          verified boolean NOT NULL DEFAULT false,
          active boolean NOT NULL DEFAULT true,
          created_at timestamptz NOT NULL DEFAULT now(),
          updated_at timestamptz NOT NULL DEFAULT now()
        );
        "#,
        "CREATE INDEX IF NOT EXISTS rescue_specialist_providers_location_idx ON rescue_specialist_providers (lat, lng) WHERE active = true AND lat IS NOT NULL AND lng IS NOT NULL;",
        "CREATE INDEX IF NOT EXISTS rescue_specialist_providers_state_scope_idx ON rescue_specialist_providers USING gin (animal_scopes);",
        "CREATE INDEX IF NOT EXISTS rescue_specialist_providers_user_idx ON rescue_specialist_providers (user_id) WHERE user_id IS NOT NULL;",
        r#"
        CREATE TABLE IF NOT EXISTS rescue_escalation_attempts (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          fanout_state_id uuid NOT NULL REFERENCES rescue_fanout_states(id) ON DELETE CASCADE,
          post_id uuid NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
          phase integer NOT NULL,
          strategy text NOT NULL,
          radius_km numeric NOT NULL,
          candidate_count integer NOT NULL DEFAULT 0,
          contacted_count integer NOT NULL DEFAULT 0,
          animal_scopes text[] NOT NULL DEFAULT '{}',
          created_at timestamptz NOT NULL DEFAULT now()
        );
        "#,
        "CREATE INDEX IF NOT EXISTS rescue_escalation_attempts_post_created_idx ON rescue_escalation_attempts (post_id, created_at DESC);",
        "CREATE INDEX IF NOT EXISTS rescue_escalation_attempts_state_created_idx ON rescue_escalation_attempts (fanout_state_id, created_at DESC);",
        "CREATE INDEX IF NOT EXISTS rescue_sessions_active_updated_idx ON rescue_sessions (updated_at DESC) WHERE status = 'active';",
        "CREATE INDEX IF NOT EXISTS rescue_sessions_post_idx ON rescue_sessions (post_id, created_at DESC);",
        "CREATE INDEX IF NOT EXISTS rescue_sessions_location_idx ON rescue_sessions (lat, lng) WHERE status = 'active';",
        "CREATE INDEX IF NOT EXISTS rescue_location_points_rescue_recorded_idx ON rescue_location_points (rescue_id, recorded_at DESC);",
        "CREATE INDEX IF NOT EXISTS rescue_incidents_rescue_created_idx ON rescue_incidents (rescue_id, created_at DESC);",
        "ALTER TABLE donations ADD COLUMN IF NOT EXISTS idempotency_key text;",
        "ALTER TABLE donations ALTER COLUMN ong_id DROP NOT NULL;",
        "ALTER TABLE donations ADD COLUMN IF NOT EXISTS purpose text NOT NULL DEFAULT 'ong_donation';",
        "ALTER TABLE donations ADD COLUMN IF NOT EXISTS recurrence text NOT NULL DEFAULT 'one_time';",
        "ALTER TABLE donations ADD COLUMN IF NOT EXISTS public_message text;",
        "ALTER TABLE donations DROP CONSTRAINT IF EXISTS donations_purpose_check;",
        "ALTER TABLE donations ADD CONSTRAINT donations_purpose_check CHECK (purpose IN ('ong_donation', 'platform_maintenance'));",
        "ALTER TABLE donations DROP CONSTRAINT IF EXISTS donations_recurrence_check;",
        "ALTER TABLE donations ADD CONSTRAINT donations_recurrence_check CHECK (recurrence IN ('one_time', 'monthly'));",
        "ALTER TABLE donations DROP CONSTRAINT IF EXISTS donations_platform_maintenance_bounds;",
        "ALTER TABLE donations ADD CONSTRAINT donations_platform_maintenance_bounds CHECK (purpose <> 'platform_maintenance' OR (ong_id IS NULL AND currency = 'BRL' AND recurrence = 'monthly' AND amount_cents BETWEEN 10 AND 100));",
        "CREATE UNIQUE INDEX IF NOT EXISTS donations_donor_idempotency_idx ON donations (donor_id, idempotency_key) WHERE idempotency_key IS NOT NULL;",
        r#"
        CREATE TABLE IF NOT EXISTS donation_ledger_entries (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          donation_id uuid NOT NULL REFERENCES donations(id) ON DELETE CASCADE,
          entry_type text NOT NULL,
          amount_cents bigint NOT NULL,
          currency char(3) NOT NULL,
          metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
          created_at timestamptz NOT NULL DEFAULT now()
        );
        "#,
        "CREATE INDEX IF NOT EXISTS donation_ledger_entries_donation_idx ON donation_ledger_entries (donation_id, created_at);",
        r#"
        CREATE TABLE IF NOT EXISTS volunteer_profiles (
          user_id uuid PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
          active boolean NOT NULL DEFAULT true,
          service_radius_km double precision NOT NULL DEFAULT 8 CHECK (service_radius_km BETWEEN 0.3 AND 100),
          animal_scopes text[] NOT NULL DEFAULT ARRAY['general']::text[],
          capabilities text[] NOT NULL DEFAULT '{}'::text[],
          notes text CHECK (notes IS NULL OR char_length(notes) <= 280),
          verified boolean NOT NULL DEFAULT false,
          responses_count bigint NOT NULL DEFAULT 0,
          arrived_count bigint NOT NULL DEFAULT 0,
          created_at timestamptz NOT NULL DEFAULT now(),
          updated_at timestamptz NOT NULL DEFAULT now()
        );
        "#,
        "CREATE INDEX IF NOT EXISTS volunteer_profiles_active_scopes_idx ON volunteer_profiles USING gin (animal_scopes) WHERE active = true;",
        r#"
        CREATE TABLE IF NOT EXISTS payment_webhook_events (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          provider text NOT NULL,
          provider_event_id text NOT NULL,
          event_type text NOT NULL,
          donation_id uuid REFERENCES donations(id) ON DELETE SET NULL,
          payload jsonb NOT NULL,
          processed_at timestamptz,
          created_at timestamptz NOT NULL DEFAULT now(),
          UNIQUE (provider, provider_event_id)
        );
        "#,
        "CREATE INDEX IF NOT EXISTS payment_webhook_events_donation_idx ON payment_webhook_events (donation_id, created_at DESC);",
        r#"
        CREATE TABLE IF NOT EXISTS ong_kyb_documents (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          ong_id uuid NOT NULL REFERENCES ong_profiles(id) ON DELETE CASCADE,
          document_type text NOT NULL,
          object_key text NOT NULL,
          public_url text NOT NULL,
          status text NOT NULL DEFAULT 'pending_review',
          reviewer_user_id uuid REFERENCES users(id) ON DELETE SET NULL,
          rejection_reason text,
          created_at timestamptz NOT NULL DEFAULT now(),
          reviewed_at timestamptz
        );
        "#,
        "CREATE INDEX IF NOT EXISTS ong_kyb_documents_ong_status_idx ON ong_kyb_documents (ong_id, status, created_at DESC);",
        r#"
        CREATE TABLE IF NOT EXISTS moderation_jobs (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          subject_type text NOT NULL,
          subject_id text NOT NULL,
          image_url text,
          status text NOT NULL DEFAULT 'queued',
          score smallint CHECK (score IS NULL OR score BETWEEN 0 AND 100),
          labels text[] NOT NULL DEFAULT '{}',
          provider text,
          error text,
          created_at timestamptz NOT NULL DEFAULT now(),
          updated_at timestamptz NOT NULL DEFAULT now()
        );
        "#,
        "CREATE INDEX IF NOT EXISTS moderation_jobs_status_created_idx ON moderation_jobs (status, created_at);",
    ];

    for statement in base_statements {
        sqlx::query(statement).execute(db).await?;
    }

    if postgis_enabled {
        for statement in [
            "CREATE EXTENSION IF NOT EXISTS postgis;",
            "ALTER TABLE posts ADD COLUMN IF NOT EXISTS geo geography(Point, 4326);",
            "UPDATE posts SET geo = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography WHERE geo IS NULL AND latitude IS NOT NULL AND longitude IS NOT NULL;",
            "CREATE INDEX IF NOT EXISTS posts_geo_gist_idx ON posts USING gist (geo);",
        ] {
            sqlx::query(statement).execute(db).await?;
        }
    }

    Ok(())
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChatEvent {
    pub room_id: String,
    pub message_id: String,
}

impl AppState {
    pub async fn subscribe_chat_room(&self, room_id: &str) -> broadcast::Receiver<ChatEvent> {
        let mut channels = self.chat_channels.lock().await;
        channels
            .entry(room_id.to_string())
            .or_insert_with(|| broadcast::channel(256).0)
            .subscribe()
    }

    pub async fn deliver_chat_event(&self, event: ChatEvent) {
        let channel = {
            let channels = self.chat_channels.lock().await;
            channels.get(&event.room_id).cloned()
        };
        if let Some(channel) = channel {
            let _ = channel.send(event);
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RescueEvent {
    pub rescue_id: String,
    pub post_id: String,
    pub status: String,
    pub lat: f64,
    pub lng: f64,
    pub accuracy: Option<f64>,
    pub updated_at: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FeedEvent {
    pub post_id: String,
    pub post_type: String,
    pub urgent: bool,
    pub rescue_status: String,
    pub lat: Option<f64>,
    pub lng: Option<f64>,
    pub created_at: String,
}

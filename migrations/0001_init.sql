CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS citext;

CREATE TYPE account_type AS ENUM ('person', 'ong', 'vet', 'admin');
CREATE TYPE post_type AS ENUM ('adoption', 'lost', 'found', 'emergency', 'campaign', 'post');
CREATE TYPE moderation_status AS ENUM ('queued', 'approved', 'rejected', 'needs_review');

CREATE TABLE users (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name text NOT NULL,
  email citext UNIQUE,
  avatar_url text,
  password_hash text NOT NULL,
  account_type account_type NOT NULL DEFAULT 'person',
  verified boolean NOT NULL DEFAULT false,
  trust_score smallint NOT NULL DEFAULT 20 CHECK (trust_score BETWEEN 0 AND 100),
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE refresh_tokens (
  token text PRIMARY KEY,
  user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  expires_at timestamptz NOT NULL,
  revoked_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX refresh_tokens_user_idx ON refresh_tokens (user_id, expires_at DESC);

CREATE TABLE ong_profiles (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  legal_name text NOT NULL,
  cnpj text UNIQUE,
  mission text,
  city text,
  state text,
  latitude double precision CHECK (latitude IS NULL OR latitude BETWEEN -90 AND 90),
  longitude double precision CHECK (longitude IS NULL OR longitude BETWEEN -180 AND 180),
  verified_at timestamptz,
  area_type text,
  contact_phone text,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE posts (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  author_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  post_type post_type NOT NULL,
  animal_type text NOT NULL,
  name text,
  breed text,
  age text,
  title text,
  description text NOT NULL,
  latitude double precision CHECK (latitude IS NULL OR latitude BETWEEN -90 AND 90),
  longitude double precision CHECK (longitude IS NULL OR longitude BETWEEN -180 AND 180),
  location_label text,
  neighborhood text,
  contact text,
  tags text[] NOT NULL DEFAULT '{}',
  urgent boolean NOT NULL DEFAULT false,
  text_only boolean NOT NULL DEFAULT false,
  likes_count integer NOT NULL DEFAULT 0 CHECK (likes_count >= 0),
  comments_count integer NOT NULL DEFAULT 0 CHECK (comments_count >= 0),
  shares_count integer NOT NULL DEFAULT 0 CHECK (shares_count >= 0),
  moderation_status moderation_status NOT NULL DEFAULT 'queued',
  fraud_risk smallint NOT NULL DEFAULT 0 CHECK (fraud_risk BETWEEN 0 AND 100),
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX posts_location_idx ON posts (latitude, longitude);
CREATE INDEX posts_feed_idx ON posts (created_at DESC, post_type, urgent);
CREATE INDEX posts_tags_idx ON posts USING gin(tags);
CREATE INDEX ong_profiles_location_idx ON ong_profiles (latitude, longitude);

CREATE TABLE media_upload_intents (
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

CREATE INDEX media_upload_intents_user_idx ON media_upload_intents (user_id, created_at DESC);

CREATE TABLE post_media (
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
  moderation_status moderation_status NOT NULL DEFAULT 'queued',
  moderation_labels text[] NOT NULL DEFAULT '{}',
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX post_media_post_idx ON post_media (post_id, sort_order);
CREATE INDEX post_media_moderation_idx ON post_media (moderation_status, created_at);

CREATE TABLE chat_rooms (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  post_id uuid REFERENCES posts(id) ON DELETE SET NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE chat_messages (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  room_id uuid NOT NULL REFERENCES chat_rooms(id) ON DELETE CASCADE,
  sender_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  body text NOT NULL CHECK (char_length(body) BETWEEN 1 AND 2000),
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX chat_messages_room_created_idx ON chat_messages (room_id, created_at DESC);

CREATE TABLE donations (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  donor_id uuid REFERENCES users(id) ON DELETE SET NULL,
  ong_id uuid NOT NULL REFERENCES ong_profiles(id) ON DELETE CASCADE,
  amount_cents bigint NOT NULL CHECK (amount_cents > 0),
  currency char(3) NOT NULL DEFAULT 'BRL',
  provider text,
  provider_reference text,
  status text NOT NULL DEFAULT 'pending',
  created_at timestamptz NOT NULL DEFAULT now()
);

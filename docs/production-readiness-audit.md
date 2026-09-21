# Helpers Production Readiness Audit

**Repository:** `techleadevelopers/helpers-platform-core`  
**Audit commit:** `2d4108ff521f314701d6a7d39e55d87317014105`  
**Audit date:** 2026-09-21  
**Scope:** repository inspection only. No production-code changes, optimizations, new infrastructure, or benchmark results are claimed by this document.

## Executive summary

The backend is a Rust/Axum/Tokio service with PostgreSQL as the durable source of truth, optional Redis rate limiting, NATS plain pub/sub for cross-process realtime events, PostgreSQL-backed push/fanout job tables, and in-process WebSocket broadcast channels. The rescue path has meaningful durability and idempotency foundations, but the repository does not yet provide measured production capacity or sufficient telemetry to answer the end-to-end rescue SLO questions.

The most important audit conclusion is architectural: **critical rescue and notification durability currently relies on PostgreSQL tables, while NATS and WebSocket delivery are best-effort realtime surfaces.** This is a defensible pilot shape, but it must be proven with failure/restart tests before public scale claims. NATS is configured with JetStream in `docker-compose.yml`, but `src/services/messaging/event_bus.rs` uses `publish`/`subscribe` only; there is no durable consumer, acknowledgement, replay, or lag measurement in the current implementation.

No numerical capacity, latency, error-rate, CPU, memory, Redis, NATS, worker, or notification baseline was available in the repository. The values in existing k6 thresholds are test gates, not observed production measurements and are not treated as baseline results.

## Evidence reviewed

- `Cargo.toml`, `Dockerfile`, `docker-compose.yml`, `.env.example`
- `README.md`, `docs/operational-mvp.md`, `docs/production-readiness.md`, `docs/architecture/README.md`
- `src/main.rs`, `src/config.rs`, `src/state.rs`, `src/domain.rs`, `src/error.rs`
- all route groups registered by `src/routes/mod.rs`, with detailed review of feed, rescue, chat, nearby, observability, health, and admin queue paths
- rescue fanout, notifications, security rate limiting, event bus, push worker, geocoding worker, and auth services
- migrations `0001_init.sql` through `0035_post_comment_idempotency.sql` and runtime schema creation in `src/state.rs`
- `benchmarks/k6/*`, `benchmarks/locust/locustfile.py`, `benchmarks/vegeta/*`, and observability configuration

## 1. Current architecture

```text
Mobile/Admin clients
  -> Axum HTTP and WebSocket routes
  -> PostgreSQL (authoritative state, migrations, rescue/fanout/notification jobs)
  -> optional Redis (rate limiting; client is configured at startup)
  -> NATS plain pub/sub (cross-process chat/feed/rescue realtime bridge)
  -> in-process Tokio broadcast channels (local WebSocket delivery)
  -> PostgreSQL polling workers
       - rescue fanout worker
       - push delivery/receipt worker
       - geocoding worker
  -> Expo push provider
  -> optional AI worker and SMTP/Cloudinary/Google Maps integrations
```

### Runtime and process roles

`src/config.rs` supports `all`, `api`, `workers`, `push-worker`, `geocode-worker`, and `fanout-worker`. `AppState::new` creates a lazy SQLx PostgreSQL pool, runs migration/runtime schema setup, connects to NATS, creates Redis client state, and starts workers according to role. `main.rs` serves HTTP only for `all` and `api`; worker-only processes wait for shutdown after initialization.

### API surface

`src/routes/mod.rs` registers authentication, feed/posts/media/search, geolocation/maps, chat HTTP/WebSocket, notifications, rescue sessions/WebSocket/responses/reports, NGO/trust/support/donations, admin moderation/KYB/users/queue operations, health, metrics, and observability endpoints. Admin endpoints use JWT claims with `AccountType::Admin` checks in the reviewed handlers.

### Persistence and migrations

PostgreSQL tables cover users, posts, media, chat, notifications, push delivery jobs, rescue sessions, location points, rescue events/responses, fanout state/attempts, specialist escalation, final reports, moderation, audit events, and support. Migrations include explicit idempotency and dedupe work, notably notification dedupe (`0014`), progressive fanout (`0018`), latency indexes (`0029`), fanout query fix (`0030`), push receipts (`0031`), active rescue idempotency (`0034`), and comment idempotency (`0035`). Runtime schema creation in `src/state.rs` still performs a large number of `CREATE/ALTER/UPDATE` statements during startup for compatibility with legacy preview databases.

## 2. Critical rescue data flow

1. Emergency post creation validates coordinates and persists the post/fanout state through the post route and rescue fanout service.
2. `rescue_fanout_states` is claimed by `src/services/rescue/fanout.rs` using `FOR UPDATE SKIP LOCKED`.
3. Candidate discovery reads push subscriptions, users, volunteer profiles, and notification history using a latitude/longitude bounding box, then applies Haversine distance and operational ranking in Rust.
4. Notification events and push delivery jobs are inserted into PostgreSQL, with per-user dedupe constraints for notification events.
5. `push_worker.rs` claims jobs, calls Expo with connect/request timeouts, persists provider tickets and receipts, retries with persisted backoff, invalidates invalid tokens, and dead-letters exhausted jobs.
6. A helper response is upserted by `(post_id, user_id, action)` and fanout counts are refreshed; confirmed responses pause active fanout without resolving the rescue.
7. Rescue events are persisted separately and also sent to local broadcast/NATS for realtime clients.
8. WebSockets deliver transient updates; clients must reload authoritative state from PostgreSQL after reconnect.

## 3. Findings by priority

### P0 — rescue integrity or availability risk

#### P0-1: Rescue creation and fanout-state atomicity is not uniformly proven

**Evidence:** `src/routes/operations/rescue.rs` commits `rescue_sessions`, location, and post status at lines 301–352, then calls `create_fanout_state_for_post` after commit at lines 354–355. The fanout service has a transaction-aware `create_fanout_state_for_post_tx`, and the post creation path has a test asserting a fanout state, but this audit did not establish that every emergency post creation path uses the transaction-aware variant.

**Impact:** A process/database failure between the post commit and the later fanout-state insert can leave a durable emergency post without a durable fanout command.

**Required evidence/fix:** Trace every emergency post creation path and add a failure-injection/integration test proving the post and fanout state commit atomically, or add a repair/reconciliation job with measured detection latency. Do not change infrastructure before this test.

#### P0-2: Critical event publication is fire-and-forget and plain NATS pub/sub is non-replayable

**Evidence:** `src/services/messaging/event_bus.rs` logs and returns from `client.publish` failures; `publish_chat`, `publish_rescue`, and `publish_feed` expose no delivery result. `spawn_subscription` uses `subscribe`, without durable consumer, acknowledgement, replay, or lag handling. Callers spawn publication tasks and do not await them (`broadcast_rescue_event` and `broadcast_chat_message`).

**Impact:** Cross-process realtime updates can be lost during NATS/API restart or subscriber outage. PostgreSQL remains authoritative for reviewed rescue/chat state, but clients may miss live updates until refresh/reconnect.

**Classification:** P0 for any workflow that treats realtime delivery as state; P1 for the current design if all critical state transitions are always re-read from PostgreSQL.

**Required evidence/fix:** Prove that no successful state depends solely on NATS/WebSocket delivery. Add restart/replay tests and explicit metrics. Use durable NATS semantics only if measured failure tests show replay is required; do not assume JetStream is automatically necessary.

#### P0-3: No proven end-to-end notification guarantee

**Evidence:** PostgreSQL notification and push-job rows are durable, and Expo ticket/receipt handling exists. There is no repository evidence of real provider delivery under failure, worker restart, provider outage, or receipt delay. `push_delivery_jobs` has no database lease/claim expiry field; the claim transaction marks rows `failed` before external delivery and relies on the current worker completing deferral/update.

**Impact:** A worker crash after claim can leave a job in `failed` with a future retry time determined by its previous state, or otherwise produce ambiguous delivery state. Provider acceptance is not equivalent to device delivery.

**Required evidence/fix:** Run worker-kill/restart and provider-failure tests; verify all claimed jobs become retryable and all terminal states are inspectable. Add a lease/reaper only if the test demonstrates an orphaned-claim problem.

### P1 — significant degradation or scale risk

#### P1-1: Fanout polling and candidate query can amplify PostgreSQL load

**Evidence:** `process_due_fanouts` polls every 15 seconds and selects up to 20 IDs without claiming in that selection query; each ID is then claimed serially. Candidate ranking loads all bounding-box rows, executes two correlated notification-count subqueries per subscription, performs Haversine filtering and sorting in Rust, and truncates to 250 candidates. Notification persistence then performs individual inserts per recipient inside one transaction.

**Impact:** At 100/500/1,000 simultaneous rescues, database CPU, pool wait, transaction duration, and row volume may become the first bottleneck. The current repository contains no EXPLAIN ANALYZE output or queue-age measurements.

**Required evidence/fix:** Capture query plans and timings with a representative seeded dataset, then benchmark fanout concurrency. Consider batching only after measurements identify the dominant cost.

#### P1-2: Fanout state selection can repeatedly scan due rows

**Evidence:** `rescue_fanout_states_due_idx` exists, but the outer `SELECT id ... LIMIT 20` in `process_due_fanouts` does not use `FOR UPDATE SKIP LOCKED`; the inner transaction protects individual work. Multiple worker processes can repeatedly select the same due IDs before inner claims reject them.

**Impact:** Extra database work and unfairness under multiple fanout workers; not directly a duplicate side effect because the inner claim is locked.

**Required evidence/fix:** Measure duplicate selection/claim rejection under multiple workers. If material, claim a batch atomically with a lease or locking query and add metrics before changing behavior.

#### P1-3: Feed and nearby paths duplicate expensive work

**Evidence:** `/v1/geo/nearby` calls `load_db_posts` with a limit of 100, then recomputes Haversine distance in Rust. `load_db_posts` also performs follow-up final-report and media queries. Feed ranking contains expression-heavy ordering and, in the fallback mode, bounding-box predicates plus Rust-side enrichment.

**Impact:** Nearby requests can be materially more expensive than the endpoint contract suggests, and feed latency may scale with the candidate set rather than response limit.

**Required evidence/fix:** Run `EXPLAIN (ANALYZE, BUFFERS)` for both PostGIS and fallback SQL with realistic row counts; compare query count and pool wait under load.

#### P1-4: WebSocket delivery is bounded, in-memory, and does not provide replay

**Evidence:** global rescue/feed channels have capacity 4096; per-room chat channels have capacity 256. Receiver lag causes the loop to terminate on broadcast error. WebSocket handlers do not implement a reconnect cursor or replay protocol.

**Impact:** Bursty events or slow clients can disconnect and miss updates. In-memory channel state is lost on process restart and is not shared without the NATS bridge.

**Required evidence/fix:** Add metrics for connected sockets, lagged receivers, disconnect causes, send duration, and replay/reconnect success. Use database history on reconnect before considering a broker redesign.

#### P1-5: Observability endpoint is a synthetic snapshot, not the requested metrics model

**Evidence:** `src/routes/platform/observability.rs` manually formats a small set of gauges. It does not expose request counters/histograms, in-flight requests, errors/timeouts, pool wait, query duration, Redis command metrics, NATS publish/consume metrics, worker/job metrics, or rescue fanout/notification latency histograms. `/metrics` itself performs several database counts and is JWT-admin protected, which may be incompatible with Prometheus scraping unless the deployment supplies authentication.

**Impact:** Current dashboards cannot establish P50/P90/P95/P99 API or rescue latency, identify pool starvation, calculate queue lag, or correlate a rescue across workers and notifications.

**Required evidence/fix:** Instrument the request, DB, Redis, NATS, workers, rescue, and provider boundaries. Decide whether metrics should use a protected scrape path/service account or a private network; do not expose internal metrics publicly.

#### P1-6: Health/readiness checks only prove PostgreSQL reachability

**Evidence:** `/readyz` calls `SELECT 1` and reports Redis/NATS/AI as configured booleans, not connectivity checks. `healthz` always returns OK.

**Impact:** An instance can receive rescue traffic while Redis rate limiting, NATS realtime, or required workers are unavailable. Whether this is acceptable depends on the explicitly defined degraded-mode contract, which is not yet measured/documented.

**Required evidence/fix:** Define dependency criticality by process role, then test restart/degraded modes. Readiness must reflect required dependencies for that role, while health should remain suitable for process liveness.

### P2 — important hardening gaps

#### P2-1: In-memory rate-limit fallback can grow without bounded cleanup

**Evidence:** `AppState.rate_limiter` stores `HashMap<String, Vec<Instant>>`. Entries are pruned only when the same key is checked; there is no periodic eviction, maximum key count, or capacity metric. Redis is used outside development, but local fallback remains reachable when no Redis client is configured.

**Impact:** In a misconfigured or degraded deployment, attacker-controlled keys can grow process memory.

**Required evidence/fix:** Test memory behavior with many unique IP/action keys and instrument key count. Prefer rejecting/failing closed outside development unless a bounded fallback is explicitly required.

#### P2-2: Redis rate limiting opens a multiplexed connection per check

**Evidence:** `check_redis` calls `get_multiplexed_async_connection()` on every request. No Redis command duration, pool wait, connection, or error metrics are present.

**Impact:** Connection setup/handshake overhead and unmeasured Redis pressure can inflate latency at scale.

**Required evidence/fix:** Measure command latency and connection behavior under load. Introduce a shared connection/pool only if profiling shows this is material and verify reconnect behavior.

#### P2-3: Admin observability is incomplete for the requested operational view

**Evidence:** Admin routes expose queue status/jobs, users, moderation, reports, and rescue final reports. The requested `/admin/metrics`, active rescue phase/candidate/notification/response view, worker status, Redis/NATS health, and notification inspection are not all present as dedicated endpoints. `/v1/observability` provides a broader snapshot but uses placeholders for several values, such as Sentry issue data and latency series.

**Impact:** Operators cannot yet reconstruct “active rescues” or distinguish notification requested, enqueued, provider accepted, delivered, and failed from one operational API.

#### P2-4: Structured correlation is inconsistent

**Evidence:** Rescue-specific logs include `rescue_id` in several paths; worker batch failures often log only `error`; request ID/trace ID propagation is not visible in `main.rs` or route handlers. OpenTelemetry initialization exists, but no explicit rescue span hierarchy or span attributes was found in reviewed paths.

**Impact:** A rescue cannot reliably be reconstructed across HTTP request, fanout attempt, push job, provider ticket, and helper response.

#### P2-5: External client construction and timeout policy are inconsistent

**Evidence:** Push worker reuses a configured `reqwest::Client` with connect/request timeouts. AI calls create a new `reqwest::Client` per request and set a three-second request timeout. Geocoding and maps integrations require separate inspection/measurement; no common retry, circuit-breaker, or dependency budget abstraction was found in the reviewed code.

**Impact:** Connection reuse, timeout, retry, and provider-failure behavior may differ across integrations.

#### P2-6: Startup migration/runtime schema work is on the application startup path

**Evidence:** `AppState::new` runs migration and a large `ensure_runtime_schema` statement list before serving. Legacy adoption has an advisory lock, but concurrent deploy/startup and long schema changes have not been benchmarked.

**Impact:** Restart or horizontal scale events can delay readiness and create startup contention during an incident.

#### P2-7: Security controls need evidence, not only code presence

**Evidence:** JWT auth, refresh-token rotation, Argon2 passwords, admin account checks, input validation, body limits, rate limiting, upload-intent constraints, and WebSocket tickets are present. The README itself identifies missing/imperfect areas: immediate access-token invalidation, full audit coverage, stronger role authorization, provider receipt evidence, and rate-limit proof. WebSocket rescue auth accepts an access token in the query string (`/v1/rescue/active/:id/ws`), which can leak through proxy/access logs unless logging is controlled.

**Required tests:** IDOR matrix for rescue/chat/admin resources, token revocation after deletion/ban, rate-limit bypass across proxy headers, upload abuse, WebSocket ticket/token leakage, and secret/log redaction.

### P3 — future improvements

- Replace manual Prometheus string generation with a maintained metrics registry once metric names/labels are finalized.
- Add a reproducible city-seeded dataset generator and benchmark report metadata.
- Add backup/restore and migration rollback/runbook evidence.
- Add per-city partitioning/sharding only if measured single-database limits require it.
- Evaluate Redis geospatial lookup only after PostgreSQL fallback/PostGIS query plans and fanout timings establish the need.
- Add operational dashboards and alerts for the SLOs defined below.

## 4. Race-condition and consistency review

| Area | Current control | Remaining risk / verification |
|---|---|---|
| Rescue session creation | PostgreSQL advisory transaction lock plus unique active-session migration support | Verify all entry points and commit/fanout atomicity under killed requests |
| Helper response | `ON CONFLICT (post_id, user_id, action)` upsert | Concurrent status transitions (`confirmed`, `cancelled`, `arrived`) need a state-transition policy and concurrency tests |
| Fanout worker | Inner `FOR UPDATE SKIP LOCKED` claim | Outer due-ID scan duplicates work; measure multi-worker fairness |
| Notification dedupe | Unique `(dedupe_key,user_id)` notification index | Push job itself has no equivalent unique constraint; prove event/job transaction behavior and retries |
| Chat message | Required idempotency key and unique sender/room/key index | Validate same key with different body semantics; test concurrent retries |
| Refresh token | Row lock, revoke-and-replace transaction | Test simultaneous refresh requests and session revocation semantics |
| WebSocket events | Local broadcast plus NATS bridge | Broadcast lag, process restart, subscriber outage, and replay are not covered |
| Post/fanout | Migration/runtime schema supports durable state | Verify emergency post creation cannot commit without fanout state |

## 5. Query and index audit priorities

The repository has many relevant indexes, including partial due-job indexes, rescue/fanout indexes, notification user/post/dedupe indexes, and optional PostGIS GIST indexing. No index should be added from this static review alone.

Run and archive `EXPLAIN (ANALYZE, BUFFERS)` for:

1. `ranked_candidates` standard phase, with 1k/10k/100k push subscriptions and realistic notification history.
2. `ranked_specialist_candidates` and verified fallback.
3. `process_due_fanouts` and the inner locked claim.
4. PostGIS feed ranking query with `ST_DWithin` and `ST_Distance` ordering.
5. Non-PostGIS feed bounding-box/ranking query.
6. `load_db_posts` follow-up report/media queries.
7. Chat room listing with last-message and unread lateral subqueries.
8. Push worker due-job claim and receipt selection.
9. Admin queue counts and active-rescue queries.

Measure pool acquisition wait separately from query execution time; current code exposes pool size/idle count but not wait duration.

## 6. Timeouts, retries, idempotency, and circuit breakers

### Present

- SQLx pool acquire timeout: five seconds.
- Push HTTP connect timeout: five seconds; request timeout: fifteen seconds.
- AI worker request timeout: three seconds.
- Push retry attempts, persisted `next_attempt_at`, exponential-ish minute backoff, and dead-letter state.
- Geocoding retry state and maximum attempts.
- Refresh-token rotation.
- Post/comment/chat/rescue response conflict-safe idempotency mechanisms.
- Admin queue retry operation.

### Not proven or missing

- Database statement timeouts and cancellation budgets.
- Explicit NATS publish timeout/error propagation to callers.
- Provider-specific retry classification beyond invalid-token detection.
- Circuit breaker/bulkhead behavior for AI, maps, geocoding, SMTP, Cloudinary, and Expo.
- Queue lag/oldest-job age metrics and alert thresholds.
- Lease expiry/recovery semantics for jobs claimed by a killed worker.
- End-to-end trace propagation through workers/provider calls.

A circuit breaker should be introduced only for a measured dependency failure mode where retries would worsen pressure; it should not be added by default.

## 7. Observability gaps against required metrics

| Required area | Current evidence | Gap |
|---|---|---|
| HTTP count/latency/errors/in-flight/timeouts | Tower HTTP tracing and a small `/metrics` snapshot | No request metric registry or percentiles |
| Database duration/pool wait/errors | `SELECT 1` latency, pool size/idle | No per-query duration, wait, or error counters |
| Redis | Optional client and rate-limit errors in logs | No command/connection/wait metrics |
| NATS | Warning logs on publish/subscribe failure | No published/consumed/error/lag/redelivery metrics; no durable consumer |
| Workers | Logs and DB status fields | No started/completed/failed/retried/duration/queue-depth metrics |
| Rescue | Impact endpoint computes historical medians; rescue logs include some IDs | No canonical rescue lifecycle latency counters/histograms |
| Structured logs | JSON tracing layer and selected rescue fields | No universal request/trace/user/job/notification correlation |
| Tracing | Optional OTLP exporter and tracing-opentelemetry layer | No verified rescue span trace or provider propagation artifact |

## 8. Proposed SLO measurement model (targets intentionally pending baseline)

Do not set final numeric budgets until the baseline run. Record at minimum:

- HTTP request count, RPS, status/error/timeout rate, in-flight requests, P50/P90/P95/P99 by route.
- DB query duration by operation, pool size/idle/acquire wait, connection errors, query errors.
- Redis command duration/errors and rate-limit fallback count.
- NATS publish/consume counts/errors and subscriber disconnects.
- Worker queue depth, oldest age, started/completed/failed/retried counts and duration.
- Rescue timestamps: post accepted, persistence committed, fanout started, each phase completed, notification event/job created, provider accepted, receipt delivered/failed, first helper response, resolution.

The primary rescue measurement is:

```text
POST /v1/posts accepted
  -> first durable rescue response with status confirmed or arrived
```

Use P50/P90/P95/P99 and a separate no-response/timeout population. Do not substitute notification enqueue time for device delivery time.

## 9. Load, spike, soak, and failure-test status

The repository contains k6 HTTP/WebSocket, Locust read-path, Vegeta feed, and SQL geo benchmark assets. They currently emphasize health/feed/geo/search/notifications and WebSocket chat. They do not yet provide a complete synthetic rescue lifecycle with creation, fanout, push-provider stub/receipt, helper response, resolution, and final-state assertions.

No load, spike, soak, or failure results are claimed because this audit environment did not execute infrastructure or external-provider tests.

Required blocked-test record before claiming readiness:

- **Blocked by:** no captured staging topology, seeded dataset, provider sandbox/stub, Prometheus time series, or raw benchmark reports in the repository.
- **Required dependencies:** Docker Compose services, Rust toolchain, PostgreSQL dataset, Redis, NATS, k6/Locust/Vegeta, synthetic users/tokens, controllable push-provider test double, and host CPU/RAM/DB telemetry.
- **How to reproduce:** start the Compose dependencies; configure `.env`; run `cargo run` with separate process roles; execute the existing benchmark commands in `benchmarks/README.md`; add raw output and infrastructure metadata under `benchmarks/reports/`.
- **Unverified:** capacity limits at 10/25/50/100/250/500/1000 concurrent users, 100/500/1000 rescue spikes, 1h/4h soak behavior, restart recovery, notification device delivery, and measured before/after improvements.

## 10. Recommended order of work (still audit-first)

1. Establish staging topology and seeded city datasets; capture current commit and infrastructure metadata.
2. Add measurement-only instrumentation for HTTP, SQLx pool/query timing, Redis, NATS, workers, rescue phases, notifications, and WebSockets.
3. Produce baseline and query plans without changing behavior.
4. Execute rescue-focused concurrency, provider-failure, restart, duplicate, delayed/out-of-order, and reconnect tests.
5. Fix only confirmed P0/P1 defects, with regression tests and before/after evidence.
6. Add admin rescue operations view and alerts based on the measured SLOs.
7. Repeat the exact benchmark matrix and publish the final report with raw artifacts.

## Audit disposition

**Not production-ready by evidence standard yet.** This is not a statement that the service cannot operate a closed pilot. It means the repository currently lacks the measured evidence required to answer capacity, latency, failure-recovery, notification-delivery, and regression questions for a city-by-city scale-up. The next safe action is instrumentation and reproducible measurement, not an infrastructure expansion or speculative optimization.

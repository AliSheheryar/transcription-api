# Design Decisions

This document is the "why" behind the code. `README.md` covers how to run it; this covers the reasoning — the alternatives that were rejected, the trade-offs accepted, and the principles that would inform the next decision.

Written from the seat of the person who has to live with these choices at 3 AM, defend them in review, and explain the AWS bill.

---

## 1. Problem & scope

**What we're building.** An HTTP API that takes an audio file and returns a transcript with timestamped segments, for thousands of concurrent users.

**What matters most.**
1. **Correctness under retry** — a client retrying a submit must never double-charge or duplicate work.
2. **Survivability** — a worker crash, a Redis restart, or a Whisper outage must not lose in-flight work.
3. **Fairness** — one heavy user must not starve everyone else.
4. **Explainability** — when something is slow or failing at 3 AM, the answer must be visible in a dashboard, not deduced from prose.

**What we deliberately do not care about.**
- Sub-second end-to-end latency (Whisper is 3s per audio-minute; that dominates everything).
- Multi-region strong consistency (single region until proven needed).
- Human user sessions (this is an API, not a web app; no cookies, no CSRF, no browser flows).
- Custom ML infrastructure (we buy Whisper; we don't train it).

---

## 2. The one decision everything else follows from: **async**

A sync `POST /transcribe` that blocks until Whisper returns breaks in four ways at real audio lengths:

| Failure mode | Root cause |
|---|---|
| Client times out at 30s | Browsers, proxies, and LBs kill long-lived connections |
| One slow job pins a worker | Every in-flight request holds a thread doing nothing but waiting on Whisper |
| Retries duplicate work | No way to identify "this is the same request" — burn two Whisper calls, charge twice |
| Server restart loses jobs | Jobs exist only in the running request; a crash erases them |

Async fixes all four. Submit writes the job to durable storage and returns `202` immediately; workers process asynchronously; clients poll for status. Same pattern as Stripe charges, S3 uploads, video encoders.

**Consequence:** the whole system is shaped around a durable **state machine** — `queued → processing → done | failed` — stored in Postgres. Every component reads and writes the same row; nothing calls anything else directly. That's what lets each piece scale, restart, or get replaced independently.

---

## 3. Stack, one component at a time

### Language: **Python 3.13**

**Why:** The workload is 100% I/O-bound (HTTP → Redis → Postgres → Kafka → HTTP-to-Whisper). Python's GIL is irrelevant when we spend 99.9% of wall time waiting on network. The AI ecosystem (OpenAI SDK, whisper.cpp bindings, pyannote for diarization, torchaudio for enhancement) is Python-first. Team hireability is best in class.

**What I gave up:** Raw per-core throughput. A Go equivalent uses ~1/3 the memory. That's irrelevant when Whisper at $0.006/audio-minute dwarfs infrastructure cost — we scale on model spend, not on CPU.

**Rejected:**
- **Go / Rust** — faster, but faster doesn't matter here, and hiring is harder. Novelty cost without upside.
- **Node.js** — viable, but the AI ecosystem is thinner and Pydantic-quality validation doesn't have a native equivalent.

### Web framework: **FastAPI + uvicorn**

**Why:**
- **Async-native.** ASGI handles thousands of concurrent I/O-bound requests on one process. WSGI (Flask, Django) needs a worker per request — non-starter for this workload.
- **OpenAPI from type hints.** The spec is generated from endpoint signatures; Swagger UI, ReDoc, and typed SDK generators are free. Docs never drift from code.
- **Pydantic-native.** Validation, serialization, and OpenAPI schemas are the same object. In Flask you write three separate things that drift.
- **Dependency injection fits the domain.** `Depends(rate_limited("submit"))` per endpoint is idiomatic and readable.

**Rejected:**
- **Flask / Django** — sync-first; async is bolted on. Wrong shape.
- **Starlette (raw)** — FastAPI is Starlette-plus. All the ergonomics cost nothing at runtime.
- **Litestar** — cleaner internals in some places, but smaller ecosystem and fewer engineers who know it. Migration risk without offsetting gain.
- **aiohttp** — lower-level; you'd rebuild half of FastAPI.

### Database: **PostgreSQL 16** (via `asyncpg`)

**Why:**
- **The data model is relational.** Users, API keys, jobs — with FKs, unique constraints, and a compound unique index for idempotency (`UNIQUE(user_id, idempotency_key)`). Textbook relational.
- **Constraints prevent bugs the app can't.** The unique-index-on-idempotency is what makes the submit-race recovery work: Postgres refuses to insert two jobs with the same `(user_id, idempotency_key)`, full stop. In a document store you'd reimplement this in application code.
- **`asyncpg` is fastest-in-class.** 3× throughput of psycopg2, native async, prepared-statement caching.
- **Boring is a feature.** Every cloud runs managed Postgres; every engineer knows it; every observability tool integrates with it. At 3 AM, boring wins.

**Rejected:**
- **MySQL** — fine, but Postgres has stronger constraint support (partial indexes, `EXCLUDE`, real transactional DDL). No good reason to prefer MySQL for a greenfield system.
- **DynamoDB / MongoDB** — no joins, no unique constraints beyond PK, no ad-hoc queries. Wrong for anything with relations. Also: vendor lock-in.
- **CockroachDB / Yugabyte** — solve multi-region strong consistency, which we don't need. Real complexity, real per-query overhead. Come back if we ever go multi-region.

**Ceiling:** single-writer Postgres tops out around ~50K writes/sec. Above that, the answer is Citus, logical replication with sharded writes, or an event-sourcing rethink. Good problem to have.

### Cache & rate limiter: **Redis 7**

**Why:**
- **Sub-millisecond and atomic.** Rate limiting needs a check-and-increment no other request can interleave with. Redis Lua scripts run single-threaded on the server — one round-trip, guaranteed atomic. In Postgres, that's either a serialized transaction (slow) or a race hazard (broken).
- **`SET NX EX` is the exact primitive idempotency needs.** Set-if-not-exists, with TTL, in one round-trip.
- **Sorted sets are the right structure for sliding-window log rate limiting.** `ZADD` + `ZREMRANGEBYSCORE` + `ZCARD` models a windowed event log natively.

**Rejected:**
- **Memcached** — no atomic scripting, no sorted sets. Can't do sliding-window rate limits correctly.
- **In-memory (single process)** — works for one API instance; broken the moment you scale to two. Each instance has its own counter → non-shared rate limit.
- **Postgres as the cache** — 5-20ms/query vs Redis's 0.5-1ms. Hitting the DB on every rate-limit check is a scaling anti-pattern.

### Job queue: **Kafka 3.7** (KRaft mode, via `aiokafka`)

**This was the hardest choice.** I want to be honest about it.

**Why Kafka:**
- **Durable + replayable.** A worker crash mid-job doesn't lose work (manual commit only after write succeeds → message redelivered). A bug fix can reprocess a window by resetting the consumer offset. Neither RabbitMQ nor SQS offers this out of the box.
- **Partitioned by `user_id` = per-user ordering + fair fan-out in one primitive.** A user's jobs process in submit order (same partition); different users spread across partitions and run in parallel.
- **Backpressure without dropping.** If workers can't keep up, the topic buffers. Producer stays fast. Consumer catches up.
- **Standard scaling pattern.** `HPA on Kafka consumer lag`. No custom autoscaling logic to write.

**Rejected:**
- **Redis Streams** — genuinely a strong contender at smaller scale. I chose Kafka because (a) Redis is optimized as a cache, not a durable log; (b) replay is clunkier; (c) doubling up Redis as both rate limiter and queue makes it a single point of failure for two independent concerns.
- **RabbitMQ** — no replay, weaker ordering under fan-out, no story for downstream consumers observing the event stream (real-time analytics on submits, audit trails). Kafka's log-as-interface is more future-proof.
- **SQS** — FIFO SQS caps at 300 msg/sec per group. If group = user, we've imposed a hard 300 jobs/user/sec ceiling we can't lift without leaving AWS. Also vendor lock-in.
- **Postgres SKIP LOCKED** — great pattern up to ~1M jobs/day. Above that, jobs table contention starts hurting the same Postgres that also serves reads. Kafka separates queue from state store — cleaner.

**What I gave up:** operational weight. Kafka is the heaviest piece in the stack. KRaft mode (no ZooKeeper) helps. Managed Kafka (Confluent Cloud, MSK, Aiven) removes most of the pain at a cost. If we were truly single-tenant, I'd start with Postgres SKIP LOCKED and migrate later. For "thousands of users," the migration cost hurts more than adopting Kafka up front.

**Why 32 partitions:** enough parallelism for ~10× today's expected throughput; enough fan-out that no single user exceeds ~3% of the pool; well under Kafka's per-broker partition budget. Formula: `max(peak_rps / worker_rps, 10 × largest_user_share, 12)`, rounded up to the nearest power of 2. Movable, but shouldn't move often.

### Transcription: **OpenAI Whisper** (with mock fallback)

**Why:** Best-in-class quality for the price. `whisper-1` is $0.006 per audio-minute — cheaper than any self-hosted alternative once you count GPU time.

**The mock is deliberate:** `transcribe.py` has `_whisper` (real API) and `_mock` (fake segments after 200ms). Same signature. `OPENAI_API_KEY` env var switches between them. This means tests run in CI without an API key or credits, local dev doesn't burn money, and anyone can swap Whisper for AssemblyAI, Deepgram, or local whisper.cpp by editing one function.

**What we'd change at scale:**
- Self-hosted `whisper.cpp` on GPU runs 10× realtime — 60s clip in ~6s. Break-even vs OpenAI depends on utilization; makes sense above ~100 hours/day of throughput.
- Chunk long files and transcribe in parallel — 60-min file into 6× 10-min chunks cuts wall-clock ~6×.
- Trim silence before upload — free 2× speedup on typical meeting audio.

### Auth: **Bearer tokens, SHA-256 hashed, `hmac.compare_digest`**

**Why:**
- **The audience is programmatic** — scripts, CI, backend services. Session cookies are wrong for machines; OAuth is overkill until there are third-party developers on the platform.
- **API keys are the industry norm** for this shape. OpenAI, Stripe, Anthropic, SendGrid, Twilio all work this way.
- **Instant revocation.** SHA-256 hashed keys + `revoked_at` column mean a leaked key dies the moment you set the column. JWT gives you no such thing without a blocklist that defeats statelessness.
- **`hmac.compare_digest`** is the right primitive — constant-time compare prevents timing-oracle attacks that could leak the key byte by byte. Standard-library one-liner solves a documented, exploitable class of bug.

**Rejected:**
- **JWT** — no instant revocation without a blocklist; historically buggy libraries (algorithm confusion, `none` attacks). Not worth the footgun.
- **OAuth 2.0** — the right answer when we build a marketplace of third-party apps. Overkill for direct API access. Comes later.
- **mTLS** — right for high-security internal service mesh; client-side cert management is a UX disaster for user-facing keys.

### Metrics: **prometheus-client → Prometheus → Grafana**

**Why:**
- **Pull model handles ephemeral workers naturally.** Prometheus scrapes on an interval; workers just expose `/metrics`. In a push model (StatsD), a worker dying mid-batch loses its final metrics.
- **PromQL fits the questions.** `histogram_quantile(0.95, sum by (le, endpoint) (rate(http_request_duration_seconds_bucket[5m])))` — P95 per endpoint in one line. Vendor query UIs make this a click-fest.
- **Dashboards as code.** Grafana dashboards live in git as JSON, provisioned automatically, reviewed like any code. Vendor SaaS dashboards live in a UI where anyone can edit them.
- **No vendor lock-in.** OpenTelemetry protocol on top means we can ship to DataDog/New Relic later without touching app code.

**Rejected:**
- **DataDog / New Relic** — fine tools, terrible ROI for a system where infra cost is already small. Vendor bills run $500-2000/month for modest workloads; self-hosted Prometheus is nearly free.

---

## 4. Multi-tenancy model

**Every job is scoped by `user_id`.** Poll and fetch queries include `WHERE user_id = $1`; other users' jobs return `404` (not `403` — never leak existence).

**Rate limits are per-user, per-endpoint, per-plan.** Redis keys are `rl:{user_id}:{dimension}`. Free tier: 60 rpm submit / 300 rpm poll / 60 rpm fetch. Pro tier: 10× that.

**Idempotency is namespaced by user.** Redis keys are `idem:{user_id}:{key}`. Two users sending the same idempotency key get two different jobs.

**Fairness comes from Kafka partitioning by `user_id`.** A heavy user occupies at most `1/N` of the worker pool (N = partition count). This is coarse but effective. If a truly hostile user needs to be contained: separate consumer group, separate partition, dedicated (starved) worker pool.

---

## 5. Idempotency: two systems, one truth

This one deserves detail because it's where the interesting correctness problem lives.

**The setup:** we use both Redis (fast) and Postgres (source of truth) to deduplicate submits.

- **Redis** — `SET NX EX` on `idem:{user}:{key}` returns the first-seen `job_id`. Sub-millisecond, atomic.
- **Postgres** — `UNIQUE(user_id, idempotency_key)` index makes the DB refuse a second insert with the same key.

**The race:** two requests A and B arrive with the same key at the same time.

1. A wins Redis SET NX with `A_id`. Hasn't yet inserted its DB row (network hop / GC pause).
2. B hits Redis, gets `A_id` back, queries `db.get_job(A_id)` → `None` (A hasn't inserted).
3. If we naively returned "not found", B would fall through and try to insert as `B_id`. Meanwhile A finally inserts. Now Redis maps the key to `A_id` but Postgres owns it as `A_id` — inconsistent, and every future retry from that user with the same key wastes an audio write and a doomed DB insert.

**The fix:** treat Postgres as source of truth and **repair Redis whenever we discover it's stale**. Two branches in `submit`:

- After a successful insert following a stale Redis hit → overwrite Redis to point to our `fresh_id`.
- After a unique-constraint catch (someone else already inserted) → look up the true winner and overwrite Redis with that id.

Also unlink the orphan audio blob in the losing path.

**Principle:** eventually-consistent caches converge on the source of truth by *writing back on discovery of inconsistency*, not by trying to prevent inconsistency up front. Trying to prevent it costs a round-trip on the hot path and doesn't actually work under all interleavings.

---

## 6. Failure handling

**Every failure lands in the job row.** The worker's `try/except` is deliberately broad — any exception becomes `{error_code, error_message, error_retryable}` on the row, surfaced on the next poll. Nothing ever "just disappears."

**`retryable` is the client's signal.** `RateLimitError` → retry with backoff. `InvalidFileFormat` → don't bother resubmitting.

**Kafka commit is manual and post-write.** If a worker crashes after processing but before commit, the message redelivers — the idempotent job-status update makes reprocessing safe.

**Poison-pill protection** (production hardening): after 3 crashes on the same message, route to `transcriptions.dlq` and page an operator. Prevents one bad audio file from blocking a partition forever.

**Circuit breaker + bulkheads around Whisper** (planned): when Whisper error rate crosses a threshold, fail fast for a cooldown period, then probe with N concurrent requests before fully closing. Separate connection pools per upstream so a saturated Whisper doesn't drain the S3 pool. State lives in Redis so all workers share it.

---

## 7. Observability

**RED metrics** (Rate / Errors / Duration) for every endpoint. **USE metrics** (Utilization / Saturation / Errors) for every worker.

Fifteen specific metrics land in Prometheus:
- HTTP: `http_requests_total`, `http_request_duration_seconds`, `http_requests_inflight`
- Auth: `auth_failures_total{reason}`
- Rate limit: `rate_limit_rejections_total{endpoint, plan}`
- Jobs: `jobs_submitted_total`, `jobs_dedup_hits_total`, `jobs_completed_total{status}`, `jobs_in_flight`
- Latency: `job_duration_seconds{status}`, `worker_transcribe_seconds`
- Upstream: `whisper_errors_total{error_code}`

**Grafana dashboard is provisioned from JSON**, checked into git, reviewed as code. Panels ordered by what you look at first during an incident: RPS → error rate → in-flight jobs → jobs/min, then latency, then per-component drilldowns.

**The Whisper latency panel is the most important one.** 99% of end-to-end job time is the Whisper call. If that doubles, everything doubles.

---

## 8. Trade-offs, honestly

| We chose | We gave up | Why we live with it |
|---|---|---|
| Python | ~3× runtime performance | I/O-bound workload; Whisper cost dwarfs CPU cost |
| Postgres | Horizontal scale ceiling ~50K writes/sec | We're nowhere near it; migration path is well-known |
| Kafka | Operational weight of running a broker | Replay + partitioning are worth it above single-tenant scale |
| Redis for rate limit + idempotency | Second stateful dependency to run | It's trivial; the alternative is broken correctness |
| API keys instead of JWT | Stateless auth verification | Instant revocation matters more than saving a DB hit |
| Self-hosted Prometheus | Building & maintaining the observability stack | Vendor lock-in and $$$ cost more at our scale |
| Single-region | Global latency, disaster resilience | Multi-region strong consistency is a bigger project than the whole current system |
| In-memory fallback mode | Two code paths in `app.py` | Tests run in CI without infra; enormous developer-productivity win |

---

## 9. What we deliberately didn't build

Every decision to *not* build something is worth writing down, because the pressure to build them will come back.

- **Signup / user-management endpoints.** Users seeded via `seed.py`. No password reset, no email verification. Comes when there's a self-serve product on top.
- **Batch endpoint.** The three primitives compose into batch behavior via a client-side loop. See "Why no batch" reasoning: server-side batch adds partial-failure semantics, a second state machine, and payload-size problems the async design was built to avoid.
- **Webhooks for job completion.** Poll is enough for now. Webhooks are on the roadmap; polling stays as fallback because webhooks fail.
- **Multi-region.** Cross-region write consistency is a fundamentally harder problem than the whole current system. Not until there's a customer that pays for it.
- **Custom ML training / fine-tuning.** We buy Whisper. Training is a different company.
- **A dashboard UI.** API-first; a UI can wrap the API later. Building the UI up front is committing to features we haven't validated.

---

## 10. Meta-principle

**Boring, correct, and battle-tested beats new, clever, and specialized — for the parts of the system that need to work at 3 AM.**

Every piece of this stack has been in production somewhere at scale for a decade or more (Postgres since 1996, Redis since 2009, Kafka since 2011, Prometheus since 2012). Nothing here is a bet.

The bet is in *how the pieces are composed*: the async submit/poll/fetch shape, partition-by-user-id fairness, Redis-repair-on-DB-race idempotency, the mock-transcriber fallback that keeps tests fast. That's where the engineering judgment lives; the stack is just the tools.

**The best compliment this stack could get:** "there's nothing surprising here." That's the point. Surprising infrastructure is expensive at 3 AM. Surprising *product* is the goal.

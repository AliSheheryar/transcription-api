# Production Architecture

The repo ships a single-process demo (in-memory dict + threading). This document describes the target production architecture — Kafka for the job queue, Redis for rate limiting and idempotency, circuit breakers around Whisper — and why each piece exists.

---

## Target topology

```
                          ┌──────────────┐
                          │   Clients    │
                          └──────┬───────┘
                                 │
                    ┌────────────▼────────────┐
                    │   API Gateway (Envoy)   │  ← TLS, JWT, global rate limit
                    └────────────┬────────────┘
                                 │
        ┌────────────────────────┴───────────────────────┐
        │                                                │
┌───────▼────────┐                              ┌────────▼────────┐
│  API service   │───reads/writes───┐           │ Result service  │
│ (FastAPI, N x) │                  │           │  (fetch only)   │
└───────┬────────┘                  │           └────────┬────────┘
        │                           │                    │
        │ produce job event         │                    │ signed URL
        ▼                           ▼                    ▼
┌──────────────────┐        ┌──────────────┐      ┌──────────────┐
│  Kafka topic     │        │   Redis      │      │    S3        │
│  transcriptions  │        │ (idempotency │      │ (audio +     │
│  (partitioned)   │        │  + rate-lim  │      │  transcripts)│
└────────┬─────────┘        │  + circuit   │      └──────────────┘
         │                  │   breaker)   │
         │                  └──────────────┘
         │                         ▲
         ▼                         │
┌──────────────────┐               │
│  Worker pool     │───state───────┘
│  (K x consumers) │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐        ┌──────────────────┐
│    Postgres      │        │  Whisper / ASR   │
│  (job state,     │        │   (upstream)     │
│   audit log)     │        └──────────────────┘
└──────────────────┘
```

**Split of responsibilities**
- **API service** — accepts uploads, enforces rate limits, dedupes via Redis, writes job row to Postgres, produces to Kafka, returns `202`.
- **Kafka** — durable job queue. Partitioned so N workers consume in parallel with ordering per key.
- **Worker pool** — consumes from Kafka, streams audio to Whisper, writes transcript to S3, updates Postgres.
- **Result service** — read-only, serves fetch. Separated so heavy reads don't crowd writes.
- **Redis** — hot path for anything that must be sub-millisecond: rate-limit counters, idempotency keys, circuit-breaker state.
- **Postgres** — source of truth for job state and audit log.

---

## Kafka as the job queue

**Why Kafka over Redis Lists / SQS / RabbitMQ.**
- **Durable + replayable** — a worker crash doesn't lose the job; a bug fix can reprocess a window of jobs by resetting the consumer offset.
- **Partitioned parallelism** — partition key = `tenant_id`. Same tenant's jobs land on the same partition → per-tenant ordering; different tenants fan out across the pool.
- **Backpressure without dropping** — if workers can't keep up, the topic buffers. Producers stay fast.
- **Exactly-once with the transactional API** — job produce + Postgres write can be linked so we never end up with a Postgres row and no Kafka message (or vice versa).

**Topic layout**
```
transcriptions.submitted     (partitions: 32, retention: 7d)
transcriptions.completed     (partitions: 32, retention: 7d) — for downstream consumers
transcriptions.dlq           (partitions: 8,  retention: 30d) — permanent failures
```

**Consumer contract**
- Manual commit **after** transcript is written to S3 and Postgres status = `done` — no acking work that isn't finished.
- Max-poll timeout tuned to Whisper's P99 latency + margin, so long jobs don't trigger rebalances.
- Rebalance listener flushes in-flight work to Postgres as `queued` again so nothing is stranded.

---

## Redis for rate limiting

**Algorithm: sliding-window log with Lua.**

Naive fixed-window rate limits allow bursts at the boundary (send 100 at 59.9s and 100 at 60.1s = 200/min through a "100/min" limit). Sliding-window log is exact.

```lua
-- rate_limit.lua — atomic, evaluated inside Redis
local key   = KEYS[1]
local now   = tonumber(ARGV[1])
local win   = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])

redis.call('ZREMRANGEBYSCORE', key, 0, now - win)
local count = redis.call('ZCARD', key)
if count >= limit then
  return {0, count}
end
redis.call('ZADD', key, now, now .. ':' .. redis.sha1hex(ARGV[4]))
redis.call('EXPIRE', key, math.ceil(win / 1000))
return {1, count + 1}
```

**Applied per tenant** at the API edge: `ratelimit:{tenant_id}` and `ratelimit:{tenant_id}:audio_seconds` (byte budget so a 3-hour upload doesn't count the same as a 30-second clip). On reject: `429` with `Retry-After` + `X-RateLimit-Remaining`.

**Why Lua.** The check-and-increment must be atomic; a Python `GET` + `INCR` has a race where two concurrent requests both see `count = limit - 1`. A Lua script is a single round-trip, single-threaded inside Redis.

---

## Redis for idempotency

Move the in-memory `IDEMPOTENCY` dict to Redis with a 24-hour TTL:

```
SET idempotency:{tenant}:{key} {job_id} NX EX 86400
```

`NX` (set-if-not-exists) is the atomic guard — the client that wins the race gets the write; every other retry reads back the same `job_id`. TTL bounds memory and stops old keys from ever colliding.

For clients that don't send the header, the key is `sha256(audio_bytes)`. Same primitive, same guarantees.

---

## Circuit breaker around Whisper

**The problem.** When Whisper is slow or throwing 5xx, our workers pile up waiting on it. Threads block, memory grows, healthy requests get starved, we DDoS the upstream trying to keep up. This is the cascading-failure pattern that took down Netflix in 2012 and gave the industry Hystrix.

**The pattern (three states).**

```
   ┌─────────┐  N failures within window   ┌────────┐
   │ CLOSED  │ ──────────────────────────▶ │  OPEN  │
   │(healthy)│                              │(fail   │
   └────▲────┘                              │  fast) │
        │                                   └───┬────┘
        │  M successes in HALF_OPEN             │  after cooldown
        │                                       │
        │       ┌──────────┐                    │
        └───────│HALF_OPEN │◀───────────────────┘
                │(probe)   │
                └──────────┘
```

**State in Redis** (shared across all worker instances):

```
whisper:cb:state       → "closed" | "open" | "half_open"
whisper:cb:failures    → count in current window
whisper:cb:opened_at   → epoch ms
whisper:cb:half_open_tokens → limited concurrency for probes
```

**Config**
- Threshold: **≥50% error rate over the last 20 requests** (not just N failures — % is robust to traffic bursts).
- Open cooldown: **30s**, exponential backoff up to **5m** on repeat opens.
- Half-open: allow **3 concurrent probes**; require **all 3 successes** to close.
- Timeout: **hard 60s** per Whisper call — no unbounded waits, even if the socket doesn't error.

**Behavior when OPEN.** The worker doesn't call Whisper. It marks the job `failed` with `error.code = "circuit_open"`, `retryable = true`, and re-produces to Kafka with a delay. Client polling sees `failed → retryable=true`; internal retry re-enqueues automatically. Users get instant `503`-style feedback instead of stacking behind a wall of dead requests.

**Bulkheading.** Separate connection pool for Whisper (10 conns) vs S3 (50 conns) vs Postgres (20 conns). One saturated upstream can't drain the pools the others need. This is the second half of the circuit-breaker discipline that most people skip.

---

## Rate limiting AND circuit breaker together

They solve mirror-image problems:

| | Rate limit | Circuit breaker |
|---|---|---|
| Protects | **us** from clients | **upstream** from us |
| Direction | Inbound | Outbound |
| Trigger | Request count | Error rate / latency |
| Action | Reject with `429` | Fail fast without calling |

Both live in Redis because both need shared state across an N-instance service and sub-ms lookups on the hot path.

---

## Additional patterns worth calling out

- **Outbox pattern** — API writes job to Postgres and outbox row in the same transaction; a relay ships outbox rows to Kafka. Prevents the "wrote to DB, crashed before publishing" split-brain.
- **Idempotent consumers** — every Kafka message carries `job_id`; worker checks Postgres before doing work. Kafka gives at-least-once; this makes the effect exactly-once.
- **Poison-pill quarantine** — after 3 worker crashes on the same message, route to `transcriptions.dlq` and page an operator. Never let one bad audio file block a partition forever.
- **Backpressure signaling** — API exposes `X-Queue-Depth` header when Kafka lag > threshold. Sophisticated clients slow down; naive ones just see slower polling.
- **Signed URL fetch** — result service returns `303 See Other` to a pre-signed S3 URL with 5-min TTL. Transcript bytes never touch our servers on the read path.
- **Graceful shutdown** — SIGTERM stops Kafka consumption, waits for in-flight jobs to commit, then exits. Deploys don't drop jobs.

---

## What's in this repo vs. the target

| Concern | This repo | Production |
|---|---|---|
| Job state | Python dict | Postgres + audit log |
| Queue | `threading.Thread` | Kafka (partitioned, replayable) |
| Idempotency | In-memory dict | Redis `SET NX EX` |
| Rate limiting | None | Redis sliding-window (Lua) |
| Circuit breaker | None | Redis-backed 3-state breaker + bulkheads |
| Audio storage | Local `storage/` | S3 with signed URLs |
| Auth | None | JWT at gateway, per-tenant scoping |
| Deploy model | Single process | API pool + worker pool + result pool, HPA on Kafka lag |
| Failure recovery | Lost on restart | Kafka replay + Postgres source-of-truth |

The public API surface (`POST /v1/transcriptions`, `GET /{id}`, `GET /{id}/transcript`) is identical in both. That's the point of the async design: infrastructure can be swapped end-to-end without any client seeing a change.

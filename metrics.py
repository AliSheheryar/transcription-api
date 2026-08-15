"""Prometheus metrics — exposed at /metrics.

Registered as module-level so both the API and the worker can import and
increment the same counters. Scraped by Prometheus, visualized in Grafana.
"""
from prometheus_client import Counter, Gauge, Histogram


# ---------- HTTP layer ----------

http_requests_total = Counter(
    "http_requests_total",
    "Count of HTTP requests.",
    ["method", "endpoint", "status"],
)

http_request_duration_seconds = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency in seconds.",
    ["method", "endpoint"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

http_requests_inflight = Gauge(
    "http_requests_inflight",
    "In-flight HTTP requests.",
    ["endpoint"],
)


# ---------- Auth & rate limiting ----------

auth_failures_total = Counter(
    "auth_failures_total",
    "Count of auth failures.",
    ["reason"],  # missing | invalid
)

rate_limit_rejections_total = Counter(
    "rate_limit_rejections_total",
    "Count of 429 rejections.",
    ["endpoint", "plan"],
)


# ---------- Jobs ----------

jobs_submitted_total = Counter(
    "jobs_submitted_total",
    "Count of jobs accepted (excludes dedup hits).",
    ["plan"],
)

jobs_dedup_hits_total = Counter(
    "jobs_dedup_hits_total",
    "Count of submits deduped by idempotency.",
    ["plan"],
)

jobs_completed_total = Counter(
    "jobs_completed_total",
    "Count of jobs that reached a terminal state.",
    ["status"],  # done | failed
)

job_duration_seconds = Histogram(
    "job_duration_seconds",
    "End-to-end job processing time (queued → done/failed).",
    ["status"],
    buckets=(0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600),
)

worker_transcribe_seconds = Histogram(
    "worker_transcribe_seconds",
    "Time spent inside the transcriber (Whisper API or mock).",
    buckets=(0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300),
)

jobs_in_flight = Gauge(
    "jobs_in_flight",
    "Jobs currently being processed by workers.",
)


# ---------- Upstream health ----------

whisper_errors_total = Counter(
    "whisper_errors_total",
    "Whisper/OpenAI errors by exception class.",
    ["error_code"],
)

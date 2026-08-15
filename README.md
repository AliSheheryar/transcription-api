# Transcription API

Async audio transcription API with submit / poll / fetch endpoints. Accepts any common audio format (mp3, wav, m4a, flac, ogg, opus, webm, aac, wma, aiff, amr, 3gp, mka). Uses OpenAI Whisper when `OPENAI_API_KEY` is set, otherwise returns a mock transcript so tests and local dev work without an API key.

## Design

**Approach.** The naive `POST /transcribe → wait → return text` breaks the moment transcription takes more than a few seconds: clients time out, retries duplicate work, crashes lose jobs. So it's **async**: submit returns immediately, work happens in the background, client polls.

**Key decisions.**
1. **Three endpoints, not one** — submit / poll / fetch. Poll stays cheap (tiny status blob); fetch ships the full transcript only once.
2. **State machine in durable storage** — `queued → processing → done | failed`. Every component reads the same row; nothing calls anything else directly.
3. **Idempotency-Key + content-hash fallback** — retries collapse to one job, so a flaky network never double-charges.
4. **Failures written into the job row** — worker exception becomes `{code, message, retryable}` on the next poll; nothing disappears silently.
5. **Whisper swappable behind one function** — real call when `OPENAI_API_KEY` is set, mock otherwise. Tests + local dev need no key.
6. **Format check is loose** — extension OR `audio/*` mime. Whisper decides if the bytes are actually audio.

**End-to-end flow.**

```
CLIENT                     API                           WORKER                    STORAGE
  │                         │                              │                          │
  │  POST audio ──────────▶ │                              │                          │
  │                         │ validate format (400 if bad) │                          │
  │                         │ compute idempotency key      │                          │
  │                         │ ┌─ LOCK ─────────────────┐   │                          │
  │                         │ │ key seen? → return id  │   │                          │
  │                         │ │ else: mint UUID,       │   │                          │
  │                         │ │   write audio blob ────┼──────────────────────────▶  │
  │                         │ │   JOBS[id]=queued      │   │                          │
  │                         │ └────────────────────────┘   │                          │
  │                         │ start background thread ────▶│                          │
  │ ◀── 202 { job_id } ─────│                              │ status: processing       │
  │                         │                              │ read audio ◀────────────│
  │                         │                              │ call Whisper (or mock)   │
  │                         │                              │ write transcript.json ──▶│
  │                         │                              │ status: done             │
  │                         │                              │ (on error → failed +     │
  │                         │                              │  {code, msg, retryable}) │
  │  GET /{id} ───────────▶ │ read JOBS[id] under lock     │                          │
  │ ◀── {status} ───────────│                              │                          │
  │        ...poll...       │                              │                          │
  │  GET /{id}/transcript ▶ │ if not done → 409            │                          │
  │                         │ else read blob ◀─────────────────────────────────────── │
  │ ◀── { segments } ───────│                              │                          │
```

**The one principle.** Every layer talks only through the job row. That's what lets the dict become Postgres, the thread become a queue, and the local file become S3 — with the endpoints unchanged.

## Install

```bash
pip install -r requirements.txt
```

## Run

```bash
# Real transcription
export OPENAI_API_KEY=sk-...        # PowerShell: $env:OPENAI_API_KEY="sk-..."
uvicorn app:app --reload

# Mock mode (no key)
uvicorn app:app --reload
```

Optional: `WHISPER_MODEL` (default `whisper-1`), `API_KEY` (enables auth — see below).

## Authentication

Set `API_KEY` to require a Bearer token on every endpoint:

```powershell
$env:API_KEY = "your-long-random-secret"
uvicorn app:app
```

Clients send it in the `Authorization` header:

```bash
curl -H "Authorization: Bearer your-long-random-secret" \
     -F file=@meeting.mp3 http://localhost:8000/v1/transcriptions
```

Without `API_KEY` set, auth is **disabled** (dev/test mode). Missing/wrong keys return `401 Unauthorized`. Comparison uses `hmac.compare_digest` (constant-time) to prevent timing attacks.

## Endpoints

| Verb | Path | Success | Notes |
|---|---|---|---|
| POST | `/v1/transcriptions` | `202 { job_id, status }` | multipart `file`; optional `Idempotency-Key` header |
| GET  | `/v1/transcriptions/{id}` | `200 { status }` | `queued \| processing \| done \| failed`; failed jobs include `error: { code, message, retryable }` |
| GET  | `/v1/transcriptions/{id}/transcript` | `200 { segments: [{start, end, text}] }` | `409` if not yet `done`, `404` if unknown id |

## Example

```bash
curl -F file=@meeting.m4a http://localhost:8000/v1/transcriptions
# → { "job_id": "abc...", "status": "queued" }

curl http://localhost:8000/v1/transcriptions/abc...
# → { "status": "done" }

curl http://localhost:8000/v1/transcriptions/abc.../transcript
# → { "segments": [ ... ] }
```

## Record from your mic → transcribe end-to-end

Real-mic demo. Requires `OPENAI_API_KEY` (mock mode also works, you'll just get a fake transcript).

```powershell
# Terminal 1 — start the API with your key
$env:OPENAI_API_KEY = "sk-..."
uvicorn app:app

# Terminal 2 — record 5s from default mic, upload, poll, print
python record_and_transcribe.py                 # 5s default
python record_and_transcribe.py --seconds 15    # longer clip
```

The script records 16 kHz mono WAV, POSTs it to `/v1/transcriptions`, polls until `done`, and prints timestamped segments.

## Test

```bash
pip install pytest
pytest -v
```

### Coverage

| Test | What it proves |
|---|---|
| `test_full_flow` | Submit → poll → fetch happy path |
| `test_rejects_non_audio` | Non-audio uploads → `400` before any job is created |
| `test_accepts_many_formats` | mp3 / m4a / flac / ogg / opus / webm all accepted |
| `test_idempotency` | Same `Idempotency-Key` returns the same `job_id` |
| `test_missing_job` | Unknown id → `404` on both poll and fetch |
| `test_failed_job_surfaces_error` | Worker exception → `status: failed` with typed `error.code`, `error.message`, `error.retryable` on the poll response |
| `test_fetch_before_done_returns_409` | Fetching before processing finishes returns `409` with the current status — never a partial or fake transcript |
| `test_content_hash_dedup_without_key` | Two submits of identical bytes with no `Idempotency-Key` collapse to one job via content hash |
| `test_concurrent_submits_collapse_to_one_job` | 5 threads racing the same `Idempotency-Key` produce exactly one job — the `LOCK` around the dedup table holds under contention |
| `test_auth_disabled_when_no_env` | No `API_KEY` set → all requests accepted (dev mode) |
| `test_auth_requires_bearer_when_key_set` | `API_KEY` set + missing Authorization header → `401` |
| `test_auth_rejects_wrong_key` | Wrong bearer token → `401` |
| `test_auth_accepts_correct_key` | Correct bearer token → `202` |

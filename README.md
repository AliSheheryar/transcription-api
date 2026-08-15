# Transcription API

Async audio transcription API with submit / poll / fetch endpoints. Accepts any common audio format (mp3, wav, m4a, flac, ogg, opus, webm, aac, wma, aiff, amr, 3gp, mka). Uses OpenAI Whisper when `OPENAI_API_KEY` is set, otherwise returns a mock transcript so tests and local dev work without an API key.

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

Optional: `WHISPER_MODEL` (default `whisper-1`).

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

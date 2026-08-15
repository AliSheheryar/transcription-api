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

- `POST /v1/transcriptions` — multipart `file`, returns `202 { job_id, status }`. Optional `Idempotency-Key` header.
- `GET  /v1/transcriptions/{id}` — poll status (`queued | processing | done | failed`). Failed jobs include `error: { code, message, retryable }`.
- `GET  /v1/transcriptions/{id}/transcript` — fetch `{ segments: [{start, end, text}] }` once done.

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
python test_api.py
```

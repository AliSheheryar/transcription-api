# Transcription API

Async MP3 transcription API with submit / poll / fetch endpoints.

## Install

```bash
pip install -r requirements.txt
```

## Run

```bash
uvicorn app:app --reload
```

## Endpoints

- `POST /v1/transcriptions` — submit an mp3 (multipart `file`), returns `202 { job_id, status }`. Optional `Idempotency-Key` header.
- `GET  /v1/transcriptions/{id}` — poll job status.
- `GET  /v1/transcriptions/{id}/transcript` — fetch segments once done.

## Test

```bash
python test_api.py
```

import hashlib
import hmac
import os
import threading
import time
import uuid
from enum import Enum
from pathlib import Path
from typing import Optional

from fastapi import Depends, FastAPI, File, Header, HTTPException, UploadFile
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field

API_DESCRIPTION = """
Async audio transcription API. Three endpoints — **submit**, **poll**, **fetch** —
form a durable job pipeline over OpenAI Whisper (or a mock in dev mode).

## Flow

1. `POST /v1/transcriptions` with an audio file → `202 { job_id, status: "queued" }`.
2. `GET /v1/transcriptions/{job_id}` — poll until `status == "done"` or `"failed"`.
3. `GET /v1/transcriptions/{job_id}/transcript` — retrieve `{ segments: [{start, end, text}] }`.

## Auth

Set the `API_KEY` env var on the server to require `Authorization: Bearer <key>`
on every request. When unset, auth is disabled (dev mode).

## Idempotency

Include an `Idempotency-Key` header on submit; a retried submit with the same
key returns the same `job_id`. Without a header, the SHA-256 of the audio bytes
is used as an implicit key.
"""

tags_metadata = [
    {"name": "transcriptions", "description": "Submit, poll, and fetch transcription jobs."},
]

app = FastAPI(
    title="Transcription API",
    version="1.0.0",
    description=API_DESCRIPTION,
    openapi_tags=tags_metadata,
    contact={"name": "AliSheheryar", "url": "https://github.com/AliSheheryar/transcription-api"},
    license_info={"name": "MIT"},
)

bearer_scheme = HTTPBearer(auto_error=False, description="Bearer token; value of the server's API_KEY env var.")


def require_api_key(
    creds: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme),
) -> None:
    """Validate Bearer token against API_KEY env var. If unset, auth is disabled."""
    expected = os.environ.get("API_KEY")
    if not expected:
        return
    if creds is None or creds.scheme.lower() != "bearer":
        raise HTTPException(401, "missing bearer token", headers={"WWW-Authenticate": "Bearer"})
    if not hmac.compare_digest(creds.credentials, expected):
        raise HTTPException(401, "invalid api key", headers={"WWW-Authenticate": "Bearer"})


JOBS: dict[str, dict] = {}
IDEMPOTENCY: dict[str, str] = {}
LOCK = threading.Lock()

STORAGE = Path(__file__).parent / "storage"
STORAGE.mkdir(exist_ok=True)

AUDIO_EXTS = {
    ".mp3", ".wav", ".m4a", ".mp4", ".aac", ".flac", ".ogg", ".oga",
    ".opus", ".webm", ".wma", ".aiff", ".aif", ".amr", ".3gp", ".mka",
}
AUDIO_MIME_PREFIXES = ("audio/", "video/mp4", "video/webm")


# ---------- Schemas (drive OpenAPI + client SDK types) ----------

class JobStatus(str, Enum):
    queued = "queued"
    processing = "processing"
    done = "done"
    failed = "failed"


class SubmitResponse(BaseModel):
    job_id: str = Field(..., description="Opaque job identifier.", examples=["8f2d1c74-..."])
    status: JobStatus = Field(..., description="Initial status; usually `queued`.")


class JobError(BaseModel):
    code: str = Field(..., description="Exception class name from the worker.", examples=["RateLimitError"])
    message: str = Field(..., description="Human-readable error message.")
    retryable: bool = Field(..., description="True if a resubmit is likely to succeed.")


class JobStatusResponse(BaseModel):
    job_id: str
    status: JobStatus
    error: Optional[JobError] = Field(None, description="Present only when `status == \"failed\"`.")


class Segment(BaseModel):
    start: float = Field(..., description="Segment start time (seconds).", examples=[0.0])
    end: float = Field(..., description="Segment end time (seconds).", examples=[1.5])
    text: str = Field(..., description="Transcribed text for this segment.")


class TranscriptResponse(BaseModel):
    segments: list[Segment]


class HTTPError(BaseModel):
    detail: str


# ---------- Transcribers ----------

def _transcribe_whisper(audio_path: Path) -> list[dict]:
    """Call OpenAI Whisper API. Requires OPENAI_API_KEY."""
    from openai import OpenAI
    client = OpenAI()
    with audio_path.open("rb") as f:
        result = client.audio.transcriptions.create(
            model=os.environ.get("WHISPER_MODEL", "whisper-1"),
            file=f,
            response_format="verbose_json",
            timestamp_granularities=["segment"],
        )
    return [
        {"start": s["start"], "end": s["end"], "text": s["text"].strip()}
        for s in (result.segments or [])
    ]


def _transcribe_mock(audio_path: Path) -> list[dict]:
    return [
        {"start": 0.0, "end": 1.5, "text": "Hello world."},
        {"start": 1.5, "end": 3.0, "text": f"Transcribed {audio_path.name}."},
    ]


def _process(job_id: str, audio_path: Path) -> None:
    with LOCK:
        JOBS[job_id]["status"] = "processing"
    import json
    try:
        if os.environ.get("OPENAI_API_KEY"):
            segments = _transcribe_whisper(audio_path)
        else:
            time.sleep(1)
            segments = _transcribe_mock(audio_path)
        transcript_path = STORAGE / f"{job_id}.json"
        transcript_path.write_text(json.dumps({"segments": segments}))
        with LOCK:
            JOBS[job_id]["status"] = "done"
            JOBS[job_id]["transcript_path"] = str(transcript_path)
    except Exception as e:
        with LOCK:
            JOBS[job_id]["status"] = "failed"
            JOBS[job_id]["error"] = {"code": type(e).__name__, "message": str(e), "retryable": True}


# ---------- Endpoints ----------

@app.post(
    "/v1/transcriptions",
    status_code=202,
    response_model=SubmitResponse,
    responses={
        400: {"model": HTTPError, "description": "Unsupported audio format."},
        401: {"model": HTTPError, "description": "Missing or invalid API key."},
    },
    tags=["transcriptions"],
    summary="Submit an audio file for transcription.",
    dependencies=[Depends(require_api_key)],
)
async def submit(
    file: UploadFile = File(..., description="Audio file (mp3, wav, m4a, flac, ogg, opus, webm, ...)."),
    idempotency_key: Optional[str] = Header(
        None,
        alias="Idempotency-Key",
        description="Optional client-supplied dedup key; retries with the same key return the same job_id.",
    ),
):
    """Accepts an audio file, enqueues a transcription job, returns `202` with a `job_id`.

    Does not block on transcription. Use the poll endpoint to check status.
    """
    ext = Path(file.filename or "").suffix.lower()
    mime = (file.content_type or "").lower()
    ext_ok = ext in AUDIO_EXTS
    mime_ok = any(mime.startswith(p) for p in AUDIO_MIME_PREFIXES)
    if not (ext_ok or mime_ok):
        raise HTTPException(400, f"Unsupported audio format (ext='{ext}', mime='{mime}')")

    data = await file.read()
    key = idempotency_key or hashlib.sha256(data).hexdigest()

    with LOCK:
        if key in IDEMPOTENCY:
            existing_id = IDEMPOTENCY[key]
            return {"job_id": existing_id, "status": JOBS[existing_id]["status"]}

        job_id = str(uuid.uuid4())
        audio_path = STORAGE / f"{job_id}{ext or '.bin'}"
        audio_path.write_bytes(data)

        JOBS[job_id] = {"status": "queued", "audio_path": str(audio_path)}
        IDEMPOTENCY[key] = job_id

    threading.Thread(target=_process, args=(job_id, audio_path), daemon=True).start()
    return {"job_id": job_id, "status": "queued"}


@app.get(
    "/v1/transcriptions/{job_id}",
    response_model=JobStatusResponse,
    responses={
        401: {"model": HTTPError, "description": "Missing or invalid API key."},
        404: {"model": HTTPError, "description": "Unknown job_id."},
    },
    tags=["transcriptions"],
    summary="Poll job status.",
    dependencies=[Depends(require_api_key)],
)
def poll(job_id: str):
    """Returns current `status` (queued | processing | done | failed).

    On `failed`, includes an `error` object with `code`, `message`, and `retryable`.
    """
    with LOCK:
        job = JOBS.get(job_id)
    if not job:
        raise HTTPException(404, "job not found")
    resp = {"job_id": job_id, "status": job["status"]}
    if job["status"] == "failed":
        resp["error"] = job.get("error", {"code": "processing_error", "message": "unknown", "retryable": True})
    return resp


@app.get(
    "/v1/transcriptions/{job_id}/transcript",
    response_model=TranscriptResponse,
    responses={
        401: {"model": HTTPError, "description": "Missing or invalid API key."},
        404: {"model": HTTPError, "description": "Unknown job_id."},
        409: {"description": "Job is not yet `done`; response body carries the current status."},
    },
    tags=["transcriptions"],
    summary="Fetch the finished transcript.",
    dependencies=[Depends(require_api_key)],
)
def fetch(job_id: str):
    """Returns `{ segments: [{start, end, text}] }` once the job is `done`.

    Returns `409 Conflict` with `{status}` if called before completion — never a
    partial or fabricated transcript.
    """
    with LOCK:
        job = JOBS.get(job_id)
    if not job:
        raise HTTPException(404, "job not found")
    if job["status"] != "done":
        return JSONResponse({"status": job["status"]}, status_code=409)
    import json
    return json.loads(Path(job["transcript_path"]).read_text())

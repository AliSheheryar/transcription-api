"""Transcription API — production multi-user version.

Modes:
- Production (default when DATABASE_URL is set): Postgres users/jobs,
  Redis rate-limit + idempotency, Kafka produce-to-worker.
- In-memory (INMEMORY=1 or DATABASE_URL unset in tests): keeps the demo/test
  path working without infrastructure.

Endpoints:
  POST /v1/transcriptions            → 202 { job_id, status }
  GET  /v1/transcriptions/{id}       → 200 { status, [error] }
  GET  /v1/transcriptions/{id}/transcript → 200 { segments } | 409
"""
import asyncio
import hashlib
import os
import threading
import time
import uuid
from contextlib import asynccontextmanager
from enum import Enum
from pathlib import Path
from typing import Optional

from fastapi import Depends, FastAPI, File, Header, HTTPException, Request, UploadFile
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field


STORAGE = Path(__file__).parent / "storage"
STORAGE.mkdir(exist_ok=True)

AUDIO_EXTS = {
    ".mp3", ".wav", ".m4a", ".mp4", ".aac", ".flac", ".ogg", ".oga",
    ".opus", ".webm", ".wma", ".aiff", ".aif", ".amr", ".3gp", ".mka",
}
AUDIO_MIME_PREFIXES = ("audio/", "video/mp4", "video/webm")


def is_inmemory_mode() -> bool:
    return os.environ.get("INMEMORY") == "1" or not os.environ.get("DATABASE_URL")


# ---------- In-memory fallback state (for tests/dev without infra) ----------

_JOBS: dict[str, dict] = {}
_IDEMPOTENCY: dict[str, str] = {}
_LOCK = threading.Lock()


# ---------- Lifespan: init/close pools ----------

@asynccontextmanager
async def lifespan(app: FastAPI):
    if not is_inmemory_mode():
        import db, ratelimit, kqueue
        await db.init_pool()
        await ratelimit.init()
        await kqueue.start_producer()
    yield
    if not is_inmemory_mode():
        import db, ratelimit, kqueue
        await kqueue.stop_producer()
        await ratelimit.close()
        await db.close_pool()


app = FastAPI(
    title="Transcription API",
    version="2.0.0",
    description="Multi-user async transcription. Postgres + Redis + Kafka in production; in-memory in dev.",
    lifespan=lifespan,
)

bearer_scheme = HTTPBearer(auto_error=False)


# ---------- Schemas ----------

class JobStatus(str, Enum):
    queued = "queued"
    processing = "processing"
    done = "done"
    failed = "failed"


class SubmitResponse(BaseModel):
    job_id: str
    status: JobStatus


class JobError(BaseModel):
    code: str
    message: str
    retryable: bool


class JobStatusResponse(BaseModel):
    job_id: str
    status: JobStatus
    error: Optional[JobError] = None


class Segment(BaseModel):
    start: float
    end: float
    text: str


class TranscriptResponse(BaseModel):
    segments: list[Segment]


# ---------- Auth ----------

async def authenticate(
    creds: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme),
):
    """Returns an AuthenticatedUser (production) or a sentinel dict (in-memory)."""
    if is_inmemory_mode():
        expected = os.environ.get("API_KEY")
        if expected:
            if creds is None or creds.scheme.lower() != "bearer":
                raise HTTPException(401, "missing bearer token", headers={"WWW-Authenticate": "Bearer"})
            import hmac
            if not hmac.compare_digest(creds.credentials, expected):
                raise HTTPException(401, "invalid api key", headers={"WWW-Authenticate": "Bearer"})
        return {"user_id": 0, "plan": "free", "email": "dev@local"}

    if creds is None or creds.scheme.lower() != "bearer":
        raise HTTPException(401, "missing bearer token", headers={"WWW-Authenticate": "Bearer"})

    import db
    user = await db.lookup_key(creds.credentials)
    if user is None:
        raise HTTPException(401, "invalid api key", headers={"WWW-Authenticate": "Bearer"})
    return user


def _user_tuple(user) -> tuple[int, str]:
    if isinstance(user, dict):
        return user["user_id"], user["plan"]
    return user.user_id, user.plan


# ---------- Rate limit dependency factory ----------

def rate_limited(dimension: str):
    async def _dep(user=Depends(authenticate)):
        if is_inmemory_mode():
            return user
        import ratelimit
        user_id, plan = _user_tuple(user)
        d = await ratelimit.check(user_id, plan, dimension)
        if not d.allowed:
            retry_s = max(1, d.window_ms // 1000)
            raise HTTPException(
                429,
                f"rate limit exceeded ({d.limit} per {d.window_ms // 1000}s)",
                headers={
                    "Retry-After": str(retry_s),
                    "X-RateLimit-Limit": str(d.limit),
                    "X-RateLimit-Remaining": str(d.remaining),
                },
            )
        return user
    return _dep


# ---------- In-memory worker (only used in INMEMORY mode) ----------

def _inmemory_process(job_id: str, audio_path: Path) -> None:
    import json
    with _LOCK:
        _JOBS[job_id]["status"] = "processing"
    try:
        if os.environ.get("OPENAI_API_KEY"):
            from transcribe import _whisper
            segments = _whisper(audio_path)
        else:
            time.sleep(0.2)
            from transcribe import _mock
            segments = _mock(audio_path)
        transcript_path = STORAGE / f"{job_id}.json"
        transcript_path.write_text(json.dumps({"segments": segments}))
        with _LOCK:
            _JOBS[job_id]["status"] = "done"
            _JOBS[job_id]["transcript_path"] = str(transcript_path)
    except Exception as e:
        with _LOCK:
            _JOBS[job_id]["status"] = "failed"
            _JOBS[job_id]["error"] = {"code": type(e).__name__, "message": str(e), "retryable": True}


# ---------- Endpoints ----------

@app.post("/v1/transcriptions", status_code=202, response_model=SubmitResponse)
async def submit(
    file: UploadFile = File(...),
    idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key"),
    user=Depends(rate_limited("submit")),
):
    ext = Path(file.filename or "").suffix.lower()
    mime = (file.content_type or "").lower()
    if not (ext in AUDIO_EXTS or any(mime.startswith(p) for p in AUDIO_MIME_PREFIXES)):
        raise HTTPException(400, f"Unsupported audio format (ext='{ext}', mime='{mime}')")

    data = await file.read()
    idem_key = idempotency_key or hashlib.sha256(data).hexdigest()

    if is_inmemory_mode():
        with _LOCK:
            if idem_key in _IDEMPOTENCY:
                existing = _IDEMPOTENCY[idem_key]
                return {"job_id": existing, "status": _JOBS[existing]["status"]}
            job_id = str(uuid.uuid4())
            audio_path = STORAGE / f"{job_id}{ext or '.bin'}"
            audio_path.write_bytes(data)
            _JOBS[job_id] = {"status": "queued", "audio_path": str(audio_path)}
            _IDEMPOTENCY[idem_key] = job_id
        threading.Thread(target=_inmemory_process, args=(job_id, audio_path), daemon=True).start()
        return {"job_id": job_id, "status": "queued"}

    # Production path
    import db, ratelimit, kqueue
    user_id, _ = _user_tuple(user)
    fresh_id = str(uuid.uuid4())
    stored_id = await ratelimit.idempotency_lookup_or_set(user_id, idem_key, fresh_id)
    if stored_id != fresh_id:
        job = await db.get_job(uuid.UUID(stored_id), user_id)
        if job:
            return {"job_id": stored_id, "status": job["status"]}
    audio_path = STORAGE / f"{fresh_id}{ext or '.bin'}"
    audio_path.write_bytes(data)
    try:
        await db.insert_job(uuid.UUID(fresh_id), user_id, str(audio_path), idem_key)
    except Exception:
        # Race: another submit won; fetch the winner
        winner = await db.get_job_id_by_idempotency(user_id, idem_key)
        if winner:
            job = await db.get_job(winner, user_id)
            return {"job_id": str(winner), "status": job["status"] if job else "queued"}
        raise
    await kqueue.publish_job(fresh_id, user_id)
    return {"job_id": fresh_id, "status": "queued"}


@app.get("/v1/transcriptions/{job_id}", response_model=JobStatusResponse)
async def poll(job_id: str, user=Depends(rate_limited("poll"))):
    if is_inmemory_mode():
        with _LOCK:
            job = _JOBS.get(job_id)
        if not job:
            raise HTTPException(404, "job not found")
        resp = {"job_id": job_id, "status": job["status"]}
        if job["status"] == "failed":
            resp["error"] = job.get("error", {"code": "processing_error", "message": "unknown", "retryable": True})
        return resp

    import db
    user_id, _ = _user_tuple(user)
    job = await db.get_job(uuid.UUID(job_id), user_id)
    if not job:
        raise HTTPException(404, "job not found")
    resp = {"job_id": job_id, "status": job["status"]}
    if job["status"] == "failed":
        resp["error"] = {
            "code": job["error_code"] or "processing_error",
            "message": job["error_message"] or "unknown",
            "retryable": bool(job["error_retryable"]),
        }
    return resp


@app.get("/v1/transcriptions/{job_id}/transcript", response_model=TranscriptResponse)
async def fetch(job_id: str, user=Depends(rate_limited("fetch"))):
    import json
    if is_inmemory_mode():
        with _LOCK:
            job = _JOBS.get(job_id)
        if not job:
            raise HTTPException(404, "job not found")
        if job["status"] != "done":
            return JSONResponse({"status": job["status"]}, status_code=409)
        return json.loads(Path(job["transcript_path"]).read_text())

    import db
    user_id, _ = _user_tuple(user)
    job = await db.get_job(uuid.UUID(job_id), user_id)
    if not job:
        raise HTTPException(404, "job not found")
    if job["status"] != "done":
        return JSONResponse({"status": job["status"]}, status_code=409)
    return json.loads(Path(job["transcript_path"]).read_text())

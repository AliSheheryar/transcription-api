import hashlib
import os
import threading
import time
import uuid
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, File, Header, HTTPException, UploadFile
from fastapi.responses import JSONResponse

app = FastAPI(title="Transcription API")

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


@app.post("/v1/transcriptions", status_code=202)
async def submit(
    file: UploadFile = File(...),
    idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key"),
):
    ext = Path(file.filename or "").suffix.lower()
    mime = (file.content_type or "").lower()
    ext_ok = ext in AUDIO_EXTS
    mime_ok = any(mime.startswith(p) for p in AUDIO_MIME_PREFIXES)
    if not (ext_ok or mime_ok):
        raise HTTPException(
            400,
            f"Unsupported audio format (ext='{ext}', mime='{mime}')",
        )

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


@app.get("/v1/transcriptions/{job_id}")
def poll(job_id: str):
    with LOCK:
        job = JOBS.get(job_id)
    if not job:
        raise HTTPException(404, "job not found")
    resp = {"job_id": job_id, "status": job["status"]}
    if job["status"] == "failed":
        resp["error"] = job.get("error", {"code": "processing_error", "retryable": True})
    return resp


@app.get("/v1/transcriptions/{job_id}/transcript")
def fetch(job_id: str):
    with LOCK:
        job = JOBS.get(job_id)
    if not job:
        raise HTTPException(404, "job not found")
    if job["status"] != "done":
        return JSONResponse({"status": job["status"]}, status_code=409)
    import json
    return json.loads(Path(job["transcript_path"]).read_text())

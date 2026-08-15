import hashlib
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


def _process(job_id: str, audio_path: Path) -> None:
    time.sleep(2)
    with LOCK:
        JOBS[job_id]["status"] = "processing"
    time.sleep(2)
    segments = [
        {"start": 0.0, "end": 1.5, "text": "Hello world."},
        {"start": 1.5, "end": 3.0, "text": f"Transcribed {audio_path.name}."},
    ]
    transcript_path = STORAGE / f"{job_id}.json"
    import json
    transcript_path.write_text(json.dumps({"segments": segments}))
    with LOCK:
        JOBS[job_id]["status"] = "done"
        JOBS[job_id]["transcript_path"] = str(transcript_path)


@app.post("/v1/transcriptions", status_code=202)
async def submit(
    file: UploadFile = File(...),
    idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key"),
):
    if not file.filename or not file.filename.lower().endswith(".mp3"):
        raise HTTPException(400, "Expected .mp3 file")

    data = await file.read()
    key = idempotency_key or hashlib.sha256(data).hexdigest()

    with LOCK:
        if key in IDEMPOTENCY:
            existing_id = IDEMPOTENCY[key]
            return {"job_id": existing_id, "status": JOBS[existing_id]["status"]}

        job_id = str(uuid.uuid4())
        audio_path = STORAGE / f"{job_id}.mp3"
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
        resp["error"] = {"code": "processing_error", "retryable": True}
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

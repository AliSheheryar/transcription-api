"""Kafka consumer worker: pulls jobs, calls Whisper, updates Postgres.

Run: python worker.py

Manual commit AFTER transcript + status write, so a crash mid-job re-delivers
the message and the worker picks it back up.
"""
import asyncio
import json
import signal
import uuid
from pathlib import Path

import db
import kqueue
import transcribe


STORAGE = Path(__file__).parent / "storage"
STORAGE.mkdir(exist_ok=True)


async def process_one(payload: dict) -> None:
    job_id = uuid.UUID(payload["job_id"])
    job = await db.get_job_internal(job_id)
    if job is None:
        print(f"[worker] job {job_id} not found — skipping")
        return
    if job["status"] not in ("queued", "processing"):
        print(f"[worker] job {job_id} already {job['status']} — skipping")
        return

    audio_path = Path(job["audio_path"])
    await db.set_job_processing(job_id)
    print(f"[worker] processing {job_id} ({audio_path.name})")
    try:
        segments = await transcribe.transcribe(audio_path)
        transcript_path = STORAGE / f"{job_id}.json"
        transcript_path.write_text(json.dumps({"segments": segments}))
        await db.set_job_done(job_id, str(transcript_path))
        print(f"[worker] done {job_id}")
    except Exception as e:
        await db.set_job_failed(job_id, type(e).__name__, str(e), retryable=True)
        print(f"[worker] failed {job_id}: {type(e).__name__}: {e}")


async def main() -> None:
    await db.init_pool()
    stop = asyncio.Event()

    def _stop(*_):
        print("[worker] shutdown requested")
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            asyncio.get_running_loop().add_signal_handler(sig, _stop)
        except NotImplementedError:
            pass  # windows

    print("[worker] consuming from Kafka…")
    async for item in kqueue.consume_jobs():
        if stop.is_set():
            break
        try:
            await process_one(item["payload"])
            await item["consumer"].commit()
        except Exception as e:
            print(f"[worker] unexpected error: {e!r}")
            # Do NOT commit on unexpected errors — message will be re-delivered

    await db.close_pool()


if __name__ == "__main__":
    asyncio.run(main())

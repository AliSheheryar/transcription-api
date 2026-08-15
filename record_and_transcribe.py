"""Record from your microphone and transcribe via the API.

Usage:
    # 1. Start the server in one terminal (with your key set)
    #    PowerShell:  $env:OPENAI_API_KEY="sk-..."; uvicorn app:app
    #    bash:        export OPENAI_API_KEY=sk-...; uvicorn app:app
    #
    # 2. In another terminal:
    #    python record_and_transcribe.py            # 5 sec default
    #    python record_and_transcribe.py --seconds 15
    #    python record_and_transcribe.py --api http://localhost:8000
"""
import argparse
import io
import sys
import time
import wave

import httpx
import numpy as np
import sounddevice as sd


SAMPLE_RATE = 16_000  # Whisper is happy with 16 kHz mono
CHANNELS = 1


def record(seconds: float) -> bytes:
    print(f"Recording {seconds}s from default mic ({SAMPLE_RATE} Hz mono)...")
    audio = sd.rec(int(seconds * SAMPLE_RATE), samplerate=SAMPLE_RATE, channels=CHANNELS, dtype="int16")
    sd.wait()
    print("Done recording.")

    buf = io.BytesIO()
    with wave.open(buf, "wb") as wf:
        wf.setnchannels(CHANNELS)
        wf.setsampwidth(2)  # int16 → 2 bytes
        wf.setframerate(SAMPLE_RATE)
        wf.writeframes(audio.tobytes())
    return buf.getvalue()


def submit(api: str, wav_bytes: bytes) -> str:
    r = httpx.post(
        f"{api}/v1/transcriptions",
        files={"file": ("mic.wav", wav_bytes, "audio/wav")},
        timeout=60.0,
    )
    r.raise_for_status()
    body = r.json()
    print(f"Submitted → job_id={body['job_id']} status={body['status']}")
    return body["job_id"]


def poll_until_done(api: str, job_id: str, timeout_s: float = 300.0) -> None:
    deadline = time.time() + timeout_s
    last = None
    while time.time() < deadline:
        r = httpx.get(f"{api}/v1/transcriptions/{job_id}", timeout=10.0)
        r.raise_for_status()
        body = r.json()
        if body["status"] != last:
            print(f"  status: {body['status']}")
            last = body["status"]
        if body["status"] == "done":
            return
        if body["status"] == "failed":
            sys.exit(f"Job failed: {body.get('error')}")
        time.sleep(1.0)
    sys.exit("Timed out waiting for job")


def fetch(api: str, job_id: str) -> None:
    r = httpx.get(f"{api}/v1/transcriptions/{job_id}/transcript", timeout=10.0)
    r.raise_for_status()
    segments = r.json().get("segments", [])
    print("\n--- Transcript ---")
    for s in segments:
        print(f"[{s['start']:>6.2f} → {s['end']:>6.2f}] {s['text']}")
    print("------------------")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--seconds", type=float, default=5.0)
    p.add_argument("--api", default="http://localhost:8000")
    args = p.parse_args()

    wav_bytes = record(args.seconds)
    job_id = submit(args.api, wav_bytes)
    poll_until_done(args.api, job_id)
    fetch(args.api, job_id)


if __name__ == "__main__":
    main()

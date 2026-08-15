"""The transcribers. `transcribe(path)` picks the right one from env."""
import asyncio
import os
from pathlib import Path


def _whisper(audio_path: Path) -> list[dict]:
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


def _mock(audio_path: Path) -> list[dict]:
    return [
        {"start": 0.0, "end": 1.5, "text": "Hello world."},
        {"start": 1.5, "end": 3.0, "text": f"Transcribed {audio_path.name}."},
    ]


async def transcribe(audio_path: Path) -> list[dict]:
    """Async wrapper — Whisper is sync + I/O bound, run in a thread."""
    if os.environ.get("OPENAI_API_KEY"):
        return await asyncio.to_thread(_whisper, audio_path)
    await asyncio.sleep(0.2)
    return _mock(audio_path)

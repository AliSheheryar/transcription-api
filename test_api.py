import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import app as app_module
from app import app
from fastapi.testclient import TestClient

client = TestClient(app)


def _wait_for(job_id: str, target: str, timeout_s: float = 15.0) -> dict:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        r = client.get(f"/v1/transcriptions/{job_id}")
        assert r.status_code == 200
        body = r.json()
        if body["status"] == target:
            return body
        time.sleep(0.1)
    raise AssertionError(f"job {job_id} never reached {target}; last={body}")


def test_full_flow():
    files = {"file": ("sample.wav", b"RIFFfake-wav-bytes", "audio/wav")}
    r = client.post("/v1/transcriptions", files=files)
    assert r.status_code == 202
    job_id = r.json()["job_id"]
    assert r.json()["status"] == "queued"

    _wait_for(job_id, "done")

    r = client.get(f"/v1/transcriptions/{job_id}/transcript")
    assert r.status_code == 200
    assert "segments" in r.json()
    assert len(r.json()["segments"]) == 2


def test_rejects_non_audio():
    files = {"file": ("x.txt", b"data", "text/plain")}
    r = client.post("/v1/transcriptions", files=files)
    assert r.status_code == 400


def test_accepts_many_formats():
    for name, mime in [
        ("a.mp3", "audio/mpeg"),
        ("a.m4a", "audio/mp4"),
        ("a.flac", "audio/flac"),
        ("a.ogg", "audio/ogg"),
        ("a.opus", "audio/opus"),
        ("a.webm", "audio/webm"),
    ]:
        r = client.post("/v1/transcriptions", files={"file": (name, b"x", mime)})
        assert r.status_code == 202, f"{name} rejected"


def test_idempotency():
    files = {"file": ("a.flac", b"same-bytes", "audio/flac")}
    r1 = client.post("/v1/transcriptions", files=files, headers={"Idempotency-Key": "k1"})
    files = {"file": ("a.flac", b"same-bytes", "audio/flac")}
    r2 = client.post("/v1/transcriptions", files=files, headers={"Idempotency-Key": "k1"})
    assert r1.json()["job_id"] == r2.json()["job_id"]


def test_missing_job():
    assert client.get("/v1/transcriptions/nope").status_code == 404


def test_failed_job_surfaces_error(monkeypatch):
    def boom(_path):
        raise RuntimeError("whisper exploded")

    monkeypatch.setattr(app_module, "_transcribe_mock", boom)
    files = {"file": ("f.mp3", b"unique-fail-bytes", "audio/mpeg")}
    r = client.post("/v1/transcriptions", files=files)
    job_id = r.json()["job_id"]

    body = _wait_for(job_id, "failed")
    assert body["error"]["code"] == "RuntimeError"
    assert body["error"]["message"] == "whisper exploded"
    assert body["error"]["retryable"] is True


def test_fetch_before_done_returns_409(monkeypatch):
    def slow(_path):
        time.sleep(2)
        return [{"start": 0, "end": 1, "text": "hi"}]

    monkeypatch.setattr(app_module, "_transcribe_mock", slow)
    files = {"file": ("s.mp3", b"slow-bytes-" + uuid.uuid4().hex.encode(), "audio/mpeg")}
    r = client.post("/v1/transcriptions", files=files)
    job_id = r.json()["job_id"]

    r = client.get(f"/v1/transcriptions/{job_id}/transcript")
    assert r.status_code == 409
    assert r.json()["status"] in ("queued", "processing")


def test_content_hash_dedup_without_key():
    payload = b"identical-content-" + uuid.uuid4().hex.encode()
    r1 = client.post("/v1/transcriptions", files={"file": ("a.mp3", payload, "audio/mpeg")})
    r2 = client.post("/v1/transcriptions", files={"file": ("b.mp3", payload, "audio/mpeg")})
    assert r1.status_code == 202 and r2.status_code == 202
    assert r1.json()["job_id"] == r2.json()["job_id"]


def test_concurrent_submits_collapse_to_one_job():
    payload = b"race-" + uuid.uuid4().hex.encode()
    key = "race-key-" + uuid.uuid4().hex
    barrier = threading.Barrier(5)

    def submit():
        barrier.wait()
        return client.post(
            "/v1/transcriptions",
            files={"file": ("r.mp3", payload, "audio/mpeg")},
            headers={"Idempotency-Key": key},
        ).json()["job_id"]

    with ThreadPoolExecutor(max_workers=5) as pool:
        job_ids = set(pool.map(lambda _: submit(), range(5)))

    assert len(job_ids) == 1, f"expected 1 job, got {len(job_ids)}: {job_ids}"


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-v"]))

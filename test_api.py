import os
os.environ["INMEMORY"] = "1"
os.environ.pop("DATABASE_URL", None)

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
    body = {}
    while time.time() < deadline:
        r = client.get(f"/v1/transcriptions/{job_id}")
        assert r.status_code == 200
        body = r.json()
        if body["status"] == target:
            return body
        time.sleep(0.1)
    raise AssertionError(f"job {job_id} never reached {target}; last={body}")


def test_full_flow():
    r = client.post("/v1/transcriptions", files={"file": ("s.wav", b"RIFFfake", "audio/wav")})
    assert r.status_code == 202
    job_id = r.json()["job_id"]
    _wait_for(job_id, "done")
    r = client.get(f"/v1/transcriptions/{job_id}/transcript")
    assert r.status_code == 200
    assert len(r.json()["segments"]) == 2


def test_rejects_non_audio():
    r = client.post("/v1/transcriptions", files={"file": ("x.txt", b"data", "text/plain")})
    assert r.status_code == 400


def test_accepts_many_formats():
    for name, mime in [("a.mp3","audio/mpeg"),("a.m4a","audio/mp4"),("a.flac","audio/flac"),
                       ("a.ogg","audio/ogg"),("a.opus","audio/opus"),("a.webm","audio/webm")]:
        r = client.post("/v1/transcriptions", files={"file": (name, b"x", mime)})
        assert r.status_code == 202


def test_idempotency():
    r1 = client.post("/v1/transcriptions", files={"file": ("a.flac", b"same", "audio/flac")}, headers={"Idempotency-Key": "k1"})
    r2 = client.post("/v1/transcriptions", files={"file": ("a.flac", b"same", "audio/flac")}, headers={"Idempotency-Key": "k1"})
    assert r1.json()["job_id"] == r2.json()["job_id"]


def test_missing_job():
    assert client.get("/v1/transcriptions/nope").status_code == 404


def test_failed_job_surfaces_error(monkeypatch):
    def boom(_path):
        raise RuntimeError("whisper exploded")

    import transcribe
    monkeypatch.setattr(transcribe, "_mock", boom)
    r = client.post("/v1/transcriptions", files={"file": ("f.mp3", b"unique-fail-" + os.urandom(8).hex().encode(), "audio/mpeg")})
    job_id = r.json()["job_id"]
    body = _wait_for(job_id, "failed")
    assert body["error"]["code"] == "RuntimeError"
    assert body["error"]["message"] == "whisper exploded"
    assert body["error"]["retryable"] is True


def test_fetch_before_done_returns_409(monkeypatch):
    def slow(_path):
        time.sleep(2)
        return [{"start": 0, "end": 1, "text": "hi"}]

    import transcribe
    monkeypatch.setattr(transcribe, "_mock", slow)
    r = client.post("/v1/transcriptions", files={"file": ("s.mp3", b"slow-" + os.urandom(8).hex().encode(), "audio/mpeg")})
    job_id = r.json()["job_id"]
    r = client.get(f"/v1/transcriptions/{job_id}/transcript")
    assert r.status_code == 409
    assert r.json()["status"] in ("queued", "processing")


def test_content_hash_dedup_without_key():
    payload = b"identical-" + uuid.uuid4().hex.encode()
    r1 = client.post("/v1/transcriptions", files={"file": ("a.mp3", payload, "audio/mpeg")})
    r2 = client.post("/v1/transcriptions", files={"file": ("b.mp3", payload, "audio/mpeg")})
    assert r1.status_code == 202 and r2.status_code == 202
    assert r1.json()["job_id"] == r2.json()["job_id"]


def test_auth_disabled_when_no_env(monkeypatch):
    monkeypatch.delenv("API_KEY", raising=False)
    r = client.post("/v1/transcriptions", files={"file": ("a.mp3", b"noauth", "audio/mpeg")})
    assert r.status_code == 202


def test_auth_requires_bearer_when_key_set(monkeypatch):
    monkeypatch.setenv("API_KEY", "s3cret")
    r = client.post("/v1/transcriptions", files={"file": ("a.mp3", b"x1", "audio/mpeg")})
    assert r.status_code == 401


def test_auth_rejects_wrong_key(monkeypatch):
    monkeypatch.setenv("API_KEY", "s3cret")
    r = client.post("/v1/transcriptions", files={"file": ("a.mp3", b"x2", "audio/mpeg")}, headers={"Authorization": "Bearer wrong"})
    assert r.status_code == 401


def test_auth_accepts_correct_key(monkeypatch):
    monkeypatch.setenv("API_KEY", "s3cret")
    r = client.post("/v1/transcriptions", files={"file": ("a.mp3", b"x3", "audio/mpeg")}, headers={"Authorization": "Bearer s3cret"})
    assert r.status_code == 202


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

    assert len(job_ids) == 1


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-v"]))

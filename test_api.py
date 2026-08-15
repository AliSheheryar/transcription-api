import time
from fastapi.testclient import TestClient
from app import app

client = TestClient(app)


def test_full_flow():
    files = {"file": ("sample.mp3", b"ID3fake-mp3-bytes", "audio/mpeg")}
    r = client.post("/v1/transcriptions", files=files)
    assert r.status_code == 202
    job_id = r.json()["job_id"]
    assert r.json()["status"] == "queued"

    for _ in range(30):
        r = client.get(f"/v1/transcriptions/{job_id}")
        assert r.status_code == 200
        if r.json()["status"] == "done":
            break
        time.sleep(0.5)
    else:
        raise AssertionError("job did not finish")

    r = client.get(f"/v1/transcriptions/{job_id}/transcript")
    assert r.status_code == 200
    assert "segments" in r.json()
    assert len(r.json()["segments"]) == 2


def test_rejects_non_mp3():
    files = {"file": ("x.wav", b"data", "audio/wav")}
    r = client.post("/v1/transcriptions", files=files)
    assert r.status_code == 400


def test_idempotency():
    files = {"file": ("a.mp3", b"same-bytes", "audio/mpeg")}
    r1 = client.post("/v1/transcriptions", files=files, headers={"Idempotency-Key": "k1"})
    files = {"file": ("a.mp3", b"same-bytes", "audio/mpeg")}
    r2 = client.post("/v1/transcriptions", files=files, headers={"Idempotency-Key": "k1"})
    assert r1.json()["job_id"] == r2.json()["job_id"]


def test_missing_job():
    assert client.get("/v1/transcriptions/nope").status_code == 404


if __name__ == "__main__":
    test_full_flow()
    test_rejects_non_mp3()
    test_idempotency()
    test_missing_job()
    print("all tests passed")

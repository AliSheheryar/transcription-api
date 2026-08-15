"""Kafka producer/consumer helpers (aiokafka).

Topic layout:
  transcriptions.submitted  (partitions: 32, key = user_id) — API produces here
"""
import json
import os
from typing import AsyncIterator, Optional

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer


TOPIC_SUBMITTED = "transcriptions.submitted"

_producer: Optional[AIOKafkaProducer] = None


def _bootstrap() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")


async def start_producer() -> None:
    global _producer
    if _producer is None:
        _producer = AIOKafkaProducer(
            bootstrap_servers=_bootstrap(),
            acks="all",
            enable_idempotence=True,
        )
        await _producer.start()


async def stop_producer() -> None:
    global _producer
    if _producer is not None:
        await _producer.stop()
        _producer = None


async def publish_job(job_id: str, user_id: int) -> None:
    if _producer is None:
        await start_producer()
    assert _producer is not None
    payload = json.dumps({"job_id": job_id, "user_id": user_id}).encode()
    await _producer.send_and_wait(
        TOPIC_SUBMITTED,
        value=payload,
        key=str(user_id).encode(),
    )


async def consume_jobs(group_id: str = "workers") -> AsyncIterator[dict]:
    consumer = AIOKafkaConsumer(
        TOPIC_SUBMITTED,
        bootstrap_servers=_bootstrap(),
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    try:
        async for msg in consumer:
            payload = json.loads(msg.value.decode())
            yield {"payload": payload, "msg": msg, "consumer": consumer}
    finally:
        await consumer.stop()

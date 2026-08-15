"""Postgres access layer. Uses asyncpg with a shared pool.

Public functions are the only surface the rest of the app uses; swapping to
a different store is a matter of reimplementing this module.
"""
import os
import uuid
from typing import Optional

import asyncpg

from auth import AuthenticatedUser, hash_key


_pool: Optional[asyncpg.Pool] = None


def _dsn() -> str:
    return os.environ.get(
        "DATABASE_URL",
        "postgresql://tx:tx@localhost:5432/transcription",
    )


async def init_pool() -> None:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(dsn=_dsn(), min_size=2, max_size=10)


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


async def pool() -> asyncpg.Pool:
    if _pool is None:
        await init_pool()
    assert _pool is not None
    return _pool


# ---------- Users & keys ----------

async def create_user(email: str, plan: str = "free") -> int:
    p = await pool()
    row = await p.fetchrow(
        "INSERT INTO users(email, plan) VALUES ($1, $2) ON CONFLICT(email) DO UPDATE SET email=EXCLUDED.email RETURNING id",
        email, plan,
    )
    return row["id"]


async def create_api_key_record(user_id: int, key_prefix: str, key_hash: str, name: Optional[str]) -> int:
    p = await pool()
    row = await p.fetchrow(
        "INSERT INTO api_keys(user_id, key_prefix, key_hash, name) VALUES ($1,$2,$3,$4) RETURNING id",
        user_id, key_prefix, key_hash, name,
    )
    return row["id"]


async def lookup_key(raw_key: str) -> Optional[AuthenticatedUser]:
    p = await pool()
    row = await p.fetchrow(
        """
        SELECT k.id AS api_key_id, u.id AS user_id, u.email, u.plan
        FROM api_keys k JOIN users u ON u.id = k.user_id
        WHERE k.key_hash = $1 AND k.revoked_at IS NULL
        """,
        hash_key(raw_key),
    )
    if row is None:
        return None
    return AuthenticatedUser(
        user_id=row["user_id"],
        email=row["email"],
        plan=row["plan"],
        api_key_id=row["api_key_id"],
    )


# ---------- Jobs ----------

async def insert_job(
    job_id: uuid.UUID,
    user_id: int,
    audio_path: str,
    idempotency_key: Optional[str],
) -> None:
    p = await pool()
    await p.execute(
        """
        INSERT INTO jobs(id, user_id, status, audio_path, idempotency_key)
        VALUES ($1, $2, 'queued', $3, $4)
        """,
        job_id, user_id, audio_path, idempotency_key,
    )


async def get_job_id_by_idempotency(user_id: int, key: str) -> Optional[uuid.UUID]:
    p = await pool()
    row = await p.fetchrow(
        "SELECT id FROM jobs WHERE user_id = $1 AND idempotency_key = $2",
        user_id, key,
    )
    return row["id"] if row else None


async def get_job(job_id: uuid.UUID, user_id: int) -> Optional[dict]:
    p = await pool()
    row = await p.fetchrow(
        "SELECT id, status, audio_path, transcript_path, error_code, error_message, error_retryable "
        "FROM jobs WHERE id = $1 AND user_id = $2",
        job_id, user_id,
    )
    return dict(row) if row else None


async def get_job_internal(job_id: uuid.UUID) -> Optional[dict]:
    """Worker-side fetch — no user_id scoping."""
    p = await pool()
    row = await p.fetchrow(
        "SELECT id, user_id, status, audio_path FROM jobs WHERE id = $1",
        job_id,
    )
    return dict(row) if row else None


async def set_job_processing(job_id: uuid.UUID) -> None:
    p = await pool()
    await p.execute(
        "UPDATE jobs SET status='processing', updated_at=NOW() WHERE id = $1",
        job_id,
    )


async def set_job_done(job_id: uuid.UUID, transcript_path: str) -> None:
    p = await pool()
    await p.execute(
        "UPDATE jobs SET status='done', transcript_path=$2, updated_at=NOW() WHERE id = $1",
        job_id, transcript_path,
    )


async def set_job_failed(job_id: uuid.UUID, code: str, message: str, retryable: bool) -> None:
    p = await pool()
    await p.execute(
        """
        UPDATE jobs SET status='failed',
            error_code=$2, error_message=$3, error_retryable=$4, updated_at=NOW()
        WHERE id = $1
        """,
        job_id, code, message, retryable,
    )

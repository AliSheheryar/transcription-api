"""Redis-backed sliding-window rate limiter (Lua-scripted, atomic).

One round-trip per check. Rejects when the request count in the last WINDOW
seconds exceeds LIMIT for the given key. Returns (allowed, remaining).

Per-user, per-dimension keys. Plans map to different limits.
"""
import os
import time
from dataclasses import dataclass
from typing import Optional

import redis.asyncio as redis


_client: Optional[redis.Redis] = None

# --- Sliding-window log Lua ---
# KEYS[1] = bucket key
# ARGV[1] = now_ms
# ARGV[2] = window_ms
# ARGV[3] = limit
# ARGV[4] = unique member (avoid collisions inside the same ms)
_LUA = """
redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, tonumber(ARGV[1]) - tonumber(ARGV[2]))
local count = redis.call('ZCARD', KEYS[1])
if count >= tonumber(ARGV[3]) then
  return {0, count}
end
redis.call('ZADD', KEYS[1], ARGV[1], ARGV[4])
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return {1, count + 1}
"""

_script_sha: Optional[str] = None


def _url() -> str:
    return os.environ.get("REDIS_URL", "redis://localhost:6379/0")


async def init() -> None:
    global _client, _script_sha
    if _client is None:
        _client = redis.from_url(_url(), decode_responses=True)
        _script_sha = await _client.script_load(_LUA)


async def close() -> None:
    global _client
    if _client is not None:
        await _client.close()
        _client = None


@dataclass
class Limit:
    key: str        # bucket suffix
    limit: int
    window_ms: int


# Per-plan defaults. Add more tiers here.
PLAN_LIMITS: dict[str, dict[str, Limit]] = {
    "free": {
        "submit": Limit("submit", 60, 60_000),
        "poll":   Limit("poll",   300, 60_000),
        "fetch":  Limit("fetch",  60, 60_000),
    },
    "pro": {
        "submit": Limit("submit", 600, 60_000),
        "poll":   Limit("poll",   3000, 60_000),
        "fetch":  Limit("fetch",  600, 60_000),
    },
}


@dataclass
class Decision:
    allowed: bool
    remaining: int
    limit: int
    window_ms: int


async def check(user_id: int, plan: str, dimension: str) -> Decision:
    """Check + increment. `dimension` is one of 'submit'|'poll'|'fetch'."""
    if _client is None:
        await init()
    assert _client is not None and _script_sha is not None

    limits = PLAN_LIMITS.get(plan, PLAN_LIMITS["free"])
    lim = limits[dimension]
    bucket = f"rl:{user_id}:{lim.key}"
    now_ms = int(time.time() * 1000)
    unique = f"{now_ms}:{os.urandom(4).hex()}"

    result = await _client.evalsha(
        _script_sha, 1, bucket, str(now_ms), str(lim.window_ms), str(lim.limit), unique
    )
    allowed = bool(result[0])
    count = int(result[1])
    remaining = max(0, lim.limit - count)
    return Decision(allowed=allowed, remaining=remaining, limit=lim.limit, window_ms=lim.window_ms)


# ---------- Idempotency ----------

async def idempotency_lookup_or_set(user_id: int, key: str, job_id: str, ttl_seconds: int = 86_400) -> str:
    """Atomic 'get-or-set'. Returns the job_id that was stored (either fresh or existing)."""
    if _client is None:
        await init()
    assert _client is not None
    ikey = f"idem:{user_id}:{key}"
    # SET NX — win if this is the first request, else return the pre-existing one
    won = await _client.set(ikey, job_id, nx=True, ex=ttl_seconds)
    if won:
        return job_id
    existing = await _client.get(ikey)
    return existing or job_id


async def idempotency_force_set(user_id: int, key: str, job_id: str, ttl_seconds: int = 86_400) -> None:
    """Overwrite the idempotency mapping. Used to repair a stale Redis entry
    after we learn the true DB owner of the idem_key from a race."""
    if _client is None:
        await init()
    assert _client is not None
    await _client.set(f"idem:{user_id}:{key}", job_id, ex=ttl_seconds)

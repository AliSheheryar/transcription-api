"""Create a user + API key. Prints the plaintext key ONCE — save it.

Usage:
    python seed.py you@example.com               # free plan
    python seed.py you@example.com --plan pro
"""
import argparse
import asyncio

import auth
import db


async def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("email")
    p.add_argument("--plan", default="free", choices=["free", "pro"])
    p.add_argument("--name", default=None, help="Optional key label")
    args = p.parse_args()

    await db.init_pool()
    user_id = await db.create_user(args.email, args.plan)
    raw, prefix, key_hash = auth.generate_api_key()
    await db.create_api_key_record(user_id, prefix, key_hash, args.name)
    await db.close_pool()

    print(f"user_id: {user_id}")
    print(f"email:   {args.email}")
    print(f"plan:    {args.plan}")
    print()
    print(f"API KEY (save now, will not be shown again):")
    print(f"  {raw}")
    print()
    print(f"Use it as:  curl -H 'Authorization: Bearer {raw}' http://localhost:8000/v1/transcriptions ...")


if __name__ == "__main__":
    asyncio.run(main())

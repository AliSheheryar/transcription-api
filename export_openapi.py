"""Dump the OpenAPI spec to openapi.json so it can be committed and shipped to clients."""
import json
from pathlib import Path

from app import app

out = Path(__file__).parent / "openapi.json"
out.write_text(json.dumps(app.openapi(), indent=2))
print(f"Wrote {out} ({out.stat().st_size:,} bytes)")

"""Source clients. Each one validates the response shape before anything is written.

A response that doesn't match the expected schema raises SchemaDriftError and is saved to the
quarantine folder for inspection, instead of silently loading wrong data.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from energy_platform.config import quarantine_dir


class SchemaDriftError(RuntimeError):
    """The source changed its response format or units. Nothing was loaded."""


def quarantine(source: str, payload: object, reason: str) -> str:
    folder = quarantine_dir()
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{source}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')}.json"
    path.write_text(json.dumps({"reason": reason, "payload": payload}, ensure_ascii=False, default=str), encoding="utf-8")
    return str(path)

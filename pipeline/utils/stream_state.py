"""Processed-file state for the streaming poller.

A small JSON document at ``streaming.state_path`` records which stream files
have already been processed, so the polling loop can resume idempotently
across container restarts. Writes are atomic (write-temp + os.replace).
"""

import json
import os
from pathlib import Path
from typing import Iterable, Set
from uuid import uuid4


def load_processed(path: str) -> Set[str]:
    p = Path(path)
    if not p.exists():
        return set()
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        return set(data.get("processed_files", []))
    except (OSError, ValueError):
        return set()


def save_processed(path: str, processed: Iterable[str]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_name(f"{p.name}.tmp.{uuid4().hex}")
    payload = {"processed_files": sorted(processed)}
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    os.replace(str(tmp), str(p))

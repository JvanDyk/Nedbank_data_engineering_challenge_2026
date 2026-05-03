import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional
from uuid import uuid4


STATE_VERSION = 1


def _write_json_atomic(data: Dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.tmp.{uuid4().hex}")
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(str(temp_path), str(path))


def load_pipeline_state(path: str) -> Dict[str, Any]:
    state_path = Path(path)
    if not state_path.exists():
        return {"version": STATE_VERSION, "completed_stages": {}}
    with open(state_path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_pipeline_state(path: str, state: Dict[str, Any]) -> None:
    state_path = Path(path)
    state["version"] = STATE_VERSION
    _write_json_atomic(state, state_path)


def mark_stage_complete(path: str, stage_name: str, metadata: Optional[Dict[str, Any]] = None) -> None:
    state = load_pipeline_state(path)
    state.setdefault("completed_stages", {})[stage_name] = {
        "completed_at": datetime.now(timezone.utc).isoformat(),
        **(metadata or {}),
    }
    save_pipeline_state(path, state)


def get_stage_state(state: Dict[str, Any], stage_name: str) -> Optional[Dict[str, Any]]:
    return state.get("completed_stages", {}).get(stage_name)


def is_stage_complete(state: Dict[str, Any], stage_name: str) -> bool:
    return get_stage_state(state, stage_name) is not None

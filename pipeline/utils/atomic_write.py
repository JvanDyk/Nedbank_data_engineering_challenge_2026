import os
import shutil
from pathlib import Path
from uuid import uuid4

from pyspark.sql import DataFrame


def _remove_path(path: Path) -> None:
    if not path.exists():
        return
    if path.is_dir():
        shutil.rmtree(path)
    else:
        path.unlink()


def write_delta_atomic(df: DataFrame, target_path: str) -> None:
    """Atomically overwrite a Delta table directory.

    Writes to a temporary sibling directory first, then replaces the target
    path with an atomic rename. This avoids leaving partial output in place if
    the write fails.
    """
    target = Path(target_path)
    parent = target.parent
    parent.mkdir(parents=True, exist_ok=True)

    temp_target = parent / f"{target.name}.tmp.{uuid4().hex}"
    if temp_target.exists():
        _remove_path(temp_target)

    df.write.format("delta").mode("overwrite").save(str(temp_target))

    if target.exists():
        _remove_path(target)

    os.replace(str(temp_target), str(target))


def is_delta_table(path: str) -> bool:
    delta_log = Path(path) / "_delta_log"
    return delta_log.is_dir() and any(delta_log.iterdir())

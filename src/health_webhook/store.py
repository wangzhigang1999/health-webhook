"""JSONL + Paimon 双写编排。"""

import os
import threading

from health_webhook import healthkit, paimon_store
from health_webhook.config import settings
from health_webhook.models import HealthPayload

_lock = threading.Lock()


def _append_jsonl(raw: str) -> None:
    settings.jsonl_path.parent.mkdir(parents=True, exist_ok=True)
    with open(settings.jsonl_path, "a", encoding="utf-8") as f:
        f.write(raw)
        if not raw.endswith("\n"):
            f.write("\n")
        f.flush()
        os.fsync(f.fileno())


def save(raw_json: str, payload: HealthPayload) -> dict[str, int | bool]:
    """双写一条 webhook 数据：先 JSONL 兜底，再 Paimon 湖表。"""
    rows = healthkit.flatten(payload)
    with _lock:
        _append_jsonl(raw_json)
        written = paimon_store.write(rows)
    return {"jsonl_saved": True, "total_rows": len(rows), "paimon_rows": written}


def warmup() -> None:
    paimon_store.warmup()

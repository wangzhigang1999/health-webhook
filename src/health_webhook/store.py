"""JSONL 本地落盘（Paimon 由定时同步任务批量导入）。"""

import os
import threading

from health_webhook import healthkit
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
    """写一条 webhook 数据到本地 JSONL。

    Paimon 不在此实时写入，由 ``health-webhook-backfill`` 定时批量同步，
    避免每条 POST 都触发 OSS 提交（随 snapshot 增多越来越慢）。
    """
    rows = healthkit.flatten(payload)
    with _lock:
        _append_jsonl(raw_json)
    return {"jsonl_saved": True, "total_rows": len(rows)}


def warmup() -> None:
    """本地落盘无需 OSS warmup；仅确保数据目录存在。"""
    settings.jsonl_path.parent.mkdir(parents=True, exist_ok=True)

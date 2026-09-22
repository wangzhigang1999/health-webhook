"""把 JSONL 兜底数据回灌进 Paimon（幂等，按 sample_uuid 去重）。"""

import json

from health_webhook import healthkit, paimon_store
from health_webhook.config import settings
from health_webhook.models import HealthPayload


def _extract_payload(line: str) -> HealthPayload:
    """兼容新旧两种 JSONL 格式：旧格式包在 ``body`` 字段里，新格式为裸 payload。"""
    data = json.loads(line)
    if isinstance(data, dict) and "body" in data:
        data = data["body"]
    return HealthPayload.model_validate(data)


def backfill() -> int:
    """回灌所有 JSONL 中尚未入库的样本，返回新写入行数。"""
    path = settings.jsonl_path
    if not path.exists():
        print(f"JSONL 不存在: {path}")
        return 0

    existing = paimon_store.read_sample_uuids()
    print(f"Paimon 中已有 {len(existing)} 个样本")

    new_rows = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                payload = _extract_payload(line)
            except Exception as exc:  # 单行脏数据不应中断整体回灌
                print(f"跳过无法解析的行: {exc}")
                continue
            for row in healthkit.flatten(payload):
                if row.sample_uuid and row.sample_uuid not in existing:
                    new_rows.append(row)
                    existing.add(row.sample_uuid)

    if not new_rows:
        print("没有需要导入的新样本")
        return 0

    written = paimon_store.write(new_rows)
    print(f"新写入 {written} 行")
    return written


def main() -> None:
    backfill()


if __name__ == "__main__":
    main()

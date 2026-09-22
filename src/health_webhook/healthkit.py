"""HealthKit 批量数据打平成度量行。"""

from datetime import UTC, datetime, timedelta

from health_webhook.models import HealthPayload, MetricRow

_UTC8 = timedelta(hours=8)


def now_ms() -> int:
    return int(datetime.now(UTC).timestamp() * 1000)


def bj_date(ms: int) -> str:
    """北京时区日期，用作分区键 dt。"""
    t = datetime.fromtimestamp(ms / 1000, tz=UTC) + _UTC8
    return t.strftime("%Y-%m-%d")


def flatten(payload: HealthPayload) -> list[MetricRow]:
    """把一条 webhook body 打平成 metrics 行。

    只处理 quantity / category 类型样本；workout route 等复杂类型
    不进入窄表，由 JSONL 兜底保留原始数据。
    """
    rows: list[MetricRow] = []
    received = now_ms()
    for batch in payload.batches:
        for sample in batch.samples:
            if sample.quantity is None and sample.category is None:
                continue
            value: float | None = None
            unit: str | None = None
            category: str | None = None
            if sample.quantity is not None:
                value = sample.quantity.value
                unit = sample.quantity.unit
            elif sample.category is not None:
                value = float(sample.category.value) if sample.category.value is not None else None
                category = sample.category.value_name
            start = sample.start_unix_ms
            rows.append(
                MetricRow(
                    sample_uuid=sample.uuid,
                    device_id=payload.device_id,
                    metric_type=batch.hk_type_id,
                    value=value,
                    unit=unit,
                    category=category,
                    source_name=sample.source.name if sample.source else None,
                    start_time=start,
                    end_time=sample.end_unix_ms,
                    received_at=received,
                    dt=bj_date(start or received),
                )
            )
    return rows

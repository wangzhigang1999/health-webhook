"""健康看板数据聚合：从 Paimon 读取并整理成前端 JSON。"""

import statistics
from collections import defaultdict
from datetime import UTC, datetime, timedelta

from health_webhook import paimon_store

_UTC8 = timedelta(hours=8)
_TEST_DEVICES = {"test-device", "smoke-dev"}

_METRIC_LABELS = {
    "HKQuantityTypeIdentifierHeartRate": "心率",
    "HKQuantityTypeIdentifierHeartRateVariabilitySDNN": "HRV",
    "HKQuantityTypeIdentifierActiveEnergyBurned": "活动能量",
    "HKQuantityTypeIdentifierBasalEnergyBurned": "基础能量",
    "HKQuantityTypeIdentifierStepCount": "步数",
    "HKQuantityTypeIdentifierDistanceWalkingRunning": "步行距离",
    "HKQuantityTypeIdentifierAppleStandTime": "站立时间",
    "HKQuantityTypeIdentifierAppleStandHour": "站立小时",
    "HKQuantityTypeIdentifierPhysicalEffort": "体能付出",
    "HKQuantityTypeIdentifierOxygenSaturation": "血氧",
    "HKCategoryTypeIdentifierSleepAnalysis": "睡眠",
}

# 卡片展示的指标及其短键（点值指标）
_CARD_KEYS = {
    "HKQuantityTypeIdentifierHeartRate": "heart_rate",
    "HKQuantityTypeIdentifierHeartRateVariabilitySDNN": "hrv",
    "HKQuantityTypeIdentifierOxygenSaturation": "oxygen",
    "HKQuantityTypeIdentifierStepCount": "steps",
    "HKQuantityTypeIdentifierActiveEnergyBurned": "active_energy",
    "HKQuantityTypeIdentifierBasalEnergyBurned": "basal_energy",
    "HKQuantityTypeIdentifierDistanceWalkingRunning": "distance",
}


def _read_rows() -> list[dict]:
    table = paimon_store._get_table()
    read_builder = table.new_read_builder()
    table_scan = read_builder.new_scan()
    table_read = read_builder.new_read()
    return table_read.to_arrow(table_scan.plan().splits()).to_pylist()


def _ms(value) -> int | None:
    """datetime 或 epoch ms -> epoch ms。"""
    if value is None:
        return None
    if isinstance(value, datetime):
        return int(value.replace(tzinfo=UTC).timestamp() * 1000)
    return int(value)


def dashboard_data() -> dict:
    rows = _read_rows()
    real = [r for r in rows if r.get("device_id") not in _TEST_DEVICES]

    groups: dict[str, list[dict]] = defaultdict(list)
    for r in real:
        groups[r["metric_type"]].append(r)

    cards: dict[str, dict] = {}
    for metric, short in _CARD_KEYS.items():
        items = groups.get(metric)
        if not items:
            continue
        ordered = sorted(items, key=lambda r: _ms(r["start_time"]) or 0)
        latest = ordered[-1]
        values = [float(i["value"]) for i in items if i["value"] is not None]
        card: dict = {
            "label": _METRIC_LABELS.get(metric, metric),
            "value": latest["value"],
            "unit": latest["unit"] or "",
            "time_ms": _ms(latest["start_time"]),
            "count": len(values),
        }
        if values:
            card["min"] = min(values)
            card["max"] = max(values)
            card["avg"] = round(statistics.mean(values), 1)
        cards[short] = card

    heart_rate_series = [
        [_ms(r["start_time"]), r["value"]]
        for r in sorted(
            groups.get("HKQuantityTypeIdentifierHeartRate", []),
            key=lambda r: _ms(r["start_time"]) or 0,
        )
        if r["start_time"] is not None and r["value"] is not None
    ]

    records = [
        {
            "time_ms": _ms(r["start_time"]),
            "metric": _METRIC_LABELS.get(r["metric_type"], r["metric_type"]),
            "value": r["value"],
            "unit": r["unit"] or "",
            "source": r["source_name"],
        }
        for r in sorted(real, key=lambda r: _ms(r["start_time"]) or 0, reverse=True)
    ][:50]

    return {
        "generated_at_ms": int(datetime.now(UTC).timestamp() * 1000),
        "count": len(real),
        "cards": cards,
        "heart_rate_series": heart_rate_series,
        "records": records,
    }

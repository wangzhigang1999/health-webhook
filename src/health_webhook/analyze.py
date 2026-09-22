"""从 Paimon 读取并分析健康指标。"""

import statistics
from collections import defaultdict
from datetime import UTC, datetime, timedelta

from health_webhook import paimon_store

_UTC8 = timedelta(hours=8)
_TEST_DEVICES = {"test-device", "smoke-dev"}

_LABELS = {
    "HKQuantityTypeIdentifierHeartRate": "❤️  心率",
    "HKQuantityTypeIdentifierHeartRateVariabilitySDNN": "💓 心率变异性 (HRV)",
    "HKQuantityTypeIdentifierActiveEnergyBurned": "🔥 活动能量",
    "HKQuantityTypeIdentifierBasalEnergyBurned": "🛋️ 基础能量",
    "HKQuantityTypeIdentifierStepCount": "🚶 步数",
    "HKQuantityTypeIdentifierDistanceWalkingRunning": "📏 步行/跑步距离",
    "HKQuantityTypeIdentifierAppleStandTime": "🧍 站立时间",
    "HKQuantityTypeIdentifierAppleStandHour": "⏰ 站立小时",
    "HKQuantityTypeIdentifierPhysicalEffort": "💪 体能付出",
    "HKQuantityTypeIdentifierOxygenSaturation": "🫁 血氧",
    "HKCategoryTypeIdentifierSleepAnalysis": "😴 睡眠",
}


def _bj(value) -> str:
    """时间值（datetime 或 epoch ms）-> 北京时间字符串。"""
    if value is None:
        return "-"
    if isinstance(value, datetime):
        dt = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    else:
        dt = datetime.fromtimestamp(value / 1000, tz=UTC)
    return (dt + _UTC8).strftime("%m-%d %H:%M")


def _read_rows() -> list[dict]:
    table = paimon_store._get_table()
    read_builder = table.new_read_builder()
    table_scan = read_builder.new_scan()
    table_read = read_builder.new_read()
    splits = table_scan.plan().splits()
    return table_read.to_arrow(splits).to_pylist()


def _num(r: dict) -> float | None:
    return float(r["value"]) if r["value"] is not None else None


def analyze() -> None:
    rows = _read_rows()
    real = [r for r in rows if r.get("device_id") not in _TEST_DEVICES]
    if not real:
        print("暂无真实数据")
        return

    starts = [r["start_time"] for r in real if r["start_time"] is not None]
    ends = [r["end_time"] for r in real if r["end_time"] is not None]
    print("=" * 60)
    print(f"健康数据概览（{len(real)} 条真实样本）")
    print(f"时间范围：{_bj(min(starts))} ~ {_bj(max(ends))}（北京时间）")
    print("=" * 60)

    groups: dict[str, list[dict]] = defaultdict(list)
    for r in real:
        groups[r["metric_type"]].append(r)

    for metric, items in sorted(groups.items()):
        label = _LABELS.get(metric, metric)
        values = [_num(i) for i in items]
        values = [v for v in values if v is not None]
        unit = items[0]["unit"] or ""

        if items[0]["category"] is not None:
            cats = [i["category"] for i in items]
            print(f"\n{label}：{cats}（{len(items)} 条）")
        elif metric in {
            "HKQuantityTypeIdentifierActiveEnergyBurned",
            "HKQuantityTypeIdentifierBasalEnergyBurned",
            "HKQuantityTypeIdentifierDistanceWalkingRunning",
            "HKQuantityTypeIdentifierAppleStandTime",
            "HKQuantityTypeIdentifierStepCount",
        }:
            print(f"\n{label}：合计 {sum(values):.1f} {unit}（{len(items)} 条样本）")
        else:
            print(
                f"\n{label}：n={len(values)}  最小={min(values):.1f}  最大={max(values):.1f}"
                f"  平均={statistics.mean(values):.1f}  中位={statistics.median(values):.1f} {unit}"
            )

    # 心率时间序列（用户最关心）
    hr = sorted(
        groups.get("HKQuantityTypeIdentifierHeartRate", []),
        key=lambda r: r["start_time"] or datetime.min.replace(tzinfo=UTC),
    )
    if hr:
        print("\n" + "-" * 60)
        print("心率时间序列（北京时间）")
        for r in hr:
            print(f"  {_bj(r['start_time'])}   {r['value']:.0f} 次/分")

    if not groups.get("HKCategoryTypeIdentifierSleepAnalysis"):
        print("\n⚠️  暂无真实睡眠数据（发送端尚未同步 HKCategoryTypeIdentifierSleepAnalysis）")


def main() -> None:
    analyze()


if __name__ == "__main__":
    main()

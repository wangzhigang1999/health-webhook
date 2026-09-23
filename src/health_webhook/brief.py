"""Private health brief: consistent sources, complete sleep episodes, explicit gaps."""

import html
import json
import math
import statistics
from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any

TZ = timezone(timedelta(hours=8))
ASLEEP = {"asleep", "asleepUnspecified", "asleepCore", "asleepDeep", "asleepREM"}
ADDITIVE = {"StepCount", "AppleExerciseTime", "ActiveEnergyBurned"}


def union_ms(intervals):
    total, right = 0, None
    for a, b in sorted(intervals):
        total += max(0, b - max(a, a if right is None else right))
        right = max(b, b if right is None else right)
    return total


def local_day(ms):
    return datetime.fromtimestamp(ms / 1000, TZ).date()


def median(values):
    values = [v for v in values if v is not None]
    return statistics.median(values) if values else None


def collect(db, day: date, table="samples_current", history_days=35) -> dict[str, Any]:
    if table not in {"samples_current", "samples"}:
        raise ValueError("unsupported input relation")
    first = day - timedelta(days=history_days)
    sources = db.execute(
        f"SELECT source_bundle FROM {table} WHERE source_product LIKE 'Watch%' "
        "AND dt BETWEEN ? AND ? GROUP BY source_bundle ORDER BY count(*) DESC LIMIT 1",
        [day - timedelta(days=28), day],
    ).fetchall()
    source = sources[0][0] if sources else None
    rows = (
        db.execute(
            f"SELECT metric_type,start_ms,end_ms,value,category,unit FROM {table} "
            "WHERE source_bundle=? AND dt BETWEEN ? AND ? ORDER BY start_ms,end_ms",
            [source, first - timedelta(days=2), day + timedelta(days=1)],
        ).fetchall()
        if source
        else []
    )
    daily = defaultdict(lambda: defaultdict(list))
    exercise = defaultdict(list)
    stood = defaultdict(set)
    coverage = defaultdict(set)
    sleeps = []
    for metric, a, b, value, category, _unit in rows:
        if a is None or b is None or b < a:
            continue
        key = metric.removeprefix("HKQuantityTypeIdentifier")
        d = local_day(a)
        if metric.endswith("SleepAnalysis"):
            sleeps.append((a, b, category))
        if metric.endswith("AppleStandHour") and category == "stood":
            stood[d].add(datetime.fromtimestamp(a / 1000, TZ).hour)
        if key == "HeartRate":
            coverage[d].add(datetime.fromtimestamp(a / 1000, TZ).hour)
        if value is None or not math.isfinite(value):
            continue
        if key in ADDITIVE and b > a:
            cursor = a
            while cursor < b:
                part_day = local_day(cursor)
                stop = min(
                    b,
                    int(
                        datetime.combine(part_day + timedelta(days=1), time(), TZ).timestamp()
                        * 1000
                    ),
                )
                if key == "AppleExerciseTime":
                    if abs(value - (b - a) / 60000) > 0.01:
                        raise ValueError("exercise interval does not match quantity")
                    exercise[part_day].append((cursor, stop))
                else:
                    daily[part_day][key].append(value * (stop - cursor) / (b - a))
                cursor = stop
        elif key not in ADDITIVE:
            daily[d][key].append(value)
    for d, intervals in exercise.items():
        daily[d]["AppleExerciseTime"] = [union_ms(intervals) / 60000]
    episodes: list[list[tuple]] = []
    episode_end = 0
    for row in sleeps:
        if not episodes or row[0] - episode_end > 90 * 60000:
            episodes.append([])
            episode_end = row[1]
        episodes[-1].append(row)
        episode_end = max(episode_end, row[1])
    nights = {}
    for episode in episodes:
        asleep = [(a, b) for a, b, c in episode if c in ASLEEP]
        if not asleep:
            continue
        a, b = min(v[0] for v in asleep), max(v[1] for v in asleep)
        d = local_day(b)
        if not first <= d <= day:
            continue
        minutes = union_ms(asleep) / 60000
        if d in nights and nights[d]["minutes"] >= minutes:
            continue
        stages = {
            c: union_ms(
                [(max(x, a), min(y, b)) for x, y, z in episode if z == c and x < b and y > a]
            )
            / 60000
            for c in ASLEEP | {"awake"}
        }
        vitals = {}
        for key in ("HeartRate", "RespiratoryRate", "OxygenSaturation"):
            values = [
                v
                for m, x, y, v, c, u in rows
                if m == "HKQuantityTypeIdentifier" + key
                and a <= x < b
                and v is not None
                and math.isfinite(v)
            ]
            if key == "OxygenSaturation":
                # HealthKit percent quantities are ratios in this upload contract.
                values = [v * 100 for v in values if 0 <= v <= 1]
            vitals[key] = {"value": median(values), "samples": len(values)}
        nights[d] = {
            "minutes": minutes,
            "start": datetime.fromtimestamp(a / 1000, TZ).isoformat(),
            "end": datetime.fromtimestamp(b / 1000, TZ).isoformat(),
            "stages": stages,
            "vitals": vitals,
        }
    history = [day - timedelta(days=i) for i in range(7, 0, -1)]
    keys = ADDITIVE | {"RestingHeartRate", "HeartRateVariabilitySDNN", "WalkingHeartRateAverage"}
    daily_values = {}
    for offset in range(history_days + 1):
        d = day - timedelta(days=offset)
        values = {}
        for key in keys:
            vs = daily[d][key]
            values[key] = (sum(vs) if key in ADDITIVE else median(vs)) if vs else None
        values["stood_hours"] = len(stood[d]) if stood[d] else None
        values["heart_rate_hours"] = len(coverage[d])
        daily_values[str(d)] = values
    metrics = {}
    for key in keys:
        past = [daily_values[str(d)][key] for d in history if daily_values[str(d)][key] is not None]
        metrics[key] = {
            "value": daily_values[str(day)][key],
            "baseline": median(past),
            "days": len(past),
        }
    latest = (
        db.execute(
            f"SELECT value,dt FROM {table} WHERE metric_type='HKQuantityTypeIdentifierVO2Max' "
            "AND source_bundle=? AND dt<=? AND value IS NOT NULL ORDER BY start_ms DESC LIMIT 1",
            [source, day],
        ).fetchall()
        if source
        else []
    )
    return {
        "schema_version": 2,
        "date": str(day),
        "timezone": "Asia/Shanghai",
        "source_available": source is not None,
        "metrics": metrics,
        "sleep": nights.get(day),
        "nights": {str(k): v for k, v in sorted(nights.items())},
        "sleep_baseline": median([nights[d]["minutes"] for d in history if d in nights]),
        "daily": daily_values,
        "vo2max": {"value": latest[0][0], "date": str(latest[0][1])} if latest else None,
    }


def fmt(value, digits=0):
    return "暂无记录" if value is None else f"{value:,.{digits}f}"


def duration(value):
    if value is None:
        return "暂无记录"
    minutes = round(value)
    return f"{minutes // 60} 小时 {minutes % 60:02d} 分钟"


def change(value, baseline, unit):
    if value is None or baseline is None:
        return "暂不比较"
    diff = value - baseline
    if abs(diff) < 0.05:
        return "接近常态"
    return f"{'多' if diff > 0 else '少'}约 {abs(diff):.1f} {unit}"


def render(report: dict[str, Any]) -> str:
    day = date.fromisoformat(report["date"])
    sleep = report["sleep"]
    baseline = report["sleep_baseline"]
    dates = [str(day - timedelta(days=i)) for i in range(7, -1, -1)]
    nights = [report["nights"].get(d) for d in dates]
    esc = html.escape
    title = "先关注睡眠时长与作息" if sleep and sleep["minutes"] < 420 else "把今天放进近期趋势里看"
    if not sleep:
        title = "睡眠记录尚不完整，先看已知信息"
    body = [
        f'<header><div><div class="eyebrow">每日 · 健康观察</div><h1><i class="fa-solid" aria-hidden="true">&#xf21e;</i>健康简报</h1><p class="date">{day} · 北京时间</p></div><div class="actions"><span class="badge">私人简报</span><button onclick="window.print()">打印 / 保存 PDF</button></div></header>',  # noqa: E501
        f'<section class="lead"><div class="label">今天重点</div><h2>{title}</h2><p>与自己的前 7 日记录比较，留意持续变化，并结合精神状态一起看。缺失记录不计为零，单日指标不用于诊断。</p></section>',  # noqa: E501
    ]
    stages = sleep["stages"] if sleep else {}
    stage_keys = ["asleepCore", "asleepREM", "asleepDeep", "asleepUnspecified", "asleep"]
    stage_values = [stages.get(k, 0) for k in stage_keys]
    stage_labels = [
        f"{label} · {duration(value)}"
        for label, value in zip(
            ["核心", "REM", "深睡", "未分期", "睡眠（未细分）"], stage_values, strict=True
        )
    ]
    visible_stages = [
        (v, label) for v, label in zip(stage_values, stage_labels, strict=True) if v > 0
    ]
    stage_values = [v for v, label in visible_stages]
    stage_labels = [label for v, label in visible_stages]
    start = (
        (datetime.fromisoformat(sleep["start"]) + timedelta(seconds=30)).strftime("%H:%M")
        if sleep
        else "—"
    )
    end = (
        (datetime.fromisoformat(sleep["end"]) + timedelta(seconds=30)).strftime("%H:%M")
        if sleep
        else "—"
    )
    body.append(
        f'<section class="sleep-panel"><h3><i class="fa-solid" aria-hidden="true">&#xf236;</i>睡眠 · 昨晚怎么睡的</h3><div class="sleep-grid"><div><span>入睡 · 手表识别</span><strong>{start}</strong></div><div><span>醒来 · 睡眠结束</span><strong>{end}</strong></div><div><span>实际睡眠</span><strong style="font-size:18px">{duration(sleep["minutes"] if sleep else None)}</strong></div><div><span>期间清醒记录</span><strong>{fmt(stages.get("awake"), 1)}<small> 分</small></strong></div></div><p class="small">未记录实际离床时间；清醒片段不等于全部清醒时间。睡眠归入醒来日期。</p>'  # noqa: E501
    )
    body.append(
        '<div class="charts-row"><div class="chart-card shadow-sm"><h4>睡眠分期</h4><div class="chart-frame"><canvas id="stages" role="img" aria-label="睡眠分期图"></canvas></div></div><div class="chart-card shadow-sm"><h4>近 8 晚睡眠时长</h4><div class="chart-frame"><canvas id="duration" role="img" aria-label="近八晚睡眠时长"></canvas></div></div></div>'  # noqa: E501
    )
    body.append(
        f'<p class="small">前 7 晚中位数：{duration(baseline)}。分期用于观察，不按单晚深睡比例打分；无记录日期保留空缺。</p><div class="chart-card mt-5"><h4>入睡 → 醒来 · 作息变化</h4><div class="schedule-frame"><canvas id="schedule" role="img" aria-label="入睡与醒来时间区间"></canvas></div><p class="small">条形包含期间清醒片段，不等于纯睡眠时长。</p></div></section>'  # noqa: E501
    )
    body.append(
        '<section><div class="section-head"><h3>当天与近期常态</h3><span class="small">前 7 日每日值中位数</span></div><table><thead><tr><th>指标</th><th>当天</th><th>前 7 日</th><th>变化</th></tr></thead><tbody>'  # noqa: E501
    )
    for key, label, unit, digits in (
        ("StepCount", "手表步数", "步", 0),
        ("AppleExerciseTime", "运动时段", "分钟", 0),
        ("ActiveEnergyBurned", "活动热量", "千卡", 0),
        ("RestingHeartRate", "静息心率", "次/分", 0),
        ("HeartRateVariabilitySDNN", "HRV（SDNN）", "毫秒", 0),
    ):
        m = report["metrics"][key]
        body.append(
            f"<tr><td>{label}</td><td>{fmt(m['value'], digits)} {unit}</td><td>{fmt(m['baseline'], digits)} {unit}<br><small>有效 {m['days']}/7 日</small></td><td>{change(m['value'], m['baseline'], unit)}</td></tr>"  # noqa: E501
        )
    body.append(
        f'</tbody></table><p class="note">站立达标小时：{fmt(report["daily"][str(day)]["stood_hours"])}。表示这些小时内有站立活动，并非连续站立这么久。运动时段不等于一次连续训练；活动热量不是热量缺口。</p></section>'  # noqa: E501
    )
    sparse = [d[5:] for d in dates if report["daily"][d]["heart_rate_hours"] < 16]
    if sparse:
        body.append(
            f'<p class="note">{esc("、".join(sparse))} 心率记录覆盖较少时段，可能存在佩戴或采集空缺；低步数不一定代表活动少。</p>'  # noqa: E501
        )
    body.append(
        '<section class="vitals-panel"><h3>夜间身体状态</h3><table><thead><tr><th>指标</th><th>当晚</th><th>前 7 晚</th><th>变化</th></tr></thead><tbody>'  # noqa: E501
    )
    for key, label, unit, digits in (
        ("HeartRate", "夜间心率", "次/分", 0),
        ("RespiratoryRate", "呼吸频率", "次/分", 1),
        ("OxygenSaturation", "血氧饱和度", "%", 1),
    ):
        value = sleep["vitals"][key]["value"] if sleep else None
        past = [
            n["vitals"][key]["value"]
            for n in nights[:-1]
            if n and n["vitals"][key]["value"] is not None
        ]
        b = median(past)
        body.append(
            f"<tr><td>{label}</td><td>{fmt(value, digits)} {unit}</td><td>{fmt(b, digits)} {unit}<br><small>有效 {len(past)}/7 晚</small></td><td>{change(value, b, '个百分点' if key == 'OxygenSaturation' else unit)}</td></tr>"  # noqa: E501
        )
    oxygen_n = sleep["vitals"]["OxygenSaturation"]["samples"] if sleep else 0
    body.append(
        f'</tbody></table><p class="note">夜间指标为主睡眠起止时间内采样中位数。血氧当晚 {oxygen_n} 次采样，不能代表整夜连续血氧或排查睡眠呼吸问题。夜间心率与全天静息心率分别观察。</p></section>'  # noqa: E501
    )
    advice = (
        "下一晚可以多预留约半小时睡眠机会。"
        if sleep and sleep["minutes"] < 420
        else "继续保持规律的睡觉和起床时段。"
    )
    body.append(
        f'<section class="next"><div class="label">下一步</div><p><strong>{advice}</strong>明早记录“精神尚可 / 有些困 / 明显疲惫”，结合接下来几天的变化观察。成人一般建议至少睡 7 小时，具体随年龄与个人情况而异。<a href="https://www.cdc.gov/sleep/about/">CDC 参考</a></p></section>'  # noqa: E501
    )
    vo2 = report["vo2max"]
    body.append('<div class="longterm"><strong>长期指标 · 最近一次记录</strong><br>')
    body.append(
        f"VO₂ max {fmt(vo2['value'], 2)} mL/kg/min · {esc(vo2['date'])}。非当天测量，留作长期趋势参考。"  # noqa: E501
        if vo2
        else "近期没有可用 VO₂ max 记录。"
    )
    body.append("体重来自独立链路，未接入时不据 HealthKit 缺失判断未称重。</div>")
    body.append(
        '<details><summary>数据口径与局限</summary><ol><li>选用近期主要的同一手表来源，不叠加手机。步数与热量跨午夜按时长比例估算，可能与健康 App 合并结果不同。</li><li>睡眠相隔不超过 90 分钟的片段归为一个时段；每天选醒来日期对应的最长主睡眠，睡眠时长合并重叠后扣除清醒。不是全部午睡总和。</li><li>运动重叠时段合并；缺失日不填零。心率、HRV 先取每日中位数，再与前 7 日中位数比较；夜间指标同理。血氧比例乘以 100。</li><li>补传会修订后续快照。结果仅描述已收到记录，不代替医疗评估。<a href="https://support.apple.com/en-us/108906">Apple 睡眠记录说明</a></li></ol></details><footer class="footer"><span>Apple Watch 记录 · 后续补传可能修订结果</span><span>私人健康简报</span></footer>'  # noqa: E501
    )
    ranges, texts = [], []
    for n in nights:
        if not n:
            ranges.append(None)
            texts.append("暂无记录")
            continue
        a, b = datetime.fromisoformat(n["start"]), datetime.fromisoformat(n["end"])
        midnight = b.replace(hour=0, minute=0, second=0, microsecond=0)
        ranges.append(
            [(a - midnight).total_seconds() / 3600, (b - midnight).total_seconds() / 3600]
        )
        texts.append(a.strftime("%H:%M") + " → " + b.strftime("%H:%M"))
    chart = {
        "labels": [d[5:].replace("-", "/") for d in dates],
        "hours": [n["minutes"] / 60 if n else None for n in nights],
        "ranges": ranges,
        "texts": texts,
        "stages": stage_values,
        "stageLabels": stage_labels,
        "sleepMinutes": sleep["minutes"] if sleep else 1,
        "sleepBaselineHours": baseline / 60 if baseline is not None else None,
    }
    template = (
        Path(__file__).with_name("templates").joinpath("brief.html").read_text(encoding="utf-8")
    )
    chart_json = json.dumps(chart, ensure_ascii=False, allow_nan=False).replace("<", "\\u003c")
    return template.replace("__REPORT_BODY__", "".join(body)).replace("__CHART_DATA__", chart_json)

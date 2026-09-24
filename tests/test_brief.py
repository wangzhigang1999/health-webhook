import json
from datetime import date, datetime

import pytest

from health_webhook.brief import TZ, collect, render
from health_webhook.offline import ingest, initialize


def row(uid, start, end, *, value=None, category=None, product="WatchTest"):
    item = {
        "uuid": uid,
        "startUnixMs": int(datetime.fromisoformat(start).replace(tzinfo=TZ).timestamp() * 1000),
        "endUnixMs": int(datetime.fromisoformat(end).replace(tzinfo=TZ).timestamp() * 1000),
        "source": {"bundleId": product, "productType": product},
    }
    if category is not None:
        item["category"] = {"valueName": category}
    else:
        item["quantity"] = {"value": value, "unit": "test"}
    return item


def test_brief_sleep_crosses_midnight_overlap_and_missing(tmp_path):
    db = initialize(tmp_path / "brief.duckdb")
    batches = [
        {
            "hkTypeId": "HKCategoryTypeIdentifierSleepAnalysis",
            "samples": [
                row("a", "2026-01-01T23:00", "2026-01-02T02:00", category="asleepCore"),
                row("b", "2026-01-02T01:00", "2026-01-02T02:00", category="asleepCore"),
                row("c", "2026-01-02T02:00", "2026-01-02T02:10", category="awake"),
                row("d", "2026-01-02T02:10", "2026-01-02T07:00", category="asleepREM"),
            ],
        },
        {
            "hkTypeId": "HKQuantityTypeIdentifierAppleExerciseTime",
            "samples": [
                row("e", "2026-01-02T10:00", "2026-01-02T10:20", value=20),
                row("f", "2026-01-02T10:10", "2026-01-02T10:30", value=20),
            ],
        },
        {
            "hkTypeId": "HKQuantityTypeIdentifierStepCount",
            "samples": [
                row("g", "2026-01-01T23:30", "2026-01-02T00:30", value=100),
                row("phone", "2026-01-02T10:00", "2026-01-02T11:00", value=999, product="Phone"),
            ],
        },
        {
            "hkTypeId": "HKQuantityTypeIdentifierOxygenSaturation",
            "samples": [
                row("o", "2026-01-02T03:00", "2026-01-02T03:00", value=0.97),
            ],
        },
    ]
    ingest(db, "synthetic", json.dumps({"batches": batches}).encode())
    result = collect(db, date(2026, 1, 2))
    assert result["sleep"]["minutes"] == 470
    assert result["sleep"]["stages"]["awake"] == 10
    assert result["sleep"]["vitals"]["OxygenSaturation"]["value"] == pytest.approx(97)
    assert result["daily"]["2026-01-02"]["AppleExerciseTime"] == 30
    assert result["metrics"]["AppleExerciseTime"]["value"] is None
    assert result["metrics"]["AppleExerciseTime"]["baseline"] is None
    assert result["metrics"]["StepCount"]["value"] == 50
    assert result["metrics"]["StepCount"]["baseline"] is None
    assert result["metrics"]["StepCount"]["days"] == 0
    assert result["activity_date"] == "2026-01-01"
    rendered = render(result)
    assert "23:00" in rendered and "07:00" in rendered
    assert "__CHART_DATA__" not in rendered
    assert "本次新增批次" not in rendered
    assert '<script src="http' not in rendered
    db.close()


def test_empty_brief_does_not_invent_values(tmp_path):
    db = initialize(tmp_path / "empty.duckdb")
    report = collect(db, date(2026, 1, 2))
    assert report["sleep"] is None
    assert report["metrics"]["StepCount"]["value"] is None
    assert "暂无记录" in render(report)
    assert "昨晚睡眠尚未同步" in render(report)
    db.close()


def test_noon_report_uses_last_night_yesterday_and_weight_cutoff(tmp_path):
    from health_webhook.iot_weight import SOURCE

    db = initialize(tmp_path / "noon.duckdb")
    weights = [
        row("w1", "2026-01-03T09:00", "2026-01-03T09:00", value=66, product=SOURCE),
        row("w2", "2026-01-03T13:00", "2026-01-03T13:00", value=70, product=SOURCE),
    ]
    for weight in weights:
        weight["quantity"]["unit"] = "kg"
    batches = [
        {"hkTypeId": "HKQuantityTypeIdentifierBodyMass", "samples": weights},
        {
            "hkTypeId": "HKQuantityTypeIdentifierStepCount",
            "samples": [
                row("s1", "2026-01-02T10:00", "2026-01-02T11:00", value=200),
                row("s2", "2026-01-03T10:00", "2026-01-03T11:00", value=30),
            ],
        },
        {
            "hkTypeId": "HKCategoryTypeIdentifierSleepAnalysis",
            "samples": [
                row("old", "2026-01-01T23:00", "2026-01-02T07:00", category="asleepCore"),
            ],
        },
    ]
    ingest(db, "initial", json.dumps({"batches": batches}).encode())
    cutoff = datetime(2026, 1, 3, 12, tzinfo=TZ)
    report = collect(db, cutoff.date(), as_of=cutoff)
    assert report["sleep"] is None  # never substitute the previous night
    assert report["nights"]["2026-01-02"]["minutes"] == 480
    assert report["metrics"]["StepCount"]["value"] == 200
    assert report["weight"]["latest"]["kg"] == 66
    assert report["weight"]["today"] == 66
    assert "2026-01-03 09:00" in render(report)
    late = [
        {
            "hkTypeId": "HKCategoryTypeIdentifierSleepAnalysis",
            "samples": [
                row("new", "2026-01-02T23:00", "2026-01-03T06:00", category="asleepCore"),
            ],
        }
    ]
    ingest(db, "late", json.dumps({"batches": late}).encode())
    revised = collect(db, cutoff.date(), as_of=cutoff)
    assert revised["date"] == report["date"]
    assert revised["sleep"]["minutes"] == 420
    assert revised["sleep_baseline"] == 480
    assert "昨晚睡眠尚未同步" not in render(revised)
    db.close()

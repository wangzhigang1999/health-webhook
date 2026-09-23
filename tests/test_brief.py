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
    assert result["metrics"]["AppleExerciseTime"]["value"] == 30
    assert result["metrics"]["AppleExerciseTime"]["baseline"] is None
    assert result["metrics"]["StepCount"]["value"] == 50
    assert result["metrics"]["StepCount"]["baseline"] == 50
    assert result["metrics"]["StepCount"]["days"] == 1
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
    assert "睡眠记录尚不完整" in render(report)
    db.close()

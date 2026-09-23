import hashlib
import io
import json
from datetime import date

import pytest

from health_webhook import app, iot_weight, report_reader
from health_webhook.brief import collect
from health_webhook.offline import ingest, initialize, report_history


def snapshot(kg=66, person="me"):
    return {
        "source": "real",
        "weights": [
            {
                "id": "scale:a:1",
                "ts": 1767312000,
                "kg": kg,
                "stable": True,
                "removed": False,
                "reason": None,
                "person": person,
            },
            {
                "id": "scale:a:2",
                "ts": 1767312060,
                "kg": 82,
                "stable": True,
                "removed": False,
                "reason": None,
                "person": "partner",
            },
        ],
    }


def test_weight_failure_then_correction_removal_and_restoration(tmp_path):
    cursor = tmp_path / "cursor.json"
    db = initialize(tmp_path / "analysis.duckdb")
    emitted = []

    def writer(raw):
        emitted.append(raw)
        ingest(db, hashlib.sha256(raw).hexdigest(), raw)

    def unavailable(raw):
        raise TimeoutError()

    with pytest.raises(TimeoutError):
        iot_weight.sync_snapshot(snapshot(), cursor, unavailable)
    assert not cursor.exists() and cursor.with_suffix(".pending.json").exists()
    iot_weight.sync_snapshot(snapshot(67), cursor, writer)
    assert len(emitted) == 2  # finish pending publication, then correct it
    assert db.execute("SELECT value FROM samples_current").fetchall() == [(67,)]
    assert not iot_weight.sync_snapshot(snapshot(67), cursor, writer)
    assert all(b"partner" not in raw and b"scale:a:2" not in raw for raw in emitted)
    iot_weight.sync_snapshot(snapshot(67, "partner"), cursor, writer)
    assert db.execute("SELECT count(*) FROM samples_current").fetchall()[0][0] == 0
    # Restoring a formerly deleted source reading must use a fresh UUID revision.
    iot_weight.sync_snapshot(snapshot(67), cursor, writer)
    assert db.execute("SELECT value FROM samples_current").fetchall() == [(67,)]
    report = collect(db, date(2026, 1, 3))
    assert report["weight"]["today"] is None  # do not forward-fill yesterday's weight
    assert report["weight"]["latest"]["kg"] == 67
    assert report["weight"]["week_days"] == 1
    db.close()


def test_invalid_iot_snapshot_cannot_erase_cursor(tmp_path):
    cursor = tmp_path / "cursor.json"
    iot_weight.sync_snapshot(snapshot(), cursor, lambda _: None)
    before = cursor.read_bytes()
    with pytest.raises(ValueError):
        iot_weight.sync_snapshot({"error": "temporarily unavailable"}, cursor, lambda _: None)
    assert cursor.read_bytes() == before


def test_report_requires_capability_and_checks_private_committed_content(monkeypatch):
    monkeypatch.setattr(app.settings, "report_access_key", "synthetic-private-reader-key")
    monkeypatch.setattr(report_reader, "_cached_manifest", None)
    content = b"<!doctype html><html><body><main>Synthetic report</main></body></html>"
    item = {
        "key": "health/v2/reports/date=2026-01-02/test.html",
        "sha256": hashlib.sha256(content).hexdigest(),
        "bytes": len(content),
    }
    doc = {"report": item, "report_history": [{**item, "date": "2026-01-02"}]}

    class Bucket:
        def __init__(self):
            self.reads = []

        def get_object(self, key):
            self.reads.append(key)
            return io.BytesIO(json.dumps(doc).encode() if key.endswith("latest.json") else content)

    target = Bucket()
    monkeypatch.setattr(report_reader, "bucket", lambda: target)
    client = app.app.test_client()
    assert client.get("/reports").status_code == 401
    assert client.get("/reports", headers={"Remote-User": "admin"}).status_code == 401
    assert not target.reads
    headers = {"X-Health-Report-Key": "synthetic-private-reader-key"}
    result = client.get("/reports", headers=headers)
    assert result.status_code == 200 and b"Synthetic report" in result.data
    assert result.headers["Cache-Control"] == "private, no-store"
    assert result.headers["X-Robots-Tag"].startswith("noindex")
    assert client.get("/reports?date=../../raw", headers=headers).status_code == 400
    assert client.get("/reports?date=2026-01-01", headers=headers).status_code == 404
    item["sha256"] = "mismatch"
    monkeypatch.setattr(report_reader, "_cached_manifest", None)
    assert client.get("/reports", headers=headers).status_code == 503
    assert client.get("/", headers={"Accept": "text/html"}).location == "/reports"
    assert client.get("/healthz").status_code == 200


def test_history_preserves_previous_days_and_replaces_regenerated_day():
    old = {"key": "health/v2/reports/date=2026-01-01/old.html", "sha256": "a"}
    new = {"key": "health/v2/reports/date=2026-01-02/new.html", "sha256": "b"}
    history = report_history({"report": old}, new, date(2026, 1, 2))
    assert [v["date"] for v in history] == ["2026-01-02", "2026-01-01"]
    replacement = {**new, "sha256": "c"}
    updated = report_history({"report_history": history}, replacement, date(2026, 1, 2))
    assert len(updated) == 2 and updated[0]["sha256"] == "c"

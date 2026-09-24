import json

import pytest

from health_webhook.offline import ingest, initialize
from health_webhook.strava_sync import DeferredError, envelope, resume, sync


def activity(name="Ride"):
    return {
        "id": 42,
        "name": name,
        "start_date": "2026-09-23T00:00:00Z",
        "elapsed_time": 120,
        "moving_time": 90,
        "sport_type": "Ride",
        "device_watts": True,
    }


class FakeClient:
    def __init__(self, rows, fail_streams=False):
        self.rows = rows
        self.fail_streams = fail_streams

    def get(self, path, **kwargs):
        if path.startswith("athlete/"):
            return self.rows
        if "/streams?" in path:
            if self.fail_streams:
                raise DeferredError()
            return {"time": {"data": [0, 1]}, "heartrate": {"data": [110, 111]}}
        return self.rows[0] if self.rows else None


def test_revisions_and_removal_do_not_conflict(tmp_path):
    db = initialize(tmp_path / "test.duckdb")
    docs = []

    def writer(raw):
        docs.append(raw)
        ingest(db, str(len(docs)), raw)

    sync(tmp_path, FakeClient([activity()]), writer)
    sync(tmp_path, FakeClient([activity()]), writer)
    assert len(docs) == 1
    sync(tmp_path, FakeClient([activity("Corrected")]), writer)
    assert db.execute("SELECT count(*) FROM workouts").fetchone() == (1,)
    assert db.execute("SELECT count(*) FROM sample_conflicts").fetchone() == (0,)
    sync(tmp_path, FakeClient([]), writer)
    assert db.execute("SELECT count(*) FROM workouts").fetchone() == (0,)
    sync(tmp_path, FakeClient([activity()]), writer)
    assert db.execute("SELECT count(*) FROM workouts").fetchone() == (1,)
    db.close()


def test_failed_upload_journal_recovers(tmp_path):
    def fail(raw):
        raise OSError("offline")

    with pytest.raises(OSError):
        sync(tmp_path, FakeClient([activity()]), fail)
    assert not (tmp_path / "cursor.json").exists()
    docs = []
    resume(tmp_path, docs.append)
    sync(tmp_path, FakeClient([activity()]), docs.append)
    assert len(docs) == 1
    assert not (tmp_path / "pending.json").exists()


def test_partial_fetch_never_marks_complete(tmp_path):
    with pytest.raises(DeferredError):
        sync(tmp_path, FakeClient([activity()], fail_streams=True), lambda raw: None)
    assert not (tmp_path / "cursor.json").exists()


def test_incomplete_catalog_never_deletes(tmp_path):
    docs = []
    sync(tmp_path, FakeClient([activity()]), docs.append)

    class Partial:
        def get(self, path):
            if "page=1&" in path:
                return [{**activity(), "id": i + 100} for i in range(200)]
            raise DeferredError()

    with pytest.raises(DeferredError):
        sync(tmp_path, Partial(), docs.append)
    assert len(docs) == 1
    assert json.loads((tmp_path / "cursor.json").read_text())["activities"]["42"]["active"]


def test_time_and_power_provenance():
    _, body = envelope(activity(), {}, 1)
    sample = body["batches"][0]["samples"][0]
    assert sample["endUnixMs"] - sample["startUnixMs"] == 120000
    assert sample["workout"]["movingTimeSeconds"] == 90
    assert sample["strava"]["powerSource"] == "device"
    assert sample["strava"]["streams"] == {}

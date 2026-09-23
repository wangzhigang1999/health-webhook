import hashlib
import json
from types import SimpleNamespace

import pytest

from health_webhook import app, raw_store
from health_webhook.offline import ingest, initialize


def body(samples=None, deleted=None, **extra):
    return json.dumps(
        {
            "batchId": "b1",
            "deviceId": "phone",
            "batches": [
                {"hkTypeId": "heart", "samples": samples or [], "deletedUuids": deleted or []}
            ],
            **extra,
        }
    ).encode()


def sample(uuid="a", value=75):
    return {
        "uuid": uuid,
        "startUnixMs": "1790170983174",
        "endUnixMs": 1790170983174,
        "quantity": {"value": value, "unit": "count/min"},
    }


@pytest.fixture
def spool(tmp_path, monkeypatch):
    monkeypatch.setattr(raw_store.settings, "data_dir", str(tmp_path))
    monkeypatch.setattr(raw_store.settings, "min_disk_free_bytes", 0)
    return tmp_path / "outbox-v2"


def test_failure_preserves_outbox_and_verified_success_removes_it(spool):
    raw = body([sample()])
    digest = raw_store.stage(raw)
    assert raw_store.stage(raw) == digest

    class Failing:
        def put_object(self, *args, **kwargs):
            raise TimeoutError()

    with pytest.raises(TimeoutError):
        raw_store.upload_staged(digest, Failing())
    assert (spool / f"{digest}.json.gz").exists()

    class Good:
        def put_object(self, key, data, headers):
            assert headers["x-oss-meta-sha256"] == hashlib.sha256(raw).hexdigest()
            assert headers["x-oss-forbid-overwrite"] == "true"

    raw_store.upload_staged(digest, Good())
    assert not list(spool.glob("*.json.gz"))


def test_outbox_limit_never_discards_pending(spool, monkeypatch):
    digest = raw_store.stage(body([sample()]))
    monkeypatch.setattr(raw_store.settings, "outbox_max_bytes", 10)
    with pytest.raises(raw_store.OutboxFullError):
        raw_store.stage(body([sample("different")]))
    assert (spool / f"{digest}.json.gz").exists()


def test_existing_remote_object_must_match(spool):
    import oss2

    digest = raw_store.stage(body([sample()]))

    class Collision:
        def put_object(self, *args, **kwargs):
            raise oss2.exceptions.ServerError(409, {}, b"", {"Code": "FileAlreadyExists"})

        def head_object(self, key):
            return SimpleNamespace(headers={"x-oss-meta-sha256": "wrong"})

    with pytest.raises(ValueError):
        raw_store.upload_staged(digest, Collision())
    assert (spool / f"{digest}.json.gz").exists()


def test_endpoint_compatibility_auth_failure_and_no_dashboard(monkeypatch):
    monkeypatch.setattr(app.settings, "auth_token", "test")
    client = app.app.test_client()
    assert client.post("/", data=body()).status_code == 401
    headers = {"Authorization": "Bearer test"}
    monkeypatch.setattr(app, "save", lambda raw: "receipt")
    for route in ("/", "/ingest"):
        result = client.post(route, data=body([sample()]), headers=headers)
        assert result.status_code == 200
        assert result.json["total_rows"] == 1 and result.json["oss_saved"]
    assert client.get("/api/dashboard").status_code == 404
    assert client.get("/static/app.js").status_code == 404

    def fail(raw):
        raise TimeoutError()

    monkeypatch.setattr(app, "save", fail)
    assert client.post("/", data=body(), headers=headers).status_code == 503
    assert client.post("/", data=b"broken", headers=headers).status_code == 400
    assert client.post("/", data=b"x" * (2 * 1024 * 1024 + 1), headers=headers).status_code == 413


def test_deleted_before_add_duplicate_workout_and_conflict(tmp_path):
    db = initialize(tmp_path / "test.duckdb")
    try:
        ingest(db, "delete-first", body(deleted=["a"]))
        ingest(db, "add-later", body([sample()]))
        assert db.execute("SELECT count(*) FROM samples_current").fetchone()[0] == 0
        assert not ingest(db, "add-later", body([sample()]))
        ingest(db, "repeat", body([sample()]))
        assert db.execute("SELECT count(*) FROM sample_events").fetchone()[0] == 1
        w = {
            "uuid": "w",
            "startUnixMs": 1790170983174,
            "endUnixMs": 1790171083174,
            "workout": {"durationSeconds": 100, "events": []},
        }
        ingest(db, "workout", body([w, sample("b")]))
        assert db.execute("SELECT count(*) FROM workouts").fetchone()[0] == 1
        ingest(db, "conflict", body([sample("b", 99)]))
        assert db.execute("SELECT count(*) FROM sample_conflicts").fetchone()[0] == 1
        assert db.execute("SELECT count(*) FROM samples_current").fetchone()[0] == 1
    finally:
        db.close()


def test_full_batch_has_bounded_json_parse_memory(tmp_path):
    db = initialize(tmp_path / "full-batch.duckdb")
    db.execute("SET memory_limit='128MB'")
    items = [sample(str(i)) for i in range(2000)]
    for item in items:
        item["source"] = {"name": "Synthetic watch " * 20, "bundleId": "com.example.health"}
    try:
        ingest(db, "full-batch", body(items))
        assert db.execute("SELECT count(*) FROM samples_current").fetchone()[0] == 2000
    finally:
        db.close()


def test_multipart_order_commit_guard_and_abort(tmp_path):
    import pytest

    from health_webhook.offline import multipart_file

    path = tmp_path / "checkpoint.bin"
    path.write_bytes(b"a" * (256 * 1024) + b"b" * 1024)

    class Target:
        aborted = False
        fail_commit = False

        def init_multipart_upload(self, key, headers):
            return SimpleNamespace(upload_id="our-upload")

        def upload_part(self, key, upload_id, number, content, headers):
            import oss2

            assert headers["Content-MD5"] == oss2.utils.content_md5(content)
            assert content == (b"a" * (256 * 1024) if number == 1 else b"b" * 1024)
            return SimpleNamespace(etag=str(number), crc=None)

        def complete_multipart_upload(self, key, upload_id, parts, headers):
            assert headers["x-oss-forbid-overwrite"] == "true"
            assert [p.part_number for p in parts] == [1, 2]
            if self.fail_commit:
                raise RuntimeError("simulated failed commit")

        def abort_multipart_upload(self, key, upload_id):
            assert upload_id == "our-upload"
            self.aborted = True

    target = Target()
    multipart_file(target, "private-state", path, {"x-oss-forbid-overwrite": "true"})
    assert not target.aborted
    target.fail_commit = True
    with pytest.raises(RuntimeError):
        multipart_file(target, "private-state", path, {"x-oss-forbid-overwrite": "true"})
    assert target.aborted


def test_publish_pull_and_incremental_restore(tmp_path, monkeypatch):
    import gzip
    import io

    import duckdb
    import oss2

    from health_webhook import offline

    class MemoryBucket:
        def __init__(self):
            self.files = {}

        def get_object(self, key):
            if key not in self.files:
                raise oss2.exceptions.NoSuchKey(404, {}, b"", {})
            return io.BytesIO(self.files[key])

        def put_object_from_file(self, key, filename, headers):
            from pathlib import Path

            self.files[key] = Path(filename).read_bytes()

        def get_object_to_file(self, key, filename):
            from pathlib import Path

            Path(filename).write_bytes(self.files[key])

        def delete_object(self, key):
            del self.files[key]

    target = MemoryBucket()
    raw = body([sample()])
    key = offline.ROOT + "/raw/default/aa/" + hashlib.sha256(raw).hexdigest() + ".json.gz"
    target.files[key] = gzip.compress(raw)
    monkeypatch.setattr(offline, "connect_bucket", lambda _: target)
    monkeypatch.setattr(
        offline.oss2,
        "ObjectIteratorV2",
        lambda b, prefix: [SimpleNamespace(key=k) for k in sorted(b.files) if k.startswith(prefix)],
    )
    args = SimpleNamespace(directory=tmp_path / "builder", credentials_csv=None)
    offline.build(args)
    manifest = offline.load_latest(target)
    assert manifest["quality"]["active_samples"] == 1
    assert len(manifest["files"]) == 1
    offline.pull(SimpleNamespace(directory=tmp_path / "reader", credentials_csv=None))
    db = duckdb.connect(str(tmp_path / "reader" / "analysis.duckdb"))
    assert db.execute("SELECT count(*) FROM samples").fetchone()[0] == 1
    db.close()
    # Simulate a fresh CI runner loading a private compressed checkpoint.
    offline.build(SimpleNamespace(directory=tmp_path / "fresh-ci", credentials_csv=None))
    assert offline.load_latest(target)["quality"]["active_samples"] == 1
    # A deletion-only snapshot must replace existing reader views with an empty schema.
    raw_delete = body(deleted=["a"])
    delete_key = (
        offline.ROOT + "/raw/default/bb/" + hashlib.sha256(raw_delete).hexdigest() + ".json.gz"
    )
    target.files[delete_key] = gzip.compress(raw_delete)
    offline.build(SimpleNamespace(directory=tmp_path / "empty-ci", credentials_csv=None))
    offline.pull(SimpleNamespace(directory=tmp_path / "reader", credentials_csv=None))
    db = duckdb.connect(str(tmp_path / "reader" / "analysis.duckdb"))
    assert db.execute("SELECT count(*) FROM samples").fetchone()[0] == 0
    assert db.execute("SELECT count(*) FROM workouts").fetchone()[0] == 0
    db.close()

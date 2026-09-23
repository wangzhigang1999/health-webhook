"""Incremental DuckDB ingestion, immutable daily Parquet and private daily reports."""

import argparse
import csv
import gzip
import hashlib
import json
import os
import shutil
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import oss2
from filelock import FileLock

ROOT = "health/v2"
PARSER_VERSION = 1


def connect_bucket(credentials_csv=None):
    if credentials_csv:
        with open(credentials_csv, encoding="utf-8-sig", newline="") as stream:
            credentials = next(csv.DictReader(stream))
        ak, sk = credentials["AccessKey ID"], credentials["AccessKey Secret"]
        token = ""
    else:
        ak, sk = os.environ["OSS_ACCESS_KEY_ID"], os.environ["OSS_ACCESS_KEY_SECRET"]
        token = os.environ.get("OSS_SESSION_TOKEN", "")
    auth = oss2.StsAuth(ak, sk, token) if token else oss2.Auth(ak, sk)
    endpoint = os.environ.get("OSS_ENDPOINT", "oss-cn-beijing.aliyuncs.com")
    endpoint = "https://" + endpoint.removeprefix("https://").removeprefix("http://")
    return oss2.Bucket(
        auth, endpoint, os.environ.get("OSS_BUCKET", "zhigang-health"), connect_timeout=30
    )


def initialize(path: Path):
    db = duckdb.connect(str(path))
    db.execute("SET memory_limit='768MB'")
    db.execute("SET threads=2")
    db.execute("SET TimeZone='UTC'")
    db.execute("""
        CREATE TABLE IF NOT EXISTS ingest_objects (
          object_key VARCHAR PRIMARY KEY, raw_sha256 VARCHAR, samples BIGINT,
          deletions BIGINT, is_test BOOLEAN, imported_at TIMESTAMP DEFAULT current_timestamp);
        CREATE TABLE IF NOT EXISTS sample_events (
          sample_uuid VARCHAR, version_hash VARCHAR, metric_type VARCHAR, device_id VARCHAR,
          start_ms BIGINT, end_ms BIGINT, dt DATE, kind VARCHAR, value DOUBLE, unit VARCHAR,
          category VARCHAR, source_name VARCHAR, source_bundle VARCHAR, source_product VARCHAR,
          payload JSON, first_object VARCHAR, PRIMARY KEY(sample_uuid, version_hash));
        CREATE TABLE IF NOT EXISTS deletion_events (
          sample_uuid VARCHAR, metric_type VARCHAR, object_key VARCHAR,
          PRIMARY KEY(sample_uuid, metric_type, object_key));
        CREATE OR REPLACE VIEW sample_conflicts AS
          SELECT sample_uuid, count(*) AS versions FROM sample_events
          GROUP BY sample_uuid HAVING count(*) > 1;
        CREATE OR REPLACE VIEW samples_current AS
          SELECT s.* FROM sample_events s
          WHERE sample_uuid <> '' AND start_ms IS NOT NULL AND end_ms >= start_ms
          AND NOT EXISTS (SELECT 1 FROM deletion_events d WHERE d.sample_uuid=s.sample_uuid)
          AND NOT EXISTS (SELECT 1 FROM sample_conflicts c WHERE c.sample_uuid=s.sample_uuid);
        CREATE OR REPLACE VIEW health_metrics AS
          SELECT * FROM samples_current WHERE kind IN ('quantity','category');
        CREATE OR REPLACE VIEW workouts AS
          SELECT * FROM samples_current WHERE kind='workout';
    """)
    return db


def ingest(db, key: str, raw: bytes) -> bool:
    digest = hashlib.sha256(raw).hexdigest()
    if db.execute("SELECT 1 FROM ingest_objects WHERE object_key=?", [key]).fetchone():
        return False
    data = json.loads(raw)
    if "body" in data:
        data = data["body"]
        if isinstance(data, str):
            data = json.loads(data)
    batches = data.get("batches", [])
    sample_count = sum(len(b.get("samples", [])) for b in batches)
    delete_count = sum(len(b.get("deletedUuids", [])) for b in batches)
    is_test = data.get("deviceId") in ("smoke-dev", "test-device")
    # Normalize timestamps before hashing; keep the unchanged source object in OSS/cache.
    for batch in batches:
        for sample in batch.get("samples", []):
            for field in ("startUnixMs", "endUnixMs"):
                with suppress(KeyError, TypeError, ValueError):
                    sample[field] = int(str(sample[field]))
    entries = [
        {"metric": batch.get("hkTypeId"), "sample": sample}
        for batch in batches
        for sample in batch.get("samples", [])
    ]
    deletions = [
        {"metric": batch.get("hkTypeId", ""), "uuid": uid}
        for batch in batches
        for uid in batch.get("deletedUuids", [])
    ]
    doc = json.dumps(entries, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    deletion_doc = json.dumps(deletions, ensure_ascii=False)
    db.execute("BEGIN")
    try:
        if not is_test:
            db.execute(
                """
                INSERT OR IGNORE INTO sample_events
                WITH entries AS (
                  SELECT json_extract_string(e.value,'$.metric') AS metric,
                         json_extract(e.value,'$.sample') AS sample
                  FROM json_each(?::JSON) e
                ), parsed AS (
                  SELECT *, try_cast(json_extract_string(sample,'$.startUnixMs') AS BIGINT) AS a,
                            try_cast(json_extract_string(sample,'$.endUnixMs') AS BIGINT) AS z
                  FROM entries
                )
                SELECT coalesce(json_extract_string(sample,'$.uuid'),''),
                       sha256(coalesce(metric,'') || ':' || sample::VARCHAR), metric, ?, a, z,
                       try_cast(epoch_ms(a) + INTERVAL 8 HOUR AS DATE),
                       CASE WHEN json_exists(sample,'$.quantity') THEN 'quantity'
                            WHEN json_exists(sample,'$.category') THEN 'category'
                            WHEN json_exists(sample,'$.workout') THEN 'workout' ELSE 'other' END,
                       coalesce(try_cast(json_extract_string(sample,'$.quantity.value') AS DOUBLE),
                                try_cast(json_extract_string(sample,'$.category.value') AS DOUBLE)),
                       json_extract_string(sample,'$.quantity.unit'),
                       json_extract_string(sample,'$.category.valueName'),
                       json_extract_string(sample,'$.source.name'),
                       json_extract_string(sample,'$.source.bundleId'),
                       json_extract_string(sample,'$.source.productType'), sample, ?
                FROM parsed
            """,
                [doc, data.get("deviceId"), key],
            )
            db.execute(
                """
                INSERT OR IGNORE INTO deletion_events
                SELECT json_extract_string(d.value,'$.uuid'),
                       coalesce(json_extract_string(d.value,'$.metric'),''), ?
                FROM json_each(?::JSON) d
            """,
                [key, deletion_doc],
            )
        db.execute(
            "INSERT INTO ingest_objects VALUES (?,?,?,?,?,current_timestamp)",
            [key, digest, sample_count, delete_count, is_test],
        )
        db.execute("COMMIT")
    except Exception:
        db.execute("ROLLBACK")
        raise
    return True


def sha_file(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def put_file(bucket, key, path, *, immutable=True):
    digest = sha_file(path)
    headers = {"x-oss-meta-sha256": digest, "x-oss-object-acl": "private"}
    if immutable:
        headers["x-oss-forbid-overwrite"] = "true"
    try:
        if Path(path).stat().st_size >= 32 * 1024 * 1024:
            multipart_file(bucket, key, path, headers)
        else:
            bucket.put_object_from_file(key, str(path), headers=headers)
    except oss2.exceptions.ServerError as exc:
        if exc.status != 409 or exc.code != "FileAlreadyExists":
            raise
        result = bucket.head_object(key)
        if result.headers.get("x-oss-meta-sha256") != digest:
            raise ValueError("published object digest mismatch") from None
    return {"key": key, "sha256": digest, "bytes": Path(path).stat().st_size}


def multipart_file(bucket, key, path, headers):
    """Bound large transfers; pass forbid-overwrite on the final atomic commit too."""
    upload_id = bucket.init_multipart_upload(key, headers=headers).upload_id
    part_bytes = oss2.determine_part_size(Path(path).stat().st_size, preferred_size=256 * 1024)

    def upload_part(number):
        with Path(path).open("rb") as stream:
            stream.seek((number - 1) * part_bytes)
            content = stream.read(part_bytes)
        for attempt in range(3):
            try:
                result = bucket.upload_part(
                    key,
                    upload_id,
                    number,
                    content,
                    headers={"Content-MD5": oss2.utils.content_md5(content)},
                )
                return oss2.models.PartInfo(
                    number, result.etag, size=len(content), part_crc=result.crc
                )
            except oss2.exceptions.RequestError:
                if attempt == 2:
                    raise
                time.sleep(2**attempt)

    try:
        count = (Path(path).stat().st_size + part_bytes - 1) // part_bytes
        with ThreadPoolExecutor(max_workers=4) as pool:
            parts = list(pool.map(upload_part, range(1, count + 1)))
        bucket.complete_multipart_upload(key, upload_id, parts, headers=headers)
    except BaseException:
        with suppress(Exception):
            bucket.abort_multipart_upload(key, upload_id)
        raise


def download_checked(bucket, item, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and sha_file(path) == item["sha256"]:
        return
    temporary = path.with_suffix(path.suffix + ".part")
    bucket.get_object_to_file(item["key"], str(temporary))
    if sha_file(temporary) != item["sha256"]:
        temporary.unlink()
        raise ValueError("download checksum mismatch")
    os.replace(temporary, path)


def load_latest(bucket):
    try:
        result = bucket.get_object(f"{ROOT}/manifests/latest.json")
        try:
            manifest = json.loads(result.read())
        finally:
            result.close()
        if manifest["parser_version"] != PARSER_VERSION:
            raise ValueError("unsupported parser version")
        return manifest
    except oss2.exceptions.NoSuchKey:
        return None


def state_files(state):
    if state.get("format") == "parquet-v1":
        return state["files"]
    return [state]


def restore_state(bucket, state, work, dbpath):
    temporary = dbpath.with_suffix(".restore")
    temporary.unlink(missing_ok=True)
    Path(str(temporary) + ".wal").unlink(missing_ok=True)
    if state.get("format") != "parquet-v1":
        checkpoint = work / "checkpoint.duckdb.gz"
        download_checked(bucket, state, checkpoint)
        with gzip.open(checkpoint, "rb") as src, temporary.open("wb") as dst:
            shutil.copyfileobj(src, dst)
    else:

        def fetch(item):
            local = work / "checkpoint" / (item["sha256"] + ".parquet")
            download_checked(bucket, item, local)

        # Checkpoints contain many small daily objects; overlap network round trips.
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(fetch, state["files"]))
        restored = initialize(temporary)
        try:
            for table in ("sample_events", "deletion_events", "ingest_objects"):
                for item in state["files"]:
                    if item["table"] != table:
                        continue
                    local = work / "checkpoint" / (item["sha256"] + ".parquet")
                    restored.execute(
                        f"INSERT INTO {table} BY NAME "
                        "SELECT * FROM read_parquet(?, hive_partitioning=false)",
                        [str(local)],
                    )
            restored.execute("CHECKPOINT")
        finally:
            restored.close()
    os.replace(temporary, dbpath)


def publish_state(db, bucket, export, previous):
    """Columnar checkpoints reuse unchanged days instead of uploading a full database."""
    old_files = {item["key"]: item for item in state_files(previous)} if previous else {}
    files = []
    folder = export / "checkpoint"
    folder.mkdir()
    days = db.execute("SELECT DISTINCT dt FROM sample_events ORDER BY dt").fetchall()
    for (day,) in days:
        label = str(day) if day is not None else "unknown"
        path = folder / f"events-{label}.parquet"
        target = str(path).replace("'", "''")
        db.execute(
            "COPY (SELECT * FROM sample_events WHERE dt IS NOT DISTINCT FROM ? "
            "ORDER BY metric_type,start_ms,sample_uuid,version_hash) "
            f"TO '{target}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 16384)",
            [day],
        )
        key = f"{ROOT}/state/events/dt={label}/{sha_file(path)}.parquet"
        info = old_files.get(key) or put_file(bucket, key, path)
        files.append({**info, "table": "sample_events"})
    for table, order in (
        ("deletion_events", "sample_uuid,metric_type,object_key"),
        ("ingest_objects", "object_key"),
    ):
        path = folder / f"{table}.parquet"
        db.execute(
            f"COPY (SELECT * FROM {table} ORDER BY {order}) "
            "TO ? (FORMAT PARQUET, COMPRESSION ZSTD)",
            [str(path)],
        )
        key = f"{ROOT}/state/{table}/{sha_file(path)}.parquet"
        info = old_files.get(key) or put_file(bucket, key, path)
        files.append({**info, "table": table})
    return {"format": "parquet-v1", "files": files}


def build(args):
    bucket = connect_bucket(args.credentials_csv)
    work = args.directory.resolve()
    work.mkdir(parents=True, exist_ok=True)
    with FileLock(str(work / "analysis.lock"), timeout=1):
        previous = load_latest(bucket)
        dbpath = work / "health.duckdb"
        if not dbpath.exists() and previous and previous.get("state"):
            print("Restoring private analysis checkpoint.", flush=True)
            restore_state(bucket, previous["state"], work, dbpath)
        db = initialize(dbpath)
        before_samples = db.execute("SELECT count(*) FROM sample_events").fetchone()[0]
        before_deletions = db.execute("SELECT count(*) FROM deletion_events").fetchone()[0]
        new_objects = 0
        for item in oss2.ObjectIteratorV2(bucket, prefix=f"{ROOT}/raw/default/"):
            key = item.key
            if not key.endswith(".json.gz"):
                continue
            if db.execute("SELECT 1 FROM ingest_objects WHERE object_key=?", [key]).fetchone():
                continue
            digest = key.rsplit("/", 1)[-1].removesuffix(".json.gz")
            cache = work / "raw" / f"{digest}.json.gz"
            cache.parent.mkdir(parents=True, exist_ok=True)
            if not cache.exists():
                temporary = cache.with_suffix(".part")
                bucket.get_object_to_file(key, str(temporary))
                os.replace(temporary, cache)
            raw = gzip.decompress(cache.read_bytes())
            if hashlib.sha256(raw).hexdigest() != digest:
                raise ValueError("raw object checksum mismatch")
            new_objects += ingest(db, key, raw)
            if new_objects % 100 == 0:
                print("Incremental import checkpoint completed", flush=True)
        now = datetime.now(UTC)
        generation = now.strftime("%Y%m%dT%H%M%SZ-") + uuid.uuid4().hex[:8]
        export = work / "exports" / generation
        export.mkdir(parents=True)
        files = []
        print("Exporting daily partitions.", flush=True)
        db.execute("CREATE TEMP TABLE current_export AS SELECT * FROM samples_current")
        # Write one day at a time: hundreds of concurrently buffered partition writers
        # can exceed the memory budget even when the full dataset is relatively small.
        db.execute("SET threads=1")
        days = db.execute("SELECT DISTINCT dt FROM current_export ORDER BY dt").fetchall()
        for (day,) in days:
            destination = export / "dataset" / f"dt={day}" / "data.parquet"
            destination.parent.mkdir(parents=True)
            target_literal = str(destination).replace("'", "''")
            db.execute(
                "COPY (SELECT * EXCLUDE(dt) FROM current_export WHERE dt=? "
                "ORDER BY metric_type,start_ms,sample_uuid) "
                f"TO '{target_literal}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 16384)",
                [day],
            )
        previous_files = {f["key"]: f for f in previous["files"]} if previous else {}
        for file in sorted((export / "dataset").glob("dt=*/*.parquet")):
            day = file.parent.name.removeprefix("dt=")
            digest = sha_file(file)
            key = f"{ROOT}/parquet/samples/dt={day}/{digest}.parquet"
            info = previous_files.get(key) or put_file(bucket, key, file)
            files.append({**info, "dt": str(day)})
        report_day = (now + timedelta(hours=8) - timedelta(days=1)).date()
        metrics = db.execute(
            """
            SELECT metric_type,kind,unit,count(*) AS samples,
                   min(value) FILTER (WHERE kind='quantity'),
                   max(value) FILTER (WHERE kind='quantity'),
                   avg(value) FILTER (WHERE kind='quantity')
            FROM current_export WHERE dt=?
            GROUP BY metric_type,kind,unit ORDER BY metric_type
        """,
            [report_day],
        ).fetchall()
        quality = dict(
            zip(
                [
                    "raw_objects",
                    "sample_versions",
                    "distinct_uuids",
                    "deletion_events",
                    "conflicted_uuids",
                    "active_samples",
                    "invalid_samples",
                ],
                [
                    db.execute(sql).fetchone()[0]
                    for sql in (
                        "SELECT count(*) FROM ingest_objects WHERE NOT is_test",
                        "SELECT count(*) FROM sample_events",
                        "SELECT count(DISTINCT sample_uuid) FROM sample_events",
                        "SELECT count(*) FROM deletion_events",
                        "SELECT count(*) FROM sample_conflicts",
                        "SELECT count(*) FROM current_export",
                        "SELECT count(*) FROM sample_events WHERE sample_uuid='' "
                        "OR start_ms IS NULL OR end_ms IS NULL OR end_ms<start_ms",
                    )
                ],
                strict=True,
            )
        )
        report = {
            "schema_version": 1,
            "generated_at": now.isoformat(),
            "sample_date": str(report_day),
            "new_raw_objects": new_objects,
            "new_sample_versions": quality["sample_versions"] - before_samples,
            "new_deletion_events": quality["deletion_events"] - before_deletions,
            "quality": quality,
            "metrics": [
                dict(
                    zip(
                        ["metric_type", "kind", "unit", "samples", "min", "max", "mean"],
                        row,
                        strict=True,
                    )
                )
                for row in metrics
            ],
            "note": "Descriptive sample statistics, not Apple Health totals. "
            "Units and sources may differ; no diagnosis.",
        }
        report_path = export / "report.json"
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        report_json = put_file(
            bucket, f"{ROOT}/reports/date={report_day}/{generation}.json", report_path
        )
        markdown = [
            f"# 健康数据日报 · {report_day}",
            "",
            f"生成时间（UTC）：{now.isoformat()}",
            "",
            f"本次新增批次：{new_objects}；新增样本版本：{report['new_sample_versions']}；"
            f"新增删除事件：{report['new_deletion_events']}。",
            "",
            f"当前有效样本：{quality['active_samples']}；冲突 UUID："
            f"{quality['conflicted_uuids']}；无效样本：{quality['invalid_samples']}。",
            "",
            "下表按北京时间的样本开始日期统计；不是接收日期。",
            "",
            "| 指标 | 类型 | 单位 | 样本数 | 最小 | 最大 | 平均 |",
            "|---|---|---|---:|---:|---:|---:|",
        ]
        for row in metrics:
            cells = [
                "—" if v is None else (f"{v:.2f}" if isinstance(v, float) else str(v)) for v in row
            ]
            markdown.append("| " + " | ".join(v.replace("|", "\\|") for v in cells) + " |")
        markdown.extend(
            [
                "",
                "这是样本描述统计，不是 Apple 健康的跨设备合并总量。"
                "分类和运动只统计数量；不对分类代码求均值。",
                "",
            ]
        )
        markdown_path = export / "report.md"
        markdown_path.write_text("\n".join(markdown), encoding="utf-8")
        report_info = put_file(
            bucket, f"{ROOT}/reports/date={report_day}/{generation}.md", markdown_path
        )
        print("Publishing changed columnar checkpoint partitions.", flush=True)
        if previous and previous["state"].get("format") == "parquet-v1" and not new_objects:
            state = previous["state"]
        else:
            state = publish_state(db, bucket, export, previous.get("state") if previous else None)
        db.execute("CHECKPOINT")
        db.close()
        manifest = {
            "schema_version": 1,
            "parser_version": PARSER_VERSION,
            "generation": generation,
            "generated_at": now.isoformat(),
            "files": files,
            "state": state,
            "report": report_info,
            "report_json": report_json,
            "quality": quality,
        }
        manifest_path = export / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        put_file(bucket, f"{ROOT}/manifests/{generation}.json", manifest_path)
        put_file(bucket, f"{ROOT}/manifests/latest.json", manifest_path, immutable=False)
        # Only our rebuildable checkpoints; retain current + previous, never delete raw/Parquet.
        keep_states = {item["key"] for item in state_files(state)}
        if previous and previous.get("state"):
            keep_states.update(item["key"] for item in state_files(previous["state"]))
        for item in oss2.ObjectIteratorV2(bucket, prefix=f"{ROOT}/state/"):
            managed = item.key.endswith(".duckdb.gz") or (
                item.key.endswith(".parquet")
                and any(
                    item.key.startswith(f"{ROOT}/state/{table}/")
                    for table in ("events", "deletion_events", "ingest_objects")
                )
            )
            if managed and item.key not in keep_states:
                bucket.delete_object(item.key)
        print(
            "Private Parquet snapshot and daily report published; manifest committed.", flush=True
        )


def pull(args):
    bucket = connect_bucket(args.credentials_csv)
    manifest = load_latest(bucket)
    if manifest is None:
        raise ValueError("No published snapshot yet")
    work = args.directory.resolve()
    work.mkdir(parents=True, exist_ok=True)
    paths = []
    for item in manifest["files"]:
        path = work / "parquet" / ("dt=" + item["dt"]) / Path(item["key"]).name
        download_checked(bucket, item, path)
        paths.append(str(path).replace("\\", "/"))
    report_path = work / "reports" / (manifest["generation"] + ".md")
    download_checked(bucket, manifest["report"], report_path)
    db = duckdb.connect(str(work / "analysis.duckdb"))
    db.execute("BEGIN")
    if paths:
        literals = ",".join("'" + p.replace("'", "''") + "'" for p in paths)
        db.execute(
            "CREATE OR REPLACE VIEW samples AS SELECT * FROM "
            f"read_parquet([{literals}], hive_partitioning=true)"
        )
    else:
        db.execute(
            "CREATE OR REPLACE VIEW samples AS SELECT "
            "NULL::VARCHAR AS sample_uuid, NULL::VARCHAR AS version_hash, "
            "NULL::VARCHAR AS metric_type, NULL::VARCHAR AS device_id, "
            "NULL::BIGINT AS start_ms, NULL::BIGINT AS end_ms, NULL::DATE AS dt, "
            "NULL::VARCHAR AS kind, NULL::DOUBLE AS value, NULL::VARCHAR AS unit, "
            "NULL::VARCHAR AS category, NULL::VARCHAR AS source_name, "
            "NULL::VARCHAR AS source_bundle, NULL::VARCHAR AS source_product, "
            "NULL::JSON AS payload, NULL::VARCHAR AS first_object WHERE false"
        )
    db.execute(
        "CREATE OR REPLACE VIEW health_metrics AS SELECT * FROM samples "
        "WHERE kind IN ('quantity','category')"
    )
    db.execute("CREATE OR REPLACE VIEW workouts AS SELECT * FROM samples WHERE kind='workout'")
    db.execute("COMMIT")
    db.close()
    (work / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print("Verified snapshot downloaded; analysis.duckdb is ready.")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["build", "pull"])
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--credentials-csv", type=Path)
    args = parser.parse_args()
    try:
        (build if args.command == "build" else pull)(args)
    except Exception as exc:
        # Public CI must not dump raw records, SQL parameters, credentials or health statistics.
        raise SystemExit(
            f"Offline job failed ({type(exc).__name__}); inspect private inputs/configuration."
        ) from None


if __name__ == "__main__":
    main()

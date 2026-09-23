"""Immutable raw objects and a bounded, crash-recoverable local outbox."""

import gzip
import hashlib
import json
import os
import shutil
import time
from pathlib import Path

import oss2
from filelock import FileLock

from health_webhook import oss_auth
from health_webhook.config import settings


class OutboxFullError(Exception):
    pass


def inspect_payload(raw: bytes) -> dict[str, int]:
    data = json.loads(raw)
    if not isinstance(data, dict) or not isinstance(data.get("batches", []), list):
        raise ValueError("invalid envelope")
    rows = samples = deleted = 0
    for batch in data.get("batches", []):
        if not isinstance(batch, dict):
            raise ValueError("invalid batch")
        items = batch.get("samples", [])
        tombstones = batch.get("deletedUuids", [])
        if not isinstance(items, list) or not isinstance(tombstones, list):
            raise ValueError("invalid samples or deletions")
        for sample in items:
            if not isinstance(sample, dict):
                raise ValueError("invalid sample")
            samples += 1
            rows += int(sample.get("quantity") is not None or sample.get("category") is not None)
        if any(not isinstance(u, str) for u in tombstones):
            raise ValueError("invalid deleted UUID")
        deleted += len(tombstones)
    return {"total_rows": rows, "sample_count": samples, "deleted_count": deleted}


def bucket():
    creds = oss_auth.get_credentials()
    auth = (
        oss2.StsAuth(creds.ak, creds.sk, creds.token)
        if creds.token
        else oss2.Auth(creds.ak, creds.sk)
    )
    endpoint = "https://" + settings.oss_endpoint.removeprefix("https://").removeprefix("http://")
    return oss2.Bucket(
        auth, endpoint, settings.bucket_name, connect_timeout=settings.oss_timeout_seconds
    )


def key_for(digest: str) -> str:
    return f"{settings.raw_prefix.rstrip('/')}/{digest[:2]}/{digest}.json.gz"


def sync_dir(path: Path) -> None:
    if os.name != "nt":
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def atomic_write(path: Path, data: bytes) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("wb") as out:
        out.write(data)
        out.flush()
        os.fsync(out.fileno())
    os.replace(temporary, path)
    sync_dir(path.parent)


def warmup() -> None:
    settings.outbox_path.mkdir(parents=True, exist_ok=True)


def stage(raw: bytes, *, source: str = "webhook") -> str:
    warmup()
    digest = hashlib.sha256(raw).hexdigest()
    folder = settings.outbox_path
    with FileLock(str(folder / "queue.lock"), timeout=3):
        path = folder / f"{digest}.json.gz"
        if path.exists():
            return digest
        compressed = gzip.compress(raw, compresslevel=1, mtime=0)
        used = sum(p.stat().st_size for p in folder.iterdir() if p.is_file())
        if used + len(compressed) + 4096 > settings.outbox_max_bytes:
            raise OutboxFullError()
        if shutil.disk_usage(folder).free < settings.min_disk_free_bytes + len(compressed) + 4096:
            raise OutboxFullError()
        meta = {
            "sha256": digest,
            "raw_bytes": len(raw),
            "source": source,
            "received_at_ms": int(time.time() * 1000) if source == "webhook" else None,
        }
        atomic_write(folder / f"{digest}.meta", json.dumps(meta).encode())
        atomic_write(path, compressed)
    return digest


def upload_staged(digest: str, target=None) -> None:
    folder = settings.outbox_path
    with FileLock(str(folder / "queue.lock"), timeout=3):
        path = folder / f"{digest}.json.gz"
        if not path.exists():
            content = meta = None
        else:
            content = path.read_bytes()
            meta = json.loads((folder / f"{digest}.meta").read_text())
    target = target or bucket()
    key = key_for(digest)
    if content is not None:
        if meta is None:
            raise ValueError("missing outbox metadata")
        if hashlib.sha256(gzip.decompress(content)).hexdigest() != digest:
            raise ValueError("outbox checksum mismatch")
        headers = {
            "x-oss-forbid-overwrite": "true",
            "Content-Type": "application/gzip",
            "Content-MD5": oss2.utils.content_md5(content),
            "x-oss-meta-sha256": digest,
            "x-oss-meta-raw-bytes": str(meta["raw_bytes"]),
            "x-oss-meta-source": meta["source"],
        }
        if meta["received_at_ms"] is not None:
            headers["x-oss-meta-received-at-ms"] = str(meta["received_at_ms"])
        try:
            target.put_object(key, content, headers=headers)
        except oss2.exceptions.ServerError as exc:
            if exc.status != 409 or exc.code != "FileAlreadyExists":
                raise
            result = target.head_object(key)
            if result.headers.get("x-oss-meta-sha256") != digest or result.headers.get(
                "x-oss-meta-raw-bytes"
            ) != str(meta["raw_bytes"]):
                raise ValueError("existing OSS object does not match receipt") from None
    else:
        result = target.head_object(key)
        if result.headers.get("x-oss-meta-sha256") != digest:
            raise ValueError("missing or mismatched remote receipt")
    with FileLock(str(folder / "queue.lock"), timeout=3):
        path.unlink(missing_ok=True)
        (folder / f"{digest}.meta").unlink(missing_ok=True)
        sync_dir(folder)


def save(raw: bytes) -> str:
    digest = stage(raw)
    upload_staged(digest)
    return digest


def main() -> None:
    warmup()
    delay = 5
    while True:
        failed = False
        for path in settings.outbox_path.glob("*.json.gz"):
            try:
                upload_staged(path.name.removesuffix(".json.gz"))
            except Exception as exc:
                print(f"outbox retry deferred: {type(exc).__name__}", flush=True)
                failed = True
                break
        delay = min(delay * 2, 300) if failed else 5
        time.sleep(delay)


if __name__ == "__main__":
    main()

"""Incremental private OSS bridge for the existing IoT scale's `me` projection.

The IoT database stays authoritative. Reclassification/correction creates deletion
events and fresh UUID revisions, including when an earlier reading is restored.
"""

import hashlib
import json
import math
import urllib.request
import uuid
from pathlib import Path

from filelock import FileLock

from health_webhook.config import settings
from health_webhook.raw_store import atomic_write, stage, upload_staged

METRIC = "HKQuantityTypeIdentifierBodyMass"
SOURCE = "site.bupt.iot.scale.me"


def fingerprint(row):
    measurement = {
        "source_id": row["id"],
        "ts_ms": round(row["ts"] * 1000),
        "kg": row["kg"],
        "assignment": "confirmed" if row.get("confirmed_person") == "me" else "inferred",
    }
    packed = json.dumps(measurement, sort_keys=True, separators=(",", ":"))
    return measurement, hashlib.sha256(packed.encode()).hexdigest()


def project(snapshot, old):
    if snapshot.get("source") != "real" or not isinstance(snapshot.get("weights"), list):
        raise ValueError("invalid IoT snapshot")
    current = {}
    for row in snapshot["weights"]:
        if row.get("person") != "me":
            continue
        if not row.get("stable") or row.get("removed") or row.get("reason"):
            raise ValueError("invalid accepted weight")
        if not isinstance(row.get("id"), str) or not row["id"]:
            raise ValueError("missing weight identity")
        if any(
            type(row.get(k)) not in (int, float) or not math.isfinite(row[k]) for k in ("ts", "kg")
        ) or not (0 < row["kg"] < 500 and 0 < row["ts"] < 4102444800):
            raise ValueError("invalid weight value")
        if row["id"] in current:
            raise ValueError("duplicate weight identity")
        current[row["id"]] = fingerprint(row)
    state = {key: dict(value) for key, value in old.items()}
    additions, deletions = [], []
    for source_id, previous in sorted(old.items()):
        if previous["active"] and source_id not in current:
            deletions.append(previous["uuid"])
            state[source_id]["active"] = False
    for source_id, (measurement, digest) in sorted(current.items()):
        previous = old.get(source_id)
        if previous and previous["active"] and previous["fingerprint"] == digest:
            continue
        revision = previous["revision"] + 1 if previous else 1
        if previous and previous["active"]:
            deletions.append(previous["uuid"])
        uid = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{SOURCE}:{source_id}:{digest}:{revision}"))
        state[source_id] = {
            "fingerprint": digest,
            "uuid": uid,
            "revision": revision,
            "active": True,
        }
        additions.append(
            {
                "uuid": uid,
                "startUnixMs": measurement["ts_ms"],
                "endUnixMs": measurement["ts_ms"],
                "quantity": {"value": measurement["kg"], "unit": "kg"},
                "source": {"name": "IoT scale (me)", "bundleId": SOURCE, "productType": "IoTScale"},
                "iot": {
                    "sourceId": source_id,
                    "person": "me",
                    "assignment": measurement["assignment"],
                    "revision": revision,
                },
            }
        )
    body = {
        "deviceId": "iot-scale-me",
        "batches": [{"hkTypeId": METRIC, "samples": additions, "deletedUuids": sorted(deletions)}],
    }
    return state, body, bool(additions or deletions)


def publish(raw):
    digest = stage(raw, source="iot-weight")
    upload_staged(digest)


def sync_snapshot(snapshot, path: Path, writer=publish):
    pending = path.with_suffix(".pending.json")
    if pending.exists():
        # Finish an older publication before diffing a newer IoT snapshot. Without
        # this journal, a failed upload followed by a correction can leave both
        # versions active when the shared outbox eventually retries the old one.
        job = json.loads(pending.read_text(encoding="utf-8"))
        writer(job["raw"].encode())
        atomic_write(path, json.dumps(job["cursor"]).encode())
        pending.unlink()
    old = json.loads(path.read_text(encoding="utf-8"))["measurements"] if path.exists() else {}
    state, body, changed = project(snapshot, old)
    if changed:
        raw = json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
        cursor = {"schema": 1, "measurements": state}
        atomic_write(pending, json.dumps({"raw": raw.decode(), "cursor": cursor}).encode())
        writer(raw)
        # Persist the cursor only after immutable OSS confirmation. Retry is idempotent.
        atomic_write(path, json.dumps(cursor).encode())
        pending.unlink()
    return changed


def main():
    folder = Path(settings.data_dir) / "iot-weight"
    folder.mkdir(parents=True, exist_ok=True)
    try:
        with FileLock(str(folder / "sync.lock"), timeout=1):
            # Fixed loopback origin, no browser/session cookie or partner data sent to OSS.
            with urllib.request.urlopen("http://127.0.0.1:8802/api/dashboard", timeout=20) as resp:
                raw = resp.read(8_000_001)
            if len(raw) > 8_000_000:
                raise ValueError("IoT snapshot exceeds budget")
            sync_snapshot(json.loads(raw), folder / "cursor.json")
        print("Private IoT weight archive synchronized.")
    except Exception as exc:
        raise SystemExit(
            f"IoT weight sync failed ({type(exc).__name__}); retry scheduled."
        ) from None


if __name__ == "__main__":
    main()

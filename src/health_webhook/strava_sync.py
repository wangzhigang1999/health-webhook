"""Bounded personal Strava pull; credentials stay on the host, data in private OSS.

Full catalog reconciliation detects edits/deletions. Details and streams are
backfilled newest first across runs, with a durable publication journal.
"""

import argparse
import hashlib
import json
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime
from pathlib import Path

from filelock import FileLock

from health_webhook.config import settings
from health_webhook.raw_store import atomic_write, stage, upload_staged

SOURCE = "com.strava.api.personal"
METRIC = "HKWorkoutTypeIdentifier"
STREAMS = "time,distance,heartrate,watts,cadence,velocity_smooth,altitude,moving,grade_smooth"


def packed(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def fingerprint(summary):
    # Social counts change independently of the activity's health data.
    ignored = {"kudos_count", "comment_count", "athlete_count", "photo_count", "total_photo_count"}
    return hashlib.sha256(
        packed({k: v for k, v in summary.items() if k not in ignored})
    ).hexdigest()


class DeferredError(Exception):
    """A later timer run can resume without losing progress."""


class Client:
    def __init__(self, token_path: Path, budget: int = 30):
        self.path = token_path
        self.token = json.loads(token_path.read_text())
        self.remaining = budget
        self.blocked = False

    def refresh(self):
        if self.token["expires_at"] > time.time() + 120:
            return
        data = urllib.parse.urlencode(
            {k: self.token[k] for k in ("client_id", "client_secret", "refresh_token")}
            | {"grant_type": "refresh_token"}
        ).encode()
        request = urllib.request.Request("https://www.strava.com/oauth/token", data=data)
        with urllib.request.urlopen(request, timeout=30) as response:
            self.token.update(json.load(response))
        atomic_write(self.path, packed(self.token))

    def get(self, path, *, absent_ok=False):
        if self.remaining <= 0 or self.blocked:
            raise DeferredError("request budget reached")
        self.refresh()
        self.remaining -= 1
        request = urllib.request.Request(
            "https://www.strava.com/api/v3/" + path,
            headers={"Authorization": "Bearer " + self.token["access_token"]},
        )
        try:
            with urllib.request.urlopen(request, timeout=40) as response:
                for prefix in ("X-ReadRateLimit", "X-RateLimit"):
                    limits = response.headers.get(prefix + "-Limit", "100,1000")
                    usage = response.headers.get(prefix + "-Usage", "0,0")
                    self.blocked |= any(
                        int(used) >= int(limit) * 0.8
                        for used, limit in zip(usage.split(","), limits.split(","), strict=True)
                    )
                return json.load(response)
        except urllib.error.HTTPError as exc:
            if exc.code == 429:
                raise DeferredError("remote rate limit") from None
            if exc.code == 404 and absent_ok:
                return None
            raise


def envelope(detail, streams, revision, old_uuid=None):
    activity_id = str(detail["id"])
    uid = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{SOURCE}:{activity_id}:{revision}"))
    start = round(
        datetime.fromisoformat(detail["start_date"].replace("Z", "+00:00")).timestamp() * 1000
    )
    sport = detail.get("sport_type", detail.get("type", "Other"))
    activity_type = {"Ride": 13, "VirtualRide": 13, "Run": 37, "Walk": 52}.get(sport, 3000)
    sample = {
        "uuid": uid,
        "startUnixMs": start,
        "endUnixMs": start + round(detail["elapsed_time"] * 1000),
        "source": {"name": "Strava API", "bundleId": SOURCE},
        "workout": {
            "activityType": activity_type,
            "durationSeconds": detail["elapsed_time"],
            "movingTimeSeconds": detail["moving_time"],
            "totalDistanceM": detail.get("distance"),
            "totalEnergyKcal": detail.get("calories"),
            "isIndoor": sport.startswith("Virtual") or detail.get("trainer", False),
        },
        "strava": {
            "activityId": activity_id,
            "revision": revision,
            "detail": detail,
            "streams": streams,
            "powerSource": "device" if detail.get("device_watts") else "estimated_or_unknown",
        },
    }
    return uid, {
        "deviceId": "strava-api",
        "batches": [
            {
                "hkTypeId": METRIC,
                "samples": [sample],
                "deletedUuids": [old_uuid] if old_uuid else [],
            }
        ],
    }


def publish(raw):
    upload_staged(stage(raw, source="strava-api"))


def resume(folder, writer=publish):
    pending = folder / "pending.json"
    if pending.exists():
        job = json.loads(pending.read_text())
        writer(packed(job["body"]))
        atomic_write(folder / "cursor.json", packed(job["state"]))
        pending.unlink()


def commit(folder, state, body, writer=publish):
    atomic_write(folder / "pending.json", packed({"state": state, "body": body}))
    resume(folder, writer)


def sync(folder: Path, client, writer=publish):
    resume(folder, writer)
    path = folder / "cursor.json"
    state = json.loads(path.read_text()) if path.exists() else {"activities": {}}
    catalog = {}
    # No deletion is inferred from a partial/failed listing. Freeze the upper
    # timestamp so a newly uploaded activity cannot shift page boundaries.
    before = int(time.time()) + 1
    page = 1
    while True:
        rows = client.get(f"athlete/activities?per_page=200&page={page}&before={before}")
        if not isinstance(rows, list):
            raise ValueError("invalid activity catalog")
        for row in rows:
            catalog[str(row["id"])] = row
        if len(rows) < 200:
            break
        page += 1
    activities = state["activities"]
    removed = [key for key, old in activities.items() if old["active"] and key not in catalog]
    # Confirm missing IDs individually, avoiding destructive pagination races.
    for key in removed:
        if client.get(f"activities/{key}", absent_ok=True) is not None:
            continue
        old = activities[key]
        old["active"] = False
        commit(
            folder,
            state,
            {
                "deviceId": "strava-api",
                "batches": [{"hkTypeId": METRIC, "samples": [], "deletedUuids": [old["uuid"]]}],
            },
            writer,
        )
    imported = 0
    for key, row in catalog.items():
        digest = fingerprint(row)
        old = activities.get(key)
        start = datetime.fromisoformat(row["start_date"].replace("Z", "+00:00")).timestamp()
        refresh_recent = (
            old
            and start > time.time() - 7 * 86400
            and time.time() - old.get("fetched_at", 0) > 86400
        )
        if old and old["active"] and old["fingerprint"] == digest and not refresh_recent:
            continue
        detail = client.get(f"activities/{key}", absent_ok=True)
        if detail is None:
            continue
        streams = client.get(
            f"activities/{key}/streams?keys={STREAMS}&key_by_type=true", absent_ok=True
        )
        revision = old["revision"] + 1 if old else 1
        uid, body = envelope(
            detail, streams or {}, revision, old["uuid"] if old and old["active"] else None
        )
        activities[key] = {
            "uuid": uid,
            "revision": revision,
            "fingerprint": digest,
            "active": True,
            "fetched_at": int(time.time()),
        }
        commit(folder, state, body, writer)
        imported += 1
    return {
        "catalog": len(catalog),
        "imported": imported,
        "archived": sum(v["active"] for v in activities.values()),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, default=Path(settings.data_dir) / "strava")
    parser.add_argument("--requests", type=int, default=30)
    args = parser.parse_args()
    args.directory.mkdir(parents=True, exist_ok=True)
    try:
        with FileLock(str(args.directory / "sync.lock"), timeout=1):
            result = sync(args.directory, Client(args.directory / "token.json", args.requests))
        print(json.dumps(result))
    except DeferredError:
        print("Strava request budget reached; saved progress will resume on the next timer run.")
    except Exception as exc:
        # Exception text/HTTP response may contain credentials or personal data.
        raise SystemExit(f"Strava sync failed ({type(exc).__name__}); progress retained.") from None


if __name__ == "__main__":
    main()

"""Archive legacy JSONL at a fixed byte boundary with resumable receipts."""

import argparse
import hashlib
import json
import sqlite3
from pathlib import Path

from health_webhook.config import settings
from health_webhook.raw_store import stage, upload_staged


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, default=settings.jsonl_path)
    args = parser.parse_args()
    source = args.source.resolve()
    boundary = source.stat().st_size
    db = sqlite3.connect(str(source) + ".oss-migration.sqlite3")
    db.execute("CREATE TABLE IF NOT EXISTS receipts (digest TEXT PRIMARY KEY)")
    digest_all = hashlib.sha256()
    lines = uploaded = 0
    with source.open("rb") as stream:
        while stream.tell() < boundary:
            line = stream.readline(boundary - stream.tell())
            digest_all.update(line)
            raw = line.rstrip(b"\r\n")
            if not raw:
                continue
            json.loads(raw)
            lines += 1
            digest = hashlib.sha256(raw).hexdigest()
            if db.execute("SELECT 1 FROM receipts WHERE digest=?", (digest,)).fetchone():
                continue
            upload_staged(stage(raw, source="legacy-jsonl"))
            db.execute("INSERT INTO receipts VALUES (?)", (digest,))
            db.commit()
            uploaded += 1
            if uploaded % 100 == 0:
                print(json.dumps({"lines": lines, "new_objects": uploaded}), flush=True)
    report = {
        "source_bytes": boundary,
        "source_sha256": digest_all.hexdigest(),
        "lines": lines,
        "new_objects": uploaded,
        "receipts": db.execute("SELECT count(*) FROM receipts").fetchone()[0],
    }
    Path(str(source) + ".oss-migration-report.json").write_text(json.dumps(report, indent=2))
    db.close()
    print(json.dumps(report), flush=True)


if __name__ == "__main__":
    main()

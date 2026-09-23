"""Compatible upload endpoint; acknowledge only after durable OSS storage."""

import hmac
import logging
import threading

from flask import Flask, jsonify, request
from werkzeug.exceptions import RequestEntityTooLarge

from health_webhook.config import settings
from health_webhook.raw_store import OutboxFullError, inspect_payload, save, warmup

app = Flask(__name__, static_folder=None)
app.config["MAX_CONTENT_LENGTH"] = settings.max_body_bytes
_slots = threading.BoundedSemaphore(2)
log = logging.getLogger(__name__)


@app.get("/")
@app.get("/health")
@app.get("/healthz")
def health():
    return jsonify(ok=True, service="health-webhook-oss", storage="oss")


@app.errorhandler(RequestEntityTooLarge)
def too_large(_exc):
    return jsonify(ok=False, error="payload too large"), 413


@app.post("/")
@app.post("/ingest")
def ingest():
    expected = f"Bearer {settings.auth_token}"
    if settings.auth_token and not hmac.compare_digest(
        request.headers.get("Authorization", "").encode(), expected.encode()
    ):
        return jsonify(ok=False, error="unauthorized"), 401
    if not _slots.acquire(blocking=False):
        return jsonify(ok=False, error="busy; retry later"), 503, {"Retry-After": "5"}
    try:
        raw = request.get_data(cache=False)
        try:
            stats = inspect_payload(raw)
        except (ValueError, TypeError, UnicodeError):
            return jsonify(ok=False, error="invalid json"), 400
        try:
            receipt = save(raw)
        except (OutboxFullError, OSError):
            return jsonify(ok=False, error="local storage unavailable; retry later"), 503
        except Exception as exc:
            log.warning("OSS upload failed: %s", type(exc).__name__)
            return (
                jsonify(ok=False, error="OSS unavailable; retry later"),
                503,
                {"Retry-After": "10"},
            )
        return jsonify(ok=True, jsonl_saved=True, oss_saved=True, receipt_id=receipt, **stats)
    finally:
        _slots.release()


def main() -> None:
    from waitress import serve

    warmup()
    logging.basicConfig(level=logging.INFO)
    print(
        f"health-webhook OSS serving on {settings.listen_host}:{settings.listen_port}", flush=True
    )
    serve(
        app,
        host=settings.listen_host,
        port=settings.listen_port,
        threads=4,
        connection_limit=100,
        max_request_body_size=settings.max_body_bytes,
    )

"""Flask 入口：health.bupt.site -> JSONL + Paimon 双写。"""

from flask import Flask, jsonify, request
from pydantic import ValidationError

from health_webhook import store
from health_webhook.config import settings
from health_webhook.models import HealthPayload

app = Flask(__name__)


@app.get("/")
@app.get("/health")
@app.get("/healthz")
def health():
    return jsonify(ok=True, service="health-webhook-paimon", warehouse=settings.oss_warehouse)


@app.post("/")
@app.post("/ingest")
def ingest():
    if settings.auth_token and request.headers.get("Authorization", "") != (
        f"Bearer {settings.auth_token}"
    ):
        return jsonify(ok=False, error="unauthorized"), 401

    if (request.content_length or 0) > settings.max_body_bytes:
        return jsonify(ok=False, error="payload too large"), 413

    raw = request.get_data(cache=False)
    if len(raw) > settings.max_body_bytes:
        return jsonify(ok=False, error="payload too large"), 413

    try:
        payload = HealthPayload.model_validate_json(raw)
    except ValidationError:
        return jsonify(ok=False, error="invalid json"), 400

    try:
        stats = store.save(raw.decode("utf-8", "replace"), payload)
    except Exception as exc:  # JSONL 已落盘，仅 Paimon 失败
        return jsonify(ok=False, error=f"paimon write failed: {exc}"), 502

    return jsonify({"ok": True, **stats})


def main() -> None:
    from waitress import serve

    store.warmup()
    print(
        f"health-webhook serving on {settings.listen_host}:{settings.listen_port} "
        f"-> {settings.oss_warehouse}",
        flush=True,
    )
    serve(app, host=settings.listen_host, port=settings.listen_port)

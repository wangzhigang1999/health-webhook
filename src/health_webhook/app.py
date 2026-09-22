"""Flask 入口：health.bupt.site -> 健康看板 + JSONL/Paimon 双写。"""

from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory
from pydantic import ValidationError

from health_webhook import dashboard, store
from health_webhook.config import settings
from health_webhook.models import HealthPayload

WEB_DIR = Path(__file__).parent / "web"
app = Flask(__name__, static_folder=str(WEB_DIR), static_url_path="/static")


@app.get("/")
def index():
    return send_from_directory(WEB_DIR, "index.html")


@app.get("/health")
@app.get("/healthz")
def health():
    return jsonify(ok=True, service="health-webhook-paimon", warehouse=settings.oss_warehouse)


@app.get("/api/dashboard")
def api_dashboard():
    return jsonify(dashboard.dashboard_data())


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

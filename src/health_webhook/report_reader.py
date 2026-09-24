"""Read private, committed reports behind the existing portal authentication."""

import hashlib
import hmac
import html
import json
import re
import threading
import time
from urllib.parse import urlencode

from flask import Blueprint, Response, request

from health_webhook.config import settings
from health_webhook.raw_store import bucket

reader = Blueprint("reports", __name__)
_lock = threading.Lock()
_cached_manifest = None
_cached_until = 0.0
ROOT = "health/v2"


def read_bounded(target, key, limit):
    result = target.get_object(key)
    try:
        data = result.read(limit + 1)
    finally:
        result.close()
    if not isinstance(data, bytes):
        raise ValueError("expected binary OSS response")
    if len(data) > limit:
        raise ValueError("object exceeds reader budget")
    return data


def manifest(target):
    global _cached_manifest, _cached_until
    with _lock:
        if _cached_manifest is None or time.monotonic() >= _cached_until:
            fresh = json.loads(read_bounded(target, f"{ROOT}/manifests/latest.json", 4_000_000))
            _cached_manifest = fresh
            _cached_until = time.monotonic() + 15
        return _cached_manifest


def entries(doc):
    records = {item["date"]: item for item in doc.get("report_history", [])}
    latest = doc["report"]
    match = re.fullmatch(r"health/v2/reports/date=(\d{4}-\d{2}-\d{2})/[^/]+\.html", latest["key"])
    if match:
        records[match[1]] = {**latest, "date": match[1]}
    return records


def response(body, status=200):
    return Response(
        body,
        status=status,
        content_type="text/html; charset=utf-8",
        headers={
            "Cache-Control": "private, no-store",
            "X-Content-Type-Options": "nosniff",
            "X-Frame-Options": "DENY",
            "Referrer-Policy": "no-referrer",
            "X-Robots-Tag": "noindex, nofollow, noarchive",
        },
    )


@reader.get("/reports")
@reader.get("/reports/")
def report_page():
    # Caddy injects this only after forward_auth succeeds. Direct backend access
    # and arbitrary user identity headers must never grant access.
    key = settings.report_access_key
    supplied = request.headers.get("X-Health-Report-Key", "")
    if not key or not hmac.compare_digest(supplied.encode(), key.encode()):
        return response("请先登录个人空间。", 401)
    day = request.args.get("date")
    if day and not re.fullmatch(r"\d{4}-\d{2}-\d{2}", day):
        return response("日期格式应为 YYYY-MM-DD。", 400)
    try:
        target = bucket()
        doc = manifest(target)
        records = entries(doc)
        if not records:
            return response("日报尚未生成，请稍后再来。", 404)
        selected = day or max(records)
        item = records.get(selected)
        if not item:
            return response("这一天暂无日报。<a href='/reports'>查看最新日报</a>", 404)
        if not re.fullmatch(
            rf"health/v2/reports/date={re.escape(selected)}/[^/]+\.html", item["key"]
        ):
            raise ValueError("report outside permitted namespace")
        raw = read_bounded(target, item["key"], 3_000_000)
        if hashlib.sha256(raw).hexdigest() != item["sha256"]:
            raise ValueError("report digest mismatch")
        body = raw.decode("utf-8")
        options = "".join(
            f'<option value="{html.escape(d)}" {"selected" if d == selected else ""}>'
            f"{html.escape(d)}</option>"
            for d in sorted(records, reverse=True)
        )
        link = "/reports?" + urlencode({"date": selected})
        nav = (
            '<nav aria-label="日报导航" style="max-width:940px;margin:18px auto;'
            'padding:0 20px;display:flex;gap:16px;align-items:center;flex-wrap:wrap">'
            '<a href="/reports">最新日报</a><form action="/reports" method="get">'
            '<label>历史日期 <select name="date" style="padding:5px">'
            + options
            + '</select></label> <button type="submit" style="display:inline">查看</button>'
            f'</form><a href="{link}">刷新</a>'
            '<span style="font-size:12px">每天约 12:00 更新：'
            "昨晚睡眠＋昨日活动 · 北京时间</span></nav>"
        )
        return response(body.replace("<body>", "<body>" + nav, 1))
    except Exception:
        # Do not expose OSS paths, credentials, records or upstream exception text.
        return response("暂时无法读取日报，请稍后刷新。", 503)

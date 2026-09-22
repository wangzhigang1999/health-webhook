"""ECS 实例 RAM 角色 STS 凭证（免密），带自动刷新。"""

import json
import time
import urllib.request
from dataclasses import dataclass
from datetime import datetime

from health_webhook.config import settings


@dataclass
class Credentials:
    ak: str
    sk: str
    token: str
    expires: float


_sts: Credentials | None = None


def _fetch() -> Credentials:
    with urllib.request.urlopen(settings.oss_metadata_url, timeout=15) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    expires = datetime.fromisoformat(data["Expiration"].replace("Z", "+00:00")).timestamp()
    return Credentials(
        ak=data["AccessKeyId"],
        sk=data["AccessKeySecret"],
        token=data["SecurityToken"],
        expires=expires,
    )


def get_credentials() -> Credentials:
    """显式 env 凭证优先；否则走 ECS RAM 角色，临期前 10 分钟自动刷新。"""
    if settings.oss_access_key_id:
        return Credentials(
            ak=settings.oss_access_key_id,
            sk=settings.oss_access_key_secret,
            token=settings.oss_access_key_token,
            expires=float("inf"),
        )
    global _sts
    if _sts is None or time.time() > _sts.expires - 600:
        _sts = _fetch()
    return _sts

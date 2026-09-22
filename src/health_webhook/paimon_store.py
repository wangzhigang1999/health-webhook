"""Paimon 表写入封装（OSS 仓库，内网端点）。"""

from typing import Any

import pyarrow as pa
from pypaimon import CatalogFactory, Schema

from health_webhook import oss_auth
from health_webhook.config import settings
from health_webhook.models import MetricRow

_catalog: Any | None = None
_catalog_token: str | None = None

METRICS_SCHEMA = pa.schema(
    [
        ("sample_uuid", pa.string()),
        ("device_id", pa.string()),
        ("metric_type", pa.string()),
        ("value", pa.float64()),
        ("unit", pa.string()),
        ("category", pa.string()),
        ("source_name", pa.string()),
        ("start_time", pa.timestamp("ms")),
        ("end_time", pa.timestamp("ms")),
        ("received_at", pa.timestamp("ms")),
        ("dt", pa.string()),
    ]
)


def _options(creds: oss_auth.Credentials) -> dict[str, str]:
    return {
        "warehouse": settings.oss_warehouse,
        "fs.oss.endpoint": settings.oss_endpoint,
        "fs.oss.region": settings.oss_region,
        "fs.oss.accessKeyId": creds.ak,
        "fs.oss.accessKeySecret": creds.sk,
        "fs.oss.securityToken": creds.token,
    }


def _get_catalog() -> Any:
    """STS 刷新后（token 变化）重建 catalog，保证凭证不过期。"""
    global _catalog, _catalog_token
    creds = oss_auth.get_credentials()
    if _catalog is None or _catalog_token != creds.token:
        _catalog = CatalogFactory.create(_options(creds))
        _catalog.create_database(settings.database, ignore_if_exists=True)
        _catalog_token = creds.token
    return _catalog


def _get_table() -> Any:
    ident = f"{settings.database}.{settings.table_metrics}"
    schema = Schema.from_pyarrow_schema(
        pa_schema=METRICS_SCHEMA,
        partition_keys=["dt"],
        options={"bucket": "2"},
        comment="health metrics (long table)",
    )
    cat = _get_catalog()
    cat.create_table(ident, schema=schema, ignore_if_exists=True)
    return cat.get_table(ident)


def write(rows: list[MetricRow]) -> int:
    """把打平后的行写入 Paimon，返回写入行数。调用方负责串行化。"""
    if not rows:
        return 0
    table = _get_table()
    write_builder = table.new_batch_write_builder()
    table_write = write_builder.new_write()
    table_commit = write_builder.new_commit()
    try:
        import pandas as pd

        df = pd.DataFrame([row.model_dump() for row in rows])
        for col in ("start_time", "end_time", "received_at"):
            df[col] = pd.to_datetime(df[col], unit="ms", utc=True)
        pa_table = pa.Table.from_pandas(df, schema=METRICS_SCHEMA, preserve_index=False)
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
    finally:
        table_write.close()
        table_commit.close()
    return len(rows)


def read_sample_uuids() -> set[str]:
    """读取 Paimon 表中已存在的 sample_uuid 集合。"""
    table = _get_table()
    read_builder = table.new_read_builder()
    table_scan = read_builder.new_scan()
    table_read = read_builder.new_read()
    splits = table_scan.plan().splits()
    result = table_read.to_arrow(splits)
    if result.num_rows == 0:
        return set()
    return {uuid for uuid in result.column("sample_uuid").to_pylist() if uuid is not None}


def warmup() -> None:
    """启动时预热：验证 OSS 连通并建表。"""
    _get_table()

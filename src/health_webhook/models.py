"""Apple HealthKit 数据的 Pydantic 模型。"""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


def _normalize_ms(value: Any) -> int | None:
    """把毫秒时间戳（int/str/float）归一化为 int。"""
    if value is None or value == "":
        return None
    return int(float(str(value)))


class _Model(BaseModel):
    """允许未知字段、支持按字段名或别名填充的基类。"""

    model_config = ConfigDict(extra="allow", populate_by_name=True)


class Source(_Model):
    name: str | None = None
    bundle_id: str | None = Field(default=None, alias="bundleId")
    product_type: str | None = Field(default=None, alias="productType")


class Quantity(_Model):
    value: float | None = None
    unit: str | None = None


class Category(_Model):
    value: int | None = None
    value_name: str | None = Field(default=None, alias="valueName")


class Sample(_Model):
    uuid: str | None = None
    start_unix_ms: int | None = Field(default=None, alias="startUnixMs")
    end_unix_ms: int | None = Field(default=None, alias="endUnixMs")
    source: Source | None = None
    quantity: Quantity | None = None
    category: Category | None = None
    route: dict[str, Any] | None = None

    @field_validator("start_unix_ms", "end_unix_ms", mode="before")
    @classmethod
    def _coerce_ms(cls, value: Any) -> int | None:
        return _normalize_ms(value)


class Batch(_Model):
    hk_type_id: str | None = Field(default=None, alias="hkTypeId")
    samples: list[Sample] = Field(default_factory=list)


class HealthPayload(_Model):
    schema_version: str | None = Field(default=None, alias="schemaVersion")
    batch_id: str | None = Field(default=None, alias="batchId")
    device_id: str | None = Field(default=None, alias="deviceId")
    sent_at_unix_ms: int | None = Field(default=None, alias="sentAtUnixMs")
    batches: list[Batch] = Field(default_factory=list)

    @field_validator("sent_at_unix_ms", mode="before")
    @classmethod
    def _coerce_ms(cls, value: Any) -> int | None:
        return _normalize_ms(value)


class MetricRow(BaseModel):
    """打平后的一行度量数据。"""

    model_config = ConfigDict(extra="allow")

    sample_uuid: str | None = None
    device_id: str | None = None
    metric_type: str | None = None
    value: float | None = None
    unit: str | None = None
    category: str | None = None
    source_name: str | None = None
    start_time: int | None = None
    end_time: int | None = None
    received_at: int
    dt: str

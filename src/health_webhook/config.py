"""应用配置，使用 pydantic-settings 从环境变量 / .env 加载。"""

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """全局配置。字段名与环境变量名大小写不敏感地一一对应。"""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # 认证
    auth_token: str = ""
    # Internal Caddy-to-reader capability; never reuse the phone upload token.
    report_access_key: str = ""

    # 有界待上传队列及历史 JSONL 迁移
    data_dir: str = "/data"
    max_body_bytes: int = 2 * 1024 * 1024

    # OSS（OSS_WAREHOUSE 仅兼容旧配置中的 bucket 名称）
    oss_warehouse: str = "oss://your-bucket/paimon/health"
    oss_endpoint: str = "oss-cn-beijing-internal.aliyuncs.com"
    oss_region: str = "cn-beijing"
    oss_bucket: str = ""
    raw_prefix: str = "health/v2/raw/default"
    outbox_max_bytes: int = 256 * 1024 * 1024
    min_disk_free_bytes: int = 256 * 1024 * 1024
    oss_timeout_seconds: float = 5
    oss_ram_role: str = ""
    oss_access_key_id: str = ""
    oss_access_key_secret: str = ""
    oss_access_key_token: str = ""

    # HTTP
    listen_host: str = "0.0.0.0"
    listen_port: int = 8080

    @property
    def jsonl_path(self) -> Path:
        return Path(self.data_dir) / "events.jsonl"

    @property
    def bucket_name(self) -> str:
        from urllib.parse import urlparse

        return self.oss_bucket or urlparse(self.oss_warehouse).netloc

    @property
    def outbox_path(self) -> Path:
        return Path(self.data_dir) / "outbox-v2"

    @property
    def oss_metadata_url(self) -> str:
        return (
            f"http://100.100.100.200/latest/meta-data/ram/security-credentials/{self.oss_ram_role}"
        )


settings = Settings()

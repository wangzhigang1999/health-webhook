"""应用配置，使用 pydantic-settings 从环境变量 / .env 加载。"""

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """全局配置。字段名与环境变量名大小写不敏感地一一对应。"""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # 认证
    auth_token: str = ""

    # JSONL 兜底落盘
    data_dir: str = "/data"
    max_body_bytes: int = 2 * 1024 * 1024

    # OSS / Paimon
    oss_warehouse: str = "oss://your-bucket/paimon/health"
    oss_endpoint: str = "oss-cn-beijing-internal.aliyuncs.com"
    oss_region: str = "cn-beijing"
    oss_ram_role: str = ""
    oss_access_key_id: str = ""
    oss_access_key_secret: str = ""
    oss_access_key_token: str = ""

    # Paimon 表
    database: str = "default"
    table_metrics: str = "health_metrics"

    # HTTP
    listen_host: str = "0.0.0.0"
    listen_port: int = 8080

    @property
    def jsonl_path(self) -> Path:
        return Path(self.data_dir) / "events.jsonl"

    @property
    def oss_metadata_url(self) -> str:
        return (
            f"http://100.100.100.200/latest/meta-data/ram/security-credentials/{self.oss_ram_role}"
        )


settings = Settings()

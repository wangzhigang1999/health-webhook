# health-webhook

把 Apple HealthKit 数据经 webhook 摄入后，**双写 JSONL + Apache Paimon（阿里云 OSS）** 的开源实现。

## 特性

- **双写兜底**：每条数据先落 JSONL（永不丢），再打平写入 Paimon 湖表
- **OSS 直写**：通过 [pypaimon](https://paimon.apache.org) 直写 OSS，无 Flink/Spark 常驻进程
- **内网端点 + 免密**：OSS 走 `*-internal` 内网域名（省公网流量），ECS 实例 RAM 角色自动拉取/刷新 STS 临时凭证
- **现代 Python 工程**：`pyproject.toml` + `src` 布局，pydantic / pydantic-settings，ruff + pyright + pytest
- **敏感信息外置**：密钥全部走 `.env`，`.env.example` 提供模板，`.gitignore` 保证不入库

## 架构

```
iPhone / Apple Watch
      │  HealthKit 批量 JSON
      ▼
health.bupt.site (Caddy :443, 自动 HTTPS)
      │  reverse_proxy
      ▼
Flask + waitress (本机 127.0.0.1:8081)
      ├─ 校验 Bearer Token
      ├─ JSONL 兜底 → events.jsonl
      └─ pydantic 解析 → 打平 → Paimon 直写
             └─ oss://<bucket>/paimon/health/default/health_metrics
```

## 快速开始

```bash
# 1. 安装依赖（uv 会自动创建 .venv 并装好运行时 + dev 依赖）
uv sync

# 2. 配置环境变量
cp .env.example .env
#    编辑 .env，填入 AUTH_TOKEN、OSS_WAREHOUSE、OSS_RAM_ROLE 等

# 3. 运行
uv run health-webhook
# 或：uv run python -m health_webhook
```

## 配置（.env）

| 变量 | 说明 | 默认 |
|------|------|------|
| `AUTH_TOKEN` | webhook 访问令牌（Bearer） | 空（不校验） |
| `OSS_WAREHOUSE` | Paimon 仓库 | `oss://your-bucket/paimon/health` |
| `OSS_RAM_ROLE` | ECS 实例 RAM 角色名（免密） | 空 |
| `OSS_ENDPOINT` | OSS 端点（内网） | `oss-cn-beijing-internal.aliyuncs.com` |
| `OSS_REGION` | OSS 区域 | `cn-beijing` |
| `OSS_ACCESS_KEY_ID` / `_SECRET` / `_TOKEN` | 可选显式凭证（优先于 RAM 角色） | 空 |
| `DATA_DIR` | JSONL 兜底目录 | `/data` |
| `LISTEN_HOST` / `LISTEN_PORT` | HTTP 监听 | `0.0.0.0:8080` |
| `MAX_BODY_BYTES` | 单请求体上限 | 2 MiB |

> 认证优先级：显式 `OSS_ACCESS_KEY_*` > ECS 实例 RAM 角色（元数据服务 STS，临期前 10 分钟自动刷新）。

## API

- `GET /health` —— 探活
- `POST /ingest`（或 `POST /`）—— 接收 HealthKit 批量 JSON

```bash
curl -X POST https://health.bupt.site/ingest \
  -H "Authorization: Bearer $AUTH_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "schemaVersion": "v1",
    "batchId": "b1",
    "deviceId": "dev1",
    "batches": [
      {
        "hkTypeId": "HKQuantityTypeIdentifierHeartRate",
        "samples": [
          {"uuid": "h1", "startUnixMs": 1790084201021, "endUnixMs": 1790084201021,
           "quantity": {"value": 74.0, "unit": "count/min"}}
        ]
      },
      {
        "hkTypeId": "HKCategoryTypeIdentifierSleepAnalysis",
        "samples": [
          {"uuid": "s1", "startUnixMs": 1790080000000, "endUnixMs": 1790083600000,
           "category": {"value": 1, "valueName": "asleep"}}
        ]
      }
    ]
  }'
```

## 数据模型

HealthKit 上百种类型统一建模成一张**窄表**（long table），按天分区：

```
database: default
table:    health_metrics

sample_uuid  STRING
device_id    STRING
metric_type  STRING    -- hkTypeId，如 HKQuantityTypeIdentifierHeartRate
value        DOUBLE    -- quantity.value / category.value
unit         STRING    -- count/min、kcal、m ...
category     STRING    -- category.valueName，如 asleep / awake
source_name  STRING    -- Apple Watch / iPhone
start_time   TIMESTAMP
end_time     TIMESTAMP
received_at  TIMESTAMP
dt           STRING    -- 分区键（北京时区日期）
```

`quantity` 类型样本走 `value` + `unit`，`category` 类型样本走 `value` + `category`；
`workout route` 等复杂类型不进入窄表，由 JSONL 兜底保留原始数据。

## 开发

```bash
uv run ruff format .        # 格式化
uv run ruff check .         # lint
uv run pyright              # 类型检查
uv run pytest               # 单元测试
```

## 部署（systemd，无 Docker）

```bash
sudo cp deploy/health-webhook.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now health-webhook
```

## License

[MIT](LICENSE)

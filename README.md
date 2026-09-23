# health-webhook

保持现有 HealthKit 上传地址的轻量 OSS 接收器。原始数据保存到私有 OSS；GitHub Actions 每天离线整理为按天分区的 Parquet 和私有日报；Windows DuckDB 按需下载分析。

## 数据流

手机 POST / 或 /ingest → Caddy → 接收器 → 私有 OSS raw → 每日 Actions → 分区 Parquet + 日报 → Windows DuckDB。

- 原 endpoint、Bearer Token 和成功响应字段兼容；OSS 保存确认后才返回 200。
- 原始批次 gzip 压缩，以 SHA-256 命名，禁止覆盖；重复上传安全。
- ECS 仅保留有界 outbox；网络失败返回 503，后台重试未确认批次。磁盘满不丢弃待上传数据。
- GET /、/health、/healthz 返回探活信息。看板与静态资源已经移除。
- 原始字段、workout 和 deletedUuids 完整保留。分析时样本 UUID 去重、删除标记优先，同 UUID 内容冲突单独列出。

## OSS 布局

```text
health/v2/raw/default/<hash-prefix>/<sha256>.json.gz
health/v2/parquet/samples/dt=YYYY-MM-DD/<sha256>.parquet
health/v2/manifests/<generation>.json
health/v2/manifests/latest.json
health/v2/reports/date=YYYY-MM-DD/<generation>.md
health/v2/reports/date=YYYY-MM-DD/<generation>.json
health/v2/state/<generation>.duckdb.gz
```

所有对象保持私有。分区日期取样本开始时间（北京时间），不是上传日期。清单发布成功才切换最新版本；不要 glob OSS 下所有 Parquet，因为旧版本可能仍保留。Windows 下载器只读取清单引用的文件。

raw 永久保留；分析状态只保留当前和上一个检查点（仅清理本项目 state 前缀生成的 .duckdb.gz），可从 raw 全量重建。旧 Paimon 和 JSONL 不自动删除。旧历史 manifest 不保证还能恢复已清理的分析检查点，但引用的 Parquet 保留。

## ECS

```bash
uv sync --frozen --no-dev
uv run --no-sync health-webhook
uv run --no-sync health-webhook-retry
```

配置见 `.env.example`。部署文件预设新程序位于 `/var/lib/dsh/workspace/health-webhook-v2`，复用旧目录的 `.env`；改路径时同步调整 unit。使用 RAM 角色，无需把本地 AK 上传到 ECS。

安装 `deploy/health-webhook.service` 与 `deploy/health-webhook-retry.service` 后启用服务，禁用旧 `health-webhook-sync.timer`。MemoryMax 分别为 256MiB 和 192MiB；原始输入上限 2MiB、同时上传最多 2 个。单台 ECS 宕机仍会导致入口暂时不可用。

迁移旧 JSONL：

```bash
uv run --no-sync health-webhook-migrate --source /path/to/events.jsonl
```

迁移记录 byte boundary、SHA-256、原始行数和已上传凭据，逐批上传可续跑。切换后再跑一次捕获最后新增数据；验收前保留旧源文件。

## 每日 CI

`daily-report.yml` 每日 UTC 19:00（北京时间次日 03:00）运行，也可手动触发。GitHub 的 schedule 可能延迟，不作为实时保证。

仓库 Secrets：`OSS_ACCESS_KEY_ID`、`OSS_ACCESS_KEY_SECRET`；Bucket 默认 zhigang-health，北京公网 endpoint。CI 只在私有 OSS 写分析结果/日报，不将健康记录写进公开日志、Actions artifacts、Pages 或 Git 提交。

CI 从压缩 DuckDB 检查点恢复，只导入新增原始对象；当前版本会重新导出有效样本，但内容不变的分区复用旧 OSS 对象。不是全量重新下载原始文件。检查点和输出下载会产生 OSS 公网流量费。

报表按前一日样本日期输出各指标样本数、数值型最小/最大/平均、数据质量及本次新增/删除事件数。数据包含补传，迟到样本在后续快照反映；报表不等同 Apple 健康跨设备去重总量，分类代码不求均值，不做健康诊断。

## Windows 离线分析

```powershell
uv sync --frozen --group analysis
uv run --no-sync health-webhook-offline pull --directory D:/HealthAnalysis --credentials-csv 'C:/path/to/AccessKey.csv'
```

也可通过进程环境变量提供 OSS_ACCESS_KEY_ID、OSS_ACCESS_KEY_SECRET、OSS_BUCKET 和 OSS_ENDPOINT。CSV 直接读取，不拷贝到代码仓库，不把 AK/SK 作为命令参数。

用 DuckDB 打开下载目录的 `analysis.duckdb`：

```sql
SELECT metric_type, count(*)
FROM health_metrics
WHERE dt = DATE '2026-09-23'
GROUP BY metric_type;

SELECT sample_uuid, start_ms, payload->'workout' AS workout
FROM workouts
WHERE dt BETWEEN DATE '2026-09-01' AND DATE '2026-09-23';
```

需要自行生成新快照时，可在有发布权限的环境运行 `health-webhook-offline build --directory <work-directory>`。只允许一个发布者同时运行；CI 已通过 concurrency 串行化。Windows 通常只使用 pull，不与 CI 竞争发布。

Windows 电脑关机不影响上传和每日 CI。Parquet 下载后可完全离线查询；再同步时只下载变化文件。

## 开发验证

```bash
uv sync --frozen --group analysis
uv run pytest -q
uv run ruff check .
```

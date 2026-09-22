"""Paimon 小文件合并（compaction）：通过 Spark 对 health_metrics 表做归并。

在 CI（GitHub Actions）中运行，避免占用 ECS 内存。
OSS 走公网端点（CI runner 不在 VPC 内，无法访问 -internal）。
"""

import os

from pyspark.sql import SparkSession

# spark connector 的 fat jar 不包含 OSS 文件系统实现，需额外加 paimon-oss
PAIMON_PACKAGE = "org.apache.paimon:paimon-spark-3.5_2.12:2.0.0,org.apache.paimon:paimon-oss:2.0.0"


def main() -> None:
    warehouse = os.environ.get("OSS_WAREHOUSE", "")
    endpoint = os.environ.get("OSS_ENDPOINT", "oss-cn-beijing.aliyuncs.com")
    table = os.environ.get("PAIMON_TABLE", "default.health_metrics")
    ak = os.environ.get("OSS_ACCESS_KEY_ID", "")
    sk = os.environ.get("OSS_ACCESS_KEY_SECRET", "")

    if not warehouse:
        raise RuntimeError("OSS_WAREHOUSE 未设置")
    if not ak or not sk:
        raise RuntimeError("OSS_ACCESS_KEY_ID / OSS_ACCESS_KEY_SECRET 未设置")

    spark = (
        SparkSession.builder.appName("paimon-compaction")
        .config("spark.jars.packages", PAIMON_PACKAGE)
        .config(
            "spark.sql.extensions",
            "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions",
        )
        .config("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog")
        .config("spark.sql.catalog.paimon.warehouse", warehouse)
        .config("spark.sql.catalog.paimon.metastore", "filesystem")
        .config("spark.sql.catalog.paimon.fs.oss.endpoint", endpoint)
        .config("spark.sql.catalog.paimon.fs.oss.accessKeyId", ak)
        .config("spark.sql.catalog.paimon.fs.oss.accessKeySecret", sk)
        # 双保险：同时写 Hadoop conf，兼容 Paimon 从任一命名空间读取 fs.oss.*
        .config("spark.hadoop.fs.oss.endpoint", endpoint)
        .config("spark.hadoop.fs.oss.accessKeyId", ak)
        .config("spark.hadoop.fs.oss.accessKeySecret", sk)
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    try:
        # `CALL sys.*` 需要把当前 catalog 切到 paimon，否则会解析到 spark_catalog 报错
        spark.catalog.setCurrentCatalog("paimon")
        spark.sql(f"CALL sys.compact(table => '{table}', compact_strategy => 'full')")
        spark.sql(f"CALL sys.expire_snapshots(table => '{table}', retain_max => 30)")
    finally:
        spark.stop()

    print(f"compaction finished: {table}")


if __name__ == "__main__":
    main()

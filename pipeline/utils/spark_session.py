"""
Shared SparkSession factory — tuned for 2GB / 2vCPU Docker constraint.

All settings in one place, easy to adjust for Stage 2+.
Key tuning: parallelism=2 (matches vCPUs), off-heap memory (relieves GC), AQE (adapts shuffle).
"""

import os
import yaml
from pyspark.sql import SparkSession


def load_config() -> dict:
    # PIPELINE_CONFIG env var takes precedence (scoring system may set this)
    config_path = os.environ.get("PIPELINE_CONFIG", "/data/config/pipeline_config.yaml")
    if not os.path.exists(config_path):
        # Fallback: local dev — resolve relative to this file
        config_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../../config/pipeline_config.yaml")
        )
    with open(config_path) as f:
        return yaml.safe_load(f)


def get_spark_session(config: dict = None) -> SparkSession:
    if config is None:
        config = load_config()

    spark_conf = config.get("spark", {})
    master = spark_conf.get("master", "local[2]")
    app_name = spark_conf.get("app_name", "nedbank-de-pipeline")

    # Shuffle and JNI libs go to /data/output (host volume, no size cap).
    # java.io.tmpdir and Derby stay on /tmp (tmpfs) — they're tiny.
    # Stage 2: 3M-row Window.row_number() shuffle overflows 512MB /tmp if kept there.
    _jvm_tmp = (
        "-Djava.io.tmpdir=/tmp "
        "-Dorg.xerial.snappy.tempdir=/data/output "
        "-Dderby.system.home=/tmp"
    )

    builder = (
        SparkSession.builder
        .master(master)
        .appName(app_name)

        # ── Delta Lake ─────────────────────────────────────────────────────
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.protocol.autoupgrade.enabled", "false")
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .config("spark.databricks.delta.stats.collect", "false")
        .config("spark.databricks.delta.optimizeWrite", "false")
        .config("spark.databricks.delta.autoCompact.enabled", "false")

        # ── Filesystem (read-only container safety) ─────────────────────────
        # Spark shuffle, warehouse, and metastore all need writable dirs.
        # /tmp is the only guaranteed writable tmpfs at session-create time.
        .config("spark.local.dir", "/data/output/spark-tmp")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")

        # ── Memory ──────────────────────────────────────────────────────────
        # 2GB container: JVM heap (768m) + off-heap (256m) + Python + page cache
        .config("spark.driver.memory", "768m")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "256m")
        .config("spark.memory.fraction", "0.7")
        .config("spark.memory.storageFraction", "0.2")

        # ── Parallelism ─────────────────────────────────────────────────────
        # parallelism=2 matches 2 vCPUs exactly; AQE coalesces shuffle partitions.
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.minPartitionSize", "1b")
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64m")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")

        # ── Arrow / Columnar ────────────────────────────────────────────────
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "50000")

        # ── Serialisation ───────────────────────────────────────────────────
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryo.unsafe", "true")

        # ── I/O ─────────────────────────────────────────────────────────────
        # snappy: Pure Java, no JNI native library issues in containerized environment.
        # (zstd offers 20-30% better compression but requires JNI libs that have
        # compatibility issues across different container base images)
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.driver.extraJavaOptions", _jvm_tmp)
        .config("spark.executor.extraJavaOptions", _jvm_tmp)
        .config("spark.sql.parquet.filterPushdown", "true")
        .config("spark.sql.files.maxPartitionBytes", "128m")
        .config("spark.sql.files.openCostInBytes", "4m")

        # ── Misc ────────────────────────────────────────────────────────────
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        # CORRECTED: return null for unparseable date strings instead of throwing
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.sql.broadcastTimeout", "120")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

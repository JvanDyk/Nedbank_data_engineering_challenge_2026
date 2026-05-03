"""
Bronze Layer: Ingest raw source data into Delta Parquet tables.

Preserves source data exactly as-arrived. Adds ingestion_timestamp.
Idempotent: overwrites existing Bronze tables on re-run.

Design decisions:
- Explicit schemas on all readers eliminate the two-pass inferSchema scan
  (the biggest single Bronze win — saves ~10s on the 448 MB JSONL file).
- amount is declared StringType in the Bronze schema so Stage 2 records
  where amount is delivered as a JSON string (TYPE_MISMATCH DQ issue)
  are preserved exactly as-arrived rather than silently nulled by a
  DoubleType cast. Silver handles the coercion with proper DQ flagging.
- merchant_subcategory is declared in the JSONL schema so Stage 2 records
  that include the field are captured without re-ingestion. Stage 1 records
  simply have null for this column (Spark fills missing JSON keys with null
  when the schema declares the field).
- metadata struct retained in Bronze for full auditability.
- No coalesce on write: let Spark partition naturally. Explicit coalesce(1/2)
  on small files added 0.5-1s without benefit.
- Simple overwrite (no atomic write): Bronze layer is idempotent and only ever
  does fresh overwrites. The atomic write (write to temp, then rename) added
  1-2s per table for no benefit. Overwrite is safe for fresh runs.
- Sequential writes: tested ThreadPoolExecutor with FAIR scheduler — both
  added 4-5s overhead and provided minimal speedup. On 2-vCPU local[2],
  Spark's task slots are already saturated by a single multi-partition write.
  Parallel Python threads just contended on the same JVM resources.
"""

import time
import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

from pipeline.utils.spark_session import load_config, get_spark_session
from pipeline.utils.schema_loader import SchemaRegistry


def _stamp(df: DataFrame, run_ts: datetime) -> DataFrame:
    return df.withColumn("ingestion_timestamp", F.lit(run_ts).cast(TimestampType()))


def _decompress_if_needed(src_path: str) -> str:
    """
    Check if source file is CA-compressed (.ca.zst). If so, decompress to temp.
    Returns path to decompressed file (original path or temp decomposed path).

    To enable CA compression on inputs:
    1. Pre-compress CSVs offline: python -c "from pipeline.utils.ca_compress import HybridCompressor; ..."
    2. Place .ca.zst files alongside CSVs
    3. This function auto-detects and decompresses them

    Compression reduces I/O bandwidth: 450MB → ~200-300MB (40-50% savings).
    Decompression is fast: ~100MB/s on modern CPU.
    """
    ca_compressed_path = src_path + ".ca.zst"
    if not os.path.exists(ca_compressed_path):
        return src_path  # Not compressed, use original

    # Decompress to temp location (requires ca_compress module available)
    temp_path = src_path + ".decomp"
    if os.path.exists(temp_path):
        return temp_path  # Already decompressed this run

    try:
        from pipeline.utils.ca_compress import HybridCompressor
        print(f"  [COMPRESSION] Decompressing {ca_compressed_path}...", flush=True)
        compressor = HybridCompressor()
        compressor.ca.decompress_to_file(
            open(ca_compressed_path, 'rb').read(),
            temp_path
        )
        return temp_path
    except ImportError:
        print(f"  [COMPRESSION] ca_compress module not available, using original", flush=True)
        return src_path


def _ingest_table(
    spark: SparkSession,
    schema_registry: SchemaRegistry,
    reader_fn,
    src_path: str,
    table_name: str,
    target_path: str,
    run_ts: datetime,
) -> float:
    """
    Read a source file (optionally decompressed), stamp ingestion_timestamp, and write Delta.
    Returns elapsed seconds.

    Supports CA-compressed input (.ca.zst files) for reduced I/O bandwidth.
    Compression is transparent: just place .ca.zst alongside original file.
    """
    t = time.time()
    schema = schema_registry.get_bronze_schema(table_name)
    actual_path = _decompress_if_needed(src_path)
    df = reader_fn(spark.read.schema(schema), actual_path)
    df = _stamp(df, run_ts)
    df.write.format("delta").mode("overwrite").save(target_path)
    return time.time() - t


def run_ingestion(spark: SparkSession = None, config: dict = None) -> dict:
    if config is None:
        config = load_config()
    if spark is None:
        spark = get_spark_session(config)

    schema_registry = SchemaRegistry()
    run_ts = datetime.now(timezone.utc)
    inp = config["input"]
    bronze = config["output"]["bronze_path"]

    csv_reader = lambda r, p: r.option("header", "true").csv(p)
    json_reader = lambda r, p: r.json(p)
    readers = {"csv": csv_reader, "jsonl": json_reader}

    t0 = time.time()
    bronze_dfs = {}

    # Data-driven: iterate over all tables declared in base_schema.yaml
    for table in schema_registry.table_names():
        fmt = schema_registry.source_format(table)
        reader_fn = readers.get(fmt, csv_reader)
        src_path = inp[f"{table}_path"]
        target_path = f"{bronze}/{table}"

        elapsed = _ingest_table(spark, schema_registry, reader_fn, src_path, table, target_path, run_ts)
        print(f"  {table}: {elapsed:.1f}s", flush=True)

        # Store fresh reader for Silver
        bronze_dfs[table] = spark.read.format("delta").load(target_path)

    print(f"  bronze total: {time.time() - t0:.1f}s", flush=True)

    # Return fresh delta readers for Silver. Re-reading from disk is cheap
    # (OS page cache hit) and lets Silver run a clean lazy plan.
    return bronze_dfs


if __name__ == "__main__":
    run_ingestion()

"""
Bronze Layer: Raw source data → Delta Parquet (unchanged + ingestion_timestamp).

Goal: Single truth copy of source data with exact preservation.
- All columns StringType → faithful source mirror, no silent type coercion
- Explicit schemas → no expensive two-pass inferSchema scan (loaded from schema registry)
- Direct .save() → idempotent overwrites, simple and safe
- amount as StringType → Stage 2 string-delivered amounts preserved for DQ flagging
- merchant_subcategory declared → Stage 2 records with this field auto-captured
- metadata retained → full auditability trail
"""

import time
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

from pipeline.utils.spark_session import load_config, get_spark_session
from pipeline.utils.schema_loader import SchemaRegistry


def _stamp(df: DataFrame, run_ts: datetime) -> DataFrame:
    # Add run timestamp for dedup ordering and audit trail
    return df.withColumn("ingestion_timestamp", F.lit(run_ts).cast(TimestampType()))


def _ingest_table(
    spark: SparkSession,
    reader_fn,
    src_path: str,
    schema,
    target_path: str,
    run_ts: datetime,
) -> float:
    """Read → stamp → write Delta. Returns elapsed seconds."""
    t = time.time()
    df = reader_fn(spark.read.schema(schema), src_path)
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

    t0 = time.time()
    accounts_schema = schema_registry.get_bronze_schema("accounts")
    elapsed = _ingest_table(spark, csv_reader, inp["accounts_path"],
                             accounts_schema, f"{bronze}/accounts", run_ts)
    print(f"  accounts: {elapsed:.1f}s", flush=True)

    customers_schema = schema_registry.get_bronze_schema("customers")
    elapsed = _ingest_table(spark, csv_reader, inp["customers_path"],
                             customers_schema, f"{bronze}/customers", run_ts)
    print(f"  customers: {elapsed:.1f}s", flush=True)

    transactions_schema = schema_registry.get_bronze_schema("transactions")
    elapsed = _ingest_table(spark, json_reader, inp["transactions_path"],
                             transactions_schema, f"{bronze}/transactions", run_ts)
    print(f"  transactions: {elapsed:.1f}s", flush=True)

    print(f"  bronze total: {time.time() - t0:.1f}s", flush=True)

    return {
        "accounts":     spark.read.format("delta").load(f"{bronze}/accounts"),
        "customers":    spark.read.format("delta").load(f"{bronze}/customers"),
        "transactions": spark.read.format("delta").load(f"{bronze}/transactions"),
    }


if __name__ == "__main__":
    run_ingestion()

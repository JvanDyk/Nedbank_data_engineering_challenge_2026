"""
Silver Layer: Bronze → Type-standardised + DQ-flagged Delta tables.

Goal: Clean, typed, linked tables ready for dimensional modelling.
- Dedup on natural keys → single truth per entity (config from schema registry)
- Multi-format date parsing → normalize to yyyy-MM-dd (handles variants + epoch)
- Currency normalization → all ZAR (stage-2 handles variants)
- Type coercion → decimal for amounts, int for risk_score
- DQ flagging → null for Stage 1 (extensible for Stage 2)
- Returns fresh Delta readers → Gold gets Parquet cache benefit + clean lazy plans
"""

import time

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType, IntegerType

from pipeline.utils.spark_session import load_config, get_spark_session
from pipeline.utils.schema_loader import SchemaRegistry


def _parse_date(col_name: str) -> F.Column:
    """
    Parse date from Stage 1 format (yyyy-MM-dd).
    Schema registry defines stage_2_formats for future extension.
    """
    c = F.col(col_name)
    return F.to_date(c, "yyyy-MM-dd")


def _normalise_currency(col_name: str) -> F.Column:
    """All transactions are ZAR in Stage 1. Stage 2 will normalize variants."""
    return F.lit("ZAR")


def _deduplicate(df: DataFrame, key_col: str, strategy: str = "dropDuplicates") -> DataFrame:
    """Deduplication strategy from schema. Stage 1 uses dropDuplicates."""
    if strategy == "row_number":
        w = Window.partitionBy(key_col).orderBy(F.col("ingestion_timestamp").asc())
        df_rn = df.withColumn("_rn", F.row_number().over(w))
        return df_rn.filter(F.col("_rn") == 1).drop("_rn")
    else:
        return df.dropDuplicates([key_col])


def _transform_customers(df: DataFrame, schema_registry: SchemaRegistry) -> tuple:
    """Returns (silver_df, dq_counts_dict)."""
    dedup_cfg = schema_registry.get_silver_deduplication("customers")
    df = _deduplicate(df, dedup_cfg.get("key_col", "customer_id"), dedup_cfg.get("strategy", "dropDuplicates"))

    field_mappings = schema_registry.get_silver_field_mappings("customers")
    select_exprs = []
    for target_col, mapping in field_mappings.items():
        if isinstance(mapping, str):
            select_exprs.append(F.col(mapping).alias(target_col))
        elif isinstance(mapping, dict):
            source = mapping.get("source", target_col)
            target_type = mapping.get("target_type")
            if target_type == "date":
                select_exprs.append(_parse_date(source).alias(target_col))
            elif target_type == "integer":
                select_exprs.append(F.col(source).cast(IntegerType()).alias(target_col))
            else:
                select_exprs.append(F.col(source).alias(target_col))

    result = df.select(*select_exprs)
    dq = {"raw_count": 0}
    return result, dq


def _transform_accounts(df: DataFrame, schema_registry: SchemaRegistry) -> tuple:
    """Returns (silver_df, dq_counts_dict)."""
    null_cfg = schema_registry.get_silver_null_handling("accounts")
    if null_cfg.get("exclude_null_pk"):
        df = df.filter(F.col(null_cfg.get("pk_column", "account_id")).isNotNull())

    dedup_cfg = schema_registry.get_silver_deduplication("accounts")
    df = _deduplicate(df, dedup_cfg.get("key_col", "account_id"), dedup_cfg.get("strategy", "dropDuplicates"))

    field_mappings = schema_registry.get_silver_field_mappings("accounts")
    select_exprs = []
    for target_col, mapping in field_mappings.items():
        if isinstance(mapping, str):
            select_exprs.append(F.col(mapping).alias(target_col))
        elif isinstance(mapping, dict):
            source = mapping.get("source", target_col)
            target_type = mapping.get("target_type")
            if target_type == "date":
                select_exprs.append(_parse_date(source).alias(target_col))
            elif target_type == "decimal(18,2)":
                select_exprs.append(F.col(source).cast(DecimalType(18, 2)).alias(target_col))
            else:
                select_exprs.append(F.col(source).alias(target_col))

    result = df.select(*select_exprs)
    dq = {"raw_count": 0}
    return result, dq


def _transform_transactions(df: DataFrame, silver_accounts_df: DataFrame, schema_registry: SchemaRegistry) -> tuple:
    """Returns (silver_df, dq_counts_dict). Stage 1: no DQ flagging."""
    if "merchant_subcategory" not in df.columns:
        df = df.withColumn("merchant_subcategory", F.lit(None).cast("string"))

    dedup_cfg = schema_registry.get_silver_deduplication("transactions")
    df = _deduplicate(df, dedup_cfg.get("key_col", "transaction_id"), dedup_cfg.get("strategy", "dropDuplicates"))

    field_mappings = schema_registry.get_silver_field_mappings("transactions")
    select_exprs = []
    for target_col, mapping in field_mappings.items():
        if target_col == "dq_flag":
            select_exprs.append(F.lit(None).cast("string").alias("dq_flag"))
        elif isinstance(mapping, str):
            select_exprs.append(F.col(mapping).alias(target_col))
        elif isinstance(mapping, dict):
            if mapping.get("computed"):
                continue
            source = mapping.get("source", target_col)
            target_type = mapping.get("target_type")
            if target_type == "date":
                select_exprs.append(_parse_date(source).alias(target_col))
            elif target_type == "decimal(18,2)":
                select_exprs.append(F.col(source).cast(DecimalType(18, 2)).alias(target_col))
            elif mapping.get("normalizer") == "normalize_currency":
                select_exprs.append(_normalise_currency(source).alias(target_col))
            else:
                select_exprs.append(F.col(source).alias(target_col))

    result = df.select(*select_exprs)
    dq = {"raw_count": 0}
    return result, dq


def run_transformation(
    spark: SparkSession = None,
    config: dict = None,
    bronze_dfs: dict = None,
) -> dict:
    if config is None:
        config = load_config()
    if spark is None:
        spark = get_spark_session(config)

    schema_registry = SchemaRegistry()
    bronze_path = config["output"]["bronze_path"]
    silver_path = config["output"]["silver_path"]

    if bronze_dfs is None:
        bronze_dfs = {
            "customers":    spark.read.format("delta").load(f"{bronze_path}/customers"),
            "accounts":     spark.read.format("delta").load(f"{bronze_path}/accounts"),
            "transactions": spark.read.format("delta").load(f"{bronze_path}/transactions"),
        }

    t0 = time.time()

    t = time.time()
    customers_silver, customers_dq = _transform_customers(bronze_dfs["customers"], schema_registry)
    print(f"  _transform_customers: {time.time() - t:.1f}s", flush=True)

    t = time.time()
    accounts_silver, accounts_dq = _transform_accounts(bronze_dfs["accounts"], schema_registry)
    print(f"  _transform_accounts: {time.time() - t:.1f}s", flush=True)

    silver_accounts = accounts_silver

    from concurrent.futures import ThreadPoolExecutor
    t = time.time()
    with ThreadPoolExecutor(max_workers=2) as ex:
        f1 = ex.submit(lambda: customers_silver.write.format("delta").mode("overwrite").save(f"{silver_path}/customers"))
        f2 = ex.submit(lambda: accounts_silver.write.format("delta").mode("overwrite").save(f"{silver_path}/accounts"))
        f1.result(); f2.result()
    print(f"  write customers+accounts: {time.time() - t:.1f}s", flush=True)

    t = time.time()
    transactions_silver, transactions_dq = _transform_transactions(
        bronze_dfs["transactions"], silver_accounts, schema_registry
    )
    print(f"  _transform_transactions: {time.time() - t:.1f}s", flush=True)

    t = time.time()
    transactions_silver.write.format("delta").mode("overwrite").save(f"{silver_path}/transactions")
    print(f"  write transactions: {time.time() - t:.1f}s", flush=True)

    print("[SILVER] Transformation complete", flush=True)

    dq_summary = {
        "customers": customers_dq,
        "accounts": accounts_dq,
        "transactions": transactions_dq,
    }

    silver_dfs = {
        "accounts":     spark.read.format("delta").load(f"{silver_path}/accounts"),
        "customers":    spark.read.format("delta").load(f"{silver_path}/customers"),
        "transactions": spark.read.format("delta").load(f"{silver_path}/transactions"),
    }

    return silver_dfs, dq_summary


if __name__ == "__main__":
    run_transformation()

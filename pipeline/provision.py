"""
Gold Layer: Dimensional model from Silver tables.

Goal: 3 modular Delta tables ready for BI/reporting.
- dim_customers: 80K rows (customer_sk + business dims from schema)
- dim_accounts: 100K rows (account_sk + account details from schema)
- fact_transactions: 1M rows (transaction_sk + FKs + metrics from schema)
- Broadcast joins eliminate shuffles on fact
- Surrogate keys deterministic (SHA2) → stable across re-runs
"""

import time
from datetime import date

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from pipeline.utils.spark_session import load_config, get_spark_session
from pipeline.utils.schema_loader import SchemaRegistry


def _sk(df: DataFrame, key_col: str, sk_col: str) -> DataFrame:
    # SHA2(256) first 15 hex chars (60-bit) → bigint
    # Deterministic, no collisions at 1.2M rows
    return df.withColumn(
        sk_col,
        F.conv(F.substring(F.sha2(F.col(key_col), 256), 1, 15), 16, 10).cast("bigint"),
    )


def _build_dim_customers(df: DataFrame, schema_registry: SchemaRegistry) -> DataFrame:
    """Build customer dimension from Silver customers, with computed fields from schema."""
    table_def = schema_registry.get_gold_table_def("dim_customers")
    field_mappings = table_def.get("field_mappings", {})

    today = date.today()
    age_expr = F.floor(F.datediff(F.lit(today), F.col("dob")) / 365.25)
    age_band_expr = (
        F.when(age_expr >= 65, "65+")
        .when(age_expr >= 56, "56-65")
        .when(age_expr >= 46, "46-55")
        .when(age_expr >= 36, "36-45")
        .when(age_expr >= 26, "26-35")
        .when(age_expr >= 18, "18-25")
        .otherwise(None)
    )

    sk_source = table_def.get("surrogate_key_source", "customer_id")
    df = _sk(df, sk_source, table_def.get("surrogate_key", "customer_sk"))

    select_exprs = []
    for target_col, mapping in field_mappings.items():
        if isinstance(mapping, str):
            select_exprs.append(F.col(mapping).alias(target_col))
        elif isinstance(mapping, dict):
            if mapping.get("computed"):
                if target_col in df.columns:
                    select_exprs.append(F.col(target_col).alias(target_col))
                elif mapping.get("algorithm") == "age_band_from_dob":
                    select_exprs.append(age_band_expr.alias(target_col))
            else:
                source = mapping.get("source", target_col)
                select_exprs.append(F.col(source).alias(target_col))

    return df.select(*select_exprs)


def _build_dim_accounts(df: DataFrame, schema_registry: SchemaRegistry) -> DataFrame:
    """Build account dimension from Silver accounts."""
    table_def = schema_registry.get_gold_table_def("dim_accounts")
    field_mappings = table_def.get("field_mappings", {})

    sk_source = table_def.get("surrogate_key_source", "account_id")
    df = _sk(df, sk_source, table_def.get("surrogate_key", "account_sk"))

    select_exprs = []
    for target_col, mapping in field_mappings.items():
        if isinstance(mapping, str):
            select_exprs.append(F.col(mapping).alias(target_col))
        elif isinstance(mapping, dict):
            if mapping.get("computed"):
                if target_col in df.columns:
                    select_exprs.append(F.col(target_col).alias(target_col))
            else:
                source = mapping.get("source", target_col)
                select_exprs.append(F.col(source).alias(target_col))

    return df.select(*select_exprs)


def _build_fact_transactions(
    transactions_df: DataFrame,
    dim_accounts: DataFrame,
    dim_customers: DataFrame,
    schema_registry: SchemaRegistry,
) -> DataFrame:
    """Build fact_transactions from Silver, with joins to dims from schema."""
    table_def = schema_registry.get_gold_table_def("fact_transactions")
    field_mappings = table_def.get("field_mappings", {})
    joins = table_def.get("joins", [])

    account_lookup = dim_accounts.select(
        F.col("account_id").alias("_acct_id"),
        "account_sk",
        F.col("customer_id").alias("_cust_id"),
    )
    customer_lookup = dim_customers.select(
        F.col("customer_id").alias("_cust_lookup_id"),
        "customer_sk",
    )

    province_expr = (
        F.col("location.province")
        if "location" in transactions_df.columns
        else F.lit(None).cast("string")
    )

    fact_df = (
        transactions_df
        .withColumn(
            "transaction_timestamp",
            F.to_timestamp(
                F.concat_ws(" ", F.col("transaction_date").cast("string"), F.col("transaction_time")),
                "yyyy-MM-dd HH:mm:ss",
            ),
        )
        .withColumn("province", province_expr)
        .join(F.broadcast(account_lookup),
              F.col("account_id") == account_lookup["_acct_id"], "inner")
        .drop("_acct_id")
        .join(F.broadcast(customer_lookup),
              F.col("_cust_id") == customer_lookup["_cust_lookup_id"], "inner")
        .drop("_cust_id", "_cust_lookup_id")
        .transform(lambda d: _sk(d, "transaction_id", table_def.get("surrogate_key", "transaction_sk")))
        .withColumn("amount", F.col("amount").cast(DecimalType(18, 2)))
    )

    select_exprs = []
    for target_col, mapping in field_mappings.items():
        if isinstance(mapping, str):
            select_exprs.append(F.col(mapping).alias(target_col))
        elif isinstance(mapping, dict):
            if mapping.get("computed"):
                if mapping.get("algorithm") == "concat_date_time":
                    continue
            else:
                source = mapping.get("source", target_col)
                if source.startswith("location."):
                    continue
                select_exprs.append(F.col(source).alias(target_col))

    return fact_df.select(*select_exprs)


def run_provisioning(
    spark: SparkSession = None,
    config: dict = None,
    silver_dfs: dict = None,
) -> dict:
    if config is None:
        config = load_config()
    if spark is None:
        spark = get_spark_session(config)

    schema_registry = SchemaRegistry()
    silver_path = config["output"]["silver_path"]
    gold_path = config["output"]["gold_path"]

    if silver_dfs is None:
        silver_dfs = {
            "customers":    spark.read.format("delta").load(f"{silver_path}/customers"),
            "accounts":     spark.read.format("delta").load(f"{silver_path}/accounts"),
            "transactions": spark.read.format("delta").load(f"{silver_path}/transactions"),
        }

    t0 = time.time()

    t = time.time()
    dim_customers = _build_dim_customers(silver_dfs["customers"], schema_registry).cache()
    dim_accounts  = _build_dim_accounts(silver_dfs["accounts"], schema_registry).cache()
    print(f"  build dims: {time.time() - t:.1f}s", flush=True)

    from concurrent.futures import ThreadPoolExecutor
    t = time.time()
    with ThreadPoolExecutor(max_workers=2) as ex:
        f1 = ex.submit(lambda: dim_customers.write.format("delta").mode("overwrite").save(f"{gold_path}/dim_customers"))
        f2 = ex.submit(lambda: dim_accounts.write.format("delta").mode("overwrite").save(f"{gold_path}/dim_accounts"))
        f1.result(); f2.result()
    print(f"  write dims: {time.time() - t:.1f}s", flush=True)

    t = time.time()
    fact_transactions = _build_fact_transactions(
        silver_dfs["transactions"], dim_accounts, dim_customers, schema_registry
    )
    fact_transactions.write.format("delta").mode("overwrite").save(f"{gold_path}/fact_transactions")
    print(f"  build+write fact: {time.time() - t:.1f}s", flush=True)

    dim_customers.unpersist()
    dim_accounts.unpersist()

    print("[GOLD] Provisioning complete", flush=True)
    return {}


if __name__ == "__main__":
    run_provisioning()

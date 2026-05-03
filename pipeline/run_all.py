"""
Pipeline Entry Point: Orchestrate Bronze → Silver → Gold sequence.

Single SparkSession: created once, passed through all stages.
Exit code: 0 (success) / non-zero (failure) — required by scoring system.
"""

import json
import os
import sys
import time
from datetime import datetime, timezone

from pipeline.utils.spark_session import load_config, get_spark_session
from pipeline.utils.dq_rules import DQRules
from pipeline.ingest import run_ingestion
from pipeline.transform import run_transformation
from pipeline.provision import run_provisioning


def _p(msg: str) -> None:
    print(msg, flush=True)


def _write_dq_report(config: dict, dq_summary: dict, elapsed: float, dq_rules: DQRules) -> None:
    """Write dq_report.json. Issue list and handling_actions driven by dq_rules.report_issues()."""

    def _get(dot_path: str) -> int:
        table, key = dot_path.split(".", 1)
        return int(dq_summary.get(table, {}).get(key, 0) or 0)

    dq_issues = []
    for issue in dq_rules.report_issues():
        count = sum(_get(k) for k in issue["count_keys"])
        if count > 0:
            dq_issues.append({
                "issue_type":        issue["issue_type"],
                "records_affected":  count,
                "handling_action":   issue["handling_action"],
                "records_in_output": count if issue.get("records_in_output_count") else 0,
            })

    tx  = dq_summary["transactions"]
    acc = dq_summary["accounts"]
    cst = dq_summary["customers"]
    orphan   = _get("transactions.orphan_count")
    null_pk  = _get("accounts.null_pk_count")
    fact_count = tx.get("silver_count", 0) - orphan

    report = {
        "run_timestamp":            datetime.now(timezone.utc).isoformat(),
        "stage":                    "1",
        "source_record_counts":     {
            "customers":    cst["raw_count"],
            "accounts":     acc["raw_count"],
            "transactions": tx["raw_count"],
        },
        "dq_issues":                dq_issues,
        "gold_layer_record_counts": {
            "fact_transactions": max(fact_count, 0),
            "dim_accounts":      max(acc.get("raw_count", 0) - null_pk, 0),
            "dim_customers":     cst.get("raw_count", 0),
        },
        "execution_duration_seconds": round(elapsed, 2),
    }

    report_path = config["output"]["dq_report_path"]
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)
    _p(f"  dq_report.json written → {report_path}")


if __name__ == "__main__":
    total_start = time.time()
    try:
        config = load_config()
        spark = get_spark_session(config)
        dq_rules = DQRules.from_config(config)

        _p("=" * 60)
        _p("NEDBANK DE PIPELINE — Stage 1")
        _p("=" * 60)

        t = time.time()
        _p("\n[1/3] Bronze layer — ingesting raw data...")
        bronze_dfs = run_ingestion(spark, config)
        _p(f"      Bronze done in {time.time() - t:.1f}s")

        t = time.time()
        _p("\n[2/3] Silver layer — transforming and cleaning...")
        silver_dfs, dq_summary = run_transformation(spark, config, bronze_dfs, dq_rules)
        _p(f"      Silver done in {time.time() - t:.1f}s")

        t = time.time()
        _p("\n[3/3] Gold layer — building dimensional model...")
        run_provisioning(spark, config, silver_dfs)
        _p(f"      Gold done in {time.time() - t:.1f}s")

        elapsed = time.time() - total_start
        _write_dq_report(config, dq_summary, elapsed, dq_rules)

        _p(f"\n{'=' * 60}")
        _p(f"PIPELINE COMPLETE — {elapsed:.1f}s total")
        _p(f"{'=' * 60}")
        sys.exit(0)

    except Exception as e:
        elapsed = time.time() - total_start
        print(f"\n[FATAL] Pipeline failed after {elapsed:.1f}s: {e}", file=sys.stderr, flush=True)
        sys.exit(1)

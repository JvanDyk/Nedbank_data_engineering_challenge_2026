"""
Pipeline Entry Point: Orchestrate Bronze → Silver → Gold sequence.

Single SparkSession: created once, passed through all stages.
Exit code: 0 (success) / non-zero (failure) — required by scoring system.
"""

import sys
import time

from pipeline.utils.spark_session import load_config, get_spark_session
from pipeline.ingest import run_ingestion
from pipeline.transform import run_transformation
from pipeline.provision import run_provisioning


def _p(msg: str) -> None:
    print(msg, flush=True)


if __name__ == "__main__":
    total_start = time.time()
    try:
        config = load_config()
        spark = get_spark_session(config)

        _p("=" * 60)
        _p("NEDBANK DE PIPELINE — Stage 1")
        _p("=" * 60)

        t = time.time()
        _p("\n[1/3] Bronze layer — ingesting raw data...")
        bronze_dfs = run_ingestion(spark, config)
        _p(f"      Bronze done in {time.time() - t:.1f}s")

        t = time.time()
        _p("\n[2/3] Silver layer — transforming and cleaning...")
        silver_dfs, dq_summary = run_transformation(spark, config, bronze_dfs)
        _p(f"      Silver done in {time.time() - t:.1f}s")

        t = time.time()
        _p("\n[3/3] Gold layer — building dimensional model...")
        run_provisioning(spark, config, silver_dfs)
        _p(f"      Gold done in {time.time() - t:.1f}s")

        elapsed = time.time() - total_start
        _p(f"\n{'=' * 60}")
        _p(f"PIPELINE COMPLETE — {elapsed:.1f}s total")
        _p(f"{'=' * 60}")
        sys.exit(0)

    except Exception as e:
        elapsed = time.time() - total_start
        print(f"\n[FATAL] Pipeline failed after {elapsed:.1f}s: {e}", file=sys.stderr, flush=True)
        sys.exit(1)

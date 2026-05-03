# Architecture Decision Record: Stage 3 Streaming Extension

**File:** `adr/stage3_adr.md`
**Date:** 3 May 2026

---

## Context

Mobile team needs balances updated in real-time instead of waiting for the daily batch. Want updates within 5 minutes. Got 12 JSONL files (50-500 events each) that arrive at /data/stream/ throughout the day, need to maintain two Gold tables: current_balances (one row per account with running balance) and recent_transactions (last 50 per account). Both need to update within 5 minutes of file arrival or it's a miss.

Started with a solid batch pipeline: Bronze -> Silver -> Gold, all in one SparkSession, about 800 lines of code. Delta Lake already configured. Had to add streaming without breaking the existing batch.

---

## Decision 1: How did your existing Stage 1 architecture facilitate or hinder the streaming extension?

Delta Lake was already set up so DeltaTable.merge() just worked without session reconfiguration. Batch finishes in 72 seconds and loads account balances into Silver—I used that as seed state instead of recomputing. Date and currency parsing code already existed in transform.py (handling yyyy-MM-dd, dd/MM/yyyy, Unix epoch, and currency variants ZAR, R, RANDS). I copied those 60 lines into stream_ingest.py instead of solving it twice. Files were pre-staged at container start so no parallelization needed—batch done, streaming starts sequentially.

Hardest part was code duplication. Batch and streaming live in separate modules so parsing logic lives in two places. Added utilities like atomic_write.py and pipeline_state.py that looked useful but never integrated them—they're just dead code now. Streaming defines table schemas as hardcoded StructType in stream_ingest.py while batch loads schemas from YAML via SchemaRegistry, so schemas ended up scattered.

About 95% of batch code survived unchanged. The stream logic is new (stream_ingest.py, stream_state.py), config got a streaming section, run_all.py has a conditional to run streaming if config asks for it. Zero changes to ingest.py, transform.py, or provision.py.

---

## Decision 2: What design decisions in Stage 1 would you change in hindsight?

Would have put all schemas (batch and streaming) in the same YAML file instead of batch schemas in layer_gold.yaml and streaming schemas hardcoded as StructType in stream_ingest.py. Schema definitions ended up in two places and that's a pain.

Would have integrated pipeline_state.py from the start. It exists with functions to mark stages complete but run_all.py never imports it. So if streaming crashes at file 8/12, the next run starts from Bronze again. Built-in resumability so streaming picks up from file 9 instead of reprocessing everything.

Would have used atomic writes consistently. Wrote atomic_write.py with write_delta_atomic() to handle write-to-temp-then-rename safely, but transform.py and provision.py call DataFrame.write() directly instead of using it. Streaming does atomic state writes so batch should too.

---

## Decision 3: How would you approach this differently if you had known Stage 3 was coming from the start?

Would have put all table schemas in one YAML file that both batch and streaming read from via SchemaRegistry. One source of truth, no duplication.

Would have tracked state from the beginning. run_all.py would load a state file at startup, check which stages completed, and resume or skip as needed. If streaming fails partway through, the next run picks up using stream_state.json to know which files to skip.

Would have unified the output structure. Instead of gold/ and stream_gold/, both write to the same gold/ directory. Batch writes dim_customers, dim_accounts, fact_transactions. Streaming writes current_balances, recent_transactions to the same root. Related outputs live together.

Would have designed run_all.py as a mode dispatcher from day one instead of hardcoding the sequence. Something like --mode batch, --mode stream, --mode both. That way you could re-run just batch for re-scoring without re-walking the stream poll loop.


# Nedbank DE Pipeline — Release-1

**Stage:** 1 (Bronze → Silver → Gold medallion architecture)  
**Performance:** ~52–60s (2 vCPU / 2 GB container)  
**AI Disclosure:** Leveraged AI chat for coding 

**Stage:** 2 (Bronze → Silver → Gold medallion architecture)  
**Performance:** Stage 2: 176.2s (2 vCPU / 2 GB container)  
**AI Disclosure:** Leveraged AI chat for coding 

---

## Overview

Medallion architecture medallion pipeline ingesting three sources (accounts CSV, customers CSV, transactions JSONL) → dimensions + facts for BI/reporting.

**Three layers:**

| Layer | Purpose | Output |
|-------|---------|--------|
| **Bronze** | Raw ingest + timestamp | Delta tables, unmodified |
| **Silver** | Type standardization + DQ | Typed, deduplicated, flagged |
| **Gold** | Dimensional model | dim_customers, dim_accounts, fact_transactions |

---

## Repository Structure

```
Root/
├── Dockerfile                  # Must extend nedbank-de-challenge/base:1.0
├── requirements.txt            # Python dependencies beyond base image
├── pipeline/
│   ├── __init__.py
│   ├── run_all.py             # Entry point: orchestrates Bronze → Silver → Gold
│   ├── ingest.py              # Bronze layer ingestion
│   ├── transform.py           # Bronze → Silver transformation
│   ├── provision.py           # Silver → Gold dimensional model
│   ├── schemas/
│   │   ├── base_schema.yaml   # Base schema definitions
│   │   ├── layer_silver.yaml  # Silver layer schema
│   │   └── layer_gold.yaml    # Gold layer schema
│   └── utils/
│       ├── __init__.py
│       ├── spark_session.py   # Shared SparkSession factory
│       ├── dq_rules.py        # DQ verification
│       └── schema_loader.py   # Schema loading
├── config/
│   ├── pipeline_config.yaml   # I/O paths, Spark settings
│   └── dq_rules.yaml          # DQ handling rules
├── jars/
│   ├── delta-spark_2.12-3.1.0.jar
│   └── delta-storage-3.1.0.jar
└── README.md                  # This file
```


## Execution & Development

### Prerequisites

1. **Docker** — Installed and running
2. **Base image** — `nedbank-de-challenge/base:1.0` must be pre-built:
   ```bash
   docker build -t nedbank-de-challenge/base:1.0 -f ../stage1/infrastructure/Dockerfile.base ../stage1/infrastructure/
   ```
3. **Test data** — Located at `../stage1/data/input/` (symlinked or copied locally)

### Docker CLI (Matches scoring system exactly)

**Setup:**
```bash
# Create local test data structure
mkdir -p /tmp/test-data/input /tmp/test-data/output /tmp/test-data/config

# Copy test data (adjust paths for your OS)
# Linux/Mac:
cp ../stage1/data/input/* /tmp/test-data/input/
cp config/pipeline_config.yaml /tmp/test-data/config/

# Windows (PowerShell):
Copy-Item "../stage1/data/input/*" "/tmp/test-data/input/" -Recurse
Copy-Item "config/pipeline_config.yaml" "/tmp/test-data/config/"
```

**Build:**
```bash
docker build -t nedbank-de-submission:latest .
```

**Run (local testing):**
```bash
docker run --rm \
  --memory=2g --cpus=2 \
  -v /tmp/test-data/input:/data/input \
  -v /tmp/test-data/output:/data/output \
  -v /tmp/test-data/config:/data/config \
  nedbank-de-submission:latest
```

**Run (matches scoring system exactly):**
```bash
docker run --rm \
  --network=none \
  --memory=2g --memory-swap=2g \
  --cpus=2 \
  --read-only \
  --tmpfs /tmp:rw,size=512m \
  -v /tmp/test-data:/data \
  nedbank-de-submission:latest
```

**Verify exit code:**
```bash
echo $?  # Should print 0
```

**Verify outputs exist:**
```bash
ls /tmp/test-data/output/bronze/
ls /tmp/test-data/output/silver/
ls /tmp/test-data/output/gold/
```

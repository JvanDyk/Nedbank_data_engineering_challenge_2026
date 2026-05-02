# Nedbank DE Pipeline — Release-0.1

**Stage:** 1 (Bronze → Silver → Gold medallion architecture)  
**Performance:** ~52–60s (2 vCPU / 2 GB container) on i5-9600k 3.8 Ghz   
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
Release-0.1/
├── Dockerfile                  # Must extend nedbank-de-challenge/base:1.0
├── requirements.txt            # Python dependencies beyond base image
├── pipeline/
│   ├── __init__.py
│   ├── run_all.py             # Entry point: orchestrates Bronze → Silver → Gold
│   ├── ingest.py              # Bronze layer ingestion
│   ├── transform.py           # Bronze → Silver transformation
│   ├── provision.py           # Silver → Gold dimensional model
│   └── utils/
│       ├── __init__.py
│       └── spark_session.py   # Shared SparkSession factory
├── config/
│   ├── pipeline_config.yaml   # I/O paths, Spark settings
│   └── dq_rules.yaml          # DQ handling rules (Stage 2+)
├── adr/                       # (not used at Stage 1, reserved for Stage 3)
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

### Option 1: Docker Compose (Recommended for local development)

**Setup:**
```bash
# From Release-0 directory
mkdir -p data/output
cp ../stage1/data/input/* .  # Copy or symlink test data
```

**Run:**
```bash
docker-compose up --build
```

**Verify outputs:**
```bash
ls -la data/output/bronze/
ls -la data/output/silver/
ls -la data/output/gold/
```

### Option 2: Docker CLI (Matches scoring system exactly)

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
docker build --no-cache -t candidate-submission:latest .
```

**Run (exact scoring system constraints):**
```bash
docker run --rm \
  --network=none \
  --memory=2g --memory-swap=2g \
  --cpus=2 \
  --read-only \
  --tmpfs /tmp:rw,size=512m \
  -v /tmp/test-data:/data \
  candidate-submission:latest
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

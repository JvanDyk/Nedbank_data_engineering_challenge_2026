# Nedbank DE Pipeline — Release-2

Stage: 1/2/3 (Bronze → Silver → Gold + Streaming)  
Performance: ~88s batch + ~250s streaming (2 vCPU / 2 GB container)  
AI Disclosure: Leveraged AI chat for coding

---

## How to Execute

### Prerequisites

- Docker installed and running
- Test data from stage1/data/input and stage3_delta/data/stream

### Build

```bash
cd "e:\GITHUB\SYMMETRICAL CELLULAR AUTOMATA\Nedbank\temp_challenge0.1\Release-2"
docker build -t nedbank-de-release2:latest .
```

### Setup Data (PowerShell)

```powershell
cd "e:\GITHUB\SYMMETRICAL CELLULAR AUTOMATA\Nedbank\temp_challenge0.1\Release-2"

@("input","output","stream","config") | % { mkdir "data\$_" -Force > $null }

$batch_src="e:\GITHUB\SYMMETRICAL CELLULAR AUTOMATA\Nedbank\temp_challenge0.1\stage1\data\input"
Get-ChildItem "$batch_src" -File | Copy-Item -Destination "data\input\" -Force

$stream_src="e:\GITHUB\SYMMETRICAL CELLULAR AUTOMATA\Nedbank\temp_challenge0.1\stage3_delta\data\stream"
Get-ChildItem "$stream_src" -File | Copy-Item -Destination "data\stream\" -Force

Copy-Item "config\pipeline_config.yaml","config\dq_rules.yaml" "data\config\" -Force
```

### Setup Data (Bash)

```bash
cd "/e/GITHUB/SYMMETRICAL CELLULAR AUTOMATA/Nedbank/temp_challenge0.1/Release-2"

mkdir -p data/{input,output,stream,config}

cp "/e/GITHUB/SYMMETRICAL CELLULAR AUTOMATA/Nedbank/temp_challenge0.1/stage1/data/input"/* data/input/
cp "/e/GITHUB/SYMMETRICAL CELLULAR AUTOMATA/Nedbank/temp_challenge0.1/stage3_delta/data/stream"/* data/stream/
cp config/*.yaml data/config/
```

### Run

```bash
docker run --rm \
  --memory=2g --memory-swap=2g --cpus=2 \
  --tmpfs /tmp:rw,size=512m \
  -v "$(pwd)/data:/data" \
  nedbank-de-release2:latest
```

### Verify

```bash
# Check exit code (should be 0)
echo $?

# Check outputs
ls data/output/bronze/
ls data/output/silver/
ls data/output/gold/
ls data/output/stream_gold/

# Check stream processing
cat data/output/stream_state.json
```

Expected output: Pipeline runs batch (Bronze → Silver → Gold → DQ report), then processes 12 streaming JSONL files into current_balances and recent_transactions tables. Exit code 0.

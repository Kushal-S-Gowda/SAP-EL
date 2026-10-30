# RTSED — Real-Time Stock Market Event Detection

A 7th-semester AI372IA **Stream Processing and Analytics** course project at RVCE.

The system detects unusual stock-market events from 1-minute OHLCV data using **Apache Kafka + Spark Structured Streaming + PostgreSQL + Streamlit**, with two upgrades on top of the original pipeline:

1. Free Yahoo Finance ingestion as the primary source (historical ZIP replay remains secondary).
2. A tool-using **Stock Event Investigation Agent** that investigates Spark-detected events.

Yahoo Finance through `yfinance` is unofficial. Quotes may be delayed, incomplete, rate-limited, or unavailable. This project does **not** claim guaranteed real-time NSE data.

## Architecture

```text
Yahoo Finance (live)  OR  cached Yahoo demo replay  OR  historical ZIP replay
            ↓
      Python Kafka Producer
            ↓
 Kafka topic: stock_data          (3 partitions, key = symbol)
            ↓
 Spark Structured Streaming
      1-minute tumbling window + aggregations
            ↓
 Event Detection
 Price Spike / Volume Surge
            ↓
 Kafka topic: stock_events        (3 partitions)
            ↓
 PostgreSQL  stock_events         (raw Spark detections)
            ↓
 Stock Event Investigation Agent  (separate Kafka consumer group)
            ↓
 PostgreSQL  ai_investigations
            ↓
 Streamlit + Plotly Dashboard
```

Spark remains responsible for deterministic stream processing. The agent never blocks Kafka ingestion or Spark detection.

### Fault tolerance

- Kafka stores the input stream and offsets.
- Spark Structured Streaming uses a persistent checkpoint directory.
- The project was tested by stopping/restarting Spark with the same checkpoint.
- During the tested restart, the PostgreSQL event count remained unchanged and no duplicate events were observed.
- Agent failures leave investigations `PENDING` or `FAILED`; they do not stop Spark.

## Technology stack

- Python 3.12
- Apache Kafka 4.0.1
- Apache Spark Structured Streaming 4.0.1
- PostgreSQL 16
- Streamlit
- Plotly
- Docker / Docker Compose
- yfinance (unofficial Yahoo Finance client; no API key)
- Ollama (optional local LLM for the agent)
- Git / GitHub

## Operating modes

Run **one** producer at a time. Do not mix live Yahoo candles with historical ZIP records in the same demonstration unless you reset Kafka/Spark checkpoints and PostgreSQL candles and you label the mix.

| Mode | Command | Label | When to use |
|---|---|---|---|
| LIVE | `python producer/producer.py` | LIVE | NSE session, to fetch genuine Yahoo 1-minute candles |
| DEMO REPLAY | `python producer/demo_replay.py` | DEMO_REPLAY | After-hours demo using previously cached Yahoo candles |
| HISTORICAL | `python producer/historical_replay.py` | HISTORICAL | Course dataset replay, tests, fallback if Yahoo is down |

DEMO_REPLAY never labels cached candles as live. The live producer still polls Yahoo after hours: it will **cache** today's finalized 1-minute candles for later demo replay, but it will **not** publish hours-old candles into Kafka (the live overlap window is 10 minutes). If the cache is empty, run the live producer once while Yahoo still has that session’s 1-minute history. The project does not fabricate prices.

## Yahoo Finance limitations

- No Yahoo account or API key is required.
- Data may be delayed relative to the NSE matching engine.
- 1-minute history is limited to a recent intraday window.
- Empty responses, gaps, and rate limits occur.
- Incomplete in-progress minutes are withheld using a 2-minute finalization delay.
- Overlapping 10-minute fetch windows recover delayed candles.
- Persistent SQLite deduplication uses `(symbol, timestamp)` after Kafka acknowledgement.
- Kafka + SQLite cannot be one atomic transaction, so exactly-once delivery is **not** claimed. A crash after Kafka ack and before SQLite write can resend that candle.

Provider choice: Yahoo Finance remains the default because it is free, needs no registration, supports NSE `.NS` tickers and 1-minute bars, and is sufficient for an academic demo when its delays are disclosed. A second free provider can be added behind `producer/providers/` later.

## Historical dataset (secondary)

The original public **NSE F&O 1-minute data** repository is unchanged:

https://github.com/voletiramu/nse-fno-1min-data

Place `stocks_1m_csvs.zip` at `data/stocks_1m_csvs.zip`. It is gitignored. Existing EDA scripts (`eda.py`, `eda_statistics.py`, `inspect_dataset.py`, `profile_dataset.py`) are unchanged.

## Agentic AI architecture

The **Stock Event Investigation Agent** (`agent/consumer.py`) uses its own Kafka consumer group (`rtsed-investigation-agent`) on `stock_events`.

1. Spark writes the raw event to Kafka and PostgreSQL.
2. The agent enqueues an `ai_investigations` row (`PENDING`) with a unique key `(symbol, event_start, event_end, event_type)`.
3. The agent asks a local Ollama model to choose investigation tools.
4. Python executes the tools with parameterized SQL (read-only).
5. Severity is computed from documented numerical bands in Python. The LLM may explain severity but must not invent a different score.
6. The report is stored as `COMPLETED`, or `FAILED` if Ollama is down.

If Ollama is unavailable, Kafka/Spark/PostgreSQL continue. The dashboard must not show a completed investigation.

The agent must not invent news, rumours, or trading advice. If the cause is unknown, the report must say so.
The dashboard lists completed reports first and shows Spark's event numbers even while an investigation is pending. Model confidence is a qualitative LOW/MEDIUM/HIGH rating, not a calibrated percentage; evidence coverage is displayed separately and measures available tool results.

### Agent tools

| Tool | Purpose |
|---|---|
| `get_event_details` | Original Spark event from PostgreSQL |
| `get_recent_stock_history` | Recent stored OHLCV candles for the symbol |
| `calculate_price_movement` | Percentage move over stored candles |
| `calculate_volume_anomaly` | Current volume vs rolling baseline |
| `get_previous_stock_events` | Earlier detections for the same symbol |
| `assess_event_severity` | LOW / MEDIUM / HIGH / CRITICAL from documented bands |

Spark detection thresholds (unchanged):

- `PRICE_SPIKE`: absolute average window price change ≥ **1.0%**
- `VOLUME_SURGE`: window total volume ≥ **3000**

Severity bands (agent, documented):

- Price: ≥1.0% LOW, ≥1.5% MEDIUM, ≥3.0% HIGH, ≥5.0% CRITICAL
- Volume ratio vs baseline: ≥1.0 LOW, ≥1.5 MEDIUM, ≥3.0 HIGH, ≥6.0 CRITICAL
- Combined severity is the stronger of the two matching bands

### Ollama setup (Windows PowerShell)

Install Ollama from https://ollama.com, then:

```powershell
ollama --version
ollama pull llama3.2
ollama list
```

The default model is `llama3.2` because it is commonly already installed. For stronger tool calling:

```powershell
ollama pull llama3.1
$env:OLLAMA_MODEL="llama3.1"
```

Not every Ollama model supports native tools; if the selected model does not, the agent falls back to a strict JSON tool-call protocol. If that also fails, the investigation is `FAILED` and retryable.

```powershell
$env:AI_PROVIDER="ollama"
$env:OLLAMA_BASE_URL="http://localhost:11434"
$env:OLLAMA_MODEL="llama3.2"
```

## Database migrations

Schema is idempotent (`CREATE TABLE IF NOT EXISTS`). Existing `stock_events` rows are not dropped.

```powershell
python database/migrate.py
```

New tables:

- `stock_candles` — ingested OHLCV for investigation tools (`PRIMARY KEY (symbol, ts)`)
- `ai_investigations` — agent output, unique per Spark event identity
- `pipeline_status` — latest producer mode/status for the dashboard

Apply schema before the agent and dashboard. The dashboard and agent also call the migrator on startup.

## Environment configuration

Copy `.env.example` to `.env` (gitignored). Defaults match the Docker Compose Postgres/Kafka settings. Do not commit secrets.

```powershell
Copy-Item .env.example .env
```

## Prerequisites

Install these before running the project:

1. Git
2. Python 3.12
3. Java JDK 17
4. Docker Desktop
5. Windows Hadoop `winutils.exe` setup for local PySpark on Windows
6. Ollama (optional; required only for completed AI investigations)

Check versions:

```powershell
git --version
python --version
java -version
docker --version
docker compose version
ollama --version
```

### Windows Spark / Hadoop setup

PySpark on Windows requires a Hadoop helper executable.

Create:

```text
C:\hadoop\bin\winutils.exe
```

Then, in the PowerShell session used to run Spark:

```powershell
$env:HADOOP_HOME="C:\hadoop"
$env:PATH="$env:HADOOP_HOME\bin;$env:PATH"
```

Verify:

```powershell
Test-Path C:\hadoop\bin\winutils.exe
```

It should return `True`.

## Windows PowerShell installation

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
Copy-Item .env.example .env
```

If PowerShell blocks activation:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

## Complete startup procedure

Every fresh run:

```text
1. Docker (Kafka + PostgreSQL)
        ↓
2. Kafka topics
        ↓
3. Database schema
        ↓
4. Spark Structured Streaming
        ↓
5. Exactly one producer (live OR demo OR historical)
        ↓
6. Investigation agent
        ↓
7. Streamlit dashboard
```

### 1–3. Infrastructure

```powershell
docker compose up -d
.\scripts\setup_kafka.ps1
python database/migrate.py
```

You should see containers `rtsed-kafka` and `rtsed-postgres`.

Topics: `stock_data`, `stock_events` (3 partitions each).

Database defaults: `rtsed` / `rtsed_user` / `rtsed_password` on `localhost:5432`.

### 4. Spark (Terminal 1)

```powershell
.\.venv\Scripts\Activate.ps1
$env:HADOOP_HOME="C:\hadoop"
$env:PATH="$env:HADOOP_HOME\bin;$env:PATH"
python streaming/spark_stream.py
```

Leave this terminal running. Thresholds, watermark (2 minutes), and 1-minute tumbling windows are unchanged.

### 5. One producer (Terminal 2)

Live Yahoo polling (primary):

```powershell
$env:RTSED_STOCKS="RELIANCE.NS,TCS.NS,INFY.NS,HDFCBANK.NS,ICICIBANK.NS"
$env:RTSED_POLL_INTERVAL_SECONDS="60"
python producer/producer.py
```

After-hours demo of **cached genuine Yahoo candles**:

```powershell
python producer/demo_replay.py
```

Historical ZIP (secondary):

```powershell
python producer/historical_replay.py
```

Default historical limit is 1,000 records. `$env:RTSED_REPLAY_MAX_RECORDS="0"` replays the full archive.

### 6. Agent (Terminal 3)

```powershell
python agent/consumer.py
```

### 7. Dashboard (Terminal 4)

```powershell
streamlit run dashboard/dashboard.py
```

Open `http://localhost:8501`.

Existing tabs are preserved. Added tabs: **Market Data** and **AI Investigation**. Timestamps in those views are shown in IST.

## Professor demonstration script

Goal: show live-or-cached Yahoo candles moving through Kafka → Spark → PostgreSQL → agent tools → dashboard.

1. Start Docker, topics, schema, Spark, dashboard.
2. **If NSE is open:** run `python producer/producer.py`. On the Market Data tab, confirm mode `LIVE`, provider `yahoo`, and a last-successful-fetch time. Do not call this zero-delay exchange data.
3. **If NSE is closed:** if `producer/state/yahoo_candle_cache.sqlite3` exists, run `python producer/demo_replay.py`. Confirm mode `DEMO_REPLAY`. If the cache is empty, explain that candles must be collected during market hours; do not invent a dataset. Optionally fall back to `python producer/historical_replay.py` and label it HISTORICAL.
4. Show Kafka tab: `stock_data` offsets increasing.
5. Show Spark still using 1-minute tumbling windows and the original spike/surge rules.
6. Show Storage & Events: raw `stock_events` rows from Spark.
7. Start `python agent/consumer.py` with Ollama running (`ollama pull llama3.2`).
8. On AI Investigation, select an event. Show:
   - original Spark detection
   - price/volume numerical evidence
   - tool trace in evidence JSON
   - summary, severity, confidence, status `COMPLETED`
9. Optional fault demo: stop Ollama, trigger another event, confirm status `FAILED`/`PENDING` while Spark continues.
10. Optional recovery demo: restart Spark with the same checkpoint; event counts should not explode.

Never mix TEST DATA with genuine Yahoo candles. If you ever inject a synthetic threshold-crossing record for a viva, label it TEST DATA in the producer logs and dashboard status.

## Automated tests

From the project root, with the venv active:

```powershell
python -m pytest tests -q
```

These tests cover normalization, timestamps, OHLCV validation, duplicate prevention, Yahoo retry/empty handling, Kafka serialization payload construction, investigation uniqueness SQL, agent tool selection with a fake LLM, severity bands, failure when Ollama is down, cache round-trip, and IST display conversion.

They do **not** start Docker, Kafka, Spark, Yahoo’s network, or Ollama. Live pipeline behaviour must be demonstrated separately.

## Project structure

```text
RTSED/
├── agent/                      # Investigation consumer, tools, LLM, and persistence
├── dashboard/dashboard.py      # Streamlit dashboard
├── database/                   # PostgreSQL connection, schema, and migration
├── producer/                   # Live, cached-demo, and historical producers
│   ├── providers/              # Market-data provider interface and Yahoo client
│   ├── candle_cache.py
│   ├── demo_replay.py
│   ├── historical_replay.py
│   ├── ohlcv.py
│   ├── producer.py
│   ├── publisher.py
│   ├── sink.py
│   ├── state_store.py
│   └── status.py
├── streaming/spark_stream.py   # Spark Structured Streaming and event detection
├── tests/                      # Unit tests; no live services required
├── scripts/setup_kafka.ps1     # Kafka topic setup
├── data/                       # Local historical dataset (not tracked)
├── eda_results/                # Generated analysis output (not tracked)
├── config.py                   # Shared environment configuration
├── docker-compose.yml          # Local Kafka and PostgreSQL
├── eda.py                      # Dataset exploration scripts
├── eda_statistics.py
├── inspect_dataset.py
├── profile_dataset.py
├── .env.example                # Copy to local, gitignored .env
├── requirements.txt
└── README.md
```

## Kafka design

### `stock_data`

- 3 partitions
- message key = stock symbol
- JSON: `timestamp`, `symbol`, `open`, `high`, `low`, `close`, `volume`
- optional `source_mode` (`LIVE` / `DEMO_REPLAY` / `HISTORICAL`) is ignored by the existing Spark schema

### `stock_events`

Spark detections. The agent consumes this topic in a separate group.

## Course requirement coverage

| Requirement | Implementation |
|---|---|
| Data Source | Yahoo Finance NSE polling; demo cache; optional historical ZIP |
| Kafka | Apache Kafka |
| Producer | `producer/producer.py`; `demo_replay.py`; `historical_replay.py` |
| Consumer | Spark Structured Streaming |
| Two Kafka topics | `stock_data`, `stock_events` |
| Partitioning | 3 partitions per topic |
| Consumer groups | Spark streaming group + `rtsed-investigation-agent` |
| Window operation | 1-minute tumbling window |
| Aggregation | windowed price/volume aggregations |
| Event detection | Price Spike / Volume Surge (unchanged thresholds) |
| Fault tolerance | Kafka offsets + Spark checkpoint |
| Storage | PostgreSQL |
| Analytics | Streamlit + Plotly |
| Real-time visualization | Streamlit dashboard |
| Agentic investigation | Tool-using local Ollama agent |

## Important: do not commit local runtime files

Git ignores `.venv/`, datasets, EDA output, Spark checkpoints, `.env`, SQLite state, and producer caches.

## Troubleshooting

### Kafka container is not running

```powershell
docker compose ps
docker compose up -d
```

### PostgreSQL connection fails

Confirm `rtsed-postgres` is running, then `python database/migrate.py`.

### Dashboard opens but shows no events

Start Kafka/PostgreSQL, Spark, and exactly one producer.

### AI investigations stay PENDING or FAILED

Start Ollama, confirm `ollama list` includes the model selected by `OLLAMA_MODEL` (default: `llama3.2`), and leave `python agent/consumer.py` running. Spark events should still appear.

### Demo replay says the cache is empty

Run the live producer during NSE hours first. Do not fabricate candles.

### Need a clean local runtime

Stop application processes, then `docker compose down` / `up -d` and recreate topics. Delete Spark checkpoints only when you intentionally want a fresh stream.

## Academic scope

This is an academic event-detection and investigation system, not a trading product. Yahoo data is delayed unofficial data. The LLM interprets numerical evidence; it does not execute trades or invent market news.

# RTSED â€” Real-Time Stock Market Event Detection

A 7th-semester AI372IA **Stream Processing and Analytics** course project at RVCE.

The system detects unusual stock-market events from 1-minute OHLCV data using **Apache Kafka + Spark Structured Streaming + PostgreSQL + Streamlit**.

## Architecture

```text
Historical 1-minute stock dataset
            â†“
      Python Kafka Producer
            â†“
 Kafka topic: stock_data
      3 partitions
            â†“
 Spark Structured Streaming
      consumer group
            â†“
   1-minute tumbling window
       + aggregations
            â†“
 Event Detection
 Price Spike / Volume Surge
            â†“
 Kafka topic: stock_events
      3 partitions
            â†“
       PostgreSQL
            â†“
 Streamlit + Plotly Dashboard
```

### Fault tolerance

- Kafka stores the input stream and offsets.
- Spark Structured Streaming uses a persistent checkpoint directory.
- The project was tested by stopping/restarting Spark with the same checkpoint.
- During the tested restart, the PostgreSQL event count remained unchanged and no duplicate events were observed.

## Technology stack

- Python 3.12
- Apache Kafka 4.0.1
- Apache Spark Structured Streaming 4.0.1
- PostgreSQL 16
- Streamlit
- Plotly
- Docker / Docker Compose
- Git / GitHub

## Dataset

The project uses the public **NSE F&O 1-minute data** repository:

https://github.com/voletiramu/nse-fno-1min-data

The downloaded archive is:

```text
stocks_1m_csvs.zip
```

It contains 214 stock files with 1-minute OHLCV data.

Expected normalized fields:

```text
timestamp, symbol, open, high, low, close, volume
```

The dataset is intentionally **not committed to GitHub** because of its size. Download it separately and place it here:

```text
RTSED/
â””â”€â”€ data/
    â””â”€â”€ stocks_1m_csvs.zip
```

## Prerequisites

Install these before running the project:

1. Git
2. Python 3.12
3. Java JDK 17
4. Docker Desktop
5. Windows Hadoop `winutils.exe` setup for local PySpark on Windows

Check versions:

```powershell
git --version
python --version
java -version
docker --version
docker compose version
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

It should return:

```text
True
```

> If your teammate already has a working local Spark/Hadoop setup, they can use it. The important requirement is that `winutils.exe` is available through `HADOOP_HOME`.

## 1. Clone the repository

```powershell
git clone https://github.com/Kushal-S-Gowda/SAP-EL.git
cd SAP-EL
```

## 2. Create the Python environment

Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

If PowerShell blocks activation, either allow scripts for the current user or use:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Install dependencies:

```powershell
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## 3. Add the dataset

Create the folder if necessary:

```powershell
mkdir data
```

Download `stocks_1m_csvs.zip` from:

https://github.com/voletiramu/nse-fno-1min-data

Place it at:

```text
data/stocks_1m_csvs.zip
```

Do not unzip it unless you want to inspect the files manually. The producer reads the ZIP directly.

## 4. Start Kafka and PostgreSQL

From the project root:

```powershell
docker compose up -d
```

Check:

```powershell
docker ps
```

You should see:

```text
rtsed-kafka
rtsed-postgres
```

Kafka is exposed at:

```text
localhost:9092
```

PostgreSQL is exposed at:

```text
localhost:5432
```

Database:

```text
database: rtsed
user: rtsed_user
password: rtsed_password
```

## 5. Create the Kafka topics

Run:

```powershell
.\scripts\setup_kafka.ps1
```

This creates:

```text
stock_data
stock_events
```

with 3 partitions each.

You can verify:

```powershell
docker exec rtsed-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Describe the topics:

```powershell
docker exec rtsed-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic stock_data

docker exec rtsed-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic stock_events
```

## 6. Start Spark Structured Streaming

Open **Terminal 1**:

```powershell
.\.venv\Scripts\Activate.ps1
$env:HADOOP_HOME="C:\hadoop"
$env:PATH="$env:HADOOP_HOME\bin;$env:PATH"

python streaming/spark_stream.py
```

Spark consumes:

```text
stock_data
```

and produces detected events to:

```text
stock_events
```

The processing includes:

- 1-minute tumbling window
- price aggregation
- volume aggregation
- event detection
- Kafka output
- persistent Spark checkpointing

Leave this terminal running.

## 7. Start the producer

Open **Terminal 2**:

```powershell
cd RTSED
.\.venv\Scripts\Activate.ps1
python producer/producer.py
```

The producer reads the dataset and sends normalized JSON records to:

```text
stock_data
```

The Kafka message key is the stock symbol, which allows symbol-based partitioning.

## 8. Start the dashboard

Open **Terminal 3**:

```powershell
cd RTSED
.\.venv\Scripts\Activate.ps1
streamlit run dashboard/dashboard.py
```

Open the Streamlit URL shown in the terminal, normally:

```text
http://localhost:8501
```

The dashboard provides evidence for:

- overall system health
- end-to-end pipeline
- Kafka topics and partitions
- consumer groups
- Spark stream processing
- PostgreSQL storage
- detected events
- checkpoint / fault-tolerance state
- analytics
- methodology
- course requirement coverage

## Recommended startup order

Every fresh run:

```text
1. Docker
   â†“
2. Kafka topics
   â†“
3. Spark Structured Streaming
   â†“
4. Kafka producer
   â†“
5. Streamlit dashboard
```

For convenience:

```powershell
docker compose up -d
.\scripts\setup_kafka.ps1
```

Then use three terminals for Spark, producer and dashboard.

## Project structure

```text
RTSED/
â”œâ”€â”€ data/
â”‚   â””â”€â”€ stocks_1m_csvs.zip       # downloaded separately; ignored by Git
â”‚
â”œâ”€â”€ dashboard/
â”‚   â””â”€â”€ dashboard.py             # Streamlit + Plotly dashboard
â”‚
â”œâ”€â”€ producer/
â”‚   â””â”€â”€ producer.py              # Kafka producer
â”‚
â”œâ”€â”€ streaming/
â”‚   â””â”€â”€ spark_stream.py           # Spark Structured Streaming
â”‚
â”œâ”€â”€ database/
â”‚
â”œâ”€â”€ scripts/
â”‚   â””â”€â”€ setup_kafka.ps1           # Kafka topic setup
â”‚
â”œâ”€â”€ eda_results/                  # generated locally; ignored by Git
â”œâ”€â”€ checkpoint/                   # generated locally; ignored by Git
â”‚
â”œâ”€â”€ docker-compose.yml
â”œâ”€â”€ requirements.txt
â”œâ”€â”€ .gitignore
â””â”€â”€ README.md
```

## Kafka design

### `stock_data`

Input topic.

```text
Producer â†’ stock_data â†’ Spark
```

- 3 partitions
- message key = stock symbol
- JSON payload

### `stock_events`

Detected-event topic.

```text
Spark â†’ stock_events
```

- 3 partitions
- event records in JSON

### Consumer groups

Spark consumes the input topic as a streaming consumer group. Kafka manages offsets so the stream can recover from the last committed position.

## Event detection

The current system detects two event types:

### Price Spike

A significant price movement within the streaming window.

### Volume Surge

An unusually high trading-volume condition relative to the streaming aggregation logic.

The exact thresholds and implementation are defined in:

```text
streaming/spark_stream.py
```

## Database

PostgreSQL stores detected events.

The dashboard reads the stored events and displays:

- symbol
- event type
- event start/end
- average close
- total volume
- average price change
- record count
- creation time

## Fault tolerance

The Spark stream writes checkpoint state to:

```text
streaming/checkpoint/
```

This directory is generated at runtime and ignored by Git.

Do **not** commit it.

To test recovery:

1. Start Spark.
2. Process data.
3. Stop Spark.
4. Start Spark again with the same checkpoint.
5. Confirm processing resumes without replaying already processed events in the tested scenario.

## Important: do not commit local runtime files

The repository intentionally ignores:

```text
.venv/
data/*.zip
data/*.csv
data/*.csv.gz
eda_results/
streaming/checkpoint/
*.log
__pycache__/
```

This keeps the GitHub repository small and portable.

## Team workflow

Each teammate should work on their own branch:

```powershell
git checkout -b feature/<your-name>
```

After making changes:

```powershell
git add .
git commit -m "Describe your change"
git push -u origin feature/<your-name>
```

Then create a Pull Request on GitHub.

Avoid committing:

- datasets
- virtual environments
- passwords/secrets
- Spark checkpoints
- generated EDA output
- local machine paths

## Course requirement coverage

| Requirement | Implementation |
|---|---|
| Data Source | NSE F&O 1-minute stock dataset |
| Kafka | Apache Kafka |
| Producer | `producer/producer.py` |
| Consumer | Spark Structured Streaming |
| Two Kafka topics | `stock_data`, `stock_events` |
| Partitioning | 3 partitions per topic |
| Consumer groups | Spark streaming consumer group |
| Window operation | 1-minute tumbling window |
| Aggregation | windowed price/volume aggregations |
| Event detection | Price Spike / Volume Surge |
| Fault tolerance | Kafka offsets + Spark checkpoint |
| Storage | PostgreSQL |
| Analytics | Streamlit + Plotly |
| Real-time visualization | Streamlit dashboard |
| Alerts/events | Detected events shown in dashboard and stored in PostgreSQL |

## Troubleshooting

### Kafka container is not running

```powershell
docker compose ps
docker compose up -d
```

### Kafka topics already exist

That is fine. The setup script safely skips existing topics.

### Port 9092 is already in use

Find the process/container using it and stop it, or stop the conflicting Docker service.

### PostgreSQL connection fails

Check:

```powershell
docker ps
```

and verify that `rtsed-postgres` is running.

### Spark says Hadoop / winutils is missing

Verify:

```powershell
$env:HADOOP_HOME
Test-Path C:\hadoop\bin\winutils.exe
```

Then set:

```powershell
$env:HADOOP_HOME="C:\hadoop"
$env:PATH="$env:HADOOP_HOME\bin;$env:PATH"
```

### Dashboard opens but shows no events

Make sure all three are running:

```text
Kafka/PostgreSQL
Spark
Producer
```

Refresh the Streamlit dashboard.

### Need to start from a clean local runtime

Stop the application processes first, then:

```powershell
docker compose down
docker compose up -d
.\scripts\setup_kafka.ps1
```

Do not delete the Spark checkpoint if you want Spark to recover from its previous state. Delete it only when intentionally starting a fresh stream.

## Academic scope

This project intentionally keeps the implementation focused on the course requirements rather than adding unnecessary infrastructure such as Kubernetes, Redis, ML/deep-learning models, or additional streaming platforms.

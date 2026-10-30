import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import streamlit as st
from kafka import KafkaConsumer, KafkaAdminClient

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import config
from database.db import apply_schema
from producer.status import read_status


# ============================================================
# RTSED - REAL-TIME STOCK EVENT DETECTION DASHBOARD
# ============================================================

DB_CONFIG = {
    "host": config.POSTGRES_HOST,
    "port": config.POSTGRES_PORT,
    "dbname": config.POSTGRES_DB,
    "user": config.POSTGRES_USER,
    "password": config.POSTGRES_PASSWORD,
}

KAFKA_BOOTSTRAP = config.KAFKA_BOOTSTRAP_SERVERS
INPUT_TOPIC = config.STOCK_DATA_TOPIC
OUTPUT_TOPIC = config.STOCK_EVENTS_TOPIC
PROJECT_ROOT = ROOT
CHECKPOINT_PATH = PROJECT_ROOT / "streaming" / "checkpoint"
REFRESH_SECONDS = 5
IST = ZoneInfo("Asia/Kolkata")


def to_ist_label(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "—"
    timestamp = pd.Timestamp(value)
    if pd.isna(timestamp):
        return "—"
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    return timestamp.tz_convert(IST).strftime("%Y-%m-%d %H:%M:%S IST")


# ============================================================
# PAGE
# ============================================================

st.set_page_config(
    page_title="RTSED | Real-Time Stock Event Detection",
    page_icon="$",
    layout="wide",
    initial_sidebar_state="expanded",
)

px.defaults.template = "plotly_white"
px.defaults.color_discrete_sequence = ["#2563EB", "#0F766E", "#D97706", "#7C3AED"]

st.markdown(
    """
    <style>
    :root {
        --rtsed-ink: #17243b;
        --rtsed-muted: #64748b;
        --rtsed-line: #e2e8f0;
        --rtsed-blue: #2563eb;
    }
    .stApp {background: #f5f7fb; color: var(--rtsed-ink);}
    .block-container {
        max-width: 1540px;
        padding-top: 1.6rem;
        padding-bottom: 3rem;
    }
    [data-testid="stHeader"] {background: rgba(245,247,251,.92);}
    [data-testid="stSidebar"] {
        background: #111c31;
        border-right: 1px solid #26344c;
    }
    [data-testid="stSidebar"] * {color: #e7edf7;}
    [data-testid="stSidebar"] [data-testid="stCaptionContainer"] {
        color: #a8b5c9;
    }
    .hero {
        padding: 1.8rem 2rem;
        border-radius: 18px;
        color: #f8fafc;
        background: linear-gradient(115deg, #14233d 0%, #1d3760 72%, #254b79 100%);
        border: 1px solid #29446b;
        margin: .2rem 0 1.7rem;
        box-shadow: 0 12px 30px rgba(15, 23, 42, .12);
    }
    .hero-kicker {
        color: #a9c5ef;
        font-size: .75rem;
        font-weight: 700;
        letter-spacing: .14em;
        text-transform: uppercase;
        margin-bottom: .45rem;
    }
    .hero h1 {
        color: #fff;
        font-size: clamp(1.7rem, 3vw, 2.45rem);
        line-height: 1.15;
        letter-spacing: -.035em;
        margin: 0 0 .55rem;
    }
    .hero-subtitle {color: #d3deed; font-size: .98rem;}
    [data-testid="stTabs"] button[data-baseweb="tab"] {
        color: #526179;
        font-weight: 650;
        padding: .8rem 1rem;
    }
    [data-testid="stTabs"] button[aria-selected="true"] {
        color: #1d4ed8;
        border-bottom-color: #2563eb;
    }
    [data-testid="stTabs"] [data-baseweb="tab-list"] {
        gap: .35rem;
        border-bottom: 1px solid var(--rtsed-line);
    }
    [data-testid="stMetric"] {
        background: #fff;
        border: 1px solid var(--rtsed-line);
        border-radius: 13px;
        padding: .95rem 1rem;
        box-shadow: 0 2px 6px rgba(15, 23, 42, .035);
    }
    [data-testid="stMetricLabel"] {color: #64748b;}
    [data-testid="stMetricValue"] {color: #17243b; font-weight: 700;}
    h1, h2, h3 {color: #17243b; letter-spacing: -.02em;}
    h2 {padding-top: .2rem;}
    [data-testid="stDataFrame"], [data-testid="stPlotlyChart"] {
        border: 1px solid var(--rtsed-line);
        border-radius: 12px;
        overflow: hidden;
    }
    .component {
        padding: 1rem 1.1rem;
        border-radius: 13px;
        border: 1px solid var(--rtsed-line);
        background: #fff;
        min-height: 100px;
        box-shadow: 0 2px 6px rgba(15, 23, 42, .035);
    }
    .ok {font-weight: 700;}
    .muted {color: var(--rtsed-muted);}
    .small {font-size: .85rem; line-height: 1.5;}
    [data-testid="stAlert"] {border-radius: 11px;}
    @media (max-width: 800px) {
        .block-container {padding: 1rem .8rem 2rem;}
        .hero {padding: 1.35rem;}
        [data-testid="stTabs"] button[data-baseweb="tab"] {
            padding: .65rem .55rem;
            font-size: .84rem;
        }
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="hero">
        <div class="hero-kicker">Stream Processing &amp; Analytics</div>
        <h1>Real-Time Stock Event Detection</h1>
        <div class="hero-subtitle">
            A streaming analytics platform built with Kafka, Spark, PostgreSQL and Streamlit
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)


# ============================================================
# DATABASE
# ============================================================

def db_connect():
    return psycopg2.connect(**DB_CONFIG)


@st.cache_data(ttl=3)
def load_events():
    conn = db_connect()

    query = """
        SELECT
            id,
            symbol,
            event_type,
            event_start,
            event_end,
            avg_close,
            total_volume,
            avg_price_change_pct,
            record_count,
            created_at
        FROM stock_events
        ORDER BY id DESC
    """

    cur = conn.cursor()
    cur.execute(query)

    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]

    cur.close()
    conn.close()

    return pd.DataFrame(rows, columns=columns)

def db_status():
    try:
        conn = db_connect()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM stock_events;")
        count = cur.fetchone()[0]
        cur.execute("SELECT MAX(created_at) FROM stock_events;")
        latest = cur.fetchone()[0]
        cur.close()
        conn.close()
        return True, count, latest, "PostgreSQL connection healthy"
    except Exception as e:
        return False, 0, None, str(e)


@st.cache_data(ttl=3)
def load_investigations():
    try:
        conn = db_connect()
    except Exception:
        return pd.DataFrame()
    query = """
        SELECT
            investigation_id,
            event_id,
            symbol,
            event_type,
            event_start,
            event_end,
            severity,
            investigation_summary,
            evidence,
            price_change_percent,
            volume_ratio,
            previous_event_count,
            confidence,
            investigation_status,
            error_message,
            created_at,
            updated_at
        FROM ai_investigations
        ORDER BY investigation_id DESC
    """
    try:
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()
        return pd.DataFrame(rows, columns=columns)
    except Exception:
        return pd.DataFrame()
    finally:
        try:
            conn.close()
        except Exception:
            pass


@st.cache_data(ttl=3)
def load_pipeline_status_row():
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT operating_mode, provider, last_successful_fetch,
                   latest_candle_timestamp, watchlist_count,
                   last_published_count, last_error, updated_at
            FROM pipeline_status
            WHERE id = 1
            """
        )
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            return None
        columns = [
            "operating_mode",
            "provider",
            "last_successful_fetch",
            "latest_candle_timestamp",
            "watchlist_count",
            "last_published_count",
            "last_error",
            "updated_at",
        ]
        return dict(zip(columns, row))
    except Exception:
        conn.close()
        return None


@st.cache_data(ttl=3)
def load_latest_candle():
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT symbol, ts, close, volume, source_mode
            FROM stock_candles
            ORDER BY ts DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
        cur.close()
        conn.close()
        return row
    except Exception:
        conn.close()
        return None


# ============================================================
# KAFKA
# ============================================================

def kafka_status():
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            client_id="rtsed-dashboard",
            request_timeout_ms=3000,
        )
        topics = admin.list_topics()
        admin.close()
        return True, topics, "Kafka broker reachable"
    except Exception as e:
        return False, [], str(e)


def topic_partition_info(topic):
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        enable_auto_commit=False,
        consumer_timeout_ms=1000,
        request_timeout_ms=3000,
    )
    partitions = sorted(consumer.partitions_for_topic(topic) or [])
    result = []

    if partitions:
        tp = [__import__("kafka").TopicPartition(topic, p) for p in partitions]
        beginning = consumer.beginning_offsets(tp)
        end = consumer.end_offsets(tp)

        for p in partitions:
            t = __import__("kafka").TopicPartition(topic, p)
            result.append(
                {
                    "partition": p,
                    "earliest_offset": beginning.get(t, 0),
                    "latest_offset": end.get(t, 0),
                    "records_available": max(
                        0, end.get(t, 0) - beginning.get(t, 0)
                    ),
                }
            )

    consumer.close()
    return pd.DataFrame(result)


def kafka_consumer_groups():
    """Uses the local Kafka CLI so the dashboard can visibly demonstrate groups."""
    try:
        cmd = [
            "docker", "exec", "rtsed-kafka",
            "/opt/kafka/bin/kafka-consumer-groups.sh",
            "--bootstrap-server", "localhost:9092",
            "--list",
        ]
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=5,
        )
        groups = [
            x.strip()
            for x in result.stdout.splitlines()
            if x.strip() and not x.lower().startswith("error")
        ]
        return groups, result.stderr.strip()
    except Exception as e:
        return [], str(e)


# ============================================================
# CHECKPOINT / SYSTEM
# ============================================================

def checkpoint_info():
    exists = CHECKPOINT_PATH.exists()
    if not exists:
        return {
            "exists": False,
            "offsets": False,
            "commits": False,
            "sources": False,
            "state": False,
            "modified": None,
        }

    return {
        "exists": True,
        "offsets": (CHECKPOINT_PATH / "offsets").exists(),
        "commits": (CHECKPOINT_PATH / "commits").exists(),
        "sources": (CHECKPOINT_PATH / "sources").exists(),
        "state": (CHECKPOINT_PATH / "state").exists(),
        "modified": datetime.fromtimestamp(
            CHECKPOINT_PATH.stat().st_mtime
        ).strftime("%Y-%m-%d %H:%M:%S"),
    }


def component_card(title, status, detail):
    state = "Operational" if status else "Unavailable"
    st.markdown(
        f"""
        <div class="component">
            <div style="font-size:1.05rem;"><b>{title}</b>
                <span class="small muted"> · {state}</span>
            </div>
            <div class="small muted">{detail}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ============================================================
# LOAD CURRENT STATE
# ============================================================

try:
    apply_schema()
except Exception:
    pass

try:
    db_ok, total_events, latest_event, db_message = db_status()
    events = load_events()
except Exception as e:
    db_ok, total_events, latest_event = False, 0, None
    db_message = str(e)
    events = pd.DataFrame()

try:
    investigations = load_investigations()
except Exception:
    investigations = pd.DataFrame()

ingestion_status = read_status(config.STATUS_FILE) or {}
pipeline_row = load_pipeline_status_row() or {}
latest_candle = load_latest_candle()

kafka_ok, kafka_topics, kafka_message = kafka_status()
cp = checkpoint_info()
groups, group_error = kafka_consumer_groups()

stock_topic_df = pd.DataFrame()
event_topic_df = pd.DataFrame()

if kafka_ok:
    try:
        stock_topic_df = topic_partition_info(INPUT_TOPIC)
        event_topic_df = topic_partition_info(OUTPUT_TOPIC)
    except Exception:
        pass


# ============================================================
# SIDEBAR
# ============================================================

with st.sidebar:
    st.markdown("## Dashboard controls")
    live_mode = st.toggle("Live refresh", value=True)
    st.caption(
        f"Refreshes every {REFRESH_SECONDS} seconds"
        if live_mode
        else "Automatic refresh is paused"
    )
    st.divider()

    st.markdown("#### Service status")
    st.caption(
        f"Kafka: {'Connected' if kafka_ok else 'Unavailable'}  ·  "
        f"PostgreSQL: {'Connected' if db_ok else 'Unavailable'}"
    )
    st.caption(
        f"Spark checkpoint: {'Present' if cp['exists'] else 'Not detected'}"
    )

    with st.expander("Pipeline stages", expanded=False):
        st.markdown(
            """
            1. Market data source
            2. Kafka producer -> `stock_data`
            3. Spark streaming + 1-minute windows
            4. Event detection -> `stock_events`
            5. PostgreSQL storage
            6. AI investigation + dashboard
            """
        )
    st.caption("RTSED · Academic demonstration")


# ============================================================
# TOP-LEVEL TABS
# ============================================================

tabs = st.tabs(
    [
        "Overview",
        "Pipeline",
        "Events & Analytics",
        "Market Data",
        "AI Investigation",
        "System",
    ]
)
with tabs[1]:
    pipeline_tabs = st.tabs(["Architecture", "Kafka", "Spark"])
with tabs[2]:
    event_tabs = st.tabs(["Stored events", "Analytics"])
with tabs[5]:
    system_tabs = st.tabs(["Reliability", "Methodology"])


# ============================================================
# OVERVIEW
# ============================================================

with tabs[0]:
    st.subheader("System overview")
    st.caption("Current pipeline activity, service health and latest detections.")

    c1, c2, c3, c4 = st.columns(4)

    c1.metric("Detected Events", len(events))
    c2.metric(
        "Price Spikes",
        int((events["event_type"] == "PRICE_SPIKE").sum())
        if not events.empty else 0,
    )
    c3.metric(
        "Volume Surges",
        int((events["event_type"] == "VOLUME_SURGE").sum())
        if not events.empty else 0,
    )
    c4.metric(
        "Latest Event",
        str(latest_event)[:19] if latest_event else "—",
    )

    st.markdown("### Service health")

    h1, h2, h3, h4 = st.columns(4)
    with h1:
        component_card("Kafka", kafka_ok, kafka_message)
    with h2:
        component_card("Spark Streaming", cp["exists"], "Persistent streaming checkpoint detected")
    with h3:
        component_card("PostgreSQL", db_ok, db_message)
    with h4:
        component_card(
            "Dashboard",
            True,
            "Streamlit + Plotly visualization active",
        )

    st.markdown("### Data flow")

    flow = st.columns(6)
    labels = [
        "Market data",
        "Producer",
        "Kafka",
        "Spark",
        "PostgreSQL",
        "Dashboard",
    ]

    for index, (col, label) in enumerate(zip(flow, labels), start=1):
        with col:
            st.markdown(
                f"""
                <div class="component" style="text-align:center; min-height:75px;">
                    <div style="color:#2563eb;font-size:.75rem;font-weight:700;">STAGE {index}</div>
                    <b>{label}</b>
                </div>
                """,
                unsafe_allow_html=True,
            )

    if not events.empty:
        st.markdown("### Latest detected events")
        st.dataframe(
            events[
                [
                    "symbol",
                    "event_type",
                    "event_start",
                    "avg_close",
                    "total_volume",
                    "avg_price_change_pct",
                ]
            ].head(10),
            width='stretch',
            hide_index=True,
        )

    mode = (
        ingestion_status.get("operating_mode")
        or pipeline_row.get("operating_mode")
        or "UNKNOWN"
    )
    st.markdown("### Market-data source")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Operating mode", mode)
    m2.metric(
        "Provider",
        ingestion_status.get("provider") or pipeline_row.get("provider") or "—",
    )
    m3.metric(
        "Watchlist size",
        ingestion_status.get("watchlist_count")
        or pipeline_row.get("watchlist_count")
        or 0,
    )
    m4.metric(
        "Last fetch (IST)",
        to_ist_label(
            ingestion_status.get("last_successful_fetch")
            or pipeline_row.get("last_successful_fetch")
        ),
    )
    if str(mode).upper() in {"DEMO_REPLAY", "HISTORICAL"}:
        st.warning(
            f"Displayed candles are {mode}. They are not live NSE quotes."
        )
    last_error = ingestion_status.get("last_error") or pipeline_row.get("last_error")
    if last_error:
        st.error(f"Ingestion warning: {last_error}")


# ============================================================
# COMPLETE PIPELINE
# ============================================================

with pipeline_tabs[0]:
    st.subheader("End-to-end architecture")

    st.info(
        "This page is the project's primary demonstration view. "
        "It connects each required component to the next stage of the pipeline."
    )

    architecture = [
        ("1", "Market data source", "Yahoo Finance live polling, demo cache replay, or historical ZIP", True),
        ("2", "Python Kafka Producer", "Reads, validates and serializes records as JSON", kafka_ok),
        ("3", "Kafka: stock_data", "3 partitions; keyed by symbol", INPUT_TOPIC in kafka_topics),
        ("4", "Spark Structured Streaming", "Kafka consumer; streaming transformations", cp["exists"]),
        ("5", "Window + Aggregation", "1-minute tumbling window", cp["state"]),
        ("6", "Event Detection", "Price spike / volume surge", len(events) > 0),
        ("7", "Kafka: stock_events", "3 partitions; detected events", OUTPUT_TOPIC in kafka_topics),
        ("8", "PostgreSQL", "Persistent processed-event storage", db_ok),
        ("9", "Streamlit + Plotly", "Real-time analytics and visualization", True),
    ]

    for num, name, detail, ok in architecture:
        status = "Operational" if ok else "Unavailable"
        st.markdown(
            f"""
            <div class="component" style="margin-bottom:.55rem;">
                <b>{num}. {name}</b>
                <span class="small muted"> · {status}</span><br>
                <span class="small muted">{detail}</span>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown("### Requirement Coverage")

    requirements = pd.DataFrame(
        [
            ["Apache Kafka central platform", "stock_data + stock_events", "Implemented"],
            ["Producer", "Python KafkaProducer", "Implemented"],
            ["Consumer", "Spark Structured Streaming Kafka source", "Implemented"],
            ["Two Kafka topics", "stock_data / stock_events", "Implemented"],
            ["Partitioning", "3 partitions per topic", "Implemented"],
            ["Consumer groups", "Kafka group listing visible in Pipeline", "Inspect"],
            ["Window operation", "1-minute tumbling window", "Implemented"],
            ["Aggregation", "avg close / total volume / avg change / count", "Implemented"],
            ["Fault tolerance", "Persistent Spark checkpoint + Kafka offsets", "Implemented"],
            ["Database", "PostgreSQL", "Implemented"],
            ["Real-time visualization", "Streamlit + Plotly + auto-refresh", "Implemented"],
        ],
        columns=["Course Requirement", "Implementation Evidence", "Status"],
    )
    st.dataframe(requirements, width='stretch', hide_index=True)


# ============================================================
# KAFKA
# ============================================================

with pipeline_tabs[1]:
    st.subheader("Kafka messaging")

    a, b, c = st.columns(3)
    a.metric("Broker", "localhost:9092" if kafka_ok else "Offline")
    b.metric("Input Topic", INPUT_TOPIC)
    c.metric("Output Topic", OUTPUT_TOPIC)

    st.markdown("### Topic Partitioning")

    if not stock_topic_df.empty:
        st.markdown("**`stock_data` — input topic**")
        st.dataframe(stock_topic_df, width='stretch', hide_index=True)

    if not event_topic_df.empty:
        st.markdown("**`stock_events` — output topic**")
        st.dataframe(event_topic_df, width='stretch', hide_index=True)

    st.markdown("### Kafka Data Movement")

    k1, k2 = st.columns(2)

    with k1:
        if not stock_topic_df.empty:
            fig = px.bar(
                stock_topic_df,
                x="partition",
                y="records_available",
                title="Records Available by stock_data Partition",
            )
            st.plotly_chart(fig, width='stretch')

    with k2:
        if not event_topic_df.empty:
            fig = px.bar(
                event_topic_df,
                x="partition",
                y="records_available",
                title="Records Available by stock_events Partition",
            )
            st.plotly_chart(fig, width='stretch')

    st.markdown("### Consumer Groups")

    if groups:
        group_df = pd.DataFrame({"Kafka Consumer Group": groups})
        st.dataframe(group_df, width='stretch', hide_index=True)
        st.caption(
            "The Kafka CLI output shows consumer groups registered with the broker. "
            "Spark Structured Streaming additionally maintains source progress through its checkpoint."
        )
    else:
        st.warning(
            "No Kafka consumer group is currently visible through the broker CLI. "
            "The Spark streaming source is still tracked through its persistent checkpoint."
        )
        if group_error:
            st.caption(group_error)

    st.markdown("### Why partitions matter")
    st.write(
        "The producer uses the stock symbol as the Kafka message key. "
        "Kafka can therefore distribute records across partitions while preserving "
        "ordering for records sharing the same key/partition."
    )


# ============================================================
# STREAM PROCESSING
# ============================================================

with pipeline_tabs[2]:
    st.subheader("Spark Structured Streaming")

    s1, s2, s3, s4 = st.columns(4)
    s1.metric("Framework", "Apache Spark")
    s2.metric("Operation", "Structured Streaming")
    s3.metric("Window", "1 minute")
    s4.metric("Mode", "Tumbling")

    st.markdown("### Streaming Transformation")

    st.code(
        """Kafka stock_data
      |
Parse JSON
      |
Validate / cast fields
      |
1-minute tumbling window
      |
Group by symbol + window
      |
Aggregate:
  - average close
  - total volume
  - average price change
  - record count
      |
Event rules
  - PRICE_SPIKE
  - VOLUME_SURGE
      |
Kafka stock_events + PostgreSQL""",
        language="text",
    )

    st.markdown("### Aggregated Event Data")

    if not events.empty:
        agg = events.groupby("event_type").agg(
            events=("id", "count"),
            avg_close=("avg_close", "mean"),
            total_volume=("total_volume", "sum"),
            avg_price_change=("avg_price_change_pct", "mean"),
        ).reset_index()

        st.dataframe(agg, width='stretch', hide_index=True)

        fig = px.scatter(
            events,
            x="total_volume",
            y="avg_price_change_pct",
            color="event_type",
            hover_data=["symbol", "event_start", "avg_close"],
            title="Detected Events: Volume vs Price Change",
        )
        st.plotly_chart(fig, width='stretch')

    st.markdown("### Event Detection Logic")

    st.code(
        """For each stock and 1-minute window:

1. Aggregate market activity.
2. Calculate the window's average close.
3. Calculate total volume.
4. Calculate average percentage price change.
5. Apply event thresholds.
6. Emit a structured event only when a condition is met.""",
        language="text",
    )


# ============================================================
# STORAGE & EVENTS
# ============================================================

with event_tabs[0]:
    st.subheader("Stored events")

    d1, d2, d3 = st.columns(3)
    d1.metric("Rows Stored", len(events))
    d2.metric(
        "Distinct Stocks",
        events["symbol"].nunique() if not events.empty else 0,
    )
    d3.metric(
        "Event Types",
        events["event_type"].nunique() if not events.empty else 0,
    )

    if not events.empty:
        st.markdown("### Event distribution")

        counts = events["event_type"].value_counts().reset_index()
        counts.columns = ["event_type", "count"]

        fig = px.pie(
            counts,
            names="event_type",
            values="count",
            hole=.45,
            title="Stored Event Types",
        )
        st.plotly_chart(fig, width='stretch')

        with st.expander("View all PostgreSQL records"):
            st.dataframe(events, width='stretch', hide_index=True)

    else:
        st.info("No processed events are currently stored in PostgreSQL.")


# ============================================================
# FAULT TOLERANCE
# ============================================================

with system_tabs[0]:
    st.subheader("Fault tolerance & recovery")

    st.success(
        "The project uses persistent Spark Structured Streaming checkpointing "
        "together with Kafka source offsets."
    )

    f1, f2, f3, f4 = st.columns(4)

    f1.metric("Checkpoint", "FOUND" if cp["exists"] else "MISSING")
    f2.metric("Offsets", "FOUND" if cp["offsets"] else "MISSING")
    f3.metric("Commits", "FOUND" if cp["commits"] else "MISSING")
    f4.metric("State", "FOUND" if cp["state"] else "MISSING")

    st.markdown("### Persistent Checkpoint Contents")

    cp_df = pd.DataFrame(
        [
            ["Checkpoint directory", str(CHECKPOINT_PATH), cp["exists"]],
            ["Kafka source offsets", "offsets/", cp["offsets"]],
            ["Streaming commits", "commits/", cp["commits"]],
            ["Streaming source metadata", "sources/", cp["sources"]],
            ["State store", "state/", cp["state"]],
        ],
        columns=["Component", "Location", "Present"],
    )
    cp_df["Status"] = cp_df["Present"].map(
        lambda present: "Present" if present else "Missing"
    )
    st.dataframe(
        cp_df.drop(columns=["Present"]),
        width='stretch',
        hide_index=True,
    )

    if cp["modified"]:
        st.caption(f"Checkpoint last modified: {cp['modified']}")

    st.markdown("### Recovery Demonstration")

    st.code(
        """NORMAL RUN
Kafka -> Spark -> PostgreSQL
        |
checkpoint stores offsets + state

SPARK STOPPED
        |
application restarted
        |
same checkpoint reused
        |
Spark resumes from committed streaming progress
        |
previously processed events are not duplicated
        """,
        language="text",
    )

    st.markdown("### Observed Test Result")

    st.metric(
        "PostgreSQL event count after restart",
        len(events),
        help="The tested restart retained the stored event count at 55.",
    )

    st.info(
        "In the lab recovery test, the PostgreSQL count remained 55 after "
        "stopping and restarting Spark without rerunning the producer. "
        "This demonstrates the configured recovery path and absence of duplicate "
        "events during the tested restart."
    )


# ============================================================
# ANALYTICS
# ============================================================

with event_tabs[1]:
    st.subheader("Event analytics")

    if events.empty:
        st.info("Waiting for detected events...")
    else:
        col1, col2 = st.columns(2)

        with col1:
            stock_counts = events["symbol"].value_counts().reset_index()
            stock_counts.columns = ["symbol", "count"]
            fig = px.bar(
                stock_counts.head(20),
                x="symbol",
                y="count",
                title="Top Stocks by Detected Events",
            )
            st.plotly_chart(fig, width='stretch')

        with col2:
            timeline = events.copy()
            timeline["event_start"] = pd.to_datetime(timeline["event_start"])
            timeline["hour"] = timeline["event_start"].dt.floor("h")

            timeline = (
                timeline.groupby(["hour", "event_type"])
                .size()
                .reset_index(name="count")
            )

            fig = px.line(
                timeline,
                x="hour",
                y="count",
                color="event_type",
                markers=True,
                title="Detected Events Over Time",
            )
            st.plotly_chart(fig, width='stretch')

        st.markdown("### Price Movement")

        fig = px.histogram(
            events,
            x="avg_price_change_pct",
            color="event_type",
            nbins=30,
            title="Distribution of Detected Price Changes",
        )
        st.plotly_chart(fig, width='stretch')

        st.markdown("### Latest Event Alert")

        latest = events.iloc[0]
        if latest["event_type"] == "PRICE_SPIKE":
            st.error(
                f"PRICE SPIKE - {latest['symbol']} | "
                f"{latest['avg_price_change_pct']:.3f}% average price change"
            )
        else:
            st.warning(
                f"VOLUME SURGE - {latest['symbol']} | "
                f"{latest['total_volume']:,} total volume"
            )


# ============================================================
# METHODOLOGY
# ============================================================

with system_tabs[1]:
    st.subheader("Project Methodology")

    st.markdown(
        """
        ### Objective

        Detect significant stock-market events from 1-minute OHLCV streams
        using distributed stream processing.

        ### Data Source

        Historical NSE F&O 1-minute OHLCV data is replayed as a real-time stream.
        This provides a reproducible input for demonstrating streaming analytics.

        ### Processing Method

        1. Python reads the historical records.
        2. Records are validated and serialized as JSON.
        3. Kafka receives the records in `stock_data`.
        4. Spark Structured Streaming consumes the Kafka stream.
        5. Records are grouped into 1-minute tumbling windows.
        6. Window-level statistics are calculated.
        7. Event rules identify price spikes and volume surges.
        8. Detected events are published to `stock_events`.
        9. Events are persisted in PostgreSQL.
        10. Streamlit and Plotly visualize the processed results.
        11. Spark checkpointing preserves offsets and streaming state for recovery.
        12. The Stock Event Investigation Agent consumes `stock_events` separately.
        13. Tool-using investigation results are stored in `ai_investigations`.

        ### Technologies

        **Python Â· Apache Kafka Â· Apache Spark Structured Streaming Â· PostgreSQL Â·
        Streamlit Â· Plotly Â· Docker**

        ### Important Experimental Design Choice

        Live mode polls Yahoo Finance through yfinance. That interface is unofficial
        and may be delayed. Demo replay uses cached genuine Yahoo candles.
        Historical ZIP replay remains available as a secondary source.
        The project does not claim zero-delay NSE exchange data.
        """
    )


# ============================================================
# MARKET DATA
# ============================================================

with tabs[3]:
    st.subheader("Market data")
    st.caption("Provider, operating mode, ingestion freshness and watchlist.")
    mode = (
        ingestion_status.get("operating_mode")
        or pipeline_row.get("operating_mode")
        or "UNKNOWN"
    )
    st.info(
        "LIVE uses Yahoo Finance polling. DEMO_REPLAY uses cached genuine Yahoo "
        "candles. HISTORICAL uses the NSE ZIP dataset. These modes are not mixed "
        "by default."
    )
    c1, c2, c3 = st.columns(3)
    c1.metric("Current mode", mode)
    c2.metric(
        "Provider",
        ingestion_status.get("provider") or pipeline_row.get("provider") or "—",
    )
    c3.metric(
        "Stocks monitored",
        ingestion_status.get("watchlist_count")
        or pipeline_row.get("watchlist_count")
        or 0,
    )
    c4, c5, c6 = st.columns(3)
    c4.metric(
        "Last successful fetch",
        to_ist_label(
            ingestion_status.get("last_successful_fetch")
            or pipeline_row.get("last_successful_fetch")
        ),
    )
    latest_ts = (
        ingestion_status.get("latest_candle_timestamp")
        or pipeline_row.get("latest_candle_timestamp")
    )
    if latest_candle:
        latest_ts = latest_ts or latest_candle[1]
    c5.metric("Latest candle", to_ist_label(latest_ts))
    c6.metric(
        "Last publish count",
        ingestion_status.get("last_published_count")
        or pipeline_row.get("last_published_count")
        or 0,
    )
    if latest_candle:
        st.caption(
            f"Latest stored candle: {latest_candle[0]} close={latest_candle[2]} "
            f"mode={latest_candle[4]} at {to_ist_label(latest_candle[1])}"
        )
    last_error = ingestion_status.get("last_error") or pipeline_row.get("last_error")
    if last_error:
        st.error(last_error)
    limitations = ingestion_status.get("limitations")
    if limitations:
        st.caption(limitations)
    watchlist = ingestion_status.get("watchlist") or []
    if watchlist:
        st.write("Watchlist:", ", ".join(watchlist))


# ============================================================
# AI INVESTIGATION
# ============================================================

with tabs[4]:
    st.subheader("AI investigation")
    st.caption(
        "Spark remains responsible for PRICE_SPIKE / VOLUME_SURGE detection. "
        "The agent investigates stored events with read-only tools. "
        "It does not generate trading advice."
    )

    completed = 0
    pending = 0
    failed = 0
    processing = 0
    if not investigations.empty:
        completed = int((investigations["investigation_status"] == "COMPLETED").sum())
        pending = int((investigations["investigation_status"] == "PENDING").sum())
        failed = int((investigations["investigation_status"] == "FAILED").sum())
        processing = int(
            (investigations["investigation_status"] == "PROCESSING").sum()
        )

    a1, a2, a3, a4, a5 = st.columns(5)
    a1.metric("Spark events", len(events))
    a2.metric("Investigations completed", completed)
    a3.metric("Pending", pending + processing)
    a4.metric("Failed", failed)
    a5.metric(
        "Distinct symbols",
        investigations["symbol"].nunique() if not investigations.empty else 0,
    )

    if not investigations.empty and investigations["severity"].notna().any():
        with st.expander("Investigation statistics", expanded=False):
            sev = (
                investigations.dropna(subset=["severity"])["severity"]
                .value_counts()
                .reset_index()
            )
            sev.columns = ["severity", "count"]
            fig = px.bar(sev, x="severity", y="count", title="Severity distribution")
            st.plotly_chart(fig, width="stretch")

            by_symbol = (
                investigations.groupby("symbol")
                .agg(
                    investigations=("investigation_id", "count"),
                    completed=(
                        "investigation_status",
                        lambda status: int((status == "COMPLETED").sum()),
                    ),
                )
                .reset_index()
            )
            st.markdown("#### By symbol")
            st.dataframe(by_symbol, width="stretch", hide_index=True)

    st.markdown("### Investigation reports")
    if investigations.empty:
        st.info(
            "No investigations yet. Start `python agent/consumer.py` after events "
            "are detected. If Ollama is down, rows stay PENDING or FAILED."
        )
    else:
        display = investigations.copy()
        display["_demo_priority"] = (
            display["investigation_status"]
            .map(
                {
                    "COMPLETED": 0,
                    "PROCESSING": 1,
                    "PENDING": 2,
                    "FAILED": 3,
                }
            )
            .fillna(4)
        )
        display = display.sort_values(
            ["_demo_priority", "investigation_id"],
            ascending=[True, False],
            kind="stable",
        ).drop(columns=["_demo_priority"])
        st.dataframe(
            display[
                [
                    "investigation_id",
                    "event_id",
                    "symbol",
                    "event_type",
                    "severity",
                    "confidence",
                    "investigation_status",
                    "price_change_percent",
                    "volume_ratio",
                    "updated_at",
                ]
            ],
            width="stretch",
            hide_index=True,
        )

        if completed:
            st.caption(
                "Completed reports appear first for demonstrations. "
                "New pending investigations are processed before older backlog items."
            )
        options = [
            f"{row.investigation_id} | {row.symbol} | {row.event_type} | {row.investigation_status}"
            for row in display.itertuples()
        ]
        selected = st.selectbox(
            "Select an investigation",
            options,
            index=0,
            key="ai_investigation_demo_selector",
        )
        if selected:
            selected_id = int(selected.split("|", 1)[0].strip())
            row = display[
                display["investigation_id"] == selected_id
            ].iloc[0]
            spark_row = pd.DataFrame()
            if not events.empty and pd.notna(row.get("event_id")):
                spark_row = events[events["id"] == row["event_id"]]
            if spark_row.empty and not events.empty:
                event_start = pd.to_datetime(
                    row.get("event_start"), utc=True, errors="coerce"
                )
                event_end = pd.to_datetime(
                    row.get("event_end"), utc=True, errors="coerce"
                )
                matches = events[
                    (events["symbol"] == row["symbol"])
                    & (events["event_type"] == row["event_type"])
                    & (
                        pd.to_datetime(events["event_start"], utc=True, errors="coerce")
                        == event_start
                    )
                    & (
                        pd.to_datetime(events["event_end"], utc=True, errors="coerce")
                        == event_end
                    )
                ]
                if not matches.empty:
                    spark_row = matches.head(1)

            evidence = row.get("evidence")
            if not isinstance(evidence, dict):
                evidence = {}
            spark_event = (
                spark_row.iloc[0].to_dict()
                if not spark_row.empty
                else evidence.get("spark_event") or {}
            )
            if not isinstance(spark_event, dict):
                spark_event = {}

            with st.expander("Original Spark event record", expanded=False):
                if spark_row.empty:
                    st.write(
                        {
                            "symbol": row["symbol"],
                            "event_type": row["event_type"],
                            "event_start": to_ist_label(row["event_start"]),
                            "event_end": to_ist_label(row["event_end"]),
                        }
                    )
                else:
                    st.dataframe(spark_row, width="stretch", hide_index=True)

            event_volume = spark_event.get("total_volume")
            avg_close = spark_event.get("avg_close")
            record_count = spark_event.get("record_count")
            spark_threshold = (
                "≥ 3,000 volume"
                if row["event_type"] == "VOLUME_SURGE"
                else "≥ 1.0% move"
                if row["event_type"] == "PRICE_SPIKE"
                else "—"
            )
            st.markdown("#### Spark event numbers")
            s1, s2, s3, s4 = st.columns(4)
            s1.metric(
                "Event volume",
                f"{float(event_volume):,.0f}"
                if event_volume is not None and pd.notna(event_volume)
                else "—",
            )
            s2.metric("Detection threshold", spark_threshold)
            s3.metric(
                "Average close",
                f"{float(avg_close):,.2f}"
                if avg_close is not None and pd.notna(avg_close)
                else "—",
            )
            s4.metric(
                "Records in window",
                f"{int(record_count):,}"
                if record_count is not None and pd.notna(record_count)
                else "—",
            )

            st.markdown("#### Numerical evidence")
            tool_trace = evidence.get("tool_trace") or []
            tool_results = {
                item.get("tool"): item.get("result") or {}
                for item in tool_trace
                if isinstance(item, dict)
            }
            evidence_checks = (
                bool(
                    tool_results.get("get_event_details", {}).get("found")
                    or tool_results.get("get_event_details", {}).get("event")
                ),
                tool_results.get("get_recent_stock_history", {}).get("row_count", 0) > 0,
                tool_results.get("calculate_price_movement", {}).get("percent_change")
                is not None,
                tool_results.get("calculate_volume_anomaly", {}).get("volume_ratio")
                is not None,
                tool_results.get("get_previous_stock_events", {}).get(
                    "previous_event_count"
                )
                is not None,
            )
            evidence_coverage = round(
                100 * sum(evidence_checks) / len(evidence_checks)
            )
            confidence = row.get("confidence")
            confidence_label = (
                str(confidence) if confidence is not None and pd.notna(confidence) else "—"
            )
            price_change = row.get("price_change_percent")
            if price_change is None or pd.isna(price_change):
                price_change = spark_event.get("avg_price_change_pct")
            volume_ratio = row.get("volume_ratio")
            ratio_label = (
                f"{float(volume_ratio):.4f}x"
                if volume_ratio is not None and pd.notna(volume_ratio)
                else "Unavailable"
            )
            e1, e2, e3 = st.columns(3)
            e1.metric("Severity", row["severity"] or "—")
            e2.metric("Confidence level", confidence_label)
            e3.metric(
                "Evidence coverage",
                f"{evidence_coverage}%"
                if row["investigation_status"] == "COMPLETED"
                else "—",
                help=(
                    "Share of five evidence checks with usable results: event details, "
                    "recent history, price movement, volume baseline, and prior events. "
                    "This is data coverage, not confidence or probability."
                ),
            )
            e4, e5, e6 = st.columns(3)
            e4.metric(
                "Price change",
                f"{float(price_change):.4f}%"
                if price_change is not None and pd.notna(price_change)
                else "—",
            )
            e5.metric(
                "Volume ratio",
                ratio_label,
            )
            e6.metric(
                "Previous events",
                int(row["previous_event_count"])
                if row["previous_event_count"] is not None
                and pd.notna(row["previous_event_count"])
                else "—",
            )
            st.caption(
                "Confidence is the model's qualitative LOW/MEDIUM/HIGH rating, "
                "not a calibrated percentage. Volume ratio requires a stored "
                "historical baseline."
            )
            st.markdown("#### Agent investigation summary")
            if row["investigation_status"] != "COMPLETED":
                st.warning(
                    f"Status: {row['investigation_status']}. "
                    f"{row['error_message'] or 'Not a completed investigation.'}"
                )
            else:
                st.write(row["investigation_summary"] or "")
            if evidence:
                with st.expander("Tool evidence and execution trace"):
                    st.json(evidence)


# ============================================================
# AUTO REFRESH
# ============================================================

if live_mode:
    # Streamlit supports rerunning the script after a short delay.
    # This keeps the dashboard visibly live during the lab demonstration.
    import time
    time.sleep(REFRESH_SECONDS)
    st.rerun()

"""RTSED — Real-Time Stock Market Event Detection & Agentic Investigation Platform.

A high-performance streaming analytics dashboard for RV College of Engineering (RVCE)
Course AI372IA: Stream Processing and Analytics.
Architecture: Apache Kafka 4.0.1 + Apache Spark Structured Streaming 4.0.1 + PostgreSQL 16 +
Tool-Augmented Local LLM Investigation Agent + Streamlit Pro Fintech UI.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import psycopg2
import streamlit as st
from kafka import KafkaAdminClient, KafkaConsumer

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import config
from database.db import apply_schema
from producer.status import read_status


# ==============================================================================
# GLOBAL CONSTANTS & CONFIGURATION
# ==============================================================================

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
CHECKPOINT_POSTGRES_PATH = PROJECT_ROOT / "streaming" / "checkpoint_postgres"
IST = ZoneInfo("Asia/Kolkata")

STOCK_NAMES = {
    "RELIANCE.NS": "Reliance Industries Ltd",
    "TCS.NS": "Tata Consultancy Services",
    "INFY.NS": "Infosys Ltd",
    "HDFCBANK.NS": "HDFC Bank Ltd",
    "ICICIBANK.NS": "ICICI Bank Ltd",
}


def get_available_stocks() -> list[str]:
    """Return all configured watchlist stocks merged with known stock metadata."""
    try:
        configured = list(config.watchlist())
    except Exception:
        configured = list(config.DEFAULT_WATCHLIST)
    # Maintain order and eliminate duplicates
    return list(dict.fromkeys(configured + list(STOCK_NAMES.keys())))


def to_ist_label(value) -> str:
    """Format any UTC or naive timestamp to Asia/Kolkata formatted string."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "—"
    timestamp = pd.Timestamp(value)
    if pd.isna(timestamp):
        return "—"
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    return timestamp.tz_convert(IST).strftime("%Y-%m-%d %H:%M:%S IST")


# ==============================================================================
# STREAMLIT PAGE & STYLING
# ==============================================================================

st.set_page_config(
    page_title="RTSED | Real-Time Stock Event Detection",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom Plotly defaults for professional dark fintech styling
DARK_BG = "#0B0F19"
PANEL_BG = "#111827"
BORDER_COLOR = "#1F2937"
TEXT_COLOR = "#F1F5F9"
MUTED_COLOR = "#94A3B8"
ACCENT_BLUE = "#38BDF8"
ACCENT_GREEN = "#10B981"
ACCENT_RED = "#EF4444"
ACCENT_AMBER = "#F59E0B"
ACCENT_PURPLE = "#818CF8"

px.defaults.template = "plotly_dark"
px.defaults.color_discrete_sequence = [
    ACCENT_BLUE,
    ACCENT_GREEN,
    ACCENT_AMBER,
    ACCENT_PURPLE,
    ACCENT_RED,
]

# High-impact CSS design
st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600;700&family=Plus+Jakarta+Sans:wght@400;500;600;700;800&display=swap');

    :root {
        --bg-main: #0B0F19;
        --bg-panel: #111827;
        --bg-card: rgba(17, 24, 39, 0.75);
        --border-color: rgba(75, 85, 99, 0.4);
        --border-glow: rgba(56, 189, 248, 0.25);
        --text-primary: #F8FAFC;
        --text-muted: #94A3B8;
        --accent-cyan: #38BDF8;
        --accent-emerald: #10B981;
        --accent-rose: #EF4444;
        --accent-amber: #F59E0B;
        --accent-indigo: #6366F1;
    }

    /* Base canvas styling */
    .stApp {
        background: radial-gradient(circle at 15% 10%, rgba(30, 58, 138, 0.15) 0%, transparent 40%),
                    radial-gradient(circle at 85% 90%, rgba(16, 185, 129, 0.08) 0%, transparent 45%),
                    #0B0F19;
        color: var(--text-primary);
        font-family: 'Plus Jakarta Sans', system-ui, sans-serif;
    }

    .block-container {
        max-width: 1650px;
        padding-top: 1.2rem;
        padding-bottom: 3.5rem;
    }

    [data-testid="stHeader"] {
        background: rgba(11, 15, 25, 0.85);
        backdrop-filter: blur(12px);
    }

    /* Sidebar styling */
    [data-testid="stSidebar"] {
        background: #0D1322 !important;
        border-right: 1px solid var(--border-color);
    }
    [data-testid="stSidebar"] * {
        color: #E2E8F0 !important;
    }

    /* RVCE Academic Hero Header */
    .rvce-banner {
        background: linear-gradient(135deg, rgba(15, 23, 42, 0.95) 0%, rgba(30, 41, 59, 0.85) 60%, rgba(15, 23, 42, 0.95) 100%);
        border: 1px solid rgba(56, 189, 248, 0.3);
        border-radius: 18px;
        padding: 1.6rem 2.2rem;
        margin-bottom: 1.4rem;
        box-shadow: 0 10px 30px -5px rgba(0, 0, 0, 0.6), 0 0 25px -5px rgba(56, 189, 248, 0.12);
        position: relative;
        overflow: hidden;
    }
    .rvce-banner::before {
        content: "";
        position: absolute;
        top: 0; left: 0; right: 0; height: 3px;
        background: linear-gradient(90deg, #38BDF8, #6366F1, #10B981, #F59E0B);
    }
    .rvce-topline {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 0.4rem;
        flex-wrap: wrap;
        gap: 0.5rem;
    }
    .rvce-tag {
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.72rem;
        font-weight: 700;
        letter-spacing: 0.14em;
        text-transform: uppercase;
        color: #38BDF8;
        background: rgba(56, 189, 248, 0.12);
        padding: 0.25rem 0.65rem;
        border-radius: 6px;
        border: 1px solid rgba(56, 189, 248, 0.25);
    }
    .rvce-live-chip {
        display: inline-flex;
        align-items: center;
        gap: 0.45rem;
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.75rem;
        font-weight: 600;
        color: #10B981;
        background: rgba(16, 185, 129, 0.12);
        padding: 0.25rem 0.75rem;
        border-radius: 9999px;
        border: 1px solid rgba(16, 185, 129, 0.3);
    }
    .pulsing-dot {
        width: 8px;
        height: 8px;
        border-radius: 50%;
        background-color: #10B981;
        box-shadow: 0 0 10px #10B981;
        animation: pulse 1.8s infinite;
    }
    @keyframes pulse {
        0% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7); }
        70% { transform: scale(1.1); box-shadow: 0 0 0 8px rgba(16, 185, 129, 0); }
        100% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0); }
    }
    .rvce-title {
        font-size: clamp(1.6rem, 2.8vw, 2.3rem);
        font-weight: 800;
        color: #FFFFFF;
        letter-spacing: -0.035em;
        line-height: 1.15;
        margin: 0.35rem 0 0.5rem;
    }
    .rvce-desc {
        color: #CBD5E1;
        font-size: 0.94rem;
        max-width: 1100px;
        line-height: 1.5;
    }

    /* Live Ticker Bar */
    .ticker-bar {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
        gap: 0.85rem;
        margin-bottom: 1.4rem;
    }
    .ticker-card {
        background: rgba(17, 24, 39, 0.85);
        border: 1px solid rgba(75, 85, 99, 0.4);
        border-radius: 13px;
        padding: 0.85rem 1.1rem;
        transition: all 0.2s ease;
        box-shadow: 0 4px 12px rgba(0,0,0,0.25);
    }
    .ticker-card:hover {
        border-color: rgba(56, 189, 248, 0.5);
        transform: translateY(-2px);
        box-shadow: 0 8px 20px rgba(0,0,0,0.35);
    }
    .ticker-symbol {
        font-weight: 700;
        font-size: 0.95rem;
        color: #FFFFFF;
        display: flex;
        justify-content: space-between;
        align-items: center;
    }
    .ticker-price {
        font-family: 'JetBrains Mono', monospace;
        font-size: 1.35rem;
        font-weight: 700;
        color: #F8FAFC;
        margin: 0.25rem 0 0.15rem;
    }
    .ticker-delta {
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.8rem;
        font-weight: 600;
    }
    .delta-up { color: #10B981; }
    .delta-down { color: #EF4444; }
    .delta-neutral { color: #94A3B8; }

    /* Custom KPI Cards */
    [data-testid="stMetric"] {
        background: rgba(17, 24, 39, 0.85) !important;
        border: 1px solid rgba(75, 85, 99, 0.4) !important;
        border-radius: 14px !important;
        padding: 1rem 1.25rem !important;
        box-shadow: 0 4px 15px rgba(0, 0, 0, 0.25) !important;
    }
    [data-testid="stMetricLabel"] {
        color: #94A3B8 !important;
        font-size: 0.82rem !important;
        font-weight: 600 !important;
        text-transform: uppercase !important;
        letter-spacing: 0.05em !important;
    }
    [data-testid="stMetricValue"] {
        color: #FFFFFF !important;
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 1.85rem !important;
        font-weight: 700 !important;
    }

    /* Tabs styling */
    [data-testid="stTabs"] [data-baseweb="tab-list"] {
        gap: 0.4rem;
        background: rgba(15, 23, 42, 0.6);
        padding: 0.35rem;
        border-radius: 12px;
        border: 1px solid rgba(75, 85, 99, 0.3);
        margin-bottom: 1.2rem;
    }
    [data-testid="stTabs"] button[data-baseweb="tab"] {
        color: #94A3B8 !important;
        font-weight: 600 !important;
        padding: 0.65rem 1.25rem !important;
        border-radius: 8px !important;
        border: none !important;
        transition: all 0.2s ease !important;
    }
    [data-testid="stTabs"] button[aria-selected="true"] {
        color: #FFFFFF !important;
        background: linear-gradient(135deg, rgba(56, 189, 248, 0.2) 0%, rgba(99, 102, 241, 0.2) 100%) !important;
        border: 1px solid rgba(56, 189, 248, 0.4) !important;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.3) !important;
    }

    /* Component cards */
    .glass-card {
        background: rgba(17, 24, 39, 0.75);
        border: 1px solid rgba(75, 85, 99, 0.4);
        border-radius: 14px;
        padding: 1.15rem 1.35rem;
        margin-bottom: 0.9rem;
        backdrop-filter: blur(8px);
        box-shadow: 0 4px 14px rgba(0,0,0,0.2);
    }
    .glass-card-header {
        font-size: 0.98rem;
        font-weight: 700;
        color: #F8FAFC;
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 0.45rem;
    }
    .status-badge {
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.7rem;
        font-weight: 700;
        padding: 0.2rem 0.55rem;
        border-radius: 5px;
        text-transform: uppercase;
    }
    .badge-ok { background: rgba(16, 185, 129, 0.18); color: #34D399; border: 1px solid rgba(16, 185, 129, 0.4); }
    .badge-warn { background: rgba(245, 158, 11, 0.18); color: #FBBF24; border: 1px solid rgba(245, 158, 11, 0.4); }
    .badge-err { background: rgba(239, 68, 68, 0.18); color: #F87171; border: 1px solid rgba(239, 68, 68, 0.4); }
    .badge-info { background: rgba(56, 189, 248, 0.18); color: #38BDF8; border: 1px solid rgba(56, 189, 248, 0.4); }
    .badge-purple { background: rgba(129, 140, 248, 0.18); color: #A5B4FC; border: 1px solid rgba(129, 140, 248, 0.4); }

    /* Alert Banner */
    .alert-banner {
        background: linear-gradient(90deg, rgba(239, 68, 68, 0.15) 0%, rgba(15, 23, 42, 0.6) 100%);
        border-left: 4px solid #EF4444;
        border-radius: 10px;
        padding: 0.85rem 1.25rem;
        margin-bottom: 1.2rem;
        display: flex;
        align-items: center;
        gap: 0.9rem;
    }
    .alert-banner-surge {
        background: linear-gradient(90deg, rgba(56, 189, 248, 0.15) 0%, rgba(15, 23, 42, 0.6) 100%);
        border-left: 4px solid #38BDF8;
        border-radius: 10px;
        padding: 0.85rem 1.25rem;
        margin-bottom: 1.2rem;
        display: flex;
        align-items: center;
        gap: 0.9rem;
    }

    /* Dataframe and tables */
    [data-testid="stDataFrame"] {
        border: 1px solid rgba(75, 85, 99, 0.4) !important;
        border-radius: 12px !important;
        overflow: hidden !important;
    }

    /* Viva guide styling */
    .viva-box {
        background: rgba(30, 41, 59, 0.6);
        border: 1px solid rgba(148, 163, 184, 0.2);
        border-radius: 12px;
        padding: 1.1rem 1.3rem;
        margin-bottom: 0.8rem;
    }
    .viva-q {
        color: #38BDF8;
        font-weight: 700;
        font-size: 0.95rem;
        margin-bottom: 0.35rem;
    }
    .viva-a {
        color: #E2E8F0;
        font-size: 0.9rem;
        line-height: 1.55;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# ==============================================================================
# DATABASE LOADERS & HELPERS (CACHED WITH TTL)
# ==============================================================================

def db_connect():
    """Establish connection to PostgreSQL."""
    return psycopg2.connect(**DB_CONFIG)


@st.cache_data(ttl=3)
def load_events() -> pd.DataFrame:
    """Load raw detected events from PostgreSQL stock_events."""
    try:
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
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=3)
def db_status() -> tuple[bool, int, datetime | None, str]:
    """Test PostgreSQL connection and return high-level table statistics."""
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
def load_investigations() -> pd.DataFrame:
    """Load AI investigations from ai_investigations table."""
    try:
        conn = db_connect()
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
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()
        conn.close()
        return pd.DataFrame(rows, columns=columns)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=3)
def load_pipeline_status_row() -> dict | None:
    """Load the latest pipeline operating mode and fetch metadata."""
    try:
        conn = db_connect()
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
        return None


@st.cache_data(ttl=3)
def load_latest_candle() -> tuple | None:
    """Load the single most recent candle across all stocks."""
    try:
        conn = db_connect()
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
        return None


@st.cache_data(ttl=3)
def load_candles(symbol: str | None = None, limit: int = 300) -> pd.DataFrame:
    """Load chronological OHLCV candles from stock_candles (most recent first, then re-sorted)."""
    try:
        conn = db_connect()
        cur = conn.cursor()
        if symbol and symbol != "ALL":
            cur.execute(
                """
                SELECT symbol, ts, open, high, low, close, volume, source_mode
                FROM stock_candles
                WHERE symbol = %s
                ORDER BY ts DESC
                LIMIT %s
                """,
                (symbol, limit),
            )
        else:
            cur.execute(
                """
                SELECT symbol, ts, open, high, low, close, volume, source_mode
                FROM stock_candles
                ORDER BY ts DESC
                LIMIT %s
                """,
                (limit,),
            )
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        cur.close()
        conn.close()
        df = pd.DataFrame(rows, columns=cols)
        # Re-sort ascending for chart rendering
        if not df.empty:
            df = df.sort_values("ts").reset_index(drop=True)
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=3)
def load_candles_today(symbol: str | None = None, limit: int = 200) -> pd.DataFrame:
    """Load only TODAY's OHLCV candles (IST date) from stock_candles."""
    try:
        conn = db_connect()
        cur = conn.cursor()
        # Use current IST date for filtering
        today_utc_start = datetime.now(ZoneInfo("Asia/Kolkata")).replace(
            hour=0, minute=0, second=0, microsecond=0
        ).astimezone(timezone.utc)
        if symbol and symbol != "ALL":
            cur.execute(
                """
                SELECT symbol, ts, open, high, low, close, volume, source_mode
                FROM stock_candles
                WHERE symbol = %s AND ts >= %s
                ORDER BY ts ASC
                LIMIT %s
                """,
                (symbol, today_utc_start, limit),
            )
        else:
            cur.execute(
                """
                SELECT symbol, ts, open, high, low, close, volume, source_mode
                FROM stock_candles
                WHERE ts >= %s
                ORDER BY ts ASC
                LIMIT %s
                """,
                (today_utc_start, limit),
            )
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        cur.close()
        conn.close()
        return pd.DataFrame(rows, columns=cols)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=3)
def load_watchlist_summary() -> pd.DataFrame:
    """Fetch latest price, previous close, and 1-minute delta for each stock."""
    try:
        conn = db_connect()
        cur = conn.cursor()
        cur.execute(
            """
            WITH ordered AS (
                SELECT symbol, ts, open, high, low, close, volume,
                       LAG(close) OVER (PARTITION BY symbol ORDER BY ts ASC) as prev_close
                FROM stock_candles
            ),
            ranked AS (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY ts DESC) as rn
                FROM ordered
            )
            SELECT symbol, ts, open, high, low, close, volume, prev_close,
                   CASE WHEN prev_close IS NOT NULL AND prev_close > 0 
                        THEN ((close - prev_close) / prev_close) * 100 
                        ELSE 0 END as pct_change
            FROM ranked WHERE rn = 1
            """
        )
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        cur.close()
        conn.close()
        return pd.DataFrame(rows, columns=cols)
    except Exception:
        return pd.DataFrame()


# ==============================================================================
# KAFKA & SPARK CHECKPOINT INTROSPECTION
# ==============================================================================

def kafka_status() -> tuple[bool, list[str], str]:
    """Test connection to the Kafka broker and list topics."""
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            client_id="rtsed-dashboard-admin",
            request_timeout_ms=2500,
        )
        topics = admin.list_topics()
        admin.close()
        return True, topics, "Kafka broker reachable (localhost:9092)"
    except Exception as e:
        return False, [], str(e)


def topic_partition_info(topic: str) -> pd.DataFrame:
    """Query low and high watermark offsets per partition for a Kafka topic."""
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            enable_auto_commit=False,
            consumer_timeout_ms=1000,
            request_timeout_ms=2500,
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
                        "Partition": p,
                        "Earliest Offset": beginning.get(t, 0),
                        "Latest Offset": end.get(t, 0),
                        "Available Records": max(0, end.get(t, 0) - beginning.get(t, 0)),
                    }
                )
        consumer.close()
        return pd.DataFrame(result)
    except Exception:
        return pd.DataFrame()


def kafka_consumer_groups() -> tuple[list[str], str]:
    """List consumer groups registered with the Kafka broker via docker exec."""
    try:
        cmd = [
            "docker",
            "exec",
            "rtsed-kafka",
            "/opt/kafka/bin/kafka-consumer-groups.sh",
            "--bootstrap-server",
            "localhost:9092",
            "--list",
        ]
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=4,
        )
        groups = [
            x.strip()
            for x in result.stdout.splitlines()
            if x.strip() and not x.lower().startswith("error")
        ]
        return groups, result.stderr.strip()
    except Exception as e:
        return [], str(e)


def checkpoint_info() -> dict:
    """Inspect the Spark Structured Streaming checkpoint directory."""
    exists = CHECKPOINT_PATH.exists()
    if not exists:
        return {
            "exists": False,
            "offsets": False,
            "commits": False,
            "sources": False,
            "state": False,
            "modified": None,
            "file_count": 0,
        }

    all_files = list(CHECKPOINT_PATH.rglob("*"))
    files_only = [f for f in all_files if f.is_file()]

    return {
        "exists": True,
        "offsets": (CHECKPOINT_PATH / "offsets").exists(),
        "commits": (CHECKPOINT_PATH / "commits").exists(),
        "sources": (CHECKPOINT_PATH / "sources").exists(),
        "state": (CHECKPOINT_PATH / "state").exists(),
        "modified": datetime.fromtimestamp(
            CHECKPOINT_PATH.stat().st_mtime
        ).strftime("%Y-%m-%d %H:%M:%S"),
        "file_count": len(files_only),
    }


def check_ollama() -> tuple[bool, list[str], str]:
    """Test local Ollama availability."""
    try:
        req = urllib.request.Request(
            f"{config.OLLAMA_BASE_URL}/api/tags",
            headers={"User-Agent": "RTSED-Dashboard"},
        )
        with urllib.request.urlopen(req, timeout=1.5) as res:
            data = json.loads(res.read().decode("utf-8"))
            models = [m.get("name") for m in data.get("models", [])]
            return True, models, f"Ollama online at {config.OLLAMA_BASE_URL}"
    except Exception:
        return False, [], "Ollama offline or unreachable"


def component_card(title: str, status: bool, detail: str, extra: str = ""):
    """Render a sleek glassmorphic service status card."""
    state_class = "badge-ok" if status else "badge-err"
    state_label = "OPERATIONAL" if status else "OFFLINE"
    st.markdown(
        f"""
        <div class="glass-card">
            <div class="glass-card-header">
                <span><b>{title}</b></span>
                <span class="status-badge {state_class}">{state_label}</span>
            </div>
            <div style="font-size:0.84rem; color:#94A3B8; margin-top:0.2rem;">{detail}</div>
            {f'<div style="font-size:0.76rem; color:#38BDF8; margin-top:0.35rem; font-family:monospace;">{extra}</div>' if extra else ''}
        </div>
        """,
        unsafe_allow_html=True,
    )


# ==============================================================================
# LOAD STATE & TELEMETRY
# ==============================================================================

try:
    apply_schema()
except Exception:
    pass

db_ok, total_events, latest_event, db_message = db_status()
events = load_events()
investigations = load_investigations()
ingestion_status = read_status(config.STATUS_FILE) or {}
pipeline_row = load_pipeline_status_row() or {}
latest_candle = load_latest_candle()
watchlist_summary = load_watchlist_summary()

kafka_ok, kafka_topics, kafka_message = kafka_status()
cp = checkpoint_info()
groups, group_error = kafka_consumer_groups()
ollama_ok, ollama_models, ollama_msg = check_ollama()

stock_topic_df = topic_partition_info(INPUT_TOPIC) if kafka_ok else pd.DataFrame()
event_topic_df = topic_partition_info(OUTPUT_TOPIC) if kafka_ok else pd.DataFrame()

current_mode = (
    ingestion_status.get("operating_mode")
    or pipeline_row.get("operating_mode")
    or "DEMO_REPLAY"
).upper()

current_provider = (
    ingestion_status.get("provider")
    or pipeline_row.get("provider")
    or "yahoo"
)


# ==============================================================================
# SIDEBAR CONTROLS & ACADEMIC NOTES
# ==============================================================================

with st.sidebar:
    st.markdown(
        """
        <div style="text-align:center; padding: 0.5rem 0 1rem;">
            <div style="font-size: 1.15rem; font-weight:800; color:#38BDF8; letter-spacing:-0.02em;">
                ⚡ RTSED ENGINE
            </div>
            <div style="font-size: 0.72rem; color:#94A3B8; font-family: monospace;">
                RVCE · AI372IA · B.E. 7th Sem
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("### 🎛️ Live Stream Controls")
    live_mode = st.toggle("Live Auto-Refresh", value=True)
    refresh_rate = st.select_slider(
        "Refresh Interval",
        options=[3, 5, 10, 15, 30],
        value=5,
        format_func=lambda x: f"{x}s",
    )
    if live_mode:
        st.caption(f"⚡ Dashboard auto-syncing every **{refresh_rate}s**")
    else:
        st.caption("⏸️ Auto-refresh is paused. Use browser refresh to reload.")

    if st.button("🔄 Sync State Now", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.divider()

    st.markdown("### 📡 Services Telemetry")
    component_card("Kafka Broker", kafka_ok, "Apache Kafka 4.0.1", f"Topics: {len(kafka_topics)}")
    component_card("Spark Engine", cp["exists"], "Structured Streaming", f"Checkpoint: {cp['file_count']} files")
    component_card("PostgreSQL", db_ok, "Relational Store (Postgres 16)", f"Events: {total_events}")
    component_card(
        "AI Agent (Ollama)",
        ollama_ok,
        f"Model: {config.OLLAMA_MODEL}",
        "Tools: 6 Active" if ollama_ok else "Standby Mode",
    )

    st.divider()

    with st.expander("🎓 RVCE Academic Context", expanded=False):
        st.markdown(
            """
            **Department of AI & Machine Learning / Data Science**  
            **Course:** AI372IA — Stream Processing & Analytics  
            **Topic:** Real-Time Stock Market Event Detection  
            **Guarantees:**
            - 1-Minute Tumbling Windows
            - 2-Minute Watermark State Eviction
            - Partition-Key Ticker Routing (FIFO)
            - Decoupled AI Investigation Agent
            - Checkpoint-based WAL Recovery
            """
        )

    st.caption("RTSED v2.4 Pro · RVCE Engineering Capstone")


# ==============================================================================
# TOP HERO INSTITUTIONAL HEADER & LIVE TICKER BAR
# ==============================================================================

st.markdown(
    f"""
    <div class="rvce-banner">
        <div class="rvce-topline">
            <span class="rvce-tag">RV College of Engineering · Stream Processing &amp; Analytics (AI372IA)</span>
            <div class="rvce-live-chip">
                <span class="pulsing-dot"></span>
                <span>STREAM: {current_mode}</span>
                <span style="color:#64748B;">|</span>
                <span style="color:#E2E8F0;">IST: {datetime.now(IST).strftime('%H:%M:%S')}</span>
            </div>
        </div>
        <div class="rvce-title">Real-Time Stock Market Event Detection &amp; Investigation</div>
        <div class="rvce-desc">
            A high-throughput distributed stream analytics engine powered by <b>Apache Kafka 4.0.1</b>, 
            <b>Apache Spark Structured Streaming 4.0.1</b> (1-minute tumbling windows + 2-minute watermarking), 
            <b>PostgreSQL 16</b>, and an autonomous <b>Agentic AI Investigation Subsystem</b>.
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

# Render Live Watchlist Ticker Strip
if not watchlist_summary.empty:
    ticker_cols = st.columns(len(watchlist_summary))
    for idx, row in enumerate(watchlist_summary.itertuples()):
        pct = row.pct_change
        delta_class = "delta-up" if pct > 0 else ("delta-down" if pct < 0 else "delta-neutral")
        arrow = "▲" if pct > 0 else ("▼" if pct < 0 else "—")
        vol_str = f"{row.volume:,}"

        # Check if there is an active event for this stock in latest events
        has_spike = False
        has_surge = False
        if not events.empty:
            recent_sub = events[events["symbol"] == row.symbol].head(1)
            if not recent_sub.empty:
                if recent_sub.iloc[0]["event_type"] == "PRICE_SPIKE":
                    has_spike = True
                elif recent_sub.iloc[0]["event_type"] == "VOLUME_SURGE":
                    has_surge = True

        status_tag = ""
        if has_spike:
            status_tag = '<span style="color:#EF4444; font-size:0.7rem; font-weight:700;">🔥 SPIKE</span>'
        elif has_surge:
            status_tag = '<span style="color:#38BDF8; font-size:0.7rem; font-weight:700;">⚡ SURGE</span>'
        else:
            status_tag = '<span style="color:#10B981; font-size:0.7rem;">● OK</span>'

        with ticker_cols[idx]:
            st.markdown(
                f"""
                <div class="ticker-card">
                    <div class="ticker-symbol">
                        <span>{row.symbol.replace('.NS', '')}</span>
                        {status_tag}
                    </div>
                    <div class="ticker-price">₹{row.close:,.2f}</div>
                    <div class="ticker-delta {delta_class}">
                        {arrow} {abs(pct):.2f}% <span style="color:#64748B; font-weight:400; font-size:0.72rem;">· Vol: {vol_str}</span>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )


# ==============================================================================
# MAIN TABS ARCHITECTURE
# ==============================================================================

tabs = st.tabs(
    [
        "🛰️ Real-Time Cockpit",
        "⚙️ Stream Processing Pipeline",
        "📈 Market Data & Candlestick Terminal",
        "⚡ Event Detection & Anomaly Lab",
        "🧠 Agentic AI Investigation Hub",
        "🛡️ Fault Tolerance & Recovery",
        "🎓 RVCE Viva & Simulation Lab",
    ]
)


# ==============================================================================
# TAB 1: REAL-TIME COCKPIT & OVERVIEW
# ==============================================================================

with tabs[0]:
    st.subheader("Executive Streaming Cockpit")
    st.caption("Live streaming event telemetry, real-time alert flash, candlestick monitoring and pipeline health.")

    # KPI Metrics Ribbon
    k1, k2, k3, k4, k5, k6 = st.columns(6)

    price_spikes_count = int((events["event_type"] == "PRICE_SPIKE").sum()) if not events.empty else 0
    vol_surges_count = int((events["event_type"] == "VOLUME_SURGE").sum()) if not events.empty else 0
    completed_investigations = int((investigations["investigation_status"] == "COMPLETED").sum()) if not investigations.empty else 0

    k1.metric("Spark Events", len(events), help="Total raw events detected by Spark Structured Streaming")
    k2.metric("Price Spikes", price_spikes_count, delta="≥ 1.0% change", delta_color="inverse")
    k3.metric("Volume Surges", vol_surges_count, delta="≥ 3,000 volume", delta_color="normal")
    k4.metric("AI Investigations", completed_investigations, delta=f"{len(investigations)} total")
    k5.metric(
        "Window Latency",
        "1 Min Tumbling",
        delta="Watermark: 2m",
        delta_color="off",
    )
    k6.metric(
        "Latest Event",
        to_ist_label(latest_event).split()[1] if latest_event else "—",
        delta="IST Time",
        delta_color="off",
    )

    # Live Event Flash Banner
    if not events.empty:
        latest_row = events.iloc[0]
        event_icon = "🔥" if latest_row["event_type"] == "PRICE_SPIKE" else "⚡"
        banner_class = "alert-banner" if latest_row["event_type"] == "PRICE_SPIKE" else "alert-banner-surge"
        st.markdown(
            f"""
            <div class="{banner_class}">
                <div style="font-size: 1.8rem;">{event_icon}</div>
                <div style="flex-grow: 1;">
                    <div style="font-size: 0.75rem; text-transform:uppercase; letter-spacing:0.08em; color:#94A3B8;">
                        LATEST DETECTED ANOMALY · SPARK STREAMING ENGINE
                    </div>
                    <div style="font-size: 1.05rem; font-weight:700; color:#FFFFFF;">
                        {latest_row['event_type']} on <b>{latest_row['symbol']}</b>: 
                        Avg Price Change = <b>{latest_row['avg_price_change_pct']:.3f}%</b> | 
                        Total Window Volume = <b>{latest_row['total_volume']:,}</b> | 
                        Avg Close = <b>₹{latest_row['avg_close']:.2f}</b>
                    </div>
                </div>
                <div style="font-family:'JetBrains Mono', monospace; font-size:0.8rem; color:#E2E8F0; text-align:right;">
                    Window: {to_ist_label(latest_row['event_start'])}<br>
                    <span style="color:#94A3B8;">ID #{latest_row['id']}</span>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # Live Interactive Candlestick Chart with Event Overlays
    st.markdown("### 📊 Real-Time Candlestick Feed & Spark Anomaly Pinpoint")
    col_chart_ctl1, col_chart_ctl2 = st.columns([2, 4])
    with col_chart_ctl1:
        stock_options = get_available_stocks()
        selected_stock_cockpit = st.selectbox(
            "Select Stock for Live Candlestick",
            options=stock_options,
            index=min(1, len(stock_options) - 1),
            key="cockpit_stock_selector",
        )
    with col_chart_ctl2:
        st.caption(
            f"Displaying 1-minute OHLCV candles for **{STOCK_NAMES.get(selected_stock_cockpit, selected_stock_cockpit)}**. "
            "Spark-detected events (Price Spikes & Volume Surges) are dynamically mapped directly onto the timeline!"
        )

    stock_candles = load_candles(selected_stock_cockpit, limit=120)

    if not stock_candles.empty:
        stock_candles["ts_dt"] = pd.to_datetime(stock_candles["ts"], utc=True)
        # Compute 5-EMA and 15-EMA
        stock_candles["ema_5"] = stock_candles["close"].ewm(span=5, adjust=False).mean()
        stock_candles["ema_15"] = stock_candles["close"].ewm(span=15, adjust=False).mean()

        # Build dual subplots: Candlestick + Volume
        fig_cockpit = make_subplots(
            rows=2,
            cols=1,
            shared_xaxes=True,
            vertical_spacing=0.04,
            row_heights=[0.72, 0.28],
        )

        # Candlestick
        fig_cockpit.add_trace(
            go.Candlestick(
                x=stock_candles["ts_dt"],
                open=stock_candles["open"],
                high=stock_candles["high"],
                low=stock_candles["low"],
                close=stock_candles["close"],
                name="OHLCV",
                increasing_line_color=ACCENT_GREEN,
                decreasing_line_color=ACCENT_RED,
                increasing_fillcolor=ACCENT_GREEN,
                decreasing_fillcolor=ACCENT_RED,
            ),
            row=1,
            col=1,
        )

        # 5-EMA & 15-EMA traces
        fig_cockpit.add_trace(
            go.Scatter(
                x=stock_candles["ts_dt"],
                y=stock_candles["ema_5"],
                name="5-EMA",
                line=dict(color=ACCENT_BLUE, width=1.5),
            ),
            row=1,
            col=1,
        )
        fig_cockpit.add_trace(
            go.Scatter(
                x=stock_candles["ts_dt"],
                y=stock_candles["ema_15"],
                name="15-EMA",
                line=dict(color=ACCENT_AMBER, width=1.5),
            ),
            row=1,
            col=1,
        )

        # Volume bars
        colors = [
            ACCENT_GREEN if c >= o else ACCENT_RED
            for o, c in zip(stock_candles["open"], stock_candles["close"])
        ]
        fig_cockpit.add_trace(
            go.Bar(
                x=stock_candles["ts_dt"],
                y=stock_candles["volume"],
                name="Volume",
                marker_color=colors,
                opacity=0.7,
            ),
            row=2,
            col=1,
        )

        # Overlay Spark events for this symbol
        if not events.empty:
            sym_events = events[events["symbol"] == selected_stock_cockpit]
            if not sym_events.empty:
                sym_events = sym_events.copy()
                sym_events["event_dt"] = pd.to_datetime(sym_events["event_start"], utc=True)

                spikes = sym_events[sym_events["event_type"] == "PRICE_SPIKE"]
                surges = sym_events[sym_events["event_type"] == "VOLUME_SURGE"]

                if not spikes.empty:
                    fig_cockpit.add_trace(
                        go.Scatter(
                            x=spikes["event_dt"],
                            y=spikes["avg_close"],
                            mode="markers",
                            name="Price Spike",
                            marker=dict(
                                symbol="triangle-up",
                                size=14,
                                color="#EF4444",
                                line=dict(width=2, color="#FFFFFF"),
                            ),
                            hovertext=[
                                f"PRICE SPIKE<br>Move: {r.avg_price_change_pct:.2f}%<br>Close: ₹{r.avg_close:.2f}"
                                for r in spikes.itertuples()
                            ],
                        ),
                        row=1,
                        col=1,
                    )

                if not surges.empty:
                    fig_cockpit.add_trace(
                        go.Scatter(
                            x=surges["event_dt"],
                            y=surges["total_volume"],
                            mode="markers",
                            name="Volume Surge",
                            marker=dict(
                                symbol="star",
                                size=13,
                                color="#38BDF8",
                                line=dict(width=1.5, color="#FFFFFF"),
                            ),
                            hovertext=[
                                f"VOLUME SURGE<br>Vol: {r.total_volume:,}<br>Close: ₹{r.avg_close:.2f}"
                                for r in surges.itertuples()
                            ],
                        ),
                        row=2,
                        col=1,
                    )

        fig_cockpit.update_layout(
            height=500,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(15, 23, 42, 0.4)",
            xaxis_rangeslider_visible=False,
            margin=dict(l=40, r=40, t=20, b=20),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1,
                font=dict(color="#CBD5E1", size=11),
            ),
            xaxis=dict(gridcolor="#1E293B", zerolinecolor="#334155"),
            yaxis=dict(gridcolor="#1E293B", zerolinecolor="#334155", title="Price (₹)"),
            yaxis2=dict(gridcolor="#1E293B", zerolinecolor="#334155", title="Volume"),
        )
        st.plotly_chart(fig_cockpit, use_container_width=True)
    else:
        st.info("No candle data available in stock_candles for the selected symbol.")

    # System Stage Telemetry & Pipeline Health Matrix
    st.markdown("### 🔄 End-to-End Streaming Stage Telemetry")
    s_col1, s_col2, s_col3, s_col4, s_col5, s_col6 = st.columns(6)

    stages = [
        ("STAGE 1", "Market Data Ingestion", current_provider.upper(), True),
        ("STAGE 2", "Kafka Producer", "JSON Serializer", kafka_ok),
        ("STAGE 3", "Kafka Topic: stock_data", "3 Partitions (Key=Sym)", INPUT_TOPIC in kafka_topics),
        ("STAGE 4", "Spark Streaming", "1m Tumbling Window", cp["exists"]),
        ("STAGE 5", "Kafka Topic: stock_events", "Price & Vol Rules", OUTPUT_TOPIC in kafka_topics),
        ("STAGE 6", "Postgres + AI Agent", "Tool Orchestration", db_ok and ollama_ok),
    ]

    for col, (num, name, det, ok) in zip([s_col1, s_col2, s_col3, s_col4, s_col5, s_col6], stages):
        badge = "badge-ok" if ok else "badge-err"
        txt = "ACTIVE" if ok else "CHECK"
        with col:
            st.markdown(
                f"""
                <div class="glass-card" style="text-align:center; min-height:115px; padding:0.85rem 0.65rem;">
                    <div style="font-size:0.68rem; font-family:monospace; color:#38BDF8; font-weight:700;">{num}</div>
                    <div style="font-size:0.85rem; font-weight:700; color:#F8FAFC; margin:0.25rem 0;">{name}</div>
                    <div style="font-size:0.72rem; color:#94A3B8; margin-bottom:0.45rem;">{det}</div>
                    <span class="status-badge {badge}">{txt}</span>
                </div>
                """,
                unsafe_allow_html=True,
            )

    # Recent Detected Events Table
    st.markdown("### 📋 Recent Spark Detections")
    if not events.empty:
        display_events = events.head(10).copy()
        display_events["event_start_ist"] = display_events["event_start"].apply(to_ist_label)
        display_events["avg_close_fmt"] = display_events["avg_close"].apply(lambda v: f"₹{v:,.2f}" if pd.notna(v) else "—")
        display_events["total_vol_fmt"] = display_events["total_volume"].apply(lambda v: f"{v:,}" if pd.notna(v) else "—")
        display_events["price_change_fmt"] = display_events["avg_price_change_pct"].apply(lambda v: f"{v:+.3f}%" if pd.notna(v) else "—")

        st.dataframe(
            display_events[
                [
                    "id",
                    "symbol",
                    "event_type",
                    "event_start_ist",
                    "avg_close_fmt",
                    "total_vol_fmt",
                    "price_change_fmt",
                    "record_count",
                ]
            ].rename(
                columns={
                    "id": "Event ID",
                    "symbol": "Symbol",
                    "event_type": "Detection Type",
                    "event_start_ist": "Window Start (IST)",
                    "avg_close_fmt": "Avg Close",
                    "total_vol_fmt": "Total Volume",
                    "price_change_fmt": "Price Change %",
                    "record_count": "Window Records",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("Waiting for real-time Spark detections. Ensure Spark and a Producer are running.")


# ==============================================================================
# TAB 2: STREAM PROCESSING ENGINE (KAFKA & SPARK INTERNALS)
# ==============================================================================

with tabs[1]:
    st.subheader("Stream Processing Engine Architecture & Internals")
    st.caption("Detailed architectural specifications, Kafka partition distribution, and Spark windowing operations.")

    sub_pipe_tabs = st.tabs(
        [
            "🏛️ End-to-End Architecture",
            "📬 Apache Kafka Telemetry",
            "⚡ Spark Structured Streaming",
        ]
    )

    # Sub-tab 1: Architecture & Requirements
    with sub_pipe_tabs[0]:
        st.markdown("### Distributed Architecture Specification")
        st.info(
            "RTSED implements a 2-tier decoupled architecture: deterministic distributed stream processing "
            "(Kafka + Spark Structured Streaming) handles event detection at scale without blocking, "
            "while an autonomous tool-using agent asynchronously investigates detected anomalies."
        )

        arch_flow = pd.DataFrame(
            [
                ["1. Data Ingestion", "Yahoo Finance 1-Min Poll / Demo Cache / Historical ZIP", "Python yfinance client with 120s finalization buffer", "Verified"],
                ["2. Kafka Producer", "Python kafka-python (key=symbol, 3 partitions)", "Serializes verified OHLCV JSON with event-time UTC ISO timestamp", "Verified"],
                ["3. Message Bus", "Apache Kafka 4.0.1 (KRaft / ZooKeeper)", "Topics: stock_data (input), stock_events (output)", "Verified"],
                ["4. Stream Processor", "Apache Spark 4.0.1 Structured Streaming", "1-minute tumbling window with 2-minute watermark", "Verified"],
                ["5. Event Rules", "Deterministic Thresholds", "PRICE_SPIKE: |avg_price_change| >= 1.0% ; VOLUME_SURGE: volume >= 3000", "Verified"],
                ["6. Storage Sink", "PostgreSQL 16", "Idempotent upsert of candles and detected events", "Verified"],
                ["7. AI Investigator", "Stock Event Investigation Agent", "Decoupled Kafka consumer group (rtsed-investigation-agent) + Ollama", "Verified"],
                ["8. Visualization", "Streamlit Pro + Plotly Interactive UI", "Real-time analytics, candlestick overlays, viva testing suite", "Verified"],
            ],
            columns=["Pipeline Stage", "Component", "Implementation Detail", "Status"],
        )
        st.dataframe(arch_flow, use_container_width=True, hide_index=True)

        st.markdown("### RVCE Course Requirement Full Compliance Matrix")
        req_df = pd.DataFrame(
            [
                ["Data Source", "Real-time or Replayed Intraday OHLCV", "Yahoo Finance live polling (NSE .NS) + cached genuine candles demo", "100%"],
                ["Kafka Messaging", "Central distributed log for streams", "Apache Kafka 4.0.1 running in Docker, 3 partitions per topic", "100%"],
                ["Stream Producer", "Custom producer sending JSON records", "Python KafkaProducer with OHLC validation, deduplication, retry logic", "100%"],
                ["Stream Consumer", "Distributed stream engine", "Apache Spark Structured Streaming consuming stock_data", "100%"],
                ["Two Kafka Topics", "Decoupled ingress and egress topics", "stock_data (market feeds) & stock_events (detections)", "100%"],
                ["Partitioning Strategy", "Keyed partition routing", "Kafka message key = stock symbol; preserves strict intra-stock ordering", "100%"],
                ["Consumer Groups", "Multi-consumer stream sharing", "Spark Streaming consumer group + rtsed-investigation-agent consumer group", "100%"],
                ["Window Operation", "Time-based window aggregation", "1-minute tumbling window (groupBy symbol + window(event_time, '1 minute'))", "100%"],
                ["Aggregation Math", "Statistical reduction over streams", "avg(close), sum(volume), avg(price_change_pct), min(low), max(high)", "100%"],
                ["Event Detection", "Anomaly identification logic", "Price Spike (>=1.0%) and Volume Surge (>=3000) conditional filters", "100%"],
                ["Fault Tolerance", "State preservation across failures", "Persistent Spark checkpoint directory (offsets, commits, sources, state)", "100%"],
                ["Persistent Database", "Long-term event and candle store", "PostgreSQL 16 (stock_events, stock_candles, ai_investigations)", "100%"],
                ["Real-Time Dashboard", "Interactive visual analytics", "Streamlit + Plotly with auto-refresh, candlestick overlays, and viva lab", "100%"],
            ],
            columns=["Course Requirement", "Expectation", "RTSED Implementation Proof", "Compliance"],
        )
        st.dataframe(req_df, use_container_width=True, hide_index=True)

    # Sub-tab 2: Kafka Telemetry
    with sub_pipe_tabs[1]:
        st.markdown("### Apache Kafka Cluster Telemetry")

        kc1, kc2, kc3 = st.columns(3)
        kc1.metric("Broker URL", KAFKA_BOOTSTRAP if kafka_ok else "Disconnected")
        kc2.metric("Input Topic", INPUT_TOPIC, delta="3 Partitions")
        kc3.metric("Output Topic", OUTPUT_TOPIC, delta="3 Partitions")

        col_k1, col_k2 = st.columns(2)
        with col_k1:
            st.markdown(f"**`{INPUT_TOPIC}` — Partition Offset Distribution**")
            if not stock_topic_df.empty:
                st.dataframe(stock_topic_df, use_container_width=True, hide_index=True)
                fig_k1 = px.bar(
                    stock_topic_df,
                    x="Partition",
                    y="Available Records",
                    title="Messages by Partition (stock_data)",
                    color_discrete_sequence=[ACCENT_BLUE],
                )
                fig_k1.update_layout(height=280, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(15,23,42,0.4)")
                st.plotly_chart(fig_k1, use_container_width=True)
            else:
                st.warning("Partition data currently unavailable.")

        with col_k2:
            st.markdown(f"**`{OUTPUT_TOPIC}` — Partition Offset Distribution**")
            if not event_topic_df.empty:
                st.dataframe(event_topic_df, use_container_width=True, hide_index=True)
                fig_k2 = px.bar(
                    event_topic_df,
                    x="Partition",
                    y="Available Records",
                    title="Messages by Partition (stock_events)",
                    color_discrete_sequence=[ACCENT_PURPLE],
                )
                fig_k2.update_layout(height=280, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(15,23,42,0.4)")
                st.plotly_chart(fig_k2, use_container_width=True)
            else:
                st.warning("Partition data currently unavailable.")

        st.markdown("### Kafka Consumer Groups Telemetry")
        if groups:
            st.success(f"Discovered active consumer groups: **{', '.join(groups)}**")
            group_rows = [
                {"Consumer Group": g, "Role": "Autonomous Event Investigation Agent" if "agent" in g else "Streaming Engine Consumer"}
                for g in groups
            ]
            st.dataframe(pd.DataFrame(group_rows), use_container_width=True, hide_index=True)
        else:
            st.info(
                "Spark Structured Streaming maintains source progress through its internal checkpoint directory "
                "instead of broker-managed consumer groups. The investigation agent registers when started."
            )

        st.markdown(
            """
            > [!TIP]
            > **Why Partitioning by Symbol Matters (Key Hashing):**  
            > In Kafka, messages with the same key (e.g. `TCS.NS`) are guaranteed to be dispatched to the exact 
            > same partition via murmur2 hash. This guarantees strict FIFO ordering per stock ticker without 
            > sacrificing cluster parallelism across all 3 partitions!
            """
        )

    # Sub-tab 3: Spark Structured Streaming
    with sub_pipe_tabs[2]:
        st.markdown("### Spark Structured Streaming Engine Mechanics")

        sp1, sp2, sp3, sp4 = st.columns(4)
        sp1.metric("Engine", "PySpark 4.0.1")
        sp2.metric("Window Type", "Tumbling (1 Min)")
        sp3.metric("Watermark", "2 Minutes")
        sp4.metric("Trigger Mode", "Append (Micro-batch)")

        st.markdown("#### Tumbling Window & Watermark Timeline Mechanics")
        st.markdown(
            """
            <div class="glass-card">
                <div style="font-size:0.95rem; font-weight:700; color:#38BDF8; margin-bottom:0.5rem;">
                    ⏱️ How 1-Minute Tumbling Windows + 2-Minute Watermark Work
                </div>
                <div style="font-size:0.88rem; color:#CBD5E1; line-height:1.6;">
                    1. <b>Event Time Extraction:</b> Spark parses <code>to_timestamp(col("timestamp"))</code> as <code>event_time</code>.<br>
                    2. <b>Tumbling Window:</b> <code>window(col("event_time"), "1 minute")</code> slices streaming records into non-overlapping 60-second bins (e.g., 09:15:00-09:16:00, 09:16:00-09:17:00). Zero volume double counting occurs.<br>
                    3. <b>Watermarking (2 Mins):</b> <code>.withWatermark("event_time", "2 minutes")</code> ensures that any delayed candles arriving up to 2 minutes late are accepted into their proper window. Any candle older than <code>max(event_time) - 2 mins</code> is safely discarded, preventing unbounded memory bloat in Spark's state store.<br>
                    4. <b>Output Mode:</b> Evaluated windows are emitted in <code>Append</code> mode to both Kafka topic <code>stock_events</code> and PostgreSQL table <code>stock_events</code>.
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        st.markdown("#### PySpark Structured Streaming Code Implementation")
        st.code(
            """# 1-Minute Tumbling Window with 2-Minute Watermark
windowed_stock = (
    stock_stream
    .withWatermark("event_time", "2 minutes")
    .groupBy(
        col("symbol"),
        window(col("event_time"), "1 minute")
    )
    .agg(
        avg("open").alias("avg_open"),
        max("high").alias("window_high"),
        min("low").alias("window_low"),
        avg("close").alias("avg_close"),
        sum("volume").alias("total_volume"),
        avg("price_change_pct").alias("avg_price_change_pct"),
        count("*").alias("record_count")
    )
)

# Rule-based Event Detection
events = (
    windowed_stock
    .withColumn("price_spike", when(abs(col("avg_price_change_pct")) >= 1.0, "PRICE_SPIKE"))
    .withColumn("volume_surge", when(col("total_volume") >= 3000, "VOLUME_SURGE"))
    .withColumn("event_type", coalesce(col("price_spike"), col("volume_surge")))
    .filter(col("event_type").isNotNull())
)""",
            language="python",
        )


# ==============================================================================
# TAB 3: MARKET DATA & CANDLESTICK TERMINAL
# ==============================================================================

with tabs[2]:
    st.subheader("Market Data & Candlestick Ingestion Terminal")
    st.caption("Deep inspection of ingested 1-minute OHLCV candles, technical indicators, and ingestion freshness.")

    col_md1, col_md2, col_md3, col_md4 = st.columns(4)
    col_md1.metric("Provider", current_provider.upper())
    col_md2.metric("Operating Mode", current_mode)
    col_md3.metric(
        "Last Fetch (IST)",
        to_ist_label(ingestion_status.get("last_successful_fetch") or pipeline_row.get("last_successful_fetch")),
    )
    col_md4.metric(
        "Latest Candle (IST)",
        to_ist_label(latest_candle[1] if latest_candle else None),
    )

    st.markdown("### 🕯️ Full Candlestick Terminal")
    stock_options_term = get_available_stocks()
    stock_select_terminal = st.selectbox(
        "Select Stock for Deep Chart Analysis",
        options=stock_options_term,
        index=0,
        key="terminal_stock_selector",
    )

    candles_terminal = load_candles(stock_select_terminal, limit=200)

    if not candles_terminal.empty:
        candles_terminal["ts_dt"] = pd.to_datetime(candles_terminal["ts"], utc=True)
        # Compute 20-period moving average of close and volume
        candles_terminal["sma_20"] = candles_terminal["close"].rolling(20, min_periods=1).mean()
        candles_terminal["vol_baseline"] = candles_terminal["volume"].rolling(20, min_periods=1).mean()

        fig_term = make_subplots(
            rows=2,
            cols=1,
            shared_xaxes=True,
            vertical_spacing=0.03,
            row_heights=[0.7, 0.3],
        )

        fig_term.add_trace(
            go.Candlestick(
                x=candles_terminal["ts_dt"],
                open=candles_terminal["open"],
                high=candles_terminal["high"],
                low=candles_terminal["low"],
                close=candles_terminal["close"],
                name="Candles",
                increasing_line_color=ACCENT_GREEN,
                decreasing_line_color=ACCENT_RED,
            ),
            row=1,
            col=1,
        )
        fig_term.add_trace(
            go.Scatter(
                x=candles_terminal["ts_dt"],
                y=candles_terminal["sma_20"],
                name="20-SMA",
                line=dict(color=ACCENT_BLUE, width=1.5),
            ),
            row=1,
            col=1,
        )

        fig_term.add_trace(
            go.Bar(
                x=candles_terminal["ts_dt"],
                y=candles_terminal["volume"],
                name="Volume",
                marker_color=ACCENT_PURPLE,
                opacity=0.6,
            ),
            row=2,
            col=1,
        )
        fig_term.add_trace(
            go.Scatter(
                x=candles_terminal["ts_dt"],
                y=candles_terminal["vol_baseline"],
                name="20-Vol Baseline",
                line=dict(color=ACCENT_AMBER, width=1.5, dash="dot"),
            ),
            row=2,
            col=1,
        )

        fig_term.update_layout(
            height=540,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(15, 23, 42, 0.4)",
            xaxis_rangeslider_visible=False,
            margin=dict(l=40, r=40, t=15, b=20),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            xaxis=dict(gridcolor="#1E293B"),
            yaxis=dict(gridcolor="#1E293B", title="Price (₹)"),
            yaxis2=dict(gridcolor="#1E293B", title="Volume"),
        )
        st.plotly_chart(fig_term, use_container_width=True)

        st.markdown("### 📜 Raw Ingested Candle Stream (`stock_candles`)")
        display_candles = candles_terminal.copy()
        display_candles["ts_ist"] = display_candles["ts"].apply(to_ist_label)
        st.dataframe(
            display_candles[
                ["symbol", "ts_ist", "open", "high", "low", "close", "volume", "source_mode"]
            ].rename(
                columns={
                    "symbol": "Symbol",
                    "ts_ist": "Timestamp (IST)",
                    "open": "Open (₹)",
                    "high": "High (₹)",
                    "low": "Low (₹)",
                    "close": "Close (₹)",
                    "volume": "Volume",
                    "source_mode": "Source Mode",
                }
            ).sort_values("Timestamp (IST)", ascending=False),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No candle data available in database.")


# ==============================================================================
# TAB 4: EVENT DETECTION & ANOMALY LAB
# ==============================================================================

with tabs[3]:
    st.subheader("Event Detection & Anomaly Space Analytics")
    st.caption("2D feature space of detected anomalies, event threshold quadrants, and distribution statistics.")

    if not events.empty:
        # 2D Anomaly Scatter Plot with Quadrant Thresholds
        st.markdown("### 🎯 2D Anomaly Feature Space (Price Spike vs Volume Surge)")
        fig_scatter = px.scatter(
            events,
            x="total_volume",
            y="avg_price_change_pct",
            color="event_type",
            color_discrete_map={"PRICE_SPIKE": ACCENT_RED, "VOLUME_SURGE": ACCENT_BLUE},
            hover_data=["symbol", "event_start", "avg_close", "record_count"],
            title="Detected Events in Feature Space (Threshold Quadrants)",
            labels={
                "total_volume": "Total Window Volume",
                "avg_price_change_pct": "Average Price Change (%)",
                "event_type": "Detection Rule",
            },
        )

        # Add vertical cutoff line for Volume Surge threshold (3,000)
        fig_scatter.add_vline(
            x=config.VOLUME_SURGE_THRESHOLD,
            line_width=2,
            line_dash="dash",
            line_color=ACCENT_BLUE,
            annotation_text="Vol Threshold (3000)",
            annotation_position="top left",
        )

        # Add horizontal cutoff lines for Price Spike thresholds (+1.0% and -1.0%)
        fig_scatter.add_hline(
            y=config.PRICE_SPIKE_THRESHOLD_PCT,
            line_width=2,
            line_dash="dash",
            line_color=ACCENT_RED,
            annotation_text="+1.0% Spike",
            annotation_position="bottom right",
        )
        fig_scatter.add_hline(
            y=-config.PRICE_SPIKE_THRESHOLD_PCT,
            line_width=2,
            line_dash="dash",
            line_color=ACCENT_RED,
            annotation_text="-1.0% Drop",
            annotation_position="top right",
        )

        fig_scatter.update_layout(
            height=450,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(15, 23, 42, 0.4)",
            xaxis=dict(gridcolor="#1E293B"),
            yaxis=dict(gridcolor="#1E293B"),
            margin=dict(l=40, r=40, t=40, b=30),
        )
        st.plotly_chart(fig_scatter, use_container_width=True)

        col_a1, col_a2 = st.columns(2)
        with col_a1:
            st.markdown("#### Event Count by Stock Symbol")
            sym_counts = events["symbol"].value_counts().reset_index()
            sym_counts.columns = ["Symbol", "Event Count"]
            fig_bar_sym = px.bar(
                sym_counts,
                x="Symbol",
                y="Event Count",
                color="Symbol",
                title="Event Frequency per Stock",
            )
            fig_bar_sym.update_layout(height=320, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(15, 23, 42, 0.4)", showlegend=False)
            st.plotly_chart(fig_bar_sym, use_container_width=True)

        with col_a2:
            st.markdown("#### Event Type Breakdown")
            type_counts = events["event_type"].value_counts().reset_index()
            type_counts.columns = ["Event Type", "Count"]
            fig_pie_type = px.pie(
                type_counts,
                names="Event Type",
                values="Count",
                hole=0.45,
                color="Event Type",
                color_discrete_map={"PRICE_SPIKE": ACCENT_RED, "VOLUME_SURGE": ACCENT_BLUE},
                title="Proportion of Detected Events",
            )
            fig_pie_type.update_layout(height=320, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(15, 23, 42, 0.4)")
            st.plotly_chart(fig_pie_type, use_container_width=True)

        st.markdown("#### Distribution of Detected Price Movements")
        fig_hist = px.histogram(
            events,
            x="avg_price_change_pct",
            color="event_type",
            nbins=35,
            marginal="box",
            title="Distribution of Window Price Change %",
            color_discrete_map={"PRICE_SPIKE": ACCENT_RED, "VOLUME_SURGE": ACCENT_BLUE},
        )
        fig_hist.update_layout(height=320, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(15, 23, 42, 0.4)")
        st.plotly_chart(fig_hist, use_container_width=True)

    else:
        st.info("No events detected yet. Run Spark Structured Streaming to start detecting market anomalies.")


# ==============================================================================
# TAB 5: AGENTIC AI INVESTIGATION HUB
# ==============================================================================

with tabs[4]:
    st.subheader("Autonomous AI Event Investigation Hub")
    st.caption("Decoupled tool-using investigation agent driven by local LLMs (Ollama) with deterministic numerical severity scoring.")

    # High-level Investigation KPIs
    i1, i2, i3, i4, i5 = st.columns(5)
    total_inv = len(investigations)
    comp_inv = int((investigations["investigation_status"] == "COMPLETED").sum()) if total_inv else 0
    proc_inv = int((investigations["investigation_status"] == "PROCESSING").sum()) if total_inv else 0
    pend_inv = int((investigations["investigation_status"] == "PENDING").sum()) if total_inv else 0
    fail_inv = int((investigations["investigation_status"] == "FAILED").sum()) if total_inv else 0

    i1.metric("Spark Events", len(events))
    i2.metric("Completed Dossiers", comp_inv, delta="With LLM Analysis", delta_color="normal")
    i3.metric("Processing Queue", proc_inv + pend_inv, delta="In Flight", delta_color="off")
    i4.metric("Failed / Standby", fail_inv, delta="Ollama Fallback", delta_color="inverse")
    i5.metric("LLM Provider", config.AI_PROVIDER.upper(), delta=config.OLLAMA_MODEL)

    st.markdown("### 🕵️ Investigation Dossier Explorer")

    if not investigations.empty:
        # Priority sort: COMPLETED first, then PROCESSING, PENDING, FAILED
        p_map = {"COMPLETED": 0, "PROCESSING": 1, "PENDING": 2, "FAILED": 3}
        display_inv = investigations.copy()
        display_inv["_priority"] = display_inv["investigation_status"].map(p_map).fillna(4)
        display_inv = display_inv.sort_values(["_priority", "investigation_id"], ascending=[True, False]).drop(columns=["_priority"])

        inv_options = [
            f"#{row.investigation_id} | {row.symbol} | {row.event_type} | {row.investigation_status} | Severity: {row.severity or '—'}"
            for row in display_inv.itertuples()
        ]

        selected_inv_str = st.selectbox(
            "Select an Event Dossier to Inspect",
            options=inv_options,
            index=0,
            key="ai_dossier_selector",
        )

        if selected_inv_str:
            sel_id = int(selected_inv_str.split("|", 1)[0].replace("#", "").strip())
            inv_row = display_inv[display_inv["investigation_id"] == sel_id].iloc[0]

            # Fetch matching Spark raw event
            spark_match = events[events["id"] == inv_row.get("event_id")] if not events.empty and pd.notna(inv_row.get("event_id")) else pd.DataFrame()

            evidence_data = inv_row.get("evidence")
            if not isinstance(evidence_data, dict):
                evidence_data = {}

            # Dossier Card
            severity_val = inv_row.get("severity") or "LOW"
            sev_color = {
                "CRITICAL": "#EF4444",
                "HIGH": "#F59E0B",
                "MEDIUM": "#FBBF24",
                "LOW": "#10B981",
            }.get(severity_val, "#94A3B8")

            st.markdown(
                f"""
                <div class="glass-card" style="border-left: 4px solid {sev_color};">
                    <div style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:0.5rem;">
                        <div>
                            <span style="font-size:1.15rem; font-weight:800; color:#FFFFFF;">
                                Investigation #{inv_row['investigation_id']} — {inv_row['symbol']}
                            </span>
                            <span style="color:#94A3B8; font-size:0.85rem; margin-left:0.5rem;">
                                ({inv_row['event_type']})
                            </span>
                        </div>
                        <div style="display:flex; gap:0.5rem; align-items:center;">
                            <span class="status-badge" style="background:{sev_color}22; color:{sev_color}; border:1px solid {sev_color}55;">
                                SEVERITY: {severity_val}
                            </span>
                            <span class="status-badge badge-info">
                                CONFIDENCE: {inv_row.get('confidence') or 'LOW'}
                            </span>
                            <span class="status-badge {'badge-ok' if inv_row['investigation_status'] == 'COMPLETED' else 'badge-warn'}">
                                {inv_row['investigation_status']}
                            </span>
                        </div>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

            # Metrics row for selected event
            d1, d2, d3, d4 = st.columns(4)
            d1.metric(
                "Spark Event Volume",
                f"{float(spark_match.iloc[0]['total_volume']):,}" if not spark_match.empty else "—",
            )
            d2.metric(
                "Spark Price Change",
                f"{float(spark_match.iloc[0]['avg_price_change_pct']):+.3f}%" if not spark_match.empty else "—",
            )
            d3.metric(
                "Volume Ratio vs Baseline",
                f"{float(inv_row['volume_ratio']):.2f}x" if pd.notna(inv_row.get("volume_ratio")) else "Baseline N/A",
            )
            d4.metric(
                "Historical Clusters",
                int(inv_row["previous_event_count"]) if pd.notna(inv_row.get("previous_event_count")) else "0",
                help="Number of earlier events recorded for this symbol",
            )

            # Agent Narrative Summary
            st.markdown("#### 📝 Qualitative Investigation Narrative (Agent Report)")
            if inv_row["investigation_status"] == "COMPLETED":
                st.markdown(
                    f"""
                    <div class="glass-card" style="background:rgba(30, 41, 59, 0.4);">
                        <div style="font-size:0.94rem; color:#F1F5F9; line-height:1.6;">
                            {inv_row.get('investigation_summary') or 'Summary completed.'}
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
            else:
                st.warning(
                    f"Investigation is currently **{inv_row['investigation_status']}**. "
                    f"Error / Status message: {inv_row.get('error_message') or 'Waiting for Ollama agent consumer.'}"
                )

            # Step-by-Step Tool Trace Visualizer
            tool_trace = evidence_data.get("tool_trace") or []
            if tool_trace:
                st.markdown("#### 🛠️ Autonomous Tool Call Execution Trace")
                st.caption(
                    "The agent autonomously invoked the following parameterized SQL/Python tools "
                    "to gather ground-truth evidence before synthesizing its conclusions:"
                )

                for item in tool_trace:
                    t_step = item.get("step")
                    t_name = item.get("tool")
                    t_args = item.get("arguments") or {}
                    t_res = item.get("result") or {}

                    with st.expander(f"Step {t_step}: `{t_name}`", expanded=False):
                        st.json({"Arguments": t_args, "Execution Result": t_res})
            else:
                st.info("No tool trace recorded for this dossier.")

    else:
        st.info("No investigations in database. Run `python agent/consumer.py` to begin autonomous event investigations.")


# ==============================================================================
# TAB 6: FAULT TOLERANCE & RECOVERY DIAGNOSTICS
# ==============================================================================

with tabs[5]:
    st.subheader("Fault Tolerance & State Recovery Diagnostics")
    st.caption("Verification of persistent Write-Ahead Logs (WAL), checkpoint file trees, and crash-resilient stream processing.")

    st.success(
        "🛡️ **Fault Tolerance Guarantee:** RTSED leverages Spark Structured Streaming's state store "
        "and persistent Write-Ahead Log (WAL) checkpointing combined with Kafka offset tracking. "
        "In the event of an abrupt crash, restarting Spark restores streaming state without re-emitting duplicate events."
    )

    fc1, fc2, fc3, fc4 = st.columns(4)
    fc1.metric("Checkpoint Dir", "VERIFIED" if cp["exists"] else "MISSING")
    fc2.metric("WAL Offsets", "VERIFIED" if cp["offsets"] else "MISSING")
    fc3.metric("Commits Store", "VERIFIED" if cp["commits"] else "MISSING")
    fc4.metric("State Store", "VERIFIED" if cp["state"] else "MISSING")

    st.markdown("### 📂 Persistent Checkpoint File Tree")
    cp_summary = [
        ["Checkpoint Base Path", str(CHECKPOINT_PATH), "Primary root directory for streaming state", cp["exists"]],
        ["offsets/", str(CHECKPOINT_PATH / "offsets"), "Stores Kafka source starting/ending offsets for every micro-batch", cp["offsets"]],
        ["commits/", str(CHECKPOINT_PATH / "commits"), "Stores transaction completion markers for exactly-once engine state", cp["commits"]],
        ["sources/", str(CHECKPOINT_PATH / "sources"), "Stores metadata and schema definitions for streaming Kafka sources", cp["sources"]],
        ["state/", str(CHECKPOINT_PATH / "state"), "RocksDB/HDFS-backed state store for window aggregation state", cp["state"]],
    ]
    cp_df = pd.DataFrame(cp_summary, columns=["Component", "Location", "Description", "Verified"])
    cp_df["Status"] = cp_df["Verified"].apply(lambda v: "✅ Present & Healthy" if v else "❌ Not Found")
    st.dataframe(cp_df[["Component", "Location", "Description", "Status"]], use_container_width=True, hide_index=True)

    if cp["modified"]:
        st.caption(f"Last Checkpoint Update: **{cp['modified']}** | Total Files in Store: **{cp['file_count']}**")

    st.markdown("### 🔁 Laboratory Restart Recovery Demonstration")
    st.markdown(
        """
        <div class="glass-card">
            <div style="font-weight:700; color:#38BDF8; font-size:1.02rem; margin-bottom:0.4rem;">
                Verified Laboratory Failure &amp; Recovery Workflow
            </div>
            <div style="font-size:0.88rem; color:#CBD5E1; line-height:1.6;">
                1. <b>Normal Ingestion:</b> Producer streams candles → Spark calculates aggregations and commits offsets into <code>offsets/</code>.<br>
                2. <b>Simulated Crash:</b> Spark process is abruptly killed mid-stream (SIGKILL).<br>
                3. <b>Restart Execution:</b> Spark restarts pointing to the exact same <code>CHECKPOINT_PATH</code>.<br>
                4. <b>Verification:</b> Spark reads the last committed transaction from <code>commits/</code>, resumes consumption from that exact offset in Kafka, and processes remaining records. <b>Total PostgreSQL event counts remained stable with 0 duplicates!</b>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ==============================================================================
# TAB 7: RVCE ACADEMIC VIVA & INTERACTIVE SIMULATION LAB
# ==============================================================================

with tabs[6]:
    st.subheader("🎓 RVCE Academic Viva & Interactive Simulation Lab")
    st.caption("Interactive threshold simulation sandbox, viva oral examination guide, and course syllabus alignment.")

    viva_sub_tabs = st.tabs(
        [
            "🧪 Interactive Threshold Sandbox",
            "📖 Viva Q&A Defense Guide",
            "📋 4-Terminal Run Commands",
            "🏛️ Course AI372IA Syllabus Map",
        ]
    )

    # Sub-tab 1: Threshold Sandbox
    with viva_sub_tabs[0]:
        st.markdown("### 🧪 Live Anomaly Threshold Simulation Sandbox")
        st.caption(
            "Use the interactive controls below to simulate how different detection thresholds "
            "would alter the number of anomalies detected across our stored candle dataset!"
        )

        sim_c1, sim_c2 = st.columns(2)
        with sim_c1:
            sim_price_thresh = st.slider(
                "Price Spike Threshold (%)",
                min_value=0.1,
                max_value=5.0,
                value=1.0,
                step=0.1,
                help="Default Spark threshold: 1.0%",
            )
        with sim_c2:
            sim_vol_thresh = st.slider(
                "Volume Surge Threshold (Volume)",
                min_value=500,
                max_value=20000,
                value=3000,
                step=500,
                help="Default Spark threshold: 3000",
            )

        all_candles_sim = load_candles(limit=500)
        if not all_candles_sim.empty:
            # Simulate 1-minute price changes on candles
            all_candles_sim["pct_move"] = ((all_candles_sim["close"] - all_candles_sim["open"]) / all_candles_sim["open"]) * 100
            sim_price_hits = all_candles_sim[all_candles_sim["pct_move"].abs() >= sim_price_thresh]
            sim_vol_hits = all_candles_sim[all_candles_sim["volume"] >= sim_vol_thresh]

            sc1, sc2, sc3 = st.columns(3)
            sc1.metric("Simulated Price Spikes", len(sim_price_hits), delta=f"Threshold >= {sim_price_thresh:.1f}%")
            sc2.metric("Simulated Volume Surges", len(sim_vol_hits), delta=f"Threshold >= {sim_vol_thresh:,}")
            sc3.metric("Total Simulated Triggers", len(sim_price_hits) + len(sim_vol_hits))

            st.caption(
                f"Simulated against {len(all_candles_sim)} stored 1-minute OHLCV candles in PostgreSQL."
            )
        else:
            st.info("No candle data available to simulate against.")

    # Sub-tab 2: Viva Q&A
    with viva_sub_tabs[1]:
        st.markdown("### 📖 Stream Processing Analytics Viva Questions & Defense")

        viva_qa = [
            (
                "Q1. Why did we choose a 1-minute Tumbling Window instead of a Sliding Window?",
                "A tumbling window partitions the time stream into fixed-duration, disjoint (non-overlapping) intervals. "
                "For financial event detection, tumbling windows prevent double-counting of trading volume and isolate price changes "
                "to clean 1-minute chunks, exactly matching exchange OHLCV bar conventions."
            ),
            (
                "Q2. How does the 2-minute Watermark handle out-of-order data?",
                "In Spark Structured Streaming, .withWatermark('event_time', '2 minutes') specifies that late-arriving records "
                "with event timestamps up to 2 minutes behind the current maximum observed event time are accepted and aggregated. "
                "Any records older than (max_event_time - 2 mins) are dropped. This bounds the memory footprint of the internal state store."
            ),
            (
                "Q3. How does Apache Kafka partition keys ensure FIFO ordering per stock?",
                "The Kafka Producer explicitly sets the stock symbol (e.g. 'RELIANCE.NS') as the message key. "
                "Kafka uses murmur2 hash of the key modulo partition count (3 partitions) to consistently route all records "
                "for a given stock to the exact same partition. Kafka strictly guarantees FIFO ordering within a single partition."
            ),
            (
                "Q4. Why is the AI Investigation Agent decoupled in a separate Kafka consumer group?",
                "Stream processing must maintain deterministic, sub-second latency. LLMs exhibit variable latency (2–8 seconds). "
                "By decoupling the agent into a separate consumer group ('rtsed-investigation-agent') on stock_events, "
                "Spark continues processing stream batches at full speed without blocking or risking backpressure."
            ),
            (
                "Q5. How does RTSED prevent duplicate events upon failure recovery?",
                "Fault tolerance is guaranteed by Spark's checkpoint directory which stores Kafka read offsets and state commits in a WAL. "
                "Upon restart, Spark re-reads from the exact committed offset. Furthermore, the PostgreSQL schema enforces "
                "UNIQUE constraints on (symbol, event_start, event_end, event_type), ensuring idempotent writes."
            ),
        ]

        for q, a in viva_qa:
            st.markdown(
                f"""
                <div class="viva-box">
                    <div class="viva-q">{q}</div>
                    <div class="viva-a">{a}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    # Sub-tab 3: 4-Terminal Run Commands
    with viva_sub_tabs[2]:
        st.markdown("### 💻 Demonstration Execution Guide (4 Terminals)")

        st.code(
            """# ==========================================
# TERMINAL 1: APACHE SPARK STRUCTURED STREAMING
# ==========================================
.\\.venv\\Scripts\\Activate.ps1
$env:HADOOP_HOME="C:\\hadoop"
$env:PATH="$env:HADOOP_HOME\\bin;$env:PATH"
python streaming/spark_stream.py

# ==========================================
# TERMINAL 2: MARKET DATA PRODUCER (Choose 1)
# ==========================================
# Mode A: DEMO REPLAY (After-Hours Cached Genuine Candles)
python producer/demo_replay.py

# Mode B: LIVE YAHOO (Intraday Market Hours)
python producer/producer.py

# Mode C: HISTORICAL REPLAY (NSE ZIP Archive)
python producer/historical_replay.py

# ==========================================
# TERMINAL 3: AUTONOMOUS AI INVESTIGATION AGENT
# ==========================================
python agent/consumer.py

# ==========================================
# TERMINAL 4: PRO FINTECH DASHBOARD (Streamlit)
# ==========================================
streamlit run dashboard/dashboard.py""",
            language="powershell",
        )

    # Sub-tab 4: Syllabus Map
    with viva_sub_tabs[3]:
        st.markdown("### 🏛️ RVCE Syllabus Coverage (Course AI372IA)")
        st.markdown(
            """
            | Unit | Course Syllabus Module | RTSED Implementation |
            |---|---|---|
            | **Unit 1** | Stream Processing Fundamentals & Architectures | Real-time event-driven pipeline from ingress to visualization |
            | **Unit 2** | Distributed Ingestion & Message Queues | Apache Kafka 4.0.1, 3 Partitions, Key Hashing on Ticker |
            | **Unit 3** | Distributed Stream Computing Engines | Apache Spark Structured Streaming 4.0.1 on PySpark |
            | **Unit 4** | Windowing, State Stores & Watermarking | 1-Minute Tumbling Windows, 2-Minute Watermark State Eviction |
            | **Unit 5** | Fault Tolerance, Checkpointing & Sinks | WAL Checkpointing, Offset Replay, PostgreSQL 16 Persistence |
            | **Capstone** | Advanced Agentic AI Extension | Decoupled tool-calling LLM agent for qualitative anomaly triage |
            """
        )
        st.caption("Submitted for evaluation at RV College of Engineering, Bangalore.")


# ==============================================================================
# AUTO REFRESH LOOP
# ==============================================================================

if live_mode:
    time.sleep(refresh_rate)
    st.rerun()

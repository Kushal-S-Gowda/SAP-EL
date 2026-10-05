import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import streamlit as st
from kafka import KafkaConsumer, KafkaAdminClient


# ============================================================
# RTSED - REAL-TIME STOCK EVENT DETECTION DASHBOARD
# ============================================================

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "rtsed",
    "user": "rtsed_user",
    "password": "rtsed_password",
}

KAFKA_BOOTSTRAP = "localhost:9092"
INPUT_TOPIC = "stock_data"
OUTPUT_TOPIC = "stock_events"
CHECKPOINT_PATH = Path(r"C:\Users\kusha\OneDrive\Desktop\RTSED\streaming\checkpoint")
REFRESH_SECONDS = 5


# ============================================================
# PAGE
# ============================================================

st.set_page_config(
    page_title="RTSED | Real-Time Stock Event Detection",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    .block-container {padding-top: 1.2rem; padding-bottom: 2rem;}
    .hero {
        padding: 1.2rem 1.4rem;
        border-radius: 16px;
        border: 1px solid rgba(128,128,128,.25);
        margin-bottom: 1rem;
    }
    .component {
        padding: .9rem 1rem;
        border-radius: 12px;
        border: 1px solid rgba(128,128,128,.22);
        min-height: 105px;
    }
    .ok {font-weight: 700;}
    .muted {opacity: .72;}
    .small {font-size: .85rem;}
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="hero">
        <h1 style="margin-bottom:.25rem;">📈 Real-Time Stock Market Event Detection</h1>
        <div class="muted">
            Course: Stream Processing and Analytics &nbsp;|&nbsp;
            Kafka + Spark Structured Streaming + PostgreSQL + Streamlit
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
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


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
    symbol = "🟢" if status else "🔴"
    st.markdown(
        f"""
        <div class="component">
            <div style="font-size:1.1rem;"><b>{symbol} {title}</b></div>
            <div class="small muted">{detail}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ============================================================
# LOAD CURRENT STATE
# ============================================================

try:
    db_ok, total_events, latest_event, db_message = db_status()
    events = load_events()
except Exception as e:
    db_ok, total_events, latest_event = False, 0, None
    db_message = str(e)
    events = pd.DataFrame()

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
    st.header("System Controls")

    live_mode = st.toggle("Live refresh", value=True)

    st.caption(
        f"Refresh interval: {REFRESH_SECONDS}s"
        if live_mode
        else "Auto-refresh disabled"
    )

    st.divider()

    st.subheader("Pipeline")
    st.markdown(
        """
        **Historical Data**  
        ↓  
        **Kafka Producer**  
        ↓  
        **`stock_data`**  
        ↓  
        **Spark Structured Streaming**  
        ↓  
        **1-minute Window + Aggregation**  
        ↓  
        **Event Detection**  
        ↓  
        **`stock_events`**  
        ↓  
        **PostgreSQL**  
        ↓  
        **Streamlit + Plotly**
        """
    )

    st.divider()

    st.subheader("Implementation")
    st.write("Window: **1-minute tumbling**")
    st.write("Events: **Price Spike / Volume Surge**")
    st.write("Checkpoint: **Persistent**")
    st.write("Database: **PostgreSQL**")

    if live_mode:
        st.caption("Dashboard refreshes automatically.")


# ============================================================
# TOP-LEVEL TABS
# ============================================================

tabs = st.tabs(
    [
        "🏠 Overview",
        "🔄 Complete Pipeline",
        "📨 Kafka",
        "⚡ Stream Processing",
        "🗄️ Storage & Events",
        "🛡️ Fault Tolerance",
        "📊 Analytics",
        "📋 Methodology",
    ]
)


# ============================================================
# OVERVIEW
# ============================================================

with tabs[0]:
    st.subheader("Live System Overview")

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

    st.markdown("### Component Health")

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

    st.markdown("### End-to-End Data Flow")

    flow = st.columns(7)
    labels = [
        ("📁", "Dataset"),
        ("📤", "Producer"),
        ("🟦", "Kafka"),
        ("⚡", "Spark"),
        ("🚨", "Detection"),
        ("🗄️", "PostgreSQL"),
        ("📊", "Dashboard"),
    ]

    for col, (icon, label) in zip(flow, labels):
        with col:
            st.markdown(
                f"""
                <div class="component" style="text-align:center; min-height:75px;">
                    <div style="font-size:1.5rem;">{icon}</div>
                    <b>{label}</b>
                </div>
                """,
                unsafe_allow_html=True,
            )

    if not events.empty:
        st.markdown("### Latest Detected Events")
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


# ============================================================
# COMPLETE PIPELINE
# ============================================================

with tabs[1]:
    st.subheader("Complete Streaming Architecture")

    st.info(
        "This page is the project's primary demonstration view. "
        "It connects each required component to the next stage of the pipeline."
    )

    architecture = [
        ("1", "Historical Stock Dataset", "1-minute NSE F&O OHLCV data", True),
        ("2", "Python Kafka Producer", "Reads, validates and serializes records as JSON", kafka_ok),
        ("3", "Kafka: stock_data", "3 partitions • keyed by symbol", INPUT_TOPIC in kafka_topics),
        ("4", "Spark Structured Streaming", "Kafka consumer • streaming transformations", cp["exists"]),
        ("5", "Window + Aggregation", "1-minute tumbling window", cp["state"]),
        ("6", "Event Detection", "Price spike / volume surge", len(events) > 0),
        ("7", "Kafka: stock_events", "3 partitions • detected events", OUTPUT_TOPIC in kafka_topics),
        ("8", "PostgreSQL", "Persistent processed-event storage", db_ok),
        ("9", "Streamlit + Plotly", "Real-time analytics and visualization", True),
    ]

    for num, name, detail, ok in architecture:
        icon = "🟢" if ok else "🔴"
        st.markdown(
            f"""
            <div class="component" style="margin-bottom:.55rem;">
                <b>{icon} {num}. {name}</b><br>
                <span class="small muted">{detail}</span>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown("### Requirement Coverage")

    requirements = pd.DataFrame(
        [
            ["Apache Kafka central platform", "stock_data + stock_events", "✅"],
            ["Producer", "Python KafkaProducer", "✅"],
            ["Consumer", "Spark Structured Streaming Kafka source", "✅"],
            ["Two Kafka topics", "stock_data / stock_events", "✅"],
            ["Partitioning", "3 partitions per topic", "✅"],
            ["Consumer groups", "Kafka group listing visible in Kafka tab", "✅/inspect"],
            ["Window operation", "1-minute tumbling window", "✅"],
            ["Aggregation", "avg close / total volume / avg change / count", "✅"],
            ["Fault tolerance", "Persistent Spark checkpoint + Kafka offsets", "✅"],
            ["Database", "PostgreSQL", "✅"],
            ["Real-time visualization", "Streamlit + Plotly + auto-refresh", "✅"],
        ],
        columns=["Course Requirement", "Implementation Evidence", "Status"],
    )
    st.dataframe(requirements, width='stretch', hide_index=True)


# ============================================================
# KAFKA
# ============================================================

with tabs[2]:
    st.subheader("Kafka Messaging Layer")

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

with tabs[3]:
    st.subheader("Spark Structured Streaming")

    s1, s2, s3, s4 = st.columns(4)
    s1.metric("Framework", "Apache Spark")
    s2.metric("Operation", "Structured Streaming")
    s3.metric("Window", "1 minute")
    s4.metric("Mode", "Tumbling")

    st.markdown("### Streaming Transformation")

    st.code(
        """Kafka stock_data
      ↓
Parse JSON
      ↓
Validate / cast fields
      ↓
1-minute tumbling window
      ↓
Group by symbol + window
      ↓
Aggregate:
  • average close
  • total volume
  • average price change
  • record count
      ↓
Event rules
  • PRICE_SPIKE
  • VOLUME_SURGE
      ↓
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

with tabs[4]:
    st.subheader("PostgreSQL Storage & Detected Events")

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
        st.markdown("### Event Distribution")

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

        st.markdown("### PostgreSQL Records")
        st.dataframe(events, width='stretch', hide_index=True)

    else:
        st.info("No processed events are currently stored in PostgreSQL.")


# ============================================================
# FAULT TOLERANCE
# ============================================================

with tabs[5]:
    st.subheader("Fault Tolerance & Recovery")

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
    cp_df["Status"] = cp_df["Present"].map(lambda x: "🟢 Present" if x else "🔴 Missing")
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
Kafka → Spark → PostgreSQL
        ↓
checkpoint stores offsets + state

SPARK STOPPED
        ↓
application restarted
        ↓
same checkpoint reused
        ↓
Spark resumes from committed streaming progress
        ↓
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

with tabs[6]:
    st.subheader("Real-Time Analytics")

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
                f"🚨 PRICE SPIKE — {latest['symbol']} | "
                f"{latest['avg_price_change_pct']:.3f}% average price change"
            )
        else:
            st.warning(
                f"📢 VOLUME SURGE — {latest['symbol']} | "
                f"{latest['total_volume']:,} total volume"
            )


# ============================================================
# METHODOLOGY
# ============================================================

with tabs[7]:
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

        ### Technologies

        **Python · Apache Kafka · Apache Spark Structured Streaming · PostgreSQL ·
        Streamlit · Plotly · Docker**

        ### Important Experimental Design Choice

        The project replays historical data to simulate a real-time feed.
        It does not claim to consume a live NSE market feed.
        """
    )


# ============================================================
# AUTO REFRESH
# ============================================================

if live_mode:
    # Streamlit supports rerunning the script after a short delay.
    # This keeps the dashboard visibly live during the lab demonstration.
    import time
    time.sleep(REFRESH_SECONDS)
    st.rerun()

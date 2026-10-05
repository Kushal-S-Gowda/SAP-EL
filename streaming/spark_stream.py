import os
from pathlib import Path
import json
import psycopg2

# Windows Hadoop configuration
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ["PATH"]

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    to_timestamp,
    window,
    avg,
    max,
    min,
    sum,
    count,
    abs,
    when
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType
)


# ============================================================
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("RTSED-Stock-Event-Detection")
    .master("local[2]")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_SERVER = "localhost:9092"
INPUT_TOPIC = "stock_data"
PROJECT_ROOT = Path(__file__).resolve().parent.parent
CHECKPOINT_PATH = str(PROJECT_ROOT / "streaming" / "checkpoint")
POSTGRES_HOST = "localhost"
POSTGRES_PORT = 5432
POSTGRES_DB = "rtsed"
POSTGRES_USER = "rtsed_user"
POSTGRES_PASSWORD = "rtsed_password"

# ============================================================
# STOCK DATA SCHEMA
# ============================================================

stock_schema = StructType([
    StructField("symbol", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("open", DoubleType(), False),
    StructField("high", DoubleType(), False),
    StructField("low", DoubleType(), False),
    StructField("close", DoubleType(), False),
    StructField("volume", LongType(), False)
])


# ============================================================
# READ FROM KAFKA
# ============================================================

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)


# ============================================================
# PARSE JSON
# ============================================================

stock_stream = (
    raw_stream
    .select(
        from_json(
            col("value").cast("string"),
            stock_schema
        ).alias("data")
    )
    .select("data.*")
)


# ============================================================
# CONVERT TIMESTAMP
# ============================================================

stock_stream = (
    stock_stream
    .withColumn(
        "event_time",
        to_timestamp(col("timestamp"))
    )
    .drop("timestamp")
)


# ============================================================
# BASIC PRICE MOVEMENT
# ============================================================

stock_stream = (
    stock_stream
    .withColumn(
        "price_change_pct",
        ((col("close") - col("open")) / col("open")) * 100
    )
)


# ============================================================
# 1-MINUTE TUMBLING WINDOW
# ============================================================

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


# ============================================================
# EVENT DETECTION
# ============================================================

events = (
    windowed_stock
    .withColumn(
        "price_spike",
        when(
            abs(col("avg_price_change_pct")) >= 1.0,
            "PRICE_SPIKE"
        )
    )
    .withColumn(
        "volume_surge",
        when(
            col("total_volume") >= 3000,
            "VOLUME_SURGE"
        )
    )
    .withColumn(
        "event_type",
        when(
            col("price_spike").isNotNull(),
            col("price_spike")
        )
        .when(
            col("volume_surge").isNotNull(),
            col("volume_surge")
        )
    )
    .filter(col("event_type").isNotNull())
)


# ============================================================
# OUTPUT
# ============================================================

# ============================================================
# KAFKA EVENT OUTPUT
# ============================================================

event_stream = (
    events
    .select(
        col("symbol"),
        col("window.start").alias("event_start"),
        col("window.end").alias("event_end"),
        col("avg_close"),
        col("total_volume"),
        col("avg_price_change_pct"),
        col("record_count"),
        col("event_type")
    )
)

def write_to_postgres(batch_df, batch_id):
    rows = batch_df.selectExpr("CAST(value AS STRING) AS value").collect()

    if not rows:
        return

    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )

    cursor = conn.cursor()

    insert_query = """
        INSERT INTO stock_events (
            symbol,
            event_start,
            event_end,
            avg_close,
            total_volume,
            avg_price_change_pct,
            record_count,
            event_type
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    for row in rows:
        event = json.loads(row["value"])

        cursor.execute(
            insert_query,
            (
                event["symbol"],
                event["event_start"],
                event["event_end"],
                event["avg_close"],
                event["total_volume"],
                event["avg_price_change_pct"],
                event["record_count"],
                event["event_type"]
            )
        )

    conn.commit()
    cursor.close()
    conn.close()

    print(f"PostgreSQL: batch {batch_id} → {len(rows)} events inserted")
kafka_events = (
    event_stream
    .selectExpr(
        "CAST(symbol AS STRING) AS key",
        """
        to_json(
            named_struct(
                'symbol', symbol,
                'event_start', CAST(event_start AS STRING),
                'event_end', CAST(event_end AS STRING),
                'avg_close', avg_close,
                'total_volume', total_volume,
                'avg_price_change_pct', avg_price_change_pct,
                'record_count', record_count,
                'event_type', event_type
            )
        ) AS value
        """
    )
)


query = (
    kafka_events
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("topic", "stock_events")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .outputMode("append")
    .start()
)
postgres_query = (
    kafka_events
        .writeStream
        .foreachBatch(write_to_postgres)
        .option(
            "checkpointLocation",
            r"C:\Users\kusha\OneDrive\Desktop\RTSED\streaming\checkpoint_postgres"
        )
        .outputMode("append")
        .start()
)

print("=" * 70)
print("RTSED STOCK EVENT DETECTION")
print("=" * 70)
print("Input Topic :", INPUT_TOPIC)
print("Output Topic: stock_events")
print("Window      : 1 minute tumbling")
print("Events      : Price Spike / Volume Surge")
print("Checkpoint  :", CHECKPOINT_PATH)
print("=" * 70)



query.awaitTermination()
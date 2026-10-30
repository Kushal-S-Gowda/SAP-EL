import os
import sys
from pathlib import Path
import csv
import gzip
import io
import json
import time
import zipfile
from datetime import datetime, timezone

from kafka import KafkaProducer

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import config
from producer.sink import store_pipeline_status, store_published_candles
from producer.status import write_status


# ============================================================
# CONFIGURATION
# ============================================================
PROJECT_ROOT = ROOT
ZIP_PATH = PROJECT_ROOT / "data" / "stocks_1m_csvs.zip"

KAFKA_SERVER = config.KAFKA_BOOTSTRAP_SERVERS
KAFKA_TOPIC = config.STOCK_DATA_TOPIC

# First test: only send 1,000 real records
MAX_RECORDS = int(os.getenv("RTSED_REPLAY_MAX_RECORDS", "1000"))

# Delay between records.
# 0 = fastest possible.
# 0.01 = approximately 100 records/second.
REPLAY_DELAY = float(os.getenv("RTSED_REPLAY_DELAY", "0.01"))


# ============================================================
# ============================================================
# HELPER FUNCTIONS
# ============================================================

def unix_to_utc(unix_timestamp):
    """
    Convert a Unix timestamp (seconds) to an ISO-8601 UTC timestamp.
    """
    dt = datetime.fromtimestamp(int(unix_timestamp), tz=timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def is_valid_ohlc(open_price, high_price, low_price, close_price):
    """
    Basic OHLC validation.
    """

    if open_price <= 0:
        return False

    if high_price <= 0:
        return False

    if low_price <= 0:
        return False

    if close_price <= 0:
        return False

    if high_price < low_price:
        return False

    if high_price < open_price or high_price < close_price:
        return False

    if low_price > open_price or low_price > close_price:
        return False

    return True


# ============================================================
# MAIN PRODUCER
# ============================================================

def main():
    if MAX_RECORDS < 0:
        raise ValueError("RTSED_REPLAY_MAX_RECORDS must be zero or a positive integer")

    records_sent = 0
    records_skipped = 0
    pending_candles = []
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8")
    )

    print("=" * 60)
    print("REAL-TIME STOCK EVENT DETECTION")
    print("Historical Replay Producer (NOT live market data)")
    print("=" * 60)

    print(f"Dataset : {ZIP_PATH}")
    print(f"Topic   : {KAFKA_TOPIC}")
    print(f"Limit   : {MAX_RECORDS} records")
    print(f"Delay   : {REPLAY_DELAY} seconds")
    print()

    with zipfile.ZipFile(ZIP_PATH, "r") as z:

        stock_files = [
            name for name in z.namelist()
            if name.endswith(".csv.gz")
        ]

        stock_files.sort()

        print(f"Stock files found: {len(stock_files)}")
        print()

        for file_index, file_name in enumerate(stock_files, start=1):

            # Extract symbol from filename.
            # Example:
            # RELIANCE_1m.csv.gz -> RELIANCE

            symbol = file_name.split("/")[-1]
            symbol = symbol.replace("_1m.csv.gz", "")

            print(
                f"Processing {file_index}/{len(stock_files)}: {symbol}"
            )

            with z.open(file_name) as compressed_file:

                with gzip.GzipFile(
                    fileobj=compressed_file
                ) as gz_file:

                    text_file = io.TextIOWrapper(
                        gz_file,
                        encoding="utf-8"
                    )

                    reader = csv.DictReader(text_file)

                    for row in reader:

                        try:

                            open_price = float(row["open"])
                            high_price = float(row["high"])
                            low_price = float(row["low"])
                            close_price = float(row["close"])
                            volume = int(float(row["Volume"]))

                            # Remove invalid OHLC records.
                            if not is_valid_ohlc(
                                open_price,
                                high_price,
                                low_price,
                                close_price
                            ):
                                records_skipped += 1
                                continue

                            record = {
                                "symbol": symbol,
                                "timestamp": unix_to_utc(row["time"]),
                                "open": open_price,
                                "high": high_price,
                                "low": low_price,
                                "close": close_price,
                                "volume": volume,
                                "source_mode": "HISTORICAL",
                            }

                            # Symbol is used as Kafka key.
                            producer.send(
                                KAFKA_TOPIC,
                                key=symbol,
                                value=record
                            )
                            pending_candles.append(record)
                            records_sent += 1

                            if records_sent % 100 == 0:

                                producer.flush()
                                if pending_candles:
                                    store_published_candles(pending_candles)
                                    pending_candles = []

                                print(
                                    f"  Sent: {records_sent} records"
                                )

                            if REPLAY_DELAY > 0:
                                time.sleep(REPLAY_DELAY)

                            # Stop after test limit.
                            if MAX_RECORDS and records_sent >= MAX_RECORDS:
                                break

                        except (ValueError, TypeError, KeyError):

                            records_skipped += 1
                            continue

                    if MAX_RECORDS and records_sent >= MAX_RECORDS:
                        break

            if MAX_RECORDS and records_sent >= MAX_RECORDS:
                break

    producer.flush()
    if pending_candles:
        store_published_candles(pending_candles)
    producer.close()

    print()
    print("=" * 60)
    print("PRODUCER TEST COMPLETE")
    print("=" * 60)

    print(f"Records sent    : {records_sent}")
    print(f"Records skipped : {records_skipped}")
    status = {
        "operating_mode": "HISTORICAL",
        "provider": "nse_zip_replay",
        "watchlist_count": None,
        "last_successful_fetch": None,
        "latest_candle_timestamp": None,
        "last_published_count": records_sent,
        "last_error": None,
        "limitations": (
            "Historical ZIP replay is a secondary source for tests and "
            "demonstrations. It is not live market data."
        ),
    }
    write_status(config.STATUS_FILE, status)
    try:
        store_pipeline_status(status)
    except Exception:
        pass


if __name__ == "__main__":
    main()

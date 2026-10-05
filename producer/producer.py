import csv
import gzip
import io
import json
import time
import zipfile
from datetime import datetime, timezone, timedelta

from kafka import KafkaProducer


# ============================================================
# CONFIGURATION
# ============================================================

ZIP_PATH = r"C:\Users\kusha\OneDrive\Desktop\RTSED\data\stocks_1m_csvs.zip"

KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "stock_data"

# First test: only send 1,000 real records
MAX_RECORDS = 1000

# Delay between records.
# 0 = fastest possible.
# 0.01 = approximately 100 records/second.
REPLAY_DELAY = 0.01


# ============================================================
# KAFKA PRODUCER
# ============================================================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    key_serializer=lambda key: key.encode("utf-8"),
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def unix_to_ist(unix_timestamp):
    """
    Convert Unix timestamp (seconds) to IST.
    """
    ist = timezone(timedelta(hours=5, minutes=30))

    dt = datetime.fromtimestamp(
        int(unix_timestamp),
        tz=timezone.utc
    )


    return dt.astimezone().isoformat()


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

    records_sent = 0
    records_skipped = 0

    print("=" * 60)
    print("REAL-TIME STOCK EVENT DETECTION")
    print("Kafka Producer")
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
                                "timestamp": unix_to_ist(row["time"]),
                                "open": open_price,
                                "high": high_price,
                                "low": low_price,
                                "close": close_price,
                                "volume": volume
                            }

                            # Symbol is used as Kafka key.
                            producer.send(
                                KAFKA_TOPIC,
                                key=symbol,
                                value=record
                            )

                            records_sent += 1

                            if records_sent % 100 == 0:

                                producer.flush()

                                print(
                                    f"  Sent: {records_sent} records"
                                )

                            if REPLAY_DELAY > 0:
                                time.sleep(REPLAY_DELAY)

                            # Stop after test limit.
                            if records_sent >= MAX_RECORDS:
                                break

                        except (ValueError, TypeError, KeyError):

                            records_skipped += 1
                            continue

                    if records_sent >= MAX_RECORDS:
                        break

            if records_sent >= MAX_RECORDS:
                break

    producer.flush()
    producer.close()

    print()
    print("=" * 60)
    print("PRODUCER TEST COMPLETE")
    print("=" * 60)

    print(f"Records sent    : {records_sent}")
    print(f"Records skipped : {records_skipped}")


if __name__ == "__main__":
    main()
import zipfile
import gzip
import io
import csv
from datetime import datetime, timezone

ZIP_PATH = "data/stocks_1m_csvs.zip"

total_rows = 0
total_missing = {
    "open": 0,
    "high": 0,
    "low": 0,
    "close": 0,
    "Volume": 0
}

stock_stats = []

global_min_time = None
global_max_time = None

with zipfile.ZipFile(ZIP_PATH, "r") as z:

    files = [
        f for f in z.namelist()
        if f.endswith("_1m.csv.gz")
    ]

    print("======================================")
    print("      FULL DATASET PROFILE")
    print("======================================")

    print(f"\nNumber of stock files: {len(files)}")

    for index, filename in enumerate(files, start=1):

        symbol = filename.replace("_1m.csv.gz", "")

        rows = 0
        missing = {
    "open": 0,
    "high": 0,
    "low": 0,
    "close": 0,
    "Volume": 0
}

        min_time = None
        max_time = None

        with z.open(filename) as compressed_file:

            with gzip.GzipFile(fileobj=compressed_file) as gz:

                with io.TextIOWrapper(
                    gz,
                    encoding="utf-8",
                    errors="replace"
                ) as text_file:

                    reader = csv.DictReader(text_file)

                    for row in reader:

                        rows += 1
                        total_rows += 1

                        # Check missing values
                        for column in missing:

                            value = row.get(column, "").strip()

                            if value == "":
                                missing[column] += 1
                                total_missing[column] += 1

                        # Timestamp
                        try:
                            timestamp = int(row["time"])

                            if min_time is None or timestamp < min_time:
                                min_time = timestamp

                            if max_time is None or timestamp > max_time:
                                max_time = timestamp

                        except (ValueError, TypeError):
                            pass

        # Global time range
        if min_time is not None:

            if global_min_time is None or min_time < global_min_time:
                global_min_time = min_time

            if global_max_time is None or max_time > global_max_time:
                global_max_time = max_time

        stock_stats.append({
            "symbol": symbol,
            "rows": rows,
            "min_time": min_time,
            "max_time": max_time
        })

        if index % 20 == 0 or index == len(files):
            print(f"Processed {index}/{len(files)} stocks...")

print("\n======================================")
print("            RESULTS")
print("======================================")

print(f"\nTotal stocks: {len(files):,}")
print(f"Total records: {total_rows:,}")

if global_min_time:
    min_dt = datetime.fromtimestamp(
        global_min_time,
        tz=timezone.utc
    )

    max_dt = datetime.fromtimestamp(
        global_max_time,
        tz=timezone.utc
    )

    print(f"\nGlobal start (UTC): {min_dt}")
    print(f"Global end   (UTC): {max_dt}")

print("\nMissing values:")

for column, count in total_missing.items():

    percentage = (
        count / total_rows * 100
        if total_rows > 0
        else 0
    )

    print(
        f"{column:>8}: "
        f"{count:,} "
        f"({percentage:.4f}%)"
    )

print("\n======================================")
print("        RECORDS PER STOCK")
print("======================================")

stock_stats.sort(
    key=lambda x: x["rows"],
    reverse=True
)

for stock in stock_stats[:20]:

    print(
        f"{stock['symbol']:15}"
        f"{stock['rows']:>10,}"
    )

print("\n======================================")
print("              DONE")
print("======================================")
import zipfile
import gzip
import io
import os
import csv
from collections import Counter

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


# ============================================================
# CONFIGURATION
# ============================================================

ZIP_PATH = "data/stocks_1m_csvs.zip"
OUTPUT_DIR = "eda_results"

# Stocks used for detailed visualizations.
# Full dataset is still profiled across all 214 stocks.
SAMPLE_STOCKS = [
    "RELIANCE",
    "TCS",
    "INFY",
    "HDFCBANK",
    "ICICIBANK"
]

os.makedirs(OUTPUT_DIR, exist_ok=True)


# ============================================================
# STORAGE FOR FULL-DATASET STATISTICS
# ============================================================

total_rows = 0
total_missing = {
    "open": 0,
    "high": 0,
    "low": 0,
    "close": 0,
    "Volume": 0
}

invalid_ohlc = 0
negative_volume = 0

stock_row_counts = {}

global_min_time = None
global_max_time = None

# Sample of returns across the complete dataset.
# We don't need every return for the histogram.
return_sample = []

MAX_RETURN_SAMPLE = 500_000

# Detailed data for selected stocks only.
selected_data = {}


# ============================================================
# HELPER
# ============================================================

def update_return_sample(returns):
    """Keep a bounded random-ish sample of returns."""

    global return_sample

    values = returns.dropna().to_numpy()

    if len(values) == 0:
        return

    remaining = MAX_RETURN_SAMPLE - len(return_sample)

    if remaining > 0:
        return_sample.extend(values[:remaining])

    if len(return_sample) >= MAX_RETURN_SAMPLE:
        # Keep the size bounded.
        return_sample[:] = return_sample[:MAX_RETURN_SAMPLE]


# ============================================================
# PROCESS DATASET
# ============================================================

print("=" * 60)
print("REAL-TIME STOCK EVENT DETECTION")
print("EDA")
print("=" * 60)

with zipfile.ZipFile(ZIP_PATH, "r") as z:

    files = [
        f for f in z.namelist()
        if f.endswith("_1m.csv.gz")
    ]

    print(f"\nTotal stock files: {len(files)}")

    for index, filename in enumerate(files, start=1):

        symbol = filename.replace("_1m.csv.gz", "")

        rows = 0

        # Keep selected stocks for detailed EDA.
        selected_rows = []

        with z.open(filename) as compressed_file:

            with gzip.GzipFile(fileobj=compressed_file) as gz:

                with io.TextIOWrapper(
                    gz,
                    encoding="utf-8",
                    errors="replace"
                ) as text_file:

                    reader = csv.DictReader(text_file)

                    previous_close = None

                    for row in reader:

                        rows += 1
                        total_rows += 1

                        # ------------------------------
                        # Missing values
                        # ------------------------------

                        for column in total_missing:

                            value = row.get(column, "").strip()

                            if value == "":
                                total_missing[column] += 1

                        # ------------------------------
                        # Numeric values
                        # ------------------------------

                        try:
                            open_price = float(row["open"])
                            high_price = float(row["high"])
                            low_price = float(row["low"])
                            close_price = float(row["close"])
                            volume = float(row["Volume"])
                            timestamp = int(row["time"])

                        except (ValueError, TypeError, KeyError):
                            continue

                        # ------------------------------
                        # OHLC validation
                        # ------------------------------

                        if (
                            high_price < max(open_price, close_price)
                            or low_price > min(open_price, close_price)
                            or high_price < low_price
                        ):
                            invalid_ohlc += 1

                        if volume < 0:
                            negative_volume += 1

                        # ------------------------------
                        # Global time range
                        # ------------------------------

                        if (
                            global_min_time is None
                            or timestamp < global_min_time
                        ):
                            global_min_time = timestamp

                        if (
                            global_max_time is None
                            or timestamp > global_max_time
                        ):
                            global_max_time = timestamp

                        # ------------------------------
                        # Returns
                        # ------------------------------

                        if previous_close is not None and previous_close != 0:

                            ret = (
                                (close_price - previous_close)
                                / previous_close
                            )

                            if len(return_sample) < MAX_RETURN_SAMPLE:
                                return_sample.append(ret)

                        previous_close = close_price

                        # ------------------------------
                        # Selected stocks
                        # ------------------------------

                        if symbol in SAMPLE_STOCKS:

                            selected_rows.append({
                                "timestamp": timestamp,
                                "open": open_price,
                                "high": high_price,
                                "low": low_price,
                                "close": close_price,
                                "volume": volume
                            })

        stock_row_counts[symbol] = rows

        if symbol in SAMPLE_STOCKS:

            df = pd.DataFrame(selected_rows)

            if not df.empty:

                df["timestamp"] = pd.to_datetime(
                    df["timestamp"],
                    unit="s",
                    utc=True
                ).dt.tz_convert("Asia/Kolkata")

                df["return"] = df["close"].pct_change()

                selected_data[symbol] = df

        if index % 20 == 0 or index == len(files):

            print(
                f"Processed "
                f"{index}/{len(files)} stocks..."
            )


# ============================================================
# RESULTS
# ============================================================

print("\n" + "=" * 60)
print("DATASET RESULTS")
print("=" * 60)

print(f"\nStocks: {len(files):,}")
print(f"Total records: {total_rows:,}")

print("\nMissing values:")

for column, count in total_missing.items():

    percentage = (
        count / total_rows * 100
        if total_rows
        else 0
    )

    print(
        f"{column:>8}: "
        f"{count:,} "
        f"({percentage:.4f}%)"
    )

print(f"\nInvalid OHLC records: {invalid_ohlc:,}")
print(f"Negative volume records: {negative_volume:,}")

# ------------------------------------------------------------
# Date range
# ------------------------------------------------------------

if global_min_time and global_max_time:

    start = pd.to_datetime(
        global_min_time,
        unit="s",
        utc=True
    ).tz_convert("Asia/Kolkata")

    end = pd.to_datetime(
        global_max_time,
        unit="s",
        utc=True
    ).tz_convert("Asia/Kolkata")

    print(f"\nStart time: {start}")
    print(f"End time:   {end}")


# ============================================================
# SAVE SUMMARY
# ============================================================

summary_path = os.path.join(
    OUTPUT_DIR,
    "summary.txt"
)

with open(summary_path, "w", encoding="utf-8") as f:

    f.write("REAL-TIME STOCK MARKET EVENT DETECTION\n")
    f.write("DATASET EDA SUMMARY\n")
    f.write("=" * 50 + "\n\n")

    f.write(f"Number of stocks: {len(files):,}\n")
    f.write(f"Total records: {total_rows:,}\n")

    if global_min_time and global_max_time:

        f.write(f"Start time: {start}\n")
        f.write(f"End time: {end}\n")

    f.write("\nMissing values:\n")

    for column, count in total_missing.items():

        percentage = (
            count / total_rows * 100
            if total_rows
            else 0
        )

        f.write(
            f"{column}: "
            f"{count:,} "
            f"({percentage:.4f}%)\n"
        )

    f.write(
        f"\nInvalid OHLC records: "
        f"{invalid_ohlc:,}\n"
    )

    f.write(
        f"Negative volume records: "
        f"{negative_volume:,}\n"
    )


# ============================================================
# PLOT 1 — RECORDS PER STOCK
# ============================================================

stock_series = pd.Series(stock_row_counts)

plt.figure(figsize=(12, 6))

stock_series.sort_values(ascending=False).plot(
    kind="bar"
)

plt.title("Number of 1-Minute Records per Stock")
plt.xlabel("Stock")
plt.ylabel("Number of Records")
plt.xticks(rotation=90)
plt.tight_layout()

plt.savefig(
    os.path.join(
        OUTPUT_DIR,
        "records_per_stock.png"
    ),
    dpi=150
)

plt.close()


# ============================================================
# PLOT 2 — RETURN DISTRIBUTION
# ============================================================

returns = np.array(return_sample)

returns = returns[
    np.isfinite(returns)
]

# Remove extreme values only for visualization.
# This prevents a few outliers from destroying the plot.
plot_returns = returns[
    (returns > -0.05)
    & (returns < 0.05)
]

plt.figure(figsize=(10, 6))

plt.hist(
    plot_returns,
    bins=100
)

plt.title("Distribution of 1-Minute Returns")
plt.xlabel("Return")
plt.ylabel("Frequency")
plt.tight_layout()

plt.savefig(
    os.path.join(
        OUTPUT_DIR,
        "returns_distribution.png"
    ),
    dpi=150
)

plt.close()


# ============================================================
# PLOT 3 — PRICE SERIES
# ============================================================

for symbol, df in selected_data.items():

    plt.figure(figsize=(12, 6))

    plt.plot(
        df["timestamp"],
        df["close"]
    )

    plt.title(
        f"{symbol} — 1-Minute Closing Price"
    )

    plt.xlabel("Time")
    plt.ylabel("Close Price")

    plt.tight_layout()

    filename = (
        f"{symbol.lower()}_price.png"
    )

    plt.savefig(
        os.path.join(
            OUTPUT_DIR,
            filename
        ),
        dpi=150
    )

    plt.close()


# ============================================================
# PLOT 4 — VOLUME
# ============================================================

for symbol, df in selected_data.items():

    plt.figure(figsize=(12, 6))

    plt.plot(
        df["timestamp"],
        df["volume"]
    )

    plt.title(
        f"{symbol} — 1-Minute Volume"
    )

    plt.xlabel("Time")
    plt.ylabel("Volume")

    plt.tight_layout()

    filename = (
        f"{symbol.lower()}_volume.png"
    )

    plt.savefig(
        os.path.join(
            OUTPUT_DIR,
            filename
        ),
        dpi=150
    )

    plt.close()


# ============================================================
# PLOT 5 — PRICE VS VOLUME FOR ONE STOCK
# ============================================================

if "RELIANCE" in selected_data:

    df = selected_data["RELIANCE"]

    fig, ax1 = plt.subplots(
        figsize=(12, 6)
    )

    ax1.plot(
        df["timestamp"],
        df["close"],
        label="Close Price"
    )

    ax1.set_xlabel("Time")
    ax1.set_ylabel("Close Price")

    ax2 = ax1.twinx()

    ax2.plot(
        df["timestamp"],
        df["volume"],
        alpha=0.5,
        label="Volume"
    )

    ax2.set_ylabel("Volume")

    plt.title(
        "RELIANCE — Price and Volume"
    )

    fig.tight_layout()

    plt.savefig(
        os.path.join(
            OUTPUT_DIR,
            "reliance_price_volume.png"
        ),
        dpi=150
    )

    plt.close()


# ============================================================
# DONE
# ============================================================

print("\n" + "=" * 60)
print("EDA COMPLETE")
print("=" * 60)

print(f"\nResults saved to: {OUTPUT_DIR}/")

print("\nGenerated files:")

for filename in sorted(
    os.listdir(OUTPUT_DIR)
):

    print(" -", filename)
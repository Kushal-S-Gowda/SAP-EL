import zipfile
import gzip
import io
import csv

import numpy as np
import pandas as pd


ZIP_PATH = "data/stocks_1m_csvs.zip"

# Keep a representative sample of returns and volume ratios.
MAX_SAMPLE = 1_000_000

returns_sample = []
volume_sample = []

stocks_processed = 0
total_rows = 0


with zipfile.ZipFile(ZIP_PATH, "r") as z:

    files = [
        f for f in z.namelist()
        if f.endswith("_1m.csv.gz")
    ]

    print(f"Processing {len(files)} stocks...")

    for filename in files:

        previous_close = None

        # For a simple baseline we use the previous
        # observed volume for this stock.
        previous_volume = None

        with z.open(filename) as compressed_file:

            with gzip.GzipFile(
                fileobj=compressed_file
            ) as gz:

                with io.TextIOWrapper(
                    gz,
                    encoding="utf-8",
                    errors="replace"
                ) as text_file:

                    reader = csv.DictReader(text_file)

                    for row in reader:

                        try:
                            close = float(row["close"])
                            volume = float(row["Volume"])

                        except (ValueError, TypeError, KeyError):
                            continue

                        total_rows += 1

                        # ----------------------------------
                        # 1-minute return
                        # ----------------------------------

                        if (
                            previous_close is not None
                            and previous_close != 0
                        ):

                            ret = (
                                (close - previous_close)
                                / previous_close
                            )

                            if len(returns_sample) < MAX_SAMPLE:
                                returns_sample.append(ret)

                        previous_close = close

                        # ----------------------------------
                        # Volume change
                        # ----------------------------------

                        if (
                            previous_volume is not None
                            and previous_volume > 0
                        ):

                            ratio = (
                                volume / previous_volume
                            )

                            # Avoid absurd ratios from
                            # very tiny previous volumes.
                            if (
                                0.01 < ratio < 100
                                and len(volume_sample)
                                < MAX_SAMPLE
                            ):
                                volume_sample.append(ratio)

                        previous_volume = volume

        stocks_processed += 1

        if stocks_processed % 20 == 0:
            print(
                f"Processed "
                f"{stocks_processed}/{len(files)}..."
            )


# ============================================================
# RETURNS
# ============================================================

returns = np.array(returns_sample)

returns = returns[
    np.isfinite(returns)
]

print("\n" + "=" * 60)
print("1-MINUTE RETURN STATISTICS")
print("=" * 60)

percentiles = [
    0.1,
    1,
    5,
    25,
    50,
    75,
    95,
    99,
    99.9
]

for p in percentiles:

    value = np.percentile(
        returns,
        p
    )

    print(
        f"{p:>5}% percentile: "
        f"{value * 100:.4f}%"
    )

print(
    f"\nMean: "
    f"{np.mean(returns) * 100:.6f}%"
)

print(
    f"Std Dev: "
    f"{np.std(returns) * 100:.6f}%"
)

print(
    f"Minimum: "
    f"{np.min(returns) * 100:.4f}%"
)

print(
    f"Maximum: "
    f"{np.max(returns) * 100:.4f}%"
)


# ============================================================
# EXTREME RETURNS
# ============================================================

print("\n" + "=" * 60)
print("EXTREME PRICE MOVEMENTS")
print("=" * 60)

for threshold in [0.5, 1.0, 2.0, 3.0, 5.0]:

    count = np.sum(
        np.abs(returns) > threshold / 100
    )

    percentage = (
        count / len(returns) * 100
    )

    print(
        f"|Return| > {threshold:>4.1f}% : "
        f"{count:,} "
        f"({percentage:.4f}%)"
    )


# ============================================================
# VOLUME RATIOS
# ============================================================

volume_ratios = np.array(volume_sample)

volume_ratios = volume_ratios[
    np.isfinite(volume_ratios)
]

print("\n" + "=" * 60)
print("VOLUME CHANGE STATISTICS")
print("=" * 60)

for p in [1, 5, 25, 50, 75, 95, 99, 99.9]:

    value = np.percentile(
        volume_ratios,
        p
    )

    print(
        f"{p:>5}% percentile: "
        f"{value:.2f}x"
    )


print("\n" + "=" * 60)
print("VOLUME SURGE COUNTS")
print("=" * 60)

for threshold in [1.5, 2, 3, 5, 10]:

    count = np.sum(
        volume_ratios > threshold
    )

    percentage = (
        count / len(volume_ratios) * 100
    )

    print(
        f"Volume > {threshold:.1f}x previous: "
        f"{count:,} "
        f"({percentage:.4f}%)"
    )


print("\n" + "=" * 60)
print("DONE")
print("=" * 60)
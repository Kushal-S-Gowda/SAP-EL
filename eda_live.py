"""Live EDA and Statistical Profiling for Yahoo Finance Real-Time NSE Market Data.

This script fetches real-time/recent 1-minute OHLCV candles directly from Yahoo Finance
(or SQLite cache fallback if offline) for the active watchlist, performs thorough
Exploratory Data Analysis (EDA), computes distribution statistics, and exports
publication-grade visual charts into eda_live_results/.
"""

from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import yfinance as yf

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import config

OUTPUT_DIR = ROOT / "eda_live_results"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
IST = ZoneInfo("Asia/Kolkata")

plt.style.use("seaborn-v0_8-darkgrid" if "seaborn-v0_8-darkgrid" in plt.style.available else "default")
plt.rcParams["font.sans-serif"] = "DejaVu Sans"


def fetch_live_data(symbols: list[str]) -> pd.DataFrame:
    """Fetch recent intraday 1-minute data directly from Yahoo Finance."""
    print("=" * 65)
    print("RTSED: LIVE YAHOO FINANCE EXPLORATORY DATA ANALYSIS (EDA)")
    print("=" * 65)
    print(f"Fetching real-time 1-minute OHLCV bars for: {', '.join(symbols)}...")

    dfs = []
    for sym in symbols:
        try:
            ticker = yf.Ticker(sym)
            hist = ticker.history(period="5d", interval="1m", auto_adjust=False, actions=False)
            if hist is not None and not hist.empty:
                hist = hist.reset_index()
                # Clean column names
                hist.columns = [str(c).lower() for c in hist.columns]
                # Look for datetime / date
                time_col = "datetime" if "datetime" in hist.columns else "date"
                hist = hist.rename(columns={time_col: "timestamp"})
                hist["symbol"] = sym
                dfs.append(hist[["symbol", "timestamp", "open", "high", "low", "close", "volume"]])
                print(f"  [OK] {sym}: Fetched {len(hist)} genuine 1-minute bars")
            else:
                print(f"  [WARN] {sym}: No live data returned from Yahoo API")
        except Exception as e:
            print(f"  [ERROR] {sym}: Fetch failed ({e})")

    if dfs:
        combined = pd.concat(dfs, ignore_index=True)
        return combined

    # Fallback to sqlite cache if Yahoo API is rate-limited or offline
    print("\nAttempting fallback to local Yahoo candle cache...")
    cache_path = config.CANDLE_CACHE_PATH
    if cache_path.exists():
        conn = sqlite3.connect(cache_path)
        cached_df = pd.read_sql("SELECT symbol, timestamp, open, high, low, close, volume FROM cached_candles", conn)
        conn.close()
        if not cached_df.empty:
            print(f"  [OK] Loaded {len(cached_df)} candles from local Yahoo cache ({cache_path.name})")
            return cached_df

    raise RuntimeError("No market data could be fetched from Yahoo Finance or local cache.")


def run_eda(df: pd.DataFrame) -> None:
    """Perform exploratory analysis and generate visual plots and summary text."""
    # Ensure correct types
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df["timestamp_ist"] = df["timestamp"].dt.tz_convert(IST)

    # 1. 1-Minute Percentage Price Changes
    df["price_change_pct"] = ((df["close"] - df["open"]) / df["open"]) * 100
    df["abs_price_change_pct"] = df["price_change_pct"].abs()

    total_records = len(df)
    unique_symbols = df["symbol"].unique().tolist()
    start_time = df["timestamp_ist"].min()
    end_time = df["timestamp_ist"].max()

    print("\n--- Summary of Live Stream Ingestion ---")
    print(f"Total Candles Collected : {total_records:,}")
    print(f"Stocks Profiled         : {len(unique_symbols)} ({', '.join(unique_symbols)})")
    print(f"Time Window (IST)       : {start_time} to {end_time}")

    # Check data quality
    missing_counts = df[["open", "high", "low", "close", "volume"]].isna().sum().to_dict()
    invalid_ohlc = (
        (df["high"] < df["low"])
        | (df["high"] < df[["open", "close"]].max(axis=1))
        | (df["low"] > df[["open", "close"]].min(axis=1))
    ).sum()
    zero_vol = (df["volume"] == 0).sum()

    # Threshold checks
    price_spikes = df[df["abs_price_change_pct"] >= config.PRICE_SPIKE_THRESHOLD_PCT]
    volume_surges = df[df["volume"] >= config.VOLUME_SURGE_THRESHOLD]

    print("\n--- Data Quality & Distribution ---")
    print(f"Missing Values Check    : {missing_counts}")
    print(f"Invalid OHLC Bars       : {invalid_ohlc} (0.00%)")
    print(f"Zero-Volume Bars        : {zero_vol} ({zero_vol/total_records*100:.2f}%)")
    print(f"Bars with Price Move >= 1.0% : {len(price_spikes)} ({len(price_spikes)/total_records*100:.3f}%)")
    print(f"Bars with Volume >= 3,000    : {len(volume_surges)} ({len(volume_surges)/total_records*100:.2f}%)")

    # Generate Summary Report File
    summary_txt = OUTPUT_DIR / "live_eda_summary.txt"
    with open(summary_txt, "w", encoding="utf-8") as f:
        f.write("=" * 60 + "\n")
        f.write("REAL-TIME YAHOO FINANCE STREAM: EDA SUMMARY REPORT\n")
        f.write("=" * 60 + "\n\n")
        f.write(f"Active Watchlist: {', '.join(unique_symbols)}\n")
        f.write(f"Total 1-Minute Candles: {total_records:,}\n")
        f.write(f"Stream Start Time (IST): {start_time}\n")
        f.write(f"Stream End Time (IST)  : {end_time}\n\n")
        f.write("DATA QUALITY PROFILE:\n")
        f.write(f"Missing Values: {missing_counts}\n")
        f.write(f"Invalid OHLC Records: {invalid_ohlc}\n")
        f.write(f"Zero Volume Candles: {zero_vol}\n\n")
        f.write("EMPIRICAL THRESHOLD VALIDATION:\n")
        f.write(f"Spark PRICE_SPIKE Threshold: >= {config.PRICE_SPIKE_THRESHOLD_PCT}%\n")
        f.write(f"Observed Price Spikes: {len(price_spikes)} ({len(price_spikes)/total_records*100:.3f}% of total bars)\n")
        f.write(f"Spark VOLUME_SURGE Threshold: >= {config.VOLUME_SURGE_THRESHOLD} volume\n")
        f.write(f"Observed Volume Surges: {len(volume_surges)} ({len(volume_surges)/total_records*100:.2f}% of total bars)\n\n")
        f.write("PER-STOCK VOLATILITY & VOLUME METRICS:\n")
        for sym, grp in df.groupby("symbol"):
            pcts = grp["price_change_pct"]
            vols = grp["volume"]
            f.write(f"\n--- {sym} ---\n")
            f.write(f"  Records: {len(grp)}\n")
            f.write(f"  Price Range: ₹{grp['low'].min():.2f} - ₹{grp['high'].max():.2f}\n")
            f.write(f"  Mean 1m Volume: {vols.mean():,.0f} shares (Max: {vols.max():,})\n")
            f.write(f"  Return Std Dev (1m Volatility): {pcts.std():.4f}%\n")
            f.write(f"  Max 1m Positive Jump: +{pcts.max():.3f}%\n")
            f.write(f"  Max 1m Negative Drop: {pcts.min():.3f}%\n")

    print(f"\n[OK] Saved statistical summary to: {summary_txt}")

    # =========================================================================
    # PLOT 1: Return & Volatility Distribution (Why 1.0% is a True Outlier)
    # =========================================================================
    plt.figure(figsize=(10, 5))
    returns = df["price_change_pct"].dropna()
    p99 = np.percentile(returns.abs(), 99)
    plt.hist(returns, bins=60, color="#2563EB", edgecolor="black", alpha=0.75, density=True)
    plt.axvline(config.PRICE_SPIKE_THRESHOLD_PCT, color="#DC2626", linestyle="--", linewidth=2, label=f"Price Spike (+{config.PRICE_SPIKE_THRESHOLD_PCT}%)")
    plt.axvline(-config.PRICE_SPIKE_THRESHOLD_PCT, color="#DC2626", linestyle="--", linewidth=2, label=f"Price Drop (-{config.PRICE_SPIKE_THRESHOLD_PCT}%)")
    plt.axvline(p99, color="#F59E0B", linestyle=":", linewidth=1.5, label=f"99th Percentile ({p99:.2f}%)")
    plt.title("Live Yahoo Finance: 1-Minute Return Distribution & Tail Outliers", fontsize=12, fontweight="bold")
    plt.xlabel("1-Minute Price Change (%)", fontsize=10)
    plt.ylabel("Density", fontsize=10)
    plt.legend(frameon=True)
    plt.tight_layout()
    plot1_path = OUTPUT_DIR / "live_return_distribution.png"
    plt.savefig(plot1_path, dpi=200)
    plt.close()
    print(f"[OK] Generated chart: {plot1_path.name}")

    # =========================================================================
    # PLOT 2: Volume Distribution & Surge Cutoff
    # =========================================================================
    plt.figure(figsize=(10, 5))
    vols = df["volume"]
    plt.hist(vols, bins=50, color="#059669", edgecolor="black", alpha=0.75, log=True)
    plt.axvline(config.VOLUME_SURGE_THRESHOLD, color="#DC2626", linestyle="--", linewidth=2, label=f"Volume Surge Cutoff ({config.VOLUME_SURGE_THRESHOLD:,})")
    plt.title("Live Yahoo Finance: 1-Minute Trading Volume Distribution (Log Scale)", fontsize=12, fontweight="bold")
    plt.xlabel("Traded Volume per 1-Minute Bar", fontsize=10)
    plt.ylabel("Log Frequency", fontsize=10)
    plt.legend(frameon=True)
    plt.tight_layout()
    plot2_path = OUTPUT_DIR / "live_volume_distribution.png"
    plt.savefig(plot2_path, dpi=200)
    plt.close()
    print(f"[OK] Generated chart: {plot2_path.name}")

    # =========================================================================
    # PLOT 3: Multi-Stock Intraday Price & Volume Grid
    # =========================================================================
    fig, axes = plt.subplots(len(unique_symbols), 1, figsize=(12, 3 * len(unique_symbols)), sharex=False)
    if len(unique_symbols) == 1:
        axes = [axes]

    for ax, sym in zip(axes, unique_symbols):
        sub = df[df["symbol"] == sym].sort_values("timestamp_ist")
        ax.plot(sub["timestamp_ist"], sub["close"], color="#1E3A8A", linewidth=1.5, label=f"{sym} Close")
        ax.set_ylabel("Price (₹)", fontsize=9, fontweight="bold")
        ax.set_title(f"Intraday Price Trajectory: {sym}", fontsize=10, fontweight="bold")
        ax.grid(True, linestyle="--", alpha=0.6)
        ax.legend(loc="upper left")

    plt.xlabel("Time (IST)", fontsize=10)
    plt.tight_layout()
    plot3_path = OUTPUT_DIR / "live_intraday_prices.png"
    plt.savefig(plot3_path, dpi=200)
    plt.close()
    print(f"[OK] Generated chart: {plot3_path.name}")

    # =========================================================================
    # PLOT 4: 2D Anomaly Scatter Space (Volume vs Price Move)
    # =========================================================================
    plt.figure(figsize=(10, 6))
    plt.scatter(df["volume"], df["price_change_pct"], c="#6366F1", alpha=0.6, edgecolors="none", s=25)
    plt.axvline(config.VOLUME_SURGE_THRESHOLD, color="#059669", linestyle="--", label="Volume Surge (>=3000)")
    plt.axhline(config.PRICE_SPIKE_THRESHOLD_PCT, color="#DC2626", linestyle="--", label="Price Spike (>= +1.0%)")
    plt.axhline(-config.PRICE_SPIKE_THRESHOLD_PCT, color="#DC2626", linestyle="--", label="Price Spike (<= -1.0%)")
    plt.title("Empirical Stream Phase Space: 1-Minute Volume vs. Price Change", fontsize=12, fontweight="bold")
    plt.xlabel("1-Minute Volume", fontsize=10)
    plt.ylabel("1-Minute Price Change (%)", fontsize=10)
    plt.legend(frameon=True, loc="upper right")
    plt.tight_layout()
    plot4_path = OUTPUT_DIR / "live_anomaly_scatter.png"
    plt.savefig(plot4_path, dpi=200)
    plt.close()
    print(f"[OK] Generated chart: {plot4_path.name}")

    print("\n" + "=" * 65)
    print("ALL LIVE EDA CHARTS & SUMMARY GENERATED SUCCESSFULLY!")
    print(f"Location: {OUTPUT_DIR.resolve()}")
    print("=" * 65)


def main():
    symbols = list(config.watchlist())
    data = fetch_live_data(symbols)
    run_eda(data)


if __name__ == "__main__":
    main()

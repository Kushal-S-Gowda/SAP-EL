"""Reproducible severity scoring. The LLM must not override these numbers."""

from __future__ import annotations

from config import PRICE_SPIKE_THRESHOLD_PCT, VOLUME_SURGE_THRESHOLD

SEVERITY_ORDER = ("LOW", "MEDIUM", "HIGH", "CRITICAL")

# Price-change bands are relative to the Spark PRICE_SPIKE threshold of 1.0%.
PRICE_BANDS = (
    (5.0, "CRITICAL"),
    (3.0, "HIGH"),
    (1.5, "MEDIUM"),
    (PRICE_SPIKE_THRESHOLD_PCT, "LOW"),
)

# Volume-ratio bands compare current window volume to a rolling baseline.
VOLUME_RATIO_BANDS = (
    (6.0, "CRITICAL"),
    (3.0, "HIGH"),
    (1.5, "MEDIUM"),
    (1.0, "LOW"),
)


def _from_bands(value: float | None, bands) -> str | None:
    if value is None:
        return None
    magnitude = abs(float(value))
    for threshold, label in bands:
        if magnitude >= threshold:
            return label
    return None


def assess_event_severity(
    event_type: str,
    price_change_percent: float | None,
    volume: float | None,
    volume_ratio: float | None,
) -> dict:
    """Return a documented severity label from numerical evidence only."""
    reasons = []
    candidates = []

    price_level = _from_bands(price_change_percent, PRICE_BANDS)
    if event_type == "PRICE_SPIKE" and price_level:
        candidates.append(price_level)
        reasons.append(
            f"abs(price_change_percent)={abs(price_change_percent):.4f} "
            f"maps to {price_level} using bands "
            f">=5 CRITICAL, >=3 HIGH, >=1.5 MEDIUM, >=1.0 LOW"
        )
    elif price_level:
        candidates.append(price_level)
        reasons.append(
            f"price_change_percent={price_change_percent:.4f} contributes {price_level}"
        )

    volume_level = _from_bands(volume_ratio, VOLUME_RATIO_BANDS)
    if volume_level:
        candidates.append(volume_level)
        reasons.append(
            f"volume_ratio={volume_ratio:.4f} maps to {volume_level} using bands "
            f">=6 CRITICAL, >=3 HIGH, >=1.5 MEDIUM, >=1.0 LOW"
        )
    elif event_type == "VOLUME_SURGE" and volume is not None and volume >= VOLUME_SURGE_THRESHOLD:
        candidates.append("LOW")
        reasons.append(
            f"total_volume={volume} meets the Spark threshold "
            f"{VOLUME_SURGE_THRESHOLD} but no rolling baseline was available"
        )

    if not candidates:
        severity = "LOW"
        reasons.append("No stronger numerical band matched; defaulting to LOW")
    else:
        severity = max(candidates, key=SEVERITY_ORDER.index)

    return {
        "severity": severity,
        "criteria": reasons,
        "price_change_percent": price_change_percent,
        "volume": volume,
        "volume_ratio": volume_ratio,
        "spark_price_spike_threshold_pct": PRICE_SPIKE_THRESHOLD_PCT,
        "spark_volume_surge_threshold": VOLUME_SURGE_THRESHOLD,
    }

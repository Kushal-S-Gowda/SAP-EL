"""Replay cached genuine Yahoo Finance candles for after-hours demonstrations."""

from __future__ import annotations

import logging
import sys
import time
from pathlib import Path

from kafka import KafkaProducer

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import config
from producer.candle_cache import CandleCache, fill_cache_from_yahoo
from producer.ohlcv import serialize_record
from producer.publisher import KAFKA_ACK_TIMEOUT_SECONDS, publish_records
from producer.sink import store_pipeline_status, store_published_candles
from producer.state_store import CandleState
from producer.status import write_status

logger = logging.getLogger("rtsed.demo_replay")


def main():
    logging.basicConfig(
        level=config.LOG_LEVEL,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    delay = float(config.env("RTSED_DEMO_REPLAY_DELAY", "0.2"))
    cache = CandleCache(config.CANDLE_CACHE_PATH)
    records = cache.load_all()
    if not records:
        logger.warning(
            "Cache is empty. Fetching genuine Yahoo Finance 1-minute candles "
            "to build a DEMO REPLAY cache (not live publishing)."
        )
        fill_cache_from_yahoo(cache)
        records = cache.load_all()
    cache.close()

    if not records:
        print(
            "Yahoo Finance returned no 1-minute candles to cache.\n"
            "Try again during or shortly after an NSE session.\n"
            f"Cache path: {config.CANDLE_CACHE_PATH}\n"
            "Do not fabricate a fallback dataset. Historical ZIP replay is "
            "available with: python producer/historical_replay.py"
        )
        raise SystemExit(1)

    max_records = int(config.env("RTSED_DEMO_MAX_RECORDS", "200"))
    if max_records > 0 and len(records) > max_records:
        records = records[-max_records:]

    logger.warning(
        "DEMO REPLAY mode: publishing %d cached Yahoo Finance candles. "
        "These records are not live market data.",
        len(records),
    )

    state = CandleState(config.STATE_DIRECTORY / "published_demo.sqlite3")
    kafka_producer = KafkaProducer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=serialize_record,
    )

    published_total = []
    try:
        batch = []
        for record in records:
            record = dict(record)
            record["source_mode"] = "DEMO_REPLAY"
            batch.append(record)
            if delay > 0:
                published = publish_records(
                    [record], kafka_producer, state, config.STOCK_DATA_TOPIC
                )
                store_published_candles(published)
                published_total.extend(published)
                time.sleep(delay)
            if delay <= 0 and len(batch) >= 50:
                published = publish_records(
                    batch, kafka_producer, state, config.STOCK_DATA_TOPIC
                )
                store_published_candles(published)
                published_total.extend(published)
                batch = []

        if delay <= 0 and batch:
            published = publish_records(
                batch, kafka_producer, state, config.STOCK_DATA_TOPIC
            )
            store_published_candles(published)
            published_total.extend(published)

        latest = published_total[-1]["timestamp"] if published_total else None
        status = {
            "operating_mode": "DEMO_REPLAY",
            "provider": "yahoo_cache",
            "watchlist_count": len({row["symbol"] for row in records}),
            "last_successful_fetch": None,
            "latest_candle_timestamp": latest,
            "last_published_count": len(published_total),
            "cached_candles": len(records),
            "last_error": None,
            "limitations": (
                "Dashboard data is a replay of previously fetched Yahoo Finance "
                "candles. It is not live."
            ),
        }
        write_status(config.STATUS_FILE, status)
        store_pipeline_status(status)
        logger.info("Demo replay published %d cached candle(s)", len(published_total))
    except KeyboardInterrupt:
        logger.info("Stopped by user")
    finally:
        kafka_producer.flush(timeout=KAFKA_ACK_TIMEOUT_SECONDS)
        kafka_producer.close()
        state.close()


if __name__ == "__main__":
    main()

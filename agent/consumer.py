"""Kafka consumer for stock_events plus a retry worker for investigations.

This process never participates in Spark checkpointing. If Ollama is down,
events remain stored and investigations stay PENDING or FAILED.
"""

from __future__ import annotations

import json
import logging
import sys
import time
from pathlib import Path

from kafka import KafkaConsumer

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import config
from agent.llm import LLMUnavailable
from agent.orchestrator import InvestigationAgent
from agent.store import (
    claim_pending,
    enqueue_investigation,
    load_event_metrics,
    mark_completed,
    mark_failed,
)
from database.db import apply_schema

logger = logging.getLogger("rtsed.agent.consumer")


def process_claimed(agent: InvestigationAgent, claimed: dict) -> None:
    investigation_id = claimed["investigation_id"]
    try:
        event = load_event_metrics(claimed.get("event_id"), claimed)
        result = agent.investigate(event)
        mark_completed(investigation_id, result)
        logger.info(
            "Completed investigation %s for %s %s",
            investigation_id,
            event.get("symbol"),
            event.get("event_type"),
        )
    except LLMUnavailable as error:
        logger.warning("Investigation %s waiting on LLM: %s", investigation_id, error)
        mark_failed(investigation_id, str(error))
    except Exception as error:
        logger.exception("Investigation %s failed", investigation_id)
        mark_failed(investigation_id, str(error))


def main() -> None:
    logging.basicConfig(
        level=config.LOG_LEVEL,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    apply_schema()
    agent = InvestigationAgent()
    logger.info(
        "Stock Event Investigation Agent consuming %s as group %s (LLM=%s %s)",
        config.STOCK_EVENTS_TOPIC,
        config.AGENT_CONSUMER_GROUP,
        config.AI_PROVIDER,
        config.OLLAMA_MODEL,
    )
    if not agent.llm.is_available():
        logger.warning(
            "Ollama is not reachable at %s. Events will still be queued; "
            "investigations remain PENDING/FAILED until the model is available.",
            config.OLLAMA_BASE_URL,
        )

    consumer = KafkaConsumer(
        config.STOCK_EVENTS_TOPIC,
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        group_id=config.AGENT_CONSUMER_GROUP,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
        key_deserializer=lambda raw: raw.decode("utf-8") if raw else None,
        consumer_timeout_ms=2000,
    )

    try:
        while True:
            try:
                for message in consumer:
                    event = message.value
                    if not isinstance(event, dict):
                        logger.warning("Skipping non-JSON event payload")
                        consumer.commit()
                        continue
                    try:
                        enqueue_investigation(event)
                        consumer.commit()
                    except Exception:
                        logger.exception(
                            "Could not enqueue investigation; Kafka offset was not committed"
                        )
            except Exception:
                logger.exception("Kafka poll failed; retrying")

            try:
                for claimed in claim_pending():
                    process_claimed(agent, claimed)
            except Exception:
                logger.exception("Investigation worker loop failed")
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Stopped by user")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()

from pathlib import Path
from unittest.mock import MagicMock, patch

from agent.store import claim_pending, enqueue_investigation, mark_completed, mark_failed


SCHEMA = Path("database/schema.sql").read_text(encoding="utf-8")


def test_schema_preserves_stock_events_and_adds_investigations():
    assert "CREATE TABLE IF NOT EXISTS stock_events" in SCHEMA
    assert "CREATE TABLE IF NOT EXISTS ai_investigations" in SCHEMA
    assert "CREATE TABLE IF NOT EXISTS stock_candles" in SCHEMA
    assert "UNIQUE (symbol, event_start, event_end, event_type)" in SCHEMA
    assert "investigation_status" in SCHEMA


def test_enqueue_investigation_is_idempotent_sql():
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    mock_cur.fetchone.side_effect = [(11,), (22, "PENDING", 11)]

    with patch("agent.store.connect", return_value=mock_conn):
        result = enqueue_investigation(
            {
                "symbol": "TCS.NS",
                "event_type": "PRICE_SPIKE",
                "event_start": "2026-10-30T03:45:00",
                "event_end": "2026-10-30T03:46:00",
            }
        )

    insert_sql = mock_cur.execute.call_args_list[-1][0][0]
    assert "ON CONFLICT (symbol, event_start, event_end, event_type)" in insert_sql
    assert result["investigation_id"] == 22
    mock_conn.commit.assert_called()
    mock_conn.close.assert_called()


def test_claim_pending_prioritizes_newest_pending_investigations():
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    with patch("agent.store.connect", return_value=mock_conn):
        claim_pending()

    query = mock_cur.execute.call_args[0][0]
    assert "WHEN investigation_status = 'PENDING' THEN 0" in query
    assert "investigation_id DESC" in query


def test_mark_failed_does_not_mark_completed():
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    with patch("agent.store.connect", return_value=mock_conn):
        mark_failed(9, "Ollama is unavailable")

    sql, params = mock_cur.execute.call_args[0]
    assert "FAILED" in sql
    assert "COMPLETED" not in sql
    assert params[0].startswith("Ollama")
    assert params[1] == 9


def test_mark_completed_writes_evidence():
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    with patch("agent.store.connect", return_value=mock_conn), patch(
        "agent.store.as_json", side_effect=lambda value: value
    ):
        mark_completed(
            3,
            {
                "severity": "LOW",
                "investigation_summary": "Insufficient evidence for a cause.",
                "evidence": {"tool_trace": []},
                "price_change_percent": 1.2,
                "volume_ratio": None,
                "previous_event_count": 1,
                "confidence": "LOW",
            },
        )

    sql = mock_cur.execute.call_args[0][0]
    assert "COMPLETED" in sql
    mock_conn.commit.assert_called()

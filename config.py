"""Shared environment loading and configuration for RTSED."""

from __future__ import annotations

import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent
NSE_TIMEZONE_NAME = "Asia/Kolkata"


def load_dotenv(path: Path | None = None) -> None:
    env_path = path or (PROJECT_ROOT / ".env")
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value


def env_int(name: str, default: int) -> int:
    return int(env(name, str(default)))


def env_float(name: str, default: float) -> float:
    return float(env(name, str(default)))


load_dotenv()

DEFAULT_WATCHLIST = (
    "RELIANCE.NS",
    "TCS.NS",
    "INFY.NS",
    "HDFCBANK.NS",
    "ICICIBANK.NS",
)


def watchlist() -> tuple[str, ...]:
    raw = env("RTSED_STOCKS", ",".join(DEFAULT_WATCHLIST))
    symbols = tuple(symbol.strip() for symbol in raw.split(",") if symbol.strip())
    if not symbols:
        raise ValueError("RTSED_STOCKS must contain at least one ticker symbol")
    return symbols


KAFKA_BOOTSTRAP_SERVERS = env("RTSED_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
STOCK_DATA_TOPIC = env("RTSED_STOCK_DATA_TOPIC", "stock_data")
STOCK_EVENTS_TOPIC = env("RTSED_STOCK_EVENTS_TOPIC", "stock_events")
OPERATING_MODE = env("RTSED_MODE", "live").lower()
POLL_INTERVAL_SECONDS = env_int("RTSED_POLL_INTERVAL_SECONDS", 60)
FINALIZATION_DELAY_SECONDS = env_int("RTSED_FINALIZATION_DELAY_SECONDS", 120)
OVERLAP_WINDOW_SECONDS = env_int("RTSED_OVERLAP_WINDOW_SECONDS", 600)
MARKET_DATA_PROVIDER = env("RTSED_MARKET_DATA_PROVIDER", "yahoo")
LOG_LEVEL = env("RTSED_LOG_LEVEL", "INFO").upper()

POSTGRES_HOST = env("RTSED_POSTGRES_HOST", "localhost")
POSTGRES_PORT = env_int("RTSED_POSTGRES_PORT", 5432)
POSTGRES_DB = env("RTSED_POSTGRES_DB", "rtsed")
POSTGRES_USER = env("RTSED_POSTGRES_USER", "rtsed_user")
POSTGRES_PASSWORD = env("RTSED_POSTGRES_PASSWORD", "rtsed_password")

AI_PROVIDER = env("AI_PROVIDER", "ollama")
OLLAMA_BASE_URL = env("OLLAMA_BASE_URL", "http://localhost:11434").rstrip("/")
OLLAMA_MODEL = env("OLLAMA_MODEL", "llama3.2")
AGENT_CONSUMER_GROUP = env("RTSED_AGENT_GROUP_ID", "rtsed-investigation-agent")
AGENT_MAX_TOOL_STEPS = env_int("RTSED_AGENT_MAX_TOOL_STEPS", 8)

STATE_DIRECTORY = Path(
    env("RTSED_PRODUCER_STATE_DIR", str(PROJECT_ROOT / "producer" / "state"))
)
STATUS_FILE = STATE_DIRECTORY / "ingestion_status.json"
CANDLE_CACHE_PATH = STATE_DIRECTORY / "yahoo_candle_cache.sqlite3"
PUBLISHED_STATE_PATH = STATE_DIRECTORY / "published_candles.sqlite3"

PRICE_SPIKE_THRESHOLD_PCT = 1.0
VOLUME_SURGE_THRESHOLD = 3000

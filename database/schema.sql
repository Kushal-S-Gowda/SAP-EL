-- Idempotent RTSED schema. Existing stock_events rows are preserved.

CREATE TABLE IF NOT EXISTS stock_events (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    event_start TIMESTAMP NOT NULL,
    event_end TIMESTAMP NOT NULL,
    avg_close DOUBLE PRECISION,
    total_volume BIGINT,
    avg_price_change_pct DOUBLE PRECISION,
    record_count INTEGER,
    event_type TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_stock_events_symbol_start
    ON stock_events (symbol, event_start DESC);

CREATE TABLE IF NOT EXISTS stock_candles (
    symbol TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume BIGINT NOT NULL,
    source_mode TEXT NOT NULL DEFAULT 'UNKNOWN',
    ingested_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, ts)
);

CREATE INDEX IF NOT EXISTS idx_stock_candles_ts
    ON stock_candles (ts DESC);

CREATE TABLE IF NOT EXISTS ai_investigations (
    investigation_id SERIAL PRIMARY KEY,
    event_id INTEGER REFERENCES stock_events (id),
    symbol TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_start TIMESTAMP NOT NULL,
    event_end TIMESTAMP NOT NULL,
    severity TEXT,
    investigation_summary TEXT,
    evidence JSONB,
    price_change_percent DOUBLE PRECISION,
    volume_ratio DOUBLE PRECISION,
    previous_event_count INTEGER,
    confidence TEXT,
    investigation_status TEXT NOT NULL DEFAULT 'PENDING',
    error_message TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (symbol, event_start, event_end, event_type)
);

CREATE INDEX IF NOT EXISTS idx_ai_investigations_status
    ON ai_investigations (investigation_status);

CREATE TABLE IF NOT EXISTS pipeline_status (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    operating_mode TEXT,
    provider TEXT,
    last_successful_fetch TIMESTAMPTZ,
    latest_candle_timestamp TIMESTAMPTZ,
    watchlist_count INTEGER,
    last_published_count INTEGER,
    last_error TEXT,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- ==============================================================================
-- Tiger Cloud (TimescaleDB) Real-Time Financial Data Pipeline Schema
-- Challenge: Ingest real-time financial data into Tiger Cloud
-- ==============================================================================

-- 1. Create relational table for cryptocurrency & stock assets metadata
CREATE TABLE IF NOT EXISTS crypto_assets (
    symbol TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    asset_type TEXT DEFAULT 'Crypto',
    exchange TEXT DEFAULT 'Global',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Seed asset catalog
INSERT INTO crypto_assets (symbol, name, asset_type, exchange) VALUES
    ('BTC/USD', 'Bitcoin', 'Crypto', 'Coinbase Pro'),
    ('ETH/USD', 'Ethereum', 'Crypto', 'Huobi'),
    ('SOL/USD', 'Solana', 'Crypto', 'Binance'),
    ('AAPL', 'Apple Inc.', 'Stock', 'NASDAQ'),
    ('MSFT', 'Microsoft Corporation', 'Stock', 'NASDAQ'),
    ('NVDA', 'NVIDIA Corporation', 'Stock', 'NASDAQ'),
    ('AMZN', 'Amazon.com Inc.', 'Stock', 'NASDAQ'),
    ('GOOGL', 'Alphabet Inc.', 'Stock', 'NASDAQ')
ON CONFLICT (symbol) DO NOTHING;

-- 2. Create hypertable for streaming ticks
-- Using native Tiger Cloud hypercore storage parameters: segmentby symbol, orderby time DESC
CREATE TABLE IF NOT EXISTS crypto_ticks (
    "time" TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    day_volume NUMERIC
) WITH (
    tsdb.hypertable,
    tsdb.segmentby='symbol',
    tsdb.orderby='time DESC'
);

CREATE INDEX IF NOT EXISTS idx_crypto_ticks_symbol_time ON crypto_ticks (symbol, "time" DESC);

-- 3. Create Continuous Aggregate for 1-minute OHLCV candlestick aggregation
CREATE MATERIALIZED VIEW IF NOT EXISTS one_minute_candle
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', "time") AS bucket,
    symbol,
    FIRST(price, "time") AS "open",
    MAX(price) AS high,
    MIN(price) AS low,
    LAST(price, "time") AS "close",
    LAST(day_volume, "time") AS day_volume
FROM crypto_ticks
GROUP BY bucket, symbol;

-- 4. Create Continuous Aggregate for 1-day OHLCV candlestick aggregation
CREATE MATERIALIZED VIEW IF NOT EXISTS one_day_candle
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', "time") AS bucket,
    symbol,
    FIRST(price, "time") AS "open",
    MAX(price) AS high,
    MIN(price) AS low,
    LAST(price, "time") AS "close",
    LAST(day_volume, "time") AS day_volume
FROM crypto_ticks
GROUP BY bucket, symbol;

-- 5. Add Continuous Aggregate refresh policies
SELECT add_continuous_aggregate_policy('one_minute_candle',
    start_offset => INTERVAL '2 hours',
    end_offset => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute',
    if_not_exists => true
);

SELECT add_continuous_aggregate_policy('one_day_candle',
    start_offset => INTERVAL '3 days',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day',
    if_not_exists => true
);

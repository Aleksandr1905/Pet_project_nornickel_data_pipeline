CREATE SCHEMA IF NOT EXISTS nornickel;

CREATE TABLE IF NOT EXISTS nornickel.market_data (
    timestamp TIMESTAMPTZ NOT NULL,
    ticker    VARCHAR(20) NOT NULL,
    price     NUMERIC(18, 4),
    volume    BIGINT,
    asset_type VARCHAR(20),

    PRIMARY KEY (timestamp, ticker)
);

CREATE INDEX IF NOT EXISTS idx_market_data_ticker_time
ON nornickel.market_data (ticker, timestamp DESC);
CREATE TABLE IF NOT EXISTS market_ohlcv_features (
    exchange TEXT NOT NULL,
    symbol TEXT NOT NULL,
    interval TEXT NOT NULL,
    open_time TIMESTAMPTZ NOT NULL,
    close_time TIMESTAMPTZ NOT NULL,
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume DOUBLE PRECISION NOT NULL,
    quote_volume DOUBLE PRECISION NOT NULL,
    trade_count INTEGER NOT NULL,
    taker_buy_base_volume DOUBLE PRECISION NOT NULL,
    taker_buy_quote_volume DOUBLE PRECISION NOT NULL,
    is_synthetic BOOLEAN NOT NULL DEFAULT FALSE,
    load_source TEXT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL,
    return_1 DOUBLE PRECISION,
    return_5 DOUBLE PRECISION,
    return_20 DOUBLE PRECISION,
    log_return_1 DOUBLE PRECISION,
    ma_5 DOUBLE PRECISION,
    ma_20 DOUBLE PRECISION,
    ma_50 DOUBLE PRECISION,
    vol_5 DOUBLE PRECISION,
    vol_20 DOUBLE PRECISION,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (exchange, symbol, interval, open_time)
);

CREATE INDEX IF NOT EXISTS idx_market_ohlcv_features_symbol_interval_time
    ON market_ohlcv_features (symbol, interval, open_time DESC);

CREATE INDEX IF NOT EXISTS idx_market_ohlcv_features_exchange_time
    ON market_ohlcv_features (exchange, open_time DESC);

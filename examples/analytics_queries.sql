-- Latest 20 BTCUSDT one-minute bars with engineered features.
SELECT
    open_time,
    open,
    high,
    low,
    close,
    volume,
    return_1,
    ma_5,
    ma_20,
    vol_5
FROM market_ohlcv_features
WHERE symbol = 'BTCUSDT'
  AND interval = '1m'
ORDER BY open_time DESC
LIMIT 20;

-- Daily roll-up for monitoring ingest completeness.
SELECT
    symbol,
    interval,
    DATE_TRUNC('day', open_time) AS trading_day,
    COUNT(*) AS bar_count,
    SUM(CASE WHEN is_synthetic THEN 1 ELSE 0 END) AS synthetic_bar_count,
    MIN(open_time) AS first_bar,
    MAX(open_time) AS last_bar
FROM market_ohlcv_features
GROUP BY 1, 2, 3
ORDER BY trading_day DESC, symbol, interval;

-- Volatility screening for downstream analytics.
SELECT
    symbol,
    interval,
    open_time,
    close,
    vol_20
FROM market_ohlcv_features
WHERE interval = '5m'
  AND vol_20 IS NOT NULL
ORDER BY vol_20 DESC, open_time DESC
LIMIT 50;

"""Schema definitions shared across ingestion, transformation, and storage."""

RAW_KLINE_COLUMNS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_volume",
    "trade_count",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
    "ignore",
]

BASE_COLUMNS = [
    "exchange",
    "symbol",
    "interval",
    "open_time",
    "close_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_volume",
    "trade_count",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
    "is_synthetic",
    "load_source",
    "ingested_at",
]

FEATURE_COLUMNS = [
    "return_1",
    "return_5",
    "return_20",
    "log_return_1",
    "ma_5",
    "ma_20",
    "ma_50",
    "vol_5",
    "vol_20",
]

SERVING_COLUMNS = BASE_COLUMNS + FEATURE_COLUMNS

NUMERIC_COLUMNS = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_volume",
    "trade_count",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
]

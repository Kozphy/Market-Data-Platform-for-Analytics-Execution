"""Unit tests for OHLCV cleaning and normalization."""

from __future__ import annotations

import pandas as pd

from market_data_platform.transformation.cleaning import clean_ohlcv_frame


def test_clean_ohlcv_frame_deduplicates_and_fills_missing_bars() -> None:
    """Ensure duplicate bars are collapsed and missing intervals are synthesized.

    Returns:
        None: Assertions validate expected cleaning behavior.
    """
    raw_frame = pd.DataFrame(
        [
            {
                "exchange": "binance",
                "symbol": "BTCUSDT",
                "interval": "1m",
                "open_time": 1714521600000,
                "open": "100",
                "high": "101",
                "low": "99",
                "close": "100",
                "volume": "10",
                "close_time": 1714521659999,
                "quote_volume": "1000",
                "trade_count": "5",
                "taker_buy_base_volume": "4",
                "taker_buy_quote_volume": "400",
                "ignore": "0",
                "load_source": "binance_rest_api",
                "ingested_at": "2024-05-01T00:00:01+00:00",
            },
            {
                "exchange": "binance",
                "symbol": "BTCUSDT",
                "interval": "1m",
                "open_time": 1714521720000,
                "open": "103",
                "high": "104",
                "low": "102",
                "close": "103",
                "volume": "12",
                "close_time": 1714521779999,
                "quote_volume": "1236",
                "trade_count": "6",
                "taker_buy_base_volume": "5",
                "taker_buy_quote_volume": "515",
                "ignore": "0",
                "load_source": "binance_rest_api",
                "ingested_at": "2024-05-01T00:02:01+00:00",
            },
            {
                "exchange": "binance",
                "symbol": "BTCUSDT",
                "interval": "1m",
                "open_time": 1714521720000,
                "open": "102",
                "high": "104",
                "low": "101",
                "close": "102",
                "volume": "11",
                "close_time": 1714521779999,
                "quote_volume": "1122",
                "trade_count": "4",
                "taker_buy_base_volume": "4",
                "taker_buy_quote_volume": "408",
                "ignore": "0",
                "load_source": "binance_rest_api",
                "ingested_at": "2024-05-01T00:01:30+00:00",
            },
        ]
    )

    cleaned = clean_ohlcv_frame(raw_frame, interval="1m", synthesize_missing_bars=True)

    assert len(cleaned) == 3
    assert bool(cleaned.loc[1, "is_synthetic"]) is True
    assert cleaned.loc[1, "open"] == 100.0
    assert cleaned.loc[1, "close"] == 100.0
    assert cleaned.loc[1, "volume"] == 0.0
    assert cleaned.loc[2, "close"] == 103.0

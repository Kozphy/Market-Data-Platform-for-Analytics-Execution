"""PostgreSQL serving layer for the ETL pipeline."""

from __future__ import annotations

import logging
import time
from pathlib import Path

import pandas as pd
import psycopg

from market_data_platform.schema import SERVING_COLUMNS

LOGGER = logging.getLogger(__name__)


class PostgresServingStore:
    """Persist curated market data into PostgreSQL with upsert semantics."""

    def __init__(self, dsn: str, schema_sql_path: Path) -> None:
        """Initialize the PostgreSQL writer.

        Args:
            dsn: PostgreSQL DSN string.
            schema_sql_path: Path to the schema bootstrap SQL file.
        """
        self._dsn = dsn
        self._schema_sql_path = schema_sql_path

    def _connect(self) -> psycopg.Connection:
        """Create a PostgreSQL connection with retry handling.

        Returns:
            psycopg.Connection: Open PostgreSQL connection.

        Raises:
            psycopg.Error: If all connection attempts fail.
        """
        last_error: psycopg.Error | None = None
        for attempt in range(1, 6):
            try:
                return psycopg.connect(self._dsn)
            except psycopg.Error as error:
                last_error = error
                if attempt == 5:
                    break
                wait_seconds = attempt * 2
                LOGGER.warning(
                    "PostgreSQL connection attempt failed",
                    extra={
                        "payload": {
                            "attempt": attempt,
                            "wait_seconds": wait_seconds,
                            "error": str(error),
                        }
                    },
                )
                time.sleep(wait_seconds)

        assert last_error is not None
        raise last_error

    def ensure_schema(self) -> None:
        """Create required PostgreSQL tables and indexes.

        Returns:
            None: This method applies schema DDL statements.
        """
        sql = self._schema_sql_path.read_text(encoding="utf-8")
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql)
            connection.commit()

    def upsert_market_data(self, frame: pd.DataFrame) -> int:
        """Upsert the serving frame into PostgreSQL.

        Args:
            frame: Feature-rich serving frame.

        Returns:
            int: Number of rows attempted for loading.
        """
        if frame.empty:
            return 0

        records = [
            tuple(None if pd.isna(value) else value for value in row)
            for row in frame[SERVING_COLUMNS].itertuples(index=False, name=None)
        ]

        insert_sql = """
            INSERT INTO market_ohlcv_features (
                exchange,
                symbol,
                interval,
                open_time,
                close_time,
                open,
                high,
                low,
                close,
                volume,
                quote_volume,
                trade_count,
                taker_buy_base_volume,
                taker_buy_quote_volume,
                is_synthetic,
                load_source,
                ingested_at,
                return_1,
                return_5,
                return_20,
                log_return_1,
                ma_5,
                ma_20,
                ma_50,
                vol_5,
                vol_20
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (exchange, symbol, interval, open_time) DO UPDATE SET
                close_time = EXCLUDED.close_time,
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                quote_volume = EXCLUDED.quote_volume,
                trade_count = EXCLUDED.trade_count,
                taker_buy_base_volume = EXCLUDED.taker_buy_base_volume,
                taker_buy_quote_volume = EXCLUDED.taker_buy_quote_volume,
                is_synthetic = EXCLUDED.is_synthetic,
                load_source = EXCLUDED.load_source,
                ingested_at = EXCLUDED.ingested_at,
                return_1 = EXCLUDED.return_1,
                return_5 = EXCLUDED.return_5,
                return_20 = EXCLUDED.return_20,
                log_return_1 = EXCLUDED.log_return_1,
                ma_5 = EXCLUDED.ma_5,
                ma_20 = EXCLUDED.ma_20,
                ma_50 = EXCLUDED.ma_50,
                vol_5 = EXCLUDED.vol_5,
                vol_20 = EXCLUDED.vol_20,
                updated_at = NOW()
        """

        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.executemany(insert_sql, records)
            connection.commit()

        return len(records)

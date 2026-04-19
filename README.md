# Market Data Platform (Interview Project)

Minimal, production-style market data platform built around Binance OHLCV data.

It demonstrates:
- Binance ingestion with retries/backoff
- Bronze/Silver/Gold Parquet lakehouse
- PostgreSQL serving table with idempotent upserts
- ETL jobs (historical, incremental, realtime refresh)
- Apache Airflow (LocalExecutor) orchestrating staged ETL
- Streaming threshold alerts
- Backtesting with Sharpe and max drawdown
- Docker Compose and CLI-first workflow

## Architecture

```text
Binance REST API
  -> Extract (raw OHLCV)
  -> Bronze Parquet (raw)
  -> Clean/normalize
  -> Silver Parquet (clean)
  -> Feature engineering (returns, MA, vol)
  -> Gold Parquet (analytics-ready)
  -> PostgreSQL (market_ohlcv_features)

Binance ticker polling
  -> Tick Parquet
  -> Threshold rules
  -> JSONL alerts

Gold features + threshold logic
  -> Backtest
  -> Sharpe, drawdown, return, trade count
```

## Project Layout

```text
src/market_data_platform/
  cli.py
  config.py
  ingestion/
  transformation/
  storage/
  streaming/
  orchestration/
  signals/
  quant/
sql/postgres/001_create_market_data.sql
sql/postgres/002_create_airflow_db.sql
dags/market_data_dag.py
examples/analytics_queries.sql
tests/
docker-compose.yml
Dockerfile
Dockerfile.airflow
```

## Quick Start (Docker Compose)

1. Copy env file:

```bash
cp .env.example .env
```

2. Build and start:

```bash
docker compose up --build
```

This starts:
- `postgres` on port `5432`
- `airflow-init` (one-shot DB migrations + admin user)
- `airflow-webserver` on port `8080` (override with `AIRFLOW_WEBSERVER_PORT`)
- `airflow-scheduler` (LocalExecutor)

Artifacts are written under `./data`.

Optional legacy single-process scheduler:

```bash
docker compose --profile legacy up --build
```

Airflow DAG `market_data_pipeline` runs staged CLI tasks hourly: extract → silver → gold → Postgres → data quality. Set `catchup: true` in `dags/market_data_dag.py` or run `airflow dags backfill` for historical intervals.

If your Postgres volume was created before `sql/postgres/002_create_airflow_db.sql` existed, create the Airflow metadata database once: `CREATE DATABASE airflow;` (as a superuser), then restart `airflow-init`.

## Quick Start (Local Python)

1. Install:

```bash
pip install -e .[dev]
```

2. Copy env file:

```bash
cp .env.example .env
```

3. Bootstrap:

```bash
market-data bootstrap
```

## CLI Commands

Historical backfill:

```bash
market-data historical \
  --symbol BTCUSDT \
  --interval 1m \
  --start 2024-05-01T00:00:00+00:00 \
  --end 2024-05-02T00:00:00+00:00
```

Incremental ETL:

```bash
market-data incremental --symbol BTCUSDT --interval 1m
```

Airflow staged ETL (fixed UTC window; used by `dags/market_data_dag.py`):

```bash
WIN_START="2024-05-01T00:00:00+00:00"
WIN_END="2024-05-01T01:00:00+00:00"
market-data extract-ohlcv --symbol BTCUSDT --interval 1m --window-start "$WIN_START" --window-end "$WIN_END"
market-data transform-bronze-to-silver --symbol BTCUSDT --interval 1m --window-start "$WIN_START" --window-end "$WIN_END"
market-data transform-silver-to-gold --symbol BTCUSDT --interval 1m --window-start "$WIN_START" --window-end "$WIN_END"
market-data load-gold-to-postgres --symbol BTCUSDT --interval 1m --window-start "$WIN_START" --window-end "$WIN_END"
market-data data-quality-check --symbol BTCUSDT --interval 1m --window-start "$WIN_START" --window-end "$WIN_END"
```

Realtime refresh:

```bash
market-data realtime --symbol BTCUSDT --interval 1m --recent-bars 5
```

Streaming alerts (single poll):

```bash
market-data streaming \
  --symbol BTCUSDT \
  --rule "BTCUSDT|cross_above|70000|btc_breakout" \
  --run-once
```

Streaming alerts (continuous):

```bash
market-data streaming \
  --symbol BTCUSDT \
  --rule "BTCUSDT|cross_above|70000|btc_breakout"
```

Backtest:

```bash
market-data backtest \
  --symbol BTCUSDT \
  --interval 1m \
  --start 2024-05-01T00:00:00+00:00 \
  --end 2024-05-07T00:00:00+00:00 \
  --entry-threshold 70000 \
  --exit-threshold 68000 \
  --output-path ./data/backtests/btc_threshold_backtest.csv
```

Run scheduler:

```bash
market-data scheduler
```

## Storage Outputs

- Bronze: `data/lakehouse/bronze/...`
- Silver: `data/lakehouse/silver/...`
- Gold: `data/lakehouse/gold/...`
- Streaming ticks: `data/streaming/ticks/...`
- Streaming alerts: `data/alerts/threshold_alerts.jsonl`
- Checkpoints: `data/state/pipeline_state.json`

## PostgreSQL

Table:
- `market_ohlcv_features`

Primary key:
- `(exchange, symbol, interval, open_time)`

Schema is auto-applied from `sql/postgres/001_create_market_data.sql`.

## Example SQL

See `examples/analytics_queries.sql` for:
- latest enriched candles
- completeness checks
- volatility screens

## Tests

```bash
pytest
```

Current tests cover:
- cleaning and missing-bar synthesis
- feature generation
- streaming threshold behavior
- backtesting metrics behavior

## Interview Talking Points

- Idempotent ETL with checkpoint state
- Lakehouse + serving-store pattern (Parquet + Postgres)
- Separation of batch ETL and streaming alerts
- Reused signal logic in live monitoring and backtests
- Production-minded packaging, Dockerization, and tests

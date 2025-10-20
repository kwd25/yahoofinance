# Yahoo Finance Bronze → Silver/Gold (Databricks)

Daily batch ingest from yfinance into `workspace.yahoo.bronze_prices` via the Databricks SQL connector.
Silver/Gold are **views** in Databricks and auto-recompute on query.

## Setup

1. **Databricks** 
   - Create a SQL Warehouse and copy **Server Hostname** and **HTTP Path** from its Connection details.
   - Create a **Personal Access Token** (User Settings → Developer → Access tokens).

2. **GitHub Secrets** (Repo → Settings → Secrets and variables → Actions)
   - `DATABRICKS_SERVER` (e.g., `adb-xxxx.azuredatabricks.net`)
   - `DATABRICKS_HTTP_PATH` (e.g., `/sql/1.0/warehouses/xxxxxxxxxxxx`)
   - `DATABRICKS_TOKEN` (`dapi...`)

3. **Optional Variables**
   - `SYMBOLS` → comma list of tickers (defaults to ~40 majors)
   - `BACKFILL_YEARS` → default `10`
   - `BUFFER_DAYS` → default `7`

## Run
- **Manual:** GitHub → Actions → “Daily Ingest to Databricks” → Run workflow
- **Scheduled:** Daily 12:10 UTC (≈ 7:10 AM CT)

## Verify in Databricks
```sql
SELECT COUNT(*) FROM workspace.yahoo.bronze_prices;
SELECT * FROM workspace.yahoo.bronze_prices WHERE symbol='AAPL' ORDER BY date DESC LIMIT 5;

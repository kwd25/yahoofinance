#!/usr/bin/env python3
import os
import databricks.sql as dbsql

CATALOG = os.getenv("CATALOG", "workspace")
SCHEMA = os.getenv("SCHEMA", "yahoo")

SERVER = os.environ["DATABRICKS_SERVER"]
HTTPPATH = os.environ["DATABRICKS_HTTP_PATH"]
TOKEN = os.environ["DATABRICKS_TOKEN"]

QUERY = f"""
USE CATALOG {CATALOG};
USE SCHEMA {SCHEMA};
SET use_cached_result = false;

-- ========== SILVER ==========
CREATE OR REPLACE VIEW silver_prices AS
SELECT
  CAST(date AS DATE)        AS date,
  CAST(open AS DOUBLE)      AS open,
  CAST(high AS DOUBLE)      AS high,
  CAST(low AS DOUBLE)       AS low,
  CAST(close AS DOUBLE)     AS close,
  CAST(adj_close AS DOUBLE) AS adj_close,
  CAST(volume AS BIGINT)    AS volume,
  UPPER(TRIM(symbol))       AS symbol
FROM {CATALOG}.{SCHEMA}.bronze_prices
WHERE date IS NOT NULL AND symbol IS NOT NULL
  AND open   IS NOT NULL AND high   IS NOT NULL AND low    IS NOT NULL
  AND close  IS NOT NULL AND volume IS NOT NULL
  AND open  >= 0 AND high >= 0 AND low >= 0 AND close >= 0 AND volume >= 0
  AND high >= low;

-- ========== GOLD ==========
CREATE OR REPLACE VIEW gold_features AS
WITH base AS (
  SELECT
    date,
    symbol,
    close,
    CASE
      WHEN LAG(close) OVER (PARTITION BY symbol ORDER BY date) > 0
      THEN (close / LAG(close) OVER (PARTITION BY symbol ORDER BY date)) - 1
    END AS ret_1d,
    CASE
      WHEN LAG(close) OVER (PARTITION BY symbol ORDER BY date) > 0
      THEN LOG(close / LAG(close) OVER (PARTITION BY symbol ORDER BY date))
    END AS log_ret_1d,
    CASE
      WHEN LAG(close, 20) OVER (PARTITION BY symbol ORDER BY date) > 0
      THEN (close / LAG(close, 20) OVER (PARTITION BY symbol ORDER BY date)) - 1
    END AS mom_20d,
    STDDEV_SAMP(
      LOG(close / NULLIF(LAG(close) OVER (PARTITION BY symbol ORDER BY date), 0))
    ) OVER (
      PARTITION BY symbol
      ORDER BY date
      ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS vol_20d_raw,
    AVG(close) OVER (
      PARTITION BY symbol
      ORDER BY date
      ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS sma_20,
    AVG(close) OVER (
      PARTITION BY symbol
      ORDER BY date
      ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
    ) AS sma_50
  FROM silver_prices
)
SELECT
  date,
  symbol,
  close,
  ret_1d,
  log_ret_1d,
  mom_20d,
  CASE WHEN vol_20d_raw IS NOT NULL THEN vol_20d_raw * SQRT(252.0) END AS vol_20d,
  sma_20,
  sma_50
FROM base;
"""

def main():
    print("[PREPROCESS] Starting Silver → Gold materialization...")
    with dbsql.connect(server_hostname=SERVER, http_path=HTTPPATH, access_token=TOKEN) as conn:
        with conn.cursor() as cur:
            for stmt in QUERY.strip().split(";"):
                stmt = stmt.strip()
                if stmt:
                    cur.execute(stmt)
                    print(f"[OK] {stmt.split()[0]} ...")
    print("[PREPROCESS] Complete.")

if __name__ == "__main__":
    main()

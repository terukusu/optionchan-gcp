-- /atm_iv_bars?n=roll（期近つなぎ、#101）の 1h・1d: sql/atm_iv.sql の限月で絞らない版。1 時間足 atm_iv と、まだ 1 時間足に
-- なっていない分（[@fill_start, @fill_end) の最新版の option_price の ATM の put）を同じ計算（-- bars:begin〜end は
-- sql/ohlc/atm_hourly.sql と一字一句同じ。tests/test_ohlc.py が検査）で 1 時間足にしたものを、全限月そのままつなぐ。
-- 限月の選び方（取引日ごとに期近の最終取引日の N 営業日前で 2 限月へ乗り換える）はアプリ（bars.roll）が決める。
--   足 = started_at が [@days_start, @range_end) の全限月の 1 時間足 + 補った足（値が NULL の足も出る。アプリが除く）、
--        started_at, last_trading_day 昇順
-- 期間・補う分の決め方は sql/atm_iv.sql と同じ（アプリが api.hourly_params の同名の値を渡す。過去だけの範囲では
-- @fill_end を @fill_start より前にして生データを読まない）。
-- -- source:begin 〜 -- source:end は apps/viewer/tests/test_sql_bq.py が固定値の入力に差し替える印
WITH
-- source:begin
atm AS (
  SELECT created_at, last_trading_day, target_price, iv, price
  FROM `${project}.${dataset}.option_price_latest`(@fill_start, @fill_end)
  WHERE is_atm AND type = 2
),
hourly AS (
  SELECT open, high, low, close, last_trading_day, started_at
  FROM `${project}.${dataset}.atm_iv`
  WHERE started_at >= @days_start AND started_at < @fill_start
),
-- source:end
-- bars:begin
bars AS (
  SELECT
    last_trading_day,
    3600 AS time_frame,
    TIMESTAMP_TRUNC(created_at, HOUR) AS started_at,
    ARRAY_AGG(iv IGNORE NULLS ORDER BY created_at, target_price, iv)[SAFE_OFFSET(0)] AS iv_open,
    MAX(iv) AS iv_high,
    MIN(iv) AS iv_low,
    ARRAY_AGG(iv IGNORE NULLS ORDER BY created_at DESC, target_price DESC, iv DESC)[SAFE_OFFSET(0)] AS iv_close,
    CAST(ARRAY_AGG(price IGNORE NULLS ORDER BY created_at, target_price, price)[SAFE_OFFSET(0)] AS FLOAT64) AS price_open,
    CAST(MAX(price) AS FLOAT64) AS price_high,
    CAST(MIN(price) AS FLOAT64) AS price_low,
    CAST(ARRAY_AGG(price IGNORE NULLS ORDER BY created_at DESC, target_price DESC, price DESC)[SAFE_OFFSET(0)] AS FLOAT64) AS price_close,
    CAST(ARRAY_AGG(target_price IGNORE NULLS ORDER BY created_at, target_price)[SAFE_OFFSET(0)] AS FLOAT64) AS target_price_open,
    CAST(MAX(target_price) AS FLOAT64) AS target_price_high,
    CAST(MIN(target_price) AS FLOAT64) AS target_price_low,
    CAST(ARRAY_AGG(target_price IGNORE NULLS ORDER BY created_at DESC, target_price DESC)[SAFE_OFFSET(0)] AS FLOAT64) AS target_price_close
  FROM atm
  GROUP BY last_trading_day, started_at
)
-- bars:end
SELECT open, high, low, close, last_trading_day, started_at
FROM (
  SELECT open, high, low, close, last_trading_day, started_at
  FROM hourly
  WHERE started_at < @range_end
  UNION ALL
  SELECT iv_open AS open, iv_high AS high, iv_low AS low, iv_close AS close, last_trading_day, started_at
  FROM bars
  WHERE started_at >= @days_start AND started_at < @range_end
)
ORDER BY started_at, last_trading_day

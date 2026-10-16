-- /atm_iv_bars?n=roll&iv=mid（期近つなぎの mid IV、#101・#98）の 1h・1d: sql/atm_iv_months.sql の iv を mid IV（気配の中値）に
-- 替えたもの。1 時間足 atm_mid_iv と、まだ 1 時間足になっていない分を同じ計算（-- mid_bars:begin〜end は sql/ohlc/atm_hourly.sql と
-- 一字一句同じ。tests/test_ohlc.py が検査）で 1 時間足にしたものを、全限月そのままつなぐ。限月の選び方はアプリ（bars.roll）が決める。
-- 期間・補う分の決め方・並べ方は sql/atm_iv_months.sql と同じ。
-- 読む列は created_at・last_trading_day・target_price・bid_iv・ask_iv だけ（iv・price は読まない）。
-- -- source:begin 〜 -- source:end は apps/viewer/tests/test_sql_bq.py が固定値の入力に差し替える印
WITH
-- source:begin
atm AS (
  SELECT created_at, last_trading_day, target_price, bid_iv, ask_iv
  FROM `${project}.${dataset}.option_price_latest`(@fill_start, @fill_end)
  WHERE is_atm AND type = 2
),
hourly AS (
  SELECT open, high, low, close, last_trading_day, started_at
  FROM `${project}.${dataset}.atm_mid_iv`
  WHERE started_at >= @days_start AND started_at < @fill_start
),
-- source:end
-- mid_bars:begin
mid_bars AS (
  SELECT
    last_trading_day,
    TIMESTAMP_TRUNC(created_at, HOUR) AS started_at,
    ARRAY_AGG(mid IGNORE NULLS ORDER BY created_at, target_price, mid)[SAFE_OFFSET(0)] AS mid_open,
    MAX(mid) AS mid_high,
    MIN(mid) AS mid_low,
    ARRAY_AGG(mid IGNORE NULLS ORDER BY created_at DESC, target_price DESC, mid DESC)[SAFE_OFFSET(0)] AS mid_close
  FROM (
    SELECT created_at, last_trading_day, target_price, IF(bid_iv > 0 AND ask_iv > 0, (bid_iv + ask_iv) / 2, NULL) AS mid
    FROM atm
  )
  GROUP BY last_trading_day, started_at
)
-- mid_bars:end
SELECT open, high, low, close, last_trading_day, started_at
FROM (
  SELECT open, high, low, close, last_trading_day, started_at
  FROM hourly
  WHERE started_at < @range_end
  UNION ALL
  SELECT mid_open AS open, mid_high AS high, mid_low AS low, mid_close AS close, last_trading_day, started_at
  FROM mid_bars
  WHERE started_at >= @days_start AND started_at < @range_end
)
ORDER BY started_at, last_trading_day

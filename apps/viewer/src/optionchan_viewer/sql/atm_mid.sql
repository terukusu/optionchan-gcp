-- /atm_iv_bars?iv=mid の 1h・1d（#98）: sql/atm_iv.sql の iv を mid IV（気配の中値）に替えたもの。1 時間足 atm_mid_iv（定期クエリ
-- sql/ohlc/atm_hourly.sql が作る）と、まだ 1 時間足になっていない分（[@fill_start, @fill_end) の最新版の option_price の ATM の put）を
-- 同じ計算（-- mid_bars:begin〜end は sql/ohlc/atm_hourly.sql と一字一句同じ。tests/test_ohlc.py が検査）で 1 時間足にしたものをつなぐ。
-- 限月の選び方・期間・補う分の決め方・並べ方は sql/atm_iv.sql と同じ（アプリが api.hourly_params の同じ値を渡す。
-- @fill_start は atm_iv の最後の足の次。atm_mid_iv は同じ定期クエリが同じトランザクションで書くので最後の足は同じ）。
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
  WHERE started_at >= @hourly_start AND started_at < @fill_start
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
,
month AS (
  SELECT
    ARRAY_AGG(DISTINCT last_trading_day IGNORE NULLS ORDER BY last_trading_day)[SAFE_OFFSET(@n)] AS last_trading_day
  FROM (
    SELECT last_trading_day FROM hourly WHERE started_at > @months_since
    UNION ALL
    SELECT last_trading_day FROM mid_bars
  )
  WHERE last_trading_day >= @today
)
SELECT open, high, low, close, started_at
FROM (
  SELECT open, high, low, close, started_at
  FROM hourly
  WHERE last_trading_day = (SELECT last_trading_day FROM month) AND started_at >= @days_start AND started_at < @range_end
  UNION ALL
  SELECT mid_open AS open, mid_high AS high, mid_low AS low, mid_close AS close, started_at
  FROM mid_bars
  WHERE last_trading_day = (SELECT last_trading_day FROM month) AND started_at >= @days_start AND started_at < @range_end
)
ORDER BY started_at

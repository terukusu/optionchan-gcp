-- mid IV の 1 時間足（#98）の過去分の作り直し。最新版（option_price_latest）の ATM の put の mid から、started_at が
-- [@start_at, @end_at) の 1 時間足を作り、atm_mid_iv に MERGE する（定期クエリ sql/ohlc/atm_hourly.sql の atm_mid_iv と同じ計算。
-- -- mid_bars:begin 〜 -- mid_bars:end は atm_hourly.sql と一字一句同じ。tests/test_ohlc.py が検査）。
--   足は範囲の中の atm_iv の 1 時間足のキー（iv_keys）に限る（#122）。atm_iv の過去分（2026-10-12 22:00 UTC より前）は旧系から
--   移した値で、UTC 23 時台（08 JST、寄り前）の足や旧系が動いていなかった日の足が無い。最新版からそのまま作ると mid の足だけが
--   多くなり、約定の足とそろわない。範囲の中の atm_iv に無いキーの mid の足は消える
--   範囲: start_at・end_at は正時（1 時間足が範囲をまたがない）。end_at は now 以下
--   冪等: 範囲の中を置き換える（atm_hourly.sql と同じ MERGE。同じ範囲を流し直しても同じ結果）。範囲に ATM の行が 1 件も無いか、
--     範囲の中に atm_iv のキーが 1 つも無ければ何もしない
--   読む量: option_price のパーティションの bid_iv・ask_iv 等の列（2018-01〜2026-10 の全部で 約 40 GiB。1 年ずつ流す）と atm_iv のキー（数 MB）
-- 結果は 1 行（bars = 作った足の数、with_mid = mid の値がある足の数）。
-- 名前は runbook の render が、期間は bq のクエリパラメータが入れる。手順は docs/runbook.md「1 時間足（#49）」の「mid IV の過去分（#98）」。
-- -- source:begin 〜 -- source:end / -- result:begin 〜 -- result:end は tests/test_ohlc_bq.py が固定値の入力に差し替える印。
DECLARE start_at TIMESTAMP DEFAULT @start_at;
DECLARE end_at TIMESTAMP DEFAULT @end_at;

IF start_at IS NULL OR end_at IS NULL OR start_at >= end_at THEN
  SELECT ERROR(FORMAT('mid_backfill: invalid range [%t, %t)', start_at, end_at));
END IF;

IF TIMESTAMP_TRUNC(start_at, HOUR) != start_at OR TIMESTAMP_TRUNC(end_at, HOUR) != end_at OR end_at > CURRENT_TIMESTAMP() THEN
  SELECT ERROR(FORMAT('mid_backfill: range [%t, %t) must be on the hour and not in the future', start_at, end_at));
END IF;

CREATE TEMP TABLE bars AS
-- result:begin
WITH
-- source:begin
atm AS (
  SELECT created_at, last_trading_day, target_price, bid_iv, ask_iv
  FROM `${project}.${dataset}.option_price_latest`(start_at, end_at)
  WHERE is_atm AND type = 2
),
-- source:end
iv_keys AS (
  SELECT DISTINCT last_trading_day, started_at
  FROM `${project}.${dataset}.atm_iv`
  WHERE started_at >= start_at AND started_at < end_at AND time_frame = 3600
),
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
SELECT *, 3600 AS time_frame FROM mid_bars JOIN iv_keys USING (last_trading_day, started_at)
-- result:end
;

IF EXISTS (SELECT 1 FROM bars) THEN
  BEGIN TRANSACTION;

  MERGE `${project}.${dataset}.atm_mid_iv` AS t
  USING (SELECT mid_open AS open, mid_high AS high, mid_low AS low, mid_close AS close, last_trading_day, time_frame, started_at FROM bars) AS s
  ON t.last_trading_day = s.last_trading_day AND t.time_frame = s.time_frame AND t.started_at = s.started_at
    AND t.started_at >= start_at AND t.started_at < end_at
  WHEN MATCHED THEN UPDATE SET open = s.open, high = s.high, low = s.low, close = s.close
  WHEN NOT MATCHED THEN INSERT (open, high, low, close, last_trading_day, time_frame, started_at)
    VALUES (s.open, s.high, s.low, s.close, s.last_trading_day, s.time_frame, s.started_at)
  WHEN NOT MATCHED BY SOURCE AND t.started_at >= start_at AND t.started_at < end_at AND t.time_frame = 3600 THEN DELETE;

  COMMIT TRANSACTION;
END IF;

SELECT COUNT(*) AS bars, COUNTIF(mid_close IS NOT NULL) AS with_mid FROM bars;

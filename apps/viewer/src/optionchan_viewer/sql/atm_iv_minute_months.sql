-- /atm_iv_bars?n=roll（期近つなぎ、#101）の分足（1m・5m・15m）: sql/atm_iv_minute.sql の限月で絞らない版。
-- [@start_at, @end_at)（期間の決め方は atm_iv_minute.sql と同じ）の最新版の option_price の ATM の put を、全限月それぞれの
-- 1 分ごとの iv の OHLC にする。限月の選び方（取引日ごとの乗り換え）・5m・15m の束ねはアプリ（bars.roll・bars.resample）。
--   足 = (last_trading_day, TIMESTAMP_TRUNC(created_at, MINUTE)) ごと。open / high / low / close の式は atm_iv_minute.sql と同じ
--        （sql/ohlc/atm_hourly.sql の bars の iv の 4 式）
--   iv が全部 NULL の分も行は出る（アプリが除く）。started_at, last_trading_day 昇順
-- 読む列は atm_iv_minute.sql と同じ（created_at・last_trading_day・target_price・iv。スキャン量を増やさない）。
-- -- source:begin 〜 -- source:end は apps/viewer/tests/test_sql_bq.py が固定値の入力に差し替える印
WITH
-- source:begin
atm AS (
  SELECT created_at, last_trading_day, target_price, iv
  FROM `${project}.${dataset}.option_price_latest`(@start_at, @end_at)
  WHERE is_atm AND type = 2
)
-- source:end
SELECT
  ARRAY_AGG(iv IGNORE NULLS ORDER BY created_at, target_price, iv)[SAFE_OFFSET(0)] AS open,
  MAX(iv) AS high,
  MIN(iv) AS low,
  ARRAY_AGG(iv IGNORE NULLS ORDER BY created_at DESC, target_price DESC, iv DESC)[SAFE_OFFSET(0)] AS close,
  last_trading_day,
  TIMESTAMP_TRUNC(created_at, MINUTE) AS started_at
FROM atm
GROUP BY last_trading_day, started_at
ORDER BY started_at, last_trading_day

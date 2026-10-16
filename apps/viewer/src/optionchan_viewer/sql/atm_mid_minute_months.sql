-- /atm_iv_bars?n=roll&iv=mid（期近つなぎ、#101）の分足（1m・5m・15m）: sql/atm_mid_minute.sql の限月で絞らない版
-- （sql/atm_iv_minute_months.sql の iv を mid IV に替えたもの）。期間・足の切り方・OHLC の式・並べ方は atm_iv_minute_months.sql と同じ。
--   mid = (bid_iv + ask_iv) / 2。bid_iv・ask_iv が両方 0 より大きいときだけ（atm_mid_minute.sql と同じ規則）
--   mid が全部 NULL の分も行は出る（アプリが除く）。started_at, last_trading_day 昇順
-- 読む列は atm_mid_minute.sql と同じ（created_at・last_trading_day・target_price・bid_iv・ask_iv。iv は読まない）。
-- -- source:begin 〜 -- source:end は apps/viewer/tests/test_sql_bq.py が固定値の入力に差し替える印
WITH
-- source:begin
quote AS (
  SELECT created_at, last_trading_day, target_price, bid_iv, ask_iv
  FROM `${project}.${dataset}.option_price_latest`(@start_at, @end_at)
  WHERE is_atm AND type = 2
),
-- source:end
atm AS (
  SELECT created_at, last_trading_day, target_price, IF(bid_iv > 0 AND ask_iv > 0, (bid_iv + ask_iv) / 2, NULL) AS iv
  FROM quote
)
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

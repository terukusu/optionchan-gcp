-- /atm_iv_bars?iv=mid の分足（1m・5m・15m。#93）: sql/atm_iv_minute.sql の iv を mid IV（気配の中値）に替えたもの。
-- 期間・限月の選び方・足の切り方・OHLC の式・並べ方は atm_iv_minute.sql と同じ。
--   mid = (bid_iv + ask_iv) / 2。bid_iv・ask_iv が両方 0 より大きいときだけ（片方だけ・0 以下は NULL。apps/viewer の smile.py の mid_iv と同じ規則）
--   mid が全部 NULL の分も行は出る（アプリが除く）。started_at 昇順
-- 読む列は created_at・last_trading_day・target_price・bid_iv・ask_iv だけ（iv は読まない）。
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
),
month AS (
  SELECT
    ARRAY_AGG(DISTINCT last_trading_day IGNORE NULLS ORDER BY last_trading_day)[SAFE_OFFSET(@n)] AS last_trading_day
  FROM atm
  WHERE last_trading_day >= @today
)
SELECT
  ARRAY_AGG(iv IGNORE NULLS ORDER BY created_at, target_price, iv)[SAFE_OFFSET(0)] AS open,
  MAX(iv) AS high,
  MIN(iv) AS low,
  ARRAY_AGG(iv IGNORE NULLS ORDER BY created_at DESC, target_price DESC, iv DESC)[SAFE_OFFSET(0)] AS close,
  TIMESTAMP_TRUNC(created_at, MINUTE) AS started_at
FROM atm
WHERE last_trading_day = (SELECT last_trading_day FROM month)
GROUP BY started_at
ORDER BY started_at

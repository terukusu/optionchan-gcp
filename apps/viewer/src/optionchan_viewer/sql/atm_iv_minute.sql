-- /atm_iv_bars の分足（1m・5m・15m。#68・#77）: [@start_at, @end_at)（アプリが要求の start・end（最大 3 日）か、既定の最新のスナップショットの 1 日前の UTC の 0 時〜now を
-- 渡す。遡れるのは now の 31 日前まで）の最新版の option_price の ATM の put を、限月の 1 分ごとの iv の OHLC にする。5m・15m はアプリが束ねる。
--   限月 = 窓の中の last_trading_day のうち @today（JST の今日）以降を昇順に並べた @n 番目（範囲外は NULL → 0 行。並べ方は sql/atm_iv.sql と同じだが、候補を探すのは直近 10 日ではなく読む窓の中）
--   足 = TIMESTAMP_TRUNC(created_at, MINUTE) ごと。open / high / low / close は sql/ohlc/atm_hourly.sql の bars の iv の 4 式と同じ
--        （created_at が最も早い / 遅い NULL でない値、最大 / 最小。同じ created_at の行の順は target_price で決める）
--   iv が全部 NULL の分も行は出る（アプリが除く）。started_at 昇順
-- 読む列は created_at・last_trading_day・target_price・iv だけ（スキャン量を増やさない）。
-- -- source:begin 〜 -- source:end は apps/viewer/tests/test_sql_bq.py が固定値の入力に差し替える印
WITH
-- source:begin
atm AS (
  SELECT created_at, last_trading_day, target_price, iv
  FROM `${project}.${dataset}.option_price_latest`(@start_at, @end_at)
  WHERE is_atm AND type = 2
),
-- source:end
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

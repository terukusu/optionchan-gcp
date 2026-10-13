-- /atm_data: [@start_at, @end_at)（アプリが now − 7 日〜now を渡す）の最新版の option_price の ATM の行（type は絞らない。
-- 現行どおり call と put の両方を平均する）のうち、限月 = 期間内で @today（now の JST の日付）以降の最小の last_trading_day。
-- created_at を 300 秒（5 分）で切り捨てた時刻ごとの平均（小数 1 桁）。
-- 現行の atm_data（functions/optionchan_dao.py find_recent_iv_and_price_of_atm_options）と同じ計算。違い:
--   期間の下端が開区間（created_at > now − 7 日）→ 閉区間 [@start_at, @end_at)。
--   限月の決定が全期間の option_price → 期間内のデータ（期間内に行の無い限月は表示されないので結果は同じ）
-- -- source:begin 〜 -- source:end は apps/viewer/tests/test_sql_bq.py が固定値の入力に差し替える印
WITH
-- source:begin
atm AS (
  SELECT created_at, last_trading_day, target_price, iv, price
  FROM `${project}.${dataset}.option_price_latest`(@start_at, @end_at)
  WHERE is_atm
),
-- source:end
month AS (
  SELECT MIN(last_trading_day) AS l_min FROM atm WHERE last_trading_day >= @today
),
t2 AS (
  SELECT target_price, iv, price, TIMESTAMP_SECONDS(CAST(TRUNC(UNIX_SECONDS(created_at) / 300) AS INT64) * 300) AS time
  FROM atm
  WHERE last_trading_day = (SELECT l_min FROM month)
)
SELECT
  UNIX_SECONDS(time) AS time,
  ROUND(AVG(target_price), 1) AS target_price,
  ROUND(AVG(iv), 1) AS iv,
  ROUND(AVG(price), 1) AS price
FROM t2
GROUP BY time
ORDER BY time

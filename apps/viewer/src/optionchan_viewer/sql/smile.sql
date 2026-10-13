-- /smile_data のクエリ 2: スナップショット @start_at（[@start_at, @end_at) = その 1 秒）の最新版の option_price を、
-- 行使価格ごとに 1 限月（そのスナップショットの last_trading_day の最小）・2 限月（最大）の call / put の iv と取引時刻に並べる。
-- 現行の smile_data（functions/optionchan_dao.py find_option_price_by_created_at）と同じピボット。
-- price_time（JST の DATETIME）は UNIX 秒にする（現行の tz_localize(JST).timestamp() と同じ）。o1_put_is_atm はメタ行の ATM 用
-- -- source:begin 〜 -- source:end は apps/viewer/tests/test_sql_bq.py が固定値の入力に差し替える印
WITH
-- source:begin
snap AS (
  SELECT type, target_price, last_trading_day, iv, price_time, is_atm
  FROM `${project}.${dataset}.option_price_latest`(@start_at, @end_at)
),
-- source:end
t AS (
  SELECT MIN(last_trading_day) AS l_min, MAX(last_trading_day) AS l_max FROM snap
)
SELECT
  target_price,
  MAX(CASE WHEN last_trading_day = t.l_min AND type = 1 THEN iv END) AS o1_call_iv,
  UNIX_SECONDS(TIMESTAMP(MAX(CASE WHEN last_trading_day = t.l_min AND type = 1 THEN price_time END), 'Asia/Tokyo')) AS o1_call_price_time,
  MAX(CASE WHEN last_trading_day = t.l_min AND type = 2 THEN iv END) AS o1_put_iv,
  UNIX_SECONDS(TIMESTAMP(MAX(CASE WHEN last_trading_day = t.l_min AND type = 2 THEN price_time END), 'Asia/Tokyo')) AS o1_put_price_time,
  MAX(CASE WHEN last_trading_day = t.l_max AND type = 1 THEN iv END) AS o2_call_iv,
  UNIX_SECONDS(TIMESTAMP(MAX(CASE WHEN last_trading_day = t.l_max AND type = 1 THEN price_time END), 'Asia/Tokyo')) AS o2_call_price_time,
  MAX(CASE WHEN last_trading_day = t.l_max AND type = 2 THEN iv END) AS o2_put_iv,
  UNIX_SECONDS(TIMESTAMP(MAX(CASE WHEN last_trading_day = t.l_max AND type = 2 THEN price_time END), 'Asia/Tokyo')) AS o2_put_price_time,
  MAX(CASE WHEN last_trading_day = t.l_min AND type = 2 THEN is_atm END) AS o1_put_is_atm
FROM snap CROSS JOIN t
GROUP BY target_price
ORDER BY target_price

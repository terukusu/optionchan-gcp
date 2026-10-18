-- /smile_snapshot のクエリ 2（#79）: スナップショット @start_at（[@start_at, @end_at) = その 1 秒）の最新版の option_price を、
-- 限月 × 行使価格 × call / put の縦持ちで返す。限月の割り当ては smile.sql と同じ（そのスナップショットの last_trading_day の
-- 最小 = 1 限月、最大 = 2 限月。1 つしか無ければ両方に同じ行）。スキャンを 1 回にするため CTE を 2 回参照せず、
-- 窓関数と UNNEST([1, 2]) で割り当てる。price_time（JST の DATETIME）は UNIX 秒にする（smile.sql と同じ）。
-- IV の 0 の扱い・mid IV・OTM 合成はアプリ（smile.py）で決める。ingest_run_id（#123）はそのまま返し、#114 で作ったスナップショットかは
-- アプリ（smile.snapshot_source）で決める（option_price_latest は実行の選択で ingest_run_id を既に読むので、スキャン量は変わらない）
-- -- source:begin 〜 -- source:end は apps/viewer/tests/test_sql_bq.py が固定値の入力に差し替える印
WITH
-- source:begin
snap AS (
  SELECT type, target_price, last_trading_day, is_atm, iv, price, price_time, bid_iv, ask_iv, bid, ask, ingest_run_id
  FROM `${project}.${dataset}.option_price_latest`(@start_at, @end_at)
),
-- source:end
m AS (
  SELECT *, MIN(last_trading_day) OVER () AS l_min, MAX(last_trading_day) OVER () AS l_max FROM snap
)
SELECT
  month, type, target_price, last_trading_day, is_atm, iv, price,
  UNIX_SECONDS(TIMESTAMP(price_time, 'Asia/Tokyo')) AS price_time, bid_iv, ask_iv, bid, ask, ingest_run_id
FROM m CROSS JOIN UNNEST([1, 2]) AS month
WHERE last_trading_day = IF(month = 1, l_min, l_max)
ORDER BY month, target_price, type

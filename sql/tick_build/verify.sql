-- 過去データの行の作成（#114）の検証。東京で、[@start_at, @end_at) について 1 行を返す（build.sql の作り方は使わない独立の検査）。
-- 作った行は ingest_run_id が 'legacy-fill114-' で始まる行。ingest_runs は範囲の全体を、option_price・future_price は作ったスナップショットの
-- ある日（UTC の日のパーティション）だけを読む。作成の前にも流して基準値（legacy_runs・other_runs）を控え、作成の後に次がすべて 0 であることを見る:
--   out_of_hole_runs     作った実行のうち、スナップショットが埋める区間（旧系のスナップショットのあきが hole_gap_minutes 以上で、前の
--                        スナップショットが range_end より前）の中に無い（旧系のスナップショットと同じ時刻を含む）か、range_start 〜 range_end の外
--   run_field_errors     作った実行のうち、trigger・parser_version・source_object・started_at・finished_at・rows_spot・ingest_run_id の形が違う
--   run_count_errors     作った実行のうち、rows_option・rows_future が行の数と違う
--   orphan_rows          作った行（option・future）のうち、同じ ingest_run_id の実行が無い
--   row_field_errors     作った行のうち、parser_version・source_object が違う、created_at が実行の snapshot_at と違う、option の price・
--                        price_time・volume か future の price・price_time が NULL
--   iv_range_rows        作った option の行のうち、iv が 0 未満か 500 より大きい
--   future_trade_rows    作った行のうち、price_time が created_at（JST）の分より後（created_at までの約定だけを使う）
--   duplicate_rows       作った行のうち、同じスナップショットに同じキー（option は限月・種別・行使価格、future は 1 行）が 2 行以上
--   atm_errors           作ったスナップショット × 限月のうち、is_atm の行使価格が 2 つ以上
--   month_errors         作ったスナップショットのうち、限月が 3 つ以上
-- 参考（0 でなくてよい）: built_runs・built_option_rows・built_future_rows（作った実行・行）、built_iv_null_rows（IV が NULL の行）、
-- built_atm_rows、legacy_runs・other_runs（範囲の旧系の実行と、旧系でも作った行でもない実行。作成の前後で同じ）。
-- 名前は runbook の render が、範囲は bq のクエリパラメータが入れる。build_version・range_start・range_end・hole_gap_minutes は build.sql と同じ値。
-- -- source:begin 〜 -- source:end / -- result:begin 〜 -- result:end は tests/test_tick_build_bq.py が固定値の入力に差し替える印。
DECLARE start_at TIMESTAMP DEFAULT @start_at;
DECLARE end_at TIMESTAMP DEFAULT @end_at;
DECLARE build_version STRING DEFAULT 'legacy-fill114-1';
DECLARE range_start TIMESTAMP DEFAULT TIMESTAMP '2018-09-30 15:00:00+00';
DECLARE range_end TIMESTAMP DEFAULT TIMESTAMP '2024-08-06 03:25:00+00';
DECLARE hole_gap_minutes INT64 DEFAULT 15;
-- 作ったスナップショットのある日（option_price・future_price はこのパーティションだけを読む）
DECLARE built_days ARRAY<TIMESTAMP> DEFAULT (
  SELECT IFNULL(ARRAY_AGG(DISTINCT TIMESTAMP_TRUNC(snapshot_at, DAY)), [])
  FROM `${project}.${dataset}.ingest_runs`
  WHERE snapshot_at >= start_at AND snapshot_at < end_at AND STARTS_WITH(ingest_run_id, 'legacy-fill114-')
);

IF start_at IS NULL OR end_at IS NULL OR start_at >= end_at THEN
  SELECT ERROR(FORMAT('tick_build verify: invalid range [%t, %t)', start_at, end_at));
END IF;

-- result:begin
WITH
-- source:begin
runs AS (
  SELECT ingest_run_id, snapshot_at, source_object, parser_version, trigger, rows_spot, rows_future, rows_option, started_at, finished_at
  FROM `${project}.${dataset}.ingest_runs`
  WHERE snapshot_at >= TIMESTAMP_SUB(LEAST(start_at, range_start), INTERVAL 30 DAY)
    AND snapshot_at < TIMESTAMP_ADD(GREATEST(end_at, range_end), INTERVAL 30 DAY)
),
opt AS (
  SELECT created_at, last_trading_day, type, target_price, is_atm, price, price_time, volume, iv, ingest_run_id, parser_version,
    source_object
  FROM `${project}.${dataset}.option_price`
  WHERE created_at >= start_at AND created_at < end_at AND TIMESTAMP_TRUNC(created_at, DAY) IN UNNEST(built_days)
    AND STARTS_WITH(ingest_run_id, 'legacy-fill114-')
),
fut AS (
  SELECT created_at, contract_month, price, price_time, ingest_run_id, parser_version, source_object
  FROM `${project}.${dataset}.future_price`
  WHERE created_at >= start_at AND created_at < end_at AND TIMESTAMP_TRUNC(created_at, DAY) IN UNNEST(built_days)
    AND STARTS_WITH(ingest_run_id, 'legacy-fill114-')
),
-- source:end
legacy_snaps AS (
  SELECT DISTINCT snapshot_at FROM runs WHERE trigger = 'legacy_import'
),
gaps AS (
  SELECT prev_at, next_at
  FROM (SELECT LAG(snapshot_at) OVER (ORDER BY snapshot_at) AS prev_at, snapshot_at AS next_at FROM legacy_snaps)
  WHERE TIMESTAMP_DIFF(next_at, prev_at, MINUTE) >= hole_gap_minutes AND prev_at < range_end
),
built_runs AS (
  SELECT * FROM runs
  WHERE snapshot_at >= start_at AND snapshot_at < end_at AND STARTS_WITH(ingest_run_id, 'legacy-fill114-')
),
in_hole AS (
  SELECT r.ingest_run_id
  FROM built_runs AS r JOIN gaps AS g ON r.snapshot_at > g.prev_at AND r.snapshot_at < g.next_at
),
counts AS (
  SELECT ingest_run_id, COUNTIF(tbl = 'option') AS n_option, COUNTIF(tbl = 'future') AS n_future
  FROM (SELECT 'option' AS tbl, ingest_run_id FROM opt UNION ALL SELECT 'future', ingest_run_id FROM fut)
  GROUP BY ingest_run_id
),
rows_all AS (
  SELECT
    'option' AS tbl, created_at, FORMAT('%t|%t|%t', last_trading_day, type, target_price) AS k, price IS NULL OR price_time IS NULL
      OR volume IS NULL AS null_value, price_time, iv, ingest_run_id, parser_version, source_object
  FROM opt
  UNION ALL
  SELECT 'future', created_at, '', price IS NULL OR price_time IS NULL, price_time, NULL, ingest_run_id, parser_version, source_object
  FROM fut
),
row_checks AS (
  SELECT
    COUNTIF(r.snapshot_at IS NULL) AS orphan_rows,
    COUNTIF(r.snapshot_at IS NOT NULL AND (a.parser_version IS DISTINCT FROM build_version
      OR a.source_object IS DISTINCT FROM 'datacube:tick' OR a.created_at != r.snapshot_at OR a.null_value)) AS row_field_errors,
    COUNTIF(a.iv < 0 OR a.iv > 500) AS iv_range_rows,
    COUNTIF(a.price_time > DATETIME_SUB(DATETIME(a.created_at, 'Asia/Tokyo'), INTERVAL 1 MINUTE)) AS future_trade_rows
  FROM rows_all AS a
  LEFT JOIN built_runs AS r USING (ingest_run_id)
),
dups AS (
  SELECT COUNT(*) AS duplicate_rows
  FROM (SELECT tbl, created_at, k FROM rows_all GROUP BY tbl, created_at, k HAVING COUNT(*) > 1)
),
atm AS (
  SELECT COUNTIF(n_atm > 1) AS atm_errors
  FROM (SELECT created_at, last_trading_day, COUNT(DISTINCT IF(is_atm, target_price, NULL)) AS n_atm FROM opt GROUP BY 1, 2)
),
months AS (
  SELECT COUNTIF(n_months > 2) AS month_errors
  FROM (SELECT created_at, COUNT(DISTINCT last_trading_day) AS n_months FROM opt GROUP BY created_at)
)
SELECT
  start_at, end_at,
  (SELECT COUNT(*) FROM built_runs) AS built_runs,
  (SELECT COUNT(*) FROM opt) AS built_option_rows,
  (SELECT COUNT(*) FROM fut) AS built_future_rows,
  (SELECT COUNTIF(iv IS NULL) FROM opt) AS built_iv_null_rows,
  (SELECT COUNTIF(is_atm) FROM opt) AS built_atm_rows,
  (SELECT COUNT(*) FROM runs WHERE snapshot_at >= start_at AND snapshot_at < end_at AND trigger = 'legacy_import') AS legacy_runs,
  (SELECT COUNT(*) FROM runs WHERE snapshot_at >= start_at AND snapshot_at < end_at AND trigger != 'legacy_import'
     AND NOT STARTS_WITH(ingest_run_id, 'legacy-fill114-')) AS other_runs,
  (SELECT COUNT(*) FROM built_runs
   WHERE ingest_run_id NOT IN (SELECT ingest_run_id FROM in_hole) OR snapshot_at < range_start OR snapshot_at >= range_end)
    AS out_of_hole_runs,
  (SELECT COUNT(*) FROM built_runs
   WHERE trigger != 'legacy_fill114' OR parser_version != build_version OR source_object != 'datacube:tick'
     OR started_at != snapshot_at OR finished_at != snapshot_at OR rows_spot != 0
     OR ingest_run_id != FORMAT('legacy-fill114-%s', FORMAT_TIMESTAMP('%Y%m%dT%H%M%SZ', snapshot_at))) AS run_field_errors,
  (SELECT COUNT(*) FROM built_runs AS r LEFT JOIN counts AS c USING (ingest_run_id)
   WHERE r.rows_option != IFNULL(c.n_option, 0) OR r.rows_future != IFNULL(c.n_future, 0)) AS run_count_errors,
  c.orphan_rows, c.row_field_errors, c.iv_range_rows, c.future_trade_rows,
  d.duplicate_rows, t.atm_errors, m.month_errors
FROM row_checks AS c, dups AS d, atm AS t, months AS m
-- result:end
;

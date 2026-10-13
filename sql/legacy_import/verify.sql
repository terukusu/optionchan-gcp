-- 過去データの移行（#12、target.md 6 章 3a）の検証。東京で、created_at が [@start_at, @end_at) の範囲について 1 行を返す。
-- 移行の前にも流して基準値（new_rows_*、missing_snapshots = 移す予定の T の数）を控え、移行の後に次がすべて 0 であることを見る:
--   missing_snapshots     一時データセット ${import_dataset} の旧系の T のうち、ingest_runs に新旧どちらの記録も無いもの（移し漏れ）
--   overlap_snapshots     同じ T に legacy- の記録と新系の記録の両方がある（新旧の混在）
--   unknown_legacy_runs   legacy- の記録のうち、旧系に無い T のもの
--   bad_run_ids           legacy- の記録・行で、ingest_run_id が T から作る値（insert.sql と同じ書式）と違うもの
--   row_mismatches        legacy- の記録がある T ごと・表ごとに、旧系と移行行の（行数・XOR・合計）が違うもの（fingerprint.sql と同じ値）
--   orphan_rows           legacy- の行のうち、完了記録の無いもの（途中で失敗した残骸。insert.sql の流し直しで消える）
--   run_count_mismatches  legacy- の記録の rows_* と、その id の行の実数が違うもの
-- new_rows_*（legacy- 以外の行数）は移行の前後で変わらないこと（新系の行に触れていない）。
-- 名前は runbook の render が、期間は bq のクエリパラメータが入れる。
-- -- source:begin 〜 -- source:end / -- result:begin 〜 -- result:end は tests/test_legacy_import_bq.py が固定値の入力に差し替える印。
DECLARE start_at TIMESTAMP DEFAULT @start_at;
DECLARE end_at TIMESTAMP DEFAULT @end_at;

-- result:begin
WITH
-- source:begin
legacy_spot AS (
  SELECT price, price_time, diff, diff_rate, hv, created_at
  FROM `${project}.${import_dataset}.spot_price`
  WHERE created_at >= start_at AND created_at < end_at
),
legacy_future AS (
  SELECT price, price_time, diff, diff_rate, hv, contract_month, created_at
  FROM `${project}.${import_dataset}.future_price`
  WHERE created_at >= start_at AND created_at < end_at
),
legacy_option AS (
  SELECT
    type, target_price, is_atm, price, price_time, diff, diff_rate, iv,
    bid, bid_volume, bid_iv, ask, ask_volume, ask_iv, volume, positions,
    quotation, quotation_date, delta, gamma, theta, vega, last_trading_day, created_at
  FROM `${project}.${import_dataset}.option_price`
  WHERE created_at >= start_at AND created_at < end_at
),
runs AS (
  SELECT ingest_run_id, snapshot_at, rows_spot, rows_future, rows_option
  FROM `${project}.${dataset}.ingest_runs`
  WHERE snapshot_at >= start_at AND snapshot_at < end_at
),
tokyo_spot AS (
  SELECT price, price_time, diff, diff_rate, hv, created_at, ingest_run_id
  FROM `${project}.${dataset}.spot_price`
  WHERE created_at >= start_at AND created_at < end_at
),
tokyo_future AS (
  SELECT price, price_time, diff, diff_rate, hv, contract_month, created_at, ingest_run_id
  FROM `${project}.${dataset}.future_price`
  WHERE created_at >= start_at AND created_at < end_at
),
tokyo_option AS (
  SELECT
    type, target_price, is_atm, price, price_time, diff, diff_rate, iv,
    bid, bid_volume, bid_iv, ask, ask_volume, ask_iv, volume, positions,
    quotation, quotation_date, delta, gamma, theta, vega, last_trading_day, created_at, ingest_run_id
  FROM `${project}.${dataset}.option_price`
  WHERE created_at >= start_at AND created_at < end_at
),
-- source:end
-- 旧系・東京の行のフィンガープリント（列は fingerprint.sql と同じ。東京は追加列を除いた旧スキーマの列だけ）
legacy_fp AS (
  SELECT 'spot_price' AS tbl, created_at,
    FARM_FINGERPRINT(TO_JSON_STRING(STRUCT(price, price_time, diff, diff_rate, hv, created_at))) AS fp
  FROM legacy_spot
  UNION ALL
  SELECT 'future_price', created_at,
    FARM_FINGERPRINT(TO_JSON_STRING(STRUCT(price, price_time, diff, diff_rate, hv, contract_month, created_at)))
  FROM legacy_future
  UNION ALL
  SELECT 'option_price', created_at,
    FARM_FINGERPRINT(TO_JSON_STRING(STRUCT(
      type, target_price, is_atm, price, price_time, diff, diff_rate, iv,
      bid, bid_volume, bid_iv, ask, ask_volume, ask_iv, volume, positions,
      quotation, quotation_date, delta, gamma, theta, vega, last_trading_day, created_at)))
  FROM legacy_option
),
tokyo_rows AS (
  SELECT 'spot_price' AS tbl, created_at, ingest_run_id,
    FARM_FINGERPRINT(TO_JSON_STRING(STRUCT(price, price_time, diff, diff_rate, hv, created_at))) AS fp
  FROM tokyo_spot
  UNION ALL
  SELECT 'future_price', created_at, ingest_run_id,
    FARM_FINGERPRINT(TO_JSON_STRING(STRUCT(price, price_time, diff, diff_rate, hv, contract_month, created_at)))
  FROM tokyo_future
  UNION ALL
  SELECT 'option_price', created_at, ingest_run_id,
    FARM_FINGERPRINT(TO_JSON_STRING(STRUCT(
      type, target_price, is_atm, price, price_time, diff, diff_rate, iv,
      bid, bid_volume, bid_iv, ask, ask_volume, ask_iv, volume, positions,
      quotation, quotation_date, delta, gamma, theta, vega, last_trading_day, created_at)))
  FROM tokyo_option
),
legacy_snapshots AS (
  SELECT DISTINCT created_at FROM legacy_fp
),
legacy_runs AS (
  SELECT * FROM runs WHERE STARTS_WITH(ingest_run_id, 'legacy-')
),
new_runs AS (
  SELECT * FROM runs WHERE NOT STARTS_WITH(ingest_run_id, 'legacy-')
),
migrated_rows AS (
  SELECT * FROM tokyo_rows WHERE STARTS_WITH(ingest_run_id, 'legacy-')
),
-- (表, T) ごとの 3 値。旧系は legacy- の記録がある T だけ、東京は記録のある legacy- の行だけ（残骸は orphan_rows で数える）
legacy_agg AS (
  SELECT f.tbl, f.created_at, COUNT(*) AS n, BIT_XOR(f.fp) AS fp_xor, SUM(CAST(f.fp AS NUMERIC)) AS fp_sum
  FROM legacy_fp AS f
  WHERE f.created_at IN (SELECT snapshot_at FROM legacy_runs)
  GROUP BY f.tbl, f.created_at
),
tokyo_agg AS (
  SELECT m.tbl, m.created_at, COUNT(*) AS n, BIT_XOR(m.fp) AS fp_xor, SUM(CAST(m.fp AS NUMERIC)) AS fp_sum
  FROM migrated_rows AS m
  WHERE m.ingest_run_id IN (SELECT ingest_run_id FROM legacy_runs)
  GROUP BY m.tbl, m.created_at
),
mismatches AS (
  SELECT COALESCE(l.tbl, t.tbl) AS tbl, COALESCE(l.created_at, t.created_at) AS created_at
  FROM legacy_agg AS l
  FULL JOIN tokyo_agg AS t ON t.tbl = l.tbl AND t.created_at = l.created_at
  WHERE l.n IS NULL OR t.n IS NULL OR l.n != t.n OR l.fp_xor != t.fp_xor OR l.fp_sum != t.fp_sum
),
run_actual AS (
  SELECT
    r.ingest_run_id,
    r.rows_spot != COUNTIF(m.tbl = 'spot_price')
      OR r.rows_future != COUNTIF(m.tbl = 'future_price')
      OR r.rows_option != COUNTIF(m.tbl = 'option_price') AS mismatch
  FROM legacy_runs AS r
  LEFT JOIN migrated_rows AS m USING (ingest_run_id)
  GROUP BY r.ingest_run_id, r.rows_spot, r.rows_future, r.rows_option
)
SELECT
  start_at AS range_start,
  end_at AS range_end,
  (SELECT COUNT(*) FROM legacy_snapshots) AS import_snapshots,
  (SELECT COUNT(*) FROM legacy_runs) AS legacy_runs,
  (SELECT COUNT(DISTINCT snapshot_at) FROM new_runs) AS new_snapshots,
  (SELECT COUNT(*) FROM legacy_snapshots WHERE created_at NOT IN (SELECT snapshot_at FROM runs)) AS missing_snapshots,
  (SELECT COUNT(*) FROM legacy_runs WHERE snapshot_at IN (SELECT snapshot_at FROM new_runs)) AS overlap_snapshots,
  (SELECT COUNT(*) FROM legacy_runs WHERE snapshot_at NOT IN (SELECT created_at FROM legacy_snapshots)) AS unknown_legacy_runs,
  (
    (SELECT COUNTIF(ingest_run_id != FORMAT('legacy-%s', FORMAT_TIMESTAMP('%Y%m%dT%H%M%SZ', snapshot_at, 'UTC'))) FROM legacy_runs)
    + (SELECT COUNTIF(ingest_run_id != FORMAT('legacy-%s', FORMAT_TIMESTAMP('%Y%m%dT%H%M%SZ', created_at, 'UTC'))) FROM migrated_rows)
  ) AS bad_run_ids,
  (SELECT COUNT(*) FROM mismatches) AS row_mismatches,
  (SELECT COUNT(*) FROM migrated_rows WHERE ingest_run_id NOT IN (SELECT ingest_run_id FROM legacy_runs)) AS orphan_rows,
  (SELECT COUNTIF(mismatch) FROM run_actual) AS run_count_mismatches,
  (SELECT COUNTIF(tbl = 'spot_price') FROM migrated_rows) AS migrated_rows_spot,
  (SELECT COUNTIF(tbl = 'future_price') FROM migrated_rows) AS migrated_rows_future,
  (SELECT COUNTIF(tbl = 'option_price') FROM migrated_rows) AS migrated_rows_option,
  (SELECT COUNTIF(tbl = 'spot_price' AND NOT STARTS_WITH(IFNULL(ingest_run_id, ''), 'legacy-')) FROM tokyo_rows) AS new_rows_spot,
  (SELECT COUNTIF(tbl = 'future_price' AND NOT STARTS_WITH(IFNULL(ingest_run_id, ''), 'legacy-')) FROM tokyo_rows) AS new_rows_future,
  (SELECT COUNTIF(tbl = 'option_price' AND NOT STARTS_WITH(IFNULL(ingest_run_id, ''), 'legacy-')) FROM tokyo_rows) AS new_rows_option,
  (
    SELECT IFNULL(ARRAY_AGG(STRUCT(tbl, created_at) ORDER BY created_at, tbl LIMIT 20), [])
    FROM mismatches
  ) AS mismatch_samples
-- result:end
;

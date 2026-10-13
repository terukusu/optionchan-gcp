-- 過去データの補正（#4）の切り戻し。東京で、created_at が [@start_at, @end_at) の範囲の補正済みの行（parser_version = 版）を、
-- price_time_raw に残した元の値に戻し、price_time_raw・parser_version を NULL（移行の直後の状態、#12）に戻す。
-- 補正の印の無い行（新系の行・未補正の旧系の行）には触れない。流し直しても安全（2 回目は 0 行）。
-- 名前は runbook の render が、範囲は bq のクエリパラメータが入れる。fix_version は fix.sql・verify.sql と同じ値。
DECLARE start_at TIMESTAMP DEFAULT @start_at;
DECLARE end_at TIMESTAMP DEFAULT @end_at;
DECLARE fix_version STRING DEFAULT 'legacy-fix4-1';
DECLARE restored_spot INT64 DEFAULT 0;
DECLARE restored_future INT64 DEFAULT 0;
DECLARE restored_option INT64 DEFAULT 0;

IF start_at IS NULL OR end_at IS NULL OR start_at >= end_at THEN
  SELECT ERROR(FORMAT('price_time_fix rollback: invalid range [%t, %t)', start_at, end_at));
END IF;

UPDATE `${project}.${dataset}.spot_price`
SET price_time = SAFE.PARSE_DATETIME('%F %T', price_time_raw), price_time_raw = NULL, parser_version = NULL
WHERE created_at >= start_at AND created_at < end_at
  AND STARTS_WITH(ingest_run_id, 'legacy-') AND parser_version = fix_version
  AND SAFE.PARSE_DATETIME('%F %T', price_time_raw) IS NOT NULL;
SET restored_spot = @@row_count;

UPDATE `${project}.${dataset}.future_price`
SET price_time = SAFE.PARSE_DATETIME('%F %T', price_time_raw), price_time_raw = NULL, parser_version = NULL
WHERE created_at >= start_at AND created_at < end_at
  AND STARTS_WITH(ingest_run_id, 'legacy-') AND parser_version = fix_version
  AND SAFE.PARSE_DATETIME('%F %T', price_time_raw) IS NOT NULL;
SET restored_future = @@row_count;

UPDATE `${project}.${dataset}.option_price`
SET price_time = SAFE.PARSE_DATETIME('%F %T', price_time_raw), price_time_raw = NULL, parser_version = NULL
WHERE created_at >= start_at AND created_at < end_at
  AND STARTS_WITH(ingest_run_id, 'legacy-') AND parser_version = fix_version
  AND SAFE.PARSE_DATETIME('%F %T', price_time_raw) IS NOT NULL;
SET restored_option = @@row_count;

SELECT start_at, end_at, restored_spot, restored_future, restored_option;

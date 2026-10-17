-- 過去データの補完（#111）の切り戻し。東京で、created_at が [@start_at, @end_at) の補完済みの旧系の option の行（parser_version = 版）を、
-- 補完の前の状態（price・price_time・parser_version が NULL）に戻す。補完の対象は元がすべて NULL の行なので、印で正確に戻せる。
-- 補完の印の無い行（新系の行・未補完の旧系の行）には触れない。流し直しても安全（2 回目は 0 行）。
-- 名前は runbook の render が、範囲は bq のクエリパラメータが入れる。fill_version は fill.sql・verify.sql と同じ値。
DECLARE start_at TIMESTAMP DEFAULT @start_at;
DECLARE end_at TIMESTAMP DEFAULT @end_at;
DECLARE fill_version STRING DEFAULT 'legacy-fill111-1';
DECLARE restored_option INT64 DEFAULT 0;

IF start_at IS NULL OR end_at IS NULL OR start_at >= end_at THEN
  SELECT ERROR(FORMAT('tick_fill rollback: invalid range [%t, %t)', start_at, end_at));
END IF;

UPDATE `${project}.${dataset}.option_price`
SET price = NULL, price_time = NULL, parser_version = NULL
WHERE created_at >= start_at AND created_at < end_at
  AND STARTS_WITH(ingest_run_id, 'legacy-') AND parser_version = fill_version;
SET restored_option = @@row_count;

SELECT start_at, end_at, restored_option;

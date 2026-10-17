-- 過去データの行の作成（#114）の切り戻し。東京で、[@start_at, @end_at) の作った行（ingest_run_id が 'legacy-fill114-' で始まり、
-- parser_version = 版）を option_price・future_price・ingest_runs から消す（1 つのトランザクション）。作った行は元の表に無かった行なので、
-- 消せば作成の前と同じになる。印の無い行（旧系・新系・#111 などで補完した行）には触れない。流し直しても安全（2 回目は 0 行）。
-- option_price・future_price は、作ったスナップショットのある日（UTC の日のパーティション）だけを読む。
-- 名前は runbook の render が、範囲は bq のクエリパラメータが入れる。build_version は build.sql・verify.sql と同じ値。
DECLARE start_at TIMESTAMP DEFAULT @start_at;
DECLARE end_at TIMESTAMP DEFAULT @end_at;
DECLARE build_version STRING DEFAULT 'legacy-fill114-1';
DECLARE built_days ARRAY<TIMESTAMP> DEFAULT (
  SELECT IFNULL(ARRAY_AGG(DISTINCT TIMESTAMP_TRUNC(snapshot_at, DAY)), [])
  FROM `${project}.${dataset}.ingest_runs`
  WHERE snapshot_at >= start_at AND snapshot_at < end_at AND STARTS_WITH(ingest_run_id, 'legacy-fill114-')
);
DECLARE deleted_option INT64 DEFAULT 0;
DECLARE deleted_future INT64 DEFAULT 0;
DECLARE deleted_runs INT64 DEFAULT 0;

IF start_at IS NULL OR end_at IS NULL OR start_at >= end_at THEN
  SELECT ERROR(FORMAT('tick_build rollback: invalid range [%t, %t)', start_at, end_at));
END IF;

BEGIN TRANSACTION;

DELETE FROM `${project}.${dataset}.option_price`
WHERE created_at >= start_at AND created_at < end_at AND TIMESTAMP_TRUNC(created_at, DAY) IN UNNEST(built_days)
  AND STARTS_WITH(ingest_run_id, 'legacy-fill114-') AND parser_version = build_version;
SET deleted_option = @@row_count;

DELETE FROM `${project}.${dataset}.future_price`
WHERE created_at >= start_at AND created_at < end_at AND TIMESTAMP_TRUNC(created_at, DAY) IN UNNEST(built_days)
  AND STARTS_WITH(ingest_run_id, 'legacy-fill114-') AND parser_version = build_version;
SET deleted_future = @@row_count;

DELETE FROM `${project}.${dataset}.ingest_runs`
WHERE snapshot_at >= start_at AND snapshot_at < end_at
  AND STARTS_WITH(ingest_run_id, 'legacy-fill114-') AND parser_version = build_version AND trigger = 'legacy_fill114';
SET deleted_runs = @@row_count;

COMMIT TRANSACTION;

SELECT start_at, end_at, deleted_option, deleted_future, deleted_runs;

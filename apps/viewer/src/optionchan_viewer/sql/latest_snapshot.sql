-- /smile_data のクエリ 1: [@start_at, @end_at)（アプリが now − 14 日〜now を渡す）の完了したスナップショットのうち最新の時刻。
-- 無ければ NULL（アプリは 503 を返す）。ingest_runs は 1 分 1 行の小さい表
SELECT MAX(snapshot_at) AS snapshot_at
FROM `${project}.${dataset}.latest_runs`(@start_at, @end_at)

-- /smile_snapshot?dir=next のクエリ 1（#80）: [@start_at, @end_at)（アプリが at〜at + 14 日を渡す）の完了したスナップショットのうち
-- 最初の時刻。無ければ NULL（アプリは at 以前で最新を探す）。ingest_runs は 1 分 1 行の小さい表
SELECT MIN(snapshot_at) AS snapshot_at
FROM `${project}.${dataset}.latest_runs`(@start_at, @end_at)

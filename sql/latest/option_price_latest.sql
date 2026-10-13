-- option_price_latest(start_at TIMESTAMP, end_at TIMESTAMP): created_at が [start_at, end_at) の option_price のうち、
-- スナップショットごとに最新の完了した実行（latest_runs と同じ選び方）の行だけ。
-- ingest_runs に無い実行の行（書き込みの途中で失敗した残骸）は JOIN で落ちる（#34）。
-- created_at = ingest_runs.snapshot_at（1 限月ページの更新時刻）。テーブル関数の本文（プロジェクトとデータセットは Terraform の templatefile が入れる）
SELECT t.*
FROM `${project}.${dataset}.option_price` AS t
JOIN (
  SELECT ingest_run_id
  FROM `${project}.${dataset}.ingest_runs`
  WHERE snapshot_at >= start_at AND snapshot_at < end_at
  QUALIFY ROW_NUMBER() OVER (PARTITION BY snapshot_at ORDER BY finished_at DESC, ingest_run_id DESC) = 1
) AS r USING (ingest_run_id)
WHERE t.created_at >= start_at AND t.created_at < end_at

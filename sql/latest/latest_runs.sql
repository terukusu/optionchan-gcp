-- latest_runs(start_at TIMESTAMP, end_at TIMESTAMP): snapshot_at が [start_at, end_at) の完了した実行（ingest_runs）のうち、
-- スナップショットごとに最新の 1 件（finished_at が最も新しいもの。同時刻なら ingest_run_id の大きい方）。
-- 回収（trigger = recovery）・再処理（reprocess）は同じスナップショットを新しい ingest_run_id で追記するので（#34）、
-- 表示・集計・比較はこの関数（と *_latest）で 1 つに選ぶ。テーブル関数の本文（プロジェクトとデータセットは Terraform の templatefile が入れる）
SELECT r.*
FROM `${project}.${dataset}.ingest_runs` AS r
WHERE r.snapshot_at >= start_at AND r.snapshot_at < end_at
QUALIFY ROW_NUMBER() OVER (PARTITION BY r.snapshot_at ORDER BY r.finished_at DESC, r.ingest_run_id DESC) = 1

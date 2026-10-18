# 過去データの欠けの一覧（#123）。sql/gaps/gaps.sql（#110）の結果を、運用者が runbook「欠けの一覧の表 data_gaps（#123）」の手順で
# bq load --replace で入れる（一度きり。旧系は止めていて過去の旧系の行はもう増えない）。ビューアーの /data_gaps が全体を読む（数百行）。
# 取り込みのテーブルではないので tables.json には入れない。列の正は schema/tokyo/data_gaps.json。gaps.sql から作り直せるので
# 削除は止めない。付与の追加は無い（sa-viewer はデータセット単位の reader。iam.tf の viewer_reader）
resource "google_bigquery_table" "data_gaps" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.tokyo.dataset_id
  table_id            = "data_gaps"
  description         = "過去データの欠けの一覧（#123。sql/gaps/gaps.sql の結果。runbook の手順で bq load --replace）"
  schema              = file("${path.module}/../../schema/tokyo/data_gaps.json")
  deletion_protection = false
}

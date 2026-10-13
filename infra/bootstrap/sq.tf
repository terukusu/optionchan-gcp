# 定期クエリ（BigQuery Data Transfer Service。日次の品質チェックと新旧の突き合わせ、#40）の実行用 SA と、
# プロジェクト単位・旧データセット・SA 単位の付与。prod が作るリソース（データセット・結果のテーブル・DTS）への付与は prod の sq.tf。
# bigquery.admin は付けない（DTS の作成・更新は transfers の権限だけのカスタムロールにする）。

# target.md 3.5 の sa-sq。SA の ID は 6 文字以上なので sa-sq-dts
resource "google_service_account" "sq" {
  project         = var.project_id
  account_id      = "sa-sq-dts"
  display_name    = "scheduled queries (BigQuery DTS)"
  description     = "日次の品質チェックと新旧の突き合わせ（BigQuery Data Transfer Service）の実行用"
  deletion_policy = "PREVENT"

  depends_on = [google_project_service.this]
}

# クエリのジョブを作る（US の作業用テーブルの作成と、東京の品質チェック・突き合わせ）。プロジェクト単位でしか付けられない
resource "google_project_iam_member" "sq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = google_service_account.sq.member
}

# 旧系（US のデータセット optionchan）の前日分を読む。旧系への変更はこの 3 件の読み取りだけ（データセットの ACL は変えない）。
# 旧系の停止（#14）の後に外す
resource "google_bigquery_table_iam_member" "sq_legacy_reader" {
  for_each = toset(["spot_price", "future_price", "option_price"])

  project    = var.project_id
  dataset_id = "optionchan"
  table_id   = each.value
  role       = "roles/bigquery.dataViewer"
  member     = google_service_account.sq.member
}

# sa-tf-apply が DTS の実行 SA に sa-sq-dts を設定できるように（SA 単位）
resource "google_service_account_iam_member" "tf_apply_act_as_sq" {
  service_account_id = google_service_account.sq.name
  role               = "roles/iam.serviceAccountUser"
  member             = google_service_account.tf_apply.member
}

# DTS の設定の作成・更新・削除（transfers.get は sa-tf-apply の bigquery.user にも含まれる）。
# 公式の例は bigquery.admin だが、データの削除まで含むので付けない
resource "google_project_iam_custom_role" "bigquery_transfer_editor" {
  project     = var.project_id
  role_id     = "bigqueryTransferEditor"
  title       = "BigQuery transfer editor (optionchan)"
  description = "BigQuery Data Transfer Service の設定の作成・更新・削除だけ（#40）"
  permissions = [
    "bigquery.transfers.get",
    "bigquery.transfers.update",
  ]
}

resource "google_project_iam_custom_role" "bigquery_transfer_viewer" {
  project     = var.project_id
  role_id     = "bigqueryTransferViewer"
  title       = "BigQuery transfer viewer (optionchan)"
  description = "plan 用に BigQuery Data Transfer Service の設定を読むだけ（#40）"
  permissions = [
    "bigquery.transfers.get",
  ]
}

resource "google_project_iam_member" "tf_apply_bigquery_transfer" {
  project = var.project_id
  role    = google_project_iam_custom_role.bigquery_transfer_editor.id
  member  = google_service_account.tf_apply.member
}

resource "google_project_iam_member" "tf_plan_bigquery_transfer" {
  project = var.project_id
  role    = google_project_iam_custom_role.bigquery_transfer_viewer.id
  member  = google_service_account.tf_plan.member
}

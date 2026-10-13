# 取引カレンダーの TOML を BigQuery の最新版に合わせる PR（#66。workflow calendar-toml）の読み取り用 SA。
# 付けるのはクエリのジョブ（bigquery.jobUser。プロジェクト単位でしか付けられない）と、prod の calendar.tf の
# trading_calendar・trading_calendar_latest のテーブル単位の dataViewer だけ（書き込み権限は持たない）。
# 借用は master の ref で動く job だけ（PR のブランチ・Environment 付きの job からは借用できない）。

resource "google_service_account" "calendar_toml" {
  project         = var.project_id
  account_id      = "sa-calendar-toml"
  display_name    = "calendar TOML PR (GitHub Actions)"
  description     = "workflow calendar-toml（#66）が取引カレンダーの最新版を読む（読み取りのみ）"
  deletion_policy = "PREVENT"

  depends_on = [google_project_service.this]
}

resource "google_project_iam_member" "calendar_toml_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = google_service_account.calendar_toml.member
}

resource "google_service_account_iam_member" "calendar_toml_wif" {
  service_account_id = google_service_account.calendar_toml.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principal://iam.googleapis.com/${local.pool_name}/subject/repo:${var.github_repository}:ref:refs/heads/master"
}

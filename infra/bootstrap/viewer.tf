# 新しいビューアー（Cloud Run viewer、#53）の実行用 SA と、プロジェクト単位・SA 単位・IAP の付与。
# prod が作るリソースへの付与（データセット optionchan_tokyo の dataViewer、サービスの run.invoker = IAP のサービスエージェント）は
# prod の iam.tf / viewer.tf。IAP のアクセス許可は roles/iap.admin が要るので sa-tf-apply には付けず、ここ（所有者の apply）に置く。

resource "google_service_account" "viewer" {
  project         = var.project_id
  account_id      = "sa-viewer"
  display_name    = "viewer (Cloud Run)"
  description     = "ビューアー（Cloud Run viewer）の実行用。optionchan_tokyo の読み取りだけ"
  deletion_policy = "PREVENT"

  depends_on = [google_project_service.this]
}

# クエリのジョブを作る（プロジェクト単位でしか付けられない）。データの読み取りは prod の optionchan_tokyo の dataViewer
# （旧データセット optionchan には何も付けない）
resource "google_project_iam_member" "viewer_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = google_service_account.viewer.member
}

# sa-tf-apply が Cloud Run viewer に sa-viewer を設定できるように（SA 単位）
resource "google_service_account_iam_member" "tf_apply_act_as_viewer" {
  service_account_id = google_service_account.viewer.name
  role               = "roles/iam.serviceAccountUser"
  member             = google_service_account.tf_apply.member
}

# IAP 越しにサービス viewer を開けるユーザー（サービス単位。プロジェクト単位にしない）。
# サービス（prod）が無いと付けられないので、prod の apply の後に var.viewer_iap_members を入れて apply する（runbook「ビューアー（#13）」）。
# 組織の無いプロジェクトでは、先にカスタム OAuth クライアントを IAP に設定しておく（所有者の手作業。シークレットは Terraform に入れない）
resource "google_iap_web_cloud_run_service_iam_member" "viewer_access" {
  for_each = toset(var.viewer_iap_members)

  project                = var.project_id
  location               = var.region
  cloud_run_service_name = "viewer"
  role                   = "roles/iap.httpsResourceAccessor"
  member                 = each.value
}

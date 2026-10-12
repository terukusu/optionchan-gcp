# Terraform 用 SA。付与は追加型（*_iam_member）だけを使う（authoritative 型は既存の binding を消す）。
# basic の roles/viewer は使わない: UBLA バケットの既定 IAM で projectViewer が legacy の読み取りを持ち、
# bootstrap の state まで読めてしまうため。prod が扱うリソースの predefined ロールを個別に付ける。
# projectIamAdmin は付けない（#33。オーナー承認）。プロジェクト単位・バケット単位・SA 単位の付与はこの bootstrap に置き、
# prod が作るリソース（データセット optionchan_tokyo・Cloud Run ingest）への付与だけを prod の iam.tf に置く。
# sa-tf-apply がデータセットの IAM を変えられるのは、bigquery.user で作成した作成者（dataOwner）になるため。
# serviceAccountUser はプロジェクト単位にせず、ingest.tf で sa-ingest / sa-scheduler の SA 単位に付ける。

resource "google_service_account" "tf_plan" {
  project         = var.project_id
  account_id      = "sa-tf-plan"
  display_name    = "Terraform plan (GitHub Actions)"
  description     = "PR と master の terraform plan 用（読み取りのみ）"
  deletion_policy = "PREVENT"

  depends_on = [google_project_service.this]
}

resource "google_service_account" "tf_apply" {
  project         = var.project_id
  account_id      = "sa-tf-apply"
  display_name    = "Terraform apply (GitHub Actions)"
  description     = "Environment prod の承認後の terraform apply 用"
  deletion_policy = "PREVENT"

  depends_on = [google_project_service.this]
}

locals {
  pool_name = google_iam_workload_identity_pool.github.name

  plan_project_roles = toset([
    "roles/monitoring.viewer",
    "roles/artifactregistry.reader",
    "roles/run.viewer",
    "roles/cloudscheduler.viewer",
    "roles/bigquery.metadataViewer",
  ])

  # run.admin / cloudscheduler.admin はプロジェクト単位（Scheduler にジョブ単位の IAM が無い）。使うのは承認付きの apply だけ
  apply_project_roles = toset([
    "roles/artifactregistry.admin",
    "roles/monitoring.alertPolicyEditor",
    "roles/monitoring.notificationChannelViewer",
    "roles/run.admin",
    "roles/cloudscheduler.admin",
    "roles/bigquery.user",
  ])
}

# plan SA: このリポジトリのどの workflow からも借用できる（PR の plan 用）
resource "google_service_account_iam_member" "tf_plan_wif" {
  service_account_id = google_service_account.tf_plan.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${local.pool_name}/attribute.repository_id/${var.github_repository_id}"
}

# apply SA: Environment prod の job だけが借用できる（Environment は master のみ・承認必須）
resource "google_service_account_iam_member" "tf_apply_wif" {
  service_account_id = google_service_account.tf_apply.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principal://iam.googleapis.com/${local.pool_name}/subject/repo:${var.github_repository}:environment:prod"
}

resource "google_project_iam_member" "tf_plan" {
  for_each = local.plan_project_roles

  project = var.project_id
  role    = each.value
  member  = google_service_account.tf_plan.member
}

resource "google_project_iam_member" "tf_apply" {
  for_each = local.apply_project_roles

  project = var.project_id
  role    = each.value
  member  = google_service_account.tf_apply.member
}

# prod の state: plan は読み取り（plan は -lock=false で回す）、apply は state とロックの読み書き。
# bootstrap 用バケットには CI 用 SA の binding を付けない。
resource "google_storage_bucket_iam_member" "prod_state_plan" {
  bucket = google_storage_bucket.tfstate["prod"].name
  role   = "roles/storage.objectViewer"
  member = google_service_account.tf_plan.member
}

resource "google_storage_bucket_iam_member" "prod_state_apply" {
  bucket = google_storage_bucket.tfstate["prod"].name
  role   = "roles/storage.objectUser"
  member = google_service_account.tf_apply.member
}

# 新しい取り込み（Cloud Run ingest、#33）の実行用 SA・生 HTML のバケットと、それらへの付与。
# prod が作るリソース（データセット・サービス）への付与は prod の iam.tf。sa-ingest の bigquery.jobUser は 11c（#34）で足す。

resource "google_service_account" "ingest" {
  project         = var.project_id
  account_id      = "sa-ingest"
  display_name    = "ingest (Cloud Run)"
  description     = "取り込み（Cloud Run ingest）の実行用。optionchan_tokyo への追記と生 HTML の保存だけ"
  deletion_policy = "PREVENT"

  depends_on = [google_project_service.this]
}

resource "google_service_account" "scheduler" {
  project         = var.project_id
  account_id      = "sa-scheduler"
  display_name    = "Cloud Scheduler (OIDC)"
  description     = "Scheduler ジョブ ingest が Cloud Run を呼ぶときの OIDC トークン用"
  deletion_policy = "PREVENT"

  depends_on = [google_project_service.this]
}

# 生 HTML（JPX のページ）の保存先。上書きは ifGenerationMatch=0 と権限（削除・上書きなし）で防ぐので、
# versioning は使わない。期限（lifecycle）・保持ポリシーも付けない（後から付けられるが、保持ポリシーのロックは戻せない）。
resource "google_storage_bucket" "raw" {
  name                        = "${var.project_id}-raw"
  project                     = var.project_id
  location                    = "ASIA-NORTHEAST1"
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false
  deletion_policy             = "PREVENT"

  soft_delete_policy {
    retention_duration_seconds = 604800
  }

  lifecycle {
    prevent_destroy = true
  }
}

# objectAdmin / objectUser は削除・上書きを含むので使わない
resource "google_storage_bucket_iam_member" "raw_ingest" {
  for_each = toset([
    "roles/storage.objectCreator",
    "roles/storage.objectViewer",
  ])

  bucket = google_storage_bucket.raw.name
  role   = each.value
  member = google_service_account.ingest.member
}

# Scheduler のサービスエージェントが OIDC トークンを発行するのに必要（#19 の調査で未付与を確認）。
# プロジェクト番号はコードに書かず data source から組み立てる（bootstrap の state は非公開バケット）。
data "google_project" "this" {
  project_id = var.project_id
}

resource "google_project_iam_member" "scheduler_agent" {
  project = var.project_id
  role    = "roles/cloudscheduler.serviceAgent"
  member  = "serviceAccount:service-${data.google_project.this.number}@gcp-sa-cloudscheduler.iam.gserviceaccount.com"
}

# sa-tf-apply が Cloud Run（sa-ingest）と Scheduler（sa-scheduler）にこの SA を設定できるように。プロジェクト単位にしない
resource "google_service_account_iam_member" "tf_apply_act_as" {
  for_each = {
    ingest    = google_service_account.ingest.name
    scheduler = google_service_account.scheduler.name
  }

  service_account_id = each.value
  role               = "roles/iam.serviceAccountUser"
  member             = google_service_account.tf_apply.member
}

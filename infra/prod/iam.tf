# prod が作るリソースへの付与だけを置く（#33）。プロジェクト単位・バケット単位・SA 単位の付与は infra/bootstrap。
# sa-tf-apply は projectIamAdmin を持たない。データセットは作成者（dataOwner）として、サービスは run.admin で付与する。
locals {
  sa_ingest    = "sa-ingest@${var.project_id}.iam.gserviceaccount.com"
  sa_scheduler = "sa-scheduler@${var.project_id}.iam.gserviceaccount.com"
}

# 取り込みの追記（Storage Write API）と、回収・照合・再処理での ingest_runs の読み取り（dataEditor は読み取りを含む）。
# optionchan_tokyo だけで、旧データセット optionchan には付けない。
# クエリのジョブを作る bigquery.jobUser はプロジェクト単位なので infra/bootstrap/ingest.tf（#34）
resource "google_bigquery_dataset_iam_member" "ingest_editor" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.tokyo.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${local.sa_ingest}"
}

# Scheduler の OIDC トークンでだけ呼べるようにする（allUsers は付けない）
resource "google_cloud_run_v2_service_iam_member" "scheduler_invoker" {
  project  = var.project_id
  location = google_cloud_run_v2_service.ingest.location
  name     = google_cloud_run_v2_service.ingest.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${local.sa_scheduler}"
}

# ビューアー（#53）の読み取り。optionchan_tokyo だけで、旧データセット optionchan には付けない。
# クエリのジョブを作る bigquery.jobUser はプロジェクト単位なので infra/bootstrap/viewer.tf
resource "google_bigquery_dataset_iam_member" "viewer_reader" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.tokyo.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${local.sa_viewer}"
}

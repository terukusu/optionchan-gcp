# 新しいビューアー（#53、アプリは apps/viewer #52）: IAP（Cloud Run への直接設定）の後ろにだけ置く。
# アプリには認証も回数制限も無い（共通パスワードもやめた）。IAP を外したり allUsers に run.invoker を付けたりして公開しない
# （誰でも BigQuery のクエリを回せて課金が増える。runbook「ビューアー（#13）」）。
# SA sa-viewer・jobUser・IAP のアクセス許可（roles/iap.httpsResourceAccessor）は infra/bootstrap/viewer.tf（所有者が apply）。
# IAP の OAuth クライアント（組織の無いプロジェクトでは必須）は所有者の手作業で、シークレットを state に入れない。
locals {
  sa_viewer = "sa-viewer@${var.project_id}.iam.gserviceaccount.com"
}

# IAP のサービスエージェント（service-<番号>@gcp-sa-iap）。番号はコードに書かず data source から組み立てる
data "google_project" "this" {
  project_id = var.project_id
}

# ingress は all（IAP は run.app の URL で効く。ロードバランサは使わない）。IAM の検査は無効にしない。
# 第1世代（起動が速い）・CPU 1・リクエスト中だけ CPU。同時実行 8 は gunicorn の threads 8、タイムアウト 60 秒は gunicorn と同じ
# （BigQuery の結果待ちは 30 秒）。deletion_protection = false: データを持たないので、切り戻しを 1 つの PR で行えるようにする
resource "google_cloud_run_v2_service" "viewer" {
  project              = var.project_id
  name                 = "viewer"
  location             = var.region
  description          = "日経225オプションのビューアー（IAP の後ろ。optionchan_tokyo を読む）"
  ingress              = "INGRESS_TRAFFIC_ALL"
  iap_enabled          = true
  invoker_iam_disabled = false
  deletion_protection  = false

  template {
    service_account                  = local.sa_viewer
    execution_environment            = "EXECUTION_ENVIRONMENT_GEN1"
    max_instance_request_concurrency = 8
    timeout                          = "60s"

    scaling {
      min_instance_count = 0
      max_instance_count = 2
    }

    containers {
      image = var.viewer_image

      resources {
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
        cpu_idle          = true
        startup_cpu_boost = true
      }

      env {
        name  = "GCP_PROJECT"
        value = var.project_id
      }
      env {
        name  = "BQ_DATASET"
        value = google_bigquery_dataset.tokyo.dataset_id
      }
      env {
        name  = "BQ_LOCATION"
        value = google_bigquery_dataset.tokyo.location
      }
    }
  }

  # 読み取りの権限より先にリクエストが来ないように。/data_gaps（#123）が読む表も先に作る（無い表を読んで 500 にしない）
  depends_on = [google_bigquery_dataset_iam_member.viewer_reader, google_bigquery_table.data_gaps]
}

# IAP からだけ呼べるようにする（allUsers・allAuthenticatedUsers は付けない）。
# サービスエージェントは所有者が先に作っておく（gcloud beta services identity create --service=iap.googleapis.com）
resource "google_cloud_run_v2_service_iam_member" "viewer_iap_invoker" {
  project  = var.project_id
  location = google_cloud_run_v2_service.viewer.location
  name     = google_cloud_run_v2_service.viewer.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:service-${data.google_project.this.number}@gcp-sa-iap.iam.gserviceaccount.com"
}

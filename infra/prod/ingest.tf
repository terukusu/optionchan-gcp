# 新しい取り込み（#33）: Cloud Scheduler が毎分 Cloud Run ingest に POST / する。
# SA・生 HTML のバケットは infra/bootstrap/ingest.tf（所有者が先に apply する）。
locals {
  raw_bucket = "${var.project_id}-raw"
}

# 第1世代・CPU 0.5・同時実行 1（1 vCPU 未満の条件）。リクエスト中だけ CPU（cpu_idle）でリクエスト課金。
# タイムアウト 50 秒はアプリの期限 45 秒・gunicorn の 50 秒と揃え、毎分の起動と重ならないようにする。
# deletion_protection = false: データを持たないので、切り戻し（runbook R3）を 1 つの PR で行えるようにする
resource "google_cloud_run_v2_service" "ingest" {
  project              = var.project_id
  name                 = "ingest"
  location             = var.region
  description          = "日経225オプションの取り込み（Scheduler ingest から毎分 POST /）"
  ingress              = "INGRESS_TRAFFIC_INTERNAL_ONLY"
  invoker_iam_disabled = false
  deletion_protection  = false

  template {
    service_account                  = local.sa_ingest
    execution_environment            = "EXECUTION_ENVIRONMENT_GEN1"
    max_instance_request_concurrency = 1
    timeout                          = "50s"

    scaling {
      min_instance_count = 0
      max_instance_count = 2
    }

    containers {
      image = var.ingest_image

      resources {
        limits = {
          cpu    = "0.5"
          memory = "512Mi"
        }
        cpu_idle          = true
        startup_cpu_boost = false
      }

      # INGEST_DRY_RUN / INGEST_FAKE_NOW は設定しない（テスト用）
      env {
        name  = "GCP_PROJECT"
        value = var.project_id
      }
      env {
        name  = "RAW_BUCKET"
        value = local.raw_bucket
      }
      env {
        name  = "BQ_DATASET"
        value = google_bigquery_dataset.tokyo.dataset_id
      }
    }
  }

  # テーブルより先にリクエストが来ないように
  depends_on = [google_bigquery_table.this]
}

# Scheduler の無料枠（3 ジョブ）は旧系で使い切っているので 1 本だけ（target.md 3.1）。
# リトライ 0（次の分に任せる。回収は毎時 0 分、#34）、attempt_deadline はサービスのタイムアウトと同じ 50 秒
resource "google_cloud_scheduler_job" "ingest" {
  project          = var.project_id
  name             = "ingest"
  region           = var.region
  description      = "Cloud Run ingest を毎分起動する（時間外はアプリが 204 で何もしない）"
  schedule         = "* * * * *"
  time_zone        = "Asia/Tokyo"
  attempt_deadline = "50s"
  paused           = var.ingest_paused

  # リトライ 0 は API の既定値。retry_config を書くと API が空で返して毎回差分になるので書かない（#38）

  http_target {
    http_method = "POST"
    uri         = "${google_cloud_run_v2_service.ingest.uri}/"

    oidc_token {
      service_account_email = local.sa_scheduler
      audience              = google_cloud_run_v2_service.ingest.uri
    }
  }

  # 付与の後に最初の起動が来るように
  depends_on = [
    google_cloud_run_v2_service_iam_member.scheduler_invoker,
    google_bigquery_dataset_iam_member.ingest_editor,
  ]
}

# 再処理ジョブ（#34、target.md 3.1）: 保存済みの生 HTML から期間を指定して作り直す（追記のみ、JPX にアクセスしない）。
# 手動で実行する（定期起動しない）: gcloud run jobs execute reprocess --region ... --args=--start=...,--end=... --wait
# 既定の args は付けない（引数なしの実行は終了コード 2 で何もしない）。権限は sa-ingest と共用（target.md 3.5）。
# タスクのタイムアウト 3600 秒（アプリは settings.REPROCESS_DEADLINE_SECONDS = 3300 秒で止めて残りを出す）、リトライ 0。
# deletion_protection = false: データを持たないので、切り戻しを 1 つの PR で行えるようにする
resource "google_cloud_run_v2_job" "reprocess" {
  project             = var.project_id
  name                = "reprocess"
  location            = var.region
  deletion_protection = false

  template {
    task_count  = 1
    parallelism = 1

    template {
      service_account = local.sa_ingest
      timeout         = "3600s"
      max_retries     = 0

      containers {
        image   = var.ingest_image
        command = ["python", "-m", "optionchan_ingest.reprocess"]

        resources {
          limits = {
            cpu    = "1"
            memory = "512Mi"
          }
        }

        env {
          name  = "GCP_PROJECT"
          value = var.project_id
        }
        env {
          name  = "RAW_BUCKET"
          value = local.raw_bucket
        }
        env {
          name  = "BQ_DATASET"
          value = google_bigquery_dataset.tokyo.dataset_id
        }
      }
    }
  }

  depends_on = [google_bigquery_table.this]
}

# JSCC の VaR 証拠金計算用パラメータファイル（日経225 分）の毎日の取り込み（#164）。
# 平日 18:30 JST に Scheduler がジョブ jscc-sync を起動し、1600 版の BPF_IDX・VaRParameter・All-Instruments-VaR から
# NK225 の銘柄のシナリオの値を営業日のパーティションごと置き換えで書く（apps/ingest/src/optionchan_ingest/jscc_sync.py）。
# 書くのは sa-ingest（optionchan_tokyo の dataEditor と、プロジェクトの bigquery.jobUser。iam.tf・infra/bootstrap）なので
# データの付与の追加は無い。足す付与は sa-scheduler がこのジョブだけを起動できる run.invoker 1 件。
# 表は JSCC のファイルの中身なので非公開のデータセットに置くだけ（サイト利用案内は再配布を禁じる）。
# 列の正は schema/tokyo/jscc_*.json（取り込みのテーブルではないので tables.json には入れない）。
# データを持つ 4 表は削除を二重に止める（取り込みは置き換えなので、壊れた日は流し直しで直る）

# 銘柄ごとのシナリオの値（1 行 = 営業日 × 版 × 銘柄。1 日 約 1.2 万行・論理 約 140MB）。
# 読み手（option-simulator）は営業日と限月・行使価格・C/P で銘柄を選ぶので、銘柄の属性でクラスタする
resource "google_bigquery_table" "jscc_var_scenario" {
  project                  = var.project_id
  dataset_id               = google_bigquery_dataset.tokyo.dataset_id
  table_id                 = "jscc_var_scenario"
  description              = "JSCC の VaR 証拠金計算用パラメータファイル（BPF_IDX 1600 版）の NK225 の銘柄ごとのシナリオの値（#164。非公開）"
  schema                   = file("${path.module}/../../schema/tokyo/jscc_var_scenario.json")
  clustering               = ["product_code", "contract_period_code", "put_call_type", "strike_price"]
  require_partition_filter = true
  deletion_protection      = true
  deletion_policy          = "PREVENT"

  time_partitioning {
    type  = "DAY"
    field = "business_date"
  }

  lifecycle {
    prevent_destroy = true
  }
}

# シナリオの並び（1 行 = 営業日 × 版。scenarios の添字が jscc_var_scenario.pnl の添字）
resource "google_bigquery_table" "jscc_var_scenario_set" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.tokyo.dataset_id
  table_id            = "jscc_var_scenario_set"
  description         = "JSCC の BPF_IDX のシナリオの並び（#164。H = ヒストリカル / S = ストレス）"
  schema              = file("${path.module}/../../schema/tokyo/jscc_var_scenario_set.json")
  deletion_protection = true
  deletion_policy     = "PREVENT"

  time_partitioning {
    type  = "DAY"
    field = "business_date"
  }

  lifecycle {
    prevent_destroy = true
  }
}

# VaRParameter の全行（信頼水準・ストレスの本数・相殺の係数。1 日 約 60 行）
resource "google_bigquery_table" "jscc_var_parameter" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.tokyo.dataset_id
  table_id            = "jscc_var_parameter"
  description         = "JSCC の VaRParameter（#164。Aggregation Group ごとの信頼水準・ストレスの本数・相殺の係数）"
  schema              = file("${path.module}/../../schema/tokyo/jscc_var_parameter.json")
  deletion_protection = true
  deletion_policy     = "PREVENT"

  time_partitioning {
    type  = "DAY"
    field = "business_date"
  }

  lifecycle {
    prevent_destroy = true
  }
}

# 取り込みの記録（追記のみ）。読み手は status = succeeded の日だけを使う（3 表は別々のロード）
resource "google_bigquery_table" "jscc_ingest_runs" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.tokyo.dataset_id
  table_id            = "jscc_ingest_runs"
  description         = "JSCC の VaR パラメータの取り込みの記録（#164。営業日ごとの成功・失敗・行数・All-Instruments-VaR との照合の件数）"
  schema              = file("${path.module}/../../schema/tokyo/jscc_ingest_runs.json")
  deletion_protection = true
  deletion_policy     = "PREVENT"

  time_partitioning {
    type  = "DAY"
    field = "business_date"
  }

  lifecycle {
    prevent_destroy = true
  }
}

# 取り込みのジョブ。引数なし = 未取り込みの日（settings.JSCC_START_DATE 以降、1 回最大 5 日）、
# --date=YYYY-MM-DD = 指定の日を取り直す、--dry-run = 書かない。権限は sa-ingest と共用。
# タスクのタイムアウト 3600 秒（アプリは settings.JSCC_JOB_DEADLINE_SECONDS = 3300 秒で止める）、リトライ 0（翌平日の実行が
# archive から取り直す）。zip（約 120MB）と NDJSON の一時ファイルがメモリ（/tmp）に乗るので 1Gi。
# deletion_protection = false: データを持たない
resource "google_cloud_run_v2_job" "jscc_sync" {
  project             = var.project_id
  name                = "jscc-sync"
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
        command = ["python", "-m", "optionchan_ingest.jscc_sync"]

        resources {
          limits = {
            cpu    = "1"
            memory = "1Gi"
          }
        }

        env {
          name  = "GCP_PROJECT"
          value = var.project_id
        }
        env {
          name  = "BQ_DATASET"
          value = google_bigquery_dataset.tokyo.dataset_id
        }
      }
    }
  }

  depends_on = [
    google_bigquery_table.jscc_var_scenario,
    google_bigquery_table.jscc_var_scenario_set,
    google_bigquery_table.jscc_var_parameter,
    google_bigquery_table.jscc_ingest_runs,
  ]
}

# sa-scheduler がこのジョブだけを起動できる（run.invoker は run.jobs.run を含む）
resource "google_cloud_run_v2_job_iam_member" "jscc_sync_scheduler_invoker" {
  project  = var.project_id
  location = google_cloud_run_v2_job.jscc_sync.location
  name     = google_cloud_run_v2_job.jscc_sync.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${local.sa_scheduler}"
}

# 平日 18:30 JST に起動（一覧の更新は 16:12 ごろまで。祝日はファイルが無く「対象なし」で正常終了する）。
# Cloud Run Admin API（v2）の jobs.run を OAuth トークンで呼ぶ。失敗はログ（新系の M2 が Scheduler の失敗も拾う）。
# リトライ 0 は API の既定値（retry_config を書くと API が空で返して毎回差分になる。#38）。翌平日に取り直す
resource "google_cloud_scheduler_job" "jscc_sync" {
  project          = var.project_id
  name             = "jscc-sync"
  region           = var.region
  description      = "Cloud Run ジョブ jscc-sync を平日 18:30 JST に起動する（JSCC の VaR パラメータの取り込み、#164）"
  schedule         = "30 18 * * 1-5"
  time_zone        = "Asia/Tokyo"
  attempt_deadline = "180s"

  http_target {
    http_method = "POST"
    uri         = "https://run.googleapis.com/v2/projects/${var.project_id}/locations/${var.region}/jobs/${google_cloud_run_v2_job.jscc_sync.name}:run"

    oauth_token {
      service_account_email = local.sa_scheduler
      scope                 = "https://www.googleapis.com/auth/cloud-platform"
    }
  }

  depends_on = [google_cloud_run_v2_job_iam_member.jscc_sync_scheduler_invoker]
}

# 取引カレンダーの自動取り込み（#63）。JPX「祝日取引」の表を検証して版ごとに追記し、最新版をビューで読む。
# 取り込みは ingest サービスの 06:40 JST のリクエストが行う（Scheduler は足さない）。手動はジョブ calendar-sync。
# 書くのは sa-ingest（optionchan_tokyo の dataEditor。iam.tf）なので付与の追加は無い。
# 読み手の契約（列・最新版の選び方・SQ 日の繰り上げへの使い方）は docs/architecture/target.md 3.2「取引カレンダー（#63）」

# 版の履歴（追記のみ。年に数版 × 数十行なのでパーティションは無い）。列の正は schema/tokyo/trading_calendar.json
# （取り込みのテーブルではないので tables.json には入れない）。履歴なので削除は二重に止める
resource "google_bigquery_table" "trading_calendar" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.tokyo.dataset_id
  table_id            = "trading_calendar"
  description         = "取引カレンダーの版（#63。JPX「祝日取引」の祝日取引実施日の表。最新版はビュー trading_calendar_latest）"
  schema              = file("${path.module}/../../schema/tokyo/trading_calendar.json")
  deletion_protection = true

  lifecycle {
    prevent_destroy = true
  }
}

# 最新版（version_at が最大の版の行）。データを持たないので削除の保護はしない（定義は sql/calendar/）
resource "google_bigquery_table" "trading_calendar_latest" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.tokyo.dataset_id
  table_id            = "trading_calendar_latest"
  description         = "取引カレンダーの最新版（#63。sql/calendar/trading_calendar_latest.sql）"
  deletion_protection = false

  view {
    query = trimspace(templatefile("${path.module}/../../sql/calendar/trading_calendar_latest.sql", {
      project = var.project_id
      dataset = google_bigquery_dataset.tokyo.dataset_id
    }))
    use_legacy_sql = false
  }

  depends_on = [google_bigquery_table.trading_calendar]
}

# 手動の取り込み（定期起動しない）: gcloud run jobs execute calendar-sync --region ... [--args=--dry-run] --wait
# 引数: --dry-run（書かない）/ --accept=<理由>（前の版との差分の検証を越える）。権限は sa-ingest と共用。
# タスクのタイムアウト 600 秒（アプリは settings.CALENDAR_JOB_DEADLINE_SECONDS = 300 秒）、リトライ 0。
# 失敗は ERROR ログ（新系の M2 の対象）。deletion_protection = false: データを持たない
resource "google_cloud_run_v2_job" "calendar_sync" {
  project             = var.project_id
  name                = "calendar-sync"
  location            = var.region
  deletion_protection = false

  template {
    task_count  = 1
    parallelism = 1

    template {
      service_account = local.sa_ingest
      timeout         = "600s"
      max_retries     = 0

      containers {
        image   = var.ingest_image
        command = ["python", "-m", "optionchan_ingest.calendar_sync"]

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
          name  = "BQ_DATASET"
          value = google_bigquery_dataset.tokyo.dataset_id
        }
      }
    }
  }

  depends_on = [google_bigquery_table.trading_calendar_latest]
}

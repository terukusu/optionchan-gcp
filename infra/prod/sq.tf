# 日次の品質チェックと新旧の突き合わせ（#40、target.md 3.4）。BigQuery Data Transfer Service（DTS）の 4 本と、
# その作業用データセット・結果のテーブル・付与。実行 SA sa-sq-dts とプロジェクト単位・旧データセットへの付与は infra/bootstrap/sq.tf。
# 窓はどれも @run_time の JST の日付 D に対し [D-1 07:00, D 07:00)。スケジュールは UTC（JST − 9 時間）。
#   legacy-stage   07:02 JST  US    旧系の前日分 → optionchan_legacy_stage（旧系の夜間分は 06:20 頃までに入っている）
#   legacy-copy    07:10 JST  東京  optionchan_legacy_stage → optionchan_legacy_copy（データセットコピー）
#   quality-daily  07:15 JST  東京  品質チェック → quality_daily。違反があれば失敗（新系の M2 で通知）。ingest の 07:00 の照合・回収の後
#   legacy-compare 07:30 JST  東京  コピーと最新版を比べる → legacy_compare_daily。コピーの欠けは失敗、説明できない差分は下の切り替えで
# 時刻を 07 時台に寄せるのは、失敗の M2（通知は 1 時間に 1 回まで）の抑制が 08:45 の寄り付きの前に終わるようにするため。
# legacy-* と作業用データセット・legacy_compare_daily は並行稼働の間だけ（#14 で消す）。品質チェックは残す。
locals {
  sa_sq = "sa-sq-dts@${var.project_id}.iam.gserviceaccount.com"

  # 旧系（Cloud Functions の取り込み）のデータセット。US。Terraform 管理外で、ここでは名前を参照するだけ
  legacy_dataset = "optionchan"

  # 作業用データセットのテーブルは毎日作り直す。止まったら 7 日で消える
  legacy_work_table_expiration_ms = 7 * 24 * 60 * 60 * 1000

  # true で legacy-compare が説明できない差分（1 以上）を ERROR にする（M2 で通知）。
  # 分類が実データに合っていることを数日の legacy_compare_daily で確かめてから true にする（runbook「日次の品質チェックと新旧の突き合わせ」）
  legacy_compare_fail_on_unexplained = false
}

# ---- 作業用データセット（並行稼働の間だけ。データは毎日作り直すので削除の保護はしない） ----

resource "google_bigquery_dataset" "legacy_stage" {
  project                     = var.project_id
  dataset_id                  = "optionchan_legacy_stage"
  location                    = "US"
  description                 = "新旧の突き合わせ（#40）: 旧系の前日分の作業用（legacy-stage が作り、legacy-copy が東京へ運ぶ）"
  default_table_expiration_ms = local.legacy_work_table_expiration_ms
  max_time_travel_hours       = 48
  delete_contents_on_destroy  = true
}

resource "google_bigquery_dataset" "legacy_copy" {
  project                     = var.project_id
  dataset_id                  = "optionchan_legacy_copy"
  location                    = local.bq.dataset.location
  description                 = "新旧の突き合わせ（#40）: 旧系の前日分のコピー（legacy-copy の宛先、legacy-compare が読む）"
  default_table_expiration_ms = local.legacy_work_table_expiration_ms
  max_time_travel_hours       = 48
  delete_contents_on_destroy  = true
}

# ---- 結果のテーブル（optionchan_tokyo。列の正は schema/tokyo/<table>.json。取り込みのテーブルではないので tables.json には入れない） ----
# 1 日 1 行（窓の日付ごとに MERGE で置き換え）なのでパーティションは無い。
# 記録は #11 の判定の根拠なので deletion_protection を付ける（消すときは外す PR を先に出す）

resource "google_bigquery_table" "sq_result" {
  for_each = {
    quality_daily        = "日次の品質チェックの結果（#40。sql/quality/quality_daily.sql）"
    legacy_compare_daily = "新旧の突き合わせの結果（#40、#11 の AC 5〜7。sql/legacy/compare.sql）"
  }

  project             = var.project_id
  dataset_id          = google_bigquery_dataset.tokyo.dataset_id
  table_id            = each.key
  description         = each.value
  schema              = file("${path.module}/../../schema/tokyo/${each.key}.json")
  deletion_protection = true
}

# ---- sa-sq-dts への付与（データセット・テーブル単位） ----

# 品質チェックと突き合わせで最新版（テーブル関数）と生テーブルを読む。書けるのは下の結果のテーブルだけ
resource "google_bigquery_dataset_iam_member" "sq_tokyo_viewer" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.tokyo.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${local.sa_sq}"
}

resource "google_bigquery_table_iam_member" "sq_result_editor" {
  for_each = google_bigquery_table.sq_result

  project    = var.project_id
  dataset_id = each.value.dataset_id
  table_id   = each.value.table_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${local.sa_sq}"
}

# US の作業用テーブルを作り直す（CREATE OR REPLACE TABLE）
resource "google_bigquery_dataset_iam_member" "sq_legacy_stage" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.legacy_stage.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${local.sa_sq}"
}

# データセットコピーの宛先には datasets.update が要る（dataEditor に無い。公式の必要な権限）。この作業用データセットだけ
resource "google_bigquery_dataset_iam_member" "sq_legacy_copy" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.legacy_copy.dataset_id
  role       = "roles/bigquery.dataOwner"
  member     = "serviceAccount:${local.sa_sq}"
}

# ---- DTS ----
# 失敗は Cloud Logging の resource.type="bigquery_dts_config"・severity ERROR で、新系の M2（monitoring/ingest-m2-errors.json）が通知する。
# 失敗のメール（email_preferences）は実行 SA 宛てで届かないので使わない。

resource "google_bigquery_data_transfer_config" "quality_daily" {
  project              = var.project_id
  display_name         = "optionchan quality-daily"
  location             = local.bq.dataset.location
  data_source_id       = "scheduled_query"
  schedule             = "every day 22:15"
  service_account_name = local.sa_sq
  params = {
    query = templatefile("${path.module}/../../sql/quality/quality_daily.sql", {
      project = var.project_id
      dataset = google_bigquery_dataset.tokyo.dataset_id
    })
  }

  depends_on = [google_bigquery_dataset_iam_member.sq_tokyo_viewer, google_bigquery_table_iam_member.sq_result_editor]
}

resource "google_bigquery_data_transfer_config" "legacy_stage" {
  project              = var.project_id
  display_name         = "optionchan legacy-stage"
  location             = google_bigquery_dataset.legacy_stage.location
  data_source_id       = "scheduled_query"
  schedule             = "every day 22:02"
  service_account_name = local.sa_sq
  params = {
    query = templatefile("${path.module}/../../sql/legacy/stage.sql", {
      project        = var.project_id
      legacy_dataset = local.legacy_dataset
      stage_dataset  = google_bigquery_dataset.legacy_stage.dataset_id
    })
  }

  depends_on = [google_bigquery_dataset_iam_member.sq_legacy_stage]
}

resource "google_bigquery_data_transfer_config" "legacy_copy" {
  project                = var.project_id
  display_name           = "optionchan legacy-copy"
  location               = google_bigquery_dataset.legacy_copy.location
  data_source_id         = "cross_region_copy"
  destination_dataset_id = google_bigquery_dataset.legacy_copy.dataset_id
  schedule               = "every day 22:10"
  service_account_name   = local.sa_sq
  params = {
    source_project_id           = var.project_id
    source_dataset_id           = google_bigquery_dataset.legacy_stage.dataset_id
    overwrite_destination_table = "true"
  }

  depends_on = [google_bigquery_dataset_iam_member.sq_legacy_stage, google_bigquery_dataset_iam_member.sq_legacy_copy]
}

resource "google_bigquery_data_transfer_config" "legacy_compare" {
  project              = var.project_id
  display_name         = "optionchan legacy-compare"
  location             = local.bq.dataset.location
  data_source_id       = "scheduled_query"
  schedule             = "every day 22:30"
  service_account_name = local.sa_sq
  params = {
    query = templatefile("${path.module}/../../sql/legacy/compare.sql", {
      project             = var.project_id
      dataset             = google_bigquery_dataset.tokyo.dataset_id
      copy_dataset        = google_bigquery_dataset.legacy_copy.dataset_id
      fail_on_unexplained = local.legacy_compare_fail_on_unexplained
    })
  }

  depends_on = [
    google_bigquery_dataset_iam_member.sq_tokyo_viewer,
    google_bigquery_table_iam_member.sq_result_editor,
    google_bigquery_dataset_iam_member.sq_legacy_copy,
  ]
}

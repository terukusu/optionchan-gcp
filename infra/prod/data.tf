# 新しい取り込みのデータセット optionchan_tokyo とテーブル（#33）。
# 設定の正は schema/tokyo/tables.json、列の正は schema/tokyo/<table>.json（apps/ingest の test_schema が検査）。
# ここでは値を書き写さずに読むだけにする。データを持つので削除は二重に止める（prevent_destroy と deletion_policy / deletion_protection）。
locals {
  bq = jsondecode(file("${path.module}/../../schema/tokyo/tables.json"))
}

# access ブロックは書かない（付与は iam.tf の google_bigquery_dataset_iam_member。両方を書くと差分が出続ける）。
# 作成者の sa-tf-apply がデータセットの OWNER になる
resource "google_bigquery_dataset" "tokyo" {
  project                     = var.project_id
  dataset_id                  = local.bq.dataset.dataset_id
  location                    = local.bq.dataset.location
  storage_billing_model       = local.bq.dataset.storage_billing_model
  max_time_travel_hours       = local.bq.dataset.max_time_travel_hours
  default_table_expiration_ms = local.bq.dataset.default_table_expiration_ms
  description                 = "新しい取り込み（Cloud Run ingest）の書き込み先（#11）"
  delete_contents_on_destroy  = false
  deletion_policy             = "PREVENT"

  lifecycle {
    prevent_destroy = true
  }
}

resource "google_bigquery_table" "this" {
  for_each = local.bq.tables

  project                  = var.project_id
  dataset_id               = google_bigquery_dataset.tokyo.dataset_id
  table_id                 = each.key
  schema                   = file("${path.module}/../../schema/tokyo/${each.key}.json")
  clustering               = length(each.value.clustering) > 0 ? each.value.clustering : null
  require_partition_filter = each.value.require_partition_filter
  deletion_protection      = true
  deletion_policy          = "PREVENT"

  time_partitioning {
    type  = each.value.time_partitioning.type
    field = each.value.time_partitioning.field
  }

  lifecycle {
    prevent_destroy = true
  }
}

# 最新版の選択（#34、target.md 3.2）: スナップショットごとに最新の完了した実行の行だけを返すテーブル関数。
# 回収・再処理は同じスナップショットを追記で書き直すので、表示・集計・比較は生テーブルではなくこれを使う。
# ビューでなくテーブル関数にするのは、期間の引数を必須にして option_price の require_partition_filter と
# ingest_runs の全件スキャンを避けるため。本文は sql/latest/<名前>.sql（プロジェクトとデータセットはここで入れる）。
# データを持たないので削除の保護はしない（切り戻しは revert の PR で消える）
resource "google_bigquery_routine" "latest" {
  for_each = toset(["latest_runs", "spot_price_latest", "future_price_latest", "option_price_latest"])

  project      = var.project_id
  dataset_id   = google_bigquery_dataset.tokyo.dataset_id
  routine_id   = each.key
  routine_type = "TABLE_VALUED_FUNCTION"
  language     = "SQL"
  description  = "スナップショットごとに最新の完了した実行（ingest_runs）の行だけを返す。期間は [start_at, end_at)（sql/latest/${each.key}.sql）"
  definition_body = trimspace(templatefile("${path.module}/../../sql/latest/${each.key}.sql", {
    project = var.project_id
    dataset = google_bigquery_dataset.tokyo.dataset_id
  }))

  arguments {
    name      = "start_at"
    data_type = jsonencode({ typeKind = "TIMESTAMP" })
  }
  arguments {
    name      = "end_at"
    data_type = jsonencode({ typeKind = "TIMESTAMP" })
  }

  depends_on = [google_bigquery_table.this]
}

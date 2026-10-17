# J-Quants DataCube の日経225オプション歩み値（#111）。オーナーが購入した非公開のデータ（自己使用。第三者への提供・公開は不可）。
# CSV（月ごとに 1 ファイル）を gs://<P>-raw/datacube/option_tick/ に置き、所有者が月ごとに bq load で入れる（runbook「DataCube の
# 歩み値での穴埋め（#111）」）。取り込みのテーブルではないので tables.json には入れない。列の正は schema/tokyo/dc_option_tick.json。
# 付与の追加は無い（データセット単位の既存の付与が及ぶが、すべてこのプロジェクトの中）。データを持つので削除は二重に止める
resource "google_bigquery_table" "dc_option_tick" {
  project                  = var.project_id
  dataset_id               = google_bigquery_dataset.tokyo.dataset_id
  table_id                 = "dc_option_tick"
  description              = "DataCube 日経225オプション歩み値（#111。非公開の購入データ。取引日の月ごとに bq load --replace で入れる）"
  schema                   = file("${path.module}/../../schema/tokyo/dc_option_tick.json")
  clustering               = ["contract_month", "put_call_type", "exercise_price"]
  require_partition_filter = true
  deletion_protection      = true
  deletion_policy          = "PREVENT"

  # 1 ファイル = 1 取引月 = 1 パーティション（パーティションのデコレータ $YYYYMM と --replace で月だけを置き換える）
  time_partitioning {
    type  = "MONTH"
    field = "trade_date"
  }

  lifecycle {
    prevent_destroy = true
  }
}

# J-Quants DataCube の日経225先物（ラージ）歩み値（#114）。dc_option_tick と同じ扱い（非公開の購入データ。自己使用）。
# CSV（月ごとに 1 ファイル）を gs://<P>-raw/datacube/future_tick/ に置き、所有者が月ごとに bq load で入れる（runbook「DataCube の
# 歩み値での行の作成（#114）」）。列の正は schema/tokyo/dc_future_tick.json（2022-08 以前のファイルは最後の列 sco_category が無い）
resource "google_bigquery_table" "dc_future_tick" {
  project                  = var.project_id
  dataset_id               = google_bigquery_dataset.tokyo.dataset_id
  table_id                 = "dc_future_tick"
  description              = "DataCube 日経225先物歩み値（#114。非公開の購入データ。取引日の月ごとに bq load --replace で入れる）"
  schema                   = file("${path.module}/../../schema/tokyo/dc_future_tick.json")
  clustering               = ["contract_month"]
  require_partition_filter = true
  deletion_protection      = true
  deletion_policy          = "PREVENT"

  # 1 ファイル = 1 取引月 = 1 パーティション（パーティションのデコレータ $YYYYMM と --replace で月だけを置き換える）
  time_partitioning {
    type  = "MONTH"
    field = "trade_date"
  }

  lifecycle {
    prevent_destroy = true
  }
}

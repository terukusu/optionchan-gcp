variable "project_id" {
  description = "GCP プロジェクト ID"
  type        = string
  default     = "optionchan-222710"
}

variable "region" {
  description = "既定のリージョン"
  type        = string
  default     = "asia-northeast1"
}

# 取り込みのイメージ（#33）。CI が .github/scripts/ingest-image.sh resolve で求めて TF_VAR_ingest_image で渡す。
# plan では Artifact Registry に未作成のタグ参照も許す（初回・入力の変更時）。apply job は digest であることを確かめてから apply する
variable "ingest_image" {
  description = "Cloud Run ingest のイメージ（<repo>/ingest@sha256:<digest> か <repo>/ingest:src-<20hex>）"
  type        = string

  validation {
    condition     = can(regex("^asia-northeast1-docker\\.pkg\\.dev/[a-z0-9-]+/optionchan/ingest(@sha256:[0-9a-f]{64}|:src-[0-9a-f]{20})$", var.ingest_image))
    error_message = "ingest_image は asia-northeast1-docker.pkg.dev/<project>/optionchan/ingest の digest 参照か src-<20hex> のタグ参照で指定する。"
  }
}

# Scheduler ジョブ ingest の停止・再開を PR で行う用。緊急時に gcloud で止めたら、すぐ true にする PR を出す
# （false のままだと次の apply で再開される。runbook「新しい取り込み（ingest）」）
variable "ingest_paused" {
  description = "true で Scheduler ジョブ ingest を一時停止する"
  type        = bool
  default     = false
}

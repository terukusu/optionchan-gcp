# 既定値は持たない（リポジトリは誰でも clone して自分のプロジェクトで使う前提）。TF_VAR_project_id か tfvars で渡す
variable "project_id" {
  description = "GCP プロジェクト ID"
  type        = string
}

variable "region" {
  description = "既定のリージョン"
  type        = string
  default     = "asia-northeast1"
}

# 取り込みのイメージ（#33）。CI が .github/scripts/image.sh ingest resolve で求めて TF_VAR_ingest_image で渡す。
# plan では Artifact Registry に未作成のタグ参照も許す（初回・入力の変更時）。apply job は digest であることを確かめてから apply する
variable "ingest_image" {
  description = "Cloud Run ingest のイメージ（<repo>/ingest@sha256:<digest> か <repo>/ingest:src-<20hex>）"
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9-]+-docker\\.pkg\\.dev/[a-z0-9-]+/optionchan/ingest(@sha256:[0-9a-f]{64}|:src-[0-9a-f]{20})$", var.ingest_image))
    error_message = "ingest_image は <region>-docker.pkg.dev/<project>/optionchan/ingest の digest 参照か src-<20hex> のタグ参照で指定する。"
  }
}

# ビューアーのイメージ（#53）。ingest_image と同じく CI が .github/scripts/image.sh viewer resolve で求めて TF_VAR_viewer_image で渡す
variable "viewer_image" {
  description = "Cloud Run viewer のイメージ（<repo>/viewer@sha256:<digest> か <repo>/viewer:src-<20hex>）"
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9-]+-docker\\.pkg\\.dev/[a-z0-9-]+/optionchan/viewer(@sha256:[0-9a-f]{64}|:src-[0-9a-f]{20})$", var.viewer_image))
    error_message = "viewer_image は <region>-docker.pkg.dev/<project>/optionchan/viewer の digest 参照か src-<20hex> のタグ参照で指定する。"
  }
}

# Scheduler ジョブ ingest の停止・再開を PR で行う用。緊急時に gcloud で止めたら、すぐ true にする PR を出す
# （false のままだと次の apply で再開される。runbook「新しい取り込み（ingest）」）
variable "ingest_paused" {
  description = "true で Scheduler ジョブ ingest を一時停止する"
  type        = bool
  default     = false
}

# 旧系の停止（#14、target.md 6 章 5a）。true にする PR の apply で、旧系向けのアラート m1・m2 を消し（monitoring/ の JSON から
# 作り直せる）、新旧の突き合わせの DTS 3 本（legacy-stage / legacy-copy / legacy-compare）を無効にする。旧系の Scheduler・関数の公開・
# App Engine は Terraform 管理外なので、同じ日に gcloud で止める（runbook「旧系の停止（#14、5a）」）。false に戻す PR で元どおり作り直す。
# 旧系を消す（#24）ときに、この変数と旧系向けの定義をまとめて消す
variable "legacy_stopped" {
  description = "true で旧系向けのアラート（m1・m2）を消し、新旧の突き合わせの DTS（legacy-*）を無効にする"
  type        = bool
  default     = false
}

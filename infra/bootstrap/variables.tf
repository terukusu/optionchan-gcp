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

variable "github_repository" {
  description = "WIF で借用を許す GitHub リポジトリ（owner/name）。apply SA の subject に使う"
  type        = string
  default     = "terukusu/optionchan-gcp"
}

# GitHub の数値 ID はコード・tfvars に置かない。apply 時に gh api から TF_VAR_ で渡す（runbook「Terraform」）。
variable "github_repository_id" {
  description = "GitHub リポジトリの数値 ID（gh api repos/<owner>/<repo> --jq .id）"
  type        = string

  validation {
    condition     = can(regex("^[0-9]+$", var.github_repository_id))
    error_message = "github_repository_id は数字だけで指定する。"
  }
}

variable "github_owner_id" {
  description = "GitHub リポジトリ所有者の数値 ID（gh api repos/<owner>/<repo> --jq .owner.id）"
  type        = string

  validation {
    condition     = can(regex("^[0-9]+$", var.github_owner_id))
    error_message = "github_owner_id は数字だけで指定する。"
  }
}

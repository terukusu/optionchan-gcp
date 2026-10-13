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

# ビューアー（Cloud Run viewer）を IAP 越しに開けるユーザー（#53）。メールはコード・リポジトリに置かず、
# ローカルの infra/bootstrap/local.auto.tfvars（*.tfvars は gitignore 済み）に書く（runbook「ビューアー（#13）」）。
# 既定値は持たない: 既定を [] にすると、ファイルを置き忘れた apply で付与が消えるため（忘れたら plan がエラーで止まる）。
# サービス viewer が無いうちは付与できないので、prod の apply の前は []、後に ["user:<メール>"] にする。
variable "viewer_iap_members" {
  description = "IAP 越しに viewer を開けるプリンシパル（例: [\"user:<メール>\"]）。サービスの作成前は []"
  type        = list(string)

  validation {
    # domain: は不可（domain:gmail.com のような書き方で誰でも開ける＝誰でも BigQuery の課金を発生させられる状態になるため）
    condition     = alltrue([for m in var.viewer_iap_members : can(regex("^(user|group):[^\\s@]+@[^\\s@]+$", m))])
    error_message = "viewer_iap_members の各要素は user:<メール> か group:<メール>（domain: / allUsers / allAuthenticatedUsers は不可）。"
  }
}

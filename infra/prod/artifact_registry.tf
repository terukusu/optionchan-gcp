# コンテナイメージの置き場（イメージは #11 から）。直近 5 版を残し、30 日より古いものを消す。
resource "google_artifact_registry_repository" "app" {
  project                = var.project_id
  location               = var.region
  repository_id          = "optionchan"
  description            = "optionchan のコンテナイメージ"
  format                 = "DOCKER"
  cleanup_policy_dry_run = false
  deletion_policy        = "PREVENT"

  docker_config {
    immutable_tags = true
  }

  cleanup_policies {
    id     = "keep-recent"
    action = "KEEP"
    most_recent_versions {
      keep_count = 5
    }
  }

  # KEEP が DELETE より優先されるので、直近 5 版は 30 日を過ぎても消えない
  cleanup_policies {
    id     = "delete-old"
    action = "DELETE"
    condition {
      tag_state  = "ANY"
      older_than = "2592000s"
    }
  }
}

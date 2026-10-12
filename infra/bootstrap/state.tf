# tfstate バケット。prod 用（CI が使う）と bootstrap 用（オーナーだけが使う）を分ける。
locals {
  state_buckets = {
    prod      = "${var.project_id}-tfstate"
    bootstrap = "${var.project_id}-tfstate-bootstrap"
  }
}

resource "google_storage_bucket" "tfstate" {
  for_each = local.state_buckets

  name                        = each.value
  project                     = var.project_id
  location                    = "ASIA-NORTHEAST1"
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false
  deletion_policy             = "PREVENT"

  versioning {
    enabled = true
  }

  # 古い世代は 10 世代を超えたら消す（復旧には直近の世代を使う）
  lifecycle_rule {
    condition {
      num_newer_versions = 10
      with_state         = "ARCHIVED"
    }
    action {
      type = "Delete"
    }
  }

  lifecycle {
    prevent_destroy = true
  }
}

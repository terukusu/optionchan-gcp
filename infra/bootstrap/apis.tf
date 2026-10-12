# iam / iamcredentials / sts は WIF に必要（#10）、run は取り込みの Cloud Run に必要（#33）。残りは有効化済みで、宣言しても変更は起きない。
# destroy でも API は無効化しない（現行構成が使っているものがある）。
locals {
  services = toset([
    "iam.googleapis.com",
    "iamcredentials.googleapis.com",
    "sts.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "serviceusage.googleapis.com",
    "artifactregistry.googleapis.com",
    "monitoring.googleapis.com",
    "run.googleapis.com",
  ])
}

resource "google_project_service" "this" {
  for_each = local.services

  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

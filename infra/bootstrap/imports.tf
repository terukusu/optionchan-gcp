# bootstrap 用バケットは state の置き場なので、Terraform より先に gcloud で作って取り込む（runbook「Terraform」）。
import {
  to = google_storage_bucket.tfstate["bootstrap"]
  id = "${var.project_id}-tfstate-bootstrap"
}

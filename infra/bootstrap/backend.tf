# bootstrap の state は prod と別のバケットに置く（CI 用 SA には読ませない）。
# バケットは初回だけ gcloud で作り、imports.tf で取り込む（runbook「Terraform」参照）。
terraform {
  backend "gcs" {
    bucket = "optionchan-222710-tfstate-bootstrap"
    prefix = "bootstrap"
  }
}

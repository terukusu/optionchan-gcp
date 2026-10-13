# バケット名（<PROJECT_ID>-tfstate）はプロジェクトごとに違うので部分設定にし、
# terraform init -backend-config="bucket=<PROJECT_ID>-tfstate" で渡す（CI は vars.TF_STATE_BUCKET）。
terraform {
  backend "gcs" {
    prefix = "prod"
  }
}

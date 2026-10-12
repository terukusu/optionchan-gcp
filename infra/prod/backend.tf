terraform {
  backend "gcs" {
    bucket = "optionchan-222710-tfstate"
    prefix = "prod"
  }
}

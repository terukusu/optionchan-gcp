# プロバイダの完全名はプロジェクト番号を含むので sensitive。画面に出さず gh secret set へパイプで渡す。
output "wif_provider_name" {
  value     = google_iam_workload_identity_pool_provider.github_actions.name
  sensitive = true
}

output "sa_tf_plan_email" {
  value = google_service_account.tf_plan.email
}

output "sa_tf_apply_email" {
  value = google_service_account.tf_apply.email
}

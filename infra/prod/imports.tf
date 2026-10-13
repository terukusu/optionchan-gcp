# #9 で gcloud から作ったアラート 2 本の取り込み（#10）。
# ポリシー ID はコードに書かず、GitHub secret TF_ALERT_POLICY_IDS → TF_VAR_alert_policy_ids で渡す
# （例: {"m1":"projects/<project>/alertPolicies/<id>","m2":"..."}）。取り込み後に variable を消すと空になり、何もしない。
# この変数と import ブロックは #11 以降の PR で消してよい。
variable "alert_policy_ids" {
  description = "取り込むアラートポリシーの完全名（キーは monitoring.tf の alert_policies と同じ）"
  type        = map(string)
  default     = {}
}

# 作らないポリシー（旧系を止めた後の m1・m2。monitoring.tf の legacy_stopped）は取り込まない（import 先が無いと plan がエラーになる）
import {
  for_each = { for k, v in var.alert_policy_ids : k => v if contains(keys(local.alert_policies), k) }
  to       = google_monitoring_alert_policy.this[each.key]
  id       = each.value
}

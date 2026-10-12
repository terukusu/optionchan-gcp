# 監視アラート（#9）。定義の正は monitoring/*.json（tests/test_monitoring.py で検査）。
# ここでは JSON のキーを camelCase → snake_case に写すだけで、値は変えない。
locals {
  alert_policies = {
    m1 = jsondecode(file("${path.module}/../../monitoring/m1-liveness.json"))
    m2 = jsondecode(file("${path.module}/../../monitoring/m2-errors.json"))
  }
}

# このプロジェクトのメール通知チャネル（1 件）。他のポリシーも使うので Terraform では管理しない。
# 表示名はメールアドレスなので type で引く。2 件以上になるとエラーになり、気づける。
data "google_monitoring_notification_channel" "email" {
  project = var.project_id
  type    = "email"
}

resource "google_monitoring_alert_policy" "this" {
  for_each = local.alert_policies

  project               = var.project_id
  display_name          = each.value.displayName
  combiner              = each.value.combiner
  enabled               = each.value.enabled
  severity              = each.value.severity
  notification_channels = [data.google_monitoring_notification_channel.email.name]

  documentation {
    subject   = each.value.documentation.subject
    content   = each.value.documentation.content
    mime_type = each.value.documentation.mimeType

    dynamic "links" {
      for_each = try(each.value.documentation.links, [])
      content {
        display_name = links.value.displayName
        url          = links.value.url
      }
    }
  }

  dynamic "conditions" {
    for_each = each.value.conditions
    content {
      display_name = conditions.value.displayName

      dynamic "condition_absent" {
        for_each = can(conditions.value.conditionAbsent) ? [conditions.value.conditionAbsent] : []
        content {
          filter   = condition_absent.value.filter
          duration = condition_absent.value.duration

          dynamic "aggregations" {
            for_each = try(condition_absent.value.aggregations, [])
            content {
              alignment_period     = try(aggregations.value.alignmentPeriod, null)
              per_series_aligner   = try(aggregations.value.perSeriesAligner, null)
              cross_series_reducer = try(aggregations.value.crossSeriesReducer, null)
              group_by_fields      = try(aggregations.value.groupByFields, null)
            }
          }

          dynamic "trigger" {
            for_each = can(condition_absent.value.trigger) ? [condition_absent.value.trigger] : []
            content {
              count   = try(trigger.value.count, null)
              percent = try(trigger.value.percent, null)
            }
          }
        }
      }

      dynamic "condition_threshold" {
        for_each = can(conditions.value.conditionThreshold) ? [conditions.value.conditionThreshold] : []
        content {
          filter          = condition_threshold.value.filter
          duration        = condition_threshold.value.duration
          comparison      = condition_threshold.value.comparison
          threshold_value = try(condition_threshold.value.thresholdValue, null)

          dynamic "aggregations" {
            for_each = try(condition_threshold.value.aggregations, [])
            content {
              alignment_period     = try(aggregations.value.alignmentPeriod, null)
              per_series_aligner   = try(aggregations.value.perSeriesAligner, null)
              cross_series_reducer = try(aggregations.value.crossSeriesReducer, null)
              group_by_fields      = try(aggregations.value.groupByFields, null)
            }
          }

          dynamic "trigger" {
            for_each = can(condition_threshold.value.trigger) ? [condition_threshold.value.trigger] : []
            content {
              count   = try(trigger.value.count, null)
              percent = try(trigger.value.percent, null)
            }
          }
        }
      }
    }
  }
}

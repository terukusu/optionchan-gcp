# 監視アラート（旧系 m1・m2 は #9、新しい取り込みの ingest_m1・ingest_m2 は #35）。
# 定義の正は monitoring/*.json（tests/test_monitoring.py で検査）。
# ここでは JSON のキーを camelCase → snake_case に写すだけで、値は変えない（例外は下の enabled の連動と、legacy_stopped で旧系の 2 本を作らないことだけ）。
locals {
  # 旧系（Cloud Functions の取り込み）向け。旧系を止めたら（var.legacy_stopped = true）消す（#14）
  legacy_alert_policies = {
    m1 = jsondecode(file("${path.module}/../../monitoring/m1-liveness.json"))
    m2 = jsondecode(file("${path.module}/../../monitoring/m2-errors.json"))
  }

  alert_policies = merge({ for k, v in local.legacy_alert_policies : k => v if !var.legacy_stopped }, {
    ingest_m1 = jsondecode(file("${path.module}/../../monitoring/ingest-m1-liveness.json"))
    ingest_m2 = jsondecode(file("${path.module}/../../monitoring/ingest-m2-errors.json"))
  })

  # Scheduler ingest を止めている間（var.ingest_paused = true）は無効にするポリシー（止めると必ず鳴るため）
  paused_with_ingest = ["ingest_m1"]
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
  enabled               = each.value.enabled && !(var.ingest_paused && contains(local.paused_with_ingest, each.key))
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

  dynamic "alert_strategy" {
    for_each = can(each.value.alertStrategy) ? [each.value.alertStrategy] : []
    content {
      auto_close           = try(alert_strategy.value.autoClose, null)
      notification_prompts = try(alert_strategy.value.notificationPrompts, null)

      dynamic "notification_rate_limit" {
        for_each = can(alert_strategy.value.notificationRateLimit) ? [alert_strategy.value.notificationRateLimit] : []
        content {
          period = try(notification_rate_limit.value.period, null)
        }
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

      dynamic "condition_matched_log" {
        for_each = can(conditions.value.conditionMatchedLog) ? [conditions.value.conditionMatchedLog] : []
        content {
          filter           = condition_matched_log.value.filter
          label_extractors = try(condition_matched_log.value.labelExtractors, null)
        }
      }

      dynamic "condition_threshold" {
        for_each = can(conditions.value.conditionThreshold) ? [conditions.value.conditionThreshold] : []
        content {
          filter          = condition_threshold.value.filter
          duration        = condition_threshold.value.duration
          comparison      = condition_threshold.value.comparison
          threshold_value = try(condition_threshold.value.thresholdValue, null)

          evaluation_missing_data = try(condition_threshold.value.evaluationMissingData, null)

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

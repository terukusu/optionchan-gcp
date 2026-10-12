# runbook

本番（`optionchan-222710`）の運用手順。節は設計（`docs/architecture/target.md` 4.1）の項目ごとに、各 Issue で追記する。

## コスト

予算アラートと BigQuery の 1 日あたりクエリ量の上限（#21）。どちらも Terraform 管理外（#10 で import するか判断する）。

| 項目 | 設定 |
|---|---|
| 予算 | 表示名 `optionchan: 月額予算`。このプロジェクトのみ、暦月、500 円（税抜き、クレジット込みの実請求で比較）、実績の 50 / 80 / 100% で通知。通知先は既存の Monitoring メール通知チャネルのみ（請求先アカウントは他プロジェクトと共有のため、IAM ロール保持者への既定メールは無効） |
| クエリ量上限 | `bigquery.googleapis.com/quota/query/usage`、unit `1/d/{project}` を 32768 MiB（32 GiB/日）。Service Usage の consumer override で設定。ユーザー単位は設定しない |

予算は通知だけで課金は止めない。課金データの反映は数時間〜1 日遅れる。クエリ量上限は太平洋時間の 0 時にリセットされる。

### 共通の変数

請求先アカウント ID・プロジェクト番号・通知チャネル ID は、Issue・PR・ログに書かない。変数で扱い、出力を貼るときは件数・要約だけにする（請求先 ID は `sed -E 's/[0-9A-Z]{6}-[0-9A-Z]{6}-[0-9A-Z]{6}/<BA>/g'` でマスク）。

```sh
export CLOUDSDK_CORE_SHOULD_PROMPT_TO_ENABLE_API=false   # 未有効 API の自動有効化を防ぐ
P=optionchan-222710
BA=$(gcloud billing projects describe $P --format="value(billingAccountName)" | sed 's#billingAccounts/##')
PN=$(gcloud projects describe $P --format="value(projectNumber)")
CH=$(gcloud beta monitoring channels list --project=$P --format=json \
  | jq -r '.[] | select(.type == "email" and .enabled == true) | .name')   # enabled は API 側の --filter で使えない
echo "$CH" | grep -c .   # 1 であること
```

### 予算

予算 API はこのプロジェクトを quota project として呼ぶので、`billingbudgets.googleapis.com` をこのプロジェクトで有効にしておく。

```sh
gcloud services enable billingbudgets.googleapis.com --project=$P
gcloud services list --enabled --project=$P --filter="config.name=billingbudgets.googleapis.com" --format="value(config.name)"
```

既存の予算の確認（件数だけ見る。他プロジェクトの予算の中身は記録しない）:

```sh
gcloud billing budgets list --billing-account=$BA --billing-project=$P --format=json \
  | jq --arg p "projects/$PN" '{total: length,
      this_project: [.[] | select((.budgetFilter.projects // []) | index($p))] | length,
      all_projects: [.[] | select((.budgetFilter.projects // []) | length == 0)] | length}'
```

`this_project` が 0 なら作成する（`all_projects` は全プロジェクトを対象にした予算の件数で、あってもこのプロジェクト用の予算は別に作る。複数プロジェクトを対象にした予算にこのプロジェクトが含まれていた場合は、その予算は変えずにこのプロジェクト用を別に作る）。1 件あり、このプロジェクトだけを対象にしたものなら、下の作成手順の代わりに削除して作り直す（`gcloud billing budgets update` のしきい値の指定は create と単位が異なるため使わない）。

作成:

```sh
gcloud billing budgets create --billing-account=$BA --billing-project=$P \
  --display-name="optionchan: 月額予算" \
  --budget-amount=500JPY --calendar-period=month \
  --filter-projects=projects/$P \
  --threshold-rule=percent=0.5 --threshold-rule=percent=0.8 --threshold-rule=percent=1.0 \
  --notifications-rule-monitoring-notification-channels=$CH \
  --disable-default-iam-recipients \
  --format="value(name.basename())"
```

確認（`ok` が `true`）:

```sh
gcloud billing budgets list --billing-account=$BA --billing-project=$P --format=json \
  | jq --arg p "projects/$PN" --arg ch "${CH##*/}" '
    [.[] | select((.budgetFilter.projects // []) | index($p))] as $b
    | {count: ($b | length),
       ok: (($b | length) == 1 and ($b[0] |
         .budgetFilter.projects == [$p]
         and .amount.specifiedAmount.currencyCode == "JPY"
         and .amount.specifiedAmount.units == "500"
         and .budgetFilter.calendarPeriod == "MONTH"
         and ([.thresholdRules[].thresholdPercent] | sort) == [0.5, 0.8, 1]
         and all(.thresholdRules[]; (.spendBasis // "CURRENT_SPEND") == "CURRENT_SPEND")
         and ((.notificationsRule.monitoringNotificationChannels // []) | map(split("/") | last)) == [$ch]
         and .notificationsRule.disableDefaultIamRecipients == true))}'
```

金額の変更: `BUDGET=$(gcloud billing budgets list --billing-account=$BA --billing-project=$P --filter='displayName="optionchan: 月額予算"' --format="value(name.basename())")` で ID を取り、`gcloud billing budgets update $BUDGET --billing-account=$BA --billing-project=$P --budget-amount=<金額>JPY`。

削除: `gcloud billing budgets delete $BUDGET --billing-account=$BA --billing-project=$P`。予算をすべて消したあとなら `gcloud services disable billingbudgets.googleapis.com --project=$P` で API も戻せる。

### 予算アラートを受けたとき

1. 課金エクスポート（`PaymentInfo`、US）で、今月のこのプロジェクトのサービス別内訳を見る（テーブル名は請求先 ID を含むのでワイルドカードで指定する）

   ```sh
   bq query --project_id=$P --use_legacy_sql=false '
   SELECT service.description AS service,
          ROUND(SUM(cost)) AS cost,
          ROUND(SUM(IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0))) AS credits
   FROM `optionchan-222710.PaymentInfo.gcp_billing_export_v1_*`
   WHERE project.id = "optionchan-222710"
     AND invoice.month = FORMAT_DATE("%Y%m", CURRENT_DATE("America/Los_Angeles"))
   GROUP BY service ORDER BY cost DESC'
   ```

2. BigQuery が多ければ、スキャン量の多いジョブを探す

   ```sh
   bq query --project_id=$P --use_legacy_sql=false '
   SELECT DATE(creation_time) AS d, user_email, job_id,
          ROUND(total_bytes_billed / POW(1024, 3), 2) AS gib
   FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
   WHERE job_type = "QUERY" AND creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
   ORDER BY total_bytes_billed DESC LIMIT 20'
   ```

3. 公開関数（`smile_data` / `atm_data` / `atm_iv_data`）の呼び出しが原因なら、共通パスワードの漏えいを疑う（#3）。急ぐときは関数の `allUsers` を外す（ビューアーは表示できなくなる）: `gcloud functions remove-iam-policy-binding <関数名> --region=us-east1 --project=$P --member=allUsers --role=roles/cloudfunctions.invoker`。戻すときは `add-iam-policy-binding` で同じ指定

### BigQuery のクエリ量上限

確認:

```sh
gcloud alpha services quota list --service=bigquery.googleapis.com --consumer=projects/$P --format=json \
  | jq -r '.[] | select(.metric == "bigquery.googleapis.com/quota/query/usage")
      | .consumerQuotaLimits[] | select(.unit == "1/d/{project}") | .quotaBuckets[]
      | "effectiveLimit=\(.effectiveLimit) consumerOverride=\(.consumerOverride.overrideValue // "none")"'
```

（`quota list` の生の出力は override 名にプロジェクト番号を含むので貼らない。）

下げる前に、今日（太平洋時間）のスキャン量が新しい上限より十分小さいことを確認する（超えていると、その日の残りのクエリがすぐ失敗する）:

```sh
bq query --project_id=$P --use_legacy_sql=false '
SELECT ROUND(SUM(total_bytes_processed) / POW(1024, 3), 2) AS gib
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE job_type = "QUERY"
  AND creation_time >= TIMESTAMP(CURRENT_DATE("America/Los_Angeles"), "America/Los_Angeles")'
```

設定（既定値から 10% を超えて下げるので `--force` が必要）:

```sh
gcloud alpha services quota update \
  --service=bigquery.googleapis.com --consumer=projects/$P \
  --metric=bigquery.googleapis.com/quota/query/usage --unit='1/d/{project}' \
  --value=32768 --force
```

一時的な引き上げ（移行作業のフェーズ 3a / 3b の全件スキャンの前。`option_price` は論理 約 78GB）: 同じコマンドで `--value` を必要量（例 `204800` = 200 GiB）にし、作業が終わったら `--value=32768 --force` で戻す。

上書きの削除（Google の既定値 200 TiB/日に戻す）:

```sh
OID=$(gcloud alpha services quota list --service=bigquery.googleapis.com --consumer=projects/$P --format=json \
  | jq -r '.[] | select(.metric == "bigquery.googleapis.com/quota/query/usage")
      | .consumerQuotaLimits[] | select(.unit == "1/d/{project}") | .quotaBuckets[]
      | .consumerOverride.name // empty | split("/") | last')
gcloud alpha services quota delete \
  --service=bigquery.googleapis.com --consumer=projects/$P \
  --metric=bigquery.googleapis.com/quota/query/usage --unit='1/d/{project}' \
  --override-id=$OID --force
```

（`gcloud alpha services quota` が使えない場合の代替: `gcloud services enable cloudquotas.googleapis.com --project=$P` のうえ `gcloud beta quotas preferences create --service=bigquery.googleapis.com --project=$P --quota-id=QueryUsagePerDay --preferred-value=32768 --preference-id=bq-query-usage-per-day --allow-high-percentage-quota-decrease`。変更は `gcloud beta quotas preferences update bq-query-usage-per-day --service=bigquery.googleapis.com --project=$P --quota-id=QueryUsagePerDay --preferred-value=<値>`（10% を超えて下げるときは `--allow-high-percentage-quota-decrease` を付ける）、戻すときは `--preferred-value=209715200`。どちらで設定したかをこの節に記録する。）

設定の記録: 2026-10-12 に `gcloud alpha services quota update`（Service Usage の consumer override）で 32768 に設定した。

### クエリ量上限を超えたとき

その日の残り（太平洋時間の 0 時まで）は次のようになる。

| 処理 | 影響 |
|---|---|
| 毎分の取り込み（LOAD ジョブ） | 影響なし |
| `insert_ohlc`（毎日 00:01 UTC、クエリ 3 本） | 失敗しうる。その日の 1 時間足が欠ける |
| ビューアー（`smile_data` / `atm_data` / `atm_iv_data`） | 表示できない |
| 手作業のクエリ・移行作業 | 失敗する |

1. 原因のジョブを探す（上の「スキャン量の多いジョブ」のクエリ）。`error_result.reason = "quotaExceeded"` のジョブがあるかも見る
2. `insert_ohlc` が失敗していたら、リセット後に再実行する。`insert_ohlc` は前日分（UTC）を `CURRENT_TIMESTAMP` 基準で集計するので、**失敗した日と同じ UTC 日のうちに**行う

   ```sh
   gcloud logging read 'resource.type="cloud_function" AND resource.labels.function_name="insert_ohlc" AND severity>=ERROR' \
     --project=$P --freshness=1d --format="value(timestamp,textPayload)" | head
   ```

   クエリは `atm_iv` → `atm_option_price` → `atm_target_price` の順に実行され、途中で失敗すると残りは実行されない。前日分の行がどのテーブルに入っているかを確かめる:

   ```sh
   bq query --project_id=$P --use_legacy_sql=false '
   SELECT "atm_iv" t, COUNT(*) n FROM optionchan.atm_iv
     WHERE started_at >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AND started_at < TIMESTAMP(CURRENT_DATE())
   UNION ALL SELECT "atm_option_price", COUNT(*) FROM optionchan.atm_option_price
     WHERE started_at >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AND started_at < TIMESTAMP(CURRENT_DATE())
   UNION ALL SELECT "atm_target_price", COUNT(*) FROM optionchan.atm_target_price
     WHERE started_at >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AND started_at < TIMESTAMP(CURRENT_DATE())'
   ```

   - 3 つとも 0 件（取引のあった日）なら再実行する: `gcloud pubsub topics publish create_timeframe --message=create_timeframe --project=$P`
   - 一部だけ入っている場合は publish すると入っている分が重複する。入っていないテーブルの INSERT だけを `functions_py/timeframe/main.py` の SQL で `bq query` から実行する

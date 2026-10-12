# runbook

本番（`optionchan-222710`）の運用手順。節は設計（`docs/architecture/target.md` 4.1）の項目ごとに、各 Issue で追記する。

## コスト

予算アラートと BigQuery の 1 日あたりクエリ量の上限（#21）。どちらも Terraform 管理外で、この runbook で管理する（#10 で判断。予算は他プロジェクトと共有の請求先アカウント側のリソースで、クエリ量上限の変更には `serviceusage.quotaAdmin` 相当が要るため、CI 用 SA に権限を渡さない）。

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

## 監視アラート

現行構成の取り込み停止・失敗を検知する Cloud Monitoring のアラートポリシー 2 本（#9）。指標は Cloud Functions 組み込みの `cloudfunctions.googleapis.com/function/execution_count`（無料）で、ログ指標は作らない。通知先は既存のメール通知チャネル 1 件。#10 で Terraform（`infra/prod/monitoring.tf`）に取り込んだ。定義の正は `monitoring/*.json`（`tests/test_monitoring.py` で検査）で、Terraform は JSON を読んで組み立てる。変更は JSON を直して PR を出し、マージ後に deploy workflow が承認つきで反映する（「Terraform」節）。gcloud で直接変えると次の plan で差分になる。旧系の削除（#14）で消す。

| 表示名 | 条件 | 拾うもの | 定義 |
|---|---|---|---|
| `optionchan: M1 生存（download_jpx の起動が 15 分無い）` | `download_jpx` の実行数（status を合算）が 15 分間 無い。24 時間有効 | Scheduler `minutely_task`・Pub/Sub・関数の停止 | `monitoring/m1-liveness.json` |
| `optionchan: M2 エラー（取り込み関数の失敗）` | `download_jpx` / `load_jpx_into_bq` の失敗（status が `ok` 以外）が関数ごとに 15 分間に 3 件以上、または `insert_ohlc` の失敗が 1 件以上 | crash / timeout / connection error（メモリ上限超過を含む）が続くこと | `monitoring/m2-errors.json` |

M2 を ERROR ログの一致にしないのは、既知のエラー（毎日 04:00 / 04:20 JST の `time data ''`、接続エラー、409 Conflict）が毎日 1〜6 件出て毎日鳴るのと、timeout・connection error では ERROR ログが出ないため。単発の失敗は次の分の実行で取り戻せるので、続いたときだけ通知する。

料金: 2027-09-01 以降（公式の価格ページで 2026-10-12 に確認。"no sooner than"）、メトリクス参照 3 件（M1 1、M2 2）× $0.35 + ポイント料金（約 17 万点 / 月 → 約 $0.09）で月 約 $1.14（約 170 円、税抜き）。それまでに #14 で削除する見込み。

### 変数

「コスト」の「共通の変数」の `P` と `CH` を使う（`echo "$CH" | grep -c .` が 1 であること）。通知チャネル ID はポリシー JSON に書かない。Terraform は `type = "email"` の data source で引く（メールチャネルが 2 件以上になると plan がエラーになる）。下の「作成」では `--notification-channels` で渡す（ファイルの設定より優先される）。

### 作成

Terraform 化（#10）前の手順の記録。今は Terraform が作るので使わない。ポリシーを消してしまい CI も使えない緊急時にだけ使い、その後「Terraform」節の「アラートの取り込み」で state を合わせる（古い ID を `terraform state rm` してから import）。

同じ表示名のポリシーがあれば作らない。リポジトリ直下で実行する。

```sh
for f in monitoring/m1-liveness.json monitoring/m2-errors.json; do
  n=$(jq -r .displayName "$f")
  if [ -n "$(gcloud monitoring policies list --project=$P --filter="displayName=\"$n\"" --format='value(name)')" ]; then
    echo "exists: $n"; continue
  fi
  gcloud monitoring policies create --project=$P --policy-from-file="$f" --notification-channels="$CH" --format='value(displayName)'
done
```

### 確認

`ok` が `true` であること（`describe` の生の出力は通知チャネル ID を含むので貼らない）。

```sh
gcloud monitoring policies list --project=$P --format=json \
  | jq --arg ch "$CH" '[.[] | select(.displayName | startswith("optionchan: M"))]
      | {count: length, ok: (length == 2 and all(.[]; .enabled == true and .notificationChannels == [$ch]))}'
gcloud logging metrics list --project=$P --format='value(name)' | grep -c .   # 0（ログ指標は使わない）
```

### バックテスト

しきい値を変えるときは、先に過去 6 週間（Monitoring の保持期間）の `execution_count` に条件を当てて発火回数を見る。読み取りのみ。しきい値は `monitoring/` の JSON から読む。

```sh
python3 -I monitoring/backtest.py   # 終了コード 0: 発火なし、1: 発火あり、2: API エラー
```

2026-10-12 の実行（08-31〜10-12）: M1 の最大の空き 4 分、M2 の 15 分の最大失敗数 `download_jpx` 2・`load_jpx_into_bq` 1・`insert_ohlc` 0 で、発火 0。

### 試験発報

本番の取り込みは止めない。同じ指標・集計で「今発火する」条件にした試験用ポリシーを作り、メールが届いたら消す。

```sh
T=$(mktemp -d)
jq '.displayName = "optionchan: 試験 M2" | .documentation.subject = "[optionchan] 試験 M2（削除予定）"
    | .conditions = [.conditions[0] | .conditionThreshold.filter |= sub("NOT metric.labels.status"; "metric.labels.status")]' \
  monitoring/m2-errors.json > $T/t2.json
jq '.displayName = "optionchan: 試験 M1" | .documentation.subject = "[optionchan] 試験 M1（削除予定）"
    | .conditions[0].conditionAbsent.filter |= sub("\"download_jpx\""; "\"insert_ohlc\"")
    | .conditions[0].conditionAbsent.duration = "300s"' \
  monitoring/m1-liveness.json > $T/t1.json
gcloud monitoring policies create --project=$P --policy-from-file=$T/t2.json --notification-channels="$CH" --format='value(displayName)'
gcloud monitoring policies create --project=$P --policy-from-file=$T/t1.json --notification-channels="$CH" --format='value(displayName)'
rm -rf $T
```

- 試験 M2 は `ok` の実行数（15 分で 15 件前後）で発火するので、数分でメールが届く。いつでもよい
- 試験 M1 は `insert_ohlc`（毎日 00:01 UTC = 09:01 JST に 1 回だけ実行）の実行が 5 分間 無いことで発火し、5〜10 分でメールが届く。`insert_ohlc` の実行後は値 0 の点が 20 分ほど続くので、09:00〜09:30 JST は避ける。20 分待っても届かなければ、`insert_ohlc` を `load_jpx_into_bq` に変えて、取引の無い時間帯（06:30〜08:30 JST、16:20〜16:50 JST、週末）に作り直す
- 受信した日時（分まで）を Issue にコメントする（メール本文は貼らない）
- 削除（表示名が完全一致で 1 件のときだけ消す）:

  ```sh
  for n in "optionchan: 試験 M1" "optionchan: 試験 M2"; do
    ids=$(gcloud monitoring policies list --project=$P --filter="displayName=\"$n\"" --format='value(name)')
    [ "$(echo "$ids" | grep -c .)" = 1 ] && gcloud monitoring policies delete "$ids" --project=$P --quiet
  done
  gcloud monitoring policies list --project=$P --filter='displayName:"optionchan: 試験"' --format='value(displayName)' | grep -c .   # 0
  ```

### M1 を受けたとき

取り込みが止まっている。`execution_count` は最後の実行のあとも値 0 の点を 20 分ほど出し続け、欠落条件はそれを「データあり」とみなすので、実際の停止から通知までは 35 分前後かかる。上から順に見る。

1. Scheduler: `gcloud scheduler jobs describe minutely_task --location=asia-northeast1 --project=$P --format='value(state)'` が `ENABLED` か。実行の失敗が無いか: `gcloud logging read 'resource.type="cloud_scheduler_job" AND resource.labels.job_id="minutely_task"' --project=$P --freshness=30m --limit=5 --format='value(timestamp,severity)'`
2. 関数: `gcloud functions describe download_jpx --region=us-east1 --project=$P --format='value(status)'` が `ACTIVE` か
3. 最後の実行時刻: `gcloud logging read 'resource.type="cloud_function" AND resource.labels.function_name="download_jpx"' --project=$P --freshness=30m --limit=5 --format='value(timestamp,severity)'`
4. Google Cloud のステータスページ（Cloud Functions / Cloud Scheduler / Pub/Sub、us-east1・asia-northeast1）

Scheduler が `PAUSED` なら `gcloud scheduler jobs resume minutely_task --location=asia-northeast1 --project=$P`（本番の変更。オーナーが実行する）。止まっていた間のデータは取り戻せない（JPX のページは最新の値だけ）ので、欠けた時間帯を Issue に記録する。

### M2 を受けたとき

該当する関数のエラーを見る（出力は Issue に貼らず要約する）:

```sh
gcloud logging read 'resource.type="cloud_function" AND resource.labels.function_name="<関数名>" AND (severity>=ERROR OR textPayload:"finished with status")' \
  --project=$P --freshness=1h --format='value(timestamp,severity,textPayload)' | head -40
```

| 関数 | よくある原因 | 対応 |
|---|---|---|
| `download_jpx` | JPX の接続エラー・ページ形式の変化 | 接続エラーは収まるのを待つ。ページ構造が変わったなら別 Issue で `jpx_loader` を直す |
| `download_jpx` | メモリ上限（`Memory limit` の行。指標では `connection error`） | 512MB（D9）。続くなら別 Issue |
| `load_jpx_into_bq` | BigQuery の 409 Conflict / timeout | BigQuery のステータスを見る。続くなら別 Issue |
| `insert_ohlc` | クエリ量上限 | 「コスト」の「クエリ量上限を超えたとき」の再実行手順 |

単発で収まっていれば、インシデントは条件を満たさなくなってから自動で閉じる。

### 一時停止・再開

作業で意図的に取り込みを止めるとき（例: Scheduler を止めるメンテナンス）は、M1 を止めておく。

数時間の作業なら gcloud で止めてよい。止めている間は deploy workflow の plan に M1 の `enabled` の update が出るので、その間に apply を承認しない（承認すると M1 が有効に戻る）。作業後は必ず戻し、`gh workflow run deploy.yml --ref master` の plan が差分 0 になることを見る。何日も止めるときは `monitoring/m1-liveness.json` の `enabled` を `false` にする PR で行う（`tests/test_monitoring.py` の `enabled` の検査も合わせて直し、戻す PR で元に戻す）。

```sh
ID=$(gcloud monitoring policies list --project=$P --filter='displayName:"optionchan: M1"' --format='value(name)')
echo "$ID" | grep -c .   # 1 であること
gcloud monitoring policies update "$ID" --no-enabled --project=$P
# 戻すとき
gcloud monitoring policies update "$ID" --enabled --project=$P
```

### 削除（切り戻し・#14）

Terraform で管理しているので、`infra/prod/monitoring.tf` の `alert_policies` から消す（2 本とも消すなら `monitoring.tf` ごと消す）PR を出し、承認後の apply で削除する（`deletion_policy` は既定の `DELETE`）。gcloud で直接消すと、次の plan で create になる。

Terraform が使えないときは「試験発報」の削除と同じループで、表示名を `optionchan: M1 生存（download_jpx の起動が 15 分無い）` と `optionchan: M2 エラー（取り込み関数の失敗）` にして消し、`terraform state rm 'google_monitoring_alert_policy.this["m1"]'`（m2 も）で state から外す。別サーバー用の既存ポリシーは表示名が `optionchan: ` で始まらないので対象にならない。

### 検知できないもの

- JPX のページが更新されない（`download_jpx` は保存をスキップして `ok` で終わる）
- load job の非同期の失敗（`load_jpx_into_bq` は投入だけして `ok` で終わる）
- 行数の欠け・`price_time` の異常などのデータ品質

新系（#11）の鮮度判定と日次の品質クエリで解消する。それまでは閲覧時に気づいたら Issue にする。

## サービスアカウント鍵

旧 `optionchan` SA のユーザー管理鍵（2019 年発行、2 本）の使用確認と無効化（#23）。鍵の削除・SA の無効化・ロール除去は #15。鍵 ID は先頭 8 文字だけを Issue・PR・ログに書く（全桁・鍵の中身は書かない）。システム管理の鍵（Google が管理）は対象外。

| 鍵（先頭 8 文字） | 最終認証 | 観測期間 | 措置 |
|---|---|---|---|
| `3859bdc8` | 記録なし | 2020-03-12〜2026-10-07 | 2026-10-12 に無効化 |
| `db63f182` | 記録なし | 2020-03-12〜2026-10-07 | 2026-10-12 に無効化 |

結果: 2026-10-12 に照会し、2 本とも最終認証の記録なし（SA `optionchan` 単位でも記録なし。前提確認として App Engine デフォルト SA は 2026-10-07 に記録あり）で未使用と判定、2026-10-12 12:28 UTC に `keys disable` で無効化した。1 取引日後（2026-10-13）の確認（取り込み件数・認証エラー 0 件）は未実施。

### 最終認証日時の確認

Activity Analyzer（Policy Analyzer API `policyanalyzer.googleapis.com`。#23 で 2026-10-12 に有効化し、#15 の削除前確認のため有効のまま）を使う。`roles/policyanalyzer.activityAnalysisViewer`（オーナーは包含）が必要。Preview 機能。

生の出力は鍵 ID の全桁とプロジェクト番号を含むので貼らない。フィルタ（`--query-filter`）は付けずに全件を取り、`jq` で絞って先頭 8 文字に切る。

```sh
export CLOUDSDK_CORE_SHOULD_PROMPT_TO_ENABLE_API=false
P=optionchan-222710
SA=optionchan@$P.iam.gserviceaccount.com
KEYS=$(gcloud iam service-accounts keys list --iam-account=$SA --managed-by=user --project=$P --format='value(name.basename())')

# 鍵ごと（鍵 先頭 8 文字、最終認証、観測期間の開始・終了）
gcloud policy-intelligence query-activity --activity-type=serviceAccountKeyLastAuthentication --project=$P --format=json \
  | jq -r --arg keys "$KEYS" '($keys | split("\n")) as $k | .[]
    | (.activity.serviceAccountKey.fullResourceName | split("/") | last) as $id
    | select($k | index($id))
    | [$id[0:8], (.activity.lastAuthenticatedTime // "記録なし"), .observationPeriod.startTime[0:10], .observationPeriod.endTime[0:10]] | @tsv'

# SA 単位（前提確認: App Engine デフォルト SA（`optionchan-222710@...`）に最近の記録が出ること）
gcloud policy-intelligence query-activity --activity-type=serviceAccountLastAuthentication --project=$P --format=json \
  | jq -r '.[] | [(.activity.serviceAccount.fullResourceName | split("/") | last | split("@")[0]), (.activity.lastAuthenticatedTime // "記録なし")] | @tsv' \
  | sed -E 's/[0-9]{6,}/<N>/g'   # SA の名前に含まれるプロジェクト番号をマスク
```

- `lastAuthenticatedTime` が無い（「記録なし」）鍵は、観測期間内に認証されていない。時刻は日単位（常に `T07:00:00Z`）で、直近のイベントは反映が遅れることがある
- App Engine デフォルト SA にも記録が無い場合は、データが未生成（API 有効化直後など）なので時間をおいて再実行する。「記録なし」を未使用と判断しない
- 判定: 記録なし、または判定日の 90 日以上前 → 未使用。90 日以内 → 無効化せず、使用元を確認する
- Cloud Storage・Datastore のデータアクセスは監査ログに残らない（データアクセス監査ログ未設定）ので、ログ検索ではなくこの照会で確かめる

### 無効化と戻し方

無効化（鍵は削除されず、`keys enable` で戻せる）:

```sh
for K in $KEYS; do gcloud iam service-accounts keys disable $K --iam-account=$SA --project=$P; done
gcloud iam service-accounts keys list --iam-account=$SA --managed-by=user --project=$P --format='value(disabled)'   # 2 行とも True
```

戻す（無効化後に認証エラー・取り込みの欠損・手元スクリプトの失敗が出たとき）:

```sh
# 新しいシェルなら先に「最終認証日時の確認」の P・SA・KEYS を設定する（KEYS が空だと何もしない）
for K in $KEYS; do gcloud iam service-accounts keys enable $K --iam-account=$SA --project=$P; done
gcloud iam service-accounts keys list --iam-account=$SA --managed-by=user --project=$P --format='value(disabled)'   # 2 行とも False
```

あわせて変わっていないことを確かめる: `--managed-by=system` の鍵（`value(disabled)` の件数）、`gcloud iam service-accounts describe $SA --project=$P --format='value(disabled)'`（空 / `False`）、SA のロール 3 つ。

### 無効化後の確認

無効化直後と 1 取引日後（無効化後の最初の取引日の 15:45 JST 以降）に行う。`DT` は無効化時刻（UTC、例 `2026-10-12T12:28:00Z`）。

```sh
DT=2026-10-12T12:28:00Z
# 認証エラー（0 件）
gcloud logging read "severity>=ERROR AND timestamp>=\"$DT\" AND (textPayload:\"PERMISSION_DENIED\" OR textPayload:\"invalid_grant\" OR textPayload:\"Invalid JWT Signature\" OR protoPayload.status.message:\"PERMISSION_DENIED\" OR jsonPayload.message:\"PERMISSION_DENIED\")" \
  --project=$P --freshness=3d --format='value(timestamp,resource.type)' | wc -l
# この SA を principal とするログ（0 件）
gcloud logging read "timestamp>=\"$DT\" AND protoPayload.authenticationInfo.principalEmail=\"$SA\"" \
  --project=$P --freshness=3d --format='value(timestamp)' | wc -l
# 取り込み件数（取引日は 806〜822 件程度）
bq query --project_id=$P --use_legacy_sql=false '
SELECT DATE(created_at, "Asia/Tokyo") AS d, COUNT(*) AS n FROM optionchan.spot_price
WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 DAY)
GROUP BY d ORDER BY d'
```

クエリの書き間違いで 0 件を得ていないか、`severity>=ERROR` だけの検索（`--limit=1`）が動くことを一度見る。エラーが出たら上の「戻す」を行い、Issue にエラー種別と件数だけをコメントする。

## GitHub 設定

リポジトリ `terukusu/optionchan-gcp` の設定（#22。設計は `docs/architecture/target.md` 3.5 の CI 項目）。所有者 1 人で、PR は所有者が `gh pr merge --merge` でマージする前提。変更・確認は `gh api`（所有者の権限が必要）。

| 項目 | 設定 |
|---|---|
| `master` のブランチ保護（クラシック） | PR 必須・承認数 0、`enforce_admins` 有効、force push・削除禁止。必須 status check `pytest` / `terraform-plan`（#10、strict なし） |
| workflow の既定権限 | `read`。Actions による PR 承認は無効 |
| fork からの PR の workflow 実行 | 外部の貢献者はすべて承認必須 |
| 使えるアクション | GitHub 製 + `patterns_allowed` に列挙したもの（`hashicorp/setup-terraform@*`、`google-github-actions/auth@*`。#10）、SHA ピン留め必須 |
| Environment `prod` | `master` のみ、required reviewer は所有者（自己承認可）。deploy workflow の `apply` job が使う（#10） |
| secret / variables | secret `GCP_WIF_PROVIDER`（WIF プロバイダの完全名。プロジェクト番号を含む）、variables `GCP_SA_TF_PLAN` / `GCP_SA_TF_APPLY`（SA のメール）、secret `TF_ALERT_POLICY_IDS`（アラート取り込み用の一時的な値。Actions のログに出さないため secret にする。取り込み後に削除）（#10） |
| secret scanning | 有効（push protection も有効） |

承認数を 1 以上にすると自分の PR をマージできなくなる。`require_last_push_approval`（最後の push を別の人が承認）も同じ理由で `false` にする。

以下の節では、先に `R=repos/terukusu/optionchan-gcp` を設定しておく。

### ブランチ保護の設定

PUT は保護設定の全体を置き換えるので、項目を足すときも、この JSON 全体に足して送る。status check（#10）は、PR の CI が一度成功してから入れる（check が一度も報告されていないと、名前の誤りに気づけない）。

```sh
gh api -X PUT $R/branches/master/protection --input - <<'JSON'
{"required_status_checks":{"strict":false,"checks":[{"context":"pytest"},{"context":"terraform-plan"}]},
 "enforce_admins":true,
 "required_pull_request_reviews":{"dismiss_stale_reviews":false,"require_code_owner_reviews":false,"required_approving_review_count":0,"require_last_push_approval":false},
 "restrictions":null,
 "required_linear_history":false,"allow_force_pushes":false,"allow_deletions":false,
 "block_creations":false,"required_conversation_resolution":false,"lock_branch":false,"allow_fork_syncing":false}
JSON
```

### 確認

すべて `true` であること。

```sh
gh api $R/branches/master/protection --jq '.enforce_admins.enabled == true and .allow_force_pushes.enabled == false and .allow_deletions.enabled == false and .required_pull_request_reviews.required_approving_review_count == 0 and .required_pull_request_reviews.require_last_push_approval == false and .required_pull_request_reviews.require_code_owner_reviews == false and (.required_linear_history.enabled // false) == false and (.lock_branch.enabled // false) == false and .restrictions == null'
gh api $R/actions/permissions/workflow --jq '.default_workflow_permissions == "read" and .can_approve_pull_request_reviews == false'
gh api $R/actions/permissions/fork-pr-contributor-approval --jq '.approval_policy == "all_external_contributors"'
gh api $R/actions/permissions --jq '.enabled == true and .allowed_actions == "selected" and .sha_pinning_required == true'
gh api $R/actions/permissions/selected-actions --jq '.github_owned_allowed == true and .verified_allowed == false'
gh api $R/branches/master/protection --jq '.required_status_checks.strict == false and ([.required_status_checks.checks[].context] | sort) == ["pytest", "terraform-plan"]'
gh api $R/actions/permissions/selected-actions --jq '(.patterns_allowed | sort) == ["google-github-actions/auth@*", "hashicorp/setup-terraform@*"]'
gh api $R/environments/prod --jq '.deployment_branch_policy == {"protected_branches": false, "custom_branch_policies": true} and ([.protection_rules[] | select(.type == "required_reviewers")] | length) == 1'
gh api $R/environments/prod/deployment-branch-policies --jq '[.branch_policies[] | {name, type}] == [{"name": "master", "type": "branch"}]'
gh api $R --jq '.security_and_analysis.secret_scanning.status == "enabled" and .security_and_analysis.secret_scanning_push_protection.status == "enabled"'
```

### 許可アクションを足すとき

`patterns_allowed` は PUT で全体が置き換わるので、今の一覧に足した配列を送る（下の例の `owner/repo@*` は書き換える。そのまま送ると既存の一覧が消える）。workflow 側は `uses: owner/repo@<40 桁の SHA> # vX.Y.Z` で書く。

```sh
gh api $R/actions/permissions/selected-actions --jq .patterns_allowed
gh api -X PUT $R/actions/permissions/selected-actions --input - <<'JSON'
{"github_owned_allowed":true,"verified_allowed":false,"patterns_allowed":["owner/repo@*"]}
JSON
```

### PR を経ずに master へ push するとき（緊急時のみ）

```sh
gh api -X DELETE $R/branches/master/protection/enforce_admins
git push origin master        # force push はしない
gh api -X POST $R/branches/master/protection/enforce_admins
gh api $R/branches/master/protection --jq .enforce_admins.enabled   # true
```

戻し忘れを防ぐため 4 行を続けて実行し、最後の確認で `true` を見る。行ったことは Issue に記録する。

## Terraform

GCP のリソースを Terraform で管理する（#10。設計は `docs/architecture/target.md` 3.5）。terraform 1.16.4、google プロバイダ 8.4.0 に固定（`versions.tf`、`.terraform.lock.hcl`、workflow）。ローカルでは `mise exec terraform@1.16.4 -- terraform` で実行する。

| ルート | 中身 | apply する人 | state | state を読める主体 |
|---|---|---|---|---|
| `infra/bootstrap` | API 有効化、tfstate バケット 2 つ、WIF プール `github` / プロバイダ `github-actions`、SA `sa-tf-plan` / `sa-tf-apply` と権限 | 所有者がローカルから（CI は fmt / validate だけ） | `gs://optionchan-222710-tfstate-bootstrap/bootstrap/` | 所有者（+ Editor を持つデフォルト SA。#15 で外す） |
| `infra/prod` | Artifact Registry `optionchan`、監視アラート M1・M2 | CI（deploy workflow、Environment `prod` の承認後） | `gs://optionchan-222710-tfstate/prod/` | `sa-tf-plan`（読み取り）、`sa-tf-apply`（読み書き）、所有者（+ 同上） |

予算・クエリ量上限は Terraform 管理外（「コスト」）。state には通知チャネルのメールアドレスなどが入るので、state・plan の全文・ログをリポジトリ・Issue・PR に貼らない。CI も plan の全文は出さず、アドレスと操作だけを要約する。

### 流れ

1. PR を出すと `ci` workflow が `pytest` と `terraform-plan` を回す（どちらも必須 status check）。`terraform-plan` は fmt・bootstrap の validate・prod の plan（`sa-tf-plan`、`-lock=false`）を行い、「アドレス: 操作」の要約を PR にコメントする
2. 要約に意図しない update / delete / replace が無いことを見てからマージする
3. マージすると `deploy` workflow の `plan` job が差分を見る。差分が無ければ終わり。あれば `apply` job が Environment `prod` の承認を待つ
4. Actions の画面（または下のコマンド）で承認すると、`apply` job が `sa-tf-apply` でもう一度 plan して適用する（plan ファイルは公開 artifact にしない）。承認から適用までの間に master が進んだ場合は、その分も含めて適用される（`concurrency` で直列）

```sh
RUN=<run_id>   # gh run list --workflow=deploy.yml --limit=3
gh api $R/actions/runs/$RUN/pending_deployments --jq '.[] | {env: .environment.name, can: .current_user_can_approve}'
ENV_ID=$(gh api $R/actions/runs/$RUN/pending_deployments --jq '.[0].environment.id')
jq -n --argjson id "$ENV_ID" '{environment_ids: [$id], state: "approved", comment: "plan の要約を確認"}' \
  | gh api -X POST $R/actions/runs/$RUN/pending_deployments --input - >/dev/null
```

差分 0 の確認や再実行は `gh workflow run deploy.yml --ref master`（Environment `prod` は `master` からしか使えない）。

### 共通の変数

「コスト」の `P` に加えて、リポジトリ直下で設定する。GitHub の数値 ID・プロジェクト番号・WIF プロバイダの完全名は表示しない（変数・パイプで渡す）。

```sh
export CLOUDSDK_CORE_SHOULD_PROMPT_TO_ENABLE_API=false
P=optionchan-222710
R=repos/terukusu/optionchan-gcp
TF="mise exec terraform@1.16.4 -- terraform"
# ADC が無いときだけ（トークンは 1 時間で切れる）
export GOOGLE_OAUTH_ACCESS_TOKEN=$(gcloud auth print-access-token)
```

### bootstrap の初回 apply

state の置き場（bootstrap 用バケット）を先に gcloud で作り、`infra/bootstrap/imports.tf` で取り込む（local state を作らない）。

```sh
# 0. 作業前の記録（スクラッチに保存。貼らない）
gcloud monitoring policies list --project=$P --filter='displayName:"optionchan: M"' --format=json > <scratch>/policies-before.json
gcloud functions list --project=$P --format="value(name,status,runtime)" > <scratch>/functions-before.txt

# 1. bootstrap 用バケット
gcloud storage buckets create gs://$P-tfstate-bootstrap --project=$P --location=asia-northeast1 \
  --default-storage-class=STANDARD --uniform-bucket-level-access --public-access-prevention
gcloud storage buckets update gs://$P-tfstate-bootstrap --versioning

# 2. GitHub の数値 ID（コード・tfvars に置かない）
export TF_VAR_github_repository_id=$(gh api $R --jq .id)
export TF_VAR_github_owner_id=$(gh api $R --jq .owner.id)

# 3. plan → 要約を見てから apply
$TF -chdir=infra/bootstrap init -input=false
$TF -chdir=infra/bootstrap plan -input=false -no-color -out=b.tfplan > <scratch>/b-plan.log
$TF -chdir=infra/bootstrap show -json b.tfplan \
  | jq -r '.resource_changes[] | select(.change.actions != ["no-op"] or .change.importing) | "\(.address): \(.change.actions | join(","))\(if .change.importing then " (import)" else "" end)"'
$TF -chdir=infra/bootstrap apply -input=false b.tfplan
rm -f infra/bootstrap/b.tfplan

# 4. 差分 0（exit 0）
$TF -chdir=infra/bootstrap plan -input=false -detailed-exitcode > /dev/null; echo $?
```

手順 3 の要約の目安: import 1（`google_storage_bucket.tfstate["bootstrap"]`。lifecycle_rule の追加で update を伴う）、create は API 7・バケット 1・プール・プロバイダ・SA 2・IAM 9（プロジェクト 5、SA 2、バケット 2）、delete 0。

確認（すべて期待どおりであること）:

```sh
for b in $P-tfstate $P-tfstate-bootstrap; do
  gcloud storage buckets describe gs://$b --format=json \
    | jq -c '{name, location, versioning: (.versioning_enabled // false), ubla: .uniform_bucket_level_access, pap: .public_access_prevention}'
done   # ASIA-NORTHEAST1、true、true、enforced
gcloud iam workload-identity-pools providers describe github-actions --project=$P --location=global \
  --workload-identity-pool=github --format='value(attributeCondition)' | grep -c 'repository_owner_id'   # 1
gcloud iam service-accounts get-iam-policy sa-tf-apply@$P.iam.gserviceaccount.com --format=json \
  | jq '[.bindings[] | select(.role == "roles/iam.workloadIdentityUser") | .members[]] | length == 1 and (.[0] | endswith(":environment:prod"))'   # true
gcloud storage buckets get-iam-policy gs://$P-tfstate-bootstrap --format=json \
  | jq '[.bindings[].members[] | select(test("sa-tf-"))] | length'   # 0
```

変更時（権限の追加など）は PR でコードを直し、マージ後に所有者が手順 2〜4 を行う。CI は bootstrap を apply しない。

### GitHub の設定（bootstrap の後、PR を出す前）

```sh
# 許可アクション（今の一覧に足す。「GitHub 設定」の「許可アクションを足すとき」）
gh api -X PUT $R/actions/permissions/selected-actions --input - <<'JSON'
{"github_owned_allowed":true,"verified_allowed":false,"patterns_allowed":["hashicorp/setup-terraform@*","google-github-actions/auth@*"]}
JSON

# secret・variables（値を表示しない）
$TF -chdir=infra/bootstrap output -raw wif_provider_name | gh secret set GCP_WIF_PROVIDER -R terukusu/optionchan-gcp
$TF -chdir=infra/bootstrap output -raw sa_tf_plan_email | gh variable set GCP_SA_TF_PLAN -R terukusu/optionchan-gcp
$TF -chdir=infra/bootstrap output -raw sa_tf_apply_email | gh variable set GCP_SA_TF_APPLY -R terukusu/optionchan-gcp

# Environment prod（master のみ、所有者が承認）
jq -n --argjson id "$(gh api user --jq .id)" \
  '{wait_timer: 0, prevent_self_review: false, reviewers: [{type: "User", id: $id}],
    deployment_branch_policy: {protected_branches: false, custom_branch_policies: true}}' \
  | gh api -X PUT $R/environments/prod --input - >/dev/null
gh api -X POST $R/environments/prod/deployment-branch-policies -f name=master -f type=branch >/dev/null
```

`TF_ALERT_POLICY_IDS` は下の「アラートの取り込み」で設定する。必須 status check は PR の CI が一度成功してから「GitHub 設定」の「ブランチ保護の設定」で入れる。確認は「GitHub 設定」の「確認」。

### prod の state の初期化（#10、一度だけ）

`sa-tf-plan` は prod の state を読むだけなので、state がまだ無いと CI の `terraform init` が空の state とロックを書けずに 403 で失敗する。最初の PR を出す前に、所有者の資格情報で一度だけ `init` して空の state を作る。

```sh
export GOOGLE_OAUTH_ACCESS_TOKEN=$(gcloud auth print-access-token)   # ADC が無いとき
$TF -chdir=infra/prod init -input=false
gcloud storage ls gs://$P-tfstate/prod/   # default.tfstate があること
```

### アラートの取り込み（#10、一度だけ）

#9 で gcloud から作った M1・M2 を `infra/prod/imports.tf` の `import` ブロックで取り込む。ポリシー ID はコードに書かず、GitHub secret で渡す（variable だと Actions のログの env 表示に出る）。

```sh
gcloud monitoring policies list --project=$P --format=json \
  | jq -c '[.[] | select(.displayName | startswith("optionchan: M1 ") or startswith("optionchan: M2 "))
            | {key: (if (.displayName | startswith("optionchan: M1 ")) then "m1" else "m2" end), value: .name}]
           | from_entries' \
  | tee >(jq -r 'keys | join(",")' >&2) \
  | gh secret set TF_ALERT_POLICY_IDS -R terukusu/optionchan-gcp   # 標準エラーに m1,m2 と出ること
gh secret list -R terukusu/optionchan-gcp | grep -c '^TF_ALERT_POLICY_IDS'   # 1
```

PR の要約が `google_monitoring_alert_policy.this["m1"]` / `["m2"]` は `import` だけ（update が付かない）であることを見てマージする。update が付いていたら JSON → HCL の写し方（`infra/prod/monitoring.tf`）を直す。apply 後、ポリシーの `name` が作業前の記録と同じで、「監視アラート」の「確認」が `ok: true` であることを見る。

取り込みが済んだら secret を消し、差分 0 を確認する（`for_each` が空になり import は何もしない。`import` ブロックと変数は #11 以降の PR で消してよい）。

```sh
gh secret delete TF_ALERT_POLICY_IDS -R terukusu/optionchan-gcp
gh workflow run deploy.yml --ref master   # plan が差分なし、apply は skip
```

### CI が使えないときの手動 plan / apply

WIF・GCP・GitHub Actions の障害で CI が動かないときは、所有者の資格情報でローカルから行う（state のロックは apply が取る）。

```sh
$TF -chdir=infra/prod init -input=false
$TF -chdir=infra/prod plan -input=false -no-color -out=p.tfplan > <scratch>/p-plan.log   # 要約は上の jq で見る
$TF -chdir=infra/prod apply -input=false p.tfplan
rm -f infra/prod/p.tfplan
```

CI の障害で PR がマージできないときは、まず必須 status check を外す（「ブランチ保護の設定」の JSON を `"required_status_checks":null` にして PUT）。復旧したら戻す。

### state のロック解除と復旧

apply が途中で止まって `Error acquiring the state lock` になったら、他に apply が動いていないこと（`gh run list --workflow=deploy.yml`）を見てから解除する。Lock ID はエラーメッセージに出る。

```sh
$TF -chdir=infra/prod force-unlock <LOCK_ID>
```

state を壊したときは、バケットの世代（versioning、10 世代保持）から戻す。

```sh
gcloud storage ls -a gs://$P-tfstate/prod/default.tfstate   # 世代の一覧（#<generation> 付き）
gcloud storage cp gs://$P-tfstate/prod/default.tfstate#<generation> gs://$P-tfstate/prod/default.tfstate
$TF -chdir=infra/prod plan -input=false -detailed-exitcode > /dev/null; echo $?   # 0 であること
```

### 残るリスク

- Editor を持つデフォルト SA（4 つ）はプロジェクトの `roles/editor` で両方の tfstate バケットを読み書きできる（バケットの IAM では防げない）。#15 で Editor を外すまで残る
- `sa-tf-plan` はこのリポジトリのどの workflow・ブランチからも借用できる（PR の plan 用）。push できるのは所有者だけで、fork の PR は secret を受け取れない。fork・Dependabot の PR では `terraform-plan` が失敗して必須 check で止まる（Dependabot を入れるとき（#11 以降）に扱いを決める）
- `sa-tf-apply` の `roles/artifactregistry.admin` はプロジェクト単位（既存の `gcf-artifacts` も消せる）。apply は承認つきで、Terraform が管理しないリソースには触れない。#11 で見直す

## 取引カレンダーの更新

新しい取り込み（`apps/ingest`、#32）が時間外・休場日の判定に使う `config/trading_calendar.toml` の更新。JPX は毎年 2 月に翌年の祝日取引の実施日（予定）を、6 月と 12 月に翌年の上半期・下半期の実施日（確定）を公表する。**年 2 回（6 月・12 月の公表後）**、確定分まで `covered_until` を延ばす。

- 出典: 大阪取引所「祝日取引」の祝日取引実施日の表（https://www.jpx.co.jp/derivatives/rules/holidaytrading/index.html ）
- `covered_until` の 14 日前から 1 日 1 回（09:00 JST のリクエスト）WARNING `calendar_expiring` を出し、過ぎたら同じく 1 日 1 回 ERROR `calendar_outdated` を出す（新系の M2 で通知。11d）。通知が来たらこの手順で更新する
- `covered_until` を過ぎても取り込みは止めない。`closed_dates` に書いた日（「予定」の日を含む）は休み、書いていない平日は取引日として扱う（未知の休業日にも JPX を取りに行くだけで、データは増えない）。期限切れの間は休業日を判定できないので鮮度判定（`stale`）をしない

手順:

1. 年末年始の休業日 **12/31・1/1・1/2・1/3**（業務規程の休業日。祝日取引の表に無い日もある）のうち**平日のもの**と、公表の表で「実施しない」日（BCP テスト等）のうち平日のものを `closed_dates` に足す（土日は書かない）。平日の祝日で「実施する」日は書かない（取引日として扱う）。「予定」の日も書いてよい（`covered_until` より後でも使われる）
2. 大発会の前夜など、直前の夜間セッションが無い取引日が公表されたら `no_night_session_before` に足す（2025-12-30 の大納会は夜間を実施したので、通常は空）
3. `covered_until` を確定分の最終日に延ばす（予定の日だけで延ばさない）
4. テストを通して PR を出す。キーは `[sessions]` の表より前に書く（後に書くと `[sessions]` に入り、読み込み時にエラーになる）

```bash
cd apps/ingest
uv run --locked pytest -q tests/test_calendar.py
```

境界ケース（年末年始・祝日取引を実施しない日）のテストを足すときは `apps/ingest/tests/test_calendar.py` の `CASES` に追加する。

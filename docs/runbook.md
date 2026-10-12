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

## 監視アラート

現行構成の取り込み停止・失敗を検知する Cloud Monitoring のアラートポリシー 2 本（#9）。指標は Cloud Functions 組み込みの `cloudfunctions.googleapis.com/function/execution_count`（無料）で、ログ指標は作らない。通知先は既存のメール通知チャネル 1 件。Terraform 管理外（#10 で import するか判断する）で、旧系の削除（#14）で消す。

| 表示名 | 条件 | 拾うもの | 定義 |
|---|---|---|---|
| `optionchan: M1 生存（download_jpx の起動が 15 分無い）` | `download_jpx` の実行数（status を合算）が 15 分間 無い。24 時間有効 | Scheduler `minutely_task`・Pub/Sub・関数の停止 | `monitoring/m1-liveness.json` |
| `optionchan: M2 エラー（取り込み関数の失敗）` | `download_jpx` / `load_jpx_into_bq` の失敗（status が `ok` 以外）が関数ごとに 15 分間に 3 件以上、または `insert_ohlc` の失敗が 1 件以上 | crash / timeout / connection error（メモリ上限超過を含む）が続くこと | `monitoring/m2-errors.json` |

M2 を ERROR ログの一致にしないのは、既知のエラー（毎日 04:00 / 04:20 JST の `time data ''`、接続エラー、409 Conflict）が毎日 1〜6 件出て毎日鳴るのと、timeout・connection error では ERROR ログが出ないため。単発の失敗は次の分の実行で取り戻せるので、続いたときだけ通知する。

料金: 2027-09-01 以降（公式の価格ページで 2026-10-12 に確認。"no sooner than"）、メトリクス参照 3 件（M1 1、M2 2）× $0.35 + ポイント料金（約 17 万点 / 月 → 約 $0.09）で月 約 $1.14（約 170 円、税抜き）。それまでに #14 で削除する見込み。

### 変数

「コスト」の「共通の変数」の `P` と `CH` を使う（`echo "$CH" | grep -c .` が 1 であること）。通知チャネル ID はポリシー JSON に書かず、作成時に `--notification-channels` で渡す（ファイルの設定より優先される）。

### 作成

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

```sh
ID=$(gcloud monitoring policies list --project=$P --filter='displayName:"optionchan: M1"' --format='value(name)')
echo "$ID" | grep -c .   # 1 であること
gcloud monitoring policies update "$ID" --no-enabled --project=$P
# 戻すとき
gcloud monitoring policies update "$ID" --enabled --project=$P
```

### 削除（切り戻し・#14）

「試験発報」の削除と同じループで、表示名を `optionchan: M1 生存（download_jpx の起動が 15 分無い）` と `optionchan: M2 エラー（取り込み関数の失敗）` にする。別サーバー用の既存ポリシーは表示名が `optionchan: ` で始まらないので対象にならない。

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
| `master` のブランチ保護（クラシック） | PR 必須・承認数 0、`enforce_admins` 有効、force push・削除禁止。status check は #10 で追加 |
| workflow の既定権限 | `read`。Actions による PR 承認は無効 |
| fork からの PR の workflow 実行 | 外部の貢献者はすべて承認必須 |
| 使えるアクション | GitHub 製のみ + `patterns_allowed` に列挙したもの、SHA ピン留め必須 |
| secret scanning | 有効（push protection も有効） |

承認数を 1 以上にすると自分の PR をマージできなくなる。`require_last_push_approval`（最後の push を別の人が承認）も同じ理由で `false` にする。

以下の節では、先に `R=repos/terukusu/optionchan-gcp` を設定しておく。

### ブランチ保護の設定

PUT は保護設定の全体を置き換えるので、項目を足すとき（#10 で status check を足すなど）も、この JSON 全体に足して送る。

```sh
gh api -X PUT $R/branches/master/protection --input - <<'JSON'
{"required_status_checks":null,
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

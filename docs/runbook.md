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

現行構成の取り込み停止・失敗を検知する Cloud Monitoring のアラートポリシー 2 本（#9）。指標は Cloud Functions 組み込みの `cloudfunctions.googleapis.com/function/execution_count`（無料）で、ログ指標は作らない。通知先は既存のメール通知チャネル 1 件。#10 で Terraform（`infra/prod/monitoring.tf`）に取り込んだ。定義の正は `monitoring/*.json`（`tests/test_monitoring.py` で検査）で、Terraform は JSON を読んで組み立てる。変更は JSON を直して PR を出し、マージ後に deploy workflow が承認つきで反映する（「Terraform」節）。gcloud で直接変えると次の plan で差分になる。旧系の削除（#14）で消す。新しい取り込みのアラート 2 本（#35）は「新しい取り込みの監視（#35）」節にまとめた（同じ `monitoring.tf` が作る）。

| 表示名 | 条件 | 拾うもの | 定義 |
|---|---|---|---|
| `optionchan: M1 生存（download_jpx の起動が 15 分無い）` | `download_jpx` の実行数（status を合算）が 15 分間 無い。24 時間有効 | Scheduler `minutely_task`・Pub/Sub・関数の停止 | `monitoring/m1-liveness.json` |
| `optionchan: M2 エラー（取り込み関数の失敗）` | `download_jpx` / `load_jpx_into_bq` の失敗（status が `ok` 以外）が関数ごとに 15 分間に 3 件以上、または `insert_ohlc` の失敗が 1 件以上 | crash / timeout / connection error（メモリ上限超過を含む）が続くこと | `monitoring/m2-errors.json` |

M2 を ERROR ログの一致にしないのは、既知のエラー（毎日 04:00 / 04:20 JST の `time data ''`、接続エラー、409 Conflict）が毎日 1〜6 件出て毎日鳴るのと、timeout・connection error では ERROR ログが出ないため。単発の失敗は次の分の実行で取り戻せるので、続いたときだけ通知する。

料金: 2027-09-01 以降（公式の価格ページで 2026-10-12 に確認。"no sooner than"）、メトリクス参照 3 件（M1 1、M2 2）× $0.35 + ポイント料金（約 17 万点 / 月 → 約 $0.09）で月 約 $1.14（約 170 円、税抜き）。それまでに #14 で削除する見込み。新系の 2 本を足した並行稼働中の合計は「新しい取り込みの監視（#35）」の「料金」。

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

`ok` が `true` であること（`describe` の生の出力は通知チャネル ID を含むので貼らない）。#35 の後は新系の 2 本を含めて 4 本（`ingest_paused = true` の間は新系 M1 が `enabled: false` なので `ok` は `false` になる）。

```sh
gcloud monitoring policies list --project=$P --format=json \
  | jq --arg ch "$CH" '[.[] | select(.displayName | startswith("optionchan: M"))]
      | {count: length, ok: (length == 4 and all(.[]; .enabled == true and .notificationChannels == [$ch]))}'
gcloud logging metrics list --project=$P --format='value(name)' | grep -c .   # 0（ログ指標は使わない。新系の M2 はログ一致の条件で、ログ指標ではない）
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
# 表示名の完全一致で選ぶ（部分一致の "optionchan: M1" は新系の M1 にも当たる）
ID=$(gcloud monitoring policies list --project=$P --filter='displayName="optionchan: M1 生存（download_jpx の起動が 15 分無い）"' --format='value(name)')
echo "$ID" | grep -c .   # 1 であること
gcloud monitoring policies update "$ID" --no-enabled --project=$P
# 戻すとき
gcloud monitoring policies update "$ID" --enabled --project=$P
```

### 削除（切り戻し・#14）

Terraform で管理しているので、`infra/prod/monitoring.tf` の `alert_policies` から消す（2 本とも消すなら `monitoring.tf` ごと消す）PR を出し、承認後の apply で削除する（`deletion_policy` は既定の `DELETE`）。gcloud で直接消すと、次の plan で create になる。

Terraform が使えないときは「試験発報」の削除と同じループで、表示名を `optionchan: M1 生存（download_jpx の起動が 15 分無い）` と `optionchan: M2 エラー（取り込み関数の失敗）` にして消し、`terraform state rm 'google_monitoring_alert_policy.this["m1"]'`（m2 も）で state から外す。別サーバー用の既存ポリシーは表示名が `optionchan: ` で始まらないので対象にならない。新系の 2 本（`ingest_m1` / `ingest_m2`）は残す。

### 検知できないもの

- JPX のページが更新されない（`download_jpx` は保存をスキップして `ok` で終わる）
- load job の非同期の失敗（`load_jpx_into_bq` は投入だけして `ok` で終わる）
- 行数の欠け・`price_time` の異常などのデータ品質

新系（#11）の鮮度判定（新系の M2 で通知、#35）と日次の品質クエリ（#40）で解消する。それまでは閲覧時に気づいたら Issue にする。

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
| `infra/bootstrap` | API 有効化、tfstate バケット 2 つ、WIF プール `github` / プロバイダ `github-actions`、SA `sa-tf-plan` / `sa-tf-apply` と権限。取り込み用の SA `sa-ingest` / `sa-scheduler`、生 HTML のバケット `optionchan-222710-raw`、それらへの付与（プロジェクト単位・バケット単位・SA 単位。#33） | 所有者がローカルから（CI は fmt / validate だけ） | `gs://optionchan-222710-tfstate-bootstrap/bootstrap/` | 所有者（+ Editor を持つデフォルト SA。#15 で外す） |
| `infra/prod` | Artifact Registry `optionchan`、監視アラート M1・M2、BigQuery `optionchan_tokyo` とテーブル 4 つ、Cloud Run `ingest`、Scheduler `ingest`、それらへの付与（データセット単位・サービス単位。#33） | CI（deploy workflow、Environment `prod` の承認後） | `gs://optionchan-222710-tfstate/prod/` | `sa-tf-plan`（読み取り）、`sa-tf-apply`（読み書き）、所有者（+ 同上） |

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

取り込みのイメージ（#33）: タグは `src-<20 桁>` で、イメージの入力（`apps/ingest` の Dockerfile・`pyproject.toml`・`uv.lock`・`src/`、`config/trading_calendar.toml`、`schema/tokyo/`、`.dockerignore`）の git のハッシュから `.github/scripts/ingest-image.sh tag` が決める（テスト・ドキュメントの変更ではタグが変わらない）。plan はそのタグが Artifact Registry にあれば digest、無ければタグ参照を `ingest_image` に渡す。無いときは Cloud Run に差分が出るので `apply` job が動き、承認後に **GCP の認証より前に**ビルド・検査（import できる、`auth.txt`・`functions/`・テストが無い）してから `sa-tf-apply` で push し、digest で apply する。タグは不変（`immutable_tags`）。

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

### bootstrap の変更（#33: 取り込み用の SA・バケット・付与）

prod（Cloud Run・Scheduler・データセット）より先に apply する（`sa-tf-apply` の権限と `sa-ingest` / `sa-scheduler` が無いと prod の apply が失敗する）。PR のマージ後、deploy の `apply` job を承認する前に、master で上の手順 2〜4 を行う。

手順 3 の要約の目安: create 15（API `run.googleapis.com` 1、SA 2（`sa-ingest`・`sa-scheduler`）、バケット `optionchan-222710-raw` 1、バケット IAM 2、プロジェクト IAM 7（Scheduler のサービスエージェント 1、`sa-tf-apply` 3、`sa-tf-plan` 3）、SA IAM 2（`sa-tf-apply` の `serviceAccountUser`））、update / delete / replace 0。

確認（すべて期待どおりであること）:

```sh
gcloud storage buckets describe gs://$P-raw --format=json \
  | jq -c '{location, ubla: .uniform_bucket_level_access, pap: .public_access_prevention,
            soft_delete: .soft_delete_policy.retentionDurationSeconds, versioning: (.versioning_enabled // false),
            lifecycle: (.lifecycle_config // null), retention: (.retention_policy // null)}'
# {"location":"ASIA-NORTHEAST1","ubla":true,"pap":"enforced","soft_delete":"604800","versioning":false,"lifecycle":null,"retention":null}
gcloud storage buckets get-iam-policy gs://$P-raw --format=json \
  | jq -c '[.bindings[] | select(any(.members[]; startswith("serviceAccount:sa-"))) | {role, m: [.members[] | select(startswith("serviceAccount:sa-")) | split("@")[0]]}]'
# objectCreator・objectViewer の 2 件で、どちらも sa-ingest だけ
gcloud iam service-accounts list --project=$P --format='value(email)' | grep -cE '^sa-(ingest|scheduler)@'   # 2
gcloud projects get-iam-policy $P --format=json > <scratch>/iam.json
jq -r '.bindings[] | select(.members[] | test("sa-tf-(plan|apply)@")) | .role' <scratch>/iam.json | sort   # 下の 2 行を含み、projectIamAdmin が無い
#   sa-tf-plan: run.viewer・cloudscheduler.viewer・bigquery.metadataViewer
#   sa-tf-apply: run.admin・cloudscheduler.admin・bigquery.user
jq '[.bindings[] | select(.role == "roles/resourcemanager.projectIamAdmin") | .members[] | select(test("sa-tf-"))] | length' <scratch>/iam.json   # 0
jq '[.bindings[] | select(.role == "roles/cloudscheduler.serviceAgent") | .members[] | select(test("gcp-sa-cloudscheduler"))] | length' <scratch>/iam.json   # 1
jq '[.bindings[] | .members[] | select(test("sa-ingest@"))] | length' <scratch>/iam.json   # 0（プロジェクト単位の付与なし）
for sa in sa-ingest sa-scheduler; do
  gcloud iam service-accounts get-iam-policy $sa@$P.iam.gserviceaccount.com --format=json \
    | jq -c '[.bindings[] | {role, m: [.members[] | split("@")[0]]}]'
done   # どちらも roles/iam.serviceAccountUser に serviceAccount:sa-tf-apply だけ
rm -f <scratch>/iam.json
```

Scheduler のサービスエージェントが未作成で `scheduler_agent` の付与が失敗したら（API は有効なので通常は存在する）、エージェントを作ってから手順 3 をやり直す。

```sh
gcloud beta services identity create --service=cloudscheduler.googleapis.com --project=$P > /dev/null
```

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
  | jq -c '[.[] | select((.displayName | startswith("optionchan: M1 ") or startswith("optionchan: M2 ")) and (.displayName | contains(" ingest ") | not))
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

取り込みのイメージは `TF_VAR_ingest_image` で渡す（必須）。Artifact Registry に無ければ（`exists=false`）、apply の前にローカルでビルド・push して digest を得る。

```sh
.github/scripts/ingest-image.sh resolve   # tag= / image= / exists=
eval "$(.github/scripts/ingest-image.sh resolve | sed -n 's/^\(tag\|image\|exists\)=/IMG_\1=/p')"
if [ "$IMG_exists" = false ]; then   # ビルド・検査は apply job と同じ。HEAD をコミット済みの状態で行う（タグは HEAD から決まる）
  test -z "$(git status --porcelain -- apps/ingest .dockerignore)" || { echo "未コミットの変更がある。タグと中身がずれるのでビルドしない"; exit 1; }
  docker build -f apps/ingest/Dockerfile -t ingest:build .
  docker run --rm ingest:build python -c 'import optionchan_ingest.app'
  gcloud auth configure-docker asia-northeast1-docker.pkg.dev --quiet
  docker tag ingest:build "$IMG_image" && docker push "$IMG_image"
  eval "$(.github/scripts/ingest-image.sh resolve | sed -n 's/^\(tag\|image\|exists\)=/IMG_\1=/p')"
fi
export TF_VAR_ingest_image=$IMG_image   # @sha256: であること

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
- `sa-tf-apply` の `roles/run.admin`・`roles/cloudscheduler.admin`・`roles/bigquery.user` もプロジェクト単位（Scheduler はジョブ単位の IAM が無い。旧系の `minutely_task` も権限上は変えられる）。使うのは承認つきの apply だけで、Terraform は旧ジョブを管理しない（#33）
- `sa-ingest` の `roles/bigquery.dataEditor`（`optionchan_tokyo` だけ）は `tables.delete` を含む。Terraform の `deletion_protection` は API 経由の削除を止めない。消えたらタイムトラベル（7 日）で戻す（#33）

## 新しい取り込み（ingest）

#11 で旧系と並行稼働させる新しい取り込み（#33）。旧系（Cloud Functions・Scheduler `minutely_task`・GCS `optionchan`・データセット `optionchan`・アラート）とは独立していて、旧系には触れない。

| リソース | 設定 | 定義 |
|---|---|---|
| Scheduler `ingest`（asia-northeast1） | `* * * * *` Asia/Tokyo、POST `/`、OIDC（`sa-scheduler`、audience = サービスの URL）、リトライ 0、`attempt_deadline` 50 秒、`paused = var.ingest_paused` | `infra/prod/ingest.tf` |
| Cloud Run `ingest`（asia-northeast1） | 第1世代、CPU 0.5・512Mi・リクエスト中だけ CPU、同時実行 1、0〜2 インスタンス、50 秒、ingress internal、`sa-ingest`、digest 指定、環境変数 `GCP_PROJECT` / `RAW_BUCKET` / `BQ_DATASET` | `infra/prod/ingest.tf` |
| Cloud Run ジョブ `reprocess`（asia-northeast1） | 手動実行だけ（定期起動しない）。同じイメージで `python -m optionchan_ingest.reprocess`、CPU 1・512Mi、タスク 1、タイムアウト 3600 秒、リトライ 0、`sa-ingest` | `infra/prod/ingest.tf` |
| BigQuery `optionchan_tokyo` | asia-northeast1、PHYSICAL、タイムトラベル 168 時間。テーブル `spot_price` / `future_price` / `option_price` / `ingest_runs` | `schema/tokyo/`（`infra/prod/data.tf` が読む） |
| テーブル関数（最新版の選択） | `optionchan_tokyo.latest_runs` / `spot_price_latest` / `future_price_latest` / `option_price_latest`（引数 `start_at`, `end_at`） | `sql/latest/`（`infra/prod/data.tf` が読む） |
| GCS `optionchan-222710-raw` | 生 HTML（`raw/YYYY/MM/DD/YYYYMMDDTHHMM.json.gz`）。ソフト削除 7 日、期限なし | `infra/bootstrap/ingest.tf` |
| 権限 | `sa-ingest`: `optionchan_tokyo` の `dataEditor`、プロジェクトの `bigquery.jobUser`（回収・照合・再処理のクエリ、#34）、バケットの `objectCreator`・`objectViewer`（削除・上書きなし）。`sa-scheduler`: サービス `ingest` の `run.invoker` だけ | `infra/bootstrap/ingest.tf`、`infra/prod/iam.tf` |

時間外・休場日はアプリが 204 を返して何もしない（JPX にもアクセスしない）。取引時間中は 1 限月のページを毎分、2 限月のページは更新時だけ取得する。毎時 0 分（JST）は、通常の処理の後に取りこぼしを回収し、07:00 は前日分を照合する（「回収と照合」）。

### 変数

「Terraform」の「共通の変数」に加えて設定する。`T0` は確認の起点（UTC の TIMESTAMP 文字列、例 `2026-10-14 01:00:00`）。

```sh
L=asia-northeast1
T0='<YYYY-MM-DD HH:MM:SS>'   # UTC
```

### デプロイ直後の確認

構成（Cloud Run は v2 API の JSON をそのまま見る。フィールド名が Terraform と同じ）:

```sh
curl -sf -H "Authorization: Bearer $(gcloud auth print-access-token)" \
  https://run.googleapis.com/v2/projects/$P/locations/$L/services/ingest \
  | jq -c '{ingress, sa: (.template.serviceAccount | split("@")[0]), env: .template.executionEnvironment,
            conc: .template.maxInstanceRequestConcurrency, max: .template.scaling.maxInstanceCount, timeout: .template.timeout,
            limits: .template.containers[0].resources.limits, cpu_idle: .template.containers[0].resources.cpuIdle,
            digest: (.template.containers[0].image | test("@sha256:[0-9a-f]{64}$")), envs: [.template.containers[0].env[].name]}'
# INGRESS_TRAFFIC_INTERNAL_ONLY、sa-ingest、EXECUTION_ENVIRONMENT_GEN1、1、2、"50s"、{"cpu":"0.5"(か 500m),"memory":"512Mi"}、true、true、[GCP_PROJECT,RAW_BUCKET,BQ_DATASET]
gcloud scheduler jobs describe ingest --location $L --project $P --format=json \
  | jq -c '{schedule, timeZone, retry: (.retryConfig.retryCount // 0), attemptDeadline, state,
            method: .httpTarget.httpMethod, oidc: (.httpTarget.oidcToken.serviceAccountEmail | split("@")[0])}'
# "* * * * *"、"Asia/Tokyo"、0、"50s"、"ENABLED"、"POST"、"sa-scheduler"
gcloud scheduler jobs list --location $L --project $P --format='value(name.basename(),state)'   # 3 本（旧 2 + ingest）
bq show --format=json $P:optionchan_tokyo | jq -c '{location, storageBillingModel, maxTimeTravelHours}'   # asia-northeast1、PHYSICAL、"168"
for t in spot_price future_price option_price ingest_runs; do
  norm='map({name, type, mode: (.mode // "NULLABLE"), description})'
  diff <(bq show --schema --format=json $P:optionchan_tokyo.$t | jq -S "$norm") <(jq -S "$norm" schema/tokyo/$t.json) > /dev/null \
    && echo "$t schema ok" || echo "$t schema DIFF"
  bq show --format=json $P:optionchan_tokyo.$t \
    | jq -c --arg t $t '{t: $t, part: .timePartitioning, cluster: (.clustering.fields // []), rpf: (.requirePartitionFilter // false)}'
done   # schema/tokyo/tables.json と同じ
```

権限:

```sh
gcloud run services get-iam-policy ingest --region $L --project $P --format=json \
  | jq -c '[.bindings[] | {role, m: [.members[] | split("@")[0]]}]'   # [{"role":"roles/run.invoker","m":["serviceAccount:sa-scheduler"]}] だけ
```

バケット・プロジェクトの付与は「Terraform」の「bootstrap の変更（#33）」の確認と同じ。

書き込み（T0 後の取引時間中の 30 分。`ingest_runs` 10 行以上、不一致 0）:

```sh
bq query --location=$L --use_legacy_sql=false --parameter="t0:STRING:$T0" --format=prettyjson '
WITH r AS (
  SELECT * FROM `optionchan_tokyo.ingest_runs`
  WHERE snapshot_at >= TIMESTAMP_SUB(TIMESTAMP(@t0), INTERVAL 1 HOUR)
    AND started_at >= TIMESTAMP(@t0) AND started_at < TIMESTAMP_ADD(TIMESTAMP(@t0), INTERVAL 30 MINUTE)),
c AS (
  SELECT "s" k, ingest_run_id, COUNT(*) n FROM `optionchan_tokyo.spot_price`
    WHERE created_at >= TIMESTAMP_SUB(TIMESTAMP(@t0), INTERVAL 1 HOUR) GROUP BY 1, 2
  UNION ALL SELECT "f", ingest_run_id, COUNT(*) FROM `optionchan_tokyo.future_price`
    WHERE created_at >= TIMESTAMP_SUB(TIMESTAMP(@t0), INTERVAL 1 HOUR) GROUP BY 1, 2
  UNION ALL SELECT "o", ingest_run_id, COUNT(*) FROM `optionchan_tokyo.option_price`
    WHERE created_at >= TIMESTAMP_SUB(TIMESTAMP(@t0), INTERVAL 1 HOUR) GROUP BY 1, 2)
SELECT COUNT(*) runs,
  COUNTIF(IFNULL(s.n, 0) != r.rows_spot OR IFNULL(f.n, 0) != r.rows_future OR IFNULL(o.n, 0) != r.rows_option) mismatched
FROM r
LEFT JOIN (SELECT * FROM c WHERE k = "s") s USING (ingest_run_id)
LEFT JOIN (SELECT * FROM c WHERE k = "f") f USING (ingest_run_id)
LEFT JOIN (SELECT * FROM c WHERE k = "o") o USING (ingest_run_id)'
```

旧系との値の突き合わせ: 東京と US は 1 つのクエリで JOIN できないので、新系の `ingest_runs` から T0 後の `snapshot_at` を 3 つ選び、それぞれについて旧 `optionchan.<t>`（`--location=US`）と新 `optionchan_tokyo.<t>`（`--location=asia-northeast1`）を `created_at = TIMESTAMP(@ts)` で読み、旧スキーマの共通列（`schema/<t>.json` の列）だけを `TO_JSON_STRING` してキー順に並べたものをスクラッチで `diff` する。結果は差分の件数と列名だけを記録する（行は貼らない）。説明済みの差（SQ のある月の先物限月、2 限月ページの版の違い）は除いて数える。

生 HTML: T0 後の 30 分の `ingest_runs` の `source_object` がすべてあること（`gcloud storage objects describe` で数件、`gcloud storage ls gs://$P-raw/raw/YYYY/MM/DD/` を名前の `THHMM` で窓に絞った件数が `COUNT(DISTINCT snapshot_at)` と一致すること。差は `parse_failed` 等のログで説明できること）。

ログ（T0 の 5 分後から 1 時間。直後の数分は IAM の伝播待ちで 403 がありうるので、出た件数だけ記録する）:

```sh
W="resource.type=\"cloud_run_revision\" AND resource.labels.service_name=\"ingest\" AND timestamp>=\"<T0+5m の RFC3339>\" AND timestamp<\"<T0+65m の RFC3339>\""
gcloud logging read "$W AND severity>=ERROR AND jsonPayload.event:*" --project $P --format='value(jsonPayload.event)' | sort | uniq -c   # 0 件（stale だけは時刻と件数を記録）
gcloud logging read "$W AND jsonPayload.event=\"ingested\"" --project $P --format=json \
  | jq '[.[].jsonPayload.elapsed_ms] | sort | {n: length, max: .[-1], p50: .[(length / 2 | floor)]}'   # max < 45000
gcloud logging read "$W AND (textPayload:\"gunicorn\" OR textPayload:\"Booting worker\")" --project $P --format='value(severity)' | sort | uniq -c   # 起動行の severity を Issue に記録
```

時間外（平日 07:00〜07:30 JST）: `$W` の時刻を差し替えて `httpRequest.status!=204 AND log_name="projects/$P/logs/run.googleapis.com%2Frequests"` が 0 件、同じ窓の `ingest_runs`（`snapshot_at` で絞る）が 0 行。

Terraform の差分 0: `gh workflow run deploy.yml --ref master` の plan が「差分なし」、bootstrap の `plan -detailed-exitcode` が 0。差分が出たら（CPU の `0.5` の正規化など）、コードを実態に合わせる PR を出す。

### 取得の秒（#38）

JPX のページは分単位の更新時刻が変わった後、分の頭の数秒で中身が書き換わる（1〜2 秒目の取得は書き換え途中の版だった）。取引時間中は分の頭から 8 秒（`apps/ingest` の `settings.FETCH_AT_SECOND`）まで待ってから 1 限月のページを取得する。到着が 8 秒を過ぎていれば待たない。時間外（204）は待たない。待ちも全体の期限 45 秒に含まれる。

- 変更: 環境変数 `INGEST_FETCH_AT_SECOND`（0〜20、0 で待たない。数値でない・範囲外だと最初のリクエストで設定を読むときに失敗し、全リクエストが ERROR `unexpected` の 500 になる）を `infra/prod/ingest.tf` に足す PR。既定では設定しない
- 記録: 取引時間中のログに `wait_ms`（待った時間）と `page1_fetch_second`（1 限月ページの取得時刻の分内の秒）が出る。生 HTML の各ページの `fetched_at` にも取得時刻がある（封筒の形式は変えていない）
- コスト: 待ちも課金対象のインスタンス時間になる。月の vCPU 秒の上限見積もりは 約 16.5 万（無料枠 18 万。target.md 7.2）。10 秒にすると上限 約 19.4 万で超えるので、秒を延ばすときは試算し直す。無料枠は請求先アカウントで他プロジェクトと共有している

確認（「ログ」と同じ `$W`、取引時間中の窓）:

```sh
gcloud logging read "$W AND jsonPayload.event=\"ingested\"" --project $P --format=json \
  | jq '[.[].jsonPayload | {s: .page1_fetch_second, w: .wait_ms}] | {n: length, min_second: (map(.s) | min), max_wait_ms: (map(.w) | max)}'   # min_second >= 8
```

時間外の 204 が待っていないこと（平日 07:00〜07:30 JST の窓。`latency` は "0.012s" の形）:

```sh
gcloud logging read "$W AND log_name=\"projects/$P/logs/run.googleapis.com%2Frequests\" AND httpRequest.status=204" --project $P --format=json \
  | jq '[.[].httpRequest.latency | rtrimstr("s") | tonumber] | {n: length, max: max}'   # max < 1
```

月の vCPU 秒（1 取引日分を月換算する。値は秒）:

```sh
curl -sf -H "Authorization: Bearer $(gcloud auth print-access-token)" -G \
  "https://monitoring.googleapis.com/v3/projects/$P/timeSeries" \
  --data-urlencode 'filter=metric.type="run.googleapis.com/container/billable_instance_time" AND resource.labels.service_name="ingest"' \
  --data-urlencode "interval.startTime=<その日 00:00 JST の RFC3339>" --data-urlencode "interval.endTime=<翌日 00:00 JST の RFC3339>" \
  --data-urlencode 'aggregation.alignmentPeriod=86400s' --data-urlencode 'aggregation.perSeriesAligner=ALIGN_SUM' \
  --data-urlencode 'aggregation.crossSeriesReducer=REDUCE_SUM' \
  | jq '[.timeSeries[].points[].value.doubleValue] | add'   # × 23（月の取引日）× 0.5 が 18 万未満
```

### 回収と照合（#34）

毎時 0 分（JST。Scheduler の予定時刻 `X-CloudScheduler-ScheduleTime`、無ければ実時刻）のリクエストは、通常の処理（取引時間中は #38 の待ち 8 秒 + 取得・書き込み）の後に、同じ期限 45 秒の残りで次を行う。応答（status・outcome）は通常の処理の結果のまま（回収の失敗は応答を変えない）。

- 回収: 名前の時刻が直近 6 時間の生 HTML のうち、`ingest_runs` に完了記録が無く、作成から 3 分以上たったものを古い順に最大 10 件、保存済みの生 HTML から読み直して追記する（`trigger = recovery`、新しい `ingest_run_id`、今の `PARSER_VERSION`。JPX にはアクセスしない）。1 件を始めるのは残りが 15 秒以上のとき（07:00 の回は照合の分の 15 秒を残す）。作成から 3 分以上の生 HTML が無ければ BigQuery にクエリしない（時間外・休場日）
- 照合（07:00 の回だけ、回収の後）: 名前の時刻が前日 07:00〜当日 07:00 の生 HTML の数・完了記録のある数・未回収の数と名前（先頭 20 件）。生 HTML が 0 件の日はクエリしない
- 設定は `apps/ingest` の `settings.py`（`RECOVERY_*`・`RECONCILE_*`）

対象になるのは「生 HTML はあるのに完了記録が無い」スナップショット（`write_failed`・`deadline_exceeded`・作成の応答だけ失われた `storage_failed` など）。生 HTML が無い取りこぼし（JPX の障害・停止中）は埋められない。

ログ（`jsonPayload.event`）:

| event | severity | 意味 |
|---|---|---|
| `recovery` | INFO（残りがあれば WARNING） | 回収のまとめ: `listed`（窓の生 HTML）・`old_enough`（3 分以上）・`completed`・`pending`（未完了）・`recovered`・`parse_failed`・`write_failed`・`storage_failed`・`remaining`（上限・期限で残した件数。次の 0 分に回る）・`queried`・`elapsed_ms` |
| `recovered` | INFO | 1 件の回収（`object`・`ingest_run_id`・`rows`） |
| `recovery_parse_failed` | WARNING | 生 HTML から行を作れない（元の実行が ERROR `parse_failed` を出し済み）。6 時間の窓にある間は毎時試す |
| `write_failed` / `storage_failed`（`trigger=recovery`、`stage=recovery_read`） | ERROR | 回収の書き込み・読み出しの失敗。次の 0 分に再試行される |
| `recovery_item_failed` | ERROR | 1 件の処理の想定外の例外（`object`・`error_type`）。その件を飛ばして次へ進む（再処理ジョブでは `reprocess_item_failed`） |
| `recovery_failed` | ERROR | 一覧・クエリの失敗、想定外の例外（`stage`）。その回の回収はしない |
| `recovery_skipped` | WARNING | 期限の残りが足りず回収しなかった（`reason`）。次の 0 分に回る |
| `raw_reconciled` | INFO | 照合: `window_start`・`raw`・`completed`・`unresolved`・`unresolved_objects` |
| `raw_unrecovered` | ERROR | 照合で未回収が 1 件以上（内容は `raw_reconciled` と同じ）。#11 の AC 4 の日次の数値 |
| `reconcile_failed` | ERROR | 照合の一覧・クエリの失敗・期限 |

`raw_unrecovered` を受けたとき:

1. 同じ窓の `recovery_parse_failed` / `parse_failed` のログで、未回収の名前ごとに原因を見る（`gcloud logging read "... AND jsonPayload.object=\"<名前>\"" --project $P`）
2. 解析の失敗ならパーサーを直す PR（`PARSER_VERSION` を上げる）をデプロイしてから、「再処理ジョブ」で `--only-missing` を実行する
3. 書き込み・読み出しの失敗（BigQuery・GCS の障害が 6 時間以上続いた等）なら、そのまま「再処理ジョブ」で `--only-missing` を実行する
4. 結果（件数と原因の分類）を #11 にコメントする（名前の一覧・ログの全文は貼らない）

確認（0 分の回の結果。窓は「ログ」と同じ `$W` の形で、毎時 0〜1 分を含む時間帯にする）:

```sh
gcloud logging read "$W AND jsonPayload.event=(\"recovery\" OR \"recovery_skipped\" OR \"raw_reconciled\")" --project $P --format=json \
  | jq -c '.[].jsonPayload | {event, schedule_time, listed, old_enough, pending, recovered, remaining, raw, unresolved, elapsed_ms}'
# 0 分のリクエスト全体（通常の処理 + 回収）の時間 = recovery の elapsed_ms（期限の起点から）
gcloud logging read "$W AND jsonPayload.event=\"recovery\"" --project $P --format=json \
  | jq '[.[].jsonPayload.elapsed_ms] | max'   # 45000 未満
```

コスト: 対象が無い回は GCS の一覧 1 回だけ。クエリは平日で 1 日 25 回程度（1 回の課金は最小の 10MB）。確認（翌日、`creation_time` で 1 日に絞る）:

```sh
bq query --location=$L --use_legacy_sql=false --format=prettyjson '
SELECT COUNT(*) jobs, SUM(total_bytes_billed) billed
FROM `region-asia-northeast1`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE creation_time >= TIMESTAMP("<その日 00:00 JST の UTC>") AND creation_time < TIMESTAMP("<翌日 00:00 JST の UTC>")
  AND STARTS_WITH(user_email, "sa-ingest@")'   # 30 件以下、1 GiB 以下
```

### 再処理ジョブ（#34）

保存済みの生 HTML から、名前の時刻が `[start, end)` のスナップショットを今のイメージの `PARSER_VERSION` で作り直して追記する（`trigger = reprocess`。置換は追記のみ = target.md 3.1 の (b)。古い版は消さず、最新版の選択で新しい版が選ばれる）。JPX にはアクセスしない。BigQuery への追記なので、実行は所有者の判断で行う。

- `--start` / `--end`: タイムゾーン付きの ISO8601（`2026-10-13T08:45:00+09:00` か `2026-10-12T23:45:00Z`。無いと終了コード 2）
- `--only-missing`: 完了記録のあるものを飛ばす（取りこぼしだけを埋める）。付けなければ期間内をすべて作り直す（パーサーの修正後）
- `--dry-run`: 対象の件数と先頭 20 件の名前をログに出すだけ（GCS の一覧と BigQuery のクエリだけ）
- `--limit N`: 処理する件数の上限（古い順）
- 終了コード: 0 = すべて成功、1 = 失敗が 1 件以上（ERROR `reprocess_failed`、`reason=items_failed`。1 件の想定外の例外は ERROR `reprocess_item_failed` を出して次へ進む）または一覧・クエリ・設定の失敗（`stage`）、2 = 引数の誤り、3 = 期限（3300 秒）で途中終了（`reason=deadline`。同じ範囲を `--only-missing` で再実行）
- 1 件 1〜2 秒、1 日 約 1,200 件なので、長い期間は 1 日ずつ分けて実行する。回収（毎時）と同じスナップショットを同時に書いても、最新版の選択で 1 件が選ばれるだけ

```sh
gcloud run jobs execute reprocess --region $L --project $P --wait \
  --args=--start=2026-10-13T07:00:00+09:00,--end=2026-10-14T07:00:00+09:00,--only-missing,--dry-run
# ログで件数を見てから、--dry-run を外して実行する
gcloud logging read "resource.type=\"cloud_run_job\" AND resource.labels.job_name=\"reprocess\" AND jsonPayload.event=\"reprocess_done\"" \
  --project $P --limit 5 --format=json \
  | jq -c '.[].jsonPayload | {dry_run, listed, skipped_completed, targets, over_limit, processed, ok, parse_failed, write_failed, remaining}'
```

`--args` は `,` 区切り（値に `,` を含めない）。`+09:00` はそのまま渡せる。ジョブのログは `resource.type="cloud_run_job"`（サービスとは別。新系の M2（#35）の条件に入っているので、ジョブの ERROR（`reprocess_failed` 等）も通知される。`--dry-run` でも失敗すれば通知される）。

### 最新版の読み方（#34）

回収・再処理は同じスナップショットを新しい `ingest_run_id` で追記するので、生テーブルを直接数えると重複する。書き込みの途中で失敗した実行の行（完了記録の無い残骸）も生テーブルに残る。表示・集計・比較はテーブル関数で読む:

- `optionchan_tokyo.latest_runs(start_at, end_at)`: `snapshot_at` が `[start_at, end_at)` の `ingest_runs` のうち、スナップショットごとに最新の 1 件（`finished_at` の降順、同時刻なら `ingest_run_id` の降順）
- `optionchan_tokyo.spot_price_latest` / `future_price_latest` / `option_price_latest(start_at, end_at)`: `created_at` が同じ範囲で、`latest_runs` の実行の行だけ

期間の引数は必須（`option_price` のパーティションの絞り込み）。確認（T0〜T0+1 時間。行数が一致、重複 0、所属なし 0）:

```sh
bq query --location=$L --use_legacy_sql=false --parameter="t0:STRING:$T0" --format=prettyjson '
WITH lr AS (SELECT * FROM `optionchan_tokyo.latest_runs`(TIMESTAMP(@t0), TIMESTAMP_ADD(TIMESTAMP(@t0), INTERVAL 1 HOUR))),
o AS (SELECT * FROM `optionchan_tokyo.option_price_latest`(TIMESTAMP(@t0), TIMESTAMP_ADD(TIMESTAMP(@t0), INTERVAL 1 HOUR)))
SELECT
  (SELECT COUNT(*) FROM o) option_rows,
  (SELECT SUM(rows_option) FROM lr) expected_rows,
  (SELECT COUNT(*) FROM (SELECT 1 FROM o GROUP BY created_at, type, target_price, last_trading_day HAVING COUNT(*) > 1)) dup_keys,
  (SELECT COUNT(*) FROM o WHERE ingest_run_id NOT IN (SELECT ingest_run_id FROM lr)) orphan_rows,
  (SELECT COUNT(*) - COUNT(DISTINCT snapshot_at) FROM lr) dup_snapshots'
```

### 停止・再開

```sh
# 緊急停止（所有者）。次の apply で再開されないよう、すぐ ingest_paused = true の PR を出す
# 新系の M1 が 15 分ほどで鳴るので、先に「新しい取り込みの監視（#35）」の「一時停止・再開」で無効にしておく
gcloud scheduler jobs pause ingest --location $L --project $P   # この PR がマージされるまで、他の PR の deploy で Scheduler の再開（paused の差分）が出たら apply を承認しない
gcloud scheduler jobs describe ingest --location $L --project $P --format='value(state)'   # PAUSED
```

通常の停止・再開は `infra/prod/variables.tf` の `ingest_paused`（既定 `false`）の既定値を変える PR で行う。`ingest_paused = true` の間は新系の M1 も Terraform が無効にする（`monitoring.tf` の `paused_with_ingest`。再開の PR で有効に戻る）。止めている間は生 HTML も保存されないので、回収・再処理では埋められない（JPX のページは今の値しか出さない。旧系の値で補うかは #12 で扱う）。止めると毎時の回収と 07:00 の照合も止まるので、再開後に「再処理ジョブ」の `--only-missing --dry-run` で止めていた前後の未完了を確かめる。

### 切り戻し

| 段階 | 手段 | 影響 |
|---|---|---|
| R1 新系の起動を止める（最初に行う） | 上の「停止・再開」 | 新系の取り込みが止まるだけ。旧系・データ・生 HTML はそのまま |
| R2 イメージを前の版に戻す | 原因のコミットを revert する PR。plan が前の入力のタグを解決し、Artifact Registry に残っていれば（直近 5 版）その digest、無ければ apply job がビルドし直す | 1 回の apply で新しいリビジョン |
| R3 計算資源を消す | Scheduler・Cloud Run（サービス・ジョブ `reprocess`）・invoker・テーブル関数の定義を消す PR（Cloud Run は `deletion_protection = false`）。plan の要約が delete だけ（その件数）であることを見てマージ | 新系が無くなる。データセット・テーブル・バケット・SA は残す |

データは消さない（テーブルは `deletion_protection`、データセット・テーブル・バケットは `prevent_destroy` と `deletion_policy = "PREVENT"`）。並行稼働中に入った行は追記のみで、最新版の選択（テーブル関数、「最新版の読み方」）で選別する。

#34（回収・再処理・テーブル関数）だけを戻すときは、そのコミットを revert する PR（plan はサービスのイメージの update 1、ジョブ・テーブル関数の delete 5）。回収・再処理で書かれた行は消さない（同じスナップショットの 1 件だけが最新版として選ばれるので害は無い）。bootstrap の `bigquery.jobUser` は使われなければ無害なので残してよい（外すなら bootstrap の revert と所有者の apply）。

## 新しい取り込みの監視（#35）

新しい取り込み（「新しい取り込み（ingest）」節）の停止・失敗を検知するアラートポリシー 2 本。旧系の 2 本（「監視アラート」節）と同じく、定義の正は `monitoring/*.json`（`tests/test_monitoring.py` で検査）、作成・変更は `infra/prod/monitoring.tf`（キー `ingest_m1` / `ingest_m2`）で、PR のマージ後に deploy workflow の承認つき apply で反映する。通知先は同じメール通知チャネル。旧系の 2 本は #14 まで並行して残す。

| 表示名 | 条件 | 拾うもの | 定義 |
|---|---|---|---|
| `optionchan: M1 ingest 生存（2xx 応答が 15 分無い）` | Cloud Run `ingest` の `run.googleapis.com/request_count` のうち `response_code_class = "2xx"` の件数（リビジョン合算）が直近 15 分で 1 未満。データが無いときも発火（`EVALUATION_MISSING_DATA_ACTIVE`）。24 時間有効（時間外も Scheduler が毎分呼び 204 を返す）。`ingest_paused = true` の間は無効 | Scheduler `ingest`・サービスの停止、invoker の権限切れ（403）、5xx の連続 | `monitoring/ingest-m1-liveness.json` |
| `optionchan: M2 ingest エラー（ERROR ログ）` | サービス `ingest`（`cloud_run_revision`）とジョブ `reprocess`（`cloud_run_job`）の `severity>=ERROR` のログ一致。`jsonPayload.event="recovery_failed"` だけ除く。通知は 1 時間に 1 回まで、インシデントは 30 分で自動で閉じる | アプリの ERROR（下の表）、リクエストログの 5xx、メモリ超過などのシステムログ | `monitoring/ingest-m2-errors.json` |

- M1 を欠落条件（旧系の M1 の方式）にしないのは、`request_count` が最後のリクエストの後も値 0 の点を出し続けるため（2026-10-13 の実データで、403 の系列が 2 件の後 40 分以上 0 の点を出した）。欠落条件はそれを「データあり」とみなすので、2xx が無くなっても鳴らないことがある。しきい値（15 分で 1 未満）なら 0 の点でも鳴る。停止から通知まで 15〜20 分
- M2 から `recovery_failed`（毎時の回収の一覧・クエリの失敗）を除くのは、一時的な失敗で毎時単発に出て次の 0 分で回復するため。回収だけが続けて失敗しても、取り込み自体が成功していれば取りこぼしが残らないので通知は来ない。取りこぼしが実際に残っているか、照合のクエリも失敗したときに、07:00 の照合が `raw_unrecovered` か `reconcile_failed`（どちらも ERROR）を出して通知される
- gunicorn の起動・終了の行は stderr のテキストで severity が付かない（DEFAULT）ので M2 に当たらない（2026-10-13 のデプロイ 3 回で確認）。デプロイ直後の IAM 伝播待ちの 403 はリクエストログが WARNING、Scheduler 側のログ（`cloud_scheduler_job`）が ERROR だが、M2 の対象外（続けば M1 が鳴る）
- 定期クエリ（#40）の失敗は #40 で M2 の条件に足す
- 検知できないもの: `jpx_unavailable`（WARNING）が取引時間中に続く場合。JPX のページの形式が変わって更新時刻・行の要素が無くなっても「空ページ」と区別できず、ERROR が出ないまま保存されない（生 HTML も残らないので、照合・品質チェックにも出ない）。アプリ側の変更で別 Issue にする

料金（2027-09-01 以降。公式の価格ページで 2026-10-13 に確認）: メトリクス参照 1 件 $0.35 / 月。ログ一致の条件も 1 件と数え、点の料金は無い。M1 の点の料金は系列 1 本分（月 $0.1 未満の見込み）。並行稼働中は旧系 3 件 + 新系 2 件の 5 件で月 約 $1.9（約 290 円、税抜き。旧系の点の料金を含む）、#14 で旧系を消すと 2 件で月 約 $0.75。アラートの本数は一時的に 4 本（target.md 3.4 の「1〜2 本」は #14 の後）。

### 変数

「コスト」の「共通の変数」の `P` と `CH`、「新しい取り込み（ingest）」の `L`。

### 確認（apply の後）

```sh
gcloud monitoring policies list --project=$P --format=json \
  | jq --arg ch "$CH" '[.[] | select(.displayName | startswith("optionchan: M1 ingest ") or startswith("optionchan: M2 ingest "))]
      | {count: length, ok: (length == 2 and all(.[]; .enabled == true and .notificationChannels == [$ch]))}'   # count 2、ok true
gh workflow run deploy.yml --ref master -R terukusu/optionchan-gcp   # plan が「差分なし」
```

plan に `ingest_m2` の `alert_strategy`（`notification_prompts` 等）の update が出たら、API が返す形に `monitoring/ingest-m2-errors.json` を合わせる PR を出す。

### 誤報の評価

M2 の条件を過去のログに当てて件数を見る（読み取りのみ。ログの保持は 30 日）。0 件でなければ event 別に分類し、既知の事象なら除外を足す PR（除外は最小限にする。テストも直す）を出す。

```sh
F=$(jq -r '.conditions[0].conditionMatchedLog.filter' monitoring/ingest-m2-errors.json)
gcloud logging read "$F AND timestamp>=\"<開始の RFC3339>\"" --project $P --limit=1000 \
  --format='value(resource.type,logName.basename(),severity,jsonPayload.event)' | sort | uniq -c
```

M1 はバックテスト（「監視アラート」の「バックテスト」と同じ `python3 -I monitoring/backtest.py`）の `M1 ingest` の行で、2xx のある分どうしの最大の空きを見る（15 分を超えると発火）。

2026-10-13 の評価（新系のデプロイ 08:51 JST から 10:10 JST まで）: M2 の条件に当たるログ 0 件（ERROR 0 件。stdout の `ingested`、リクエストログの 200 と 403（WARNING）2 件、gunicorn の DEFAULT、監査ログ）、M1 の最大の空き 1 分。夜間・04 時台の空ページ・0 分の回収・07:00 の照合を含む 1 取引日分は、apply の翌営業日に同じコマンドで見直し、結果を #35 にコメントする（件数と event 名だけ）。

### 試験発報

本番の取り込みは止めない。同じ条件の一部だけを変えて「今発火する」試験用ポリシーを作り、メールが届いたら消す。いつでもよい（時間外も 204 のリクエストログが毎分ある）。

```sh
T=$(mktemp -d)
# M2: severity>=ERROR を severity>=INFO にする（毎分のリクエストログで当たる）
jq '.displayName = "optionchan: 試験 M2 ingest" | .documentation.subject = "[optionchan] 試験 M2 ingest（削除予定）"
    | .alertStrategy.notificationRateLimit.period = "300s"
    | .conditions[0].conditionMatchedLog.filter |= sub("severity>=ERROR"; "severity>=INFO")' \
  monitoring/ingest-m2-errors.json > $T/t2.json
# M1: しきい値を極端に大きくする（15 分の 2xx が 1 億未満 = 常に真で発火）。
# 5xx に変えて「データ無し」で鳴らす方法は、一度もデータの無い系列は評価されず鳴らなかった（2026-10-13）
jq '.displayName = "optionchan: 試験 M1 ingest" | .documentation.subject = "[optionchan] 試験 M1 ingest（削除予定）"
    | .conditions[0].conditionThreshold.thresholdValue = 100000000' \
  monitoring/ingest-m1-liveness.json > $T/t1.json
jq -r '.conditions[0].conditionMatchedLog.filter' $T/t2.json | grep -c 'severity>=INFO'   # 1
jq -r '.conditions[0].conditionThreshold.thresholdValue' $T/t1.json   # 100000000
gcloud monitoring policies create --project=$P --policy-from-file=$T/t2.json --notification-channels="$CH" --format='value(displayName)'
gcloud monitoring policies create --project=$P --policy-from-file=$T/t1.json --notification-channels="$CH" --format='value(displayName)'
rm -rf $T
```

- 試験 M2 は数分で、試験 M1 は 5〜10 分でメールが届く見込み（2026-10-13 の実施: M2 は作成から約 28 分、M1 は約 4 分でインシデントが開いた）。30 分待っても届かなければ、ポリシーのインシデント（コンソールの Alerting）と通知チャネルを見る
- 受信した日時（分まで）を #35 にコメントする（メール本文は貼らない）
- 削除（表示名が完全一致で 1 件のときだけ消す）:

  ```sh
  for n in "optionchan: 試験 M1 ingest" "optionchan: 試験 M2 ingest"; do
    ids=$(gcloud monitoring policies list --project=$P --filter="displayName=\"$n\"" --format='value(name)')
    [ "$(echo "$ids" | grep -c .)" = 1 ] && gcloud monitoring policies delete "$ids" --project=$P --quiet
  done
  gcloud monitoring policies list --project=$P --filter='displayName:"optionchan: 試験"' --format='value(displayName)' | grep -c .   # 0
  ```

### M1 を受けたとき

新しい取り込みの 2xx 応答が 15 分無い。上から順に見る（出力は Issue に貼らず要約する）。

1. Scheduler: `gcloud scheduler jobs describe ingest --location $L --project $P --format='value(state)'` が `ENABLED` か。`PAUSED` なら意図した停止か確かめる（意図した停止なら「一時停止・再開」で M1 を止める。意図しないなら `gcloud scheduler jobs resume ingest --location $L --project $P`。本番の変更なので所有者が実行する）
2. Scheduler の実行結果: `gcloud logging read 'resource.type="cloud_scheduler_job" AND resource.labels.job_id="ingest"' --project $P --freshness=30m --limit=5 --format='value(timestamp,severity,jsonPayload.status)'`。`PERMISSION_DENIED` なら invoker（`infra/prod/iam.tf`）、`UNAVAILABLE` / `DEADLINE_EXCEEDED` ならサービス側
3. サービスの応答: `gcloud logging read "resource.type=\"cloud_run_revision\" AND resource.labels.service_name=\"ingest\" AND log_name=\"projects/$P/logs/run.googleapis.com%2Frequests\"" --project $P --freshness=30m --limit=10 --format='value(timestamp,httpRequest.status)'`。403 は権限、5xx は M2 と同じ調べ方（下）、リクエストが無ければ Scheduler 側
4. サービスの状態: `gcloud run services describe ingest --region $L --project $P --format='value(status.conditions[0].status,status.latestReadyRevisionName)'`（`True` か）。直前のデプロイで壊れたなら「新しい取り込み（ingest）」の「切り戻し」の R2
5. Google Cloud のステータスページ（Cloud Run / Cloud Scheduler、asia-northeast1）

止まっていた間は生 HTML も保存されないので取り戻せない（JPX のページは今の値だけ）。欠けた時間帯を #11 に記録する。再開後、止まっていた前後の取りこぼしは「再処理ジョブ」の `--only-missing --dry-run` で確かめる。

### M2 を受けたとき

通知は 1 時間に 1 回までなので、通知の後に出た ERROR も含めて直近の ERROR をすべて見る:

```sh
F=$(jq -r '.conditions[0].conditionMatchedLog.filter' monitoring/ingest-m2-errors.json)
gcloud logging read "$F" --project $P --freshness=2h --limit=200 \
  --format='value(timestamp,resource.type,severity,jsonPayload.event,jsonPayload.stage,httpRequest.status)' | head -40
```

| event / ログ | 意味 | 対応 |
|---|---|---|
| `parse_failed` | ページの構造が想定と違う（生 HTML は保存済み） | 1 回だけなら次の分を見る。続くならパーサーを直す PR（`PARSER_VERSION` を上げる）→ デプロイ後に「再処理ジョブ」で期間を作り直す |
| `stale` | 取引時間中にページの更新時刻が日中 30 分 / 夜間 40 分以上止まっている（毎分出る） | JPX 側の遅延。旧系の取り込み・JPX のページを見て、続くなら #11 に記録。新系の不具合ではないので何もしなくてよいことが多い |
| `write_failed` / `storage_failed` | BigQuery / GCS への書き込み・読み出しの失敗 | 生 HTML があれば毎時 0 分の回収（「回収と照合」）が埋める。続くなら BigQuery / GCS のステータス・権限を見る |
| `deadline_exceeded`（`stage`） | 45 秒の期限切れ | 単発なら回収が埋める。続くなら `stage` の外部呼び出し（JPX・GCS・BigQuery）の遅延を見る |
| `recovery_item_failed` | 回収の 1 件の想定外の例外 | その件は次の 0 分に再試行される。続くなら `object` の生 HTML とコードを見る |
| `raw_unrecovered` | 07:00 の照合で未回収あり | 「回収と照合」の「`raw_unrecovered` を受けたとき」 |
| `reconcile_failed` | 照合の一覧・クエリの失敗 | 回収も失敗している（`recovery_failed` は通知しない）可能性が高い。`recovery_failed` のログの `stage` を見て、権限（`bigquery.jobUser`）・BigQuery のステータスを確認する。直ったら翌朝の照合か「再処理ジョブ」の `--only-missing --dry-run` で確かめる |
| `calendar_outdated` | 取引カレンダーの期限切れ（09:00 に 1 回） | 「取引カレンダーの更新」 |
| `unexpected` | 想定外の例外（500） | ログの `detail` を見て Issue にする |
| `reprocess_failed` / `reprocess_item_failed`（`cloud_run_job`） | 再処理ジョブの失敗 | 「再処理ジョブ」の終了コードの説明 |
| Cloud Run の監査ログの ERROR（`cloudaudit.googleapis.com`） | Terraform のデプロイ失敗など、サービス・ジョブの更新の失敗 | deploy workflow の apply のログを見る |
| リクエストログの 5xx（`jsonPayload.event` 無し） | アプリの 500（上の event と同時に出る）、または 503 / 504（インスタンスの起動失敗・タイムアウト） | 同時刻のアプリのログが無ければ、`log_name` が `run.googleapis.com%2Fvarlog%2Fsystem` のログ（メモリ超過など）を見る |

単発で収まっていれば、インシデントは 30 分で自動で閉じる。既知の事象で毎日鳴るようになったら、アプリの severity を下げるか条件に除外を足す PR を出す（「誤報の評価」）。

### 一時停止・再開

- `ingest_paused = true` の PR（「新しい取り込み（ingest）」の「停止・再開」）では、M1 も同じ apply で無効になり、戻す PR で有効に戻る。戻す apply では Scheduler の再開と M1 の有効化が同時に入るので、最初の 2xx が指標に届くまでの数分に M1 が 1 回鳴ることがある（再開直後の 1 回は無視してよい）。M2 はそのまま（止めている間は ERROR も出ない）
- gcloud での緊急停止（Scheduler の `pause`）のときは Terraform の連動が効かないので、M1 を手で無効にする。止めている間の deploy workflow の plan には Scheduler の `paused` と M1 の `enabled` の update が出る（「新しい取り込み（ingest）」の「停止・再開」と同じく、`ingest_paused = true` の PR がマージされるまで apply を承認しない）

```sh
ID=$(gcloud monitoring policies list --project=$P --filter='displayName="optionchan: M1 ingest 生存（2xx 応答が 15 分無い）"' --format='value(name)')
echo "$ID" | grep -c .   # 1 であること
gcloud monitoring policies update "$ID" --no-enabled --project=$P
# 戻すとき（Scheduler を再開し、数分待って 2xx が出始めてから。すぐ有効にするとデータ無しで 1 回鳴ることがある）
gcloud monitoring policies update "$ID" --enabled --project=$P
```

### 削除・切り戻し

`infra/prod/monitoring.tf` の `alert_policies` から `ingest_m1` / `ingest_m2` を消す PR（plan は delete 2）。急ぐときは上の `--no-enabled` で無効にする（次の plan で update が出るので、続けて PR を出す）。新しい取り込みの R3（計算資源を消す）を行うときは、同じ PR で消す（消さないと M1 が鳴り続ける）。

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

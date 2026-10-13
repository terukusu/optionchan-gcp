# runbook

本番（自分の GCP プロジェクト。コマンドでは下の「共通の変数」の `$P`）の運用手順。節は設計（`docs/architecture/target.md` 4.1）の項目ごとに、各 Issue で追記する。

## コスト

予算アラートと BigQuery の 1 日あたりクエリ量の上限（#21）。どちらも Terraform 管理外で、この runbook で管理する（#10 で判断。予算は他プロジェクトと共有の請求先アカウント側のリソースで、クエリ量上限の変更には `serviceusage.quotaAdmin` 相当が要るため、CI 用 SA に権限を渡さない）。

| 項目 | 設定 |
|---|---|
| 予算 | 表示名 `optionchan: 月額予算`。このプロジェクトのみ、暦月、500 円（税抜き、クレジット込みの実請求で比較）、実績の 50 / 80 / 100% で通知。通知先は既存の Monitoring メール通知チャネルのみ（請求先アカウントは他プロジェクトと共有のため、IAM ロール保持者への既定メールは無効） |
| クエリ量上限 | `bigquery.googleapis.com/quota/query/usage`、unit `1/d/{project}` を 32768 MiB（32 GiB/日）。Service Usage の consumer override で設定。ユーザー単位は設定しない |

予算は通知だけで課金は止めない。課金データの反映は数時間〜1 日遅れる。クエリ量上限は太平洋時間の 0 時にリセットされる。

### 共通の変数

請求先アカウント ID・プロジェクト番号・通知チャネル ID は、Issue・PR・ログに書かない。変数で扱い、出力を貼るときは件数・要約だけにする（請求先 ID は `sed -E 's/[0-9A-Z]{6}-[0-9A-Z]{6}-[0-9A-Z]{6}/<BA>/g'` でマスク）。

プロジェクト ID は自分の値を入れる（`gcloud config get-value project` で今の構成のプロジェクトが分かる）。以下の節のバケット（`$P-tfstate`・`$P-raw` など）・SA のメールもここから組み立てる。

```sh
export CLOUDSDK_CORE_SHOULD_PROMPT_TO_ENABLE_API=false   # 未有効 API の自動有効化を防ぐ
P=$(gcloud config get-value project 2>/dev/null)   # 自分の GCP プロジェクト ID。違うなら P=<自分の ID> で上書きする
: "${P:?P（GCP プロジェクト ID）を設定する}"; echo "P=$P"   # 自分のプロジェクトであること
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

1. 課金エクスポート（`PaymentInfo`、US。課金エクスポートの出力先のデータセットに合わせる）で、今月のこのプロジェクトのサービス別内訳を見る（テーブル名は請求先 ID を含むのでワイルドカードで指定する。`$P` を埋め込むためにクエリの途中で引用符を閉じている）

   ```sh
   bq query --project_id=$P --use_legacy_sql=false '
   SELECT service.description AS service,
          ROUND(SUM(cost)) AS cost,
          ROUND(SUM(IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0))) AS credits
   FROM `'"$P"'.PaymentInfo.gcp_billing_export_v1_*`
   WHERE project.id = "'"$P"'"
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

現行構成の取り込み停止・失敗を検知する Cloud Monitoring のアラートポリシー 2 本（#9）。指標は Cloud Functions 組み込みの `cloudfunctions.googleapis.com/function/execution_count`（無料）で、ログ指標は作らない。通知先は既存のメール通知チャネル 1 件。#10 で Terraform（`infra/prod/monitoring.tf`）に取り込んだ。定義の正は `monitoring/*.json`（`tests/test_monitoring.py` で検査）で、Terraform は JSON を読んで組み立てる。変更は JSON を直して PR を出し、マージ後に deploy workflow が承認つきで反映する（「Terraform」節）。gcloud で直接変えると次の plan で差分になる。旧系の停止（#14）で消す（`legacy_stopped`、「旧系の停止（#14、5a）」）。新しい取り込みのアラート 2 本（#35）は「新しい取り込みの監視（#35）」節にまとめた（同じ `monitoring.tf` が作る）。

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

Terraform で管理しているので、`infra/prod/variables.tf` の `legacy_stopped` の既定値を `true` にする PR を出し、承認後の apply で削除する（同じ apply で新旧の突き合わせの DTS 3 本も無効になる。旧系を止めるときの手順は「旧系の停止（#14、5a）」）。`false` に戻す PR で同じ定義から作り直せる。gcloud で直接消すと、次の plan で create になる。

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
P=$(gcloud config get-value project 2>/dev/null)   # 自分の GCP プロジェクト ID。違うなら P=<自分の ID> で上書きする
: "${P:?P（GCP プロジェクト ID）を設定する}"; echo "P=$P"   # 自分のプロジェクトであること
SA=optionchan@$P.iam.gserviceaccount.com
KEYS=$(gcloud iam service-accounts keys list --iam-account=$SA --managed-by=user --project=$P --format='value(name.basename())')

# 鍵ごと（鍵 先頭 8 文字、最終認証、観測期間の開始・終了）
gcloud policy-intelligence query-activity --activity-type=serviceAccountKeyLastAuthentication --project=$P --format=json \
  | jq -r --arg keys "$KEYS" '($keys | split("\n")) as $k | .[]
    | (.activity.serviceAccountKey.fullResourceName | split("/") | last) as $id
    | select($k | index($id))
    | [$id[0:8], (.activity.lastAuthenticatedTime // "記録なし"), .observationPeriod.startTime[0:10], .observationPeriod.endTime[0:10]] | @tsv'

# SA 単位（前提確認: App Engine デフォルト SA（`<PROJECT_ID>@...`）に最近の記録が出ること）
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

このリポジトリ（自分の GitHub リポジトリ `<OWNER>/<REPO>`）の設定（#22。設計は `docs/architecture/target.md` 3.5 の CI 項目）。所有者 1 人で、PR は所有者が `gh pr merge --merge` でマージする前提。変更・確認は `gh api`（所有者の権限が必要）。

| 項目 | 設定 |
|---|---|
| `master` のブランチ保護（クラシック） | PR 必須・承認数 0、`enforce_admins` 有効、force push・削除禁止。必須 status check `pytest` / `terraform-plan`（#10、strict なし） |
| workflow の既定権限 | `read`。Actions による PR 承認は無効 |
| fork からの PR の workflow 実行 | 外部の貢献者はすべて承認必須 |
| 使えるアクション | GitHub 製 + `patterns_allowed` に列挙したもの（`hashicorp/setup-terraform@*`、`google-github-actions/auth@*`。#10）、SHA ピン留め必須 |
| Environment `prod` | `master` のみ、required reviewer は所有者（自己承認可）。deploy workflow の `apply` job が使う（#10） |
| secret / variables | secret `GCP_WIF_PROVIDER`（WIF プロバイダの完全名。プロジェクト番号を含む）、variables `GCP_SA_TF_PLAN` / `GCP_SA_TF_APPLY`（SA のメール）、variables `GCP_PROJECT_ID`（GCP プロジェクト ID）/ `TF_STATE_BUCKET`（prod の state のバケット名 `<PROJECT_ID>-tfstate`）（#55。ci・deploy が Terraform の `project_id`・backend とイメージの参照に使う）、secret `TF_ALERT_POLICY_IDS`（アラート取り込み用の一時的な値。Actions のログに出さないため secret にする。取り込み後に削除）（#10） |
| secret scanning | 有効（push protection も有効） |

承認数を 1 以上にすると自分の PR をマージできなくなる。`require_last_push_approval`（最後の push を別の人が承認）も同じ理由で `false` にする。

以下の節では、先にリポジトリ直下で次を設定しておく。`gh repo view` は gh の既定リポジトリを返すので、fork を clone して upstream があると元リポジトリを返すことがある。初回に `gh repo set-default` で自分のリポジトリを選び、表示された `REPO` が自分のものであることを確かめる。

```sh
gh repo set-default   # 初回だけ。自分のリポジトリ（<OWNER>/<REPO>）を選ぶ（確認は gh repo set-default --view）
REPO=$(gh repo view --json nameWithOwner -q .nameWithOwner)
: "${REPO:?gh repo set-default で自分のリポジトリを選ぶ}"; echo "REPO=$REPO"   # 自分のリポジトリであること
R=repos/$REPO
```

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
gh api $R/actions/variables --jq '[.variables[].name] | contains(["GCP_PROJECT_ID", "GCP_SA_TF_APPLY", "GCP_SA_TF_PLAN", "TF_STATE_BUCKET"])'   # 値は表示しない
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
| `infra/bootstrap` | API 有効化、tfstate バケット 2 つ、WIF プール `github` / プロバイダ `github-actions`、SA `sa-tf-plan` / `sa-tf-apply` と権限。取り込み用の SA `sa-ingest` / `sa-scheduler`、生 HTML のバケット `<PROJECT_ID>-raw`、それらへの付与（プロジェクト単位・バケット単位・SA 単位。#33）。定期クエリの SA `sa-sq-dts`・旧 `optionchan` の 3 テーブルの読み取り・DTS 用のカスタムロール（#40）。ビューアーの SA `sa-viewer`・`bigquery.jobUser`・IAP のアクセス許可（`viewer_iap_members`、#53） | 所有者がローカルから（CI は fmt / validate だけ） | `gs://<PROJECT_ID>-tfstate-bootstrap/bootstrap/` | 所有者（+ Editor を持つデフォルト SA。#15 で外す） |
| `infra/prod` | Artifact Registry `optionchan`、監視アラート M1・M2、BigQuery `optionchan_tokyo` とテーブル 4 つ、Cloud Run `ingest`、Scheduler `ingest`、それらへの付与（データセット単位・サービス単位。#33）。定期クエリ（DTS）4 本・作業用データセット 2 つ・結果のテーブル 2 つ（#40）。Cloud Run `viewer`（IAP）・IAP のサービスエージェントの `run.invoker`・`sa-viewer` の `dataViewer`（#53） | CI（deploy workflow、Environment `prod` の承認後） | `gs://<PROJECT_ID>-tfstate/prod/` | `sa-tf-plan`（読み取り）、`sa-tf-apply`（読み書き）、所有者（+ 同上） |

プロジェクト ID・リポジトリ名はコードに既定値を持たない（誰でも clone して自分のプロジェクトで使う前提。#55）。`project_id` は `TF_VAR_project_id`、bootstrap の `github_repository` は `TF_VAR_github_repository` で渡し、backend のバケットは部分設定なので `terraform init -backend-config="bucket=..."` で渡す（prod は `$P-tfstate`、bootstrap は `$P-tfstate-bootstrap`。CI は repository variables `GCP_PROJECT_ID` / `TF_STATE_BUCKET`）。ローカルの値は tfvars に書いてもよい（`*.tfvars` は gitignore 済み。例は `infra/bootstrap/local.auto.tfvars.example`）。

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

Cloud Run のイメージ（取り込み `ingest` #33、ビューアー `viewer` #53）: タグは `src-<20 桁>` で、イメージの入力の git のハッシュから `.github/scripts/image.sh <ingest|viewer> tag` が決める（テスト・ドキュメントの変更ではタグが変わらない）。入力は ingest が `apps/ingest` の Dockerfile・`pyproject.toml`・`uv.lock`・`src/`、`config/trading_calendar.toml`、`schema/tokyo/`、`.dockerignore`（context はリポジトリ直下）、viewer が `apps/viewer` の Dockerfile・`.dockerignore`・`pyproject.toml`・`uv.lock`・`package.json`・`package-lock.json`・`src/`（context は `apps/viewer`）。plan はそのタグが Artifact Registry にあれば digest、無ければタグ参照を `ingest_image` / `viewer_image` に渡す。無いときは Cloud Run に差分が出るので `apply` job が動き、承認後に **GCP の認証より前に**ビルド・検査（ingest: import できる、`auth.txt`・`functions/`・テストが無い。viewer: `.github/scripts/viewer-image-check.sh`）してから `sa-tf-apply` で push し、digest で apply する。タグは不変（`immutable_tags`）。

### 共通の変数

「コスト」の `P` に加えて、リポジトリ直下で設定する。GitHub の数値 ID・プロジェクト番号・WIF プロバイダの完全名は表示しない（変数・パイプで渡す）。

```sh
export CLOUDSDK_CORE_SHOULD_PROMPT_TO_ENABLE_API=false
P=$(gcloud config get-value project 2>/dev/null)   # 自分の GCP プロジェクト ID。違うなら P=<自分の ID> で上書きする
: "${P:?P（GCP プロジェクト ID）を設定する}"; echo "P=$P"   # 自分のプロジェクトであること
REPO=$(gh repo view --json nameWithOwner -q .nameWithOwner)   # <OWNER>/<REPO>
: "${REPO:?gh repo set-default で自分のリポジトリを選ぶ}"; echo "REPO=$REPO"   # 自分のリポジトリであること（fork なら元リポジトリでないこと）
R=repos/$REPO
TF="mise exec terraform@1.16.4 -- terraform"
export TF_VAR_project_id=$P   # Terraform の project_id（既定値なし）
export GCP_PROJECT_ID=$P      # .github/scripts/image.sh resolve が使う
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

# 2. GitHub のリポジトリ名と数値 ID（数値 ID はコード・tfvars に置かない）
export TF_VAR_github_repository=$REPO
export TF_VAR_github_repository_id=$(gh api $R --jq .id)
export TF_VAR_github_owner_id=$(gh api $R --jq .owner.id)
#    ビューアーの IAP のアクセス許可（#53、必須の変数）は infra/bootstrap/local.auto.tfvars（gitignore 済み。コミットしない）に置く。
#    無ければ「No value for required variable」で止まる。サービス viewer の作成前は []、後は ["user:<メール>"]（「ビューアー（#13）」の手順）。
#    付与済みなのにファイルを無くしたときは [] で作り直さない（付与が消える）。gcloud iap web get-iam-policy で今の値を見て書き直す
ls infra/bootstrap/local.auto.tfvars   # 無ければ「No such file」。上の注意に従って作る

# 3. plan → 要約を見てから apply
$TF -chdir=infra/bootstrap init -input=false -backend-config="bucket=$P-tfstate-bootstrap"
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

手順 3 の要約の目安: create 15（API `run.googleapis.com` 1、SA 2（`sa-ingest`・`sa-scheduler`）、バケット `$P-raw` 1、バケット IAM 2、プロジェクト IAM 7（Scheduler のサービスエージェント 1、`sa-tf-apply` 3、`sa-tf-plan` 3）、SA IAM 2（`sa-tf-apply` の `serviceAccountUser`））、update / delete / replace 0。

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
$TF -chdir=infra/bootstrap output -raw wif_provider_name | gh secret set GCP_WIF_PROVIDER -R "$REPO"
$TF -chdir=infra/bootstrap output -raw sa_tf_plan_email | gh variable set GCP_SA_TF_PLAN -R "$REPO"
$TF -chdir=infra/bootstrap output -raw sa_tf_apply_email | gh variable set GCP_SA_TF_APPLY -R "$REPO"
# プロジェクト ID と prod の state のバケット（#55。ci・deploy の Terraform・イメージの参照が使う）
printf '%s' "$P" | gh variable set GCP_PROJECT_ID -R "$REPO"
printf '%s' "$P-tfstate" | gh variable set TF_STATE_BUCKET -R "$REPO"
gh variable list -R "$REPO" --json name -q '.[].name' | grep -cxE 'GCP_(SA_TF_PLAN|SA_TF_APPLY|PROJECT_ID)|TF_STATE_BUCKET'   # 4

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
$TF -chdir=infra/prod init -input=false -backend-config="bucket=$P-tfstate"
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
  | gh secret set TF_ALERT_POLICY_IDS -R "$REPO"   # 標準エラーに m1,m2 と出ること
gh secret list -R "$REPO" | grep -c '^TF_ALERT_POLICY_IDS'   # 1
```

PR の要約が `google_monitoring_alert_policy.this["m1"]` / `["m2"]` は `import` だけ（update が付かない）であることを見てマージする。update が付いていたら JSON → HCL の写し方（`infra/prod/monitoring.tf`）を直す。apply 後、ポリシーの `name` が作業前の記録と同じで、「監視アラート」の「確認」が `ok: true` であることを見る。

取り込みが済んだら secret を消し、差分 0 を確認する（`for_each` が空になり import は何もしない。`import` ブロックと変数は #11 以降の PR で消してよい）。

```sh
gh secret delete TF_ALERT_POLICY_IDS -R "$REPO"
gh workflow run deploy.yml --ref master   # plan が差分なし、apply は skip
```

### CI が使えないときの手動 plan / apply

WIF・GCP・GitHub Actions の障害で CI が動かないときは、所有者の資格情報でローカルから行う（state のロックは apply が取る）。

イメージは `TF_VAR_ingest_image` / `TF_VAR_viewer_image` で渡す（必須）。Artifact Registry に無ければ（`exists=false`）、apply の前にローカルでビルド・検査・push して digest を得る。

```sh
img() {  # $1 = ingest / viewer。IMG_tag / IMG_image / IMG_exists を設定する。resolve が失敗したら 1 を返す
  local out
  out=$(.github/scripts/image.sh "$1" resolve) || { echo "image.sh $1 resolve が失敗した。ここで止める"; return 1; }
  eval "$(sed -n 's/^\(tag\|image\|exists\)=/IMG_\1=/p' <<<"$out")"
}
img ingest || exit 1
if [ "$IMG_exists" = false ]; then   # ビルド・検査は apply job と同じ。HEAD をコミット済みの状態で行う（タグは HEAD から決まる）
  test -z "$(git status --porcelain -- apps/ingest config schema/tokyo .dockerignore)" || { echo "未コミットの変更がある。タグと中身がずれるのでビルドしない"; exit 1; }
  docker build -f apps/ingest/Dockerfile -t ingest:build .
  docker run --rm ingest:build python -c 'import optionchan_ingest.app'
  gcloud auth configure-docker asia-northeast1-docker.pkg.dev --quiet
  docker tag ingest:build "$IMG_image" && docker push "$IMG_image"
  img ingest
fi
export TF_VAR_ingest_image=$IMG_image   # @sha256: であること
img viewer || exit 1
if [ "$IMG_exists" = false ]; then
  test -z "$(git status --porcelain -- apps/viewer)" || { echo "未コミットの変更がある。タグと中身がずれるのでビルドしない"; exit 1; }
  docker build -t viewer:build apps/viewer
  .github/scripts/viewer-image-check.sh viewer:build
  gcloud auth configure-docker asia-northeast1-docker.pkg.dev --quiet
  docker tag viewer:build "$IMG_image" && docker push "$IMG_image"
  img viewer
fi
export TF_VAR_viewer_image=$IMG_image   # @sha256: であること

$TF -chdir=infra/prod init -input=false -backend-config="bucket=$P-tfstate"
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
| GCS `<PROJECT_ID>-raw` | 生 HTML（`raw/YYYY/MM/DD/YYYYMMDDTHHMM.json.gz`）。ソフト削除 7 日、期限なし | `infra/bootstrap/ingest.tf` |
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

通常の停止・再開は `infra/prod/variables.tf` の `ingest_paused`（既定 `false`）の既定値を変える PR で行う。`ingest_paused = true` の間は新系の M1 も Terraform が無効にする（`monitoring.tf` の `paused_with_ingest`。再開の PR で有効に戻る）。止めている間は生 HTML も保存されないので、回収・再処理では埋められない（JPX のページは今の値しか出さない。旧系が動いている間は、旧系の値で「過去データの移行（#12）」の「E 以降の取りこぼしを補う」の手順で補える）。止めると毎時の回収と 07:00 の照合も止まるので、再開後に「再処理ジョブ」の `--only-missing --dry-run` で止めていた前後の未完了を確かめる。

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
| `optionchan: M2 ingest エラー（ERROR ログ）` | サービス `ingest`（`cloud_run_revision`）・ジョブ `reprocess`（`cloud_run_job`）・定期クエリ（`bigquery_dts_config`、#40）の `severity>=ERROR` のログ一致。`jsonPayload.event="recovery_failed"` だけ除く。通知は 1 時間に 1 回まで、インシデントは 30 分で自動で閉じる | アプリの ERROR（下の表）、リクエストログの 5xx、メモリ超過などのシステムログ、品質チェックの違反・新旧の説明できない差分・定期クエリの失敗 | `monitoring/ingest-m2-errors.json` |

- M1 を欠落条件（旧系の M1 の方式）にしないのは、`request_count` が最後のリクエストの後も値 0 の点を出し続けるため（2026-10-13 の実データで、403 の系列が 2 件の後 40 分以上 0 の点を出した）。欠落条件はそれを「データあり」とみなすので、2xx が無くなっても鳴らないことがある。しきい値（15 分で 1 未満）なら 0 の点でも鳴る。停止から通知まで 15〜20 分
- M2 から `recovery_failed`（毎時の回収の一覧・クエリの失敗）を除くのは、一時的な失敗で毎時単発に出て次の 0 分で回復するため。回収だけが続けて失敗しても、取り込み自体が成功していれば取りこぼしが残らないので通知は来ない。取りこぼしが実際に残っているか、照合のクエリも失敗したときに、07:00 の照合が `raw_unrecovered` か `reconcile_failed`（どちらも ERROR）を出して通知される
- gunicorn の起動・終了の行は stderr のテキストで severity が付かない（DEFAULT）ので M2 に当たらない（2026-10-13 のデプロイ 3 回で確認）。デプロイ直後の IAM 伝播待ちの 403 はリクエストログが WARNING、Scheduler 側のログ（`cloud_scheduler_job`）が ERROR だが、M2 の対象外（続けば M1 が鳴る）
- 定期クエリ（BigQuery Data Transfer Service、#40）の失敗は `resource.type="bigquery_dts_config"` で入れている（プロジェクトの DTS は「日次の品質チェックと新旧の突き合わせ（#40）」の 4 本だけ。DTS を足すと同じく通知される）
- `jpx_unavailable`（WARNING。空ページ・接続断）は単発では通知しない（旧系の 2026-09-15〜10-12 のログで、空ページは 04:00 / 04:20 台を中心に 1 日 1〜2 回、すべて 1 分だけ）。取引時間中に続いたときは ERROR `jpx_unavailable_stale`（#42）で M2 に乗る。判定は `stale` と同じしきい値（日中 30 分 / 夜間 40 分）で、`jpx_unavailable` の分に生 HTML のバケットを直近のしきい値分だけ一覧し、最後に保存できたスナップショットがそれより古ければ出す。JPX の改版で更新時刻・行の要素が無くなった場合（空ページと区別できない）や、JPX からの拒否・障害の継続を、最大 30〜40 分遅れで拾う。このとき生 HTML は保存されないので、ページは通知を受けてから直接見る

料金（2027-09-01 以降。公式の価格ページで 2026-10-13 に確認）: メトリクス参照 1 件 $0.35 / 月。ログ一致の条件も 1 件と数え、点の料金は無い。M1 の点の料金は系列 1 本分（月 $0.1 未満の見込み）。並行稼働中は旧系 3 件 + 新系 2 件の 5 件で月 約 $1.9（約 290 円、税抜き。旧系の点の料金を含む）、#14 で旧系を消すと 2 件で月 約 $0.75。アラートの本数は一時的に 4 本（target.md 3.4 の「1〜2 本」は #14 の後）。

### 変数

「コスト」の「共通の変数」の `P` と `CH`、「新しい取り込み（ingest）」の `L`、「Terraform」の「共通の変数」の `REPO`。

### 確認（apply の後）

```sh
gcloud monitoring policies list --project=$P --format=json \
  | jq --arg ch "$CH" '[.[] | select(.displayName | startswith("optionchan: M1 ingest ") or startswith("optionchan: M2 ingest "))]
      | {count: length, ok: (length == 2 and all(.[]; .enabled == true and .notificationChannels == [$ch]))}'   # count 2、ok true
gh workflow run deploy.yml --ref master -R "$REPO"   # plan が「差分なし」
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
  --format='value(timestamp,resource.type,severity,jsonPayload.event,jsonPayload.stage,httpRequest.status,jsonPayload.message)' | cut -c1-300 | head -40
```

| event / ログ | 意味 | 対応 |
|---|---|---|
| `parse_failed` | ページの構造が想定と違う（生 HTML は保存済み） | 1 回だけなら次の分を見る。続くならパーサーを直す PR（`PARSER_VERSION` を上げる）→ デプロイ後に「再処理ジョブ」で期間を作り直す |
| `stale` | 取引時間中にページの更新時刻が日中 30 分 / 夜間 40 分以上止まっている（毎分出る） | JPX 側の遅延。旧系の取り込み・JPX のページを見て、続くなら #11 に記録。新系の不具合ではないので何もしなくてよいことが多い |
| `jpx_unavailable_stale` | 取引時間中に `jpx_unavailable`（ページが取れない・空）が続き、最後に保存できたスナップショット（`last_snapshot_at`。直近のしきい値分に 1 件も無ければこの項目は無い）から日中 30 分 / 夜間 40 分以上たった（`jpx_unavailable` の分ごとに出る。`detail` は取得の失敗（`HTTP 403`・`ConnectionError` など）か空ページの理由（`update-time is empty`・`no option rows`）） | `detail` で切り分ける。HTTP・接続のエラーなら JPX 側の障害か拒否（旧系の取り込みも止まっているか見る）。`update-time is empty` / `no option rows` が日中に続くなら JPX の改版を疑い、ページを開いて要素（`.update-time`・`.price-info-scroll .row-num`）を確かめ、パーサーを直す PR（`PARSER_VERSION` を上げる）。この間の生 HTML は無いので、取り込めなかった分は回復できない |
| `write_failed` / `storage_failed` | BigQuery / GCS への書き込み・読み出しの失敗 | 生 HTML があれば毎時 0 分の回収（「回収と照合」）が埋める。続くなら BigQuery / GCS のステータス・権限を見る |
| `deadline_exceeded`（`stage`） | 45 秒の期限切れ | 単発なら回収が埋める。続くなら `stage` の外部呼び出し（JPX・GCS・BigQuery）の遅延を見る |
| `recovery_item_failed` | 回収の 1 件の想定外の例外 | その件は次の 0 分に再試行される。続くなら `object` の生 HTML とコードを見る |
| `raw_unrecovered` | 07:00 の照合で未回収あり | 「回収と照合」の「`raw_unrecovered` を受けたとき」 |
| `reconcile_failed` | 照合の一覧・クエリの失敗 | 回収も失敗している（`recovery_failed` は通知しない）可能性が高い。`recovery_failed` のログの `stage` を見て、権限（`bigquery.jobUser`）・BigQuery のステータスを確認する。直ったら翌朝の照合か「再処理ジョブ」の `--only-missing --dry-run` で確かめる |
| `calendar_outdated` / `calendar_fallback` / `calendar_load_rejected` | 取引カレンダーの期限切れ・BigQuery の版を使えていない（09:00 に 1 回）・ingest が版を使わなかった（壊れている・不自然） | 「取引カレンダー（#63）」の「ERROR を受けたとき」 |
| `calendar_sync_failed` / `calendar_sync_rejected` / `calendar_sessions_changed`（サービス ingest の 06:40、ジョブ `calendar-sync`） | 取引カレンダーの取り込みの失敗・検証で拒否・取引時間の変更（前のカレンダーのまま） | 「取引カレンダー（#63）」の「ERROR を受けたとき」 |
| `unexpected` | 想定外の例外（500） | ログの `detail` を見て Issue にする |
| `reprocess_failed` / `reprocess_item_failed`（`cloud_run_job`） | 再処理ジョブの失敗 | 「再処理ジョブ」の終了コードの説明 |
| `bigquery_dts_config`（`jsonPayload.message` が `quality:`・`legacy_compare:` など） | 定期クエリの失敗（品質チェックの違反・新旧の説明できない差分・コピーの遅れ・権限） | 「日次の品質チェックと新旧の突き合わせ（#40）」の「失敗を受けたとき」 |
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

## 日次の品質チェックと新旧の突き合わせ（#40）

BigQuery Data Transfer Service（DTS）の 4 本。実行 SA は `sa-sq-dts`（target.md 3.5 の `sa-sq`。SA の ID は 6 文字以上）。定義は `infra/prod/sq.tf`、SQL の本文は `sql/quality/`・`sql/legacy/`（Terraform の `templatefile` がプロジェクト・データセット名を入れる）。窓はどれも `@run_time`（DTS の予定時刻）の JST の日付 D に対し `[D-1 07:00, D 07:00) JST`（日中 + 夜間の 1 日分。結果の日付の列は D-1 = 日中立会の日付）。

| 表示名 | 場所 | 時刻（JST） | 内容 | 結果 |
|---|---|---|---|---|
| `optionchan legacy-stage` | US | 07:02 | 旧 `optionchan` の 3 テーブルの窓の分を `optionchan_legacy_stage` に作り直す（+ 窓と件数の 1 行 `staged_window`）。旧系の夜間分は 06:20 頃までに入っている | — |
| `optionchan legacy-copy` | asia-northeast1 | 07:10 | `optionchan_legacy_stage` → `optionchan_legacy_copy`（データセットコピー、上書き。1 日 約 100MB） | — |
| `optionchan quality-daily` | asia-northeast1 | 07:15 | 品質チェック（target.md 3.4）。窓の `ingest_runs` と最新版（`*_latest`）を検査。ingest の 07:00 の照合・回収の後 | `optionchan_tokyo.quality_daily`（1 日 1 行）。違反が 1 件以上なら記録の後に失敗 |
| `optionchan legacy-compare` | asia-northeast1 | 07:30 | コピーと最新版を行単位で比べる（#11 の AC 5〜7） | `optionchan_tokyo.legacy_compare_daily`（1 日 1 行）。コピーが今回の窓でなければ記録せずに失敗、コピーの行数が stage の件数と違えば記録の後に失敗。説明できない差分が 1 以上なら記録の後に失敗（窓の日付が 2026-10-14 以降。「ERROR の切り替え」） |

- 失敗は Cloud Logging の `resource.type="bigquery_dts_config"`・severity ERROR で、新系の M2（「新しい取り込みの監視（#35）」）が通知する。DTS の失敗メール（実行 SA 宛て）は使わない
- 時刻を 07 時台に寄せているのは、M2 の通知が 1 時間に 1 回までのため。07:15 / 07:30 の失敗で始まる抑制は 08:15 / 08:30 に終わり、08:45 の寄り付き以降の ingest の ERROR は通知される。それでも DTS の失敗の通知の後の 1 時間は、ingest の ERROR が通知されないことがある（「M2 を受けたとき」のとおり直近の ERROR をすべて見る）
- 休場日・週末も毎日動く（データが無い窓は件数 0・違反 0 の行になる）
- 品質チェックの項目（`quality_daily` の列。説明は `schema/tokyo/quality_daily.json`）: `rowcount_mismatch`（`ingest_runs` の行数とテーブルの実件数）、`missing_table`（3 テーブルに同じスナップショット）、`dup_keys`（最新版のキーの重複）、`months_not_2`、`atm_not_1`（限月 × 種別で ATM が 1 行）、`price_time_future`（ページの更新時刻より後）、`price_time_too_old`（30 日以上前）、`sunday`。`orphan_rows`（書き込み途中の残骸）は情報で違反にしない
- 突き合わせの分類（`legacy_compare_daily` の列。説明は `schema/tokyo/legacy_compare_daily.json`、方法は `sql/legacy/compare.sql` の先頭）: 行単位（spot・future は `created_at` = T、option は T と `(last_trading_day, type, target_price)`）。JPX のページは各行が 1 分の中でばらばらに書き換わり、旧系は更新時刻の表示が変わった直後に取得するので、旧系の T の行を新系の T → 直前・直後のスナップショットの同じ行と比べる。説明済み: 直前か直後と全列一致（`*_version`、版の差）、option で約定の列（price・price_time・diff・diff_rate・volume・positions・quotation・quotation_date と、約定価格から求める iv・ギリシャ指標）が T・直前・直後のどれかと一致し気配と表示の列（bid・ask とその数量・IV、is_atm）だけ違う（`option_quote_only`、1 分の中の版の差）。option の `version`・`quote_only` は旧系の行が新系の T にもあるときだけ（新系の T の行の欠けは説明しない）、spot・future で新系の T が直前と同じ中身で旧系だけ新しい、または新系の T が約定時刻だけ進み価格の列（price・diff・diff_rate）が直前のままで、旧系が同じ約定時刻・同じ基準価格（price − diff）・同じ hv の別の価格のもの（`*_new_stale`、新系の版の遅れ・書き換え途中の版）。新系の版の遅れ（新系の T が直前と同じ中身）は 1 日に表ごと 3 行までなら説明済み、超えたらその形の行を全部 `unexplained` に数える（新系が系統的に古い版を返す不具合を捕まえる。2026-10-13 08:52 JST〜10-14 01:08 JST は spot 2 行・future 2 行で、上限 3 はそれに余裕を持たせた値）。書き換え途中の版は、旧系の版を新系も T の後 5 分の中で取っていれば裏付けあり。裏付けが無いもの（新系の価格の持ち越しの誤りと区別できない）は 1 日に表ごと 3 行までなら説明済み、超えたらその表の裏付けの無い行を全部 `unexplained` に数える（系統的な持ち越しの不具合は 1 日に多数出るので捕まえる。2026-10-13 は future 1 行で、上限 3 はそれに余裕を持たせた値）、旧系が新系の T より古い版・途中の版を読んだ（`*_legacy_older`、#61。下）、年の補完の修正（`future_contract_year`・`option_quotation_year`）、2 限月ページの版（`option_page2`。新系の T にも行があるときだけ）、新系のみ（`new_only_snapshots`、保存条件の差）。`unexplained` = 旧系のみ（新系の取りこぼし）+ 各表の説明できない行（約定の列がどの候補とも合わないなど）。例は `unexplained_samples`（先頭 20 件）
- `*_legacy_older`（#61）: 上のどれにも当たらない行のうち、次をすべて満たすもの。(1) 新系の T が旧系の行から進んでいる（spot・future は約定時刻が後、option は出来高が多く約定時刻が同じか後）。(2) 旧系の行が新系の T より前の版と合う: (a) T の 10 分前〜T の前のスナップショットの同じ行と一致（spot・future は全列、option は約定の列。気配と表示の列は問わない）= 旧系が古い版を読んだ、または (b) 新系の連続する 2 スナップショット S1 < S2（S1 は T の 10 分前以降、S2 ≤ T）の版の間 = 途中に出た版を旧系だけが読んだ。(b) は option なら出来高が S1 < 旧 < S2（狭い不等号）・約定時刻が S1 ≤ 旧 ≤ S2・基準価格（price − diff）と清算値・清算日が S1 か S2 と同じ・建玉が S1 と S2 の間。spot・future は出来高が無いので約定時刻が S1 < 旧 < S2・基準価格と hv が S1 か S2 と同じ（future は限月も）。(3) 新系の T の値の裏付け: 旧系の次のスナップショットの同じ行が新系の T と同じ値（spot・future は全列、option は約定の列）。または (a) と S2 < T の (b) に限り、新系の直後のスナップショットの同じ行が同じ値。新系の直後は新系自身の読み取りで独立ではない（同じ誤りが直後まで続けば満たす）ので、S2 = T の (b) には使わない。比べる相手が無ければ説明しない。(4) 旧系の次に同じ行があれば新系の T と矛盾しない（option は出来高・約定時刻が新系の T 以上、spot・future は約定時刻が新系の T 以後）。10 分は 2026-10-13 の実データで見た最も古い版（2 分前）に余裕を持たせた上限。限界: 新系の T の誤りが直後のスナップショットにも続き、旧系の次がそれと矛盾しない形は (a) と S2 < T の (b) で説明済みになりうる。また旧系の行が新系の直後と一致すると `version` になるので、新系の T だけが 1 回誤り旧系が正しい値を読んだ形は検出できない（#61 の前からの `version` の性質）。`tests/test_sql_bq.py` で検出できる形・できない形を確かめている
- 2026-10-13 09:32〜11:58 JST の実データ（旧系 103 スナップショット）で手元で再現した分類: spot 一致 35・版の差 66・新系の版の遅れ 2、future 一致 34・版の差 67・新系の版の遅れ 2、option 一致 97,107・版の差 227・気配と表示だけの差 13,596、説明できない差分 0
- 2026-10-13 08:52 JST〜10-14 01:08 JST（日中と夜間）を #61 の分類で手元で再現（旧系の 638 スナップショットをすべて比較、`legacy_only` 0）。説明できない行（`unexplained`）は #38（09:30 JST）より前の 21 行（option 19・spot 1・future 1）だけで、#38 より後は 0。#38 より後の説明済みの行のうち #61 の分類によるもの: `option_legacy_older` 2（古い版 1・途中の版 1、新系の直後で裏付け）、`future_legacy_older` 1（13:07 JST、直前と T の間の版、旧系の次で裏付け）、`future_new_stale` 3（新系の T が直前と同じ中身 2・裏付けの無い書き換え途中の版 1（13:44 JST））、`spot_new_stale` 2（新系の T が直前と同じ中身）。21:30 JST 以降（夜間）は説明できない行も #61 の分類による行も無い
- 料金: クエリは 1 日 約 0.5 GiB（US の作業用テーブル 約 150MB、東京の品質チェック・突き合わせ 各 数百 MB の見込み）。データセットコピーの転送 1 日 約 100MB。作業用データセットの表は 7 日で消える（止めたとき用）

### 変数

「Terraform」の「共通の変数」の `P` と、「新しい取り込み（ingest）」の `L`。

```sh
SQ=sa-sq-dts@$P.iam.gserviceaccount.com
# 表示名から DTS の設定名（projects/<番号>/locations/<場所>/transferConfigs/<ID>。番号を含むので貼らない）
tc() { bq ls --transfer_config --transfer_location="$2" --project_id=$P --format=json \
  | jq -r --arg n "optionchan $1" '.[] | select(.displayName == $n) | .name'; }
```

### bootstrap の変更（#40: `sa-sq-dts`・付与・カスタムロール）

prod の DTS より先に apply する（`sa-tf-apply` の DTS の権限と `sa-sq-dts` が無いと prod の apply が失敗する）。PR のマージ後、deploy の `apply` job を承認する前に、master で「bootstrap の初回 apply」の手順 2〜4 を行う。

手順 3 の要約の目安: create 11（SA `sa-sq-dts` 1、API `bigquerydatatransfer.googleapis.com` 1（有効化済み。宣言のみ）、カスタムロール 2（`bigqueryTransferEditor`・`bigqueryTransferViewer`）、プロジェクト IAM 3（`sa-sq-dts` の `bigquery.jobUser`、`sa-tf-apply` / `sa-tf-plan` のカスタムロール）、旧 `optionchan` の `spot_price`・`future_price`・`option_price` のテーブル IAM 3（`dataViewer`）、SA IAM 1（`sa-tf-apply` の `serviceAccountUser`））、update / delete / replace 0。旧系への変更はテーブル IAM の 3 件だけ。

確認:

```sh
for t in spot_price future_price option_price; do
  bq get-iam-policy --format=json $P:optionchan.$t | jq -c '[.bindings[]? | {role, m: [.members[] | split("@")[0]]}]'
done   # どれも [{"role":"roles/bigquery.dataViewer","m":["serviceAccount:sa-sq-dts"]}]
bq show --format=json $P:optionchan | jq '[.access[] | select((.userByEmail // "") | test("sa-"))] | length'   # 0（データセットの ACL は変えていない）
gcloud iam service-accounts get-iam-policy $SQ --format=json | jq -c '[.bindings[] | {role, m: [.members[] | split("@")[0]]}]'
# [{"role":"roles/iam.serviceAccountUser","m":["serviceAccount:sa-tf-apply"]}]
```

### 確認（prod の apply の後）

prod の plan の目安: create 13（データセット 2、テーブル 2、データセット IAM 3、テーブル IAM 2、DTS 4）、update 1（`ingest_m2` の条件）。

```sh
for loc in $L us; do
  bq ls --transfer_config --transfer_location=$loc --project_id=$P --format=json \
    | jq -c '.[] | {displayName, dataSourceId, schedule, disabled: (.disabled // false), owner: (.ownerInfo.email // "" | split("@")[0])}'
done   # 東京 3 本・US 1 本（#49 の後は東京に atm-hourly（22:40）が加わり 4 本）、owner はすべて sa-sq-dts、schedule は every day 22:15 / 22:10 / 22:30 / 22:02（UTC）
```

`ownerInfo` が出ない場合は `bq show --transfer_config --format=json "$(tc quality-daily $L)" | jq .ownerInfo` で見る。plan に DTS の `params` などの update が出続けるなら、API が返す形に `infra/prod/sq.tf` を合わせる PR を出す。

### 日次の確認

毎朝 07:45 以降（前日 07:00〜当日 07:00 の窓の結果）。件数・要約だけを #11 に記録する（行の全文は貼らない）。

```sh
bq query --location=$L --use_legacy_sql=false --format=pretty '
SELECT window_date, snapshots, violations, rowcount_mismatch, missing_table, dup_keys, months_not_2, atm_not_1,
  price_time_future, price_time_too_old, sunday, orphan_rows
FROM optionchan_tokyo.quality_daily ORDER BY window_date DESC LIMIT 7'
bq query --location=$L --use_legacy_sql=false --format=pretty '
SELECT compare_date, copy_complete, legacy_snapshots, new_snapshots, new_only_snapshots, legacy_only_snapshots,
  spot_version + future_version + option_version AS version_rows, spot_new_stale + future_new_stale AS new_stale_rows,
  option_quote_only, spot_legacy_older + future_legacy_older + option_legacy_older AS legacy_older_rows,
  spot_unexplained, future_unexplained, option_unexplained, unexplained, fail_on_unexplained,
  legacy_condition_candidates, legacy_condition_missed
FROM optionchan_tokyo.legacy_compare_daily ORDER BY compare_date DESC LIMIT 7'
# 実行の状態（最新 3 回）
for n in quality-daily legacy-compare legacy-copy; do
  bq ls --transfer_run --run_attempt=LATEST --max_results=3 --format=json "$(tc $n $L)" | jq -c --arg n "$n" '.[] | {n: $n, runTime, state}'
done
bq ls --transfer_run --run_attempt=LATEST --max_results=3 --format=json "$(tc legacy-stage us)" | jq -c '.[] | {runTime, state}'
```

`new_stale_rows`（`*_new_stale`）と `legacy_older_rows`（`*_legacy_older`）は説明済みだが、新系の解析の誤りを吸いうる分類なので件数も見る（2026-10-13 08:52 JST〜10-14 01:08 JST は 5 行・3 行）。前の日より急増したら、その日の該当行を新旧で並べて見る（「ERROR の切り替え」）。

前日の行が無い（実行自体が無い）ことは M2 では通知されない。上の確認で前日の行があることを見る。

### 失敗を受けたとき（M2 の `bigquery_dts_config`）

```sh
gcloud logging read 'resource.type="bigquery_dts_config" AND severity>=ERROR' --project $P --freshness=1d --limit=20 \
  --format='value(timestamp,resource.labels.location,resource.labels.config_id,jsonPayload.message)' | cut -c1-400
bq ls --transfer_config --transfer_location=$L --project_id=$P --format=json \
  | jq -r '.[] | (.name | split("/")[-1]) + " " + .displayName'   # config_id → 表示名（US は --transfer_location=us）
```

| メッセージ | 意味 | 対応 |
|---|---|---|
| `quality: {...}` | 品質チェックの違反（JSON は `quality_daily` の行と同じ） | 下の「品質チェックの違反」 |
| `legacy_compare: unexplained N on <日付> {...}` | 説明できない差分（記録は済み。「ERROR の切り替え」が有効で、窓の日付が下限以降のときだけ出る） | 下の「説明できない差分」 |
| `legacy_compare: copy incomplete on <日付> ...` | コピーの 3 テーブルの行数が stage の件数と違う（記録は済み、`copy_complete = false`。その日の比較は当てにならない） | `legacy-copy` の実行の状態とログを見る。直ったら「再実行」で copy → compare |
| `legacy_compare: copy is not for the window ...` | `legacy-stage` か `legacy-copy` が失敗・遅延して、コピーが前日分でない（記録していない） | `legacy-stage`（US）と `legacy-copy` の実行の状態とログを見る。直ったら「再実行」で stage → copy → compare |
| `... is in the future (run_time ...)` | 手動実行の `run_time` が未来の窓を指している | `run_time` を直して再実行 |
| `Access Denied` / `Permission ...` | `sa-sq-dts` の権限（bootstrap の `sq.tf`・prod の `sq.tf`） | plan で付与の差分を見る。旧テーブルの読み取りは bootstrap |
| その他（クォータ・内部エラー） | BigQuery / DTS の障害 | 翌日の実行で戻るか見る。前日分は「再実行」 |

**品質チェックの違反**: 項目ごとに原因を見る（窓は `quality_daily` の `window_start` / `window_end`。`bq query` は必ず `created_at` / `snapshot_at` で絞る）。

- `rowcount_mismatch` / `missing_table`: 書き込みの途中失敗が最新版に入った可能性。`ingest_runs` と各テーブルを `ingest_run_id` ごとに数えて差のある実行を探し、生 HTML から「再処理ジョブ」で作り直す（新しい `ingest_run_id` が最新版になる）
- `dup_keys` / `months_not_2` / `atm_not_1`: パーサーか JPX のページの変化。取り込み時の構造の検査（`parse_failed`）をすり抜けたので、パーサーを直す PR（`PARSER_VERSION` を上げる）→ 再処理
- `price_time_*` / `sunday`: 約定日時の年・日付の補完（`price_time_raw` と比べる）。祝日取引の日付は違反にしていない。JPX の表示の変化ならパーサーを直す
- 誤検知（品質チェックの定義の誤り）なら、`sql/quality/quality_daily.sql` と `tests/test_sql_bq.py` を直す PR

結果を #11 にコメントする（項目と件数、原因の分類）。

**説明できない差分**: `unexplained_samples`（`kind`・`created_at`・`detail`）から見る。

- `legacy_only`: 旧系にあって新系の完了記録に無いスナップショット（新系の取りこぼし）。その時刻の ingest のログ（`jpx_unavailable`・`skipped_no_update`・`parse_failed` など）と生 HTML の有無を見る
- `spot` / `future` / `option`: 新旧の値の違い。その `created_at`（と直前・直後）の新旧の行を並べて見る。旧系は US なので `bq query --location=US` で旧 `optionchan`、新系は `optionchan_tokyo.*_latest` を同じ時刻で引く（`option_price` は `created_at` で絞る）。新系のパーサーの不具合なら直す PR → 再処理。説明の付く新しい種類なら `sql/legacy/compare.sql` の分類と `tests/test_sql_bq.py` に足す PR（#11 にも定義を追記する）
- その日は #11 の「連続 10 営業日」が途切れる（分類を直して再実行した結果が 0 なら、その日は 0 として数えてよい。直した内容を #11 に記録する）

### 再実行・バックフィル

DTS の定期クエリは作成直後に 1 回ずつ自動で実行され、4 本が同時に走る。2026-10-13 の作成時は、legacy-stage が `staged_window` を作り直している途中に legacy-copy が走り、コピー先の `staged_window` が空になった（メタデータは 1 行、クエリでは 0 行）。legacy-compare が `copy is not for the window ... (staged: null)` で失敗したら、legacy-copy を再実行してから legacy-compare を再実行する。毎朝の実行は 07:02 → 07:10 → 07:30 の順なので起きない。

本番の結果テーブル・作業用データセットへの書き込みなので、実行は所有者が行う。同じ日を再実行しても行は置き換わるだけ（重複しない）。窓の日付 W（日中立会の日付、JST）の分は、`run_time` を W の UTC の 22:02 / 22:15 / 22:30（JST の W+1 日 07:02 / 07:15 / 07:30）にする。突き合わせは stage → copy → compare の順に、前の実行が `SUCCEEDED` になってから次を実行する。

```sh
W=2026-10-14   # 窓の日付
bq mk --transfer_run --run_time="${W}T22:15:00Z" "$(tc quality-daily $L)"
bq mk --transfer_run --run_time="${W}T22:02:00Z" "$(tc legacy-stage us)"
# stage が SUCCEEDED になってから（コピーは run_time に関係なく今の作業用データセットを運ぶ）
bq mk --transfer_run --run_time="$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$(tc legacy-copy $L)"
# copy が SUCCEEDED になってから
bq mk --transfer_run --run_time="${W}T22:30:00Z" "$(tc legacy-compare $L)"
bq ls --transfer_run --run_attempt=LATEST --max_results=1 --format=json "$(tc legacy-compare $L)" | jq -c '.[] | {runTime, state}'
```

複数の日をさかのぼるときは 1 日ずつ stage → copy → compare を繰り返す（作業用データセットは 1 日分だけ持つ）。旧系の 1 日分（旧 option 約 80 万行）の stage は約 150MB のスキャン。

### ERROR の切り替え（説明できない差分）

`legacy-compare` は説明できない差分（`unexplained`）が 1 以上なら、記録の後に失敗する（M2 で通知。07:30 の失敗なので、通知の抑制は 08:30 に終わる）。切り替えは `infra/prod/sq.tf` の 2 つの local:

- `legacy_compare_fail_on_unexplained`（true で有効）
- `legacy_compare_fail_from`（窓の日付の下限。これより前の日は、再実行しても記録だけで失敗させない）。`2026-10-14` にしているのは、2026-10-13 の分が #38 の前（09:30 JST まで）の説明できない行（option 19 行）を含むため

記録の `fail_on_unexplained` 列は、この 2 つを合わせた「その実行で ERROR にする設定だったか」（下限より前の日は false）。経緯: 2026-10-13 の記録で分類を確かめ、#61 で旧系の古い版・途中の版（`*_legacy_older`）と新系の書き換え途中の版（`*_new_stale`）を足した（2026-10-13 の結果は「突き合わせの分類」の再現の項）。

失敗を受けたら「説明できない差分」の手順で原因を調べ、新系の不具合か、分類の漏れ（説明の付く新しい種類）かを決める。分類の漏れなら `sql/legacy/compare.sql` と `tests/test_sql_bq.py` を直す PR を出し、その日を「再実行」して 0 になることを確かめる（直した分類は #11 にコメントする）。

- `option_quote_only` の比率（`option_quote_only / option_compared`）も見る。2026-10-13 の 09:32〜11:58 JST は約 12%、08:52〜21:29 JST 全体は約 8%。気配の列だけを誤る新系のパーサーの不具合はこの分類に吸収されて検出できないので、比率が大きく上がったら気配の列を新旧で並べて見る
- `*_new_stale` と `*_legacy_older` も件数を見る（2026-10-13 08:52 JST〜10-14 01:08 JST は `spot_new_stale` 2・`future_new_stale` 3（うち裏付けの無い書き換え途中の版 1）・`future_legacy_older` 1・`option_legacy_older` 2）。`*_new_stale` のうち、新系の T が直前と同じ中身の行と裏付けの無い書き換え途中の行は、それぞれ 1 日に表ごと 3 行を超えると全部 `unexplained` になる。どちらも新系の T の値が直前と同じか旧系より進んでいる行を説明済みにするので、急増したら新系の解析の誤りがすり抜けていないかを疑い、その日の行を新旧で並べて見る
- 分類が落ち着くまで止めたいときは `legacy_compare_fail_on_unexplained = false` にする PR（plan は DTS `legacy_compare` の `params` の update 1）

`copy_complete = false`（コピーの欠け）とコピーの窓の不一致は、切り替えに関係なく失敗する（比べた結果が当てにならないため）。

### #11 の判定に使う数

- **連続 10 営業日（AC 5）**: `legacy_snapshots > 0` の日を営業日として、`unexplained = 0`（かつ `copy_complete`）の日が途切れずに 10 日続いたら満たす。「説明できない差分」の定義は行単位（上の「突き合わせの分類」。#11 にコメントで明記）。突き合わせのデプロイ後の日から数える（#11）。行の無い営業日（実行が無かった日）は「再実行」で埋めてから数える

  ```sh
  bq query --location=$L --use_legacy_sql=false --format=pretty '
  SELECT compare_date, unexplained,
    COUNTIF(unexplained = 0 AND copy_complete) OVER (ORDER BY compare_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS zero_days_in_last10
  FROM optionchan_tokyo.legacy_compare_daily
  WHERE legacy_snapshots > 0
  ORDER BY compare_date DESC LIMIT 15'   # zero_days_in_last10 = 10 の行があれば、その日までの 10 営業日が連続で 0
  ```

- **取りこぼし率（AC 6）**: 観察期間の合計で、新系 = `SUM(legacy_only_snapshots) / SUM(legacy_snapshots)`、旧系 = `SUM(legacy_condition_missed) / SUM(legacy_condition_candidates)`
- **保存条件の新旧差（AC 7）**: 日ごとの `new_snapshots`・`legacy_snapshots`・`new_only_snapshots`・`legacy_only_snapshots`

### 試験発報

本番の DTS は変えない。`ERROR()` だけを呼ぶ試験用のスケジュールされたクエリを `sa-sq-dts` で作り、1 回実行して M2 のメールが届くことを見たら消す（所有者が実行。自動のスケジュールは付けない）。

```sh
bq mk --transfer_config --project_id=$P --location=$L --data_source=scheduled_query \
  --display_name='optionchan trial-error' --service_account_name=$SQ --no_auto_scheduling \
  --params='{"query":"IF TRUE THEN SELECT ERROR(\"optionchan trial: M2 test\"); END IF;"}'
bq mk --transfer_run --run_time="$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$(tc trial-error $L)"
# 数分後: ERROR のログが 1 件出ていること
gcloud logging read 'resource.type="bigquery_dts_config" AND severity>=ERROR AND jsonPayload.message:"optionchan trial"' \
  --project $P --freshness=30m --limit=5 --format='value(timestamp,severity)'
# メールが届いたら消す（表示名が完全一致で 1 件のときだけ）
ids=$(tc trial-error $L); [ "$(echo "$ids" | grep -c .)" = 1 ] && bq rm -f --transfer_config "$ids"
```

- 受信した日時（分まで）を #40 にコメントする（メール本文は貼らない）。M2 の通知は 1 時間に 1 回までなので、直前に他の M2 が鳴っていたら 1 時間あけてから行う
- 宛先テーブルが無いことで作成が拒否されたら、`--params` の `query` の先頭に `CREATE TEMP TABLE t AS SELECT 1 AS x;` を足す（DML / DDL を含むスクリプトにする）

### コスト

```sh
for r in region-asia-northeast1 region-us; do
  bq query --location=$([ $r = region-us ] && echo US || echo $L) --use_legacy_sql=false --format=pretty "
  SELECT '$r' AS region, COUNT(*) AS jobs, SUM(total_bytes_billed) AS billed
  FROM \`$r\`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
  WHERE creation_time >= TIMESTAMP('<その日 00:00 JST の UTC>') AND creation_time < TIMESTAMP('<翌日 00:00 JST の UTC>')
    AND STARTS_WITH(user_email, 'sa-sq-dts@')"
done   # 合計の billed が 3 GiB 以下
```

### 停止・削除

- 一時停止: `infra/prod/sq.tf` の該当の DTS に `disabled = true` を足す PR。急ぐときはコンソール（BigQuery → データ転送）で無効にし、続けて同じ PR を出す（出さないと次の apply で有効に戻る）
- 旧系の停止（#14）で突き合わせを止める（その前に、新系の取りこぼしを旧系の値で補う。「過去データの移行（#12）」の「E 以降の取りこぼしを補う」。作業用データセットを使う）: `legacy_stopped = true` の PR で `legacy-*` の DTS 3 本を無効にする（「旧系の停止（#14、5a）」）。旧系を消す #24 で、`legacy-*` の DTS 3 本・作業用データセット 2 つ・その付与を消し、bootstrap の旧テーブルの読み取り 3 件を外す（所有者の apply）。`legacy_compare_daily`（#11 の判定の記録）と品質チェックは残す
- 全部を戻す（この Issue の revert）: prod は delete 13 と M2 の update 1（結果のテーブルは先に `deletion_protection = false`）、bootstrap は revert + 所有者の apply（`sa-sq-dts` は `deletion_policy = "PREVENT"` なので state から外すだけになる）。旧系の取り込みには影響しない

## 過去データの移行（#12）

旧系のデータセット `optionchan`（US）の `spot_price` / `future_price` / `option_price` を、東京の `optionchan_tokyo` へ移す（target.md 6 章 3a）。**#11（並行稼働）の合格の後に、所有者が 1 回だけ行う**。一時データセット・DTS のコピー・クエリ量上限の一時変更は Terraform 外で、この節の手順で作って消す。SQL は `sql/legacy_import/`（試験は `tests/test_legacy_import.py`・`tests/test_legacy_import_bq.py`）。

| SQL | 場所 | 内容 |
|---|---|---|
| `fingerprint.sql` | US と東京 | コピーの検証。日（JST 07:00 区切り）× 表ごとに 行数・`BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(STRUCT(<旧スキーマの列>))))`・フィンガープリントの合計。US の `optionchan` と東京の一時データセットで流して CSV を diff する |
| `insert.sql` | 東京 | 移行。範囲の旧系のスナップショット T のうち、`ingest_runs` に記録の無いものを追記する。流し直しても重複しない |
| `verify.sql` | 東京 | 移行の検証（1 行）。移し漏れ・新旧の重なり・値の違い・残骸・行数の不一致がすべて 0 であること |

**何を移すか**: 旧系の T（3 表の `created_at`）のうち、新系の `ingest_runs` に同じ `snapshot_at` の記録が 1 件も無いもの。新系の稼働前（最初のスナップショットは 2026-10-12 23:39 UTC = 08:39 JST）はすべて、並行稼働中は新系の取りこぼし（`legacy_compare_daily` の `legacy_only`）だけになる。新系の記録がある T は新系のまま（#38 の前の 09:31 JST までの分も。旧系も 1 分の中のどれかの版を取っていて、#40 の分類で説明済み）。範囲の終わり E は品質チェック済みの窓の終わり以下（`insert.sql` が `quality_daily` の最大の `window_end` で止める）。

**移行行の見え方**: T ごとに `ingest_run_id = 'legacy-<T の UTC, YYYYMMDDTHHMMSSZ>'`（例 `legacy-20261009T001500Z`）。`ingest_runs` に T ごとの完了記録（`trigger = 'legacy_import'`、`parser_version = 'legacy'`、`source_object = 'legacy:optionchan'`、`started_at = finished_at = T`、行数は実数）を書くので、最新版のテーブル関数（「最新版の読み方」）がそのまま返す。追加列（`page_updated_at`・`price_time_raw`・`parser_version`・`source_object`）は NULL（元データのまま。補正は #4。`price_time_raw` は #4 が埋める）。旧系の重複（同じ `created_at` で中身が違う行）・欠け（片方の表だけのスナップショット）もそのまま移る。`finished_at = T` なので、後で同じ T の生 HTML を再処理すれば新系の版が選ばれる。

**いつ行うか**: **土曜 13:00 JST 以降**に始め、日曜のうちに終える。E はその土曜の 07:00 JST（金曜 22:00 UTC）。

- 新旧とも書き込みが無い（旧系・新系とも土曜 06:00 JST 頃に止まる。新系の毎時の回収は直近 6 時間の生 HTML なので 12 時台で終わる）。Storage Write API で直前に書いた行には DML の制約があり（target.md 3.1）、`insert.sql` の残骸の削除・切り戻しの DELETE が当たらないようにする
- 土曜 07:15 の品質チェックで E までが確認済みになり、07:30 の突き合わせで金曜分の記録がある
- クエリ量上限を週末だけ上げる。上限は太平洋時間の日ごと（0 時 = 夏時間 16:00 JST / 冬時間 17:00 JST にリセット）で、土曜 16:00（17:00）〜日曜 16:00（17:00）JST は 1 つの日になり、日曜 07:15 の品質チェック・07:30 の突き合わせ（合わせて 1GiB 程度）もその日の枠を使う。枠を使い切るとそれらが失敗する（M2 が鳴る）ので、上限は見込み（約 390GiB）に十分な余裕を持たせる（1.）
- 一連の作業は 1 つの端末で、上から順に行う（同じ年の `insert.sql` を 2 つ同時に流すと、どちらも完了記録を書く前に移す T を選ぶので二重に入る）

### 変数

「Terraform」の「共通の変数」の `P`、「新しい取り込み（ingest）」の `L`、「日次の品質チェックと新旧の突き合わせ（#40）」の `tc` に加えて:

```sh
IMP=optionchan_import
E='<YYYY-MM-DD> 22:00:00+00'   # 範囲の終わり: 開始する土曜の 07:00 JST（前日の金曜 22:00 UTC）。この書式のまま（下の比較が文字列）
S0='2018-10-01 00:00:00+00'     # 範囲の始まり（旧系の最初のパーティションは 2018-10-19）
C0='2026-10-12 23:39:00+00'     # 新系の最初のスナップショット（並行稼働の範囲の始まり。新系の行はこれ以降だけ）
W=<スクラッチのディレクトリ>       # 結果の CSV・JSON を置く（リポジトリ・Issue に貼らない）
# SQL の名前を入れる（第 2 引数: 一時データセット / fingerprint の読み先）
render() { sed -e "s/[$]{project}/$P/g" -e "s/[$]{dataset}/optionchan_tokyo/g" \
  -e "s/[$]{import_dataset}/${2:-$IMP}/g" -e "s/[$]{source_dataset}/${2:-$IMP}/g" "sql/legacy_import/$1.sql"; }
q() { bq query --project_id=$P --use_legacy_sql=false --nouse_cache "$@"; }
# 範囲 [$1, $2) をクエリパラメータ start_at / end_at で渡す（値を SQL に埋め込まない）
# （--parameter は SQL を引数で渡すときにその前に来るよう先に置く。bq はフラグが最後の引数より後ろにあると止まる）
qr() { local s=$1 e=$2; shift 2; q --parameter="start_at:TIMESTAMP:$s" --parameter="end_at:TIMESTAMP:$e" "$@"; }
```

### 事前確認

```sh
# E まで品質チェック済みであること（insert.sql も同じ条件で止まる）
q --location=$L --format=csv 'SELECT MAX(window_end) FROM optionchan_tokyo.quality_daily'   # = E
# 旧系の全体（行数・パーティション。記録して後で比べる）
q --location=US --format=pretty '
SELECT table_name, COUNT(*) parts, MIN(partition_id) first, SUM(total_rows) n
FROM optionchan.INFORMATION_SCHEMA.PARTITIONS WHERE table_name IN ("spot_price", "future_price", "option_price") GROUP BY 1'
```

- データセットコピーの転送料の基準を、#40 の `legacy-copy`（毎日 約 100MB）の課金で確かめる。課金エクスポート（「予算アラートを受けたとき」の 1. のクエリ）を `service.description = "BigQuery"` で SKU ごとに見て、リージョン間の転送の SKU の量が `optionchan_legacy_stage` の表の物理・論理のどちらに近いかを見る。物理なら転送は 約 4.8GB で 約 55 円、論理なら 約 73GiB で 約 880 円（月の予算 500 円の 50% / 80% の通知が出る。それでも進めるかを決める）。判断（基準・見込み額・進めるか）を #12 にコメントする。論理基準で進めるなら、#12 の AC 9（転送 約 100 円以下）を見込み額に直してから始める

### 1. クエリ量上限を一時的に上げる

全件のスキャンが 5 回（下の見積もり。合計 約 390GiB）あるので、「BigQuery のクエリ量上限」の設定コマンドで `--value=614400`（600GiB）にする。フィンガープリント・検証のやり直し 1 回分（約 75〜165GiB）と、日曜朝の定期実行の分の余裕を持たせる。その日（太平洋時間）の使用量（「BigQuery のクエリ量上限」の「今日のスキャン量」のクエリ）が 550GiB を超えたら、次のリセットまで新しい全件スキャンを始めない。作業の後に 6. で戻す。

### 2. 一時データセットとコピー

```sh
bq mk --dataset --location=$L --storage_billing_model=PHYSICAL --max_time_travel_hours=48 \
  --default_table_expiration=1209600 --description='旧 optionchan の一時コピー（#12。移行の検証の後に削除）' $P:$IMP
bq mk --transfer_config --project_id=$P --location=$L --data_source=cross_region_copy --target_dataset=$IMP \
  --display_name='optionchan legacy-import' --no_auto_scheduling \
  --params="{\"source_project_id\":\"$P\",\"source_dataset_id\":\"optionchan\",\"overwrite_destination_table\":\"true\"}"
# 作成直後に実行が始まることがある。1 分ほど待って実行の数を見て、0 件のときだけ手で 1 回実行する（2 本同時に走らせない。転送料が 2 倍になる）
sleep 60
n=$(bq ls --transfer_run --run_attempt=LATEST --max_results=10 --format=json "$(tc legacy-import $L)" | jq length)
echo "runs=$n"
[ "${n:-0}" = 0 ] && bq mk --transfer_run --run_time="$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$(tc legacy-import $L)"
bq ls --transfer_run --run_attempt=LATEST --max_results=3 --format=json "$(tc legacy-import $L)" | jq -c '.[] | {runTime, state}'   # 1 件
# SUCCEEDED になるまで待つ（数十分〜。旧系の全表 6 つが来る）
bq ls --format=json $P:$IMP | jq -r '.[].tableReference.tableId'
for t in spot_price future_price option_price; do
  echo "$t $(bq show --format=json $P:optionchan.$t | jq -r .numRows) $(bq show --format=json $P:$IMP.$t | jq -r .numRows)"
done   # 土曜の午後は旧系に書き込みが無いので、行数が同じこと
```

1 時間足（`atm_*`）もコピーされる。これは 5. の後、6. の後片付けの前に「1 時間足（#49）」の「過去分の移行」で移す（この節の SQL では移さない）。

### 3. コピーの検証

```sh
render fingerprint optionchan | qr "$S0" "$E" --location=US --format=csv --max_rows=100000 \
  --maximum_bytes_billed=85899345920 > $W/fp-us.csv
render fingerprint | qr "$S0" "$E" --location=$L --format=csv --max_rows=100000 \
  --maximum_bytes_billed=85899345920 > $W/fp-import.csv
wc -l $W/fp-us.csv $W/fp-import.csv   # 同じ行数（日 × 3 表 + 見出し。約 6,800）
diff $W/fp-us.csv $W/fp-import.csv && echo MATCH
```

- `MATCH` でなければ、違う日・表を見て、DTS の実行を 1 回やり直し（上書き）、`fp-import.csv` を作り直す。続けて違うなら止める（一時データセットを消して終わり。`optionchan_tokyo` には何も書いていない）
- 東京の 1 回目は 約 73GiB のスキャン（US も同じ）

### 4. 移行

移行前の基準値（並行稼働の範囲だけ。新系の行はここにしか無い）:

```sh
render verify | qr "$C0" "$E" --location=$L --format=json --maximum_bytes_billed=10737418240 > $W/verify-before.json
jq -c '.[-1][0] | {missing_snapshots, overlap_snapshots, new_snapshots, new_rows_spot, new_rows_future, new_rows_option}' $W/verify-before.json
# missing_snapshots = 並行稼働中の新系の取りこぼし（legacy_compare_daily の legacy_only_snapshots の合計と同じ見込み）。overlap 等は 0
```

書き込む前に `optionchan_tokyo` の 4 表のテーブルスナップショットを一時データセットに取る（期限 14 日。移行は追記と `legacy-` の行の削除だけなので、ベースの表との差分の保存料はほぼ 0）:

```sh
for t in spot_price future_price option_price ingest_runs; do
  bq cp --snapshot --no_clobber --expiration=1209600 $P:optionchan_tokyo.$t $P:$IMP.snap_$t
done
bq ls --format=json $P:$IMP | jq -r '.[] | select(.type == "SNAPSHOT") | .tableReference.tableId'   # 4 件
```

最初に一番小さい 2018 年（option 約 1.7GiB）だけを流し、その年の範囲で検証し、もう一度流して重複しないことを確かめる:

```sh
s='2018-01-01 00:00:00+00'; e='2019-01-01 00:00:00+00'
render insert | qr "$s" "$e" --location=$L --format=json --maximum_bytes_billed=53687091200 > $W/insert-2018.json
jq -c '.[-1][0]' $W/insert-2018.json                      # snapshots > 0
render verify | qr "$s" "$e" --location=$L --format=json --maximum_bytes_billed=21474836480 > $W/verify-2018.json
jq -c '.[-1][0] | del(.mismatch_samples)' $W/verify-2018.json   # 5. の 7 項目が 0、missing_snapshots も 0
render insert | qr "$s" "$e" --location=$L --format=json --maximum_bytes_billed=53687091200 > $W/insert-2018-again.json
jq -c '.[-1][0]' $W/insert-2018-again.json                # snapshots = 0（流し直しても重複しない）
```

どれかが期待と違えば止めて、下の「切り戻し」を 2018 年の範囲で行う。よければ残りの年を流す（1 本の DML で変えられるパーティションは 4,000 まで。旧系は 2,256 だが、やり直しの単位を小さくする）:

```sh
for y in $(seq 2019 2026); do
  s="$y-01-01 00:00:00+00"; e="$((y + 1))-01-01 00:00:00+00"; [[ "$e" > "$E" ]] && e=$E
  render insert | qr "$s" "$e" --location=$L --format=json --maximum_bytes_billed=53687091200 > $W/insert-$y.json \
    || { echo "failed: $y"; break; }
  echo "$y $(jq -c '.[-1][0] | {snapshots, rows_spot, rows_future, rows_option}' $W/insert-$y.json)"
done
```

- 1 年 約 1,000 万〜1.1 億行（option）。1 回 数分の見込み
- 失敗したら、原因（クォータ・権限・`legacy_import: ...` の `ERROR()`）を直して**同じ年から**流し直す。前回の残骸（データの表に書けて完了記録の無い `legacy-` 行）は `insert.sql` の最初に消え、完了記録のある T は選ばれないので重複しない
- `legacy_import: end_at ... is after the last checked window end`: E が品質チェック済みの窓より後。E を直す
- `legacy_import: duplicate ingest_run_id`: 秒未満の `created_at` がある（2026-10 の実測では無い）。その年を止めて調べる

### 5. 移行の検証

```sh
render verify | qr "$S0" "$E" --location=$L --format=json --maximum_bytes_billed=214748364800 > $W/verify-after.json
jq -c '.[-1][0] | del(.mismatch_samples)' $W/verify-after.json
jq -c '.[-1][0].mismatch_samples' $W/verify-after.json
```

- `missing_snapshots`・`overlap_snapshots`・`unknown_legacy_runs`・`bad_run_ids`・`row_mismatches`・`orphan_rows`・`run_count_mismatches` がすべて 0
- `new_rows_*` が移行前（`verify-before.json`）と同じ（新系の行に触れていない）
- `migrated_rows_*` + 並行稼働中に新系の記録があった旧系の行 = 旧系の行数（`fp-import.csv` の `n` の合計）
- 0 でない項目があれば `mismatch_samples`（表・T の先頭 20）から見る。その年だけ戻すなら下の「切り戻し」を年の範囲で行い、4. をその年から流し直す

T0 前後（最新版で見て、日ごとに新旧が重ならず、旧系の日は旧系の件数どおり）:

```sh
q --location=$L --format=pretty --maximum_bytes_billed=10737418240 --parameter="e:TIMESTAMP:$E" '
SELECT DATE(TIMESTAMP_SUB(created_at, INTERVAL 7 HOUR), "Asia/Tokyo") AS d, COUNT(*) AS n,
  COUNTIF(STARTS_WITH(ingest_run_id, "legacy-")) AS legacy, COUNT(DISTINCT created_at) AS snapshots
FROM `optionchan_tokyo.option_price_latest`(TIMESTAMP_SUB(@e, INTERVAL 10 DAY), @e)
GROUP BY d ORDER BY d'
# 2026-10-12 まで: n = legacy = fp-import.csv のその日の option_price の n。2026-10-13 以降: legacy は取りこぼしの分だけ
# 同じ T の移行の記録が 2 件以上（同じ年を二重に流した）: 0
q --location=$L --format=csv --parameter="s:TIMESTAMP:$S0" --parameter="e:TIMESTAMP:$E" '
SELECT COUNT(*) FROM (
  SELECT snapshot_at FROM optionchan_tokyo.ingest_runs
  WHERE snapshot_at >= @s AND snapshot_at < @e AND trigger = "legacy_import" GROUP BY snapshot_at HAVING COUNT(*) > 1)'
```

直近 10 日より前の日は、上の verify で足りる: 移行の記録は T ごとに 1 件（上の検査）で新系の記録と重ならない（`overlap_snapshots = 0`）ので、最新版のテーブル関数は移行の記録の T ごとに `legacy-` の行をすべて返す。その行は T ごとに旧系と同じ（`row_mismatches = 0`）で、旧系のすべての T に記録がある（`missing_snapshots = 0`）。したがって新系の稼働前の日は、最新版の行 = 旧系の行になる。

結果は件数・要約だけを #12 にコメントする（`verify-after.json` の 0 の項目と `migrated_rows_*`、`fp` の一致、スキャン量）。CSV・JSON の全文は貼らない。

### 6. 後片付け

先に「1 時間足（#49）」の「過去分の移行」を済ませる（一時データセットの旧 `atm_*` を使う）。

```sh
ids=$(tc legacy-import $L); [ "$(echo "$ids" | grep -c .)" = 1 ] && bq rm -f --transfer_config "$ids"
# コピーした表を消す（スナップショットは期限の 14 日まで残す）
for t in $(bq ls --format=json $P:$IMP | jq -r '.[] | select(.type == "TABLE") | .tableReference.tableId'); do bq rm -f -t $P:$IMP.$t; done
bq ls --format=json $P:$IMP | jq -r '.[] | "\(.type) \(.tableReference.tableId)"'   # SNAPSHOT 4 件だけ
```

スナップショットの期限（14 日）が過ぎたら、データセットごと消す: `bq rm -r -f -d $P:$IMP`（`bq ls --project_id=$P` に `optionchan_import` が無いこと）。

クエリ量上限を `--value=32768 --force` に戻す（「BigQuery のクエリ量上限」。その日のスキャン量が 32GiB を超えているときは、太平洋時間の 0 時を過ぎてから戻す。超えたまま下げると、その日の残りのクエリ（品質チェック・突き合わせを含む）が失敗する）。

### 切り戻し

移行行だけを消す。完了記録を先に消す（その時点でテーブル関数から見えなくなる）→ データの表の順。年の範囲だけ戻すときは `$S0` / `$E` をその年の範囲にする。土曜の午後〜日曜に行う（上の「いつ行うか」）。

```sh
qr "$S0" "$E" --location=$L '
DELETE FROM optionchan_tokyo.ingest_runs WHERE snapshot_at >= @start_at AND snapshot_at < @end_at AND trigger = "legacy_import"'
for t in spot_price future_price option_price; do
  qr "$S0" "$E" --location=$L "
  DELETE FROM optionchan_tokyo.$t WHERE created_at >= @start_at AND created_at < @end_at AND STARTS_WITH(ingest_run_id, 'legacy-')"
done
```

旧データセット `optionchan` はそのまま残っている（#15 まで消さない）。タイムトラベル（7 日）と 4. のテーブルスナップショット（14 日）で移行前の状態も読める。スナップショットからの復元（`bq cp --restore`）は移行の後に新系が書いた行も消すので、上の DELETE で戻せないときの最後の手段にする（復元するなら新系の取り込みを止め、復元の後に「再処理ジョブ」の `--only-missing` でその間を作り直す）。

### 所要時間・コストの見積もり

| 項目 | 見込み |
|---|---|
| 所要時間 | コピー 数十分〜1 時間程度（未計測）、フィンガープリント 各 数分、移行 9 回で 30〜60 分、検証 10 分程度。土曜の午後から日曜で終わる |
| データセットコピーの転送 | 物理基準なら 約 4.8GB × $0.08/GiB ≒ 約 55 円（target.md 7.3）。論理基準なら 約 73GiB で 約 880 円（事前確認で判断） |
| クエリ（スキャン） | US のフィンガープリント 72.8GiB（dry run）、東京の一時データセットのフィンガープリント 約 73GiB、移行 約 75GiB（`option_price` の全列 + 小さい表）、移行後の検証 約 165GiB（一時データセット 約 73GiB + `optionchan_tokyo` の旧スキーマの列と `ingest_run_id`）、移行前の基準値 1GiB 未満。合計 約 390GiB。月 1TiB の無料枠（他プロジェクトと共有）に収まれば 0 円、超えても 約 2.5 ドル |
| 保存 | `optionchan_tokyo` が物理 約 5GB 増える（月 約 15〜20 円、target.md 7.2）。一時データセットのコピーは消すまで + タイムトラベル 48 時間 + フェイルセーフ 7 日の間、物理 約 5GB（数円）。スナップショットはベースとの差分だけ（ほぼ 0） |

実行後に課金エクスポートで実績を確かめ、#12 に記録する（転送の SKU の量と金額、BigQuery のクエリの量）。

### 他の仕組みとの関係

- **最新版のテーブル関数**: 変えない。移行行は `legacy-` の完了記録で選ばれる。移行行の記録は `finished_at = T` なので、同じ T を新系が回収・再処理すれば新系の版が選ばれる
- **品質チェック（`quality_daily`）**: E 以降の窓だけを見るので、移行行で違反は出ない。過去の日を「再実行・バックフィル」で手で流し直すと、旧系の重複（`dup_keys`）・欠け（`missing_table`）・#4 の日付ずれ（`price_time_*`・`sunday`）で違反が出うる（`ingest_run_id` が `legacy-` の行で見分ける）。その結果は誤報として扱う
- **新旧の突き合わせ（`legacy_compare_daily`）**: 新系側から `legacy-` の行を除く（`sql/legacy/compare.sql`）。取りこぼしを補った日を再実行しても `legacy_only` は 0 にならない（新系の取りこぼしの記録として残る）
- **回収・再処理**: 完了記録の `source_object = 'legacy:optionchan'` は生 HTML の名前と一致しないので、回収・`--only-missing` の判定には影響しない（取りこぼしを補った T に生 HTML があれば、再処理すると新系の版が選ばれる）
- **#4（過去データの補正）**: 移行の後に別ステップで行う。`price_time_raw` は移行行では NULL で、#4 が元の値を残す

### E 以降の取りこぼしを補う（#14 で旧系を止めるとき）

E から旧系を止めるまでの新系の取りこぼし（`legacy_compare_daily` の `legacy_only_snapshots` が 1 以上の日）は、旧系を止める前に同じ `insert.sql` で補う。一時データセットの代わりに #40 の `optionchan_legacy_copy`（旧スキーマのまま、1 日分）を使う。行うのは上の「いつ行うか」と同じ土曜の午後〜日曜（新系の書き込みが無く、legacy-stage / legacy-copy の手動実行が毎朝 07:02〜07:30 の定期実行と重ならない）。1 日分なのでクエリ量上限は変えなくてよい:

1. 補う日 W ごとに「再実行・バックフィル」の手順で legacy-stage → legacy-copy を流す（compare は不要）
2. `render insert optionchan_legacy_copy | qr "<W の 07:00 JST の UTC>" "<W+1 の 07:00 JST の UTC>" --location=$L --format=json`、続けて同じ範囲で `render verify optionchan_legacy_copy` を流し、5. と同じ項目が 0 であることを見る
3. 作業用データセットは翌朝の定期実行で前日分に戻る
4. 補った日の 1 時間足を「1 時間足（#49）」の「再実行・バックフィル」で作り直す

新系の取り込みを止めていた期間（「停止・再開」）の分も同じ手順で補える。

## 1 時間足（#49）

旧系の `insert_ohlc`（Cloud Functions、python37、毎日 00:01 UTC）の置き換え（target.md 3.2）。新系の最新版から ATM の 1 時間足を作る BigQuery Data Transfer Service のスケジュールされたクエリ 1 本と、結果の 3 表。旧系（`insert_ohlc`・旧 `optionchan.atm_*`）は変えない（止めるのは #14）。定義は `infra/prod/sq.tf`、SQL は `sql/ohlc/`（試験は `tests/test_ohlc.py`・`tests/test_ohlc_bq.py`）。

| もの | 内容 |
|---|---|
| DTS `optionchan atm-hourly`（asia-northeast1、`sa-sq-dts`、毎日 07:40 JST = `every day 22:40` UTC） | `sql/ohlc/atm_hourly.sql`。窓は `@run_time` の JST の日付 D に対し `[D-1 07:00, D 07:00) JST`（「日次の品質チェックと新旧の突き合わせ（#40）」と同じ 1 日分）。`option_price_latest` の ATM の put（`is_atm AND type = 2`）から限月 × 1 時間（UTC の正時 = JST の正時）の OHLC を作り、3 表へ MERGE する（窓の中を置き換える。ATM の行が 1 件も無い窓では何もしない。3 表は 1 トランザクション）。#40 の 4 本（07:02〜07:30）の後で、失敗の M2 の抑制は 08:40 に終わる |
| `optionchan_tokyo.atm_iv` / `atm_option_price` / `atm_target_price` | 列は旧系と同じ（`schema/ohlc/atm_ohlc.json`）。パーティションは月（`started_at`）、クラスタは `last_trading_day`。キーは `(last_trading_day, time_frame, started_at)`、`time_frame = 3600` |
| `sql/ohlc/legacy_check.sql`（US、手で流す） | 旧系の同値の確認: 旧 `option_price` に同じ 1 時間足の計算を当てて旧 3 表と比べる |
| `sql/ohlc/export.sql`（US）→ `compare.sql`（東京） | 並行稼働中の日の新旧の突き合わせ |
| `sql/ohlc/import.sql`・`fingerprint.sql`（東京・US） | 旧系の過去分の移行と、その検証（#12 の後） |

- 1 時間足の意味は旧系と同じ: `open` / `close` = その 1 時間で `created_at` が最も早い / 遅い NULL でない値、`high` / `low` = 最大 / 最小、全部 NULL なら NULL の行。1 時間の中で ATM の行使価格が変わっても分けない。同じ `created_at` の行（旧系の重複を移した行）の順は `target_price` で決める
- 旧系は前日（UTC の日）分を 00:01 UTC に集計するので、08:45〜08:59 JST の行（JPX のページの遅れで未着）の分だけ、UTC 23 時台（08:00 JST）の 1 時間足が欠ける・違う。新系は 07:40 に前日の窓を作るので欠けない（2026-09-12〜10-12 の比較で、違いは 23 時台の各表 38〜39 件だけ）
- 失敗は Cloud Logging の `resource.type="bigquery_dts_config"`・severity ERROR で、新系の M2 が通知する（#40 と同じ。条件は変えていない）
- 料金: 1 日 約 65MB のスキャン（新系の 1 日の最新版の 5 列）。3 表は 1 表 1 日 約 40 行

### 変数

「日次の品質チェックと新旧の突き合わせ（#40）」の「変数」（`P`・`L`・`SQ`・`tc`）に加えて:

```sh
W=<スクラッチのディレクトリ>   # 結果の JSON を置く（リポジトリ・Issue に貼らない）
# sql/ohlc/<名前>.sql の名前を入れる（第 2 引数: legacy_dataset / source_dataset / import_dataset に入れるデータセット）
ro() { sed -e "s/[$]{project}/$P/g" -e "s/[$]{dataset}/optionchan_tokyo/g" -e "s/[$]{legacy_dataset}/optionchan/g" \
  -e "s/[$]{source_dataset}/${2:-optionchan}/g" -e "s/[$]{import_dataset}/${2:-optionchan_import}/g" "sql/ohlc/$1.sql"; }
q() { bq query --project_id=$P --use_legacy_sql=false --nouse_cache "$@"; }
# 範囲 [$1, $2) をクエリパラメータで渡す（値を SQL に埋め込まない。--parameter は SQL の前に置く）
qr() { local s=$1 e=$2; shift 2; q --parameter="start_at:TIMESTAMP:$s" --parameter="end_at:TIMESTAMP:$e" "$@"; }
# 窓の日付 $1（YYYY-MM-DD）の手動実行を 1 つ作り、終わるまで待って「日付 状態」を出す（SUCCEEDED 以外なら戻り値 1）
rt() { local d=$1 cfg st; cfg=$(tc atm-hourly $L < /dev/null)
  bq mk --transfer_run --run_time="${d}T22:40:00Z" "$cfg" < /dev/null > /dev/null || { echo "$d MK_FAILED"; return 1; }
  while :; do
    st=$(bq ls --transfer_run --run_attempt=LATEST --max_results=50 --format=json "$cfg" < /dev/null \
      | jq -r --arg d "${d}T22:40" '[.[] | select(.runTime | startswith($d))][0].state // "PENDING"')
    case $st in SUCCEEDED|FAILED|CANCELLED) break ;; esac; sleep 20
  done
  echo "$d $st"; [ "$st" = SUCCEEDED ]; }
# 窓の日付を標準入力から 1 行 1 件で受け、1 日ずつ順に rt する（rt の bq は標準入力を読まない）。失敗した日は最後にまとめて出す（その日だけ rt でやり直す）
rdays() { local d failed=""; while read -r d; do [ -n "$d" ] && { rt "$d" || failed="$failed $d"; }; done
  [ -z "$failed" ] && echo "all SUCCEEDED" || echo "FAILED:$failed"; }
# 窓の日付 $1〜$2 を 1 行 1 件で出す
days() { local d=$1; while [[ ! "$d" > "$2" ]]; do echo "$d"; d=$(date -u -d "$d + 1 day" +%F); done; }
```

### 確認（prod の apply の後）

prod の plan の目安: create 7（テーブル 3、テーブル IAM 3、DTS 1）、update / delete 0。

```sh
bq ls --transfer_config --transfer_location=$L --project_id=$P --format=json \
  | jq -c '.[] | select(.displayName == "optionchan atm-hourly") | {displayName, dataSourceId, schedule, disabled: (.disabled // false)}'
bq show --transfer_config --format=json "$(tc atm-hourly $L)" | jq -r '.ownerInfo.email // "" | split("@")[0]'   # sa-sq-dts
for t in atm_iv atm_option_price atm_target_price; do
  bq show --format=json $P:optionchan_tokyo.$t | jq -c '{id: .tableReference.tableId, timePartitioning, clustering, n: .numRows}'
  bq get-iam-policy --format=json $P:optionchan_tokyo.$t | jq -c '[.bindings[]? | {role, m: [.members[] | split("@")[0]]}]'
done   # MONTH・started_at、["last_trading_day"]、[{"role":"roles/bigquery.dataEditor","m":["serviceAccount:sa-sq-dts"]}]
```

DTS は作成直後に 1 回自動で実行される（`run_time` は作成時刻。窓はその日の 07:00 JST までの 1 日分）。`SUCCEEDED` になったか「日次の確認」の実行の状態で見る。失敗したら「失敗を受けたとき」。トランザクション（`BEGIN TRANSACTION`）を含むスクリプトが DTS で通らない場合は、`atm_hourly.sql` の `BEGIN TRANSACTION;` / `COMMIT TRANSACTION;` の 2 行を消す PR を出す（MERGE は 1 文ずつ原子的。途中で失敗しても再実行で直る）。

**デプロイ前の日を埋める**: 新系の最初の窓（2026-10-13 07:00 JST〜）からデプロイの前日の窓までを「再実行・バックフィル」の複数日の手順で作る。

### 日次の確認

毎朝 07:50 以降。件数・要約だけを #49（並行稼働中は #11）に記録する。

```sh
bq ls --transfer_run --run_attempt=LATEST --max_results=3 --format=json "$(tc atm-hourly $L)" | jq -c '.[] | {runTime, state}'
q --location=$L --format=pretty '
SELECT DATE(TIMESTAMP_SUB(started_at, INTERVAL 7 HOUR), "Asia/Tokyo") AS window_date, tbl, COUNT(*) AS bars,
  COUNT(DISTINCT FORMAT("%t|%t", last_trading_day, started_at)) AS keys, COUNTIF(open IS NULL) AS null_bars
FROM (SELECT "atm_iv" AS tbl, * FROM optionchan_tokyo.atm_iv UNION ALL SELECT "atm_option_price", * FROM optionchan_tokyo.atm_option_price
      UNION ALL SELECT "atm_target_price", * FROM optionchan_tokyo.atm_target_price)
WHERE started_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
GROUP BY window_date, tbl ORDER BY window_date DESC, tbl'   # 同じ窓の 3 表の bars が同じ、bars = keys（キーの重複 0）
```

`window_date` は窓の日付（JST の日中立会の日）。営業日の 1 日は 限月 2 × 約 21 時間 = 約 40 行。休場日・週末の窓は行が無い（実行は `SUCCEEDED`）。前日の窓の行が無い営業日は、実行が無かったか失敗している（実行が無いことは M2 では通知されない）。

### 失敗を受けたとき（M2 の `bigquery_dts_config`）

「日次の品質チェックと新旧の突き合わせ（#40）」の「失敗を受けたとき」のログの見方で、`config_id` が `atm-hourly` のものを見る。

| メッセージ | 意味 | 対応 |
|---|---|---|
| `atm_hourly: win_end ... is in the future` | 手動実行の `run_time` が未来の窓 | `run_time` を直して再実行 |
| `Transaction ...` / `Could not serialize access ...` | 同じ表への別の DML（移行・手動の再実行）と重なった | 重なった作業が終わってから「再実行」。トランザクションなので 3 表とも書かれていない |
| `Access Denied` / `Permission ...` | `sa-sq-dts` の 3 表の付与（`infra/prod/sq.tf` の `sq_atm_ohlc_editor`）か `optionchan_tokyo` の読み取り | plan で付与の差分を見る |
| その他（クォータ・内部エラー） | BigQuery / DTS の障害 | 翌日の実行で戻るか見る。前日分は「再実行」 |

### 再実行・バックフィル

本番の 3 表への書き込みなので、実行は所有者が行う。同じ窓を何度流しても結果は同じ（窓の中を置き換える）。窓の日付 W（JST の日中立会の日）の分は `run_time` を W の UTC の 22:40（JST の W+1 日 07:40）にする。

```sh
# 1 日（終わるまで待つ）
rt 2026-10-14
# 複数日（窓の日付 W1〜W2）。1 日ずつ順に流し、全日が SUCCEEDED であることを確かめる
days <W1> <W2> | rdays          # 最後に all SUCCEEDED。FAILED: の日は、原因を見てから rt <日付> でやり直す
bq ls --transfer_run --run_attempt=LATEST --max_results=20 --format=json "$(tc atm-hourly $L)" | jq -c '.[] | {runTime, state}'
```

**バックフィルは 1 日ずつ順に実行する**（`bq mk --transfer_run --start_time --end_time` でまとめて作らない）。3 表は月のパーティションなので、近い日の MERGE が並行に走るとトランザクションが衝突して片方が失敗しうる（失敗した日はロールバックされ、ERROR で M2 も鳴る）。流し終えたら全日が `SUCCEEDED` かを確かめ、失敗した日は再実行する。毎朝の定期実行（07:40）と重ならない時間に行う。

作り直すとき: 再処理ジョブ（「再処理ジョブ（#34）」）・回収で前の日の最新版が変わったとき、#12 の移行の後（新系の取りこぼしが旧系の行で埋まった日）、#14 の補い（「過去データの移行（#12）」の「E 以降の取りこぼしを補う」）の後。データの無い窓（#12 の前の新系の稼働前の日）を流しても何も変わらない（ATM の行が無い窓は何もしない）。

定期実行は前日の窓 1 日分だけを作る。07:40 より後に前日以前の窓の最新版が変わったこと（毎時の回収は直近 6 時間分なので通常は起きない。再処理・回収の遅れ）は、完了記録の `finished_at` がその窓の翌朝 07:40 より後の実行がある窓の日付で見つけ、その日を `rt` で作り直す:

```sh
q --location=$L --format=csv '
SELECT DISTINCT FORMAT_DATE("%F", DATE(TIMESTAMP_SUB(snapshot_at, INTERVAL 7 HOUR), "Asia/Tokyo")) AS window_date
FROM optionchan_tokyo.ingest_runs
WHERE snapshot_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
  AND finished_at > TIMESTAMP(DATETIME(DATE_ADD(DATE(TIMESTAMP_SUB(snapshot_at, INTERVAL 7 HOUR), "Asia/Tokyo"), INTERVAL 1 DAY), TIME "07:40:00"), "Asia/Tokyo")
ORDER BY window_date' | tail -n +2 | rdays   # 行が無ければ何もしない
```

### 旧系との突き合わせ

**旧系の同値の確認**（target.md 3.2「過去 30 日分で現行と同値」。US、30 日 約 0.5GB）。範囲は UTC の正時:

```sh
ro legacy_check | qr '2026-09-12 00:00:00+00' '2026-10-13 00:00:00+00' --location=US --format=json > $W/legacy-check.json
jq -c '.[-1][] | {tbl, recomputed, legacy_keys, match, late, differ, legacy_only, recomputed_only, legacy_dup_keys, legacy_conflict_keys}' $W/legacy-check.json
# differ・legacy_only・recomputed_only が 0。late は UTC 23 時台（旧系の集計の時点で未着だった行の分）
```

2026-10-13 の実行（2026-09-12〜10-13 00:00 UTC）: 3 表とも `differ`・`legacy_only`・`recomputed_only` 0、`match` 887 / 888 / 887、`late` 39 / 38 / 39。旧系を止める前（#14）にもう一度流して記録する。

**並行稼働中の日の新旧**（旧 3 表 ↔ 新系の 3 表）。旧系は窓の日付の翌日 00:01 UTC に前日（UTC の日）分を作るので、UTC の日 U の比較は U+1 日の 00:10 UTC 以降、新系の窓を作った後に行う。1 日ずつ（JSON 約 20KB。コマンドの引数の上限 128KB を超えないように）:

```sh
U=2026-10-14   # UTC の日
s="$U 00:00:00+00"; e="$(date -u -d "$U + 1 day" +%F) 00:00:00+00"
bars=$(ro export optionchan | qr "$s" "$e" --location=US --format=json | jq -r '.[0].bars')
[ "$bars" != null ] && ro compare | qr "$s" "$e" --parameter="legacy_bars:STRING:$bars" --location=$L --format=json > $W/compare-$U.json
jq -c '.[-1][] | {tbl, legacy_keys, new_keys, match, differ, legacy_only, new_only, legacy_dup_keys, conflict_keys, max_abs_diff}' $W/compare-$U.json
```

- `legacy_only` / `new_only`: 片方にだけある 1 時間足。新系の取りこぼし（その時間の `legacy_compare_daily` の `legacy_only_snapshots`）・旧系の 23 時台の欠け・新系だけの時間（保存条件の差）で説明できるかを見る
- `differ`: 新旧はスナップショットの版が違うことがある（#40 の「版の差」。ATM の行の iv・価格も 1 分の中で変わる）ので、0 にはならない。`max_abs_diff` と `samples` を見て、版の差で説明できない大きな違い（時間全体がずれている等）が無いことを確かめる
- 結果（件数・`max_abs_diff`）を #49 にコメントする（全文は貼らない）

### 過去分の移行（#12 の後）

旧 3 表のうち、`started_at` が C（新系の最初のスナップショット 2026-10-12 23:39 UTC を含む窓の始まり = **2026-10-12 22:00 UTC**）より前の行を、新系の 3 表へ移す。C 以降は定期クエリが作る。**「過去データの移行（#12）」の 5. の後、6. の後片付け（一時データセット `optionchan_import` の表を消す）の前に行う**（#12 のデータセットコピーで旧 `atm_*` も運ばれている）。1 つの端末で上から順に。旧 3 表は 1 表 約 4MB なので、クエリ量の上限の変更は要らない。

- 同じ中身の重複は 1 行にする。中身の違う重複のキー（旧 `atm_target_price` の 2019-07-01 の 44 キー。片方は option の価格の値）は移さない（下の 4. で作り直す）
- 新系に既にあるキーは上書きしない（流し直しても増えない）。値は旧系のまま（旧系の 23 時台の欠け・2019〜2020 年の一部の表だけ欠けた日もそのまま。直すなら 4. と同じ手動実行でその日を作り直す）

```sh
C='2026-10-12 22:00:00+00'; S0='2018-01-01 00:00:00+00'
# 1. コピーの検証: US の旧 3 表と一時データセットの同じ範囲が一致
ro fingerprint optionchan | qr "$S0" "$C" --location=US --format=json > $W/ohlc-fp-us.json
ro fingerprint optionchan_import | qr "$S0" "$C" --location=$L --format=json > $W/ohlc-fp-import.json
diff <(jq -S . $W/ohlc-fp-us.json) <(jq -S . $W/ohlc-fp-import.json) && echo MATCH   # MATCH でなければ止める
# 2. 移行（3 表に書く。結果は表ごとの inserted・skipped_existing・conflict_keys・dup_rows_collapsed）
ro import | qr "$S0" "$C" --location=$L --format=json > $W/ohlc-import.json; jq -c '.[-1][]' $W/ohlc-import.json
#    もう一度流して inserted が 0（冪等）
ro import | qr "$S0" "$C" --location=$L --format=json > $W/ohlc-import-again.json; jq -c '.[-1][] | {tbl, inserted}' $W/ohlc-import-again.json
# 3. 移行の検証: 新系の fp_clean_* = 旧系の fp_clean_*、新系の rows_ = keys = 旧系の keys − conflict_keys、新系の conflict_keys 0
ro fingerprint optionchan_tokyo | qr "$S0" "$C" --location=$L --format=json > $W/ohlc-fp-tokyo.json
jq -c '.[] | {tbl, keys, conflict_keys, fp_clean_xor, fp_clean_sum}' $W/ohlc-fp-us.json
jq -c '.[] | {tbl, rows_, keys, conflict_keys, fp_clean_xor, fp_clean_sum}' $W/ohlc-fp-tokyo.json
# 4. 作り直し（1 日ずつ順に。全日が SUCCEEDED であること）
#  4a. 中身の違う重複のキーがある窓の日付（移さなかったキー。窓は JST 07:00 区切りなので、UTC 23 時台のキーは翌日の窓に入る）。
#      一時データセットから出す。2026-10-13 の旧系では 2019-07-01 と 2019-07-02 の 2 日
qr "$S0" "$C" --location=$L --format=csv '
SELECT DISTINCT FORMAT_DATE("%F", DATE(TIMESTAMP_SUB(started_at, INTERVAL 7 HOUR), "Asia/Tokyo")) AS window_date
FROM (
  SELECT started_at FROM optionchan_import.atm_iv WHERE started_at >= @start_at AND started_at < @end_at
  GROUP BY last_trading_day, time_frame, started_at HAVING COUNT(DISTINCT TO_JSON_STRING(STRUCT(open, high, low, close))) > 1
  UNION ALL
  SELECT started_at FROM optionchan_import.atm_option_price WHERE started_at >= @start_at AND started_at < @end_at
  GROUP BY last_trading_day, time_frame, started_at HAVING COUNT(DISTINCT TO_JSON_STRING(STRUCT(open, high, low, close))) > 1
  UNION ALL
  SELECT started_at FROM optionchan_import.atm_target_price WHERE started_at >= @start_at AND started_at < @end_at
  GROUP BY last_trading_day, time_frame, started_at HAVING COUNT(DISTINCT TO_JSON_STRING(STRUCT(open, high, low, close))) > 1
)
ORDER BY window_date' | tail -n +2 > $W/ohlc-conflict-days.txt
cat $W/ohlc-conflict-days.txt
rdays < $W/ohlc-conflict-days.txt          # all SUCCEEDED（その窓の移行済みの行も再計算で置き換わる）
#  4b. C 以降の窓（窓の日付 2026-10-13 〜 E の前日（金曜）。#12 で新系の取りこぼしが旧系の行で埋まった）
days 2026-10-13 <E の前日（金曜）> | rdays   # all SUCCEEDED
#  4c. 確認: 作り直した窓ごとに 3 表のキーの数が同じで、キーの重複が無い（4a の日付と 4b の範囲）
for d in $(cat $W/ohlc-conflict-days.txt); do
  q --location=$L --format=csv --parameter="d:DATE:$d" '
  SELECT @d AS window_date, tbl, COUNT(*) AS bars, COUNT(DISTINCT FORMAT("%t|%t", last_trading_day, started_at)) AS keys
  FROM (SELECT "atm_iv" AS tbl, * FROM optionchan_tokyo.atm_iv UNION ALL SELECT "atm_option_price", * FROM optionchan_tokyo.atm_option_price
        UNION ALL SELECT "atm_target_price", * FROM optionchan_tokyo.atm_target_price)
  WHERE started_at >= TIMESTAMP(DATETIME(@d, TIME "07:00:00"), "Asia/Tokyo")
    AND started_at < TIMESTAMP(DATETIME(DATE_ADD(@d, INTERVAL 1 DAY), TIME "07:00:00"), "Asia/Tokyo")
  GROUP BY tbl ORDER BY tbl' | tail -n +2
done   # 日ごとに 3 表の bars が同じ、bars = keys
#    4b の範囲は「日次の確認」のクエリの INTERVAL 7 DAY を範囲に合わせて広げて同じことを見る
```

- 2. の `skipped_existing` は、移行の前に C より前の窓を手動実行していなければ 0
- 結果は件数・要約だけを #49 にコメントする（フィンガープリントの値は貼らない。一致したかどうかだけ）
- 切り戻し（移行行だけを消す。C より前は移行行だけ）: `qr "$S0" "$C" --location=$L 'DELETE FROM optionchan_tokyo.<表> WHERE started_at >= @start_at AND started_at < @end_at'` を 3 表で。4a で作り直した窓（2019-07-01・2019-07-02）の 1 時間足も消える

### 停止・削除

- 一時停止: `infra/prod/sq.tf` の `atm_hourly` に `disabled = true` を足す PR（急ぐときはコンソールで無効 → 同じ PR）
- 全部を戻す（この Issue の revert）: 3 表の `deletion_protection = false` の PR → revert の PR（delete 7）。旧系の `insert_ohlc` は動いたままなので、旧ビューアーには影響しない
- 旧系の停止（#14）: `insert_ohlc` と Scheduler `create_timeframe` を止める前に、「旧系の同値の確認」を直近 30 日でもう一度流して記録する。止めた後は新系の 3 表だけになる（旧 3 表は #15 で消すまで残る）

## ビューアー（#13）

新しいビューアー（`apps/viewer`、#52）を Cloud Run `viewer` に IAP（Cloud Run への直接設定。ロードバランサ不要・追加費用なし）の後ろでデプロイする（#53）。旧ビューアー（App Engine + 旧 HTTP 関数）とは独立していて、旧ビューアーには触れない。

**IAP なしで公開しない。** アプリには認証も回数制限も無い（共通パスワードもやめた）。IAP を外す（`--no-iap`）、`allUsers` / `allAuthenticatedUsers` に `run.invoker` を付ける、`invoker_iam_disabled = true` にする、のどれかをすると、URL を知っている誰でも BigQuery のクエリを回せて課金が増える（1 クエリの上限 1 GiB はあっても回数は制限されない）。`tests/test_infra.py` が IAP の有効・invoker・`allUsers` の不在を検査する。アプリ側での IAP の JWT の検証はしていない（invoker を IAP のサービスエージェントだけにすることで直接の呼び出しを防ぐ）。

| リソース | 設定 | 定義 |
|---|---|---|
| Cloud Run `viewer`（asia-northeast1） | IAP 有効、ingress all（run.app の URL に IAP が掛かる）、IAM の検査あり、第1世代、CPU 1・512Mi・リクエスト中だけ CPU、同時実行 8、0〜2 インスタンス、60 秒、`sa-viewer`、digest 指定、環境変数 `GCP_PROJECT` / `BQ_DATASET` / `BQ_LOCATION` | `infra/prod/viewer.tf` |
| `run.invoker` | IAP のサービスエージェント（`service-<番号>@gcp-sa-iap.iam.gserviceaccount.com`）だけ | `infra/prod/viewer.tf` |
| IAP のアクセス許可 | サービス単位の `roles/iap.httpsResourceAccessor` = `viewer_iap_members`（`infra/bootstrap/local.auto.tfvars`。コミットしない） | `infra/bootstrap/viewer.tf` |
| IAP の OAuth クライアント | 組織の無いプロジェクトでは Google 管理の OAuth クライアントが使えないので、所有者が作って asia-northeast1 の Cloud Run の単位に設定する（シークレットは Terraform・リポジトリに入れない） | 手作業（下の手順 2〜4） |
| 権限 | `sa-viewer`: `optionchan_tokyo` の `dataViewer`（データセット単位）、プロジェクトの `bigquery.jobUser`。旧 `optionchan` には何も付けない | `infra/prod/iam.tf`、`infra/bootstrap/viewer.tf` |

プロジェクト単位で `roles/iap.httpsResourceAccessor` を持つプリンシパル（App Engine の IAP 用に付けたもの等）は、継承で `viewer` にも入れる。サービス単位の一覧（下の確認）には出ないので、`gcloud projects get-iam-policy $P` でも見る。

### 変数

「Terraform」の「共通の変数」に加えて設定する。`U`（サービスの URL）はプロジェクト番号由来の文字列を含むので Issue・PR に貼らない。

```sh
L=asia-northeast1
U=$(gcloud run services describe viewer --region=$L --project=$P --format='value(status.url)')   # 手順 5 の後
```

### 初回のデプロイ（所有者の手順）

PR のマージ後、deploy の `apply` job を承認する前に手順 1〜4 を行う（`apply` job は承認まで待つ）。順番が大事: IAP のサービスエージェントが無いと invoker の付与が失敗し、サービスが無いと IAP のアクセス許可を付けられない。

1. **bootstrap（1 回目）とサービスエージェント**: `infra/bootstrap/local.auto.tfvars` に `viewer_iap_members = []` を書き、master で「bootstrap の初回 apply」の手順 2〜4 を行う。手順 3 の要約の目安: create 4（SA `sa-viewer`、プロジェクト IAM 1（`sa-viewer` の `bigquery.jobUser`）、SA IAM 1（`sa-tf-apply` の `serviceAccountUser`）、API `iap.googleapis.com` 1（有効化済み。宣言のみ））、update / delete / replace 0。続けて IAP のサービスエージェントを作る（既にあれば何もしない）:

   ```sh
   gcloud beta services identity create --service=iap.googleapis.com --project=$P > /dev/null
   ```

2. **OAuth 同意画面の確認（コンソール）**: Google Cloud コンソール →「Google Auth Platform」（「API とサービス」→「OAuth 同意画面」からも行ける）。
   1. 「ブランディング」が未作成なら「開始」→ アプリ名（例 `optionchan viewer`）とサポートメールを入れる → 対象は **外部（External）** → 連絡先を入れて作成。既にある（旧ビューアーの App Engine の IAP 用）なら変えない
   2. 「対象（Audience）」でユーザーの種類が **外部** であることを見る。公開ステータスが「テスト中」なら「テストユーザー」に、`viewer_iap_members` に入れる予定のアカウントを足す（足さないとログインで拒否される）。IAP が求めるのはメールアドレス等の基本の範囲だけなので「アプリを公開」にしてもよい
3. **OAuth クライアントの作成（コンソール）**: 「Google Auth Platform」→「クライアント」→「クライアントを作成」。組織の無いプロジェクトでは Google 管理の OAuth クライアントが使えず、API でも作れないので、ここだけは画面で行う。
   1. アプリケーションの種類 **ウェブ アプリケーション**、名前（例 `optionchan-viewer-iap`）→「作成」
   2. 表示されたクライアント ID とシークレットを控える（次の手順で使うだけ。Issue・PR・チャット・リポジトリに貼らない。JSON をダウンロードしたら手順 4 の後に消す）
   3. 作ったクライアントを開き、「承認済みのリダイレクト URI」に `https://iap.googleapis.com/v1/oauth/clientIds/<クライアント ID>:handleRedirect` を足して「保存」
4. **IAP に設定（端末）**: asia-northeast1 の Cloud Run（`--resource-type=cloud-run --region`）の単位で設定する。プロジェクト単位にしないのは旧ビューアー（App Engine の IAP）に影響させないため、サービス単位にしないのはサービスの作成（手順 5）より前に済ませるため（組織の無いプロジェクトで IAP を初めて有効にするときは、先に OAuth クライアントを設定しておくことが公式の推奨）。値は画面に出さず、一時ファイルはリポジトリの外に作ってすぐ消す。

   ```sh
   F=$(mktemp); chmod 600 "$F"
   read -r -p 'クライアント ID: ' CID; read -r -s -p 'シークレット: ' CSEC; echo
   printf 'accessSettings:\n  oauthSettings:\n    clientId: %s\n    clientSecret: %s\n' "$CID" "$CSEC" > "$F"
   gcloud iap settings set "$F" --project=$P --resource-type=cloud-run --region=$L > /dev/null; echo $?   # 0
   rm -f "$F"; unset CID CSEC
   gcloud iap settings get --project=$P --resource-type=cloud-run --region=$L \
     --format='value(accessSettings.oauthSettings.clientId)' | grep -c .   # 1（設定済み。ID は表示しない）
   ```

   リージョンの単位で失敗したら、手順 5 の後に `--region=$L --service=viewer`（サービス単位）で同じことを行う（その間はブラウザで開くと IAP のエラーになるだけ）。
5. **prod の apply（CI）**: deploy の `apply` job を承認する。plan の要約の目安: create 3（`google_cloud_run_v2_service.viewer`、`google_cloud_run_v2_service_iam_member.viewer_iap_invoker`、`google_bigquery_dataset_iam_member.viewer_reader`）、update / delete / replace 0、「イメージ viewer: … は未作成」。apply job が viewer のイメージをビルド・検査・push してから digest で apply する
6. **bootstrap（2 回目）**: `infra/bootstrap/local.auto.tfvars` を `viewer_iap_members = ["user:<自分のメール>"]` にして、「bootstrap の初回 apply」の手順 2〜4。要約の目安: create 1（`google_iap_web_cloud_run_service_iam_member.viewer_access["user:…"]`）、update / delete / replace 0。要約はメールを含むので貼らない
7. **確認**: 「変数」の `U` を設定し、下の「確認」をすべて行って、結果（件数・pass / fail だけ）を Issue にコメントする

代替（手作業が少ないが既定にしない）: 手順 2〜4 の代わりに、手順 5 の後でコンソールの Cloud Run → `viewer` →「セキュリティ」→ IAP の「ポリシーを編集」→「IAP で構成」→「認証情報を自動生成」でも OAuth クライアントが作られる。ただしこれは **プロジェクト単位** の設定になり、旧ビューアー（App Engine の IAP）に影響するかを確かめていない。使ったら旧ビューアーにログインできることも確かめる。

手順 5 の apply が IAP の有効化で失敗したとき（組織の無いプロジェクトで「最初はコンソールで」と警告されることがある）: 手順 4 の `gcloud iap settings get` が 1 であることを確かめてから `gh workflow run deploy.yml --ref master` でやり直す。それでも失敗するなら、最後の手段として OAuth クライアントをプロジェクト単位で設定する（`gcloud iap settings set <設定ファイル> --project=$P`。旧 App Engine の IAP はアプリ側の設定にクライアントを持っているので影響は小さい）か、コンソールでサービスの IAP を有効にする（Terraform の `iap_enabled = true` と一致するので state とはずれない）。どちらも行ったことを Issue に残す。

### 確認

構成（v2 API の JSON をそのまま見る）:

```sh
curl -sf -H "Authorization: Bearer $(gcloud auth print-access-token)" \
  https://run.googleapis.com/v2/projects/$P/locations/$L/services/viewer \
  | jq -c '{iap: .iapEnabled, ingress, invoker_check_disabled: (.invokerIamDisabled // false), sa: (.template.serviceAccount | split("@")[0]),
            min: (.template.scaling.minInstanceCount // 0), max: .template.scaling.maxInstanceCount,
            digest: (.template.containers[0].image | test("@sha256:[0-9a-f]{64}$")), envs: [.template.containers[0].env[].name]}'
# true、INGRESS_TRAFFIC_ALL、false、sa-viewer、0、2、true、[GCP_PROJECT,BQ_DATASET,BQ_LOCATION]
```

権限:

```sh
# run.invoker は IAP のサービスエージェントだけ。allUsers / allAuthenticatedUsers が無い
gcloud run services get-iam-policy viewer --region=$L --project=$P --format=json \
  | jq -c '[.bindings[] | {role, m: [.members[] | sub("[0-9]+"; "<num>")]}]'
# [{"role":"roles/run.invoker","m":["serviceAccount:service-<num>@gcp-sa-iap.iam.gserviceaccount.com"]}]
# IAP のアクセス許可（サービス単位）が viewer_iap_members と同じ（メールは表示しない）
diff <(gcloud iap web get-iam-policy --resource-type=cloud-run --region=$L --service=viewer --project=$P --format=json \
         | jq -r '.bindings[]? | select(.role == "roles/iap.httpsResourceAccessor") | .members[]' | sort) \
     <(sed -n 's/^viewer_iap_members *= *//p' infra/bootstrap/local.auto.tfvars | jq -r '.[]' | sort) > /dev/null && echo 'iap members ok'
# sa-viewer: プロジェクトは bigquery.jobUser だけ、optionchan_tokyo は READER（dataViewer）、旧 optionchan には無い
gcloud projects get-iam-policy $P --format=json | jq -c '[.bindings[] | select(any(.members[]; startswith("serviceAccount:sa-viewer@"))) | .role]'
# ["roles/bigquery.jobUser"]
for d in optionchan_tokyo optionchan; do
  bq show --format=json $P:$d | jq -c --arg d $d '{d: $d, r: [.access[] | select((.userByEmail // "") | startswith("sa-viewer@")) | .role]}'
done   # optionchan_tokyo は ["READER"]（か roles/bigquery.dataViewer）、optionchan は []
```

外からの呼び出し（未認証）が通らないこと:

```sh
curl -s -o /dev/null -w '%{http_code}\n' "$U/smile_data"   # 200 でない（IAP のログインへの 302 等）
```

ブラウザ（所有者のアカウント）: `$U/`・`$U/smile`・`$U/atm_iv` を開き（旧型の ATM 推移 `/atm` は #58 で削除した）、`/atm_iv` は日数と限月（`d`・`n`）を変えて 2 通り以上 Update する。チャートが描かれ、開発者ツールのコンソールに CSP の違反（`Refused to …`）が出ないこと。新しいビューアーは新系（`optionchan_tokyo`）の T0 以降のデータだけを表示する（過去分の移行 #12 の前は短い。不具合ではない）。

ログ（デプロイ・ブラウザ確認の後 1 時間）:

```sh
gcloud logging read 'resource.type="cloud_run_revision" AND resource.labels.service_name="viewer" AND severity>=ERROR' \
  --project=$P --freshness=1h --limit=20 --format='value(timestamp,severity,jsonPayload.message)'   # 0 件
```

### 更新

アプリ（`apps/viewer`）の変更は PR → マージ → deploy の `apply` job の承認で反映される（イメージのタグが変われば plan に `viewer` の update が出る）。ユーザーの追加・削除は `local.auto.tfvars` を直して bootstrap の手順 2〜4（`allUsers` 等は validation で拒否される）。

### 切り戻し・撤去

| 段階 | 手段 | 影響 |
|---|---|---|
| 緊急に閉じる | `gcloud run services remove-iam-policy-binding viewer --region=$L --project=$P --role=roles/run.invoker --member=serviceAccount:service-$(gcloud projects describe $P --format='value(projectNumber)')@gcp-sa-iap.iam.gserviceaccount.com`（IAP を通っても 403 になる）。次の apply で戻るので、続けて下の撤去か修正の PR を出す | 新しいビューアーが開けなくなるだけ。旧ビューアー・取り込みは影響なし |
| イメージを前の版に戻す | 原因のコミットを revert する PR（「新しい取り込み（ingest）」の R2 と同じ） | 1 回の apply で新しいリビジョン |
| 撤去 | `infra/prod/viewer.tf` と `viewer_reader` を消す PR（plan: delete 3。`deletion_protection = false`）→ `local.auto.tfvars` を `[]` にして bootstrap を apply（delete 1）→ 必要なら `infra/bootstrap/viewer.tf` を消す PR と bootstrap の apply（SA は `deletion_policy = "PREVENT"` なので state から外すだけ。消すなら gcloud で）→ コンソールで OAuth クライアントを削除 | 旧ビューアーはそのまま使える |

### コスト

Cloud Run は最小 0 インスタンス・リクエスト中だけ CPU で、利用者 1 人ならほぼ 0 円。IAP（Cloud Run への直接設定）は追加費用なし。BigQuery は 1 ページの表示で 1〜2 クエリ（各 `maximum_bytes_billed` 1 GiB、最小課金 10 MiB）。1 日の量は #13 の 13c で `INFORMATION_SCHEMA.JOBS` の `sa-viewer` の `total_bytes_billed` で見る（目安 8 GiB/日 以下）。

## 旧系の停止（#14、5a）

旧系（Cloud Functions 第1世代・Pub/Sub・Cloud Scheduler・Datastore・App Engine）を**戻せる形で**止め、2 週間様子を見る（target.md 6 章 5a）。消すのは #24（5b、不可逆）。**#11 の合格・#12 の移行・#13 の完了（#54 の見比べ）の後に、所有者が行う**。

| 対象（2026-10-14 の棚卸し） | 5a でやること | 手段 | 戻し方 |
|---|---|---|---|
| Scheduler `minutely_task`（毎分、→ Pub/Sub `minutely_task` → `download_jpx`）・`create_timeframe`（毎日 00:01 UTC、→ `create_timeframe` → `insert_ohlc`）。asia-northeast1 | 一時停止（`pause`） | gcloud | `resume` |
| HTTP 関数 `smile_data` / `atm_data` / `atm_iv_data`（us-east1、python37、`allUsers` に `cloudfunctions.invoker`） | `allUsers` の invoker を外す | gcloud | `add-iam-policy-binding` で同じ付与 |
| App Engine（asia-northeast1、`default` サービスの 2019-07-03 のバージョンに 100%、IAP 有効。旧ビューアー） | アプリを無効化（`servingStatus = USER_DISABLED`）。データ API が閉じると表示できないので一緒に止める。**連携している Datastore（`databaseType = CLOUD_DATASTORE_COMPATIBILITY`、`appEngineIntegrationMode = ENABLED`）の読み書きも止まる**（公式: "If you disable your App Engine app, you also disable access to the Datastore mode database linked to that app"）。Datastore を使うのは `download_jpx`（前回の先物の時刻）と `smile_data` で、どちらも先に止めるので影響しない。反映は最大 1 時間 | REST API（gcloud にフラグが無い）かコンソール | `SERVING` に戻す |
| 旧系向けアラート `optionchan: M1 生存（download_jpx の起動が 15 分無い）`・`optionchan: M2 エラー（取り込み関数の失敗）` | 削除（Scheduler を止めると M1 が必ず鳴る。2027-09-01 のアラート課金の前に旧系向けを 0 にする。EPIC 終了条件 5） | Terraform（`legacy_stopped = true`） | `legacy_stopped = false` の PR で同じ定義（`monitoring/*.json`）から作り直す |
| 新旧の突き合わせの DTS `legacy-stage` / `legacy-copy` / `legacy-compare`（#40） | 無効化（旧系の行が来なくなる。品質チェック `quality-daily` と 1 時間足 `atm-hourly` は動かし続ける） | Terraform（同じ PR） | 同上で有効に戻る |
| 共通パスワード（`auth.txt`）を読む旧コード（`appengine/main.py`・`functions_py/atm_iv/main.py`・`functions/main.py` の `smile_data` / `atm_data`。EPIC 終了条件 7） | リポジトリから消す（本番には影響しない。デプロイ済みの関数・アプリは変わらない） | PR | revert（git の履歴に残る） |

**5a でやらないこと（#24・#15）**: 関数 6 つ（`download_jpx` / `load_jpx_into_bq` は python311、他の 4 つは python37）・Pub/Sub のトピック 3 つ（`minutely_task` / `create_timeframe` / `minutely15_task`。`minutely15_task` は購読も Scheduler も無い）と購読 2 つ・Scheduler 2 ジョブ・Datastore（kind `optionchan` の 1 エンティティ）・GCS `optionchan`（中継）の削除は #24。旧系向けの Terraform の定義（`legacy_stopped`・`legacy-*` の DTS・作業用データセット 2 つ・`sa-sq-dts` の旧テーブルの読み取り 3 件（bootstrap）・`monitoring/m1-liveness.json`・`m2-errors.json`・`monitoring/backtest.py` の旧系分）も、戻す必要が無くなる #24 で消す。旧 SA `optionchan`（鍵 2 本は #23 で無効化済み。`bigquery.admin`・`datastore.owner`・`storage.objectAdmin`）とデフォルト SA（App Engine・Compute）の Editor、プロジェクト単位の `iap.httpsResourceAccessor`、旧データセット `optionchan`・`jikken`、App Engine の旧バージョン（30）、ソース zip のバケットは #15。`legacy_compare_daily`（#11 の判定の記録）は消さない（`deletion_protection`）。

**どのアカウントで**: Scheduler・関数の IAM・App Engine の変更は本番の変更なので所有者が行う。Terraform の変更は PR → マージ → deploy の `apply` job の承認（「Terraform」の「流れ」）。

### 変数

「Terraform」の「共通の変数」（`P`・`REPO`・`R`）、「新しい取り込み（ingest）」の `L`、「日次の品質チェックと新旧の突き合わせ（#40）」の `tc` に加えて:

```sh
OLD_REGION=us-east1
OLD_JOBS="minutely_task create_timeframe"
HTTP_FNS="smile_data atm_data atm_iv_data"
W=<スクラッチのディレクトリ>   # 停止前後の記録を置く（リポジトリ・Issue に貼らない）
# App Engine の servingStatus を見る / 変える（gcloud app update に servingStatus のフラグが無いので Admin API を直接呼ぶ）
app_status() { curl -s -H "Authorization: Bearer $(gcloud auth print-access-token)" \
  "https://appengine.googleapis.com/v1/apps/$P" | jq -r '.servingStatus // .error.message'; }
app_set() { curl -s -X PATCH -H "Authorization: Bearer $(gcloud auth print-access-token)" -H 'Content-Type: application/json' \
  "https://appengine.googleapis.com/v1/apps/$P?updateMask=servingStatus" -d "{\"servingStatus\":\"$1\"}" \
  | jq -c '{done: (.done // false), error: .error.message}'; }
# 旧系の状態の要約（停止前・停止後・戻した後に同じものを取って比べる）
legacy_state() {
  for j in $OLD_JOBS; do echo "scheduler $j $(gcloud scheduler jobs describe $j --location=$L --project=$P --format='value(state)')"; done
  for f in $HTTP_FNS; do echo "invoker $f $(gcloud functions get-iam-policy $f --region=$OLD_REGION --project=$P --format=json \
    | jq -c '[.bindings[]? | select(.role == "roles/cloudfunctions.invoker") | .members[]]')"; done
  echo "app $(app_status)"
  gcloud monitoring policies list --project=$P --format='value(displayName,enabled)' | grep '^optionchan: ' | sort
  for loc in $L us; do bq ls --transfer_config --transfer_location=$loc --project_id=$P --format=json \
    | jq -r '.[] | "dts \(.displayName) disabled=\(.disabled // false)"'; done | sort
}
```

### 事前確認

すべて満たしてから始める。満たさないものがあれば止めない。

1. Issue: #11（並行稼働の合格）・#12（過去データの移行）・#49（1 時間足の過去分）・#54（新旧ビューアーの 1 週間の見比べ）・#13 が closed

   ```sh
   for i in 11 12 49 54 13; do echo "#$i $(gh issue view $i --json state -q .state)"; done   # すべて CLOSED
   ```

2. 新系が正常: 直近 7 日の `quality_daily` の `violations` が 0、`legacy_compare_daily` の `unexplained` が 0（「日次の品質チェックと新旧の突き合わせ（#40）」の「日次の確認」）。新系の M1・M2 が有効で、直近 24 時間に通知が無い。`infra/prod/variables.tf` の `ingest_paused` が `false`
3. 新ビューアーで代替できる: `/smile`・`/atm_iv`（`d`・`n` を変えて）が新ビューアーで表示でき、旧ビューアーでしか見られないもの（旧型 `/atm` は #58 で廃止済み）が無いことを所有者が確かめる
4. 旧 HTTP 関数を旧ビューアー以外から呼んでいるものが無い（直近 7 日の実行数。所有者の閲覧の分だけのはず。想定外に多ければ止める前に呼び出し元を調べる）

   ```sh
   gcloud logging read 'resource.type="cloud_function" AND resource.labels.region="'$OLD_REGION'" AND textPayload:"Function execution started"
     AND resource.labels.function_name=("smile_data" OR "atm_data" OR "atm_iv_data")' \
     --project=$P --freshness=7d --format='value(resource.labels.function_name)' | sort | uniq -c
   ```

5. 日程: **土曜 13:00 JST 以降**に始め、日曜のうちに終える（「過去データの移行（#12）」の「いつ行うか」と同じ理由。旧系・新系とも書き込みが無く、legacy-stage / legacy-copy の手動実行が毎朝の定期実行と重ならない）。土曜 07:30 の突き合わせで金曜分の記録があること

### 停止の手順

上から順に。1 つの端末で。

1. 停止前の記録: `legacy_state > $W/legacy-before.txt; curl -s -o /dev/null -w '%{http_code}\n' "https://$(gcloud app describe --project=$P --format='value(defaultHostname)')/" > $W/app-before.txt`（旧 URL は IAP のログインへのリダイレクトで 302 のはず。scheduler 2 行が `ENABLED`、invoker 3 行が `["allUsers"]`、app が `SERVING`、アラート 4 本、DTS 5 本がすべて `disabled=false`）
2. 新系の取りこぼしを補う: 「過去データの移行（#12）」の「E 以降の取りこぼしを補う」。E から金曜の窓までで `legacy_compare_daily` の `legacy_only_snapshots` が 1 以上の日すべて。**Terraform の apply（手順 4）の前に行う**（DTS を無効にすると legacy-stage / legacy-copy の手動実行ができない）。補った日の 1 時間足も作り直す
3. 1 時間足の旧系の同値の確認を直近 30 日でもう一度流し、件数を #49 に記録する（「1 時間足（#49）」の「旧系との突き合わせ」。`insert_ohlc` を止める前の最後の記録）
4. Terraform: `infra/prod/variables.tf` の `legacy_stopped` の既定値を `true` にする PR を出す。PR の plan の要約が **update 3**（`google_bigquery_data_transfer_config.legacy_stage` / `legacy_copy` / `legacy_compare` の `disabled`）と **delete 2**（`google_monitoring_alert_policy.this["m1"]` / `["m2"]`）だけであることを見てマージし、deploy の `apply` job を承認する。**Scheduler より先に**行う（先に Scheduler を止めると旧系の M1 が 35 分前後で鳴る）
5. Scheduler を止める（取り込みの入口。`download_jpx` → GCS → `load_jpx_into_bq` と `insert_ohlc` が動かなくなる）

   ```sh
   for j in $OLD_JOBS; do gcloud scheduler jobs pause $j --location=$L --project=$P; done
   ```

6. HTTP 関数の公開を外す（旧ビューアーのデータ API。共通パスワードを知っていても呼べなくなる）

   ```sh
   for f in $HTTP_FNS; do
     gcloud functions remove-iam-policy-binding $f --region=$OLD_REGION --project=$P \
       --member=allUsers --role=roles/cloudfunctions.invoker --format='value(etag)' >/dev/null && echo "removed $f"
   done
   ```

7. App Engine を無効化する（旧ビューアー）。`servingStatus` は数分で `USER_DISABLED` になるが、配信が完全に止まるまで**最大 1 時間**かかる（その間は旧 URL が応答することがある。手順 10 で 1 時間後に確かめる）。連携している Datastore の読み書きも止まる（上の表）ので、必ず手順 5 の Scheduler の停止の後に行う

   ```sh
   app_set USER_DISABLED   # done が false でも受け付け済み。数分おきに app_status を見る
   app_status              # USER_DISABLED
   ```

8. 旧コードの削除の PR（EPIC 終了条件 7）: `appengine/`、`functions_py/atm_iv/`、`functions/main.py` の `smile_data`・`atm_data`・`check_auth` と `auth.txt` の読み込み、`memo.txt` の該当のデプロイ手順。`download_jpx` / `load_jpx_into_bq`（`functions/main.py` の残り）・`functions_py/timeframe/`（`insert_ohlc`）のコードは #24 まで残す（切り戻しで再開する関数の記録）。plan の差分は 0。判定は `grep -rlE "open\(['\"]auth\.txt" --include=*.py apps functions functions_py appengine` が 0 件（`apps/viewer/tests/test_image.py` は「イメージに auth.txt が無い」ことの検査で文字列を含むので、EPIC の測定コマンドの `grep -rl auth.txt` には当たる。この検査は数えない）
9. 停止後の記録: `legacy_state > $W/legacy-after.txt; diff $W/legacy-before.txt $W/legacy-after.txt`。差分が次のとおりであること

   - scheduler 2 行が `PAUSED`、invoker 3 行が `[]`、app が `USER_DISABLED`
   - アラートは `optionchan: M1 ingest ...`・`optionchan: M2 ingest ...` の 2 本だけ（どちらも `True`）
   - DTS は `legacy-stage` / `legacy-copy` / `legacy-compare` が `disabled=true`、`quality-daily` / `atm-hourly` が `disabled=false`
   - 新系は変わっていない: `gcloud scheduler jobs describe ingest --location=$L --project=$P --format='value(state)'` が `ENABLED`（`legacy_state` の対象外なので別に見る）

10. 呼び出しが止まったこと（手順 5 の 5 分後以降）と、関数が公開されていないこと:

    ```sh
    gcloud logging read 'resource.type="cloud_function" AND resource.labels.region="'$OLD_REGION'" AND textPayload:"Function execution started"' \
      --project=$P --freshness=5m --format='value(resource.labels.function_name)' | grep -c .   # 0
    for f in $HTTP_FNS; do
      u=$(gcloud functions describe $f --region=$OLD_REGION --project=$P --format='value(httpsTrigger.url)')
      echo "$f $(curl -s -o /dev/null -w '%{http_code}' "$u")"   # 403
    done
    # 新系の ingest が毎分動き続けていること（直近 5 分の 2xx。時間外も 204 で数える）
    gcloud logging read 'resource.type="cloud_run_revision" AND resource.labels.service_name="ingest" AND httpRequest.status>=200 AND httpRequest.status<300' \
      --project=$P --freshness=5m --format='value(timestamp)' | grep -c .   # 4〜5
    # 手順 7 の 1 時間後: 旧 URL が配信されないこと（手順 1 の記録（302）と違い、200・302 でない）
    curl -s -o /dev/null -w '%{http_code}\n' "https://$(gcloud app describe --project=$P --format='value(defaultHostname)')/"; cat $W/app-before.txt
    ```

    新系の ingest の 2xx が 0 なら、新系の M1 より先に「新しい取り込み（ingest）」の手順で原因を見る（旧系の停止とは独立しているはずなので、旧系を戻す前に新系を調べる）

11. 結果を #14 にコメントする（停止した日時（分まで）と、手順 9・10 の要約。`diff` の全文・URL は貼らない）

### 停止後の確認（2 週間）

止めた日から **2 週間（10 営業日以上）**。毎朝 07:45 以降に見て、要約を #14 に記録する（週 1 回まとめてよい。異常はその日に）。

- 新系の取り込み: 「日次の品質チェックと新旧の突き合わせ（#40）」の「日次の確認」の `quality_daily`（`violations` が 0、取引日の `snapshots` が止める前の同じ曜日と同程度）。legacy-compare は止めているので、その結果は増えない（実行の状態も見なくてよい）
- 新系のアラート: 新系の M1・M2 の通知が来たら、「新しい取り込みの監視（#35）」の手順で原因を調べ、**誤報**（アラートの定義の誤り）か **実障害**か、取りこぼしが残ったか（07:00 の照合の `raw_unrecovered`）を記録する
- 旧系が止まったまま: `legacy_state` が手順 9 と同じ（誰かが戻していない）、手順 10 の実行数が `--freshness=1d` で 0
- 課金（止めてから 3 日以上たってから。反映は 1 日ほど遅れる）: 旧系のサービスの日ごとの費用が止めた翌日から 0

  ```sh
  bq query --project_id=$P --use_legacy_sql=false '
  SELECT DATE(usage_start_time, "Asia/Tokyo") AS d, service.description AS service, ROUND(SUM(cost), 2) AS cost
  FROM `'"$P"'.PaymentInfo.gcp_billing_export_v1_*`
  WHERE project.id = "'"$P"'" AND usage_start_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
    AND service.description IN ("Cloud Functions", "App Engine", "Cloud Pub/Sub", "Cloud Datastore", "Cloud Firestore", "Cloud Scheduler")
  GROUP BY d, service HAVING cost > 0 ORDER BY d, service'
  ```

  Cloud Scheduler はジョブ数で課金されるので、一時停止中のジョブが数えられて 0 にならないことがある（請求先アカウントの無料枠 3 ジョブの中なら 0。削除は #24）。GCS `optionchan` の保存料金は中身が 3 日で消えるので 0 に近づく

### #24 に進む判断

次をすべて満たしたら #14 を閉じ、#24（不可逆）に進む。#24 の前に所有者の確認を取る（関数・Pub/Sub・Datastore は消すと戻せず、python37 の関数は再デプロイもできない）。

1. 止めてから 2 週間（10 営業日以上）たち、その間に切り戻していない
2. 新系の M1・M2 の誤報が 0、新系の取りこぼし（07:00 の照合で回収できずに残ったもの）が 0（target.md 6 章 5a の完了判定）。実障害で鳴った場合は、原因と対応が #14 に記録され、旧系を戻す必要が無かったこと
3. 2 週間の `quality_daily` の `violations` がすべて 0
4. 旧系の関数の実行が 0、App Engine が `USER_DISABLED`、Scheduler 2 ジョブが `PAUSED` のまま
5. 課金エクスポートで Cloud Functions・App Engine の日ごとの費用が止めた翌日から 0
6. 旧ビューアーが無くて困ったことが無い（所有者）
7. EPIC 終了条件 5（旧系向けアラート 0 本）・7（`allUsers` の付いた関数 0、`auth.txt` を読むコード 0）を満たしている（下の「EPIC の終了条件の判定」）

### EPIC の終了条件の判定（#14 の担当分）

```sh
# 5: 旧系向けアラート 0 本、新系向けが 2 本以下（2027-09-01 の時点でも同じであること）
gcloud monitoring policies list --project=$P --format='value(displayName,enabled)' | grep '^optionchan: '   # ingest の 2 本だけ
# 7: allUsers / allAuthenticatedUsers の付いた関数・Cloud Run サービスが 0
for f in $(gcloud functions list --project=$P --format='value(name.basename())'); do
  r=$(gcloud functions list --project=$P --filter="name~/$f\$" --format='value(name.segment(3))')
  gcloud functions get-iam-policy $f --region=$r --project=$P --format=json | jq -r --arg f $f '.bindings[]?.members[] | select(test("^all")) | $f'
done | grep -c .   # 0
for s in $(gcloud run services list --project=$P --format='value(metadata.name)'); do
  gcloud run services get-iam-policy $s --region=$L --project=$P --format=json | jq -r --arg s $s '.bindings[]?.members[] | select(test("^all")) | $s'
done | grep -c .   # 0
# 7: auth.txt を読むコード 0（手順 8 の判定）
grep -rlE "open\(['\"]auth\.txt" --include=*.py apps functions functions_py appengine | grep -c .   # 0
```

### 切り戻し

戻すのは新系に問題が見つかり、旧系の取り込み・旧ビューアーをもう一度使う必要があるとき。止めていた間の旧系のデータは戻らない（JPX のページは今の値だけ）。その間は新系のデータだけになる。

**全部を戻す**（下の順。App Engine を先に戻すのは、無効化中は連携している Datastore の読み書きも止まり、`download_jpx`（前回の先物の時刻を読み書きする）が失敗するため。`SERVING` に戻してから Datastore が使えるまで時間がかかることがあるので、Scheduler の再開の後に `download_jpx` の実行とエラーを見る）:

```sh
app_set SERVING; sleep 60; app_status   # SERVING
for f in $HTTP_FNS; do
  gcloud functions add-iam-policy-binding $f --region=$OLD_REGION --project=$P \
    --member=allUsers --role=roles/cloudfunctions.invoker --format='value(etag)' >/dev/null && echo "added $f"
done
for j in $OLD_JOBS; do gcloud scheduler jobs resume $j --location=$L --project=$P; done
# 5 分後: download_jpx の実行が再開していること（取引時間外も毎分起動する）
gcloud logging read 'resource.type="cloud_function" AND resource.labels.function_name="download_jpx" AND textPayload:"Function execution started"' \
  --project=$P --freshness=5m --format='value(timestamp)' | grep -c .   # 1 以上
```

GitHub secret `TF_ALERT_POLICY_IDS` に消したポリシー（`m1`・`m2`）の ID が残っていれば、PR の前に消す（`gh secret delete TF_ALERT_POLICY_IDS --repo $REPO`。残っていると作り直した後の import が古い ID を探す）。続けて `legacy_stopped` の既定値を `false` に戻す PR（plan は create 2（`m1`・`m2`）と update 3（DTS の `disabled`））。**Scheduler の再開と download_jpx の実行を確かめてから**承認する（先に作ると旧系の M1 が鳴る）。作成後に旧系の M1 が鳴ったら、上の download_jpx の実行数を見る。legacy-compare は再開の翌朝から動き、止めていた間の日は旧系の行が無いので `legacy_snapshots = 0`（営業日に数えない）。最後に `legacy_state` が手順 1 の記録と同じになることを `diff` で見る。手順 8 の旧コードの削除は戻さなくてよい（デプロイ済みの関数・アプリは変わっていない）。

**一部だけ戻す**:

| 戻すもの | 手順 | 注意 |
|---|---|---|
| 旧ビューアーだけ（閲覧に旧画面が要る） | App Engine を `SERVING`、HTTP 関数 3 つに `allUsers` | 旧データセットは止めた時点までしか無い（取り込みは止まったまま）。公開を戻している間は、予算アラートの備え（#21）だけになる。用が済んだらまた止める |
| 旧系の取り込みだけ | App Engine を `SERVING`（連携している Datastore を `download_jpx` が使うため）、Scheduler を `resume`、`legacy_stopped = false` の PR | 旧ビューアーは止めたままでよい |

## 取引カレンダー（#63）

新しい取り込み（`apps/ingest`）が時間外・休場日の判定に使う休業日の一覧を、JPX「祝日取引」の祝日取引実施日の表（https://www.jpx.co.jp/derivatives/rules/holidaytrading/index.html ）から**自動で**取り込む。JPX は毎年 2 月に翌年の予定を、6 月と 12 月に翌年の上半期・下半期の確定を公表する。手で TOML を直す運用は無い（`config/trading_calendar.toml` は取引時間の設定と、BigQuery を読めないときの予備）。

- **取り込み**: 毎日 06:40 JST の ingest のリクエスト（時間外。Scheduler は足さない）で、通常の処理の後に同じ期限の残りで行う（`apps/ingest/src/optionchan_ingest/calendar_sync.py`）。ページを取得・解析し、内容が最新版と同じなら INFO `calendar_checked` で終わる。変わっていれば内閣府の祝日 CSV（https://www8.cao.go.jp/chosei/shukujitsu/syukujitsu.csv ）も取得して検証し、通れば `optionchan_tokyo.trading_calendar` に新しい版を追記して NOTICE `calendar_updated`（今日以降の休業日の追加 `added_closed`・削除 `removed_closed`、`covered_until` の新旧）を出す
- **ingest の読み方**: インスタンスの最初のリクエストと毎時 0 分にビュー `trading_calendar_latest` を読む。読めない・版が無いなら、前に読めた版（無ければイメージの TOML）のまま（WARNING `calendar_load_failed`）。一度も読めていなければ 5 分後（`CALENDAR_RETRY_SECONDS`）以降の次のリクエストで読み直す。TOML の `covered_until` の方が新しければ TOML を使う（WARNING `calendar_source_conflict`）
- **ingest 側の検査（多層防御）**: 次の版は使わずに今の値のまま、ERROR `calendar_load_rejected`（同じ版・同じ理由では 1 回）: `content_hash` が再計算と合わない（行の欠け・同じ `version_at` の重複・書き換え）、その他の不正な値、今日から 14 日以内に今の値（前の版か TOML）に無い休業日が増える（`accepted = override` の版は確認済みなので見ない）、1 年の休業平日が 25 を超える
- **09:00 の出所の通知**: BigQuery の版を読めない・使えずに TOML で判定していれば ERROR `calendar_fallback`。TOML の方が新しく意図して TOML を使っているなら WARNING `calendar_fallback`（`reason=image_newer`）
- **期限**: `covered_until`（確定の最終日）の 14 日前から 09:00 に WARNING `calendar_expiring`、過ぎたら ERROR `calendar_outdated`（従来どおり）。自動化の後は「確定の公表を取り込めていない」ことの最後の検知になる。過ぎても取り込みは止めない（書いていない平日は取引日として扱う。期限切れの間は鮮度判定をしない）
- 列・最新版の選び方・SQ 日の繰り上げへの使い方（読み手の契約）は `docs/architecture/target.md` 3.2「取引カレンダー（#63）」

### 検証のルール

どれか 1 つでも当たれば書かずに ERROR `calendar_sync_rejected`（`problems` に `rule: 詳細`）。ingest は前のカレンダーのまま。

| rule | 内容 | `--accept` で越えられるか |
|---|---|---|
| （解析の失敗。`calendar_sync_failed` の `stage=parse`） | 見出し「祝日取引実施日」・表の見出し（実施有無・確定/予定）・日付・曜日・実施有無・確定/予定の形が違う、確定の行が無い | 不可（パーサーを直す） |
| `years_mismatch` / `current_year_missing` / `row_count` / `duplicate` / `weekend` / `status_order` | 表の前の文の年と表の年が違う、今年が無い（12 月は翌年があればよい。今年の残りは前の版から引き継ぐ）、1 年の行数が 10〜25 でない、同じ日付、土日の行、予定の後に確定 | 不可 |
| `year_end_missing` / `year_end_held` / `jan1_held` | 平日の 12/31・1/2・1/3 が表に無い、12/31・1/2 が「実施する」、1/1 が「実施する」 | 不可 |
| `not_holiday` / `holiday_missing` / `not_year_end` | 祝日 CSV に無い日が祝日として載っている、CSV の平日の祝日（1/1 を除く）が表に無い | 不可 |
| `unverified_confirmed` | 祝日 CSV にまだ無い年に「確定」の行がある（予定だけなら照合を飛ばして受け入れ、WARNING `calendar_unverified_year`） | 可 |
| `past_changed` | 過去日の休業/取引日が変わる | 可 |
| `covered_until_regressed` / `listed_until_regressed` | 確定の最終日・掲載範囲が後退する | 可 |
| `confirmed_changed` | 今日以降の確定の日が消える・予定に戻る・実施有無が変わる | 可 |
| `too_many_changes` | 前の版の掲載範囲の中で、今日以降の休業日の追加・削除が 4 件以上（翌年分の追加は数えない） | 可 |
| `closes_soon` | 今日から 14 日以内の日が新しく休業日になる（誤りなら取引日を取りこぼす。取引日に変わる向きは受け入れる。ingest 側の検査と同じ日数） | 可 |

- 版が無いとき（初回）は、イメージの TOML を前の版として差分を見る（過去日は TOML に書かないので見ない）
- 取引時間の表が TOML の `[sessions]` と違えば ERROR `calendar_sessions_changed`（休業日の更新は止めない。取引時間は自動で変えない）
- 1/1 は表に載らない（注1）ので、平日なら規則で休業の行を足す（`source = rule`）。表から消えた過去年の行は前の版から引き継ぐ

### デプロイ後（初回）

apply の後、手動のジョブで最初の版を書く（翌朝 06:40 を待ってもよい）:

```sh
gcloud run jobs execute calendar-sync --region $L --project $P --wait --args=--dry-run
gcloud run jobs execute calendar-sync --region $L --project $P --wait
bq query --location=$L --use_legacy_sql=false --format=pretty '
SELECT calendar_date, is_trading_day, status, day_type, name, covered_until, version_at
FROM `optionchan_tokyo.trading_calendar_latest` WHERE NOT is_trading_day ORDER BY calendar_date'
# 今日以降の休業日・covered_until が config/trading_calendar.toml と一致すること
gcloud logging read 'resource.type="cloud_run_revision" AND resource.labels.service_name="ingest" AND jsonPayload.event=("calendar_loaded" OR "calendar_load_failed" OR "calendar_load_rejected")' \
  --project $P --freshness=2h --limit 5 --format='value(timestamp,jsonPayload.event,jsonPayload.version_at,jsonPayload.detail)'
```

### ERROR を受けたとき

| event | 対応 |
|---|---|
| `calendar_sync_failed`（`stage`） | `load`: ビューを読めない（権限・BigQuery の障害）。`fetch_page` / `fetch_holidays`: JPX・内閣府のサイトの障害（翌日に再試行される。続くなら URL の変更を疑う）。`parse` / `parse_holidays`: 形の変更。ページを開いて `jpx_calendar.py` の解析を直す PR（fixture `apps/ingest/tests/fixtures/holiday_trading.html` も必要部分だけ差し替える）。`write`: 書き込みの失敗（翌日に再試行）。`deadline`: 期限切れ（翌日に再試行。急ぐなら手動のジョブ）。`unexpected`: `detail` を見て Issue にする |
| `calendar_sync_rejected` | `problems` を読み、JPX のページと見比べる。ページが正しく（例: BCP テストで近い祝日を休みにする公表）、`overridable` が true なら、`--dry-run` で `added_closed` / `removed_closed` を確かめてから `--accept` で受け入れる。false ならページかパーサーの問題（パーサーを直すか、ページが直るのを待つ）。受け入れるまで ingest は前のカレンダーのまま（新しい休業日にも JPX を取りに行くだけで、取りこぼしは無い） |
| `calendar_sessions_changed` | 取引時間の変更。JPX の公表を確かめ、`config/trading_calendar.toml` の `[sessions]` と鮮度のしきい値・Scheduler 前提（target.md 3.1）を見直す PR |
| `calendar_fallback`（09:00） | ingest が BigQuery の版を使えていない。直前の `calendar_load_failed` / `calendar_load_rejected` の `detail` を見る（権限・ビューの有無・版の不正）。版が無いなら上の「デプロイ後（初回）」 |
| `calendar_load_rejected` | `detail` が `data:`（`content_hash` の不一致・重複など）なら下の「版・ビューが壊れたとき」。`closes_soon` なら近い休業日が増える版（JPX のページで確かめ、正しければ `--accept` で取り込み直すと `override` の版になり ingest も使う）。`too_many_closed` はページか解析の誤り |
| `calendar_outdated` / WARNING `calendar_expiring` | 確定の公表（6 月・12 月）を取り込めていない。`calendar_sync_*` の ERROR が出ていないか、ページに確定が出ているかを見る。手動のジョブを `--dry-run` で実行して結果を見る |

```sh
# 手動の取り込み（理由は 200 文字まで。ログに残る）
gcloud run jobs execute calendar-sync --region $L --project $P --wait --args=--dry-run
gcloud run jobs execute calendar-sync --region $L --project $P --wait '--args=--accept=JPX の公表（BCP テスト）を確認'
# 結果
gcloud logging read 'jsonPayload.event:"calendar_" AND (resource.labels.job_name="calendar-sync" OR resource.labels.service_name="ingest")' \
  --project $P --freshness=1d --limit 20 --format=json \
  | jq -c '.[] | {t: .timestamp, e: .jsonPayload.event, stage: .jsonPayload.stage, problems: .jsonPayload.problems, added: .jsonPayload.added_closed, removed: .jsonPayload.removed_closed, covered_until: .jsonPayload.covered_until}'
```

ジョブの終了コード: 0 = 更新・変化なし・dry-run、1 = 失敗・拒否、2 = 引数の誤り。間違った版を受け入れたときは、正しいページで `--accept` を付けて取り込み直す（新しい版が最新版になる。古い版は履歴として残る）。**緊急回避**（ページが直らない・すぐ止めたい）: `config/trading_calendar.toml` を正しい `closed_dates` にし、`covered_until` を最新版より後の日にした PR をデプロイすると、ingest は TOML を使う（WARNING `calendar_source_conflict`）。直ったら TOML の `covered_until` を戻す。

### 版・ビューが壊れたとき

- **同じ `version_at` の重複・行の欠け**（`calendar_load_rejected` の `detail` が `data: content_hash が合わない` など。手動のジョブと 06:40 が同時に書いた等）: ingest は前の値（無ければ TOML）のままなので急がない。新しい版を書けば最新版が置き換わる。取り込みも前の版を読めずに `calendar_sync_failed`（`stage=load`）になるので、`--accept` を付けて手動のジョブを実行する（壊れた前の版を無視し、TOML を基準に検証して新しい版を書く。WARNING `calendar_previous_ignored`）:

```sh
gcloud run jobs execute calendar-sync --region $L --project $P --wait --args=--dry-run,--accept=壊れた版の復旧
gcloud run jobs execute calendar-sync --region $L --project $P --wait --args=--accept=壊れた版の復旧
# 壊れた版を履歴から消すとき（任意。書いてから 30 分ほどは DML で消せないことがある）
bq query --location=$L --use_legacy_sql=false 'SELECT version_at, COUNT(*) rows, COUNT(DISTINCT content_hash) hashes FROM `optionchan_tokyo.trading_calendar` GROUP BY version_at ORDER BY version_at DESC LIMIT 5'
bq query --location=$L --use_legacy_sql=false 'DELETE FROM `optionchan_tokyo.trading_calendar` WHERE version_at = TIMESTAMP("<壊れた版の version_at>")'
```

- **ビューが消えた・定義が変わった**: ingest・取り込みは `calendar_load_failed` / `calendar_sync_failed`（`stage=load`）になり、前の値か TOML で動き続ける。ビューは Terraform（`infra/prod/calendar.tf`）が持つので、deploy workflow を再実行するか、次の PR の apply で作り直される（plan に `trading_calendar_latest` の create / update が出る）。テーブル `trading_calendar` が消えたときはタイムトラベル（7 日）で戻す（`bq cp optionchan_tokyo.trading_calendar@<ミリ秒> optionchan_tokyo.trading_calendar`）か、apply で作り直して手動のジョブで最初の版を書く

### TOML（`config/trading_calendar.toml`）

- ingest が読むのは取引時間（`[sessions]`）・`grace_minutes`・`no_night_session_before` と、BigQuery の版を読めないときの予備の `closed_dates` / `covered_until`
- 別リポジトリのシミュレーターが TOML の raw URL を同期しているので、**シミュレーターが BigQuery の版を読むようになる（縦切り 2）まで**、休業日が変わったら TOML も合わせる。最新版から生成できる（JPX にはアクセスしない。コメント・他のキーはそのまま、`covered_until` と今日以降の `closed_dates` だけ置き換える）:

```bash
cd apps/ingest
GCP_PROJECT=$P uv run --locked python -m optionchan_ingest.calendar_sync --print-toml > /tmp/trading_calendar.toml
diff ../../config/trading_calendar.toml /tmp/trading_calendar.toml   # 差分を見て、コメント（確定の時期など）を直して PR
uv run --locked pytest -q tests/test_calendar.py tests/test_jpx_calendar.py
```

- **廃止の条件**: シミュレーターの縦切り 2 が済み、かつ 2027-06 の確定の公表（2027 年下半期）を自動で取り込めたこと。廃止しても `[sessions]` などの設定は残し、`closed_dates` / `covered_until` だけを消す（BigQuery を読めないときは書いていない平日を取引日として扱う）。消すときは `apps/ingest/src/optionchan_ingest/calendar.py` の `load_calendar` のキーの検査（`_TOP_KEYS`）と `Calendar` の予備の値の扱い（`CalendarHolder` の TOML との比較、`--print-toml`）も直す（直さないと読み込みでエラーになる）
- `no_night_session_before`（直前の夜間セッションが無い取引日）は JPX の表に無いので、公表されたら従来どおり TOML に足す（通常は空）

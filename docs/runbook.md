# runbook

本番（自分の GCP プロジェクト。コマンドでは下の「共通の変数」の `$P`）の運用手順。節は設計（`docs/architecture/target.md` 4.1）の項目ごとに、各 Issue で追記する。

## コスト

予算アラートと BigQuery の 1 日あたりクエリ量の上限（#21）。どちらも Terraform 管理外で、この runbook で管理する（#10 で判断。予算は他プロジェクトと共有の請求先アカウント側のリソースで、クエリ量上限の変更には `serviceusage.quotaAdmin` 相当が要るため、CI 用 SA に権限を渡さない）。

| 項目 | 設定 |
|---|---|
| 予算 | 表示名 `optionchan: 月額予算`。このプロジェクトのみ、暦月、500 円（税抜き、クレジット込みの実請求で比較）、実績の 50 / 80 / 100% で通知。通知先は既存の Monitoring メール通知チャネルのみ（請求先アカウントは他プロジェクトと共有のため、IAM ロール保持者への既定メールは無効） |
| クエリ量上限 | `bigquery.googleapis.com/quota/query/usage`、unit `1/d/{project}`。2026-10-18 時点の effectiveLimit は 102400 MiB（100 GiB/日。2026-10-12 の設定は 32768 MiB = 32 GiB/日。下の「設定の記録」）。Service Usage の consumer override で設定。ユーザー単位は設定しない |

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

下げる前に、今日（太平洋時間）のスキャン量（以下「今日のスキャン量」）が新しい上限より十分小さいことを確認する（超えていると、その日の残りのクエリがすぐ失敗する）。上限はプロジェクト全体（全リージョン）にかかるので、US（旧系）と東京（新系・移行）を合計する。スクリプトの親ジョブ（`statement_type = "SCRIPT"`）は子の文の量を重ねて数えるので除く:

```sh
for r in region-us region-asia-northeast1; do
  bq query --project_id=$P --location=$([ $r = region-us ] && echo US || echo asia-northeast1) --use_legacy_sql=false --format=csv "
  SELECT IFNULL(SUM(total_bytes_processed), 0) AS bytes
  FROM \`$r\`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
  WHERE job_type = 'QUERY' AND IFNULL(statement_type, '') != 'SCRIPT'
    AND creation_time >= TIMESTAMP(CURRENT_DATE('America/Los_Angeles'), 'America/Los_Angeles')" | tail -n +2
done | awk '{ s += $1 } END { printf "%.2f GiB\n", s / 1024 / 1024 / 1024 }'   # 2 リージョンの合計
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

設定の記録: 2026-10-12 に `gcloud alpha services quota update`（Service Usage の consumer override）で 32768 に設定した。2026-10-18 時点の effectiveLimit は 102400（上の「確認」のコマンドで見た値）。

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

Cloud Run のイメージ（取り込み `ingest` #33、ビューアー `viewer` #53）: タグは `src-<20 桁>` で、イメージの入力の git のハッシュから `.github/scripts/image.sh <ingest|viewer> tag` が決める（テスト・ドキュメントの変更ではタグが変わらない）。入力は ingest が `apps/ingest` の Dockerfile・`pyproject.toml`・`uv.lock`・`src/`、`schema/tokyo/`、`.dockerignore`（context はリポジトリ直下）、viewer が `apps/viewer` の Dockerfile・`.dockerignore`・`pyproject.toml`・`uv.lock`・`package.json`・`package-lock.json`・`src/`（context は `apps/viewer`）。plan はそのタグが Artifact Registry にあれば digest、無ければタグ参照を `ingest_image` / `viewer_image` に渡す。無いときは Cloud Run に差分が出るので `apply` job が動き、承認後に **GCP の認証より前に**ビルド・検査（ingest: import できる、`auth.txt`・`functions/`・テストが無い。viewer: `.github/scripts/viewer-image-check.sh`）してから `sa-tf-apply` で push し、digest で apply する。タグは不変（`immutable_tags`）。

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
LF="resource.type=\"cloud_run_revision\" AND resource.labels.service_name=\"ingest\" AND timestamp>=\"<T0+5m の RFC3339>\" AND timestamp<\"<T0+65m の RFC3339>\""
gcloud logging read "$LF AND severity>=ERROR AND jsonPayload.event:*" --project $P --format='value(jsonPayload.event)' | sort | uniq -c   # 0 件（stale だけは時刻と件数を記録）
gcloud logging read "$LF AND jsonPayload.event=\"ingested\"" --project $P --format=json \
  | jq '[.[].jsonPayload.elapsed_ms] | sort | {n: length, max: .[-1], p50: .[(length / 2 | floor)]}'   # max < 45000
gcloud logging read "$LF AND (textPayload:\"gunicorn\" OR textPayload:\"Booting worker\")" --project $P --format='value(severity)' | sort | uniq -c   # 起動行の severity を Issue に記録
```

時間外（平日 07:00〜07:30 JST）: `$LF` の時刻を差し替えて `httpRequest.status!=204 AND log_name="projects/$P/logs/run.googleapis.com%2Frequests"` が 0 件、同じ窓の `ingest_runs`（`snapshot_at` で絞る）が 0 行。

Terraform の差分 0: `gh workflow run deploy.yml --ref master` の plan が「差分なし」、bootstrap の `plan -detailed-exitcode` が 0。差分が出たら（CPU の `0.5` の正規化など）、コードを実態に合わせる PR を出す。

### 取得の秒（#38）

JPX のページは分単位の更新時刻が変わった後、分の頭の数秒で中身が書き換わる（1〜2 秒目の取得は書き換え途中の版だった）。取引時間中は分の頭から 8 秒（`apps/ingest` の `settings.FETCH_AT_SECOND`）まで待ってから 1 限月のページを取得する。到着が 8 秒を過ぎていれば待たない。時間外（204）は待たない。待ちも全体の期限 45 秒に含まれる。

- 変更: 環境変数 `INGEST_FETCH_AT_SECOND`（0〜20、0 で待たない。数値でない・範囲外だと最初のリクエストで設定を読むときに失敗し、全リクエストが ERROR `unexpected` の 500 になる）を `infra/prod/ingest.tf` に足す PR。既定では設定しない
- 記録: 取引時間中のログに `wait_ms`（待った時間）と `page1_fetch_second`（1 限月ページの取得時刻の分内の秒）が出る。生 HTML の各ページの `fetched_at` にも取得時刻がある（封筒の形式は変えていない）
- コスト: 待ちも課金対象のインスタンス時間になる。月の vCPU 秒の上限見積もりは 約 16.5 万（無料枠 18 万。target.md 7.2）。10 秒にすると上限 約 19.4 万で超えるので、秒を延ばすときは試算し直す。無料枠は請求先アカウントで他プロジェクトと共有している

確認（「ログ」と同じ `$LF`、取引時間中の窓）:

```sh
gcloud logging read "$LF AND jsonPayload.event=\"ingested\"" --project $P --format=json \
  | jq '[.[].jsonPayload | {s: .page1_fetch_second, w: .wait_ms}] | {n: length, min_second: (map(.s) | min), max_wait_ms: (map(.w) | max)}'   # min_second >= 8
```

時間外の 204 が待っていないこと（平日 07:00〜07:30 JST の窓。`latency` は "0.012s" の形）:

```sh
gcloud logging read "$LF AND log_name=\"projects/$P/logs/run.googleapis.com%2Frequests\" AND httpRequest.status=204" --project $P --format=json \
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

### JPX の接続・取り直し・本文の凍結（#88）

#11 の並行稼働の調査（2026-10-16）で見つかった 2 つの問題への対策。設定は `apps/ingest` の `settings.py`（`JPX_RETRY_*`・`BODY_FROZEN_REPEATS`）。

- **接続を使い回さない**: JPX への要求ごとに接続を張り直す（`io_jpx.fetch_page`。旧系の `requests.get` と同じ取り方）。2026-10-14 18:52〜19:27 JST の 36 分間、新系はヘッダの更新時刻だけ進み本文が 18:50〜18:51 の版のまま止まったページを受け続けた（旧系は同じ時間に止まった版と新しい版を交互に受けた）。インスタンスの中で使い回していた keep-alive の接続が、止まった返す元に張り付いた疑い（証明はできていない）
- **取り直し**: HTTP 5xx・接続エラー・タイムアウトのときだけ、1 リクエストで 1 回まで（1 限月・2 限月の合計）、3 秒待ってから取り直す。4xx・空ページは取り直さない（次の分に任せる）。期限 45 秒の残りから待ちと 15 秒（取り直しの後の GCS・BigQuery の分）を引いて 5 秒以上あるときだけ取り直し、取り直しのタイムアウトはその秒数で頭打ち（8 秒目の即時の 502 は取り直し、15 秒の読み取りタイムアウトの後は取り直さない）。JPX は毎日 04:00 JST 前後に 502 を返す傾向があり（2026-10-13〜10-15 に 1 回ずつ）、取り直しが無いとその分が欠けた
- **本文の凍結の検知**: 取引時間中（ページの更新時刻が立会の [開始, 終了)。寄り前・引け後・余裕の時間帯は数えない）に、option の本文（約定・気配・建玉の列。iv・ギリシャ指標は凍結中も変わるので入れない）が直前のスナップショットと同じことが 3 回続いたら、毎分 WARNING `body_frozen`。データはそのまま書く。判定はインスタンスのメモリなので、インスタンスが変わると数え直す。翌朝の品質チェックの `body_frozen` が必ず数える（「日次の品質チェックと新旧の突き合わせ（#40）」）。2026-10-13〜10-15 の取引時間中に本文が直前と同じだったのは、凍結を除くと静かな時間の 1 回（1 分）だけ

ログ（`jsonPayload.event`）:

| event | severity | 意味 |
|---|---|---|
| `jpx_retry` | INFO | 取り直す前（`page`・`detail` は 1 回目の失敗・`retry_wait_ms`） |
| （各ログの項目）`jpx_retry` | — | 取り直した（か期限で取り直さなかった）リクエストの以降のログに `{page, first, result}`。`result` は `ok`（取り直しで取れた）・`failed`（2 回目も失敗。`jpx_unavailable` の `detail` は 2 回目の失敗）・`skipped_deadline`（期限に余裕が無く取り直さなかった） |
| `body_frozen` | WARNING | 取引時間中に本文が直前と同じことが 3 回以上続いている（`body_repeats`・`body_since` = 本文が最後に変わったスナップショット・`snapshot_at`）。M2 では通知しない |

`body_frozen` を受けたら（ログか品質チェックの違反）: その時間の生 HTML（`source_object`）を 2〜3 分おきに開き、ヘッダの更新時刻・先物・option の約定時刻を見る。本文が止まっていれば JPX 側（返す元の一部が止まっている）で、取り込みの誤りではない。データは止まった版のまま入っていて直せない（JPX のページは今の値だけ）。時刻と件数を #11 に記録する。接続を張り直しているのに再発したら、凍結のときに取り直す（接続を変えて取り直す）対策を Issue にする。

確認（「ログ」と同じ `$LF`）:

```sh
gcloud logging read "$LF AND jsonPayload.jpx_retry.result:*" --project $P --format=json \
  | jq -c '.[].jsonPayload | {event, schedule_time, snapshot_at, r: .jpx_retry}'   # 04:00 JST 前後の 502 は result "ok" と ingested
gcloud logging read "$LF AND jsonPayload.event=\"body_frozen\"" --project $P --format='value(jsonPayload.snapshot_at,jsonPayload.body_repeats)' | wc -l   # 0
# JPX の取得時間（1 限月 + 存在確認 + 2 限月）の中央値。#88 の前後の取引時間中の同じ時間帯で比べ、#11 に記録する
gcloud logging read "$LF AND jsonPayload.event=\"ingested\"" --project $P --format=json \
  | jq '[.[].jsonPayload.jpx_fetch_ms] | sort | {n: length, p50: .[(length / 2 | floor)], max: .[-1]}'
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

確認（0 分の回の結果。窓は「ログ」と同じ `$LF` の形で、毎時 0〜1 分を含む時間帯にする）:

```sh
gcloud logging read "$LF AND jsonPayload.event=(\"recovery\" OR \"recovery_skipped\" OR \"raw_reconciled\")" --project $P --format=json \
  | jq -c '.[].jsonPayload | {event, schedule_time, listed, old_enough, pending, recovered, remaining, raw, unresolved, elapsed_ms}'
# 0 分のリクエスト全体（通常の処理 + 回収）の時間 = recovery の elapsed_ms（期限の起点から）
gcloud logging read "$LF AND jsonPayload.event=\"recovery\"" --project $P --format=json \
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
- `jpx_unavailable`（WARNING。空ページ・接続断）は単発では通知しない（HTTP 5xx・接続エラー・タイムアウトは同じ分の中で 1 回取り直した後。#88、「JPX の接続・取り直し・本文の凍結（#88）」。旧系の 2026-09-15〜10-12 のログで、空ページは 04:00 / 04:20 台を中心に 1 日 1〜2 回、すべて 1 分だけ）。取引時間中に続いたときは ERROR `jpx_unavailable_stale`（#42）で M2 に乗る。判定は `stale` と同じしきい値（日中 30 分 / 夜間 40 分）で、`jpx_unavailable` の分に生 HTML のバケットを直近のしきい値分だけ一覧し、最後に保存できたスナップショットがそれより古ければ出す。JPX の改版で更新時刻・行の要素が無くなった場合（空ページと区別できない）や、JPX からの拒否・障害の継続を、最大 30〜40 分遅れで拾う。このとき生 HTML は保存されないので、ページは通知を受けてから直接見る

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
| `calendar_outdated` / `calendar_unavailable` / `calendar_load_rejected` | 取引カレンダーの期限切れ（09:00 に 1 回）・BigQuery の版を一度も読めていない（休業日なしで判定中）・ingest が版を使わなかった（壊れている・不自然） | 「取引カレンダー（#63）」の「ERROR を受けたとき」 |
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
- 品質チェックの項目（`quality_daily` の列。説明は `schema/tokyo/quality_daily.json`）: `rowcount_mismatch`（`ingest_runs` の行数とテーブルの実件数）、`missing_table`（3 テーブルに同じスナップショット）、`dup_keys`（最新版のキーの重複）、`months_not_2`、`atm_not_1`（限月 × 種別で ATM が 1 行）、`price_time_future`（ページの更新時刻より後）、`price_time_too_old`（30 日以上前）、`sunday`、`body_frozen`（#88。取引時間中に option の本文が直前と同じことが 3 回以上続いたスナップショット。ヘッダの更新時刻だけ進み本文が止まった形。寄り前・引け後は数えない。2026-10-14 の窓は 34、10-13・10-15 は 0。#88 より前の行は NULL）。`orphan_rows`（書き込み途中の残骸）は情報で違反にしない
- 突き合わせの分類（`legacy_compare_daily` の列。説明は `schema/tokyo/legacy_compare_daily.json`、方法は `sql/legacy/compare.sql` の先頭）: 行単位（spot・future は `created_at` = T、option は T と `(last_trading_day, type, target_price)`）。JPX のページは各行が 1 分の中でばらばらに書き換わり、旧系は更新時刻の表示が変わった直後に取得するので、旧系の T の行を新系の T → 直前・直後のスナップショットの同じ行と比べる。説明済み: 直前か直後と全列一致（`*_version`、版の差）、option で約定の列（price・price_time・diff・diff_rate・volume・positions・quotation・quotation_date と、約定価格から求める iv・ギリシャ指標）が T・直前・直後のどれかと一致し気配と表示の列（bid・ask とその数量・IV、is_atm）だけ違う（`option_quote_only`、1 分の中の版の差）。option の `version`・`quote_only` は旧系の行が新系の T にもあるときだけ（新系の T の行の欠けは説明しない）、spot・future で新系の T が直前と同じ中身で旧系だけ新しい、または新系の T が約定時刻だけ進み価格の列（price・diff・diff_rate）が直前のままで、旧系が同じ約定時刻・同じ基準価格（price − diff）・同じ hv の別の価格のもの（`*_new_stale`、新系の版の遅れ・書き換え途中の版）。新系の版の遅れ（新系の T が直前と同じ中身）は 1 日に表ごと 3 行までなら説明済み、超えたらその形の行を全部 `unexplained` に数える（新系が系統的に古い版を返す不具合を捕まえる。上限 3 は #61 で 2026-10-13 08:52 JST〜10-14 01:08 JST の spot 2 行・future 2 行に余裕を持たせて決めた値。その 4 行は #89 でどれも `*_new_behind`（1 分で追いつく形）に移り、#89 の後にここへ残るのは 2 分以上止まる・1 分を超えて遅れる形だけ。2026-10-13〜15 の再計算では 10-14 の凍結の future 14 行だけで、上限を超えて `unexplained`）。書き換え途中の版は、旧系の版を新系も T の後 5 分の中で取っていれば裏付けあり。裏付けが無いもの（新系の価格の持ち越しの誤りと区別できない）は 1 日に表ごと 3 行までなら説明済み、超えたらその表の裏付けの無い行を全部 `unexplained` に数える（系統的な持ち越しの不具合は 1 日に多数出るので捕まえる。2026-10-13 は future 1 行で、上限 3 はそれに余裕を持たせた値）、新系の 1 分遅れ（`*_new_behind`、#89。下）、旧系が新系の T より古い版・途中の版を読んだ（`*_legacy_older`、#61。下）と、その裏付けだけが無いもの（`*_legacy_older_unconfirmed`、#89。下）、年の補完の修正（`future_contract_year`・`option_quotation_year`）、2 限月ページの版（`option_page2`。新系の T にも行があるときだけ）、新系のみ（`new_only_snapshots`、保存条件の差）。`unexplained` = 旧系のみ（新系の取りこぼし）+ 各表の説明できない行（約定の列がどの候補とも合わないなど）。例は `unexplained_samples`（先頭 20 件）
- `*_legacy_older`（#61）: 上のどれにも当たらない行のうち、次をすべて満たすもの。(1) 新系の T が旧系の行から進んでいる（spot・future は約定時刻が後、option は出来高が多く約定時刻が同じか後）。(2) 旧系の行が新系の T より前の版と合う: (a) T の 10 分前〜T の前のスナップショットの同じ行と一致（spot・future は全列、option は約定の列。気配と表示の列は問わない）= 旧系が古い版を読んだ、または (b) 新系の連続する 2 スナップショット S1 < S2（S1 は T の 10 分前以降、S2 ≤ T）の版の間 = 途中に出た版を旧系だけが読んだ。(b) は option なら出来高が S1 < 旧 < S2（狭い不等号）・約定時刻が S1 ≤ 旧 ≤ S2・基準価格（price − diff）と清算値・清算日が S1 か S2 と同じ・建玉が S1 と S2 の間。spot・future は出来高が無いので約定時刻が S1 ≤ 旧 < S2（S1 と同じ分は、その分の中の別の約定を旧系が読んだ形。#89 で足した）・基準価格と hv が S1 か S2 と同じ（future は限月も）。(3) 新系の T の値の裏付け: 旧系の次のスナップショットの同じ行が新系の T と同じ値（spot・future は全列、option は約定の列）。または (a) と S2 < T の (b) に限り、新系の直後のスナップショットの同じ行が同じ値。新系の直後は新系自身の読み取りで独立ではない（同じ誤りが直後まで続けば満たす）ので、S2 = T の (b) には使わない。比べる相手が無ければ説明しない。(4) 旧系の次に同じ行があれば新系の T と矛盾しない（option は出来高・約定時刻が新系の T 以上、spot・future は約定時刻が新系の T 以後）。10 分は 2026-10-13 の実データで見た最も古い版（2 分前）に余裕を持たせた上限。限界: 新系の T の誤りが直後のスナップショットにも続き、旧系の次がそれと矛盾しない形は (a) と S2 < T の (b) で説明済みになりうる。また旧系の行が新系の直後と一致すると `version` になるので、新系の T だけが 1 回誤り旧系が正しい値を読んだ形は検出できない（#61 の前からの `version` の性質）。`tests/test_sql_bq.py` で検出できる形・できない形を確かめている
- `*_new_behind`（#89。spot・future）: 新系の T の約定時刻が旧系より前で、新系の直後のスナップショットが T と違う版に進み、その約定時刻が旧系と同じか後（新系が直後で追いついた）。遅れは 1 分ちょうど（旧系の約定時刻 ≤ 新系の T + 1 分）。基準価格（price − diff）と hv が新系の T と同じ（future は限月も）。`*_new_stale` の判定より前に見る（新系の T が直前と同じ中身でも、1 分遅れで直後に追いつけばこちら）。長い凍結の最後の行（直後で現在まで飛ぶ。2026-10-14 19:28 JST の形）と古い版に戻る形は遅れが 1 分を超えるので当たらない。新系が止まり続ける形（直後も T と同じ版。2026-10-14 18:52〜19:27 JST に新系が同じ本文を受け続けた 36 分間）は当たらず、`*_new_stale` の上限で `unexplained` になる。1 日に表ごと 10 行までなら説明済み、超えたらその表の行を全部 `unexplained`（2026-10-15 は future 5 行・spot 3 行。毎分遅れる不具合なら 1 日数百行になる）
- `*_legacy_older_unconfirmed`（#89）: `*_legacy_older` の (1)(2)(4) を満たし (3) の裏付けだけが無いもののうち、旧系の次のスナップショットの同じ行が新系の T より先へ進んでいるもの（spot・future は約定時刻が後、option は出来高が多い。旧系の T ≤ 新系の T < 旧系の次）。spot・future は旧系と新系の T の約定時刻が同じで価格だけ違う形（同じ分の中の別の約定。分の精度ではどちらが先か決まらない）も含める（(1) を旧 ≤ 新、(b) を S1 ≤ 旧 ≤ S2 にする）。option は (1) のまま（同じ出来高で値が違う形は入れない）。新系がたまたま 1 回だけ値を読み誤った形と区別できないので、1 日に表ごと 3 行までなら説明済み、超えたらその表の行を全部 `unexplained`。2026-10-14・10-15 の生 HTML ではどれも新系の値が正しかった
- 2026-10-13 09:32〜11:58 JST の実データ（旧系 103 スナップショット）で手元で再現した分類: spot 一致 35・版の差 66・新系の版の遅れ 2、future 一致 34・版の差 67・新系の版の遅れ 2、option 一致 97,107・版の差 227・気配と表示だけの差 13,596、説明できない差分 0
- 2026-10-13 08:52 JST〜10-14 01:08 JST（日中と夜間）を #61 の分類で手元で再現（旧系の 638 スナップショットをすべて比較、`legacy_only` 0）。説明できない行（`unexplained`）は #38（09:30 JST）より前の 21 行（option 19・spot 1・future 1）だけで、#38 より後は 0。#38 より後の説明済みの行のうち #61 の分類によるもの: `option_legacy_older` 2（古い版 1・途中の版 1、新系の直後で裏付け）、`future_legacy_older` 1（13:07 JST、直前と T の間の版、旧系の次で裏付け）、`future_new_stale` 3（新系の T が直前と同じ中身 2・裏付けの無い書き換え途中の版 1（13:44 JST））、`spot_new_stale` 2（新系の T が直前と同じ中身）。21:30 JST 以降（夜間）は説明できない行も #61 の分類による行も無い
- 2026-10-13〜10-15 を #89 の分類で手元で再計算（読み取りだけ。変更前の SQL で `legacy_compare_daily` の記録と同じ件数になることを先に確かめた。10-13・10-14 はコピーが上書き済みなので、旧系の spot・先物は全件、option は約定の列が新系の T・直前・直後のどれとも合わない行とその旧系の次の行だけを固定値にした）: 10-15 は `unexplained` 7 → 1（旧系のみ 1。新系が 04:00 JST 前後に JPX から 502 を受けた取りこぼし）。future の 6 行は `future_new_behind` 5・`future_legacy_older_unconfirmed` 1 に、`spot_new_stale` 3 は `spot_new_behind` 3 に移った。10-14 は 196 → 190（18:54〜19:24 JST の凍結の future 14・option 176 だけ）。future の 5 行は `future_legacy_older` +3（(b) の下の境界）・`future_legacy_older_unconfirmed` 2、option 1 行は `option_legacy_older_unconfirmed` 1。10-13 は 28 → 26（#38 の前の option 26 行は変わらない）。spot・future の 09:07 JST の各 1 行（#38 の前）が `*_legacy_older_unconfirmed` に、`*_new_stale` のうち spot 2・future 2 が `*_new_behind` に移った。ほかの分類の件数は 3 日とも変わらない
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
done   # 東京 3 本・US 1 本（#49 の後は東京に atm-hourly（22:40）が加わり 4 本、#100 の後は atm-cm が加わり 5 本）、owner はすべて sa-sq-dts、schedule は every day 22:15 / 22:10 / 22:30 / 22:02（UTC）（atm-hourly は every day 22:40、atm-cm は every 1 hours（#117。それより前は every day 22:45））
```

`ownerInfo` が出ない場合は `bq show --transfer_config --format=json "$(tc quality-daily $L)" | jq .ownerInfo` で見る。plan に DTS の `params` などの update が出続けるなら、API が返す形に `infra/prod/sq.tf` を合わせる PR を出す。

### 日次の確認

毎朝 07:45 以降（前日 07:00〜当日 07:00 の窓の結果）。件数・要約だけを #11 に記録する（行の全文は貼らない）。

```sh
bq query --location=$L --use_legacy_sql=false --format=pretty '
SELECT window_date, snapshots, violations, rowcount_mismatch, missing_table, dup_keys, months_not_2, atm_not_1,
  price_time_future, price_time_too_old, sunday, body_frozen, orphan_rows
FROM optionchan_tokyo.quality_daily ORDER BY window_date DESC LIMIT 7'
bq query --location=$L --use_legacy_sql=false --format=pretty '
SELECT compare_date, copy_complete, legacy_snapshots, new_snapshots, new_only_snapshots, legacy_only_snapshots,
  spot_version + future_version + option_version AS version_rows, spot_new_stale + future_new_stale AS new_stale_rows,
  option_quote_only, spot_legacy_older + future_legacy_older + option_legacy_older AS legacy_older_rows,
  IFNULL(spot_new_behind + future_new_behind, 0) AS new_behind_rows,
  IFNULL(spot_legacy_older_unconfirmed + future_legacy_older_unconfirmed + option_legacy_older_unconfirmed, 0) AS unconfirmed_rows,
  spot_unexplained, future_unexplained, option_unexplained, unexplained, fail_on_unexplained,
  legacy_condition_candidates, legacy_condition_missed
FROM optionchan_tokyo.legacy_compare_daily ORDER BY compare_date DESC LIMIT 7'
# 実行の状態（最新 3 回）
for n in quality-daily legacy-compare legacy-copy; do
  bq ls --transfer_run --run_attempt=LATEST --max_results=3 --format=json "$(tc $n $L)" | jq -c --arg n "$n" '.[] | {n: $n, runTime, state}'
done
bq ls --transfer_run --run_attempt=LATEST --max_results=3 --format=json "$(tc legacy-stage us)" | jq -c '.[] | {runTime, state}'
```

`new_stale_rows`（`*_new_stale`）と `legacy_older_rows`（`*_legacy_older`）、`new_behind_rows`（`*_new_behind`）と `unconfirmed_rows`（`*_legacy_older_unconfirmed`、#89。列を足す前の日は 0 と出す）は説明済みだが、新系の解析の誤りを吸いうる分類なので件数も見る（2026-10-13 08:52 JST〜10-14 01:08 JST は 5 行・3 行。#89 の 2 つは 2026-10-13〜15 の再計算で 1 日 0〜8 行・1〜3 行）。前の日より急増したら、その日の該当行を新旧で並べて見る（「ERROR の切り替え」）。

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
- `body_frozen`（#88）: JPX が本文の止まったページを返した（「JPX の接続・取り直し・本文の凍結（#88）」の「`body_frozen` を受けたら」）。時間帯は窓の `option_price_latest` を本文の列でスナップショットごとにまとめ、直前と同じものを並べて探す（`created_at` で窓に絞る。1 日 数百 MB）。データは直せないので、その日は違反のまま記録を残す（品質チェックの定義の誤りでなければ再実行しない）。過去の日（2026-10-14 など）を再実行すると違反として記録されるので、#88 より前の日は再実行しない。#88 を戻すとき、`quality_daily` の `body_frozen` 列と品質チェックの SQL は戻さない（テーブルは削除保護つきで列を消せず、SQL だけ戻すと INSERT の列数が合わずに品質チェックが毎日失敗する）。誤検知なら、違反の合計から `body_frozen` を外す PR にする
- 誤検知（品質チェックの定義の誤り）なら、`sql/quality/quality_daily.sql` と `tests/test_sql_bq.py` を直す PR

結果を #11 にコメントする（項目と件数、原因の分類）。

**説明できない差分**: `unexplained_samples`（`kind`・`created_at`・`detail`）から見る。

- `legacy_only`: 旧系にあって新系の完了記録に無いスナップショット（新系の取りこぼし）。その時刻の ingest のログ（`jpx_unavailable`・`skipped_no_update`・`parse_failed` など）と生 HTML の有無を見る
- `spot` / `future` / `option`: 新旧の値の違い。その `created_at`（と直前・直後）の新旧の行を並べて見る。旧系は US なので `bq query --location=US` で旧 `optionchan`、新系は `optionchan_tokyo.*_latest` を同じ時刻で引く（`option_price` は `created_at` で絞る）。新系のパーサーの不具合なら直す PR → 再処理。説明の付く新しい種類なら `sql/legacy/compare.sql` の分類と `tests/test_sql_bq.py` に足す PR（#11 にも定義を追記する）
- その日は #11 の「連続 10 営業日」が途切れる（分類を直して再実行した結果が 0 なら、その日は 0 として数えてよい。直した内容を #11 に記録する）

### 再実行・バックフィル

DTS の定期クエリは作成直後に 1 回ずつ自動で実行され、4 本が同時に走る。2026-10-13 の作成時は、legacy-stage が `staged_window` を作り直している途中に legacy-copy が走り、コピー先の `staged_window` が空になった（メタデータは 1 行、クエリでは 0 行）。legacy-compare が `copy is not for the window ... (staged: null)` で失敗したら、legacy-copy を再実行してから legacy-compare を再実行する。毎朝の実行は 07:02 → 07:10 → 07:30 の順なので起きない。

本番の結果テーブル・作業用データセットへの書き込みなので、実行は所有者が行う。同じ日を再実行しても行は置き換わるだけ（重複しない）。窓の日付 `WD`（日中立会の日付、JST）の分は、`run_time` を `WD` の UTC の 22:02 / 22:15 / 22:30（JST の `WD`+1 日 07:02 / 07:15 / 07:30）にする（`WD` は #12 の作業ディレクトリ `WDIR` と別の名前）。突き合わせは stage → copy → compare の順に、前の実行が `SUCCEEDED` になってから次を実行する。

```sh
WD=2026-10-14   # 窓の日付
bq mk --transfer_run --run_time="${WD}T22:15:00Z" "$(tc quality-daily $L)"
bq mk --transfer_run --run_time="${WD}T22:02:00Z" "$(tc legacy-stage us)"
# stage が SUCCEEDED になってから（コピーは run_time に関係なく今の作業用データセットを運ぶ）
bq mk --transfer_run --run_time="$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$(tc legacy-copy $L)"
# copy が SUCCEEDED になってから
bq mk --transfer_run --run_time="${WD}T22:30:00Z" "$(tc legacy-compare $L)"
bq ls --transfer_run --run_attempt=LATEST --max_results=1 --format=json "$(tc legacy-compare $L)" | jq -c '.[] | {runTime, state}'
```

複数の日をさかのぼるときは 1 日ずつ stage → copy → compare を繰り返す（作業用データセットは 1 日分だけ持つ）。旧系の 1 日分（旧 option 約 80 万行）の stage は約 150MB のスキャン。

### ERROR の切り替え（説明できない差分）

`legacy-compare` は説明できない差分（`unexplained`）が 1 以上なら、記録の後に失敗する（M2 で通知。07:30 の失敗なので、通知の抑制は 08:30 に終わる）。切り替えは `infra/prod/sq.tf` の 2 つの local:

- `legacy_compare_fail_on_unexplained`（true で有効）
- `legacy_compare_fail_from`（窓の日付の下限。これより前の日は、再実行しても記録だけで失敗させない）。`2026-10-14` にしているのは、2026-10-13 の分が #38 の前（09:30 JST まで）の説明できない行（option 19 行）を含むため

記録の `fail_on_unexplained` 列は、この 2 つを合わせた「その実行で ERROR にする設定だったか」（下限より前の日は false）。経緯: 2026-10-13 の記録で分類を確かめ、#61 で旧系の古い版・途中の版（`*_legacy_older`）と新系の書き換え途中の版（`*_new_stale`）を足した（2026-10-13 の結果は「突き合わせの分類」の再現の項）。#89 で新系の 1 分遅れ（`*_new_behind`）と裏付けの無い古い版（`*_legacy_older_unconfirmed`）を足し、`*_legacy_older` の途中の版の下の境界を「S1 と同じ分」まで広げた（2026-10-13〜15 の再計算は同じ項）。

失敗を受けたら「説明できない差分」の手順で原因を調べ、新系の不具合か、分類の漏れ（説明の付く新しい種類）かを決める。分類の漏れなら `sql/legacy/compare.sql` と `tests/test_sql_bq.py` を直す PR を出し、その日を「再実行」して 0 になることを確かめる（直した分類は #11 にコメントする）。

- `option_quote_only` の比率（`option_quote_only / option_compared`）も見る。2026-10-13 の 09:32〜11:58 JST は約 12%、08:52〜21:29 JST 全体は約 8%。気配の列だけを誤る新系のパーサーの不具合はこの分類に吸収されて検出できないので、比率が大きく上がったら気配の列を新旧で並べて見る
- `*_new_stale` と `*_legacy_older` も件数を見る（2026-10-13 08:52 JST〜10-14 01:08 JST は `spot_new_stale` 2・`future_new_stale` 3（うち裏付けの無い書き換え途中の版 1）・`future_legacy_older` 1・`option_legacy_older` 2）。`*_new_stale` のうち、新系の T が直前と同じ中身の行と裏付けの無い書き換え途中の行は、それぞれ 1 日に表ごと 3 行を超えると全部 `unexplained` になる。どちらも新系の T の値が直前と同じか旧系より進んでいる行を説明済みにするので、急増したら新系の解析の誤りがすり抜けていないかを疑い、その日の行を新旧で並べて見る
- `*_new_behind`（1 日に表ごと 10 行まで）と `*_legacy_older_unconfirmed`（3 行まで）も同じ。上限を超えるとその表の分が全部 `unexplained` になる。`*_new_behind` が上限近くまで増えたら新系の取得の遅れ（JPX の返す版の遅れ）が増えていないかを見る（#89）
- `*_legacy_older_unconfirmed` が 1 以上の日は必ず、該当の行（`sql/legacy/compare.sql` の最後の SELECT を行単位に差し替えて `cls = 'legacy_older_unconfirmed'` の行を出す。結果はテーブルに書かない）の T の生 HTML（`ingest_runs.source_object` の raw）で新系の値を確かめ、件数・時刻・新系の値が正しかったかを #11 に記録する（行の全文は貼らない）。新系の値が誤っていたら新系の不具合として扱う（この分類は新系が 1 回だけ値を読み誤った形と区別できない）
- #89 の分類を戻すとき: `legacy_compare_daily` の列（`*_new_behind`・`*_legacy_older_unconfirmed` の 5 列）と `sql/legacy/compare.sql` は revert しない。列は `deletion_protection` で消せず、SQL だけ戻すと MERGE の `INSERT ROW` の列数（SQL 35・表 40）が合わず突き合わせが毎日失敗する。分類に誤りがあれば、列は残したまま該当の分類を外す（その列は 0 を出す）PR にする
- 分類が落ち着くまで止めたいときは `legacy_compare_fail_on_unexplained = false` にする PR（plan は DTS `legacy_compare` の `params` の update 1）

`copy_complete = false`（コピーの欠け）とコピーの窓の不一致は、切り替えに関係なく失敗する（比べた結果が当てにならないため）。

### #11 の判定に使う数

- **連続 10 営業日（AC 5）**: `legacy_snapshots > 0` の日を営業日として、`unexplained = 0`（かつ `copy_complete`）の日が途切れずに 10 日続いたら満たす。「説明できない差分」の定義は行単位（上の「突き合わせの分類」。#11 にコメントで明記）。突き合わせのデプロイ後の日から数える（#11）。行の無い営業日（実行が無かった日）は「再実行」で埋めてから数える
  - **#88 の対策（JPX の接続を要求ごとに張り直す・5xx 等の取り直し・本文の凍結の検知。「JPX の接続・取り直し・本文の凍結（#88）」）のデプロイ後の最初の営業日から数え直す**（下の `WHERE` に `compare_date >= '<デプロイ後の最初の営業日>'` を足す）。それより前の日は、2026-10-14 の本文の凍結（unexplained 190。本物の欠損なので説明済みにしない）と 04:00 JST の 502 の取りこぼし（2026-10-15 の `legacy_only` 1）の原因が残っていた期間として、数に入れない。数え直した後も、`quality_daily` の `body_frozen` が 1 以上の日はその日の `unexplained` の原因として #11 に記録する

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
WDIR=<スクラッチのディレクトリ>    # 結果の CSV・JSON を置く（リポジトリ・Issue に貼らない）
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

- `insert.sql`・`verify.sql` はスクリプト（`DECLARE`・`IF` を含む複数の文）なので、全体に `--dry_run` をかけても最初の文しか検証されない（見積もりのバイト数も最初の文の分だけ）。文ごとの検証・見積もりは `tests/test_legacy_import_bq.py`（DELETE・INSERT を 1 文ずつ dry run）で行う。`fingerprint.sql` は 1 文なので `--dry_run` で全体を見積もれる
- データセットコピーの転送料の基準を、#40 の `legacy-copy`（毎日 約 100MB）の課金で確かめる。課金エクスポート（「予算アラートを受けたとき」の 1. のクエリ）を `service.description = "BigQuery"` で SKU ごとに見て、リージョン間の転送の SKU の量が `optionchan_legacy_stage` の表の物理・論理のどちらに近いかを見る。物理なら転送は 約 4.8GB で 約 55 円、論理なら 約 73GiB で 約 880 円（月の予算 500 円の 50% / 80% の通知が出る。それでも進めるかを決める）。判断（基準・見込み額・進めるか）を #12 にコメントする。論理基準で進めるなら、#12 の AC 9（転送 約 100 円以下）を見込み額に直してから始める

### 1. クエリ量上限を一時的に上げる

全件のスキャンが 5 回（下の見積もり。合計 約 390GiB）あるので、「BigQuery のクエリ量上限」の設定コマンドで `--value=614400`（600GiB）にする。フィンガープリント・検証のやり直し 1 回分（約 75〜165GiB）と、日曜朝の定期実行の分の余裕を持たせる。その日（太平洋時間）の使用量（「BigQuery のクエリ量上限」の「今日のスキャン量」のクエリ。US と東京の合計）が 550GiB を超えたら、次のリセットまで新しい全件スキャンを始めない。作業の後に 6. で戻す。

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

**当日の最初の判断点: データセットコピー（`cross_region_copy`）の完了**。本番の規模（旧系の全表、論理 約 73GiB）のコピーは試していない（#40 の `legacy-copy` は 1 日分 約 100MB）。ここまでで書き込んだのは一時データセット `optionchan_import` だけで（コピーの宛先は `--target_dataset=$IMP`）、`optionchan_tokyo` には何も書いていない。失敗しても `optionchan_tokyo`・旧系・新系の取り込みは無傷。次のどれかなら 3. に進まない:

- 実行の `state` が `FAILED` / `CANCELLED`（理由は `bq show --format=json --transfer_run <実行名>` の `errorStatus` か、Cloud Logging の `resource.type="bigquery_dts_config"`。ERROR は新系の M2 で通知されることがある）
- 数時間たっても `SUCCEEDED` にならない
- 一時データセットに表が揃わない・行数が旧系と合わない

失敗したら: 一時的な原因（内部エラー・クォータ）なら、実行中の実行が無いことを確かめて手で 1 回だけ実行し直す（`overwrite_destination_table` で上書きされる）。続けて失敗する・原因が権限や制限なら、その週末は中止する: DTS を消し（6. の 1 行目）、一時データセットを消し（`bq rm -r -f -d $P:$IMP`）、1. で上げたクエリ量上限を戻し、結果（状態・エラーの要約）を #12 にコメントする。

1 時間足（`atm_*`）もコピーされる。これは 5. の後、6. の後片付けの前に「1 時間足（#49）」の「過去分の移行」で移す（この節の SQL では移さない）。

### 3. コピーの検証

```sh
render fingerprint optionchan | qr "$S0" "$E" --location=US --format=csv --max_rows=100000 \
  --maximum_bytes_billed=85899345920 > $WDIR/fp-us.csv
render fingerprint | qr "$S0" "$E" --location=$L --format=csv --max_rows=100000 \
  --maximum_bytes_billed=85899345920 > $WDIR/fp-import.csv
wc -l $WDIR/fp-us.csv $WDIR/fp-import.csv   # 同じ行数（日 × 3 表 + 見出し。約 6,800）
diff $WDIR/fp-us.csv $WDIR/fp-import.csv && echo MATCH
```

- `MATCH` でなければ、違う日・表を見て、DTS の実行を 1 回やり直し（上書き）、`fp-import.csv` を作り直す。続けて違うなら止める（一時データセットを消して終わり。`optionchan_tokyo` には何も書いていない）
- 東京の 1 回目は 約 73GiB のスキャン（US も同じ）

### 4. 移行

移行前の基準値（並行稼働の範囲だけ。新系の行はここにしか無い）:

```sh
render verify | qr "$C0" "$E" --location=$L --format=json --maximum_bytes_billed=10737418240 > $WDIR/verify-before.json
jq -c '.[-1][0] | {missing_snapshots, overlap_snapshots, new_snapshots, new_rows_spot, new_rows_future, new_rows_option}' $WDIR/verify-before.json
# missing_snapshots = 並行稼働中の新系の取りこぼし（legacy_compare_daily の compare_date が 2026-10-13〜E の前日（金曜）の legacy_only_snapshots の合計と同じ見込み）。overlap 等は 0
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
render insert | qr "$s" "$e" --location=$L --format=json --maximum_bytes_billed=53687091200 > $WDIR/insert-2018.json
jq -c '.[-1][0]' $WDIR/insert-2018.json                      # snapshots > 0
render verify | qr "$s" "$e" --location=$L --format=json --maximum_bytes_billed=21474836480 > $WDIR/verify-2018.json
jq -c '.[-1][0] | del(.mismatch_samples)' $WDIR/verify-2018.json   # 5. の 7 項目が 0、missing_snapshots も 0
render insert | qr "$s" "$e" --location=$L --format=json --maximum_bytes_billed=53687091200 > $WDIR/insert-2018-again.json
jq -c '.[-1][0]' $WDIR/insert-2018-again.json                # snapshots = 0（流し直しても重複しない）
```

どれかが期待と違えば止めて、下の「切り戻し」を 2018 年の範囲で行う。よければ残りの年を流す（1 本の DML で変えられるパーティションは 4,000 まで。旧系は 2,256 だが、やり直しの単位を小さくする）:

```sh
for y in $(seq 2019 ${E:0:4}); do   # E の年まで（E は YYYY-MM-DD で始まる）
  s="$y-01-01 00:00:00+00"; e="$((y + 1))-01-01 00:00:00+00"; [[ "$e" > "$E" ]] && e=$E
  render insert | qr "$s" "$e" --location=$L --format=json --maximum_bytes_billed=53687091200 > $WDIR/insert-$y.json \
    || { echo "failed: $y"; break; }
  echo "$y $(jq -c '.[-1][0] | {snapshots, rows_spot, rows_future, rows_option}' $WDIR/insert-$y.json)"
done
```

- 1 年 約 1,500 万（2018 年。10 月から）〜1.5 億行（2026 年。2026-10 の時点で既に 約 1.5 億行で、E までにさらに増える）（旧 `option_price`）。論理 1 年 最大 14〜15GiB（2026 年）で、`--maximum_bytes_billed`（50GiB）に収まる。1 回 数分の見込み
- 失敗したら、原因（クォータ・権限・`legacy_import: ...` の `ERROR()`）を直して**同じ年から**流し直す。前回の残骸（データの表に書けて完了記録の無い `legacy-` 行）は `insert.sql` の最初に消え、完了記録のある T は選ばれないので重複しない
- `legacy_import: end_at ... is after the last checked window end`: E が品質チェック済みの窓より後。E を直す
- `legacy_import: duplicate ingest_run_id`: 秒未満の `created_at` がある（2026-10 の実測では無い）。その年を止めて調べる

### 5. 移行の検証

```sh
render verify | qr "$S0" "$E" --location=$L --format=json --maximum_bytes_billed=214748364800 > $WDIR/verify-after.json
jq -c '.[-1][0] | del(.mismatch_samples)' $WDIR/verify-after.json
jq -c '.[-1][0].mismatch_samples' $WDIR/verify-after.json
```

- `missing_snapshots`・`overlap_snapshots`・`unknown_legacy_runs`・`bad_run_ids`・`row_mismatches`・`orphan_rows`・`run_count_mismatches` がすべて 0
- `new_rows_*` が移行前（`verify-before.json`）と同じ（新系の行に触れていない）
- `migrated_rows_*` + 並行稼働中に新系の記録があった旧系の行 = 旧系の行数（`fp-import.csv` の `n` の合計）
- 0 でない項目があれば `mismatch_samples`（表・T の先頭 20）から見る。その年だけ戻すなら下の「切り戻し」を年の範囲で行い、4. をその年から流し直す

T0 前後（最新版で見て、日ごとに新旧が重ならず、旧系の日は旧系の件数どおり）:

```sh
# 範囲は C0 の 7 日前〜E（新系の稼働前の数日と並行稼働の全期間）。スキャンは 1 日 約 30MB（created_at・ingest_run_id だけ）なので 10GiB で足りる
q --location=$L --format=pretty --maximum_bytes_billed=10737418240 --parameter="c0:TIMESTAMP:$C0" --parameter="e:TIMESTAMP:$E" '
SELECT DATE(TIMESTAMP_SUB(created_at, INTERVAL 7 HOUR), "Asia/Tokyo") AS d, COUNT(*) AS n,
  COUNTIF(STARTS_WITH(ingest_run_id, "legacy-")) AS legacy, COUNT(DISTINCT created_at) AS snapshots
FROM `optionchan_tokyo.option_price_latest`(TIMESTAMP_SUB(@c0, INTERVAL 7 DAY), @e)
GROUP BY d ORDER BY d'
# 2026-10-12 まで: n = legacy = fp-import.csv のその日の option_price の n。2026-10-13 以降: legacy は取りこぼしの分だけ
# 同じ T の移行の記録が 2 件以上（同じ年を二重に流した）: 0
q --location=$L --format=csv --parameter="s:TIMESTAMP:$S0" --parameter="e:TIMESTAMP:$E" '
SELECT COUNT(*) FROM (
  SELECT snapshot_at FROM optionchan_tokyo.ingest_runs
  WHERE snapshot_at >= @s AND snapshot_at < @e AND trigger = "legacy_import" GROUP BY snapshot_at HAVING COUNT(*) > 1)'
```

C0 の 7 日より前の日は、上の verify で足りる: 移行の記録は T ごとに 1 件（上の検査）で新系の記録と重ならない（`overlap_snapshots = 0`）ので、最新版のテーブル関数は移行の記録の T ごとに `legacy-` の行をすべて返す。その行は T ごとに旧系と同じ（`row_mismatches = 0`）で、旧系のすべての T に記録がある（`missing_snapshots = 0`）。したがって新系の稼働前の日は、最新版の行 = 旧系の行になる。

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

クエリ量上限を `--value=32768 --force` に戻す（「BigQuery のクエリ量上限」。その日のスキャン量（「今日のスキャン量」のクエリ。US と東京の合計）が 32GiB を超えているときは、太平洋時間の 0 時を過ぎてから戻す。超えたまま下げると、その日の残りのクエリ（品質チェック・突き合わせを含む）が失敗する）。

### 切り戻し

移行行だけを消す。完了記録を先に消す（その時点でテーブル関数から見えなくなる）→ データの表の順。年の範囲だけ戻すときは、下の `"$S0" "$E"` をその年の範囲（4. の `"$s" "$e"`）に置き換える（`S0`・`E` は上書きしない。5. の検証で使う）。土曜の午後〜日曜に行う（上の「いつ行うか」）。

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
- **品質チェック（`quality_daily`）**: E 以降の窓だけを見るので、移行行で違反は出ない。過去の日を「再実行・バックフィル」で手で流し直すと、旧系の重複（`dup_keys`）・欠け（`missing_table`）・旧系のページの形の違い（`atm_not_1`・`months_not_2`）・#4 の日付ずれ（`price_time_*`・`sunday`）で違反が出うる（`ingest_run_id` が `legacy-` の行で見分ける）。その結果は誤報として扱う
- **新旧の突き合わせ（`legacy_compare_daily`）**: 新系側から `legacy-` の行を除く（`sql/legacy/compare.sql`）。取りこぼしを補った日を再実行しても `legacy_only` は 0 にならない（新系の取りこぼしの記録として残る）
- **回収・再処理**: 完了記録の `source_object = 'legacy:optionchan'` は生 HTML の名前と一致しないので、回収・`--only-missing` の判定には影響しない（取りこぼしを補った T に生 HTML があれば、再処理すると新系の版が選ばれる）
- **#4（過去データの補正）**: 移行の後に別ステップで行う（「過去データの補正（#4、3b）」）。`price_time_raw` は移行行では NULL で、#4 が元の値を残す。#4 は移行行の `price_time` を書き換えるので、この節の `verify.sql`・`fingerprint.sql` は #4 の後では `row_mismatches` が出る（5. の検証は #4 の前に済ませる）

### E 以降の取りこぼしを補う（#14 で旧系を止めるとき）

E から旧系を止めるまでの新系の取りこぼし（`legacy_compare_daily` の `legacy_only_snapshots` が 1 以上の日）は、旧系を止める前に同じ `insert.sql` で補う。一時データセットの代わりに #40 の `optionchan_legacy_copy`（旧スキーマのまま、1 日分）を使う。行うのは上の「いつ行うか」と同じ土曜の午後〜日曜（新系の書き込みが無く、legacy-stage / legacy-copy の手動実行が毎朝 07:02〜07:30 の定期実行と重ならない）。1 日分なのでクエリ量上限は変えなくてよい:

1. 補う日ごとに `WD` をその日にして、「日次の品質チェックと新旧の突き合わせ（#40）」の「再実行・バックフィル」の手順で legacy-stage → legacy-copy を流す（compare は不要）
2. `render insert optionchan_legacy_copy | qr "<WD の 07:00 JST の UTC>" "<WD+1 の 07:00 JST の UTC>" --location=$L --format=json`、続けて同じ範囲で `render verify optionchan_legacy_copy` を流し、5. と同じ項目が 0 であることを見る
3. 作業用データセットは翌朝の定期実行で前日分に戻る
4. 補った日の 1 時間足を「1 時間足（#49）」の「再実行・バックフィル」で作り直す

新系の取り込みを止めていた期間（「停止・再開」）の分も同じ手順で補える。

## 過去データの補正（#4、3b）

移行した旧系の行（`ingest_run_id` が `legacy-` で始まる行、#12）の `price_time` の**日付**を推定で直す（target.md 6 章 3b・D6）。**日付が一意に決まる行だけを直し、一意に決まらない行は元のまま残す**。**#12 の 5.（移行の検証）が合格した後に、所有者が 1 回だけ行う**。SQL は `sql/price_time_fix/`（試験は `tests/test_price_time_fix.py`・`tests/test_price_time_fix_bq.py`）。クエリ量上限の一時変更・テーブルスナップショットは Terraform 外で、この節の手順で作って戻す。

| SQL | 内容 |
|---|---|
| `accuracy.sql` | 推定の一致率。新系の最新版（正しい `price_time`）を旧系の補完で旧系の保存値に戻し、`fix.sql` と同じ推定で戻した値と比べる（読み取りだけ） |
| `fix.sql` | 推定と書き込み。`@mode = 'dryrun'` は表 × 確かさ × 候補のずれの内訳を数えるだけ、`'apply'` は一意に決まる未補正の行を UPDATE する。流し直しても結果は同じ（2 回目は 0 行） |
| `verify.sql` | 補正の検証（推定を使わない独立の検査。表ごとに 1 行） |
| `rollback.sql` | 切り戻し。補正した行を `price_time_raw` の元の値に戻す |

**ずれの仕組み**: 旧系（`functions/jpx_loader.py`、2019 年〜2026-10-12）はページの約定日時 `MM/DD HH:MM` の `MM/DD` を捨て、ページの更新時刻 C（JST）の日付に時刻を付け、C より未来なら 1 日戻していた。保存値 B は「C 以前で最も新しい、その時刻の日時」で、正しい値 A は B と同じ時刻・B 以前の日付（A = B − k 日）。k ≥ 1 になるのは、前日より前の約定（週末・休業日・夜間の前日分を跨いで表示が残った約定、値の付かない行使価格の古い約定）と、表示が C より数分先の約定（k = −1。セッションの終わりの約定値など）。#2 の修正（`FIX`）以降の旧系の行と、新系の行は正しい。

**推定**（`fix.sql` の先頭のコメントに詳細）: 系列（option は限月・種別・行使価格、future は限月、spot は 1 つ）ごとに時刻順に並べ、表示（時刻・値段）が変わった行・表示が消えた後の行・取引高が増えた行を「約定の始まり」とし、始まりの行で「C + 10 分以下で立会中の、その時刻の最も新しい日時」を候補とする。前回の表示より後に同じ時刻の別の候補が無ければ日付は一意に決まり（`determined`）、同じ約定が続く行もその日付に直す（何日も表示が残った約定も始まりの日付で直る）。確かさ（`basis`）と扱い:

| basis | 意味 | 扱い |
|---|---|---|
| `determined` | 前の行（前回の表示）より後に、同じ時刻の別の候補が無い | 直す（`price_time_raw` に元の値、`parser_version = 'legacy-fix4-1'`） |
| `gap` | 前の行との間が空いていて（取り込みの停止、行使価格がページから外れていた間）、もう 1 日前の候補もありうる | 元のまま（印も付けない） |
| `series_start` | 系列の最初（範囲の前 100 日の文脈の中で） | 元のまま |
| `no_session` | 立会中の候補が無い（立会の外の時刻の表示） | 元のまま |
| `off_session` | 立会の外のスナップショット（カレンダー上の休業日の取得。2019-01-01〜03 に 1 日 1 件など）で、表示が取得の直前の約定。2023〜2025 年の 1/3 は祝日取引の実施日（#67 のカレンダーで `is_trading_day = true`）なので、その日の取得は立会中として `determined` で直る | 元のまま（根拠が無い） |

元のままの行は、約定の始まりの行だけでなく、同じ約定が続く行も元のまま（始まりの日付に揃えない）。件数は `fix.sql` の内訳の `rows_kept` に basis ごとに出るので、#4 に記録する。

**一致率（2026-10-14 の実測。AC は `determined` 99.9% 以上・全体 99% 以上）**: 旧系の #2 の修正後の行（2026-10-12 12:25〜10-14 09:00 JST、正しい値）を旧ロジックで B に戻し、修正前の旧系の行（2026-09-05〜）を文脈にして推定すると、option は `determined` 257,042 行がすべて正しく、全体 257,751 行のうち 257,263 行（99.8%。外れは元のままにした `series_start` 488 行）。future 1,349 行・spot 1,338 行はすべて `determined` で正しい（補正しない場合の一致は option 81.2%・spot 58.7%）。新系（`accuracy.sql`、2026-10-13 15:00〜）は `determined` 107,596 行がすべて正しく、全体は 111,419 行中 108,659 行（97.5%）。外れた 2,760 行はすべて新系の稼働開始の直後で前の表示が無い `series_start`（元のまま）で、事前確認 3. では最初の 1 日を文脈にして除く。過去の標本（旧系の保存値に推定を当てた内訳。正解は無い）: 2019-04-15〜05-20（10 連休を含む）は option 約 176 万行のうち `gap` 0.5%・`series_start` 0.1%（10 連休を跨いで直す行 約 7,900）、2024-12-20〜2025-01-15（年末年始）は約 174 万行のうち `gap` 0.3%・`series_start` 0.2%・`off_session` 7.0%（2025-01-03・04 の取得の 約 12 万行。過去年のカレンダーを入れる前の測定で、1/3 を休業日として数えていた。実行時（2026-10-16）の全期間の `off_session` は option 547 行・future 9 行）。

**いつ行うか**: **#12 の移行の次の週末**の**土曜 13:00 JST 以降〜日曜**（新旧とも書き込みが無い。UPDATE が新系の書き込み・定期クエリと重ならない）。**#67（取引カレンダーの過去年）を先に埋めておく**（事前確認 2.）。#12 の 5.（移行の検証）は #4 の前に済んでいること（2026-10-14 オーナー判断: 方式は in-place の UPDATE、元の値は `price_time_raw`、印は `parser_version`、一意に決まらない行と休業日の取得の行は元のまま、時期は #12 の次の週末・#67 の後、クエリ量上限 500GiB）。

### 変数

「Terraform」の「共通の変数」の `P`、「新しい取り込み（ingest）」の `L`、「過去データの移行（#12）」の `IMP`・`E`・`S0`・`C0`・`W`・`q` に加えて:

```sh
FIX='2026-10-12 03:25:00+00'   # #2 の修正後の旧系の最初のスナップショット（事前確認 1. で確かめる）
render4() { sed -e "s/[$]{project}/$P/g" -e "s/[$]{dataset}/optionchan_tokyo/g" "sql/price_time_fix/$1.sql"; }
# fix.sql: 範囲 [$1, $2)、モード $3（dryrun / apply）
fx() { local s=$1 e=$2 m=$3; shift 3; render4 fix | q --location=$L --format=json \
  --parameter="start_at:TIMESTAMP:$s" --parameter="end_at:TIMESTAMP:$e" --parameter="fixed_at:TIMESTAMP:$FIX" \
  --parameter="mode:STRING:$m" --parameter="require_calendar:BOOL:true" "$@"; }
vf() { local s=$1 e=$2; shift 2; render4 verify | q --location=$L --format=json \
  --parameter="start_at:TIMESTAMP:$s" --parameter="end_at:TIMESTAMP:$e" --parameter="fixed_at:TIMESTAMP:$FIX" "$@"; }
# fix.sql の結果（.[-1]）の列の合計
sum4() { jq "[.[-1][] | .$1 | tonumber] | add // 0" "$2"; }
```

`S0`（#12 の範囲の始まり）は `2018-10-01 00:00:00+00` のまま使う（旧系の最初のパーティションは 2018-10-19）。

### 事前確認

1. `FIX`（旧系の spot の `price_time` が日曜 → 金曜の日付に変わる境目。03:24 UTC まで `2026-10-11 15:45`、03:25 から `2026-10-09 15:45`）:

   ```sh
   q --location=US --format=pretty '
   SELECT created_at, price_time FROM optionchan.spot_price
   WHERE created_at BETWEEN "2026-10-12 03:15:00" AND "2026-10-12 03:35:00" ORDER BY created_at'
   ```

2. 取引カレンダーが十分前の年からあること（#67）。`fix.sql` は範囲の始まりの 131 日前（文脈の 100 日 + 候補の 31 日）を含む年の 1/1 から、範囲の終わりの年まで、カレンダーの版が始まっていることと、各年に年始の休業日（1/1〜1/3 の平日。版の行は平日だけなので 1/1 が土日の年は 1/2・1/3 の行）があること（年が欠けた版でないこと）を確かめ、満たさなければ `trading calendar ...` で止まる。最初の範囲は `S0`（2018-10-01）から流すので、必要なのは 2018-01-01 から（#67 は 2018-01-01 から埋めるので足りる）:

   ```sh
   q --location=$L --format=csv '
   SELECT MIN(calendar_date),
     COUNT(DISTINCT IF(EXTRACT(MONTH FROM calendar_date) = 1 AND EXTRACT(DAY FROM calendar_date) <= 3, EXTRACT(YEAR FROM calendar_date), NULL)) AS years
   FROM optionchan_tokyo.trading_calendar_latest'   # MIN が 2018-01-01 以前、years が 2018〜2027 年の 10 以上
   ```

   無ければ始めない（#67 を先に済ませる）。`require_calendar = false`（休業日を土日だけで判定）は使わない。

3. 一致率（新系の並行稼働のデータ。最初の 1 日は文脈として除く）:

   ```sh
   # start_at は新系の稼働（C0）から 1 日余り後（それより前の行は前の表示が無く series_start になるので、文脈にだけ使う）
   render4 accuracy | q --location=$L --format=json --maximum_bytes_billed=10737418240 \
     --parameter="ctx_start:TIMESTAMP:$C0" --parameter="start_at:TIMESTAMP:2026-10-14 06:00:00+00" \
     --parameter="end_at:TIMESTAMP:$E" > $W/fix4-accuracy.json
   jq -c '.[-1][]' $W/fix4-accuracy.json
   jq '[.[-1][] | select(.basis == "determined")] | {n: (map(.n | tonumber) | add), ok: (map(.ok | tonumber) | add)}' $W/fix4-accuracy.json
   jq '[.[-1][]] | {n: (map(.n | tonumber) | add), ok: (map(.ok | tonumber) | add)}' $W/fix4-accuracy.json
   ```

   #4 の AC（`determined` の一致率 99.9% 以上、全体 99% 以上）を満たさなければ止める。

4. 補正前の基準値と内訳（全期間。書き込まない）:

   ```sh
   vf "$S0" "$FIX" --maximum_bytes_billed=53687091200 > $W/fix4-verify-before.json
   jq -c '.[-1][] | {tbl, legacy_rows, unmarked_rows, out_of_scope_rows, new_rows}' $W/fix4-verify-before.json
   # legacy_rows = unmarked_rows（未補正）、out_of_scope_rows = 0
   fx "$S0" "$FIX" dryrun --maximum_bytes_billed=107374182400 > $W/fix4-dryrun.json
   jq -c '.[-1][] | {tbl, basis, candidate_shift_days, rows_total, rows_pending, rows_kept}' $W/fix4-dryrun.json
   ```

   - `rows_pending`（直す行）+ `rows_kept`（元のままの行）の合計 = `verify` の `unmarked_rows` の合計
   - `basis` ごとの `rows_kept`、`rows_pending_changed`（日付が変わる行）を #4 に記録する（件数だけ）。`gap`・`series_start`・`off_session`・`no_session` の割合が上の標本（合わせて数 %）から大きく外れたら止めて調べる

5. 書き込む前に東京の 3 表のテーブルスナップショットを取る（期限 14 日。UPDATE で書き換えたパーティションの分だけ保存料がかかる。物理 約 5GB）。#12 の一時データセット `$IMP` が残っていればそこに、無ければ同じコマンドで作る:

   ```sh
   bq show $P:$IMP >/dev/null 2>&1 || bq mk --dataset --location=$L --storage_billing_model=PHYSICAL --max_time_travel_hours=48 \
     --default_table_expiration=1209600 --description='#12・#4 の一時データセット（期限で消える）' $P:$IMP
   for t in spot_price future_price option_price; do
     bq cp --snapshot --no_clobber --expiration=1209600 $P:optionchan_tokyo.$t $P:$IMP.fix4_$t
   done
   ```

### 1. クエリ量上限を一時的に上げる

全期間で 約 400GiB（下の見積もり）。1 年分の `apply` でも `option_price` の書き換え（その年の全列）と推定の読み込みで 20〜35GiB になり、32GiB/日を超える年がある。「BigQuery のクエリ量上限」の設定コマンドで `--value=512000`（500GiB）にする。その日の使用量（「BigQuery のクエリ量上限」の「今日のスキャン量」のクエリ。`region-us` と `region-asia-northeast1` の両方）が上限の 9 割を超えたら、次のリセットまで新しい年を始めない。作業の後に 4. で戻す。

### 2. 補正

年ごとに「dryrun（書く予定の行数）→ apply（書いた行数）→ 突き合わせ」の順に流す。最初の年は `S0`（2018-10-01）から始める（2018-01-01 から始めると、文脈とカレンダーの検査が 2017 年に掛かって止まる）。2018 年を流したら、同じ範囲で `dryrun` と `verify` をもう一度流して確かめる:

```sh
y=2018; s=$S0; e='2019-01-01 00:00:00+00'
fx "$s" "$e" dryrun --maximum_bytes_billed=21474836480 > $W/fix4-plan-$y.json
fx "$s" "$e" apply --maximum_bytes_billed=53687091200 > $W/fix4-apply-$y.json
echo "plan=$(sum4 rows_pending $W/fix4-plan-$y.json) applied=$(jq '.[-1][0] | [.updated_spot, .updated_future, .updated_option | tonumber] | add' $W/fix4-apply-$y.json)"   # 同じ
fx "$s" "$e" dryrun --maximum_bytes_billed=21474836480 > $W/fix4-again-$y.json
echo "pending=$(sum4 rows_pending $W/fix4-again-$y.json) mismatch=$(sum4 rows_mismatch $W/fix4-again-$y.json)"   # 0 0
vf "$s" "$e" --maximum_bytes_billed=21474836480 | jq -c '.[-1][]'   # 下の 3. の 0 になる項目が 0
```

よければ残りの年を流す（1 本の DML で変えられるパーティションは 4,000 まで。1 年は 約 300）:

```sh
for y in $(seq 2019 2026); do
  s="$y-01-01 00:00:00+00"; e="$((y + 1))-01-01 00:00:00+00"; [[ "$e" > "$FIX" ]] && e=$FIX
  fx "$s" "$e" dryrun --maximum_bytes_billed=53687091200 > $W/fix4-plan-$y.json || { echo "failed plan: $y"; break; }
  fx "$s" "$e" apply --maximum_bytes_billed=107374182400 > $W/fix4-apply-$y.json || { echo "failed apply: $y"; break; }
  plan=$(sum4 rows_pending $W/fix4-plan-$y.json)
  applied=$(jq '.[-1][0] | [.updated_spot, .updated_future, .updated_option | tonumber] | add' $W/fix4-apply-$y.json)
  echo "$y plan=$plan applied=$applied"
  [ "$plan" = "$applied" ] || { echo "mismatch: $y"; break; }
done
```

- 1 年 数分〜十数分の見込み（未計測。推定はウィンドウ関数と一時関数で、スキャン量より計算が重い）
- 失敗したら原因（クォータ・`price_time_fix: ...` の `ERROR()`）を直して**同じ年から**流し直す。3 表の UPDATE はそれぞれ 1 文で全部書くか何も書かないかなので、途中の表から再開しても重複・二重のずらしは起きない（推定は `price_time_raw` の元の値から計算し、一意に決まる未補正の行だけを書く）。流し直した年は `plan` が残りの行数になり、`applied` と同じになる
- `price_time_fix: N rows corrected by another version`: 別の版（`legacy-fix4-*`）の補正が残っている。`rollback.sql` で戻してから流す
- 年の境目を跨ぐ約定は、範囲の前 100 日を文脈として読むので、前の年と同じ値になる（`dryrun` の `rows_mismatch` で確かめられる）

### 3. 補正の検証

```sh
vf "$S0" "$FIX" --maximum_bytes_billed=53687091200 > $W/fix4-verify-after.json
jq -c '.[-1][] | del(.legacy_rows)' $W/fix4-verify-after.json
```

- 0 になる項目: `out_of_scope_rows`・`bad_raw_rows`・`future_rows`・`sunday_changed_rows`・`off_calendar_rows`（動かした行の新しい日時がカレンダー上で立会中）・`spot_closed_rows`（補正した spot に東証の休業日・土日の日付が無い）・`shift_out_of_range_rows`（ずらした日数が −1〜31 日）
- `unmarked_rows`（元のままの行）の合計 = 4. の内訳の `rows_kept` の合計
- `new_rows` が補正前（`fix4-verify-before.json`）と同じ（新系の行に触れていない）
- `shifted_rows` の合計 = 4. の `rows_pending_changed` の合計
- `sunday_kept_rows` は元のままの行のうち日曜の分
- 全期間の `dryrun` をもう一度流すなら `rows_pending`・`rows_mismatch` の合計が 0（約 50GiB。年ごとに済ませていれば省いてよい）

結果は件数・要約だけを #4 にコメントする（`verify` の 0 の項目、`shifted_rows`、basis ごとの `rows_kept`、一致率、スキャン量）。JSON の全文は貼らない。

### 4. 後片付け

クエリ量上限を `--value=32768 --force` に戻す（「BigQuery のクエリ量上限」。その日のスキャン量が 32GiB を超えているときは太平洋時間の 0 時を過ぎてから）。テーブルスナップショット `fix4_*` は期限（14 日）で消える。様子を見て問題が無ければ、#12 の 6. と同じく `bq rm -r -f -d $P:$IMP` で早めに消してよい。

### 切り戻し

`price_time_raw` に元の値（`'%F %T'`）を残しているので、いつでも補正の前の値に正確に戻せる（年の範囲だけ戻すときは範囲をその年にする）。全期間を戻すと全列の書き換えで 約 120GiB のスキャンになり 32GiB/日を超えるので、先に「1. クエリ量上限を一時的に上げる」と同じく上限を上げ（年の範囲だけなら 1 年 20〜35GiB）、終わったら戻す。同じ土曜午後〜日曜に行う:

```sh
render4 rollback | q --location=$L --format=json --maximum_bytes_billed=214748364800 \
  --parameter="start_at:TIMESTAMP:$S0" --parameter="end_at:TIMESTAMP:$FIX" | jq -c '.[-1][0]'
vf "$S0" "$FIX" --maximum_bytes_billed=53687091200 | jq -c '.[-1][] | {tbl, legacy_rows, unmarked_rows, shifted_rows}'
# unmarked_rows = legacy_rows、shifted_rows = 0（移行の直後の状態）
```

- `rollback.sql` は補正の印（`parser_version = 'legacy-fix4-1'`）のある旧系の行だけを戻し、`price_time_raw`・`parser_version` を NULL に戻す
- 最後の手段: 2. の前のテーブルスナップショット（14 日）・タイムトラベル（7 日）。スナップショットからの復元（`bq cp --restore`）は補正の後に新系が書いた行も消すので、#12 の「切り戻し」と同じく新系を止めて再処理で補う
- 推定の規則を変えてやり直すときは、`rollback.sql` で戻してから `fix.sql` の版（`fix_version`、4 つの SQL で同じ値）を上げて流す（別の版の行が残っていると `fix.sql` は止まる）

### 所要時間・コストの見積もり

`option_price` の旧系の行は 約 6.5 億行・論理 約 73GiB（旧系の US と同じ）+ 東京で足した `ingest_run_id`（約 15GiB）。補正で `price_time_raw`・`parser_version` が 最大 約 1.6 億行（`price_time` のある行）に入り 約 5GiB 増える。

| 項目 | 見込み |
|---|---|
| 事前確認 | 一致率 数 GiB 以下、`verify` 約 30GiB、`dryrun` 約 45GiB |
| 補正（年ごと 9 回の合計） | `dryrun` 約 55GiB（7 列 + `ingest_run_id` + 範囲の前 100 日）+ `apply` 約 190GiB（推定の読み込み 約 55GiB + UPDATE 約 130GiB。DML はその年のパーティションの全列 約 88GiB + 参照する列 + 推定の一時テーブル） |
| 2018 年の確かめ（`dryrun`・`verify`） | 数 GiB |
| 補正の検証 | `verify` 約 30GiB（全期間の `dryrun` もするなら +約 50GiB） |
| 合計 | 約 400GiB（月 1TiB の無料枠（他プロジェクトと共有）に収まれば 0 円、超えた分は on-demand の単価（US は $6.25/TiB。東京の単価は実行前に料金表で確かめる）で 約 2〜3 ドル） |
| 保存 | UPDATE で書き換えたパーティションの古い版がタイムトラベル（7 日）+ フェイルセーフ（7 日）の間 物理 約 5GB、スナップショット（14 日）も同程度。数円。補正後の表は 約 5GiB（論理）増える |
| 所要時間 | 未計測。事前確認 30 分、補正 1〜2 時間、検証 30 分程度の見込み。土曜の午後〜日曜で終わる |

実行後に `INFORMATION_SCHEMA.JOBS_BY_PROJECT`（`region-asia-northeast1`）でスキャン量の実績を見て #4 に記録する。

### 他の仕組みとの関係

- **#12 の検証**: `sql/legacy_import/verify.sql`・`fingerprint.sql` は旧系の `price_time` も比べるので、#4 の後は `row_mismatches` が出る。#12 の 5. は #4 の前に済ませる
- **品質チェック（`quality_daily`）**: 定期の窓は #2 の修正後なので影響なし。過去の日を手で再実行したときの `sunday`・`price_time_*` の違反は、#4 の後は元のままの行（`determined` 以外）だけになる
- **新旧の突き合わせ・1 時間足**: `legacy_compare_daily` は `legacy-` の行を除き、1 時間足（#49）は `price_time` を使わないので影響なし
- **#14 で補う E 以降の取りこぼし**: #2 の修正後の旧系の行なので補正しない（`fix.sql` は `FIX` より前だけ）
- **ビューアー**: スマイルカーブの取引時刻（`price_time`）が直った日付で出る（元のままの行は従来どおり）

## DataCube の歩み値での穴埋め（#111）

#110 の欠け 2（**2023-04-25 13:42 〜 2023-05-08 16:31 JST**。旧系がオプションの価格の表示を解析できず、`option_price` の `price`・`price_time` がすべて NULL の期間）を、J-Quants DataCube の「日経225オプション 歩み値（ティック）」で埋める。歩み値は表 `optionchan_tokyo.dc_option_tick`（Terraform の `infra/prod/datacube.tf`、列の正は `schema/tokyo/dc_option_tick.json`）に、購入済みの全月（2018-12〜2024-08 の 26 か月）を入れる（欠け 1・3 は別 Issue）。SQL は `sql/tick_fill/`（試験は `tests/test_tick_fill.py`・`tests/test_tick_fill_bq.py`）。CSV のアップロード・`bq load`・UPDATE・テーブルスナップショットは Terraform 外で、所有者がこの節の手順で行う。

**規約（DataCube の自己使用）**: 私的な目的だけ。第三者への提供・公開はしない（加工した値も同じ扱い）。CSV・表は非公開のバケット・データセットだけに置き、**リポジトリ・Issue・PR にデータ・行の抜粋・値を貼らない**（件数・割合だけ）。ビューアーは IAP の内側でオーナーだけが見る。

| SQL | 内容 |
|---|---|
| `accuracy.sql` | 規則の一致率。正解の期間（旧系が `price`・`price_time` を持つ期間）の旧系の `volume` がある行に `fill.sql` と同じ推定を当て、保存値と比べる（読み取りだけ） |
| `fill.sql` | 推定と書き込み。`@mode = 'dryrun'` は確かさ（`basis`）ごとの内訳を数えるだけ、`'apply'` は決まる未補完の行を UPDATE する。流し直しても結果は同じ（2 回目は 0 行）。範囲は欠け 2 の中だけ（外ならエラー） |
| `verify.sql` | 補完の検証（推定を使わない独立の検査。1 行） |
| `rollback.sql` | 切り戻し。補完した行（印のある行）を NULL に戻す |

**規則**（`fill.sql` の先頭のコメントに詳細）: ページの取引高（`volume`）は、その取引日の約定の累計の取引高と一致する（正解の期間で 100%）。スナップショットの取引日（16:30 以降は翌日から数えて最初の現物の営業日。祝日取引の日は取引日にならない。DataCube の `trade_date` と同じ）の約定を時刻順に累計し、累計 = `volume` になった約定の値段を `price`、約定日時（JST、分に切り捨て）を `price_time` に入れる。祝日取引の日の夜間のスナップショットは、祝日の日中の約定を累計から除く（ページの取引高が数えていない）。印は `parser_version = 'legacy-fill111-1'`。他の列（`volume`・`diff`・`iv`・`price_time_raw` など）は変えない。

| basis | 意味 | 扱い |
|---|---|---|
| `matched` | 累計の合う約定がある | 書く |
| `ambiguous_holiday` | 祝日取引の夜間で、照合した約定より後・スナップショット + 60 秒より前に祝日の日中の約定がある（ページの約定値がどちらか決まらない） | NULL のまま（2026-10-17 オーナー判断） |
| `late_trade` | 照合した約定がスナップショット + 10 分より後 | NULL のまま（安全弁。見込み 0） |
| `no_match` | 累計の合う約定が無い（値段が整数でない場合を含む） | NULL のまま（見込み 0） |
| `no_tick_day` | 取引日の歩み値が無い（月の入れ忘れ） | `fill.sql` がエラーで止まる |

`volume` が NULL の行（ページの表示が空）は対象にしない。

**見込み（2026-10-17 の調査。件数だけ）**: 欠け 2 の旧系 2,462,430 行のうち `volume` がある行（対象）1,184,423 行。書く 約 113 万行（`matched`、約 95.5%）、残す 約 5.3 万行（すべて GW（2023-05-03〜05）の祝日取引の夜間の `ambiguous_holiday`）、`no_match` 0。書く行のうち前日比（`diff`）がある行で「約定値 − 前日比」（前日の清算値）が系列・取引日ごとに一定の割合 100%。正解の期間（2023-05-08 16:31〜05-31）では値段 100%・時刻（時:分）100%・#4 の印のある行の日付込み 100% で一致。

**いつ行うか**: いつでもよい（UPDATE が触るのは 2023-04-25〜05-08 のパーティションだけで、新系の書き込み・定期クエリと重ならない）。前提: Terraform の prod apply（この PR のマージ後の deploy.yml）で `dc_option_tick` ができていること、#4 が済んでいること（済み）、取引カレンダーに 2023 年があること（済み）。

### 変数

リポジトリのルートで、「Terraform」の「共通の変数」の `P`、「新しい取り込み（ingest）」の `L` に加えて:

```sh
P=$(gcloud config get-value project 2>/dev/null); echo "$P"   # 自分の GCP プロジェクト ID か確かめる
L=asia-northeast1
IMP=optionchan_import                      # テーブルスナップショットを置く一時データセット（#12・#4 と同じ）
B=gs://$P-raw/datacube/option_tick        # CSV の置き場所（非公開のバケット。新系の取り込みは raw/ だけを読む）
ZIPS=<zip のディレクトリ>                  # option_tick_20_YYYYMM.zip（26 個）のあるディレクトリ
W=<スクラッチのディレクトリ>               # 展開した CSV・結果の JSON を置く（リポジトリの外。Issue に貼らない）
GS='2023-04-25 04:42:00+00'; GE='2023-05-08 07:31:00+00'   # 欠け 2（fill.sql の gap_start・gap_end と同じ）
AS='2023-05-08 07:31:00+00'; AE='2023-05-31 07:30:00+00'   # 正解の期間（取引日が 05-31 までに収まる）
q() { bq query --project_id=$P --use_legacy_sql=false --nouse_cache "$@"; }
render111() { sed -e "s/[$]{project}/$P/g" -e "s/[$]{dataset}/optionchan_tokyo/g" "sql/tick_fill/$1.sql"; }
# fill.sql: 欠け 2 の全体、モード $1（dryrun / apply）
fl() { local m=$1; shift; render111 fill | q --location=$L --format=json \
  --parameter="start_at:TIMESTAMP:$GS" --parameter="end_at:TIMESTAMP:$GE" --parameter="mode:STRING:$m" "$@"; }
vf111() { render111 verify | q --location=$L --format=json \
  --parameter="start_at:TIMESTAMP:$GS" --parameter="end_at:TIMESTAMP:$GE" "$@"; }
# fill.sql の結果（.[-1]）の列の合計
sum111() { jq "[.[-1][] | .$1 | tonumber] | add // 0" "$2"; }
```

### 1. CSV の展開とアップロード

zip 1 つに CSV が 1 つ（`option_tick_20_YYYYMM.csv`）。展開した CSV を置く（zip は手元に残す。合計 約 1GB）:

```sh
bq show --format=prettyjson $P:optionchan_tokyo.dc_option_tick | jq '{timePartitioning, clustering, requirePartitionFilter}'
# timePartitioning が {"type":"MONTH","field":"trade_date"}。無ければ Terraform の apply を先に済ませる
mkdir -p $W/dc
for z in $ZIPS/option_tick_20_*.zip; do
  ym=$(basename "$z" .zip); ym=${ym#option_tick_20_}
  unzip -p "$z" "option_tick_20_$ym.csv" > "$W/dc/option_tick_20_$ym.csv" || { echo "failed: $z"; break; }
done
ls $W/dc/option_tick_20_*.csv | wc -l   # 26
for f in $W/dc/option_tick_20_*.csv; do head -1 "$f" | awk -F, '{print NF}'; done | sort | uniq -c   # すべて 13
gcloud storage cp --no-clobber $W/dc/option_tick_20_*.csv $B/
gcloud storage ls $B/ | grep -c '\.csv$'   # 26
```

### 2. 歩み値の取り込み（月ごと）

1 ファイル = 1 取引月 = 1 パーティション。パーティションのデコレータ（`$YYYYMM`）と `--replace` で**その月だけを置き換える**（同じ月を入れ直しても重複しない。月の外の行があれば load が失敗する）。列名は版で違う（2022-07 以前は大文字・`Make_Date`、2022-08 は `make_date`、2022-09 以降は `execution_date`）が、並びは同じなので、見出しの行を飛ばして明示のスキーマで入れる。まず 1 か月だけ入れて確かめる:

```sh
ld() { local ym=$1; bq load --project_id=$P --location=$L --source_format=CSV --skip_leading_rows=1 \
  --date_format=YYYYMMDD --time_format=HH24MISSFF3 --replace \
  "$P:optionchan_tokyo.dc_option_tick\$$ym" "$B/option_tick_20_$ym.csv" schema/tokyo/dc_option_tick.json; }
ld 202304
q --location=$L --format=csv '
SELECT COUNT(*) AS n, MIN(trade_date), MAX(trade_date), MIN(execution_time), MAX(execution_time)
FROM optionchan_tokyo.dc_option_tick WHERE trade_date BETWEEN "2023-04-01" AND "2023-04-30"'
awk 'END { print NR - 1 }' $W/dc/option_tick_20_202304.csv   # 上の n と同じ
```

よければ全月を入れる（流し直しても同じ結果）:

```sh
for f in $W/dc/option_tick_20_*.csv; do
  ym=$(basename "$f" .csv); ym=${ym#option_tick_20_}
  ld "$ym" || { echo "failed: $ym"; break; }
done
```

確認（26 か月。スキャン 約 0.3GB）: 月ごとの行数が CSV のデータ行数と同じで、（取引日・銘柄コード・シーケンス No）の重複が 0:

```sh
for f in $W/dc/option_tick_20_*.csv; do ym=$(basename "$f" .csv); echo "${ym#option_tick_20_},$(awk 'END { print NR - 1 }' "$f")"; done | sort > $W/dc-csv-rows.csv
q --location=$L --format=csv --maximum_bytes_billed=2147483648 '
SELECT FORMAT_DATE("%Y%m", trade_date) AS ym, COUNT(*) AS n
FROM optionchan_tokyo.dc_option_tick WHERE trade_date >= "2018-01-01"
GROUP BY ym ORDER BY ym' | tail -n +2 | sort > $W/dc-bq-rows.csv
diff $W/dc-csv-rows.csv $W/dc-bq-rows.csv && echo "rows ok"   # 26 行とも同じ
q --location=$L --format=csv --maximum_bytes_billed=2147483648 '
SELECT COUNT(*) AS duplicated FROM (
  SELECT trade_date, security_code, seq_no FROM optionchan_tokyo.dc_option_tick WHERE trade_date >= "2018-01-01"
  GROUP BY 1, 2, 3 HAVING COUNT(*) > 1)'   # 0
```

`--date_format`・`--time_format` で失敗するとき（bq の版が古い等）の代わり: 全列 STRING の一時表に入れてから、トランザクションの中でその月を消して型を付けて入れ直す（月ごと。`ym` を変えて流す。表の列は REQUIRED なので、クエリの結果をパーティションへ書く `--destination_table` は使わない）。一時表は `$IMP` に置く（無ければ「3. 事前確認」のスナップショットの前の `bq mk` で先に作る）:

```sh
ym=202304; d0="${ym:0:4}-${ym:4:2}-01"
bq load --project_id=$P --location=$L --source_format=CSV --skip_leading_rows=1 --replace $P:$IMP.dc_tick_stage "$B/option_tick_20_$ym.csv" \
  trade_date:STRING,execution_date:STRING,index_type:STRING,put_call_type:STRING,security_code:STRING,execution_time:STRING,trade_price:STRING,price_type:STRING,trade_volume:STRING,seq_no:STRING,contract_month:STRING,exercise_price:STRING,cash_futures_type:STRING
q --location=$L '
BEGIN TRANSACTION;
DELETE FROM optionchan_tokyo.dc_option_tick
WHERE trade_date >= DATE "'"$d0"'" AND trade_date < DATE_ADD(DATE "'"$d0"'", INTERVAL 1 MONTH);
INSERT INTO optionchan_tokyo.dc_option_tick
SELECT CAST(trade_date AS DATE FORMAT "YYYYMMDD"), CAST(execution_date AS DATE FORMAT "YYYYMMDD"),
  index_type, CAST(put_call_type AS INT64), security_code,
  CAST(execution_time AS TIME FORMAT "HH24MISSFF3"), CAST(trade_price AS NUMERIC), price_type,
  CAST(trade_volume AS INT64), CAST(seq_no AS INT64), contract_month,
  CAST(exercise_price AS NUMERIC), CAST(cash_futures_type AS INT64)
FROM '"$IMP"'.dc_tick_stage;
COMMIT TRANSACTION;'
bq rm -f -t $P:$IMP.dc_tick_stage   # 全月が済んだら
```

列の並びは `schema/tokyo/dc_option_tick.json` と同じ順。月の外の行があっても DELETE の範囲の外に入るので、入れた後に 2. の確認（月ごとの行数・重複 0）を流す。

### 3. 事前確認

一致率（正解の期間。読み取りだけ、約 0.5GB）:

```sh
render111 accuracy | q --location=$L --format=json --maximum_bytes_billed=10737418240 \
  --parameter="start_at:TIMESTAMP:$AS" --parameter="end_at:TIMESTAMP:$AE" > $W/fill111-accuracy.json
jq -c '.[-1][] | del(.start_at, .end_at)' $W/fill111-accuracy.json
```

- `matched` の `price_ok / n`・`hhmm_ok / n`・`datetime_ok_fix4 / n_fix4` が 99.9% 以上（#111 の AC。調査時の実測は 100%）。`datetime_ok / n` が少し低いのは #4 で日付を直さなかった行（`n_fix4` の外）
- `matched` 以外（`no_match` など）の `n` が 0 に近い。`volume_null_priced_rows` が 0（ページの表示が空の行は `volume` も空）
- 満たさなければ止める

補完の前の基準値と内訳（書き込まない。それぞれ 約 1GB 以下）:

```sh
vf111 --maximum_bytes_billed=10737418240 > $W/fill111-verify-before.json
jq -c '.[-1][0]' $W/fill111-verify-before.json
# marked_rows = 0、target_rows = 1,184,423 前後。volume_null_rows・new_rows を控える
fl dryrun --maximum_bytes_billed=10737418240 > $W/fill111-dryrun.json
jq -c '.[-1][] | {basis, rows_total, rows_pending, rows_kept, rows_base_checked, rows_base_inconsistent}' $W/fill111-dryrun.json
echo "pending=$(sum111 rows_pending $W/fill111-dryrun.json) kept=$(sum111 rows_kept $W/fill111-dryrun.json) target=$(jq '.[-1][0].target_rows' $W/fill111-verify-before.json)"
```

- `rows_pending`（書く行）+ `rows_kept`（残す行）= `verify` の `target_rows`
- `rows_base_inconsistent / rows_base_checked` が 0.1% 以下（`matched` の行）
- `rows_kept` はほぼ `ambiguous_holiday` だけ（見込み 約 5.3 万行）。`no_match`・`late_trade` が多ければ止めて調べる

書き込む前に `option_price` のテーブルスナップショットを取る（期限 14 日。保存料は UPDATE で書き換えたパーティションの分だけ）。一時データセット `$IMP` が無ければ同じコマンドで作る:

```sh
bq show $P:$IMP >/dev/null 2>&1 || bq mk --dataset --location=$L --storage_billing_model=PHYSICAL --max_time_travel_hours=48 \
  --default_table_expiration=1209600 --description='#12・#4・#111 の一時データセット（期限で消える）' $P:$IMP
bq cp --snapshot --no_clobber --expiration=1209600 $P:optionchan_tokyo.option_price $P:$IMP.fill111_option_price
```

### 4. 補完

```sh
fl apply --maximum_bytes_billed=10737418240 > $W/fill111-apply.json
echo "plan=$(sum111 rows_pending $W/fill111-dryrun.json) applied=$(jq '.[-1][0].updated_option | tonumber' $W/fill111-apply.json)"   # 同じ
fl dryrun --maximum_bytes_billed=10737418240 > $W/fill111-again.json
echo "pending=$(sum111 rows_pending $W/fill111-again.json) mismatch=$(sum111 rows_mismatch $W/fill111-again.json)"   # 0 0
```

- UPDATE は 1 文（全部書くか何も書かない）。失敗したら原因（`tick_fill: ...` の `ERROR()`・クォータ）を直して同じコマンドを流し直す
- `tick_fill: dc_option_tick has no rows for trade days [...]`: その取引日の月を入れていない（2.）
- `tick_fill: N rows filled by another version`: 別の版（`legacy-fill111-*`）の補完が残っている。`rollback.sql` で戻してから流す
- `tick_fill: dc_option_tick has N duplicated ...`: 歩み値に同じ約定が 2 回ある。2. の `ld` で、その月を入れ直す

### 5. 検証

```sh
vf111 --maximum_bytes_billed=10737418240 > $W/fill111-verify-after.json
jq -c '.[-1][0]' $W/fill111-verify-after.json
```

- 0 になる項目: `out_of_scope_rows`・`null_marked_rows`・`future_rows`・`off_session_rows`・`wrong_trade_day_rows`・`volume_null_changed_rows`
- `marked_rows` = 4. の `applied`、`target_rows` = 3. の dryrun の `rows_kept` の合計
- `volume_null_rows`・`new_rows` が補完の前（`fill111-verify-before.json`）と同じ
- `base_inconsistent_rows / base_checked_rows` が 0.1% 以下
- ビューアーの `/smile` で欠け 2 の日時（例: 2023-05-01 の日中）を選び、取引高がある行使価格に約定値・取引時刻が出ること（オーナーが目視）

結果は件数・割合だけを #111 にコメントする（一致率、dryrun の basis ごとの件数、`applied`、`verify` の 0 の項目、スキャン量）。JSON の全文・行・値は貼らない。テーブルスナップショット `fill111_option_price` は期限（14 日）で消える（`$IMP` を消すときに一緒に消えてよい）。

### 切り戻し

補完の対象は元がすべて NULL の行なので、印（`parser_version = 'legacy-fill111-1'`）で正確に戻せる:

```sh
render111 rollback | q --location=$L --format=json --maximum_bytes_billed=10737418240 \
  --parameter="start_at:TIMESTAMP:$GS" --parameter="end_at:TIMESTAMP:$GE" | jq -c '.[-1][0]'
vf111 --maximum_bytes_billed=10737418240 | jq -c '.[-1][0] | {marked_rows, target_rows}'
# marked_rows = 0、target_rows が 3. の基準値と同じ
```

- 最後の手段: 3. のテーブルスナップショット（14 日）・タイムトラベル（7 日）。スナップショットからの復元は補完の後に新系が書いた行も消すので、#12 の「切り戻し」と同じく新系を止めて再処理で補う
- 規則を変えてやり直すときは、`rollback.sql` で戻してから版（`fill_version`。4 つの SQL で同じ値）を上げて流す
- 歩み値の表: Terraform の revert では消えない（`prevent_destroy`）。消すときは別の作業で保護を外す。CSV は `gcloud storage rm $B/*`（ソフト削除 7 日）

### 費用

| 項目 | 見込み |
|---|---|
| `bq load` | 無料（共有スロット） |
| 取り込みの確認 | 約 0.3GB × 2 |
| 一致率・dryrun 2 回・verify 2 回 | 各 1GB 未満（`option_price` の欠け 2 の推定の列 約 0.1〜0.2GB、正解の期間 約 0.4GB、歩み値の 2 か月 0.1GB 未満） |
| apply | 1GB 未満（推定 + 欠け 2 のパーティションの全列の書き換え 約 0.4GB） |
| 合計 | 数 GB。クエリ量上限（32GiB/日）を上げる必要は無い |
| 保存 | CSV 約 1GB（GCS）・表 約 1.2 千万行、UPDATE で書き換えたパーティションの古い版（タイムトラベル・スナップショット）。月に数円 |

### 他の仕組みとの関係

- **#4 の `fix.sql`**: 補完した行も旧系の `price_time` のある行なので、#4 の範囲（〜2026-10-12）を流し直すと推定の文脈に入る（UPDATE は `parser_version` が NULL の行だけなので補完した行は書き換わらない）。#4 は 1 回だけの作業で済んでいるので、#111 の後に #4 を流し直さない（流し直すときは先に #111 の `rollback.sql` で戻す）
- **品質チェック・新旧の突き合わせ・1 時間足**: 欠け 2 の期間を定期では読まない（過去の日を手で再実行したときだけ値が変わる）
- **新系の取り込み**: `<P>-raw` の `raw/` だけを読むので、`datacube/` の CSV は読まない。`sa-ingest` はバケット単位の読み取りを持つが、このプロジェクトの中だけ
- **ビューアー**: `/smile` の欠け 2 の日時に約定値・取引時刻が出る（決まらない行は従来どおり空）

## DataCube の歩み値での行の作成（#114）

#110 の欠け 1（**2022-09-14 15:16 〜 2023-04-25 13:35 JST**。spot・future・option の 3 表とも行が無い）と欠け 3（前日比 ±1,000 円以上で解析が失敗した区間。修正が効いた **2024-08-06 12:25 JST** より前）の、旧系の行が無い区間に、J-Quants DataCube の歩み値から option・future・ingest_runs の行を作る。オプションの歩み値は #111 の `optionchan_tokyo.dc_option_tick`、先物（日経225 ラージ）の歩み値はこの節で `optionchan_tokyo.dc_future_tick`（Terraform の `infra/prod/datacube.tf`、列の正は `schema/tokyo/dc_future_tick.json`）に入れる（購入済みの 25 か月。欠け 1 の 8 か月と欠け 3 の 17 か月。2023-05 は欠け 2 で行と IV があるので無い）。SQL は `sql/tick_build/`（試験は `tests/test_tick_build.py`・`tests/test_tick_build_bq.py`）。`bq load`・INSERT・1 時間足の作り直しは Terraform 外で、所有者がこの節の手順で行う。

**規約（DataCube の自己使用）**: 「DataCube の歩み値での穴埋め（#111）」と同じ。作った行（約定値・IV）も加工した値として同じ扱い（非公開の表・IAP の内側のビューアーだけ）。**リポジトリ・Issue・PR にはデータ・行の抜粋・値を貼らない**（件数・割合・誤差の要約だけ）。

| SQL | 内容 |
|---|---|
| `accuracy.sql` | 作り方の一致率。旧系の行がある期間（対照の窓）に `build.sql` と同じ作り方をすべての分で当て、同じ `created_at` の旧系の行と比べる（読み取りだけ） |
| `build.sql` | 行の作成。`@mode = 'dryrun'` は表 × IV の確かさ（`iv_basis`）ごとの件数を数えるだけ、`'apply'` はまだ作っていないスナップショットの行を 3 表に INSERT する（1 つのトランザクション）。流し直しても 2 回目は 0 行。範囲は 2018-10-01 〜 2024-08-06 12:25 JST の中だけ（外ならエラー） |
| `verify.sql` | 作成の検証（作り方を使わない独立の検査。1 行） |
| `rollback.sql` | 切り戻し。作った行（印のある行）を 3 表から消す |

**埋める区間**（`build.sql` の先頭のコメントに詳細）: 旧系のスナップショット（`ingest_runs` の `trigger = 'legacy_import'`）の隣り合う 2 つの間が **15 分以上**あき、前のスナップショットが 2024-08-06 12:25 JST より前で、その間に**先物の約定のある分が 10 分以上**ある区間（2026-10-17 オーナー判断。欠け 3 のほか、原因の違う欠け（停止など）も入る）。歩み値の月を入れていない区間は約定が無いので埋めない。約定のある区間が入れた月と入れていない月にまたがるとき・入れた月の取引日の歩み値が無いときは `build.sql` がエラーで止まる。

**作る行**:

| 表 | 行 |
|---|---|
| `ingest_runs` | 区間の中で、先物の約定か近い 2 限月のオプションの約定があった分 [m, m + 1 分) ごとに 1 つ。`snapshot_at = created_at = m + 1 分`（分の終わり。その分までの約定をすべて含む。旧系も分の境目）、`ingest_run_id = 'legacy-fill114-' ‖ snapshot_at（UTC、YYYYMMDDTHHMMSSZ）`、`trigger = 'legacy_fill114'`、`parser_version = 'legacy-fill114-1'`、`source_object = 'datacube:tick'`、`rows_*` は作った行の数（spot は 0）。ビューアーの `*_latest` は実行の行で絞るので、行と一緒に作る |
| `option_price` | スナップショットの取引日 T（#111 と同じ: 16:30 以降は翌日から数えて最初の現物の営業日）の近い 2 限月（最終取引日が T 以降の月次の限月の小さい方から 2 つ。最終取引日の夜間から次の限月）の、T の約定が `created_at` までに 1 つ以上ある系列。`price` = 最後の約定の値段、`price_time` = その約定日時（JST、分に切り捨て）、`volume` = T の約定の累計（ページの取引高と同じ。#111 で 100% 一致）、`iv` = 最後の約定の時点の IV（下のモデル。値段が裁定の範囲の外なら NULL）、`is_atm` = その限月の行のうち、行使価格が `created_at` の原資産に最も近い行（同じ距離なら小さい方。コール・プットの両方）、`last_trading_day`。気配・気配の IV・ギリシャ指標・前日比・建玉は NULL。約定の無い系列の行は作らない（旧系は掲載の全行使価格の行があった）。祝日取引の日は #111 と同じ（夜間のスナップショットは祝日の日中の約定を除く） |
| `future_price` | T の直近の四半期限月（最終取引日が T 以降の 3・6・9・12 月の最も近い限月）の、T の `created_at` までの最後の約定の値段・約定日時（分）。`diff`・`diff_rate`・`hv` は NULL |
| `spot_price` | 作らない（元のデータが無い） |

**IV のモデル**（旧系の JPX の `iv` と約 65 万件の約定で突き合わせて決めた。#114 のコメント）: 原資産は、限月 M 以降で最も近い四半期限月の先物の、オプションの約定の時点の直前の約定（同じ時刻は先物が先）。四半期でない M は、取引日ごとの差（プット・コール・パリティ: 行使価格が先物 ± 500 の、分ごとの最後のコールとプットの K + C − P − 先物 の中央値を 5 円に丸める）を足す。その日のサンプルが 5 未満の日（2 限月めに多い）は、10 日前からその日までのサンプルを合わせた中央値（合わせても 5 未満なら 0。`iv_basis = 'offset_zero'`。対照の窓では 0 にしても 2 限月めの IV の差は 1 限月めと同じ程度）。残存 = （SQ − 約定日時の 6 時間前の日付）/ 365、金利 0、Black-76、二分法。SQ は第 2 金曜（現物の営業日でなければその前の営業日）、最終取引日は SQ の前の営業日（旧系の `last_trading_day` と 2018-12〜2024-08 の 55 か月で一致）。

**合い方**（件数・誤差の要約だけ）:

- 突き合わせ（#114 のコメント、4 か月・約 65 万件）: JPX の `iv` との差の絶対値 中央値 0.05 ポイント、90% 点 0.21、99% 点 0.9
- `accuracy.sql`（2026-10-17。2020-02・2021-11・2022-08・2024-02 の各 3 日、旧系の約定のある約 120 万行）: 同じ `created_at` で値段・約定時刻・取引高の 3 つとも一致 99.16%、取得の遅れ（0〜3 分）を許すと 99.78%（窓ごとに 99.55〜99.83%）。同じ約定の IV の差の絶対値 中央値 0.04〜0.05・90% 点 0.20〜0.28・99% 点 1.1〜3.4（99% 点は 10 円未満の安いオプションと 1,000 円以上の深いイン・ザ・マネーで大きい。原資産を取る時点の差）。IV が NULL の行 0。限月の一致 99.95%（その時点で約定の無い限月は行が無い）。ATM の行使価格の一致 78%（旧系は現物で、こちらは先物で選ぶ）。future の値段の一致 65%（遅れを許すと 95%）

**いつ行うか**: いつでもよい（INSERT が触るのは 2018〜2024 年の区間のパーティションだけで、新系の書き込み・定期クエリと重ならない）。前提: この PR のマージ後の deploy.yml で `dc_future_tick` ができていること、#111 が済んでいること（済み）、取引カレンダーに 2018〜2024 年があること（済み）。クエリ量は 1 日で dryrun・apply・verify を合わせて 約 15GB（「費用」）。1 時間足の作り直しは別の日に分けてよい。

### 変数

「DataCube の歩み値での穴埋め（#111）」の「変数」（`P`・`L`・`IMP`・`W`・`q`）に加えて:

```sh
B_FUT=gs://$P-raw/datacube/future_tick     # 先物の CSV（future_tick_18_YYYYMM.csv、25 個）
render114() { sed -e "s/[$]{project}/$P/g" -e "s/[$]{dataset}/optionchan_tokyo/g" "sql/tick_build/$1.sql"; }
# 範囲（JST の月の境目で 3 つに分ける。B が欠け 1。A・C が欠け 3 など）。range_start・range_end は build.sql と同じ
RA='2018-09-30 15:00:00+00'; RB='2022-08-31 15:00:00+00'; RC='2023-04-30 15:00:00+00'; RE='2024-08-06 03:25:00+00'
# build.sql: 範囲 [$1, $2)、モード $3（dryrun / apply）
bd() { local s=$1 e=$2 m=$3; shift 3; render114 build | q --location=$L --format=json --maximum_bytes_billed=10737418240 \
  --parameter="start_at:TIMESTAMP:$s" --parameter="end_at:TIMESTAMP:$e" --parameter="mode:STRING:$m" "$@"; }
vf114() { local s=$1 e=$2; shift 2; render114 verify | q --location=$L --format=json --maximum_bytes_billed=10737418240 \
  --parameter="start_at:TIMESTAMP:$s" --parameter="end_at:TIMESTAMP:$e" "$@"; }
# build.sql の結果（.[-1]）の列の合計（表を $3 で絞る）
sum114() { jq "[.[-1][] | select(.tbl == \"$3\") | .$1 | tonumber] | add // 0" "$2"; }
```

### 1. 先物の歩み値の取り込み（月ごと）

CSV は `$B_FUT/` に置いてある（25 個。無ければ `option_tick` と同じく展開して `gcloud storage cp --no-clobber`）。列は 2022-08 以前が 10 列（大文字、2 列目 `Make_Date`）、2022-09 以降が 11 列（小文字、2 列目 `execution_date`、最後に `sco_category`）。並びは同じなので、見出しを飛ばして明示のスキーマで入れ、10 列のファイルは `--allow_jagged_rows` で最後の列を NULL にする。2022-07 以前の夜間の約定日はオプションと同じく CSV のまま入れ、読む SQL（`exec_at`）で直す。まず 1 か月だけ入れて確かめる:

```sh
bq show --format=prettyjson $P:optionchan_tokyo.dc_future_tick | jq '{timePartitioning, clustering, requirePartitionFilter}'
# timePartitioning が {"type":"MONTH","field":"trade_date"}。無ければ Terraform の apply を先に済ませる
gcloud storage ls $B_FUT/ | grep -c '\.csv$'   # 25
ldf() { local ym=$1; bq load --project_id=$P --location=$L --source_format=CSV --skip_leading_rows=1 --allow_jagged_rows \
  --date_format=YYYYMMDD --time_format=HH24MISSFF3 --replace \
  "$P:optionchan_tokyo.dc_future_tick\$$ym" "$B_FUT/future_tick_18_$ym.csv" schema/tokyo/dc_future_tick.json; }
ldf 202304
q --location=$L --format=csv '
SELECT COUNT(*) AS n, MIN(trade_date), MAX(trade_date), MIN(execution_time), MAX(execution_time), COUNTIF(sco_category IS NULL) AS no_sco
FROM optionchan_tokyo.dc_future_tick WHERE trade_date BETWEEN "2023-04-01" AND "2023-04-30"'
gcloud storage cat $B_FUT/future_tick_18_202304.csv | awk 'END { print NR - 1 }'   # 上の n と同じ
```

よければ全月を入れる（流し直しても同じ結果）:

```sh
for f in $(gcloud storage ls $B_FUT/ | grep '\.csv$'); do
  ym=$(basename "$f" .csv); ym=${ym#future_tick_18_}
  ldf "$ym" || { echo "failed: $ym"; break; }
done
```

確認（25 か月。スキャン 約 0.2GB）: 月ごとの行数が CSV のデータ行数と同じで、（取引日・銘柄コード・シーケンス No）の重複が 0。2022-08 以前は `sco_category` が全部 NULL、2022-09 以降は NULL が 0:

```sh
for f in $(gcloud storage ls $B_FUT/ | grep '\.csv$'); do ym=$(basename "$f" .csv); echo "${ym#future_tick_18_},$(gcloud storage cat "$f" | awk 'END { print NR - 1 }')"; done | sort > $W/dcf-csv-rows.csv
q --location=$L --format=csv --maximum_bytes_billed=2147483648 '
SELECT FORMAT_DATE("%Y%m", trade_date) AS ym, COUNT(*) AS n
FROM optionchan_tokyo.dc_future_tick WHERE trade_date >= "2018-01-01"
GROUP BY ym ORDER BY ym' | tail -n +2 | sort > $W/dcf-bq-rows.csv
diff $W/dcf-csv-rows.csv $W/dcf-bq-rows.csv && echo "rows ok"   # 25 行とも同じ
q --location=$L --format=csv --maximum_bytes_billed=2147483648 '
SELECT
  (SELECT COUNT(*) FROM (
    SELECT trade_date, security_code, seq_no FROM optionchan_tokyo.dc_future_tick WHERE trade_date >= "2018-01-01"
    GROUP BY 1, 2, 3 HAVING COUNT(*) > 1)) AS duplicated,
  (SELECT COUNTIF((trade_date < "2022-09-01") != (sco_category IS NULL)) FROM optionchan_tokyo.dc_future_tick
   WHERE trade_date >= "2018-01-01") AS sco_mismatch'   # 0 0
```

`--date_format`・`--time_format` で失敗するときは、#111 の「2. 歩み値の取り込み」の代わりの手順（全列 STRING の一時表 → トランザクションで月を入れ直す）を先物の 11 列で行う。調査で作った全列 STRING の一時表（`$IMP.dc_future_tick_stage`）は、確認が済んだら `bq rm -f -t $P:$IMP.dc_future_tick_stage` で消してよい。

### 2. 事前確認

一致率（対照の窓。読み取りだけ、1 窓 約 0.3GB）。旧系の行があり、両方の歩み値を入れた月の数日:

```sh
for w in '2020-02-17 00:00:00+09|2020-02-20 00:00:00+09' '2021-11-15 00:00:00+09|2021-11-18 00:00:00+09' \
         '2022-08-15 00:00:00+09|2022-08-18 00:00:00+09' '2024-02-19 00:00:00+09|2024-02-22 00:00:00+09'; do
  render114 accuracy | q --location=$L --format=json --maximum_bytes_billed=10737418240 \
    --parameter="start_at:TIMESTAMP:${w%|*}" --parameter="end_at:TIMESTAMP:${w#*|}" | jq -c '.[-1][0] | del(.start_at, .end_at)'
done > $W/build114-accuracy.jsonl
```

- `all_ok_lag / option_rows` が 99% 以上（値段・約定時刻・取引高の作り方。実測 99.55〜99.83%）。`built_only_rows` が 0 に近い（実測 0〜48）
- `iv_abs_p50` が 0.1 以下・`iv_abs_p90` が 0.3 以下（実測 0.04〜0.05・0.20〜0.28）。`iv_null_rows` が 0 に近い（実測 0）
- `month_ok / month_pairs` が 99.9% 以上（近い 2 限月の選び方。実測 99.89〜100%）。`atm_ok / atm_pairs` は 60〜90%（旧系は現物で選ぶ。実測 62〜93%）
- 満たさなければ止める

作成の前の基準値と内訳（書き込まない）。範囲は 3 つ（A: 〜2022-08、B: 欠け 1 の 2022-09〜2023-04、C: 2023-05〜）:

```sh
vf114 "$RA" "$RE" > $W/build114-verify-before.json; jq -c '.[-1][0]' $W/build114-verify-before.json
# built_runs = 0。legacy_runs・other_runs を控える
for r in "$RA|$RB|A" "$RB|$RC|B" "$RC|$RE|C"; do
  IFS='|' read -r s e n <<< "$r"
  bd "$s" "$e" dryrun > $W/build114-dryrun-$n.json
  jq -c '.[-1][] | {tbl, iv_basis, rows_total, rows_pending, snapshots, snapshots_pending, holes_built, hole_active_minutes_total}' $W/build114-dryrun-$n.json
done
```

- 2026-10-17 の dryrun（件数）: B は `holes_built` 1（欠け 1）・`hole_active_minutes_total` 157,497・スナップショット 180,669・option 28,024,001 行・future 180,669 行。A は 189 区間・12,885 分・13,509 スナップショット・option 2,608,535 行、C は 18 区間・9,038 分・9,440 スナップショット・option 2,112,414 行。`iv_basis` は `ok` 95.3%・`offset_zero` 4.4%・`out_of_bounds` 0.33%・`no_future` 0。スキャンは B 1.9GB・A 1.7GB・C 0.8GB
- `iv_basis` が `ok` の行がほとんど（`offset_zero` は 2 限月めで offset が決まらず 0 にした行。IV はある）。`no_future`（夜間の寄り付きで先物の約定がまだ無い）・`out_of_bounds`（値段が裁定の範囲の外）が多ければ止めて調べる
- `tick_build: holes starting at [...] span loaded and not-loaded months`: 約定のある区間の月の歩み値が片方しか無い。その月を入れるか、区間を範囲から外す（範囲を区間の手前で切る）
- `tick_build: dc_option_tick or dc_future_tick has no rows for trade days [...]`: 入れた月の取引日の歩み値が無い（ファイルの欠け）。1. の確認をやり直す

書き込む前に 3 表のテーブルスナップショットを取る（期限 14 日。INSERT だけなので、保存料は書き込んだパーティションの古い版の分だけ）:

```sh
for t in option_price future_price ingest_runs; do
  bq cp --snapshot --no_clobber --expiration=1209600 $P:optionchan_tokyo.$t $P:$IMP.build114_$t
done
```

### 3. 作成

範囲ごとに apply し、同じ範囲の dryrun をもう一度流して書く行が 0 になることを確かめる:

```sh
for n in A B C; do
  case $n in A) s=$RA e=$RB ;; B) s=$RB e=$RC ;; C) s=$RC e=$RE ;; esac
  bd "$s" "$e" apply > $W/build114-apply-$n.json
  echo "$n plan=$(sum114 rows_pending $W/build114-dryrun-$n.json option)/$(sum114 rows_pending $W/build114-dryrun-$n.json future) applied=$(jq -c '.[-1][0] | [.inserted_option, .inserted_future, .inserted_runs]' $W/build114-apply-$n.json)"
  bd "$s" "$e" dryrun > $W/build114-again-$n.json
  echo "$n pending=$(jq '[.[-1][] | .rows_pending | tonumber] | add' $W/build114-again-$n.json)"   # 0
done
```

- `inserted_option`・`inserted_future` が dryrun の `rows_pending`（option・future）と同じ。`inserted_runs` が `snapshots_pending` と同じ
- 3 表の INSERT は 1 つのトランザクション（全部書くか何も書かない）。失敗したら原因（`tick_build: ...` の `ERROR()`・クォータ）を直して同じコマンドを流し直す（作ったスナップショットは飛ばす）
- `tick_build: N snapshots built by another version`: 別の版（`legacy-fill114-*`）で作った行が残っている。`rollback.sql` で消してから流す

### 4. 検証

```sh
for r in "$RA|$RB|A" "$RB|$RC|B" "$RC|$RE|C"; do
  IFS='|' read -r s e n <<< "$r"; vf114 "$s" "$e" | jq -c '.[-1][0] | del(.start_at, .end_at)'
done > $W/build114-verify-after.jsonl
```

- 0 になる項目: `out_of_hole_runs`・`run_field_errors`・`run_count_errors`・`orphan_rows`・`row_field_errors`・`iv_range_rows`・`future_trade_rows`・`duplicate_rows`・`atm_errors`・`month_errors`
- `built_runs`・`built_option_rows`・`built_future_rows` の合計が 3. の `inserted_runs`・`inserted_option`・`inserted_future` の合計と同じ
- `legacy_runs`・`other_runs` の合計が作成の前（`build114-verify-before.json`）と同じ（旧系・新系の行に触れていない）
- ビューアーの `/smile` で欠け 1 の日時（例: 2023-01-10 の日中）を選び、約定のある行使価格に約定値・取引時刻・IV が出ること、`/atm_iv` の約定の IV（`iv=trade`）の 1 分足が欠け 1 の期間につながること（オーナーが目視。mid は欠けのまま）

結果は件数・割合だけを #114 にコメントする（一致率の要約、dryrun の表 × `iv_basis` ごとの件数、`inserted_*`、`verify` の 0 の項目、スキャン量）。JSON の全文・行・値は貼らない。

### 5. 1 時間足の作り直し

1 時間足（#49 の `atm_iv`・`atm_option_price`・`atm_target_price`・`atm_mid_iv`）は、作った行のある窓（JST 07:00 区切りの 1 日）を作り直す。窓の中を置き換えるので、旧系の行と作った行が混ざる日（欠け 3）もその日の全体が最新版から作り直される（旧系の移行分と同じ計算。#49 の「旧系との突き合わせ」で同値）。手順は「1 時間足（#49）」の「再実行・バックフィル」（変数も同じ節）で、窓の日付を作った実行から出す:

```sh
q --location=$L --format=csv '
SELECT DISTINCT FORMAT_DATE("%F", DATE(TIMESTAMP_SUB(snapshot_at, INTERVAL 7 HOUR), "Asia/Tokyo")) AS window_date
FROM optionchan_tokyo.ingest_runs
WHERE snapshot_at >= "2018-09-30" AND snapshot_at < "2024-08-07" AND STARTS_WITH(ingest_run_id, "legacy-fill114-")
ORDER BY window_date' | tail -n +2 > $W/build114-windows.txt
wc -l < $W/build114-windows.txt
rdays < $W/build114-windows.txt   # 1 日ずつ DTS の手動実行。最後に all SUCCEEDED
```

- 窓は 約 300 日（欠け 1 の約 7 か月 + 欠け 3 の日）。DTS の手動実行は 1 日 約 1 分なので数時間かかる。毎朝の定期実行（07:40）と重ならない時間に流す
- 1 窓 約 10〜60MB（欠け 1 の日は作った行だけ、欠け 3 の日は旧系の行も読む）。全部で 約 10GB なので、作成と同じ日に流すときはクエリ量（32GiB/日）を見て分ける
- `atm_mid_iv`: 作った行は気配の IV（`bid_iv`・`ask_iv`）が NULL なので、作った行だけの 1 時間（欠け 1）の足は OHLC が NULL の行になる（旧系の気配の無い時間と同じ。ビューアーの `/atm_iv?iv=mid` は線が切れるだけ）。欠け 3 の日は同じ時間の旧系の行の mid がそのまま入る
- 30 日固定の ATM IV（#100 の `atm_cm_iv`）は気配（`bid`・`ask`）から作るので、作った行からは行ができない（欠けのまま）。`cm_backfill.sql` を流し直す必要は無い（流しても作った行の窓は何も変わらない）
- 確認: 「1 時間足（#49）」の「日次の確認」のクエリの期間を窓の範囲にして、窓ごとに 4 表の `bars` が同じで、`bars = keys`（キーの重複 0）

### 切り戻し

作った行は元の表に無かった行なので、印（`ingest_run_id` の接頭辞 `legacy-fill114-` と `parser_version = 'legacy-fill114-1'`）で消せば作成の前と同じになる:

```sh
render114 rollback | q --location=$L --format=json --maximum_bytes_billed=21474836480 \
  --parameter="start_at:TIMESTAMP:$RA" --parameter="end_at:TIMESTAMP:$RE" | jq -c '.[-1][0]'
vf114 "$RA" "$RE" | jq -c '.[-1][0] | {built_runs, built_option_rows, built_future_rows, legacy_runs, other_runs}'
# built_* = 0、legacy_runs・other_runs が作成の前と同じ
```

- 切り戻した後は 5. と同じ窓の 1 時間足を作り直す（作った行の分の足が消え、欠け 1 の窓は行が無いので何もしない → 既存の足が残る。残った足は `DELETE FROM optionchan_tokyo.<表> WHERE started_at >= '2022-09-14' AND started_at < '2023-04-26'` のように窓の範囲で 4 表から消す）
- 最後の手段: 2. のテーブルスナップショット（14 日）・タイムトラベル（7 日）。スナップショットからの復元は作成の後に新系が書いた行も消すので、#12 の「切り戻し」と同じく新系を止めて再処理で補う
- 作り方を変えてやり直すときは、`rollback.sql` で消してから版（`build_version`。4 つの SQL で同じ値）を上げて流す
- 先物の歩み値の表: Terraform の revert では消えない（`prevent_destroy`）。消すときは別の作業で保護を外す。CSV は `gcloud storage rm $B_FUT/*`（ソフト削除 7 日）

### 費用

| 項目 | 見込み |
|---|---|
| `bq load` | 無料（共有スロット） |
| 取り込みの確認 | 約 0.2GB × 2 |
| 一致率（4 窓） | 各 約 0.3GB |
| dryrun（3 範囲 × 2 回） | B 1.9GB・A 1.7GB・C 0.8GB（2026-10-17 の実測。歩み値 + 作った行の一時表の読み込み） |
| apply（3 範囲） | dryrun + 一時表の全列の読み込み（範囲 B で 約 2GB） |
| verify（前 1 回・後 3 範囲） | 作った日のパーティションの 12 列（欠け 3 の日は旧系の行も読む。全部で 約 4GB） |
| 1 時間足の作り直し | 約 10GB（5.） |
| 保存 | 先物の CSV 約 0.6GB（GCS）・表 約 1 千万行、作った行（option 約 3,270 万行・future と ingest_runs 約 20 万行）。月に数円〜十数円 |

### 他の仕組みとの関係

- **旧系の行を `ingest_run_id` の接頭辞 `legacy-` で選ぶ SQL**: 作った行も `legacy-` で始まる（`legacy-fill114-`）。#4 の `sql/price_time_fix/`・#111 の `sql/tick_fill/`・#12 の `sql/legacy_import/` の検証は済んだ 1 回だけの作業なので流し直さない（流し直すときは先に `rollback.sql` で消すか、`legacy-fill114-` を除く）。#111 の `fill.sql` の範囲（欠け 2）には作った行は無い
- **品質チェック・新旧の突き合わせ**: 新系の日だけを読むので影響しない
- **1 時間足・30 日固定の IV**: 5. のとおり（定期クエリは前日の窓だけなので、作り直さない限り変わらない）
- **ビューアー**: `/smile` の欠け 1・3 の日時に、約定のあった行使価格の約定値・取引時刻・IV が出る（気配・mid IV・デルタは空。約定の無い行使価格は行が無い）。`/atm_iv` の約定の IV がつながる（mid は欠けのまま）
- **新系の取り込み**: `<P>-raw` の `raw/` だけを読むので、`datacube/` の CSV は読まない

## 1 時間足（#49）

旧系の `insert_ohlc`（Cloud Functions、python37、毎日 00:01 UTC）の置き換え（target.md 3.2）。新系の最新版から ATM の 1 時間足を作る BigQuery Data Transfer Service のスケジュールされたクエリ 1 本と、結果の 3 表（+ #98 で足した mid IV の `atm_mid_iv`。旧系に無い表）。旧系（`insert_ohlc`・旧 `optionchan.atm_*`）は変えない（止めるのは #14）。定義は `infra/prod/sq.tf`、SQL は `sql/ohlc/`（試験は `tests/test_ohlc.py`・`tests/test_ohlc_bq.py`）。

| もの | 内容 |
|---|---|
| DTS `optionchan atm-hourly`（asia-northeast1、`sa-sq-dts`、毎日 07:40 JST = `every day 22:40` UTC） | `sql/ohlc/atm_hourly.sql`。窓は `@run_time` の JST の日付 D に対し `[D-1 07:00, D 07:00) JST`（「日次の品質チェックと新旧の突き合わせ（#40）」と同じ 1 日分）。`option_price_latest` の ATM の put（`is_atm AND type = 2`）から限月 × 1 時間（UTC の正時 = JST の正時）の OHLC を作り、4 表へ MERGE する（窓の中を置き換える。ATM の行が 1 件も無い窓では何もしない。4 表は 1 トランザクション）。#40 の 4 本（07:02〜07:30）の後で、失敗の M2 の抑制は 08:40 に終わる |
| `optionchan_tokyo.atm_iv` / `atm_option_price` / `atm_target_price` | 列は旧系と同じ（`schema/ohlc/atm_ohlc.json`）。パーティションは月（`started_at`）、クラスタは `last_trading_day`。キーは `(last_trading_day, time_frame, started_at)`、`time_frame = 3600` |
| `optionchan_tokyo.atm_mid_iv`（#98） | ATM の put の mid IV（気配の中値 =（`bid_iv` + `ask_iv`）/ 2。両方 0 より大きいときだけ）の 1 時間足。列・パーティション・クラスタ・キーは `atm_iv` と同じ。足の切り方・OHLC の式も `iv` と同じ（`atm_hourly.sql` の `mid_bars`）。ビューアーの `/atm_iv_bars?iv=mid` の 1 時間足・日足が読む |
| `sql/ohlc/mid_backfill.sql`（東京、手で流す） | `atm_mid_iv` の過去分の作り直し（下の「mid IV の過去分（#98）」） |
| `sql/ohlc/legacy_check.sql`（US、手で流す） | 旧系の同値の確認: 旧 `option_price` に同じ 1 時間足の計算を当てて旧 3 表と比べる |
| `sql/ohlc/export.sql`（US）→ `compare.sql`（東京） | 並行稼働中の日の新旧の突き合わせ |
| `sql/ohlc/import.sql`・`fingerprint.sql`（東京・US） | 旧系の過去分の移行と、その検証（#12 の後） |

- 1 時間足の意味は旧系と同じ: `open` / `close` = その 1 時間で `created_at` が最も早い / 遅い NULL でない値、`high` / `low` = 最大 / 最小、全部 NULL なら NULL の行。1 時間の中で ATM の行使価格が変わっても分けない。同じ `created_at` の行（旧系の重複を移した行）の順は `target_price` で決める
- 旧系は前日（UTC の日）分を 00:01 UTC に集計するので、08:45〜08:59 JST の行（JPX のページの遅れで未着）の分だけ、UTC 23 時台（08:00 JST）の 1 時間足が欠ける・違う。新系は 07:40 に前日の窓を作るので欠けない（2026-09-12〜10-12 の比較で、違いは 23 時台の各表 38〜39 件だけ）
- 失敗は Cloud Logging の `resource.type="bigquery_dts_config"`・severity ERROR で、新系の M2 が通知する（#40 と同じ。条件は変えていない）
- 料金: 1 日 約 41 MiB のスキャン（新系の 1 日の最新版の 7 列。#98 で `bid_iv`・`ask_iv` を足した。2026-10-13 の窓の dry run）。4 表は 1 表 1 日 約 40 行

### 変数

「日次の品質チェックと新旧の突き合わせ（#40）」の「変数」（`P`・`L`・`SQ`・`tc`）に加えて:

```sh
WDIR=<スクラッチのディレクトリ>   # 結果の JSON を置く（リポジトリ・Issue に貼らない。#12 と同じでよい）
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
ro legacy_check | qr '2026-09-12 00:00:00+00' '2026-10-13 00:00:00+00' --location=US --format=json > $WDIR/legacy-check.json
jq -c '.[-1][] | {tbl, recomputed, legacy_keys, match, late, differ, legacy_only, recomputed_only, legacy_dup_keys, legacy_conflict_keys}' $WDIR/legacy-check.json
# differ・legacy_only・recomputed_only が 0。late は UTC 23 時台（旧系の集計の時点で未着だった行の分）
```

2026-10-13 の実行（2026-09-12〜10-13 00:00 UTC）: 3 表とも `differ`・`legacy_only`・`recomputed_only` 0、`match` 887 / 888 / 887、`late` 39 / 38 / 39。旧系を止める前（#14）にもう一度流して記録する。

**並行稼働中の日の新旧**（旧 3 表 ↔ 新系の 3 表）。旧系は窓の日付の翌日 00:01 UTC に前日（UTC の日）分を作るので、UTC の日 U の比較は U+1 日の 00:10 UTC 以降、新系の窓を作った後に行う。1 日ずつ（JSON 約 20KB。コマンドの引数の上限 128KB を超えないように）:

```sh
U=2026-10-14   # UTC の日
s="$U 00:00:00+00"; e="$(date -u -d "$U + 1 day" +%F) 00:00:00+00"
bars=$(ro export optionchan | qr "$s" "$e" --location=US --format=json | jq -r '.[0].bars')
[ "$bars" != null ] && ro compare | qr "$s" "$e" --parameter="legacy_bars:STRING:$bars" --location=$L --format=json > $WDIR/compare-$U.json
jq -c '.[-1][] | {tbl, legacy_keys, new_keys, match, differ, legacy_only, new_only, legacy_dup_keys, conflict_keys, max_abs_diff}' $WDIR/compare-$U.json
```

- `legacy_only` / `new_only`: 片方にだけある 1 時間足。新系の取りこぼし（その時間の `legacy_compare_daily` の `legacy_only_snapshots`）・旧系の 23 時台の欠け・新系だけの時間（保存条件の差）で説明できるかを見る
- `differ`: 新旧はスナップショットの版が違うことがある（#40 の「版の差」。ATM の行の iv・価格も 1 分の中で変わる）ので、0 にはならない。`max_abs_diff` と `samples` を見て、版の差で説明できない大きな違い（時間全体がずれている等）が無いことを確かめる
- 結果（件数・`max_abs_diff`）を #49 にコメントする（全文は貼らない）

### 過去分の移行（#12 の後）

旧 3 表のうち、`started_at` が C（新系の最初のスナップショット 2026-10-12 23:39 UTC を含む窓の始まり = **2026-10-12 22:00 UTC**）より前の行を、新系の 3 表へ移す。C 以降は定期クエリが作る。**「過去データの移行（#12）」の 5. の後、6. の後片付け（一時データセット `optionchan_import` の表を消す）の前に行う**（#12 のデータセットコピーで旧 `atm_*` も運ばれている）。1 つの端末で上から順に。旧 3 表は 1 表 約 4MB なので、クエリ量の上限の変更は要らない。

- 同じ中身の重複は 1 行にする。中身の違う重複のキー（旧 `atm_target_price` の 2019-07-01 の 44 キー。片方は option の価格の値）は移さない（下の 4. で作り直す）
- 新系に既にあるキーは上書きしない（流し直しても増えない）。値は旧系のまま（旧系の 23 時台の欠け・2019〜2020 年の一部の表だけ欠けた日もそのまま。直すなら 4. と同じ手動実行でその日を作り直す）

```sh
C='2026-10-12 22:00:00+00'; S0_OHLC='2018-01-01 00:00:00+00'   # #12 の S0（2018-10-01）を上書きしない
# 1. コピーの検証: US の旧 3 表と一時データセットの同じ範囲が一致
ro fingerprint optionchan | qr "$S0_OHLC" "$C" --location=US --format=json > $WDIR/ohlc-fp-us.json
ro fingerprint optionchan_import | qr "$S0_OHLC" "$C" --location=$L --format=json > $WDIR/ohlc-fp-import.json
diff <(jq -S . $WDIR/ohlc-fp-us.json) <(jq -S . $WDIR/ohlc-fp-import.json) && echo MATCH   # MATCH でなければ止める
# 2. 移行（3 表に書く。結果は表ごとの inserted・skipped_existing・conflict_keys・dup_rows_collapsed）
ro import | qr "$S0_OHLC" "$C" --location=$L --format=json > $WDIR/ohlc-import.json; jq -c '.[-1][]' $WDIR/ohlc-import.json
#    もう一度流して inserted が 0（冪等）
ro import | qr "$S0_OHLC" "$C" --location=$L --format=json > $WDIR/ohlc-import-again.json; jq -c '.[-1][] | {tbl, inserted}' $WDIR/ohlc-import-again.json
# 3. 移行の検証: 新系の fp_clean_* = 旧系の fp_clean_*、新系の rows_ = keys = 旧系の keys − conflict_keys、新系の conflict_keys 0
ro fingerprint optionchan_tokyo | qr "$S0_OHLC" "$C" --location=$L --format=json > $WDIR/ohlc-fp-tokyo.json
jq -c '.[] | {tbl, keys, conflict_keys, fp_clean_xor, fp_clean_sum}' $WDIR/ohlc-fp-us.json
jq -c '.[] | {tbl, rows_, keys, conflict_keys, fp_clean_xor, fp_clean_sum}' $WDIR/ohlc-fp-tokyo.json
# 4. 作り直し（1 日ずつ順に。全日が SUCCEEDED であること）
#  4a. 中身の違う重複のキーがある窓の日付（移さなかったキー。窓は JST 07:00 区切りなので、UTC 23 時台のキーは翌日の窓に入る）。
#      一時データセットから出す。2026-10-13 の旧系では 2019-07-01 と 2019-07-02 の 2 日
qr "$S0_OHLC" "$C" --location=$L --format=csv '
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
ORDER BY window_date' | tail -n +2 > $WDIR/ohlc-conflict-days.txt
cat $WDIR/ohlc-conflict-days.txt
rdays < $WDIR/ohlc-conflict-days.txt          # all SUCCEEDED（その窓の移行済みの行も再計算で置き換わる）
#  4b. C 以降の窓（窓の日付 2026-10-13 〜 E の前日（金曜）。#12 で新系の取りこぼしが旧系の行で埋まった）
days 2026-10-13 <E の前日（金曜）> | rdays   # all SUCCEEDED
#  4c. 確認: 作り直した窓ごとに 3 表のキーの数が同じで、キーの重複が無い（4a の日付と 4b の範囲）
for d in $(cat $WDIR/ohlc-conflict-days.txt); do
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
- 切り戻し（移行行だけを消す。C より前は移行行だけ）: `qr "$S0_OHLC" "$C" --location=$L 'DELETE FROM optionchan_tokyo.<表> WHERE started_at >= @start_at AND started_at < @end_at'` を 3 表で。4a で作り直した窓（2019-07-01・2019-07-02）の 1 時間足も消える

### mid IV の過去分（#98）

`atm_mid_iv` は #98 で足した表で、定期クエリが mid を書くのは #98 のデプロイ後の窓から。それより前は `sql/ohlc/mid_backfill.sql` で最新版から作り直す（`atm_hourly.sql` の `mid_bars` と同じ計算で、範囲の中を置き換える。冪等で、何度流しても同じ）。気配の IV（`bid_iv`・`ask_iv`）は 2018 年から ATM の行の 96〜99.6% にある（2026-10-16 の確認。2018 年 96.1%・2019〜2023 年 99.2〜99.6%・2024〜2025 年 98.6〜99.0%・2026 年 93.2%）。

- 範囲: 始まりは 2018-01-01（最新版の最初の日より前でよい。行の無い範囲は何もしない）。終わり E は、#98 のデプロイ後に定期クエリが最初に作った窓の始まり（JST 07:00 = UTC 22:00）。E 以降は定期クエリが作る。範囲は正時で、now より前（SQL が検査する）
- 量: 1 年 約 6.5 GiB（2025 年の dry run で 6,980,294,851 バイト）、全部で 約 40 GiB。プロジェクトのクエリ量の上限（32 GiB/日）を超えるので、**1 日に流すのは 4 年分まで**にし、残りは翌日（太平洋時間の 0 時 = JST 16〜17 時に上限が戻った後）に流す。取り込み・定期クエリの分（1 日 数 GiB）を残す
- 1 年ずつ流す（`--maximum_bytes_billed` は 1 年分の上限。結果の `bars`・`with_mid` だけを記録する）:

```sh
E=<定期クエリが mid を書いた最初の窓の始まり>   # 例: 2026-10-15 22:00:00+00
for y in 2018 2019 2020 2021; do
  s="$y-01-01 00:00:00+00"; e="$((y + 1))-01-01 00:00:00+00"
  ro mid_backfill | qr "$s" "$e" --location=$L --format=json --maximum_bytes_billed=10737418240 | jq -c '.[-1][0]'
done
# 翌日: 2022〜2025 年と、2026-01-01〜E
ro mid_backfill | qr "2026-01-01 00:00:00+00" "$E" --location=$L --format=json --maximum_bytes_billed=10737418240 | jq -c '.[-1][0]'
```

- 確認: `atm_iv` と `atm_mid_iv` の 1 時間足のキーの数が年ごとに同じ（どちらも同じ ATM の行から作る）。mid の値のある足の割合は上の気配の IV の割合と同じ程度

```sh
q --location=$L --format=pretty "SELECT EXTRACT(YEAR FROM started_at) y, COUNTIF(t = 'iv') iv_keys, COUNTIF(t = 'mid') mid_keys,
  COUNTIF(t = 'mid' AND close IS NOT NULL) mid_with_value
FROM (SELECT 'iv' t, close, started_at FROM \`$P.optionchan_tokyo.atm_iv\` WHERE started_at >= '2018-01-01' AND started_at < '$E'
  UNION ALL SELECT 'mid', close, started_at FROM \`$P.optionchan_tokyo.atm_mid_iv\` WHERE started_at >= '2018-01-01' AND started_at < '$E')
GROUP BY y ORDER BY y"
```

- 結果（年ごとの件数）を #98 にコメントする（全文は貼らない）。戻すとき（作り直しが不要になったとき）は `DELETE FROM optionchan_tokyo.atm_mid_iv WHERE started_at < E`（ビューアーは過去の mid が無いと表示しないだけ）

### 停止・削除

- 一時停止: `infra/prod/sq.tf` の `atm_hourly` に `disabled = true` を足す PR（急ぐときはコンソールで無効 → 同じ PR）
- 全部を戻す（この Issue の revert）: 3 表の `deletion_protection = false` の PR → revert の PR（delete 7）。#98 の `atm_mid_iv` だけを戻すときも同じ（`deletion_protection = false` の PR → #98 の revert の PR。delete 1）。旧系の `insert_ohlc` は動いたままなので、旧ビューアーには影響しない
- 旧系の停止（#14）: `insert_ohlc` と Scheduler `create_timeframe` を止める前に、「旧系の同値の確認」を直近 30 日でもう一度流して記録する。止めた後は新系の 3 表だけになる（旧 3 表は #15 で消すまで残る）

## 30 日固定の ATM IV（#100）

`/atm_iv` の「30日固定」（`n=cm`）の値を作る BigQuery Data Transfer Service のスケジュールされたクエリ 1 本と、結果の表。計算の式は README の「30 日固定の ATM IV（#100）」。定義は `infra/prod/sq.tf`、SQL は `sql/cm/`（試験は `tests/test_cm.py`・`tests/test_cm_bq.py`）。

| もの | 内容 |
|---|---|
| DTS `optionchan atm-cm`（asia-northeast1、`sa-sq-dts`、毎時 :45 = `every 1 hours`、`schedule_options.start_time` を過去の :45（`2026-10-01T22:45:00Z`）にして分をそろえる。#117） | `sql/cm/atm_cm.sql`。最新版の全行使価格の気配から取り込み 1 回ごとに 1 行を作り、`atm_cm_iv` の窓の中を置き換える（結果が 0 行の窓では何もしない）。窓は 2 つ: **07:45 JST の回**（`@run_time` が JST の 7 時台）は「1 時間足（#49）」と同じ `[D-1 07:00, D 07:00) JST` の作り直し（重ねより遅れた取り込み・再処理の安全網）。**他の回**は増分 `[表の最後の行 − 2 時間, @run_time)`（始まりは `@run_time` − 3 日より前にしない）。増分の回は、3 日の範囲の snapshot で `@run_time` − 65 分以降に終わった取り込み（`ingest_runs`）が無ければ何もしない（週末・休場日・早朝。`ingest_runs` だけを読んで終わる）。失敗の M2（1 時間に 1 回まで）は毎時の回でも出る |
| `optionchan_tokyo.atm_cm_iv` | 列は `schema/cm/atm_cm_iv.json`（`created_at`・`iv30`・`method` と、限月ごとの最終取引日・SQ までの日数・フォワード・補間 ATM IV）。パーティションは月（`created_at`）。1 日 約 1,000 行 |
| `sql/cm/cm_backfill.sql`（東京、手で流す） | 範囲 `[@start_at, @end_at)` の作り直し（下の「過去分」） |

- ビューアーは表の最後の行の 1 秒後から now まで（最大 3 日）を同じ計算で補う（`apps/viewer` の `sql/atm_cm.sql`）。表は 1 時間以内に新しくなるので、補う分はふつう UTC の 1 日分のパーティション（約 110 MiB）。表が止まると最大 3〜4 日分
- 料金（課金バイト。2026-10-16 の値で実測）: 増分の回 約 150 MiB（`option_price` の UTC の日パーティション 1 つ 約 112 MiB + 検査 2 表の最低課金 10 MiB ずつ + INSERT 約 20 MiB。その日のパーティションは日中に育つので早い時刻ほど少ない。09:45〜10:45 JST の回は前日のパーティションも読む）、07:45 JST の回 約 250 MiB（2 パーティション）、何もしない回 10 MiB（`ingest_runs` だけ）。1 日で平日 4 GiB 以下（見積もり 2〜4 GiB）・日曜 0.3 GiB 以下。プロジェクトのクエリ量の上限（2026-10-18 時点の effectiveLimit 102400 MiB/日。「コスト」）の数 %
- 重ね 2 時間より遅れて終わった取り込みは、翌朝 07:45 の回まで古い値のまま。3 日を超えて止まったとき・前日より前の最新版が変わったとき（再処理・回収）は「過去分」の `cm_backfill.sql` でその範囲を作り直す

### 確認（prod の apply の後）

`tc`・`q` は「定期クエリ（#40）」・「1 時間足（#49）」の関数。件数・要約だけを #117 に記録する（行・ログの全文は貼らない）。

```sh
# 定期クエリができている（表示名で選ぶ）
bq ls --transfer_config --transfer_location=$L --project_id=$P --format=json | jq -r '.[] | select(.displayName == "optionchan atm-cm") | .schedule'
# apply の直後（#117）: schedule が every 1 hours、nextRunTime の分が :45。config_id（tc の値の最後）が apply の前と同じ（作り直しでない）
bq show --transfer_config --format=json "$(tc atm-cm $L)" | jq '{schedule, scheduleOptions, nextRunTime}'
# 数日後: 回の分が :45 のままずれていない（7 時台の回が毎日 1 回ある）。runTime の JST の時・分を並べる
bq ls --transfer_run --run_attempt=LATEST --max_results=72 --format=json "$(tc atm-cm $L)" | jq -r '.[].runTime' | sort | tail -72
# 取引日の 10:00〜15:00 JST: 表の最後の行が 120 分以内（09 時台は 08 時台の回が寄り付き前で新しい取り込みが無いので除く）
q --location=$L --format=pretty "SELECT TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(created_at), MINUTE) AS lag_min
FROM \`$P.optionchan_tokyo.atm_cm_iv\` WHERE created_at >= TIMESTAMP(CURRENT_DATE('Asia/Tokyo'), 'Asia/Tokyo')"
# 翌朝 07:45 の後: 前日の窓の行数と、iv30 のある割合
q --location=$L --format=pretty "SELECT COUNT(*) n, COUNTIF(iv30 IS NOT NULL) with_iv30, COUNTIF(method = 'interp') interp
FROM \`$P.optionchan_tokyo.atm_cm_iv\` WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)"
```

07:45 の作り直しと毎時の結果の一致（07:45 の回の後、データセットの time travel（168 時間）の間に。`D` は作り直した日の JST の日付、窓は `[D-1 07:00, D 07:00) JST`）: 07:44 JST の時点の表と今の表を `created_at` で突き合わせ、片方にしか無い行と値の違う行が 0 件であること。違えば件数と `created_at` を #117 に記録し、その取り込みの `ingest_runs.finished_at − snapshot_at` で重ね（2 時間）より遅れた取り込みか確かめる:

```sh
D=<YYYY-MM-DD>
q --location=$L --format=pretty "
DECLARE s TIMESTAMP DEFAULT TIMESTAMP('$D 07:00:00', 'Asia/Tokyo') - INTERVAL 1 DAY;
DECLARE e TIMESTAMP DEFAULT TIMESTAMP('$D 07:00:00', 'Asia/Tokyo');
-- 1 つの文で同じ表を時刻つき・なしで読めないので、07:44 の時点の窓を一時テーブルにしてから比べる
CREATE TEMP TABLE a AS SELECT * FROM \`$P.optionchan_tokyo.atm_cm_iv\` FOR SYSTEM_TIME AS OF TIMESTAMP('$D 07:44:00', 'Asia/Tokyo')
WHERE created_at >= s AND created_at < e;
SELECT COUNTIF(a.created_at IS NULL) only_after, COUNTIF(b.created_at IS NULL) only_before,
  COUNTIF(a.created_at IS NOT NULL AND b.created_at IS NOT NULL AND (
    a.iv30 IS DISTINCT FROM b.iv30 OR a.method IS DISTINCT FROM b.method
    OR a.m1_last_trading_day IS DISTINCT FROM b.m1_last_trading_day OR a.m1_days IS DISTINCT FROM b.m1_days
    OR a.m1_forward IS DISTINCT FROM b.m1_forward OR a.m1_iv IS DISTINCT FROM b.m1_iv
    OR a.m2_last_trading_day IS DISTINCT FROM b.m2_last_trading_day OR a.m2_days IS DISTINCT FROM b.m2_days
    OR a.m2_forward IS DISTINCT FROM b.m2_forward OR a.m2_iv IS DISTINCT FROM b.m2_iv)) differ
FROM a FULL OUTER JOIN (SELECT * FROM \`$P.optionchan_tokyo.atm_cm_iv\` WHERE created_at >= s AND created_at < e) b
  ON a.created_at = b.created_at"
```

最初の週末（土 06:00〜月 08:45 JST）を含む期間の後: 何もしない回が ERROR にならず `SUCCEEDED` で終わっていること（ERROR のログが 0 件）と、1 日のスキャン量（JST の日ごとの親ジョブの課金バイトの合計。平日 4 GiB 以下・日曜 0.3 GiB 以下の見込み。超えたら原因を #117 に記録）:

```sh
CID=$(tc atm-cm $L | awk -F/ '{print $NF}')
gcloud logging read "resource.type=\"bigquery_dts_config\" AND resource.labels.config_id=\"$CID\" AND severity>=ERROR" \
  --project=$P --freshness=4d --format='value(timestamp)' | wc -l
bq ls --transfer_run --run_attempt=LATEST --max_results=100 --format=json "$(tc atm-cm $L)" | jq -r '.[].state' | sort | uniq -c
q --location=$L --format=pretty "
SELECT DATE(creation_time, 'Asia/Tokyo') AS d, COUNT(*) AS runs, ROUND(SUM(total_bytes_billed) / POW(1024, 3), 2) AS gib
FROM \`region-asia-northeast1\`.INFORMATION_SCHEMA.JOBS
WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 4 DAY) AND statement_type = 'SCRIPT'
  AND EXISTS (SELECT 1 FROM UNNEST(labels) l WHERE l.key = 'dts_config_id' AND l.value = '$CID')
GROUP BY d ORDER BY d"
```

### 失敗を受けたとき・手動の再実行

「1 時間足（#49）」の「失敗を受けたとき」のログの見方・メッセージの表と同じ（`config_id` は `atm-cm`、メッセージの先頭は `atm_cm:`）。

- 増分の回の失敗: 何もしなくてよい。次の回が表の最後の行 − 2 時間から読むので、失敗した区間も埋まる（続けて失敗するなら原因を直す。M2 は 1 時間に 1 回まで鳴り続けるので、すぐ直せないときは「停止・削除」の一時停止）
- 07:45 の回の失敗（前日分の作り直し）: 原因を直した後、`rt` の `atm-hourly` を `atm-cm`、`22:40` を `22:45` に替えて、作り直す窓 `[D-1 07:00, D 07:00) JST` の始まりの日付 `D-1`（「1 時間足（#49）」の窓の日付と同じ）で手動実行する（`--run_time=<D-1>T22:45:00Z` は JST の D 07:45 = 7 時台なので 1 日の窓）。7 時台以外の `run_time` の手動実行は増分の回になる
- 手動実行は毎時の回（:45）と同じ表に DML を出して衝突しうる（`Transaction ...` / `Could not serialize access ...` で片方が失敗する）ので、:40〜:50 を避ける
- 3 日を超えて止まった・前日より前の最新版が変わった: 「過去分」の `cm_backfill.sql` でその範囲を作り直す

### 過去分

表ができる前（デプロイの後に定期クエリが最初に作った窓の始まり E より前）は `sql/cm/cm_backfill.sql` で作り直す（同じ計算で範囲の中を置き換える。冪等）。

- 範囲: 2018-10-01 〜 E（範囲は正時で now より前。SQL が検査する）。データの無い期間（2022-09 中旬〜2023-04 下旬）は何もしない
- 量: 全部で 約 35 GiB（必要な列を全期間で読んだときの dry run）。1 か月 0.3〜1.2 GiB（行数の多い 2026 年が大きい）。プロジェクトのクエリ量の上限（32 GiB/日）を超えるので、**1 日に流すのは 20 GiB まで**（2018〜2023 年で約 15 GiB、2024 年〜E で約 20 GiB の 2 日に分ける）
- 1 年ずつ流す（`ro` は「1 時間足（#49）」の変数。`sql/cm/` を読むように替える）:

```sh
rc() { sed -e "s/[$]{project}/$P/g" -e "s/[$]{dataset}/optionchan_tokyo/g" "sql/cm/$1.sql"; }
E=<定期クエリが最初に作った窓の始まり>   # 例: 2026-10-16 22:00:00+00
for y in 2018 2019 2020 2021 2022 2023; do
  s="$y-01-01 00:00:00+00"; e="$((y + 1))-01-01 00:00:00+00"
  rc cm_backfill | qr "$s" "$e" --location=$L --format=json --maximum_bytes_billed=10737418240 | jq -c '.[-1][0]'
done
# 翌日: 2024・2025 年と、2026-01-01〜E
```

- 結果は 1 行（`snapshots` = 取り込みの回数、`with_iv30` = 30 日の IV のある回数）。年ごとの件数を #100 にコメントする（全文は貼らない）
- 確認: 年ごとの `with_iv30 / snapshots` が README の「出せる割合」と同じ程度（日中・夜間の前半はほぼ 100%、2021〜2025 年は深夜が NULL で 72〜100%）

### 停止・削除

- 一時停止: `infra/prod/sq.tf` の `atm_cm` に `disabled = true` を足す PR。ビューアーは表が古いと補う分（最大 3 日）だけを計算し、それより前の期間は空になる
- 戻す: `atm_cm_iv` の `deletion_protection = false` の PR → #100 の revert の PR

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

ブラウザ（所有者のアカウント）: `$U/`・`$U/smile`・`$U/atm_iv` を開き（旧型の ATM 推移 `/atm` は #58 で削除した）、`/atm_iv`（#68・#77。期間の選択・Update ボタンは無い）は開発者ツールのネットワーク（`atm_iv_bars` で絞る）を開いたまま次を見る:

1. 開いた直後: チャートが薄くなり中央に「読み込み中…」とスピナーが出て、描けたら消える。要求は `?tf=1h&n=0`（start・end なし）1 本
2. ドラッグ・ホイール（スマートフォンはスワイプ・ピンチ）で左（過去）へ動かし、読み込んだ範囲の端に近づくと、左上に「前のデータを読み込み中…」のバッジが出て `start=&end=` 付きの要求が 1 本出る（1 時間足は 60 日分。続けて動かしても、読み込み中は次の要求を出さない）。読み込んだ範囲の中で動かしても要求は出ない
3. 足を 1時間 → 5分 に変えると、見ていた時間の範囲（分足は右端から最大 3 日）を読み直し、同じ辺りが表示される（最新を見ていたなら最新）。5分 → 15分 → 1分 は要求が出ない（1 分足を画面で束ねる）。日足・限月 0 → 1 でも範囲を保つ
4. 分足で左へ動かし続けると、9 日分で「この足で読み込むのは 9 日分までです」が出て止まる
5. 足を続けて何度も切り替えても、表示は最後に選んだ足になる（前の要求はネットワークで `(canceled)`）
6. カーソルの位置の足の時刻（JST）と OHLC がチャートの上に出る。2 限月目の iv が全部空の時間帯は「データがありません」になる（エラーではない）
7. 失敗の表示（任意）: 開発者ツールのネットワークを「オフライン」にして足を変えると「データを取得できませんでした (network)」と「再試行」が出る。オンラインに戻して「再試行」で描ける
8. 期近つなぎ（#101）: 開いた直後の限月は「0」。「期近つなぎ」を選ぶと要求は `n=roll`。日足で左へ動かし続けると 2018-10 まで描け（乗り換えの印がおおむね月に 1 つずつ並ぶ。1 時間足に限月が欠けた区間（2022・2023 年の一部）では 2 か月分飛んで 1 つになる）、2018-10-19 より前で読み込みが止まる。期近つなぎのまま足を 1分・5分・15分・1時間・日足に変えても選択が保たれる。限月を 0 / 1 に戻すと印が消え、凡例に限月が出ない
9. 期近つなぎの乗り換え（1 時間足）: 2026-09-30（水）の日中の最後の足（15 時台）までが 10 月限、夜間（値のある最初の足）から 11 月限で「11月限」の印がある。2025-12-26（金）の日中までが 1 月限、その夜間から 2 月限。カーソルを乗り換えの前後の足に当てると凡例の限月（例: `11月限（最終取引日 2026-11-12）`）が変わる。前後の足の OHLC（3 本以上）が、`atm_iv` を `started_at` の 1 時間の範囲で読んだ凡例の限月の行の値と一致する（下のクエリ。値の要約だけを記録し、結果は貼らない）。スマートフォン幅（390px）で横にはみ出さない（`document.documentElement.scrollWidth <= window.innerWidth`）

```sh
# 期近つなぎの確認（2026-09-30 15:00〜20:00 JST = 06:00〜11:00 UTC。2025-12-26 も同じ形）。月パーティションなので数 MiB
bq query --use_legacy_sql=false --maximum_bytes_billed=10737418240 --format=pretty \
  "SELECT last_trading_day, started_at, open, high, low, close FROM \`$P.optionchan_tokyo.atm_iv\`
   WHERE started_at >= TIMESTAMP '2026-09-30 06:00:00+00' AND started_at < TIMESTAMP '2026-09-30 11:00:00+00'
   ORDER BY started_at, last_trading_day"
```

`/smile`（#79）は開発者ツールのネットワーク（`smile_snapshot` で絞る）を開いたまま、取引時間中（日中 9〜15 時台・夜間 18〜翌 4 時台）に次を見る（取引時間外は気配が無く「気配がありません（取引時間外）」と約定の IV の点だけになる。不具合ではない。前後のステップで取引時間中の時点に戻して見る）:

1. 開いた直後: チャートが薄くなりスピナーが出て、描けたら消える。要求は `/smile_snapshot`（`at` なし）1 本。見出しにスナップショットの時刻（JST）、ATM の縦線（1 限月は実線、2 限月は点線）、初期の横の範囲は 1 限月の ATM ±10%
2. カーソル（スマートフォンはタップ）の行使価格の値（限月 × call / put ごとの mid / bid / ask の IV、約定の IV・価格・約定時刻、気配の価格）がチャートの上に出る
3. ホイール・ドラッグ（スマートフォンはピンチ・スワイプ）で拡大縮小・スクロールしても要求は出ない。「表示をリセット」で初期の範囲に戻る
4. 「−1時間」で `at` = 表示中の時刻 − 3600 の要求が 1 本出て見出しの時刻が変わる。続けて素早く 3 回押すと要求は 1 本（前の要求はネットワークで `(canceled)`）で、描かれるのは最後の時点。「−1日」で前の日の同じ時刻ごろ（週末・休業日をまたぐと金曜の最後 = 土曜 06:00 JST ごろ）
5. 「この時点を固定」→「−1時間」で、凡例に「表示」と「固定」の 2 つの時刻が出て、固定した曲線が破線で重なる。「固定を解除」・一度読んだ時点への移動（「+1時間」で戻る等）・1限月 / 2限月の切り替え・「OTM合成 / call・put別」の切り替えでは要求が出ない
6. この画面で読み込めるのは 20 時点まで（超えると「この画面で読み込めるのは 20 時点までです」と出て要求しない。ページを読み直すとリセット）
7. 失敗の表示（任意）: ネットワークを「オフライン」にして時点を動かすと「データを取得できませんでした (network)」と「再試行」が出る。オンラインに戻して「再試行」で描ける

`/smile_data`（旧ビューアーとの見比べ #54 で使う）は #79 の後も同じ形式の CSV を返す（`/smile` は使わなくなった）。

開発者ツールのコンソールに CSP の違反（`Refused to …`）が出ないこと。新しいビューアーは新系（`optionchan_tokyo`）の T0 以降のデータだけを表示する（過去分の移行 #12 の前は短い。不具合ではない）。

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

Cloud Run は最小 0 インスタンス・リクエスト中だけ CPU で、利用者 1 人ならほぼ 0 円。IAP（Cloud Run への直接設定）は追加費用なし。BigQuery は 1 ページの表示で 1〜3 クエリ（各 `maximum_bytes_billed` 1 GiB、最小課金 10 MiB）。`/atm_iv`（#77）は表示範囲に応じて `/atm_iv_bars?tf=&n=&start=&end=` を 1 回分ずつ読む。1 回のクエリのスキャン量の見積もり（option_price の生データは 1 日 約 90 MiB を読む。#96 で丸 1 日のパーティション（2026-10-13〜15）を dry run で測り直し、約定 IV の分足の列で 86.6〜89.7 MiB、mid IV の分足の列（bid_iv・ask_iv）で 98.2〜101.3 MiB）:

| 要求 | 読むもの | 1 回の上限（サーバーの検査） | 1 回のスキャン量の見積もり |
| --- | --- | --- | --- |
| 分足 1m・5m・15m（画面は 1m だけを読み、5m・15m は画面で束ねる） | option_price_latest の [start, end) | 3 日（日の境目に揃えないと 4 日分のパーティション）。start は now の 31 日前の UTC の 0 時まで | 3 日分 約 270 MiB、最大 約 360 MiB（1 クエリ 512 MiB 以下） |
| 分足の mid IV（`&iv=mid`、#93。分足だけ） | 同上（`iv` の代わりに `bid_iv`・`ask_iv`） | 同上 | 3 日分 約 300 MiB、最大 約 405 MiB（1 クエリ 512 MiB 以下） |
| 分足の既定（最初） | 最新のスナップショットの前日の UTC の 0 時〜now（+ ingest_runs の 14 日） | — | 約 90〜180 MiB |
| 1h・1d（最新を含む範囲） | 1 時間足 atm_iv + まだ 1 時間足になっていない分（最大 3 日）の生データ | 1h 100 日・1d 400 日 | 約 100 MiB（補う分。通常は 1 日分。最大 3 日 約 300 MiB） |
| 1h・1d（過去だけの範囲 = 左への追加） | 1 時間足 atm_iv だけ（生データは読まない） | 同上 | 数 MiB 未満（最小課金 10 MiB × 2〜3 クエリ） |
| 1h・1d の mid IV（`&iv=mid`、#98） | 上の 2 行と同じ（1 時間足は `atm_mid_iv`、補う分は `iv`・`price` の代わりに `bid_iv`・`ask_iv`） | 同上 | 上の 2 行と同じ程度（補う分は 1 日 約 100 MiB） |
| 期近つなぎ `n=roll`（#101。分足・1h・1d） | 上の各行と同じもの（限月で絞らない全限月の足。読む列・期間は同じ）+ 取引カレンダーの休業日（読んだ範囲の前日〜40 日先。数 KiB） | 上の各行と同じ | 上の各行と同じ（2026-10-16 の dry run で分足 3 日 127.7 MiB・mid 149.4 MiB・1d の最大 139.8 MiB・過去だけ 0.4 MiB。n=0 と同値）+ 休業日の最小課金 10 MiB。行が無い範囲では休業日を読まない。日足で 2018-10 まで遡っても 1 ページ 0.5 GiB 未満 |
| `/smile_snapshot?at=`（#79。`/smile` の 1 時点） | latest_runs の [at − 14 日, at] でスナップショットを探し（約 0.1 MiB、最小課金 10 MiB）、option_price_latest のその 1 秒を読む | 1 時点（at は探す時点を動かすだけ） | 約 120 MiB（1 日分のパーティション。2026-10-13 の最後のスナップショットの dry run で 119.9 MiB）+ 10 MiB |
| `/smile_snapshot?at=&dir=next`（#80。`/smile` の「+」方向） | latest_runs の [at, at + 14 日) で最初のスナップショットを探す（無ければ上と同じく at 以前で最新を探す）。あとは上と同じ | 同上 | 上 + 10 MiB（最新より後のときは + 20 MiB） |

取りすぎ防止（画面）: 同時に読むのは 1 本、連続するスクロール・拡縮は 250 ms まとめる、足・限月ごとに読んだ範囲を覚えて読み直さない。足・限月ごとに覚える日数は分足 9 日・1 時間足 400 日、分足をこの画面で読む日数の合計は 24 日（約 2 GiB）まで。データの無い範囲が 3 回続いたらそれより前は無いとみなす。1 ページを開いて分足を上限まで動かすと 約 2 GiB、1 時間足・日足だけなら 数十〜数百 MiB。1 日の量は #13 の 13c で `INFORMATION_SCHEMA.JOBS` の `sa-viewer` の `total_bytes_billed` で見る（目安 8 GiB/日 以下）。画面側の上限（分足 24 日分など）は 1 ページの中だけのもので、ページを読み直すとリセットされる。

1 日の上限（サーバー、#95）: ビューアーのプロセスが JST の 1 日に BigQuery で課金されたバイト数（ジョブの `total_bytes_billed`）を数え、環境変数 `DAILY_BYTES_BILLED`（既定 4 GiB）に達したら、それ以降の API（`/smile_data`・`/smile_snapshot`・`/atm_iv_data`・`/atm_iv_bars`）は BigQuery を呼ばずに 429 を返す（ログは `daily_limit`。画面は「今日の読み込みの上限に達しました」）。JST の 0 時に数え直す。上限はクエリを出す前に見るので、超えた後の 1 本（最大 `maximum_bytes_billed`）までは通る。数えるのはインスタンスごとで、ほかのインスタンスの分・止まる前の分は数えない（最大 2 インスタンスで 1 日 最大 約 8 GiB + 2 本。インスタンスが止まると 0 から数え直す）。プロジェクト全体の合計（`INFORMATION_SCHEMA`・`jobs.list`）は `sa-viewer` に `bigquery.jobs.list` が無い（`jobUser` だけ）ので使わない。読み込みが続く間はインスタンスは止まらないので、画面の不具合などによる繰り返しの読み込みは止まり、プロジェクトのクエリ量上限（「コスト」。2026-10-18 時点で 100 GiB/日）を取り込み・定期クエリに残す。2026-10 の `sa-viewer` の利用は 1 日 最大 0.6 GiB。上限を変えるときは `infra/prod/viewer.tf` の env に `DAILY_BYTES_BILLED`（バイト数。100 GiB 以下）を足す。429 が出ていないかは `gcloud logging read 'resource.type="cloud_run_revision" AND resource.labels.service_name="viewer" AND jsonPayload.event="daily_limit"' --project=$P --freshness=1d --limit=5 --format='value(timestamp,jsonPayload.endpoint)'`。

`/smile`（#79）: 拡大縮小・スクロール・表示の切り替え・比較用の固定ではサーバーに要求しない。時点の移動は 250 ms まとめて 1 本、前の要求は中断（BigQuery のジョブは止まらないので、中断・再試行も要求の数に数える）、一度読んだ時点は画面で覚えて読み直さない（「最新」は 60 秒まで）。1 ページで出す要求は 20 本まで（20 × 約 130 MiB = 約 2.6 GiB）。「+」方向（+1分〜+1日）は「その時刻以後で最初のスナップショット」を読む（`&dir=next`、#80）ので、取引の切れ目（15:45〜17:00・06:00〜08:45・週末・休業日）の手前から「+1分」を押すと、1 本で次の取引の最初の時点に移る。それより新しいものが無ければ最新を見せ、「これより新しいデータはありません」を出す。`/atm_iv` と `/smile` を両方上限まで使うと 1 ページずつで 約 4.6 GiB になり、何度も読み直すと目安の 8 GiB/日 を超えうる（画面の上限はページを読み直すとリセット。サーバーの 1 日の上限は上の段落）。

#77・#79 のデプロイ後（ブラウザで上の `/atm_iv`・`/smile` の確認を行った後）に、1 クエリの最大と 1 日の合計を確かめる（値・件数だけを記録する。ジョブの一覧は貼らない。確認した時間帯だけを見るときは `creation_time` の下限をその時刻にする）。あわせてログの `query_failed` が 0 件であること（`gcloud logging read 'resource.type="cloud_run_revision" AND resource.labels.service_name="viewer" AND jsonPayload.event="query_failed"' --project=$P --freshness=1h --limit=20 --format='value(timestamp,jsonPayload.endpoint,jsonPayload.error_type)'`）:

```sh
bq query --project_id=$P --location=$L --use_legacy_sql=false \
  'SELECT ROUND(MAX(total_bytes_billed)/POW(2,20)) AS max_mib, ROUND(SUM(total_bytes_billed)/POW(2,20)) AS sum_mib, COUNT(*) AS n
   FROM `region-asia-northeast1`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
   WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY) AND user_email LIKE "sa-viewer@%"'
# max_mib <= 512（分足の 1 回・/smile_snapshot の 1 回 約 120 MiB）、sum_mib は上の見積もりの範囲（/atm_iv は 1 ページ 約 2 GiB、/smile は 約 2.6 GiB 以下）
```

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
WDIR=<スクラッチのディレクトリ>   # 停止前後の記録を置く（リポジトリ・Issue に貼らない）
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

1. 停止前の記録: `legacy_state > $WDIR/legacy-before.txt; curl -s -o /dev/null -w '%{http_code}\n' "https://$(gcloud app describe --project=$P --format='value(defaultHostname)')/" > $WDIR/app-before.txt`（旧 URL は IAP のログインへのリダイレクトで 302 のはず。scheduler 2 行が `ENABLED`、invoker 3 行が `["allUsers"]`、app が `SERVING`、アラート 4 本、DTS 5 本がすべて `disabled=false`）
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
9. 停止後の記録: `legacy_state > $WDIR/legacy-after.txt; diff $WDIR/legacy-before.txt $WDIR/legacy-after.txt`。差分が次のとおりであること

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
    curl -s -o /dev/null -w '%{http_code}\n' "https://$(gcloud app describe --project=$P --format='value(defaultHostname)')/"; cat $WDIR/app-before.txt
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

新しい取り込み（`apps/ingest`）が時間外・休場日の判定に使う休業日の一覧を、JPX「祝日取引」の祝日取引実施日の表（https://www.jpx.co.jp/derivatives/rules/holidaytrading/index.html ）から**自動で**取り込む。JPX は毎年 2 月に翌年の予定を、6 月と 12 月に翌年の上半期・下半期の確定を公表する。休業日の正は BigQuery の版（`optionchan_tokyo.trading_calendar`、最新版はビュー `trading_calendar_latest`）だけで、ingest・シミュレーターともビューを読む（#75。ファイルの写しは持たない）。立会時間（日中 8:45〜15:45、夜間 17:00〜翌 6:00）・余裕 30 分・`no_night_session_before`（通常は空）は ingest のコードの定数（`apps/ingest/src/optionchan_ingest/settings.py` の `SESSION_*`・`NO_NIGHT_SESSION_BEFORE`）。

- **取り込み**: 毎日 06:40 JST の ingest のリクエスト（時間外。Scheduler は足さない）で、通常の処理の後に同じ期限の残りで行う（`apps/ingest/src/optionchan_ingest/calendar_sync.py`）。ページを取得・解析し、内容が最新版と同じなら INFO `calendar_checked` で終わる。変わっていれば内閣府の祝日 CSV（https://www8.cao.go.jp/chosei/shukujitsu/syukujitsu.csv ）も取得して検証し、通れば `optionchan_tokyo.trading_calendar` に新しい版を追記して NOTICE `calendar_updated`（今日以降の休業日の追加 `added_closed`・削除 `removed_closed`、`covered_until` の新旧）を出す
- **ingest の読み方**: インスタンスの最初のリクエストと毎時 0 分にビュー `trading_calendar_latest` を読む。一度読めた後に読めない・版が無いなら、直前に読めた版のまま（WARNING `calendar_load_failed`）
- **起動直後に読めないとき**: 一度も読めていなければ休業日なし（土日以外はすべて取引日）として取り込み、読み直すたびに ERROR `calendar_unavailable`（M2）を出す（読めなかったときも、読めたが下の検査で使わなかったときも毎回。拒否の理由は `detail` の `rejected:` の後）。5 分後（`CALENDAR_RETRY_SECONDS`）以降の次のリクエストで読み直す（毎時 0 分を待たない）。この間は休業日が分からないので、期限切れと同じく鮮度（`stale`・`jpx_unavailable_stale`）と期限（`calendar_expiring` / `calendar_outdated`）の判定をしない。休業日にも JPX を取りに行くだけで、取引日の取りこぼしは無い
- **ingest 側の検査（多層防御）**: 次の版は使わずに今の値のまま、ERROR `calendar_load_rejected`（同じ理由では 1 回。版がまだ無い間は拒否のたびに `calendar_unavailable` も出る）: `content_hash` が再計算と合わない（行の欠け・同じ `version_at` の重複・書き換え）、その他の不正な値、今日から 14 日以内に直前に読めた版に無い休業日が増える（`accepted = override` の版は確認済みなので見ない。プロセス内に直前の版が無い起動直後は、BigQuery の 1 つ前の版（テーブル `trading_calendar` の `version_at` が読んだ版の 1 つ前。`sa-ingest` は `optionchan_tokyo` の `dataEditor` で読める）と比べるので、新しいインスタンスでも同じ判定になる。1 つ前の版が無い（版が 1 つだけ）なら比べない。1 つ前の版を読めない（失敗・3 秒の期限）ときは比べずに WARNING `calendar_previous_unavailable`。「休業日なし」とは比べない: 近い休業日を持つ正しい版をすべて拒んでしまうため）、1 年の休業平日が 25 を超える
- **期限**: 版の `covered_until`（確定の最終日）の 14 日前から 09:00 に WARNING `calendar_expiring`、過ぎたら ERROR `calendar_outdated`（版を読めていない間は出さない。`calendar_unavailable` が出る）。自動化の後は「確定の公表を取り込めていない」ことの最後の検知になる。過ぎても取り込みは止めない（書いていない平日は取引日として扱う。期限切れの間は鮮度判定をしない）
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
| `past_changed` | 過去日の休業/取引日が変わる（前の版の掲載範囲より前に足した過去年の行（`source = history`、#67）は数えない） | 可 |
| `covered_until_regressed` / `listed_until_regressed` | 確定の最終日・掲載範囲が後退する | 可 |
| `confirmed_changed` | 今日以降の確定の日が消える・予定に戻る・実施有無が変わる | 可 |
| `too_many_changes` | 前の版の掲載範囲の中で、今日以降の休業日の追加・削除が 4 件以上（翌年分の追加は数えない） | 可 |
| `closes_soon` | 今日から 14 日以内の日が新しく休業日になる（誤りなら取引日を取りこぼす。取引日に変わる向きは受け入れる。ingest 側の検査と同じ日数） | 可 |

- 版が無いとき（初回、または `--accept` で壊れた前の版を無視したとき）は差分の基準が無いので、上の表の「可」の検証（前の版との差分）はせず、形・祝日・年末年始の検証（「不可」の行）だけで受け入れる。`calendar_updated` の `added_closed` には今日以降の休業日がすべて出る
- 取引時間の表が `settings.py` の `SESSION_*` と違えば ERROR `calendar_sessions_changed`（休業日の更新は止めない。取引時間は自動で変えない）
- 1/1 は表に載らない（注1）ので、平日なら規則で休業の行を足す（`source = rule`）。表から消えた過去年の行は前の版から引き継ぐ
- 過去年の行（`--backfill-from`、#67）は、xlsx と祝日 CSV の照合（下の「過去年の追加」）に通らなければ書かない（`calendar_sync_failed`、`stage=parse_history`。`--accept` でも越えない）

### デプロイ後（初回）

apply の後、手動のジョブで最初の版を書く（翌朝 06:40 を待ってもよい）:

```sh
gcloud run jobs execute calendar-sync --region $L --project $P --wait --args=--dry-run
gcloud run jobs execute calendar-sync --region $L --project $P --wait
bq query --location=$L --use_legacy_sql=false --format=pretty '
SELECT calendar_date, is_trading_day, status, day_type, name, covered_until, version_at
FROM `optionchan_tokyo.trading_calendar_latest` WHERE NOT is_trading_day ORDER BY calendar_date'
# 今日以降の休業日・covered_until が JPX「祝日取引」のページと一致すること
gcloud logging read 'resource.type="cloud_run_revision" AND resource.labels.service_name="ingest" AND jsonPayload.event=("calendar_loaded" OR "calendar_load_failed" OR "calendar_load_rejected")' \
  --project $P --freshness=2h --limit 5 --format='value(timestamp,jsonPayload.event,jsonPayload.version_at,jsonPayload.detail)'
```

### 過去年の追加（#67）

取り込み開始時の版は JPX の表の 2 年分（2026〜2027 年）だけ。過去データの `price_time` の補正（#4、`sql/price_time_fix/`）とシミュレーターの過去の SQ 日・取引最終日の繰り上げのために、旧データの最初の年（2018 年）からの行を**一度だけ**足す。

- **出典**: 内閣府の祝日 CSV（1955 年〜）と、「祝日取引」のページからリンクされた「祝日取引実施日（確定）一覧」（xlsx。1 枚のシートに 祝日取引の対象日・名称・実施有無。祝日取引の開始日 2022-09-23 から確定の日まで。1/1 は載らない）。xlsx の URL はページのリンクから取る
- **行**: 平日の国民の祝日・休日と年末年始の 12/31・1/1・1/2・1/3（`source = history`、`status = confirmed`）。祝日取引の開始より前はすべて休業（`note` = 祝日取引の開始前）、開始以後は xlsx の実施有無と名称（1/1 は休業）。大阪取引所のデリバティブの終日の臨時休場は 2018 年以降に無い（2020-10-01 の東証の売買停止は現物だけ。旧系の先物の約定時刻もこの日の日中にある）ので、臨時休場の行は無い
- **照合（どれかに当たれば書かない）**: xlsx の最初の日が 2022-09-23、開始以後の CSV の平日の祝日・年末年始がすべて xlsx にある、xlsx に CSV・年末年始に無い日（土日を含む）が無い、12/31・1/2 が「実施しない」、xlsx に 1/1 が無い、範囲の年がすべて CSV にある、xlsx と今の版（掲載範囲の確定の行）の実施有無が同じ
- **検証との関係**: 足せるのは前の版の掲載範囲（`MIN(calendar_date)` の年の 1/1）より前の行だけで、既存の行は変えない。その追加は `past_changed` に数えないので `--accept` は要らない（他の検証は通常どおり）。以後の毎日の取り込みは前の版の過去年の行を引き継ぐ（内容は変わらず `calendar_checked`）
- **ingest への影響**: ingest は今日以降の判定にしか使わないので変わらない。過去年の 1 年の休業平日は最大 20（2019 年）で、ingest 側の検査（25 超）に当たらない。14 日以内の増加にも当たらない（過去日だけ）
- **前提**: `source = history` を読めるイメージがデプロイ済みであること（古いイメージは未知の `source` の版を不正として使わず `calendar_load_rejected` になり、06:40 の取り込みも `calendar_sync_failed`（`stage=load`）になる）

```sh
# 1) 結果を見る（書かない）。history_rows（2018〜2025 年は 136 行）・history_closed（98）・history_days（日付 休業/祝日取引 名称）・problems が無いこと
gcloud run jobs execute calendar-sync --region $L --project $P --wait --args=--dry-run,--backfill-from=2018-01-01
gcloud logging read 'resource.labels.job_name="calendar-sync" AND jsonPayload.event:"calendar_"' \
  --project $P --freshness=1h --limit 5 --format=json \
  | jq -c '.[] | {t: .timestamp, e: .jsonPayload.event, stage: .jsonPayload.stage, detail: .jsonPayload.detail, problems: .jsonPayload.problems, rows: .jsonPayload.history_rows, closed: .jsonPayload.history_closed, from: .jsonPayload.history_from, until: .jsonPayload.history_until}'
# 2) 書く（1 版を追記。NOTICE calendar_updated の history_rows）
gcloud run jobs execute calendar-sync --region $L --project $P --wait --args=--backfill-from=2018-01-01
# 3) 確認: 掲載範囲の開始が 2018-01-01、年ごとの行数、2023-08 限の SQ 日の繰り上げ（8/11 が行にある）
bq query --location=$L --use_legacy_sql=false --format=pretty '
SELECT EXTRACT(YEAR FROM calendar_date) y, COUNT(*) n, COUNTIF(NOT is_trading_day) closed, STRING_AGG(DISTINCT source) sources
FROM `optionchan_tokyo.trading_calendar_latest` GROUP BY y ORDER BY y'
bq query --location=$L --use_legacy_sql=false --format=pretty '
SELECT calendar_date, is_trading_day, name, source FROM `optionchan_tokyo.trading_calendar_latest`
WHERE calendar_date BETWEEN "2023-08-01" AND "2023-08-31"'
gcloud logging read 'resource.type="cloud_run_revision" AND resource.labels.service_name="ingest" AND jsonPayload.event=("calendar_loaded" OR "calendar_load_rejected")' \
  --project $P --freshness=2h --limit 3 --format='value(timestamp,jsonPayload.event,jsonPayload.version_at,jsonPayload.detail)'
```

期待値（2018〜2025 年）: 休業 2018 年 16・2019 年 20・2020 年 19・2021 年 16・2022 年 12・2023 年 3・2024 年 7・2025 年 5、2023-08-11 は `is_trading_day = true`（山の日の祝日取引）。2026 年以降の行・`covered_until`・`listed_until` は前の版と同じ。間違えて書いたときは、壊れた版と同じ手順で消すか（「版・ビューが壊れたとき」の DELETE。書いてから 30 分ほどは DML で消せない）、正しい内容の版を `--accept` で書き直す（過去年の行の削除・変更は `past_changed`）。

### ERROR を受けたとき

| event | 対応 |
|---|---|
| `calendar_sync_failed`（`stage`） | `load`: ビューを読めない（権限・BigQuery の障害）。`fetch_page` / `fetch_holidays`: JPX・内閣府のサイトの障害（翌日に再試行される。続くなら URL の変更を疑う）。`parse` / `parse_holidays`: 形の変更。ページを開いて `jpx_calendar.py` の解析を直す PR（fixture `apps/ingest/tests/fixtures/holiday_trading.html` も必要部分だけ差し替える）。`fetch_history` / `parse_history`: `--backfill-from` の xlsx の取得・照合の失敗（`detail` を見て xlsx を開いて確かめる）。`write`: 書き込みの失敗（翌日に再試行）。`deadline`: 期限切れ（翌日に再試行。急ぐなら手動のジョブ）。`unexpected`: `detail` を見て Issue にする |
| `calendar_sync_rejected` | `problems` を読み、JPX のページと見比べる。ページが正しく（例: BCP テストで近い祝日を休みにする公表）、`overridable` が true なら、`--dry-run` で `added_closed` / `removed_closed` を確かめてから `--accept` で受け入れる。false ならページかパーサーの問題（パーサーを直すか、ページが直るのを待つ）。受け入れるまで ingest は前のカレンダーのまま（新しい休業日にも JPX を取りに行くだけで、取りこぼしは無い） |
| `calendar_sessions_changed` | 取引時間の変更。JPX の公表を確かめ、`apps/ingest/src/optionchan_ingest/settings.py` の `SESSION_*` と鮮度のしきい値・Scheduler 前提（target.md 3.1）を見直す PR |
| `calendar_unavailable` | ingest が BigQuery の版を一度も使えていない（休業日なしで判定中）。`stage`・`detail` を見る（権限・ビューの有無・BigQuery の障害。`版が無い` なら上の「デプロイ後（初回）」）。`detail` が `rejected:` で始まれば版を読めたが使わなかった（理由は続きと最初の `calendar_load_rejected`。対応は下の `calendar_load_rejected` の行）。読めるようになれば INFO `calendar_loaded` が出て止まる |
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

ジョブの終了コード: 0 = 更新・変化なし・dry-run、1 = 失敗・拒否、2 = 引数の誤り。間違った版を受け入れたときは、正しいページで `--accept` を付けて取り込み直す（新しい版が最新版になる。古い版は履歴として残る）。ページが直らないまま版を直す必要があるときも、ファイルではなく BigQuery の版で直す（JPX のページが正しくなるのを待って `--accept` で取り込み直す。待てないときは正しい内容の版を作る PR（パーサー・検証の修正）を出す）。

### 版・ビューが壊れたとき

- **同じ `version_at` の重複・行の欠け**（`calendar_load_rejected` の `detail` が `data: content_hash が合わない` など。手動のジョブと 06:40 が同時に書いた等）: ingest は直前に読めた値（起動直後なら休業日なし）のままなので急がない。新しい版を書けば最新版が置き換わる。取り込みも前の版を読めずに `calendar_sync_failed`（`stage=load`）になるので、`--accept` を付けて手動のジョブを実行する（壊れた前の版を無視し、版が無いときと同じく差分の検証をせずに新しい版を書く。WARNING `calendar_previous_ignored`）:

```sh
gcloud run jobs execute calendar-sync --region $L --project $P --wait --args=--dry-run,--accept=壊れた版の復旧
gcloud run jobs execute calendar-sync --region $L --project $P --wait --args=--accept=壊れた版の復旧
# 壊れた版を履歴から消すとき（任意。書いてから 30 分ほどは DML で消せないことがある）
bq query --location=$L --use_legacy_sql=false 'SELECT version_at, COUNT(*) rows, COUNT(DISTINCT content_hash) hashes FROM `optionchan_tokyo.trading_calendar` GROUP BY version_at ORDER BY version_at DESC LIMIT 5'
bq query --location=$L --use_legacy_sql=false 'DELETE FROM `optionchan_tokyo.trading_calendar` WHERE version_at = TIMESTAMP("<壊れた版の version_at>")'
```

- **ビューが消えた・定義が変わった**: ingest・取り込みは `calendar_load_failed`（起動直後のインスタンスは `calendar_unavailable`）/ `calendar_sync_failed`（`stage=load`）になり、直前に読めた値（起動直後なら休業日なし）で動き続ける。ビューは Terraform（`infra/prod/calendar.tf`）が持つので、deploy workflow を再実行するか、次の PR の apply で作り直される（plan に `trading_calendar_latest` の create / update が出る）。テーブル `trading_calendar` が消えたときはタイムトラベル（7 日）で戻す（`bq cp optionchan_tokyo.trading_calendar@<ミリ秒> optionchan_tokyo.trading_calendar`）か、apply で作り直して手動のジョブで最初の版を書く

### TOML と #66 の仕組みの撤去（#75、一度だけ）

#75 で `config/trading_calendar.toml` と、それを BigQuery の版に合わせる仕組み（#66 の workflow `calendar-toml`・`sa-calendar-toml`）を廃止した。GitHub と bootstrap の後片付けは所有者が手で行う。**順序**: prod の権限（`calendar_toml_reader`）を先に消し、その後で bootstrap の SA を消す（逆にすると、prod の付与が削除済みの SA を指したまま残り、prod の plan / apply がずれる）。

1. #75 の PR をマージし、deploy の `apply` を承認する（prod: `google_bigquery_table_iam_member.calendar_toml_reader` 2 件の destroy と、ingest のイメージの更新）
2. bootstrap を apply する（`sa-calendar-toml`・その `bigquery.jobUser`・WIF の借用の destroy。手順は「Terraform」の「bootstrap の初回 apply」の 2〜4。要約の目安: destroy 3（SA 1・プロジェクト IAM 1・SA IAM 1）、create / update / replace 0）。SA は `deletion_policy = "PREVENT"` なので state から外すだけで GCP には残る。消すなら gcloud で（`gcloud iam service-accounts delete sa-calendar-toml@$P.iam.gserviceaccount.com --project $P`。付与は apply で消えている）
3. GitHub の設定を消す（あるものだけ。値は表示しない）:

```sh
gh variable delete GCP_SA_CALENDAR_TOML -R "$REPO"
gh variable delete CALENDAR_APP_CLIENT_ID -R "$REPO"
gh api -X DELETE $R/environments/calendar   # secret CALENDAR_APP_PRIVATE_KEY ごと消える
# 確認（すべて true）
gh variable list -R "$REPO" --json name -q '[.[].name] | (index("GCP_SA_CALENDAR_TOML") == null) and (index("CALENDAR_APP_CLIENT_ID") == null)'
gh api $R/environments --jq '[.environments[].name] | index("calendar") == null'
```

4. GitHub App を作っていたら、自分のアカウントの Settings → Developer settings → GitHub Apps で削除する（インストールも消える）。ブランチ `bot/trading-calendar` が残っていれば消す

- `no_night_session_before`（直前の夜間セッションが無い取引日）は JPX の表に無いので、公表されたら `settings.py` の `NO_NIGHT_SESSION_BEFORE` に足す PR を出す（通常は空）

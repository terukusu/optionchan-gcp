# 用語集

このリポジトリの文書・コード・表に出る用語の短い説明。詳しくは各項のリンク先。市場の用語は一般的な意味を短く書き、このリポジトリでの扱い（列・規則）を添える。

## 市場の用語

- **限月（げんげつ）**: 先物・オプションの満期の月。オプションの表では `last_trading_day`（最終取引日）、先物の表では `contract_month` で区別する。取り込んでいるのはオプションの 2 限月（[data.md](data.md#主な表の列)）
- **期近（きぢか）**: 満期が最も近い限月。2 番目は期先（このリポジトリでは「2 限月」と呼ぶ）
- **1 限月・2 限月**: SQ 前の限月を最終取引日の順に数えたもの（`atm_cm_iv` の `m1_*`・`m2_*`）。JPX のページも 1 限月・2 限月の 2 ページを取得する
- **限月 0 / 1**: ビューアーの `/atm_iv` の限月の選択肢（`n=0`・`n=1`）。今日から見て何番目の限月か（0 が期近）。過去の日付へ移動すると、その限月の足は無い（[viewer.md](viewer.md)）
- **最終取引日**: その限月を取引できる最後の日。`option_price.last_trading_day`
- **SQ（特別清算指数）**: 満期の清算の値。このリポジトリでは、SQ の日 = 最終取引日の次の営業日（土日と取引カレンダーの休業日を飛ばす）、満期の時刻 = その日の 09:00 JST として日数を数える（`sql/cm/atm_cm.sql`、[data.md](data.md#30-日固定の-atm-iv-の計算)）
- **行使価格**: オプションを行使して原資産を売買する価格。`option_price.target_price`
- **ATM（アット・ザ・マネー）**: 行使価格が原資産の価格に最も近いこと（その行使価格）。このリポジトリでは JPX のページの印（`option_price.is_atm`）を使う。原資産が動くと隣の行使価格に移る
- **OTM（アウト・オブ・ザ・マネー）**: 今行使しても得にならない側。put は行使価格が原資産より下、call は上。スマイルカーブの「OTM 合成」は ATM より下を put、ATM 以上を call で描く（[viewer.md](viewer.md)）
- **call / put**: 買う権利 / 売る権利のオプション。`option_price.type` は 1 = call、2 = put（DataCube の歩み値の `put_call_type` は 1 = put で逆）
- **IV（インプライド・ボラティリティ）**: オプションの価格から逆算した、原資産の予想変動率。このリポジトリには種類が 3 つある（[data.md](data.md#iv-の種類)）
- **約定 IV**: 約定値の IV（`option_price.iv`）
- **mid IV**: 気配の中値の IV =（`bid_iv` + `ask_iv`）/ 2。両方 0 より大きいときだけ（IV の 0 は欠損として扱う）
- **bid / ask**: 買い気配 / 売り気配（`bid`・`ask`。その IV が `bid_iv`・`ask_iv`）。差がスプレッド
- **スマイルカーブ**: ある時点・ある限月の、行使価格ごとの IV を結んだ曲線。ビューアーの `/smile`（[viewer.md](viewer.md)）
- **フォワード**: 満期の原資産の理論価格。このリポジトリでは put-call パリティ（C − P + K の中央値）で限月ごとに出す（`atm_cm_iv.m1_forward`・`m2_forward`、[data.md](data.md#30-日固定の-atm-iv-の計算)）
- **日中 / 夜間**: 立会の時間帯。日中 8:45〜15:45、夜間 17:00〜翌 6:00（JST。`apps/ingest/src/optionchan_ingest/settings.py` の `SESSION_*`）
- **取引日**: 土日と取引カレンダーの休業日（`is_trading_day = false`）を除いた日。夜間は次の取引日に属する（[data.md](data.md#時刻)）
- **休業日**: 業務規程上の休業日に当たる平日（国民の祝日・休日と年末年始）。祝日取引を実施する日は休業日でも取引日になる（`trading_calendar.is_trading_day = true`）
- **祝日取引**: 休業日の一部で行う取引。実施日は JPX「祝日取引」のページの表で公表される（runbook の「取引カレンダー（#63）」）
- **歩み値**: 約定 1 件ずつの記録。このリポジトリでは所有者が購入した J-Quants DataCube のデータを過去の欠けの作り直しに使った（`dc_option_tick`・`dc_future_tick`、[data-gaps.md](data-gaps.md)）

## このリポジトリの用語

- **新系 / 旧系**: 新系は今の本番（`apps/ingest`・`apps/viewer`、東京の Cloud Run・BigQuery `optionchan_tokyo`、Terraform）。旧系は前の構成（`appengine/`・`functions/`・`functions_py/`、US の Cloud Functions・App Engine・データセット `optionchan`）で、並行稼働中。#14 で止め、#24 で消す（[README.md](../README.md)）
- **スナップショット**: 取り込みの 1 時点。1 限月のページの最終更新時刻で識別し、3 表の `created_at` と `ingest_runs.snapshot_at` がその時刻（[data.md](data.md#時刻)）
- **取り込み実行（`ingest_run_id`）**: スナップショット 1 つを解析して 3 表に書く 1 回の処理。ID は UUID4（旧系から移した行は `legacy-` で始まり、#114 で作った行は `legacy-fill114-` で始まる）。書き終わると `ingest_runs` に 1 行書く
- **最新版**: スナップショットごとに最新の完了した取り込み実行の行。同じスナップショットに実行が複数あるので、テーブル関数 `latest_runs`・`*_price_latest` で 1 つに選んで読む（[data.md](data.md#まず読むものテーブル関数)）
- **回収**: 生 HTML はあるのに完了記録が無いスナップショットを、毎時 0 分に生 HTML から読み直して追記すること（`trigger = recovery`）
- **照合**: 毎朝 07:00 に、前日分の生 HTML の数と完了記録の数を比べて未回収を数えること
- **再処理**: 期間を指定して生 HTML から作り直す手動のジョブ `reprocess`（`trigger = reprocess`。パーサーを直した後など）。回収・照合・再処理は runbook の「新しい取り込み（ingest）」
- **生 HTML**: JPX の 2 ページを取ったままの形で GCS に保存したもの（gzip した JSON）。取り込みの行はここから作り直せる
- **1 時間足 / 分足（足）**: ある時間の始値・高値・安値・終値（OHLC）の 1 本。1 時間足は表（`atm_iv`・`atm_mid_iv` など）にあり、分足（1・5・15 分）と日足はビューアーが作る（[data.md](data.md#主な表の列)、[viewer.md](viewer.md)）
- **期近つなぎ**: ビューアーの限月の選択肢（`n=roll`）。期近の ATM IV を、期近の最終取引日の 5 営業日前（`ROLL_BUSINESS_DAYS`、`apps/viewer/src/optionchan_viewer/settings.py`）の取引日から 2 限月へ乗り換えてつないだ長期の系列。乗り換えの所に段差が出る（[viewer.md](viewer.md)）
- **30 日固定の ATM IV**: 限月ごとにフォワードの位置で補間した ATM IV を、満期 30 日に補間した値（`atm_cm_iv.iv30`）。ATM の行使価格の乗り換え・限月の乗り換えの段差が無い。ビューアーの限月「30日固定」（`n=cm`）（[data.md](data.md#30-日固定の-atm-iv-の計算)）
- **取引カレンダー**: 休業日の一覧。JPX「祝日取引」の表から毎日自動で取り込み、版ごとに `trading_calendar` に追記する。最新版はビュー `trading_calendar_latest`。取り込み・ビューアー・30 日固定の計算が休業日の正として読む（[data.md](data.md#主な表の列)、runbook の「取引カレンダー（#63）」）
- **過去データの欠け**: 旧系の解析の不具合・停止などで行が無い・値が無い期間（[data-gaps.md](data-gaps.md)、表 `data_gaps`）
- **bootstrap / prod**: Terraform の 2 つの構成（`infra/bootstrap`・`infra/prod`）。bootstrap は tfstate・WIF・CI 用 SA・取り込みとビューアーの SA・生 HTML のバケット・IAP のアクセス許可などで、所有者が手で apply する。prod はそれ以外（BigQuery・Cloud Run・Scheduler・定期クエリ・監視など）で、CI が承認の後に apply する（[setup.md](setup.md)、runbook の「Terraform」）
- **所有者**: その GCP プロジェクト・リポジトリを持ち、手作業（bootstrap の apply・deploy の承認・手で流す SQL）を行う人。runbook の手順の多くは所有者の作業

## GCP の用語

- **定期クエリ（DTS）**: BigQuery Data Transfer Service のスケジュールされたクエリ。このリポジトリでは `atm-hourly`（1 時間足）・`atm-cm`（30 日固定）・`quality-daily`（品質チェック）と、旧系の並行稼働の間だけの `legacy-stage`・`legacy-copy`・`legacy-compare`（`infra/prod/sq.tf`）。実行 SA は `sa-sq-dts`
- **テーブル関数**: 引数を取って表を返す BigQuery のルーチン（table-valued function）。最新版の 4 つ（`latest_runs` など）は期間の引数が必須（[data.md](data.md#まず読むものテーブル関数)）
- **パーティション / クラスタ**: BigQuery の表を列の値（日・月）で分けること / 列の値で並べて保存すること。絞り込むと読む量（費用）が減る（[data.md](data.md#費用の注意)）
- **IAP（Identity-Aware Proxy）**: Google アカウントで認証し、許可した人だけを通す仕組み。ビューアー（Cloud Run `viewer`）はアプリに認証が無いので IAP の後ろに置く。許可は bootstrap の変数 `viewer_iap_members`（[viewer.md](viewer.md)、[setup.md](setup.md)）
- **WIF（Workload Identity Federation）**: 鍵を使わずに外部（GitHub Actions）から GCP の SA として認証する仕組み。CI の plan・apply が使う（`infra/bootstrap/wif.tf`、[development.md](development.md)）
- **SA（サービスアカウント）**: プログラムが使う GCP のアカウント。`sa-ingest`（取り込み）・`sa-viewer`（ビューアー）・`sa-scheduler`（Scheduler の呼び出し）・`sa-sq-dts`（定期クエリ）・CI 用の plan と apply の SA など。形は `sa-xxx@<PROJECT_ID>.iam.gserviceaccount.com`
- **Cloud Run サービス / ジョブ**: コンテナを動かす GCP のサービス。リクエストで動くサービス（`ingest`・`viewer`）と、手動で実行するジョブ（`reprocess`・`calendar-sync`）がある
- **Cloud Scheduler**: 決まった時刻に HTTP を呼ぶ GCP のサービス。Scheduler `ingest` が毎分 Cloud Run `ingest` を起動する

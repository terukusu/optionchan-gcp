# 新アーキテクチャ（リファクタリング後・設計案）

- 時点: 2026-10-12（設計レビュー #17 を反映した改訂版。決定事項は「9. 決定事項」、レビュー指摘の扱いは「10. 設計レビューの反映状況」）
- 対になる資料: [現行アーキテクチャ](current.md)（Before）
- 方針: **モダン化・IaC 化・オブザーバビリティ強化**。個人プロジェクトなので「月額を今と同程度（実請求 約 160 円）に抑え、無料枠を活かす」「運用の手間を減らす」を同じくらい重視する
- 規模: **最小構成を基準**にする。本文の「必須」が最小構成、「任意」は要否を個別に判断する追加候補。ただしセキュリティの必須項目は削らない

## 1. 設計方針

| # | 方針 | 現行の課題との対応 |
|---|---|---|
| P1 | **すべて Terraform で管理**し、デプロイは GitHub Actions から行う。ただし権限の根元（bootstrap）は分離してオーナーが手で apply する | IaC なし、ドリフト |
| P2 | **リージョンを 1 か所に集約**する。**東京（asia-northeast1）**に集約する（2026-10-12 決定、D2・2.1） | 3 リージョン分散 |
| P3 | 実行基盤は **Cloud Run** に統一し、**App Engine と第1世代 Cloud Functions をやめる** | 廃止ランタイム、第1世代 |
| P4 | 認証は **IAP と IAM に寄せ、共有パスワードを無くす**。`allUsers` 公開をやめる | #3、公開エンドポイント |
| P5 | サービスごとに **専用サービスアカウント（最小権限）**。ユーザー管理の SA 鍵は使わない（CI も Workload Identity 連携） | デフォルト SA（Editor）、SA 鍵 |
| P6 | 取り込みは **生データ（HTML）を保存してから解析**する。解析のバグを後から再処理で直せるようにする | #4（元の `MM/DD` が残っておらず過去データを正確に直せない） |
| P7 | 書き込み順を「生 HTML → データ → 完了記録（`ingest_runs`）」にし、**表示側は完了記録のあるデータだけを見る** | #5、部分書き込み |
| P8 | **監視を最初に入れる**。アラートは 1〜2 本に絞り（2027-09 から課金。公式 https://cloud.google.com/stackdriver/pricing で 2026-10-12 に確認。"no sooner than" 2027-09-01）、細かい検査はコードと定期クエリで行う | 監視なし、LINE Notify 終了 |
| P9 | 集計は関数ではなく **BigQuery の定期クエリ / ビュー**で行う | `insert_ohlc`（python37） |

## 2. システム構成図（After）

```mermaid
flowchart LR
    subgraph EXT["外部"]
        JPX["JPX 価格情報ページ"]
        USER["閲覧者"]
        GH["GitHub Actions"]
    end

    subgraph BOOT["bootstrap（オーナーが手で apply、prod の state 外）"]
        WIF["Workload Identity 連携"]
        TFP["sa-tf-plan（PR、読み取り）"]
        TFA["sa-tf-apply（Environment prod のみ）"]
        TFS[("GCS tfstate")]
    end

    subgraph PROD["optionchan-222710（asia-northeast1）"]
        SCH["Cloud Scheduler（毎分 1 ジョブ）"]
        ING["Cloud Run ingest（取り込み・回収、ingress internal、sa-ingest）"]
        RAW[("GCS raw 生 HTML（Standard、上書き禁止）")]
        BQ[("BigQuery 3 テーブル + ingest_runs")]
        SQ["定期クエリ（1 時間足・日次整合、sa-sq）"]
        JOB["Cloud Run ジョブ（手動の再処理のみ）"]
        IAP["IAP（Cloud Run に直接設定）"]
        VIEW["Cloud Run viewer（sa-viewer）"]
        AR[("Artifact Registry")]
        MON["Logging / Monitoring（アラート 1〜2 本）"]
    end

    MAIL["通知（メール）"]

    SCH -- "OIDC" --> ING
    ING -- "取得" --> JPX
    ING -- "(1) 生 HTML" --> RAW
    ING -- "(2) Storage Write API" --> BQ
    SQ --> BQ
    JOB -- "再処理" --> BQ
    USER --> IAP --> VIEW
    VIEW -- "パラメータ化 SELECT" --> BQ
    ING -.-> MON
    SQ -.-> MON
    MON --> MAIL

    GH -- "OIDC" --> WIF
    WIF --> TFP
    WIF --> TFA
    TFP -- "plan" --> TFS
    TFA -- "image push" --> AR
    TFA -- "apply" --> PROD
```

なくなるもの: App Engine、Cloud Functions 7 個、Pub/Sub 3 トピック、Datastore、GCS `optionchan`（中継）、共有パスワード（`auth.txt`）、`stackdriverLineNotify`。

### 2.1 リージョン: 東京案 / US 集約案（東京案に決定）

| | 東京案 | US 集約案 |
|---|---|---|
| Cloud Run・GCS raw | asia-northeast1 | US のリージョン（例: us-central1。BigQuery US マルチリージョンと同じ地域） |
| BigQuery | 新データセット `optionchan_tokyo` に過去データをコピー | 既存 `optionchan` を読み取り専用で残し、新データは同じ US の新データセット（例: `optionchan_v2`）へ。境界時刻 T0 で結合するビューで 1 つに見せる |
| データ移行（6 章 3a） | 必要（コピー・検証で約 1 週間、転送料 約 60 円） | **不要** |
| 期待される利点 | JPX への距離が近く、取得時間・課金時間が短い可能性 | 移行作業とリスクが小さい。`PaymentInfo`（課金エクスポート）も US にあり分散しない |
| 保存単価 | US より 1 割強高い | 現行どおり |

**2026-10-12 にオーナーの判断で東京案に決定した**（D2）。両リージョンを比べる判定スパイク（#19）は行わず、下の計測項目は東京だけで、フェーズ 2 の並行稼働（#11）の中で測る（Cloud Run の課金時間は 7 章の試算の確認に、JPX ページ更新時刻の挙動は鮮度のしきい値の調整に使う）。US 集約案の列と判定基準は検討の記録として残す。

| 測るもの | 指標 |
|---|---|
| JPX 取得時間 | 1 ページの取得時間 p50 / p95、失敗率（タイムアウト・接続断） |
| Cloud Run の課金時間 | 1 回あたりの課金対象インスタンス時間（更新あり / 更新なし / 時間外の 3 経路） |
| コールドスタート | 起動遅延の p50 / p95、毎分起動でインスタンスが残る割合 |
| JPX ページ更新時刻の挙動 | 更新間隔、実時刻との遅れ、1 限月と 2 限月の更新時刻のずれ、4 時台の空ページ |

判定基準（案。東京案に決定したため使わない）:

1. 東京の更新あり経路の処理時間 p50 が US より **30% 以上短く**、かつ月の vCPU 秒試算（7 章）で無料枠に収まるかどうかが変わる → 東京案
2. US の取引時間中の失敗率が東京より **1 ポイント以上高い** → 東京案
3. どちらにも当たらない → 移行作業の少ない **US 集約案**

## 3. コンポーネント設計

### 3.1 取り込み（ingest）

**実行基盤（必須）**

- **Cloud Run サービス 1 つ**（Python 3.13、最小インスタンス 0、**CPU 0.5**、メモリ 512MiB 程度）。1 vCPU 未満は第1世代実行環境・同時実行 1・リクエスト課金が条件で、0.5 vCPU ではメモリ上限が 1GiB 未満になる（[公式](https://docs.cloud.google.com/run/docs/configuring/services/cpu)）。0.5 未満にはしない。ジョブにしないのは、ジョブはインスタンスごとに最低 1 分課金で、毎分起動には向かないため
- ingress は `INGRESS_TRAFFIC_INTERNAL_ONLY`（同一プロジェクトの Scheduler から `run.app` URL を呼ぶ場合は内部扱い）+ IAM 認証 + OIDC の audience 固定
- **Cloud Scheduler は `* * * * *` の 1 ジョブだけ**（Scheduler の無料枠は請求先アカウントあたり 3 ジョブで、既に他で使い切っている）。リトライ 0、`attemptDeadline` と Cloud Run のリクエストタイムアウトを 50 秒前後に揃える。`X-CloudScheduler-ScheduleTime` はリトライ間で一定なので、リトライ 0 でも念のため重複排除のキーに使える
- **取引時間・休場日の判定はコード側 1 か所**（JPX の公表をもとにした設定ファイル。年 2 回更新、runbook に手順）。時間外は 204 で即返す。祝日取引から除外される日がある（例: 2026-11-23、12/31、1/1、1/2）。実測ではページ時刻のデータ範囲は 08:45〜15:46 / 17:00〜06:01 で、実時刻はその約 15 分後
- JPX への HTTP は接続・読み取りとも**タイムアウト 10 秒程度**（現行の `requests.get` はタイムアウト無し）

**スナップショットの識別子と保存条件**

- 識別子 = **1 限月ページの更新時刻**（JST）。保存条件は「ページ更新時刻が前回保存分から変わったら」
- 現行は「先物の約定時刻（時:分）が変わったら保存」（`update_prev_future_price_if_changed`）で、件数が変わりうる（1 日 約 800 回 vs 最大 約 1,200 回、未確認）。実測で `created_at` の重複が 2026 年に 34 件（中身が異なる）、spot / future の片方だけのスナップショットが 2025 年 36 件・2026 年 8 件ある。**新旧の判定差は並行稼働で比較し、#11 の AC にする**

**処理順（必須）**

1. 1 限月のページを取得し、更新時刻から生 HTML のオブジェクト名（`raw/YYYY/MM/DD/YYYYMMDDTHHMM.json.gz`。JST のページ時刻）を決める。オブジェクトが既にあれば終了（HEAD 1 回。この「更新なし」経路は JPX の取得を含むので時間は並行稼働（#11）で測る）
2. 2 限月のページも取得し、2 ページ分を 1 ファイル（gzip した JSON の封筒 `{"format": 1, "pages": [{"page", "url", "fetched_at", "http_status", "updated_at", "html"}]}`。UTF-8 でない本文は `html_b64`。`Content-Encoding` は付けない）にまとめて **`ifGenerationMatch=0`（上書き禁止）で作成**する。412 なら他の実行が処理済みなので終了（ロック兼重複排除）
3. 解析して `spot_price` / `future_price` / `option_price` の行を作る（純粋関数。年は `datetime.now()` ではなく HTML 内の更新時刻から決める。#2 のテストと古い HTML 断片を回帰テストにする。先物の限月は表示の年（`YY年MM月限`）をそのまま使う。旧パーサーは「限月の 1 日が更新時刻より前なら翌年」にしており、SQ のある月の 1 日〜限月交代に翌年の限月を記録している（2025-12-01〜12-11 の実データで確認。新旧突き合わせでは説明済みの差として扱う））
4. **Storage Write API** で 3 テーブルに追記する（旧ストリーミング挿入 `insertAll` は使わない）
5. 最後に完了記録 **`ingest_runs`** に 1 行書く（snapshot 時刻、`ingest_run_id`、テーブルごとの行数、`source_object`、`parser_version`）

- 「処理済みか」は GCS のオブジェクトと `ingest_runs` で判定し、**BigQuery の最新 `created_at` を毎回クエリしない**（最低課金 10MB × 月 2.5 万回 ≒ 250GB、遅延 1〜2 秒）
- **部分書き込み・重複への対処**: 追記のみとし、表示・集計は「`ingest_runs` に完了記録がある実行のうち、スナップショットごとに最新の `ingest_run_id`」を選ぶビューを通す。途中で落ちた実行の行は見えない。Storage Write API の exactly-once は同一ストリーム内のオフセット一致のときだけで、3 テーブル一括のコミットは未確認（実装時に確認）のため、これに頼らない
- **取りこぼしの回収（必須）**: **ingest サービスの中で行う**（Scheduler を増やさない）。毎時 0 分の呼び出し（取引時間中は通常処理の後）で、直近の GCS raw の一覧と `ingest_runs` を突き合わせ、完了記録の無い生 HTML を解析し直す。1 回あたりの件数に上限を設け、リクエストタイムアウト内に収める

**失敗の分類（必須）**

| 種別 | 例 | 扱い |
|---|---|---|
| `jpx_unavailable`（WARN） | 空ページ・接続断（4 時台に毎日発生） | 次の分に任せる。アラートしない |
| `parse_failed`（ERROR） | 形式変更、構造の検査の違反（2 限月そろわない、限月ごとに call と put の行数が違う、限月 × 種別ごとに ATM がちょうど 1 行でない、限月ごとの行使価格が下限 10 個未満、`created_at` が全行で同じでない、キー重複）。行数の固定下限は使わない（2026-09〜10 の実測で 1 スナップショット 84〜1,080 行と変動する。行数の分布は並行稼働（#11）で記録） | 生 HTML は残し、`ingest_runs` は書かない。アラート対象 |
| `write_failed`（ERROR） | BigQuery 書き込み失敗 | 回収処理で再処理。**初回から M2 で通知** |

- 行数・限月は SQ・限月交代で入れ替わるので、構造の検査と品質チェックはそれを考慮する

**再処理（必須）**

- 同じコンテナを引数違いで動かす **Cloud Run ジョブ**。**手動の期間指定再処理・バックフィル専用**で、定期起動しない（定期の回収は上記のとおり ingest サービスが行う）
- 置換方法は 2 つ:
  - (b) 既定: 追記のみ。新しい `ingest_run_id` で書き、上記のビューで最新版を選ぶ
  - (a) 大量・恒久の置換: 1 日以上前のパーティションに限り、一時テーブルからパーティション単位で DELETE + INSERT（通常の取り込みと期間で排他。Storage Write API で 30 分以内に書いた行には DML の制約がある）

### 3.2 データ（BigQuery）

- データセット: `optionchan_tokyo`（新規、asia-northeast1。東京案に決定、D2）。データセットのロケーションと名前は後から変えられない
- テーブルは現行のスキーマを基本に維持し、次を追加する（必須）:
  - `ingest_run_id`、`parser_version`、`source_object`（生 HTML のパスと generation）
  - `page_updated_at`: その行を取ったページの更新時刻（現行は 1 限月の時刻に揃えて上書きしている。`created_at` は従来どおり 1 限月の更新時刻）
  - `price_time_raw`: ページの約定日時の表示（`MM/DD HH:MM`）。#4 の推定ロジックの検証にも使う
  - 追加列はすべて NULLABLE（旧データを INSERT…SELECT で入れられるように）。定義の正は `schema/tokyo/*.json`、データセット・パーティション・クラスタの設定は `schema/tokyo/tables.json`（11b の Terraform が読む）
  - `ingest_runs` テーブル（完了記録）
- `price_time` は DATETIME（JST）のまま。列の説明に JST と明記する
- `option_price` は `require_partition_filter = true`、テーブル関数は期間引数を必須にする。パーティションは UTC 日単位で、JST の取引日（夜間取引）をまたぐ
- **1 時間足**（現 `insert_ohlc`）は定期クエリにする。現行は ATM かつ type=2、UTC 基準で前日 1 日分、`CURRENT_TIMESTAMP` 基準、単純 INSERT で、実測でキー重複（`atm_iv` 84 行、`atm_option_price` 42 行）がある。新方式は `@run_time` 基準、`(last_trading_day, time_frame, started_at)` で MERGE、`SAFE_OFFSET` を使い、再処理した日は再計算する。**過去 30 日分で現行と同値**を確認してから切り替える
- 表示用の集計（スマイルカーブ・ATM 推移）は **ビュー / テーブル関数**（上記の最新版選択ビューの上に作る）
- 古いパーティションは長期保存料金に任せ、削除しない。タイムトラベルは既定の 7 日

**過去データの補正（#4、移行とは別ステップ）**

- コピー（東京案）や結合ビュー（US 案）は**元データのまま**行い、検証が済んでから補正する
- 補正は `price_time_raw` 列に元の値を残し、推定で直した行に `price_time_estimated` フラグを立てる
- 推定ロジックの検証: 並行稼働中の生 HTML を正解とし、旧ロジックで解析 → 推定 SQL で戻した結果との**一致率を AC にする**（目標値は #4 で決める）

### 3.3 ビューアー（App Engine をやめる）

- **Cloud Run サービス 1 つ**に UI（静的ファイル）と API をまとめる。最小インスタンス 0
- 認証は **Cloud Run への IAP 直接設定**（GA。GA の時期は公式で要確認。ロードバランサ不要・追加費用なし）。ロードバランサ + IAP は採らない（費用が大きく、LB と Cloud Run の両方に IAP は併用できない）
  - IAP の有効化は Terraform の `google_cloud_run_v2_service` の `iap_enabled = true` で行う（[公式](https://docs.cloud.google.com/run/docs/securing/identity-aware-proxy-cloud-run)）
  - このプロジェクトは組織に属さないため、**独自の OAuth クライアントを事前に手作業で設定する**必要がある（IAP OAuth Admin API は 2026-03-19 に停止）。これは **Terraform 管理外**として runbook に手順を書く。OAuth クライアント情報が state に入るかは未確認（実装時に確認）
  - `run.invoker` は IAP サービスエージェントにだけ、`iap.httpsResourceAccessor` は閲覧者にサービス単位で付ける（現在はプロジェクト単位）
- API は BigQuery を **パラメータ化クエリで直接**読む。IAP 前提で CORS は許可しない。期間・件数に上限を設け、スキャン量で課金が膨らまないようにする
- Chart.js を最新版に揃え、ビルド済みの静的ファイルをコンテナに同梱する（ロックファイルで管理、CDN 依存をやめる）

### 3.4 監視

**必須（アラートは 1〜2 本）**

| # | アラート | 条件 | 拾うもの |
|---|---|---|---|
| M1 | 生存（ログ指標の欠落） | ingest の起動ログ（時間外の 204 も含む）が 15 分無い。24 時間有効 | Scheduler・Cloud Run の停止 |
| M2 | エラー（ログ一致） | ingest・再処理・定期クエリの `severity >= ERROR` | `parse_failed`、書き込み失敗、鮮度切れ、データ品質違反 |

- **現行構成（フェーズ 0、#9）の M2 はログ一致ではなく、Cloud Functions の組み込み指標 `execution_count` の失敗数（status が `ok` 以外）のしきい値**にした（`download_jpx` / `load_jpx_into_bq` は関数ごとに 15 分間に 3 件以上、`insert_ohlc` は 1 件以上）。現行のコードは既知のエラー（毎日 04:00 / 04:20 JST の空ページ、接続エラー、409 Conflict）を ERROR で出すためログ一致では毎日鳴り、timeout・connection error（メモリ上限超過を含む）では ERROR ログが出ないため。新系は既知の事象を WARN に分け（3.1 の失敗の分類）、構造化ログの ERROR だけを拾えるので、上の表のログ一致のままとする。現行の M1 も、ログ指標ではなく同じ組み込み指標の欠落で判定している
- **鮮度**は ingest が判定し ERROR ログを出す（M2 で通知）: 取引時間中（カレンダー参照）に「実時刻 − ページ更新時刻」がしきい値を超えたら。取引中の 10〜20 分の空白が 2026-09 以降 16 回（すべて 0〜6 時の夜間）あるため、しきい値は通常の遅れ 15 分 + 余裕で **日中 30 分 / 夜間 40 分**を初期値とし、並行稼働で調整する。metric-absence 条件は時間帯を指定できないので生存（M1）と分ける。立会終了後も遅れの分（30 分）取り込みを続けるが、その間は実時刻ではなく立会の終了時刻を基準に比べる（終了後にページの更新が止まることによる毎日の誤報を避ける。#32）
- **データ品質**: 行数・間隔は ingest 側で即時に検査する。**日次の定期クエリ**で整合を検査し、違反があれば `ERROR()` で失敗させる（M2 で通知）
  - `ingest_runs` の行数・テーブル別行数 = 各テーブルの実件数（GCS raw と `ingest_runs` の突き合わせは SQL ではできないので、ingest の回収処理が行う）
  - 3 テーブルに同じスナップショットがある、`created_at` の重複が無い、2 限月ともある、限月ごとに ATM がちょうど 1 つ
  - `price_time` が未来でない・30 日以上前でない、日曜の日付が無い（直近パーティションのみ。過去は #4 まで違反のまま）。祝日取引・休場・SQ を考慮
- **コスト**: 予算アラート（50% / 80% / 100%。#21）と BigQuery の 1 日あたりクエリ量の上限（カスタムクォータ）。予算アラートは Cloud Monitoring のアラート課金の対象外
- ログはすべて JSON の構造化ログ（`severity`、`ingest_run_id`、`snapshot_at`、`rows`、失敗種別）で stdout に出す
- 通知はメール（D3）

**任意（要否を個別に判断）**: ダッシュボード、ビューアーの 5xx アラート、Error Reporting、チャット通知（Discord / Slack 等、webhook は Secret Manager）、品質項目ごとの個別アラート

### 3.5 セキュリティ（すべて必須）

**サービスアカウントと権限**

| SA | 権限（範囲） |
|---|---|
| `sa-ingest`（取り込み・回収・再処理ジョブ共用） | `optionchan_tokyo` の `dataEditor`（データセット単位）、raw バケットの `objectCreator` + `objectViewer`（一覧・読み取り。削除なし）、`bigquery.jobUser`（回収処理で `ingest_runs` を読む） |
| `sa-viewer` | `optionchan_tokyo` の `dataViewer` + `bigquery.jobUser` |
| `sa-scheduler` | ingest サービス単位の `run.invoker` |
| `sa-sq`（定期クエリ） | `optionchan_tokyo` の `dataEditor` + `bigquery.jobUser` |
| `sa-tf-plan` | prod が扱うリソースの predefined 閲覧ロール（#10 時点で `monitoring.viewer`・`artifactregistry.reader`。basic の `roles/viewer` は UBLA バケットの既定 IAM で bootstrap の state まで読めるので使わない）+ prod の tfstate の `objectViewer`（plan は `-lock=false`） |
| `sa-tf-apply` | リソース作成・Artifact Registry への push（#10 時点で `artifactregistry.admin`・`monitoring.alertPolicyEditor`・`monitoring.notificationChannelViewer` + prod の tfstate の `objectUser`）。`projectIamAdmin` は prod が IAM を扱う #11 で足し、IAM Conditions で付与できるロールを限定する: `api.getAttribute('iam.googleapis.com/modifiedGrantsByRole', []).hasOnly([...])`（ロールは最大 10 個。`hasOnly()` を `&&` / `||` で複数つながない。`setIamPolicy` を含むロールや変更可能なカスタムロールを入れない。制限できるのはプロジェクト等への付与だけで、データセットへの付与には効かない。公式ドキュメントで 2026-10 に確認済み）。`serviceAccountUser` は SA 単位 |

- 再処理ジョブは最小構成のため `sa-ingest` と共用する（権限は同じで足りる）
- Compute / App Engine のデフォルト SA に `roles/editor` が付いている。Cloud Run は SA 未指定だと Compute デフォルト SA で動くので、**全リソースで `service_account` を必須**にし CI で検出する。Editor は後片付け（6 章）で外す
- 旧 `optionchan` SA の鍵（2019 年発行）は **フェーズ 0 で最終使用日時を確認**し、未使用なら無効化 → 一定期間後に削除。2026-10-12 に 2 本とも無効化済み（#23、最終認証は記録なし）。削除はフェーズ 6（#15）

**CI（GitHub Actions）と Workload Identity 連携**

- プロバイダの属性条件は数値 ID（`assertion.repository_id` / `repository_owner_id`）で絞る
- `sa-tf-apply` は `principal://…/subject/repo:terukusu/optionchan-gcp:environment:prod` にだけ許可。Environment `prod` は `master` のみ・required reviewer
- `pull_request_target` / `workflow_run` は使わない。fork からの PR はすべて承認必須、workflow の既定権限は read（この 2 つは #22 で設定済み）、`id-token: write` は job 単位
- `master` にブランチ保護、Actions による PR 承認を無効化、secret scanning と push protection を有効化（フェーズ 0 で設定・確認。#22、手順は runbook の「GitHub 設定」）
- サプライチェーン: Actions は SHA ピン留め（必須化）と許可アクションの限定（この 2 つは #22 で設定済み）、`uv.lock` はハッシュ付き、Dependabot か Renovate、ベースイメージとデプロイは digest 固定、Artifact Registry にクリーンアップポリシー

**公開面・秘匿情報・バケット**

- 一般公開のエンドポイントは無し（ビューアーは IAP、ingest は ingress internal + Scheduler の SA のみ）
- 現行の `allUsers` 公開関数は、`stackdriverLineNotify` を **フェーズ 0 で即削除**、残り（`smile_data` 等）は 5a で公開を外す。それまでの備えとして予算アラートと BigQuery の日次クォータを先に入れる。git 履歴にある 2019 年の共通パスワードは現行の値と一致しないことを確認済み（2026-10-12）
- Terraform state・公開コードに秘匿情報を入れない: メールアドレス等は GitHub Environment の variables / secrets → `TF_VAR_`、webhook URL（トークン相当）は Secret Manager に置き ID で参照、plan 出力は `sensitive`。`.gitignore` に `*.tfstate` / `.terraform/` / `*.tfvars` を追加し、`.terraform.lock.hcl` はコミットする
- raw と tfstate のバケットは `uniform_bucket_level_access = true`・`public_access_prevention = enforced`、ソフト削除 / 保持ポリシー、`prevent_destroy`。tfstate は `sa-tf-apply` とオーナーだけが書ける
- JPX のコンテンツ（生 HTML）は再配布しない（公開しない）。テスト用 fixture は必要部分だけに絞る

## 4. IaC・CI/CD

```
infra/
├── bootstrap/   # tfstate バケット、WIF プール / プロバイダ、sa-tf-plan / sa-tf-apply とその権限
│                # オーナーが手で apply。state は prod と別の、オーナーだけが読める場所
└── prod/        # 本番の root module 1 つ（modules 分割はしない。ingest.tf / data.tf / viewer.tf / monitoring.tf / iam.tf のファイル分割のみ）
```

- Terraform（google / google-beta プロバイダ）、state は GCS（バージョニング有効）
- 既存リソースとフェーズ 0 で gcloud で作ったリソースは `import` ブロックで取り込む（作り直さない）。BigQuery のテーブルは `deletion_protection = true`
- アプリ: `uv`、`ruff`、`pytest`。コンテナは自前の Dockerfile で GitHub Actions がビルドし、Artifact Registry に push（digest 固定）
- GitHub Actions:
  - PR: テスト・lint・`terraform plan`（`sa-tf-plan`、結果を PR にコメント）
  - `master` へのマージ: ビルド・push → イメージ digest を変数に `terraform apply`（`sa-tf-apply`、Environment `prod` の承認）。デプロイ経路は Terraform 1 本にし、`gcloud run deploy` との二重管理をしない
- Terraform 管理外（runbook に手順）: bootstrap の apply、IAP 用 OAuth クライアントの事前設定、予算アラートと BigQuery の 1 日あたりクエリ量の上限（フェーズ 0 で gcloud で設定、#21。手順は `docs/runbook.md` の「コスト」。予算は請求先アカウント側のリソース、クエリ量上限は Service Usage の consumer override。#10 で runbook 管理のままにすると決めた: 予算の請求先アカウントは他プロジェクトと共有で、CI 用 SA に請求先アカウントの権限を渡さない。クエリ量上限の変更には `serviceusage.quotaAdmin` 相当が要り、apply SA の権限が広がる）

### 4.1 runbook（`docs/runbook.md`、実装時に作成）

アラート（M1・M2）ごとの一次対応、Scheduler の停止 / 再開、取引カレンダーの更新（年 2 回）、期間指定の再処理、JPX の形式変更時・BigQuery 障害時の対応、tfstate のロック解除・復旧、IAP OAuth クライアントの設定。

「コスト」（予算・クエリ量上限の確認と変更、超過時の一次対応）は #21 で作成済み。「監視アラート」（現行構成の M1・M2 の作成・試験発報・一次対応）は #9 で作成済み。新系の M1・M2 は #11 以降で書き換える。

「GitHub 設定」（設定値の確認、許可アクションの追加、緊急時の直接 push）は #22 で作成済み。

「サービスアカウント鍵」（最終認証日時の確認、無効化と戻し方、無効化後の確認）は #23 で作成済み。

## 5. リポジトリ構成（After）

```
optionchan-gcp/
├── apps/
│   ├── ingest/       # 取り込み (Cloud Run サービス + 再処理ジョブ)。jpx_loader の後継
│   └── viewer/       # UI + API (Cloud Run サービス)
├── config/           # 取引カレンダー（取引時間・休場日・祝日取引の除外日）
├── sql/              # ビュー・定期クエリ・品質チェック・移行 SQL
├── infra/            # Terraform（bootstrap / prod）
├── tests/
├── docs/             # architecture/、runbook.md
└── .github/workflows/
```

## 6. 移行計画

本番は毎分取り込み中なので、**新旧を並行稼働させて比較してから切り替える**。期間は目安。全体で約 6〜8 か月、**2027-06 末までに旧系の削除（5b）を終える**ことを目標にする。Cloud Monitoring のアラート課金は 2027-09-01 から始まる（公式 https://cloud.google.com/stackdriver/pricing で 2026-10-12 に確認。"no sooner than" 2027-09-01）ので、それまでに旧系向けのアラートを片付け、新系の 1〜2 本だけが残る状態にする。**フェーズ S は中止**（2026-10-12 に東京に決定）。フェーズ 0 は 2026-10-12 に完了（#9、#20〜#23）。

| フェーズ | 内容 | 期間（目安） | 完了判定 | 切り戻し |
|---|---|---|---|---|
| S. スパイク（**中止**） | 2026-10-12 にリージョンを東京に決定したため行わない（#19）。2.1 の計測項目はフェーズ 2 で東京だけで測る | — | — | — |
| 0. 先行監視・セキュリティ即時対応 | `stackdriverLineNotify` 削除、予算アラート、BigQuery 日次クォータ、旧 SA 鍵の使用状況確認、GitHub 設定を 3.5 の CI 項目に揃える、現行構成に M1・M2 相当のアラートとメール通知（gcloud で作り、フェーズ 1 で import）。#9 で作成（組み込み指標 `execution_count` の欠落と失敗数で判定。定義は `monitoring/`） | 1 週間（2026-11） | 左記がすべて済み、アラートを 1 回ずつ試験発報して届く | 追加分を削除するだけ |
| 1. bootstrap・基盤 | bootstrap（tfstate、WIF、`sa-tf-plan` / `sa-tf-apply`）→ prod root module、Artifact Registry、CI。フェーズ 0 のリソースを import | 1〜2 週間（2026-11〜12） | PR で plan が動き、apply は承認後のみ。import 後の plan 差分 0 | 影響なし |
| 2. 新取り込みの並行稼働 | 東京に新データセット `optionchan_tokyo` と ingest を作り並行稼働（新データセットにだけ書く）。開始時刻を **T0** とする。新旧の突き合わせは定期クエリで自動化（キーは `price_time` と行使価格。東京案では別リージョンを直接 JOIN できないので、旧系の前日分を日次でコピーして比較）。年末年始の休場もここで確認 | 構築 2〜3 週間 + 並行 4 週間以上（2026-12〜2027-02） | 説明できない差分 0 が **連続 10 営業日**、新系の取りこぼし率が旧系以下、保存条件の新旧差を記録（#11）、Cloud Run の月 vCPU 秒が試算内、2.1 の計測項目（取得時間・課金時間・コールドスタート・JPX ページ更新時刻）を記録 | 新を止めるだけ |
| 3a. データ移行（**東京案のみ**） | 旧データセットを一時データセット（例 `optionchan_import`）へコピー → 日別行数と `BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t)))` を US / 東京で突き合わせ → T0 より前の行だけ `optionchan_tokyo` へ INSERT…SELECT。並行稼働中に新系が取りこぼした分は旧系から補う | 1 週間（2027-02） | 全日で行数・フィンガープリント一致、T0 前後で欠落・重複 0 | 旧データセットは残っている |
| 3a'. 結合ビュー（**US 案のみ。不採用**） | 旧 `optionchan`（T0 前）と新データセット（T0 以降）を結合するビューを作る。データのコピーは無し | 数日 | ビュー経由の日別行数が両データセットの合計と一致 | ビューを消すだけ |
| 3b. #4 補正 | 3.2 の方法で過去データを補正（移行とは別ステップ） | 1〜2 週間（2027-03） | 推定ロジックの一致率が #4 の AC を満たす、`price_time_raw` が全行に残る | `price_time_raw` から戻せる |
| 4. ビューアー | 新 viewer を IAP 付きで公開し、旧 App Engine と見比べる | 2〜3 週間（2027-03〜04） | 既存のチャートをすべて再現、1 週間の見比べで差異なし | 旧 URL はそのまま |
| 5a. 旧系の停止（可逆） | 旧 Scheduler 停止、HTTP 関数の `allUsers` を外す、旧系向けアラートを止める | 2 週間様子を見る（2027-04〜05） | 2 週間、新系のアラートの誤報・取りこぼし無し | Scheduler 再開・公開を戻す |
| 5b. 旧系の削除（不可逆） | 旧 Functions・Pub/Sub・Datastore・GCS 中継を削除。App Engine は無効化（アプリごとは削除不可）。python37 / nodejs10 の関数は**再デプロイできない**ので、5a の観察を終えてから | 1 週間（2027-05） | 削除済み、新系だけで 1 週間問題無し | 不可（ソースは git に残る） |
| 6. 後片付け | 次の順で: ソース zip（gcf-sources 等）削除 → 旧 SA の権限除去・鍵削除 → デフォルト SA の Editor 除去 → 旧 US データセット削除（東京案のみ、新側で検証済みのもの）、`jikken`、App Engine の旧バージョン、Artifact Registry の旧イメージ | 2〜3 週間（2027-05〜06） | Editor の付いた SA が 0、`allUsers` の付いたリソースが 0 | 段階ごとに確認 |

既存 Issue との関係: #3（共有パスワード）は 4 で不要になり、5a で公開が無くなる。#5（スマイルカーブ）は新 viewer で解消。#4（過去データ補正）は 3b。#11（保存条件の新旧差）は 2 の AC。#2 は現行構成で先に直した（PR #6、2026-10-12 デプロイ済み）。

## 7. コスト見積もり（月額・概算）

公開価格をもとにした概算（1 ドル 150 円、税抜き。請求額には税 10% が加わる）。実績は予算アラートと課金エクスポートで確認する。

- **請求額の集計基準**: 課金エクスポートを `usage_start_time` 基準で集計すると 2026-09 は 約 156 円、`invoice.month` 基準では 約 165 円。本資料は `usage_start_time` 基準
- **無料枠は請求先アカウント単位**で、同じ請求先アカウントの他プロジェクトと共有している（Cloud Run・BigQuery・Scheduler 等）。そのため「無料枠内」は他プロジェクトの使用量しだいで外れうる

### 7.1 現行の内訳（2026-09 の課金データ、`usage_start_time` 基準）

| 項目 | 使用額 | 請求額 |
|---|---|---|
| Cloud Functions（第1世代）CPU・メモリ（us-east1） | 約 250 円 | **0 円**（無料枠の割引で全額相殺） |
| BigQuery 保存（US、ほぼ長期保存料金。当時は論理サイズ課金） | 約 92 円 | 約 92 円 |
| GCS（書き込み操作 約 37 円、保存 約 8 円） | 約 45 円 | 約 45 円 |
| Artifact Registry | 約 2 円 | 約 2 円 |
| 税 | — | 約 17 円 |
| Scheduler・Datastore・ログ・リージョン間通信 | ほぼ 0 円 | ほぼ 0 円 |
| **合計** | 約 406 円 | **約 156 円** |

BigQuery は 2026-10-12 に物理サイズ課金へ切り替え済み（92 円 → 約 15 円の見込み。`option_price` は論理 78GB に対し物理 4.8GB）。

### 7.2 After（請求額ベース、税抜き）

| 項目 | 見込み（月額） | 根拠・前提 |
|---|---|---|
| Cloud Run（ingest） | **0 円**（目標）。無料枠を全く使えない場合 最大 約 380 円 | 試算: 更新あり 1.6 万回 × 10 秒 + 更新なし 0.9 万回 × 3 秒 + 時間外 1.9 万回 × 0.1 秒 ≒ 18.8 万秒（毎時の回収は誤差）→ CPU 0.5 で 約 9.4 万 vCPU 秒（無料枠 18 万内）、1 vCPU なら 約 18.8 万で超える。各経路の秒数は仮定で、並行稼働（#11）で測る（現行 `download_jpx` は平均 6.39 秒、p50 9.6 秒）。無料枠ゼロ時は CPU 約 340 円 + メモリ 約 35 円 |
| Cloud Scheduler | 約 15 円 | 1 ジョブ $0.10。請求先アカウントの無料枠 3 ジョブは使い切っているため |
| GCS raw（Standard） | 保存 約 30〜50 円（1 年後、以降は年ごとに増える）+ 書き込み 約 15 円 | 1 スナップショット 約 48KB × 1 日 約 800 回で年 10〜15GB。HEAD（Class B）は月 1 円未満 |
| BigQuery 保存 | 0〜約 20 円 | 物理サイズ課金で 約 5GiB ≒ 約 15〜20 円。請求先アカウントの 10GiB 無料枠（他プロジェクトと共有）が空いていれば 0 円 |
| BigQuery クエリ・書き込み | ほぼ 0 円 | クエリは月 1TiB、Storage Write API は月 2TiB の無料枠内 |
| Artifact Registry | 0 円（目標） | 無料枠 0.5GiB。クリーンアップポリシーで収める（現在は gcf-artifacts だけで 約 1.1GB） |
| ビューアー（Cloud Run） | ほぼ 0 円 | 最小インスタンス 0、利用者は本人のみ |
| アラート（2027-09-01 以降） | 約 50〜100 円 | ポリシー（メトリクス参照）1 件 $0.35 + ポイント料金で 約 50 円 / 件（公式 https://cloud.google.com/stackdriver/pricing で 2026-10-12 に確認。"no sooner than" 2027-09-01）。ログ一致条件はポイントを返さないのでポイント料金は無く、ポリシーに条件 1 つだけ（メトリクス参照 1 件分と読める。#9 で確認）。予算アラートは対象外 |
| リージョン差 | 東京案は保存単価が US より 1 割強高い（増分は数円） | — |

**合計の見込み**（税込み請求額）: 約 70〜110 円（2027-09 以降はアラート分 約 50〜100 円を加算）。Cloud Run の無料枠を使えない場合は最大 +約 380 円で、並行稼働で実測して判断する。

### 7.3 移行の一時費用

| 項目 | 費用 |
|---|---|
| データセットコピーの転送料（東京案のみ） | 約 60 円（物理バイト基準、北米 → アジア $0.08/GiB × 約 5GiB）。US 案は 0 円 |
| 新旧の突き合わせ・#4 補正のクエリ | ほぼ 0 円（月 1TiB の無料枠内） |
| BigQuery の二重保存（東京案、3a〜6 の間） | 月 約 15〜20 円（7.2 と同じ基準。10GiB 無料枠が空いていれば 0 円）。旧系の取り込み費用は 5a まで現行どおり続く |
| スパイクの試験サービス | 0 円（スパイクは中止） |

## 8. 採用しなかった案

| 案 | 理由 |
|---|---|
| Cloud Run functions（第2世代）/ `gcloud run deploy --source` | 第2世代の実体は Cloud Run サービスで、実行時の性質は自前コンテナと同じ。差は運用面: ソースデプロイは Cloud Build（とそのサービスアカウント）を挟み、Terraform 管理のイメージとの二重管理になりやすい。自前 Dockerfile + GitHub Actions でビルドすれば、テストと同じ CI でイメージを digest 固定でき、デプロイ経路が Terraform 1 本になる。Dockerfile の保守が増える点は小さいと判断（最小構成と両立） |
| リージョンを決め打ちにする | 東京・US とも利点があるため、実測で決める（2.1） |
| ロードバランサ + IAP | 費用が大きい。Cloud Run への IAP 直接設定で足りる |
| 毎分の取り込みや定期の回収を Cloud Run ジョブで動かす / 取引時間ごとに Scheduler を複数 | ジョブは最低 1 分課金。Scheduler は 1 ジョブごとに課金されるため、時間判定と回収は ingest サービス内。ジョブは手動の再処理だけ |
| Terraform を modules に分割 | 個人の小規模用途では root module 1 つで足りる |
| GKE / Compute Engine | 運用負荷とコストが大きい |
| Firestore / Bigtable 等 | 分析中心の用途で BigQuery が最適 |
| Pub/Sub を残す | Scheduler → Cloud Run の直接呼び出しで足りる |
| Looker Studio | D1 のとおり不採用（スマイルカーブの 2 軸などを再現しにくい） |
| Pulumi / CDK for Terraform | 情報量・GCP の公式サンプルの多さで Terraform を選ぶ |

## 9. 決定事項

初版は 2026-10-12 に推奨案で合意（#7）。同日、設計レビュー（#17）を受けて D2・D3・D4・D6 を更新し、D8〜D10 を追加。

| # | 論点 | 決定 | 決定日・根拠 |
|---|---|---|---|
| D1 | ビューアーを自前の Cloud Run にするか、Looker Studio にするか | Cloud Run（スマイルカーブ等の既存チャートを再現するため） | 2026-10-12（#7） |
| D2 | リージョン | **東京（asia-northeast1）**。データセットは `optionchan_tokyo`。判定スパイク（#19）は行わず、2.1 の計測項目は #11 で測る | 2026-10-12 オーナー判断（#17 で「スパイクで決める」としたのを変更） |
| D3 | 通知先 | メールのみで開始。チャットは任意（要否を個別判断。採るなら webhook は Secret Manager） | 2026-10-12（#17） |
| D4 | 生 HTML の保存クラス・期間 | **GCS Standard、期限なし**（Nearline / Archive へは移さない。再処理の元データ） | 2026-10-12（#17） |
| D5 | `jikken` と `PaymentInfo`（課金エクスポート）の扱い | `jikken` は削除、`PaymentInfo` はコスト監視に使うので残す（US のまま） | 2026-10-12（#7） |
| D6 | 過去データ補正（#4）の時期 | **データ移行とは別ステップ**（3b）。元データのまま移して検証してから補正する | 2026-10-12（#17） |
| D7 | BigQuery のタイムトラベル期間 | 既定の 7 日のまま | 2026-10-12（#7） |
| D8 | 構成の規模 | 最小構成を基準（ingest 1 サービス、IAP 直接設定、アラート 1〜2 本、Terraform root module 1 つ）。セキュリティ必須項目は削らない | 2026-10-12（#17） |
| D9 | download_jpx のメモリ | 512MB に変更済み（依存更新で 256MB を超えたため） | 2026-10-12 |
| D10 | 部分書き込み・重複の扱い | 追記のみ + `ingest_runs` の完了記録で最新版を選ぶビュー（3.1） | 2026-10-12（#17） |

## 10. 設計レビューの反映状況

#17 の設計レビュー（取り込み・データと移行・監視・セキュリティの 4 観点のレビューと、それに伴うコスト・代替案の指摘）の扱い。block / major はすべて反映した。

| 観点 | 主な指摘 | 反映先 |
|---|---|---|
| 取り込み | 完了記録 `ingest_runs`・取りこぼし回収、`ifGenerationMatch=0` による冪等化、タイムアウト・リトライ 0、Storage Write API に頼らない重複対策、スナップショットの識別子と保存条件、取引カレンダーをコード側に、CPU 0.5・高速経路、失敗の分類・行数下限、再処理の置換方法、解析の純粋性 | 3.1 |
| データ・移行 | コピーは一時データセット経由で T0 前だけ INSERT、#4 を別ステップに（フィンガープリントで検証）、1 時間足の MERGE 化、パーティション必須、スキーマ追加、期間と完了判定、5a / 5b 分割、bootstrap とフェーズ 0 の順序 | 3.2、6 |
| 監視 | アラート課金に合わせて 1〜2 本に集約、生存と鮮度の分離・しきい値、品質検査の即時 / 日次の分担、runbook | 3.4、4.1 |
| セキュリティ | WIF 条件の数値 ID 化、`sa-tf-plan` / `sa-tf-apply` 分割と Environment、apply 権限の制限と bootstrap 分離、IAP 直接設定と OAuth クライアントの手作業化、Editor・`service_account` 必須、ingress internal、公開関数・旧 SA 鍵の早期対応、state の秘匿情報、バケット設定、サプライチェーン | 3.5、4 |
| コスト | 集計基準・無料枠の共有・各項目の作り直し | 7 |
| 代替案 | 第2世代関数を退ける理由の修正、US 集約案の追加 | 8、2.1 |

一部だけ反映・反映しなかったもの:

- **`.gitignore` への `*.tfstate` 等の追加**: 方針だけ書き（3.5）、実際の追加は Terraform を置く実装の PR で行う
- **`docs/runbook.md`**: 項目だけ挙げ（4.1）、本体は実装時に作る（対象の手順がまだ無いため）
- **再処理ジョブ用の SA を分けるか**: 最小構成のため `sa-ingest` と共用に決めた
- **回収処理の起動元**: Scheduler を増やさず ingest サービス内（毎時 0 分）で行い、Cloud Run ジョブは手動の再処理専用にした
- **EPIC（#16）の子 Issue・終了条件の見直し**: 本資料の移行計画に反映し、Issue の整理は EPIC 側で行う
- 未確認のまま残した点（実装時に確認）: 保存条件の新旧で件数がどれだけ変わるか、Storage Write API での 3 テーブル一括コミット、IAP の OAuth クライアント情報が state に入るか、IAP 直接設定の GA 時期（判定基準の数値は東京案に決定したため不要）

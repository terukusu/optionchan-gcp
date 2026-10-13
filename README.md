# optionchan for GCP
日経225オプションの価格情報を蓄積していくツール。の、Google Cloud Platform 版
Cloud Functions とかでガーッと価格情報を取ってきて、BigQuery にヴァッと蓄積していきます。

* appengine
  * ビューアー用のウェブアプリ。とりあえず Chart.js でATMのIV推移と直近のスマイルカーブを可視化している

* functions
  * ４個の Cloud Functionが含まれる
    * データ取り込み用
      * download_jpx
        * スクレーピング → json化 → Cloud Storageへ保存するもの
      * load_jpx_into_bq
        * Cloud Storage へ保存されたJSONをBigQueryに読み込むもの
    * データ表示用
      * atm_data
        * ATM オプションの IV 推移表示用のデータを返す WebAPI
      * smile_data
        * 直近のスマイルカーブ表示用のデータを返す WebAPI

* apps/ingest
  * 新しい取り込み（Cloud Run サービス、Python 3.13・uv。#11 で旧系と並行稼働させる）。JPX のページを毎分取得し、生 HTML（2 ページを gzip した JSON）を GCS に、行を BigQuery（`optionchan_tokyo`、東京、Storage Write API）に書く。Cloud Run `ingest`（asia-northeast1）を Scheduler `ingest` が毎分起動する。デプロイは deploy workflow（イメージのビルド・push も含む、#33）。確認・停止・切り戻しは docs/runbook.md の「新しい取り込み（ingest）」
  * ローカルの動作確認は `INGEST_DRY_RUN=1`（GCS・BigQuery に書かず、件数と proto のサイズだけをログに出す）
  * 取引時間中は分の頭から 8 秒まで待ってから JPX を取得する（#38。ローカルでも実時刻で待つ。`INGEST_FETCH_AT_SECOND=0` で待たない）
  * 毎時 0 分に取りこぼし（生 HTML はあるが完了記録 `ingest_runs` が無いもの）を回収し、07:00 に前日分を照合する。期間を指定した作り直しは Cloud Run ジョブ `reprocess`（手動、#34）。手順は docs/runbook.md の「回収と照合」「再処理ジョブ」

* apps/viewer
  * 新しいビューアー（#13 / #52。Python 3.13・uv・Flask）。Cloud Run `viewer`（asia-northeast1）に IAP（Cloud Run への直接設定）の後ろでデプロイする（#53）。アプリに認証は無いので、IAP なし・`allUsers` で公開しない。デプロイ・オーナーの手作業（OAuth クライアント）・確認は docs/runbook.md の「ビューアー（#13）」。ページ `/` `/smile` `/atm_iv` と CSV の API `/smile_data` `/atm_iv_data?d=&n=` は現行（appengine/ + HTTP 関数）と同じ URL・同じ CSV の形式で（旧型の ATM 推移 `/atm`・`/atm_data` は移さない。#58）、BigQuery の新系（`optionchan_tokyo` の最新版のテーブル関数と 1 時間足 `atm_iv`）を直接読む
  * 設定は環境変数: `GCP_PROJECT`（必須）、`BQ_DATASET`（既定 `optionchan_tokyo`）、`BQ_LOCATION`（既定 `asia-northeast1`）、`MAX_BYTES_BILLED`（既定 1 GiB）。SQL は `src/optionchan_viewer/sql/` にだけ置き、値はクエリパラメータで渡す
  * Chart.js 等のブラウザ用ライブラリは `package-lock.json` で固定し、イメージのビルドで同梱する（CDN を使わない）

* config
  * 取引カレンダー（`trading_calendar.toml`）。更新手順は docs/runbook.md の「取引カレンダーの更新」

* schema
  * BigQueryのスキーマ。`schema/tokyo/` は新しい取り込みのテーブル（東京）の定義と設定（`tables.json`）と、日次チェックの結果のテーブル（`quality_daily`・`legacy_compare_daily`、#40）。`schema/ohlc/atm_ohlc.json` は東京の 1 時間足の 3 表（#49）の列（旧系の `atm_*.json` と同じ。取り込みのイメージに入る `schema/tokyo/` には置かない）

* sql
  * BigQuery の SQL。`sql/latest/` はスナップショットごとに最新の完了版を選ぶテーブル関数の本文（Terraform が読む。使い方は docs/runbook.md の「最新版の読み方」）
  * `sql/quality/`（日次の品質チェック）・`sql/legacy/`（旧系の前日分を東京へ運んで新旧を突き合わせる）は BigQuery Data Transfer Service の定期クエリの本文（#40。Terraform が読む。確認・再実行は docs/runbook.md の「日次の品質チェックと新旧の突き合わせ（#40）」）
  * `sql/legacy_import/` は旧系の過去データを東京へ移すときに手で流す SQL（コピーの検証・移行・移行の検証、#12。Terraform は読まない。手順は docs/runbook.md の「過去データの移行（#12）」）
  * `sql/ohlc/` は 1 時間足（ATM の IV・プレミアム・行使価格の OHLC、#49）。`atm_hourly.sql` は定期クエリの本文（Terraform が読む）、他は旧系との突き合わせ・過去分の移行で手で流す SQL（docs/runbook.md の「1 時間足（#49）」）

* monitoring
  * Cloud Monitoring のアラートポリシー定義（JSON）とバックテスト。作成・試験発報の手順は docs/runbook.md の「監視アラート」（旧系）と「新しい取り込みの監視（#35）」（新系）

* tests
  * functions・監視アラート定義・GitHub Actions の workflow・Terraform・`sql/` の静的検査のテスト (pytest)。fixture は必要な部分だけを手書きした HTML 断片
  * `tests/test_sql_bq.py` は定期クエリの SQL を BigQuery で固定値の入力に対して動かす検査と、実テーブルの dry run（CI では動かさない。`OPTIONCHAN_BQ_TEST=1 OPTIONCHAN_BQ_PROJECT=<PROJECT_ID> .venv/bin/python -m pytest -q tests/test_sql_bq.py`。プロジェクトの既定値は無い。gcloud の構成は環境変数で渡す）。`tests/test_legacy_import_bq.py`（#12）・`tests/test_ohlc_bq.py`（#49）も同じ

* infra
  * Terraform。`bootstrap`（tfstate・WIF・CI 用 SA、取り込み・ビューアーの SA・生 HTML のバケットと権限、IAP のアクセス許可。所有者が手で apply）と `prod`（Artifact Registry・監視アラート・BigQuery `optionchan_tokyo` とテーブル関数・Cloud Run / Scheduler `ingest`・Cloud Run ジョブ `reprocess`・定期クエリ（DTS）・Cloud Run `viewer`（IAP）。CI が承認後に apply）。手順は docs/runbook.md の「Terraform」
  * プロジェクト ID・GitHub リポジトリ名・state のバケット名は既定値を持たない（clone した人が自分の値を渡す）。`TF_VAR_project_id`（bootstrap は `TF_VAR_github_repository` も）と `terraform init -backend-config="bucket=<PROJECT_ID>-tfstate"`（bootstrap は `<PROJECT_ID>-tfstate-bootstrap`）で渡す

* .github/workflows
  * `ci.yml`（PR で pytest（`apps/ingest`・`apps/viewer` を含む）・terraform plan・取り込みとビューアーのイメージのビルド確認）、`deploy.yml`（master の terraform apply。Environment `prod` の承認が必要）
  * repository variables `GCP_PROJECT_ID`（GCP プロジェクト ID）・`TF_STATE_BUCKET`（prod の state のバケット）・`GCP_SA_TF_PLAN` / `GCP_SA_TF_APPLY` と secret `GCP_WIF_PROVIDER` を使う。設定は docs/runbook.md の「GitHub の設定（bootstrap の後、PR を出す前）」

## テストの実行
Python 3.11 の venv を作り、リポジトリ直下で実行する。

```bash
python3.11 -m venv .venv
.venv/bin/pip install -r functions/requirements.txt -r tests/requirements.txt
.venv/bin/python -m pytest -q tests
```

`apps/ingest` は Python 3.13 と uv（`uv.lock` で固定）で別に動かす。uv は `pip install uv==0.12.19` 等で入れる。

```bash
cd apps/ingest
uv sync --locked --python 3.13
uv run --locked ruff check
uv run --locked pytest -q

# イメージ（ビルド context はリポジトリ直下。.dockerignore は許可リスト方式）
docker build -f apps/ingest/Dockerfile -t ingest:local ../..
docker run --rm -p 127.0.0.1:8080:8080 -e INGEST_DRY_RUN=1 ingest:local  # POST / で 1 回取り込む（時間外は 204）
```

`apps/viewer` も同じく uv で動かす。ブラウザ用ライブラリは npm（`package-lock.json`）から入れる。

```bash
cd apps/viewer
uv sync --locked --python 3.13
uv run --locked ruff check
uv run --locked pytest -q

# ローカルで起動（BigQuery は読み取りだけ。認証はアプリケーションのデフォルト認証情報）
npm ci --ignore-scripts && npm run vendor
GCP_PROJECT=<project-id> uv run --locked flask --app 'optionchan_viewer.app:create_app()' run

# 新旧の SQL の同値・実テーブルの dry run（BigQuery の認証が要る。CI では動かさない）
OPTIONCHAN_BQ_TEST=1 OPTIONCHAN_BQ_PROJECT=<project-id> uv run --locked pytest -q tests/test_sql_bq.py

# イメージ（ビルド context は apps/viewer。.dockerignore は許可リスト方式）
docker build -t viewer:local .
```

PR では GitHub Actions（`.github/workflows/ci.yml`）が同じテストと `terraform plan` を回し、plan の要約（アドレスと操作だけ）を PR にコメントする。

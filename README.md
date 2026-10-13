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

* config
  * 取引カレンダー（`trading_calendar.toml`）。更新手順は docs/runbook.md の「取引カレンダーの更新」

* schema
  * BigQueryのスキーマ。`schema/tokyo/` は新しい取り込みのテーブル（東京）の定義と設定（`tables.json`）と、日次チェックの結果のテーブル（`quality_daily`・`legacy_compare_daily`、#40）

* sql
  * BigQuery の SQL。`sql/latest/` はスナップショットごとに最新の完了版を選ぶテーブル関数の本文（Terraform が読む。使い方は docs/runbook.md の「最新版の読み方」）
  * `sql/quality/`（日次の品質チェック）・`sql/legacy/`（旧系の前日分を東京へ運んで新旧を突き合わせる）は BigQuery Data Transfer Service の定期クエリの本文（#40。Terraform が読む。確認・再実行は docs/runbook.md の「日次の品質チェックと新旧の突き合わせ（#40）」）

* monitoring
  * Cloud Monitoring のアラートポリシー定義（JSON）とバックテスト。作成・試験発報の手順は docs/runbook.md の「監視アラート」（旧系）と「新しい取り込みの監視（#35）」（新系）

* tests
  * functions・監視アラート定義・GitHub Actions の workflow・Terraform・`sql/` の静的検査のテスト (pytest)。fixture は必要な部分だけを手書きした HTML 断片
  * `tests/test_sql_bq.py` は定期クエリの SQL を BigQuery で固定値の入力に対して動かす検査と、実テーブルの dry run（CI では動かさない。`OPTIONCHAN_BQ_TEST=1 .venv/bin/python -m pytest -q tests/test_sql_bq.py`。gcloud の構成は環境変数で渡す）

* infra
  * Terraform。`bootstrap`（tfstate・WIF・CI 用 SA、取り込みの SA・生 HTML のバケットと権限。所有者が手で apply）と `prod`（Artifact Registry・監視アラート・BigQuery `optionchan_tokyo` とテーブル関数・Cloud Run / Scheduler `ingest`・Cloud Run ジョブ `reprocess`・定期クエリ（DTS）。CI が承認後に apply）。手順は docs/runbook.md の「Terraform」

* .github/workflows
  * `ci.yml`（PR で pytest（`apps/ingest` を含む）・terraform plan・取り込みのイメージのビルド確認）、`deploy.yml`（master の terraform apply。Environment `prod` の承認が必要）

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

PR では GitHub Actions（`.github/workflows/ci.yml`）が同じテストと `terraform plan` を回し、plan の要約（アドレスと操作だけ）を PR にコメントする。

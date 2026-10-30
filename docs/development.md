# 開発の仕方

テストの種類と流し方、ローカルでの起動、CI、デプロイと承認。構築は [setup.md](setup.md)、運用は [runbook.md](runbook.md)。用語は [glossary.md](glossary.md)。

## ディレクトリと言語

| 場所 | 言語・環境 | 中身 |
|---|---|---|
| `tests/`（リポジトリ直下） | Python 3.11 の venv。依存は `tests/requirements.txt`（pytest・PyYAML）と、旧系の関数の `functions/requirements.txt`（ルートのテストの一部が旧系のコードを読む） | `sql/`・`schema/`・`infra/`・`monitoring/`・`.github/workflows/` の検査、SQL の固定値の検査、旧系の解析のテスト |
| `apps/ingest` | Python 3.13（`apps/ingest/pyproject.toml` の `requires-python`）、uv（`apps/ingest/uv.lock` で固定） | 取り込み。ruff・pytest |
| `apps/viewer` | Python 3.13・uv（`apps/viewer/uv.lock`）。ブラウザ用ライブラリは npm（`apps/viewer/package.json`・`apps/viewer/package-lock.json`。CDN を使わず、ビルド時に `static/vendor/` へコピーして同梱する） | ビューアー。ruff・pytest、JS は node の標準のテスト（node の版はリポジトリ直下の `.node-version`） |
| `appengine/`・`functions/`・`functions_py/` | 旧系（#14 で止め、#24 で消す） | 新しく手を入れない |

## テスト

PR の CI（下の「CI」の `pytest`）と同じものを手元で流す。どれもリポジトリ直下から始める。

ルート（Python 3.11）:

```bash
python3.11 -m venv .venv
.venv/bin/pip install -r functions/requirements.txt -r tests/requirements.txt
.venv/bin/python -m pytest -q tests
```

`apps/ingest`（Python 3.13 と uv。uv は CI と同じく `pip install uv==0.12.19` 等で入れる）:

```bash
cd apps/ingest
uv sync --locked --python 3.13
uv run --locked ruff check
uv run --locked pytest -q
```

`apps/viewer`（同じく uv。JS の検査とテストも `apps/viewer` で流す）:

```bash
cd apps/viewer
uv sync --locked --python 3.13
uv run --locked ruff check
uv run --locked pytest -q
for f in src/optionchan_viewer/static/*.js; do node --check "$f"; done
node --test tests/js/*.test.js
```

BigQuery を使うテスト（`OPTIONCHAN_BQ_TEST` を見るテスト。ルートの `tests/*_bq.py` と `apps/viewer/tests/test_sql_bq.py`。`apps/viewer/tests/test_bq.py` は偽のクライアントのテストで CI でも動く）は、`OPTIONCHAN_BQ_TEST=1` と `OPTIONCHAN_BQ_PROJECT` があるときだけ動き、無ければ skip する。`bq` コマンドで SQL を実際に流す（固定値の検査や実テーブルの dry run。何をするかは各ファイルの冒頭の説明）。BigQuery の認証が要るので CI では動かさない。

```bash
OPTIONCHAN_BQ_TEST=1 OPTIONCHAN_BQ_PROJECT=<PROJECT_ID> .venv/bin/python -m pytest -q tests/test_sql_bq.py
cd apps/viewer && OPTIONCHAN_BQ_TEST=1 OPTIONCHAN_BQ_PROJECT=<PROJECT_ID> uv run --locked pytest -q tests/test_sql_bq.py
```

文書の検査（`tests/test_docs.py`）もルートの pytest に入っている。リンク・アンカー・runbook の節名の引用・`docs/data.md` の表名と列名を検査するので、文書や見出しを変えたら流す。infra に表や主な表の列を足したら `docs/data.md` にも書く（書かないと CI の pytest が落ちる）。infra の書き方を変えて検査の拾い方（`tokyo_objects` など）が `AssertionError` で落ちたら、拾い方を足す。

## ローカルでの起動

取り込み（イメージ。ビルド context はリポジトリ直下で、`.dockerignore` は許可リスト方式）:

```bash
docker build -f apps/ingest/Dockerfile -t ingest:local .
docker run --rm -p 127.0.0.1:8080:8080 -e INGEST_DRY_RUN=1 ingest:local   # POST / で 1 回取り込む（時間外は 204）
```

`INGEST_DRY_RUN=1` のときは GCS・BigQuery に書かず、書くはずの件数をログに出す（`dry_run_append`・`dry_run_raw_create`。回収の一覧は常に空、取引カレンダーの同期はしない）。取引時間中なら JPX のページは取りに行く。時刻を決めて試すときは `INGEST_FAKE_NOW`（例 `2026-10-13T10:00:00+09:00`。`INGEST_DRY_RUN` のときだけ効く）。

ビューアー（BigQuery は読み取りだけ。認証はアプリケーションのデフォルト認証情報）:

```bash
cd apps/viewer
npm ci --ignore-scripts && npm run vendor
GCP_PROJECT=<PROJECT_ID> uv run --locked flask --app 'optionchan_viewer.app:create_app()' run
docker build -t viewer:local .   # イメージ（ビルド context は apps/viewer。.dockerignore は許可リスト方式）
```

環境変数（`GCP_PROJECT` のほか、データセット・クエリ量の上限）は [viewer.md の「設定（環境変数）」](viewer.md#設定環境変数)。

## CI

`.github/workflows/ci.yml`。`master` への PR で毎回動く（パスの絞り込みは無い）。job は 4 つで、`pytest` と `terraform-plan` がブランチ保護の必須 check（runbook の「GitHub 設定」）。

| job | 中身 |
|---|---|
| `pytest` | ルートの pytest（Python 3.11）→ `apps/ingest` の ruff・pytest（Python 3.13、`uv==0.12.19`、`uv sync --locked`）→ `apps/viewer` の ruff・pytest → `.node-version` の node で `node --check`・`node --test tests/js/*.test.js`。上の「テスト」と同じ |
| `ingest-image` | 取り込みのイメージをビルドし、import できること、`auth.txt`・`functions/`・テストがイメージに入っていないことを確かめる（push しない・認証なし。必須ではない） |
| `viewer-image` | ビューアーのイメージをビルドし、`.github/scripts/viewer-image-check.sh` で中身を確かめる（push しない・認証なし。必須ではない） |
| `terraform-plan` | `terraform fmt -check -recursive infra` → bootstrap の init（backend なし）・validate → repository variables（`GCP_PROJECT_ID`・`TF_STATE_BUCKET`）があることの確認 → WIF で `GCP_SA_TF_PLAN` の SA を借りる → イメージの解決（`.github/scripts/image.sh <ingest|viewer> resolve`）→ prod の init・validate・plan（`-lock=false`）→ 要約を job の summary と PR のコメントに書く |

- plan の要約は「アドレス: 操作」と操作ごとの件数、イメージが既存か未作成か、だけ。公開リポジトリなので plan の全文はログ・コメント・artifact に出さない。失敗したときのログの末尾は、9 桁以上の数字と SA 以外のメールアドレスを伏せて出す
- 要約を見て、意図しない update・delete・replace が無いことを確かめてからマージする（runbook の「Terraform」の「流れ」）
- fork からの PR は secret を受け取れないので `terraform-plan` が失敗する（runbook の「Terraform」の「残るリスク」）
- bootstrap は CI では fmt・validate だけで、plan・apply しない（所有者がローカルで apply する。[setup.md](setup.md)）

## デプロイ

`.github/workflows/deploy.yml`。`master` への push（マージ）と手動（`gh workflow run deploy.yml --ref master`）で動く。同時には 1 本だけ（`concurrency` の `terraform-prod`）。

1. `plan` job: `GCP_SA_TF_PLAN` の SA でイメージを解決して prod を plan する（`-detailed-exitcode`）。差分が無ければここで終わり、承認待ちを作らない
2. `apply` job: 差分があるときだけ動き、Environment `prod` の承認を待つ（承認は Actions の画面か、runbook の「Terraform」の「流れ」のコマンド）。承認後、Artifact Registry に無いイメージを **GCP の認証より前に** ビルド・検査し、`GCP_SA_TF_APPLY` の SA で push して digest を求め、もう一度 plan して apply する（digest の参照でだけデプロイする）
3. plan のファイルは artifact で `apply` job に渡さない。公開リポジトリの artifact は誰でも取得でき、state 由来の値を含むため（`.github/workflows/deploy.yml` の冒頭のコメント）。承認の間に `master` が進んだら、その分も含めて apply される

イメージのタグは `src-<20 桁の 16 進>` で、入力のパスの git のハッシュから `.github/scripts/image.sh <ingest|viewer> tag` が決める（コミット済みの内容から。作業ツリーの変更は入らない）。同じ入力なら同じタグで、タグは不変（`immutable_tags`）。

| イメージ | 入力のパス | ビルド context |
|---|---|---|
| ingest | `apps/ingest/Dockerfile`・`apps/ingest/pyproject.toml`・`apps/ingest/uv.lock`・`apps/ingest/src`・`schema/tokyo`・`.dockerignore` | リポジトリ直下 |
| viewer | `apps/viewer/Dockerfile`・`apps/viewer/.dockerignore`・`apps/viewer/pyproject.toml`・`apps/viewer/uv.lock`・`apps/viewer/package.json`・`apps/viewer/package-lock.json`・`apps/viewer/src` | `apps/viewer` |

文書・テストだけの変更ではタグも Terraform の入力も変わらないので、マージしても `plan` は「差分なし（No changes）」で `apply` は skipped になる。

```sh
gh run list -R "$REPO" --workflow=deploy.yml --branch master --limit 1   # 直近の deploy の run
```

## Terraform が読むファイル

prod の Terraform（`infra/prod/`）は次のファイルを `file()`・`templatefile()` で読む。中身はそのまま表の定義・テーブル関数やビューの本文・定期クエリの SQL・アラートの定義になるので、**SQL のコメントや列の説明を直すだけでも plan に差分が出て、承認つきの apply が要る**。bootstrap（`infra/bootstrap/`）が読むファイルは無い。

| ファイル | 読む所 | なるもの |
|---|---|---|
| `schema/tokyo/tables.json` | `infra/prod/data.tf` | データセットと取り込みの 4 表の設定（場所・パーティション・クラスタなど） |
| `schema/tokyo/*.json`（表ごと） | `infra/prod/data.tf`・`infra/prod/sq.tf`・`infra/prod/calendar.tf`・`infra/prod/gaps.tf`・`infra/prod/datacube.tf` | 表の列（列名・型・説明） |
| `schema/ohlc/atm_ohlc.json` | `infra/prod/sq.tf` | 1 時間足の表の列 |
| `schema/cm/atm_cm_iv.json` | `infra/prod/sq.tf` | 30 日固定の ATM IV の表の列 |
| `sql/latest/*.sql` | `infra/prod/data.tf` | テーブル関数（最新版の選択）の本文 |
| `sql/calendar/trading_calendar_latest.sql` | `infra/prod/calendar.tf` | ビュー `trading_calendar_latest` |
| `sql/quality/quality_daily.sql` | `infra/prod/sq.tf` | 定期クエリ `quality-daily` |
| `sql/legacy/stage.sql`・`sql/legacy/compare.sql` | `infra/prod/sq.tf` | 定期クエリ `legacy-stage`・`legacy-compare`（旧系） |
| `sql/ohlc/atm_hourly.sql` | `infra/prod/sq.tf` | 定期クエリ `atm-hourly` |
| `sql/cm/atm_cm.sql` | `infra/prod/sq.tf` | 定期クエリ `atm-cm` |
| `monitoring/m1-liveness.json`・`monitoring/m2-errors.json`・`monitoring/ingest-m1-liveness.json`・`monitoring/ingest-m2-errors.json` | `infra/prod/monitoring.tf` | アラート（定義の正は JSON。`tests/test_monitoring.py` が検査） |

`schema/tokyo/` は取り込みのイメージの入力でもあるので、直すとイメージのタグも変わる。ほかの `sql/` の下（`sql/gaps/`・`sql/legacy_import/`・`sql/price_time_fix/`・`sql/tick_fill/`・`sql/tick_build/`・`sql/cm_trade/`、`sql/ohlc/` と `sql/cm/` のほかのファイル）は runbook の手順で手で流すもので、Terraform は読まない。

## 公開リポジトリの決まり

リポジトリは公開で、コミットと Issue・PR の本文は誰でも読める。決まりの正は [CLAUDE.md](../CLAUDE.md) の「リポジトリに置くもの / 置かないもの」。要点:

- パスワード・トークン・署名付き URL・SA の鍵を置かない。プロジェクト ID・プロジェクト番号・オーナー名・リポジトリ名・バケット名・メールアドレス・ホームディレクトリのパスも書かず、コードは変数で受け取り、文書の例は `<PROJECT_ID>`・`$P`・`<OWNER>/<REPO>` にする
- ログ・クエリの結果・plan の全文を貼らない（件数・要約・該当行の抜粋にする）
- コミットの前に `git status --porcelain` で上のものが無いことを見て、`git add -A`・`git add .` を使わず 1 つずつ `git add` する

## Issue から PR まで

Claude Code のスキル `.claude/skills/implement-issue/SKILL.md` が、Issue のリファインメント → 調査・計画 → 実装 → 独立レビュー → PR →（承認後）デプロイまでの進め方を決めている（小さい Issue 向けの簡易版は `.claude/skills/implement-issue-lite/SKILL.md`）。

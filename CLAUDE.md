# optionchan-gcp

日経225オプションの価格を JPX のページから毎分取得し、GCS 経由で BigQuery に蓄積するツール。GCP プロジェクト・GitHub リポジトリは clone した人それぞれのもの（コマンドではプロジェクトを `gcloud config get-value project` か環境変数 `GCP_PROJECT_ID`、リポジトリを `gh repo view --json nameWithOwner -q .nameWithOwner` で取る。fork を clone して upstream がある場合は `gh repo set-default` で自分のリポジトリを選び、取った値を表示して確かめる）。構成は `README.md`、構築は `docs/setup.md`、開発・テスト・デプロイは `docs/development.md`、運用は `docs/runbook.md` を参照（`memo.txt` は旧系のコマンド）。

## リポジトリに置くもの / 置かないもの

このリポジトリは **公開（PUBLIC）**。コミット内容と Issue / PR の本文は誰でも読める。

**置くもの**: コード、`schema/`、`.claude/skills/`、この `CLAUDE.md`、テスト用 fixture（JPX のページはテストに必要な部分だけに絞る。丸ごと保存しない）。

**置かないもの**（コミットにも Issue / PR 本文にも入れない）:

- `auth.txt`（共通パスワード）の中身・ファイル自体、アクセストークン、署名付き URL、サービスアカウント鍵
- `.claude/settings.local.json`（個人の gh / gcloud アカウント切替）
- スキルの作業用ファイル（`.claude/handoff/`、`.claude/worktrees/`、一時ディレクトリ）
- 個人アカウントに結び付く情報（メールアドレス、別組織のアカウント名、ホームディレクトリのパス）
- 特定の利用者の値（GCP プロジェクト ID・プロジェクト番号、GitHub のオーナー名・リポジトリ名、それらから作ったバケット名など）。コード・設定は変数で受け取り、docs の例は `<PROJECT_ID>`・`$P`・`<OWNER>/<REPO>` などのプレースホルダにする
- ログ・クエリ結果の生の貼り付け（必要なら件数・要約・該当行の抜粋にする）

コミット前に `git status --porcelain` で上記が含まれていないことを確認し、`git add -A` / `git add .` は使わず個別に `git add` する。

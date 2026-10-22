# 構築の手順

clone した人が自分の GCP プロジェクトと GitHub リポジトリで、新系（取り込み・BigQuery・定期クエリ・ビューアー・監視）を最初から作る順番。各段は「やること」の要約と、コマンドのある [runbook](runbook.md) の節への案内だけを書く（コマンドは runbook と二重に持たない）。用語は [glossary.md](glossary.md)。

## この手順について

- **未検証**: 新しいプロジェクトでこの順番を最初から流して確かめていない。順番は Terraform の依存（prod は bootstrap が作る SA・API・バケットを使う）と、runbook の各節に書いてある前提から組んだ
- runbook の手順は、旧系と並行稼働していた元のプロジェクトで行ったもの。手順の中の「要約の目安」（create の件数など）や、旧系に触れる行は新しい環境では合わない。合わない所はこの文書の各段と「自分の環境に要らないもの」に書いた
- 確かめていないことは「未確認」と書いた。流して分かったことは Issue に残し、この文書を直す
- 費用は [README の「費用の目安」](../README.md#費用の目安)

## 前提

- **GCP プロジェクト**: 請求先アカウントをリンクしたもの。bootstrap は所有者がローカルから apply する（runbook の「Terraform」の表）ので、所有者の権限で gcloud にログインしておく
- **GitHub のリポジトリ**: 自分のもの（fork でも、clone して自分のリポジトリに push したものでもよい）。fork を clone して upstream があると `gh repo view` が元のリポジトリを返すことがあるので、`gh repo set-default` で自分のリポジトリを選ぶ（runbook の「GitHub 設定」）
- **ツール**: gcloud（`gcloud beta`・`gcloud alpha` と `bq` も使う）、gh、jq、terraform 1.16.4（`infra/bootstrap/versions.tf`・`infra/prod/versions.tf` の `~> 1.16.4`、google プロバイダ 8.4.0。runbook は `mise exec terraform@1.16.4 -- terraform` で動かす。リポジトリに mise の設定ファイルは無い）。ローカルでテスト・ビルドするなら uv・docker・node も（[development.md](development.md)）。イメージのビルドは deploy workflow が行うので、構築だけならローカルの docker は要らない
- **メール通知チャネル 1 件**: prod の Terraform は Cloud Monitoring のメール通知チャネルを `type = "email"` の data source で引く（`infra/prod/monitoring.tf`）。無い・2 件以上あると prod の plan がエラーになるので、「3. 最初の PR と prod の apply」より前に、コンソールの Monitoring の通知チャネルの画面でメールのチャネルを 1 件作る（作る手順は runbook に無い）
- **API**: bootstrap が有効にする API は `infra/bootstrap/apis.tf` の 10 個だけで、ファイルのコメントのとおり他は元のプロジェクトで有効化済みだったもの。新しいプロジェクトでほかに有効にする API（Cloud Scheduler・BigQuery・Cloud Storage など、prod が使うもの）があるかは未確認。plan・apply が API の未有効で止まったら `gcloud services enable` で有効にする。予算を作るときは `billingbudgets.googleapis.com` も要る（runbook の「コスト」の「予算」）
- **Cloud Scheduler のサービスエージェント**: bootstrap（`infra/bootstrap/ingest.tf`）は Cloud Scheduler のサービスエージェントに権限を付けるが、`cloudscheduler.googleapis.com` は `apis.tf` に無い。新しいプロジェクトではエージェントがまだ無く、初回 apply がここで止まる見込み（未確認）。「1.」の前に `gcloud services enable cloudscheduler.googleapis.com` で有効にしておく

## 変数

runbook と同じ名前を使う（「コスト」の「共通の変数」と「Terraform」の「共通の変数」、「新しい取り込み（ingest）」の「変数」）。段をまたいで使うのは次の 4 つ。

```sh
P=$(gcloud config get-value project 2>/dev/null); : "${P:?P（GCP プロジェクト ID）を設定する}"; echo "P=$P"
L=asia-northeast1
REPO=$(gh repo view --json nameWithOwner -q .nameWithOwner); : "${REPO:?gh repo set-default で自分のリポジトリを選ぶ}"; echo "REPO=$REPO"
R=repos/$REPO
```

ほか（`TF`・`TF_VAR_project_id`・`GCP_PROJECT_ID`・ADC が無いときのトークン）は runbook の「Terraform」の「共通の変数」を、`BA`・`PN`・`CH`（請求先アカウント・プロジェクト番号・通知チャネル）は「コスト」の「共通の変数」を、新しいシェルごとにそのまま流す。バケットの名前は `$P` から組み立てる（prod の state は `<PROJECT_ID>-tfstate`、bootstrap の state は `<PROJECT_ID>-tfstate-bootstrap`、生 HTML は `<PROJECT_ID>-raw`）。

### Terraform の変数

`infra/bootstrap/variables.tf` と `infra/prod/variables.tf` の変数。プロジェクト ID・リポジトリ名はコードに既定値を持たない。

| 変数 | ルート | 渡し方・値 |
|---|---|---|
| `project_id` | bootstrap・prod | GCP プロジェクト ID。既定値なし。ローカルは環境変数 TF_VAR_project_id、CI は repository variable GCP_PROJECT_ID から |
| `region` | bootstrap・prod | 既定 asia-northeast1。データセットの場所（schema/tokyo/tables.json）や runbook の L も同じ値なので、変えない（変えたときの動作は未確認） |
| `github_repository` | bootstrap | 自分のリポジトリ（owner/name の形）。環境変数 TF_VAR_github_repository に REPO を入れる |
| `github_repository_id` | bootstrap | リポジトリの数値 ID。コード・tfvars に置かず、apply のたびに gh api から環境変数で渡す |
| `github_owner_id` | bootstrap | リポジトリ所有者の数値 ID。上と同じ |
| `viewer_iap_members` | bootstrap | IAP 越しにビューアーを開けるプリンシパルの一覧。既定値なし。infra/bootstrap/ に local.auto.tfvars（gitignore 済み。例は local.auto.tfvars.example）を作って書く。ビューアーのサービスができる前は空の一覧、できた後に自分（user: と自分のメール）を入れる（「4. ビューアーの IAP」） |
| `ingest_image` | prod | 取り込みのイメージ。CI が .github/scripts/image.sh で決めて渡す（手で渡すのは CI が使えないときだけ。runbook の「Terraform」の「CI が使えないときの手動 plan / apply」） |
| `viewer_image` | prod | ビューアーのイメージ。上と同じ |
| `ingest_paused` | prod | 既定 false。true で Scheduler の ingest を止める（runbook の「新しい取り込み（ingest）」の「停止・再開」） |
| `legacy_stopped` | prod | 既定 false。true で旧系向けのアラート 2 本を作らず、新旧の突き合わせの定期クエリ 3 本を無効にする。新しい環境での値は「自分の環境に要らないもの」 |

## 1. tfstate と bootstrap の初回 apply

1. runbook の「Terraform」の「共通の変数」を流す
2. runbook の「Terraform」の「bootstrap の初回 apply」の手順 1〜4 を行う。bootstrap 用の state のバケットを gcloud で作り（`infra/bootstrap/imports.tf` が取り込む）、GitHub の数値 ID を環境変数で渡し、local.auto.tfvars に `viewer_iap_members = []` を書いて、plan の要約を見てから apply し、差分 0 を確かめる。手順 0（作業前の記録）は旧系のアラート・関数の記録なので要らない
3. この 1 回の apply で bootstrap の全部（API、prod の state のバケット、WIF のプール・プロバイダ、SA `sa-tf-plan`・`sa-tf-apply`・`sa-ingest`・`sa-scheduler`・`sa-sq-dts`・`sa-viewer`、生 HTML のバケット、それらへの付与）ができる。runbook の「Terraform」の「bootstrap の変更（#33: 取り込み用の SA・バケット・付与）」と、「日次の品質チェックと新旧の突き合わせ（#40）」の中の bootstrap の変更（#40）の小節は、既存の環境に後から足したときの記録で、新しい環境では初回 apply に含まれる。どの節の「要約の目安」の件数も新しい環境では合わない
4. runbook の「Terraform」の「prod の state の初期化（#10、一度だけ）」で、所有者の資格情報で prod の空の state を作る（`sa-tf-plan` は state を読むだけなので、無いと CI の `terraform-plan` が失敗する）

注意（未確認）: `infra/bootstrap/sq.tf` の `sq_legacy_reader` は、旧系のデータセット `optionchan`（US）の 3 表に `sa-sq-dts` の読み取りを条件なしで付ける。旧系の無いプロジェクトでは対象の表が無いので、この付与で apply が失敗する見込み。どう避けるか（コードを直すか、空の表を作るか）は決めていない。Scheduler のサービスエージェントが無いときは runbook の「Terraform」の「bootstrap の変更（#33: 取り込み用の SA・バケット・付与）」の最後の 1 行で作る。

## 2. GitHub の設定

1. runbook の「Terraform」の「GitHub の設定（bootstrap の後、PR を出す前）」を行う。許可アクション、secret と repository variables（値は bootstrap の output と `$P` から。表示しない）、Environment `prod`（`master` だけ、承認者は自分）
2. runbook の「GitHub 設定」の表の残り（workflow の既定権限 read、fork からの PR の承認、SHA のピン留め、secret scanning）を合わせる。表の項目のうちコマンドが runbook に無いものは、リポジトリの設定画面で合わせて「GitHub 設定」の「確認」で確かめる
3. ブランチ保護（必須 check `pytest`・`terraform-plan`）は、最初の PR の CI が一度成功してから「GitHub 設定」の「ブランチ保護の設定」で入れる（一度も報告されていない check は名前の誤りに気づけないため）

workflow（`.github/workflows/ci.yml`・`.github/workflows/deploy.yml`）が読む値:

| 種類 | 名前 | 中身 |
|---|---|---|
| repository variable | `GCP_PROJECT_ID` | GCP プロジェクト ID |
| repository variable | `TF_STATE_BUCKET` | prod の state のバケット（`<PROJECT_ID>-tfstate`） |
| repository variable | `GCP_SA_TF_PLAN` | plan 用の SA のメール（bootstrap の output `sa_tf_plan_email`） |
| repository variable | `GCP_SA_TF_APPLY` | apply 用の SA のメール（bootstrap の output `sa_tf_apply_email`） |
| secret | `GCP_WIF_PROVIDER` | WIF のプロバイダの完全名（bootstrap の output `wif_provider_name`。プロジェクト番号を含む） |

確認（名前だけを出す。値は表示しない）:

```sh
gh variable list -R "$REPO" --json name -q '.[].name' | sort; gh secret list -R "$REPO" --json name -q '.[].name'
```

## 3. 最初の PR と prod の apply

PR を出す前にそろえるもの: 前提のメール通知チャネル、「1.」の prod の state、「2.」の設定。apply を承認する前には「4. ビューアーの IAP」の 1〜4 も済ませる（IAP のサービスエージェントが無いと、prod の apply でビューアーの invoker の付与が失敗する）。

1. 何かの PR を `master` に出す（中身は何でもよい）。`ci` workflow が `pytest`・`ingest-image`・`viewer-image`・`terraform-plan` を回し、`terraform-plan` が prod の plan の要約（アドレスと操作だけ）を PR にコメントする。初回は prod の全部が create で並び、イメージは「未作成」になる（[development.md の「CI」](development.md#ci)）
2. 要約に意図しない delete・replace が無いことを見てマージする。CI が一度成功したので、ここで「2.」の 3（ブランチ保護）を入れる
3. マージで `deploy` workflow の `plan` job が差分を見つけ、`apply` job が Environment `prod` の承認を待つ。承認は Actions の画面か runbook の「Terraform」の「流れ」のコマンド。承認すると `apply` job が取り込みとビューアーのイメージをビルド・検査・push してから、digest で apply する（[development.md の「デプロイ」](development.md#デプロイ)）。PR を経ずに試すなら `gh workflow run deploy.yml --ref master` でも同じ plan が動く

apply で、Artifact Registry `optionchan`、データセット `optionchan_tokyo` と表・ビュー・テーブル関数、Cloud Run `ingest`・`viewer`、ジョブ `reprocess`・`calendar-sync`、Scheduler `ingest`、定期クエリ（`optionchan quality-daily`・`optionchan atm-hourly`・`optionchan atm-cm` と旧系向けの `optionchan legacy-*`）、アラートができる（名前は `infra/prod/` の各 .tf）。Scheduler `ingest` は `ingest_paused` が既定 false なので apply の直後から毎分動き、取引時間中は取り込みを始める。

## 4. ビューアーの IAP

runbook の「ビューアー（#13）」の「初回のデプロイ（所有者の手順）」の手順 1〜7 を、「3.」の apply をはさんで行う。新しい環境での対応:

1. 手順 1 の bootstrap（1 回目、`viewer_iap_members = []`）は「1.」で済んでいる。続く IAP のサービスエージェントの作成（1 行）だけを行う
2. 手順 2: OAuth 同意画面（コンソール）。対象は外部。公開ステータスが「テスト中」なら、ビューアーを開くアカウントをテストユーザーに足す
3. 手順 3: OAuth クライアントの作成（コンソール。組織の無いプロジェクトでは画面でしか作れない）。クライアント ID とシークレットはどこにも貼らない
4. 手順 4: asia-northeast1 の Cloud Run の単位で IAP に OAuth クライアントを設定する（端末）
5. 手順 5 は「3.」の apply の承認にあたる
6. 手順 6: local.auto.tfvars の `viewer_iap_members` に自分（user: と自分のメール）を入れて、bootstrap をもう一度 apply する（2 回目）。要約はメールを含むので貼らない
7. 手順 7: 「ビューアー（#13）」の「確認」

runbook の手順 4 と「代替」の説明にある旧ビューアー（App Engine の IAP）への影響の話は、旧系の無い環境では関係ない。開き方・設定は [viewer.md の「開き方」](viewer.md#開き方)。

## 5. 取引カレンダーの最初の版

runbook の「取引カレンダー（#63）」の「デプロイ後（初回）」で、ジョブ `calendar-sync` を `--dry-run` で流してから本番で流し、ビュー `trading_calendar_latest` の休業日が JPX「祝日取引」のページと一致することを見る。流さなくても毎日 06:40 JST の取り込みのリクエストが同じことをするが、それまでの間 ingest は休業日なし（土日以外はすべて取引日）として動き、読み直すたびに ERROR `calendar_unavailable` を出す（新系の M2 が鳴る）。

過去の年の行（同じ節の「過去年の追加（#67）」）は、新しい環境の取り込みは今日からなので、過去のデータを入れないなら要らない。

## 6. 監視・予算・クエリ量の上限

- **アラート**: 「3.」の apply で Terraform が作る（新系の `ingest_m1`・`ingest_m2`、`legacy_stopped` が false なら旧系の m1・m2 も）。runbook の「新しい取り込みの監視（#35）」の「確認（apply の後）」で、2 本が有効で通知チャネルが前提の 1 件であることを見る。メールが届くことまで見るなら同じ節の「試験発報」
- **予算**: Terraform の管理外。runbook の「コスト」の「予算」で作る。金額・しきい値は自分で決める（runbook の 500 円・50 / 80 / 100% は元の環境の値）
- **クエリ量の上限**: Terraform の管理外。runbook の「コスト」の「BigQuery のクエリ量上限」で、プロジェクトの 1 日のクエリ量の上限を設定する（変更には所有者の権限が要る）。ビューアーの 1 日の上限（`DAILY_BYTES_BILLED`）はこれより小さくする（[viewer.md の「設定（環境変数）」](viewer.md#設定環境変数)）

## 7. 動いていることの確認

読み取りだけで確かめる。各節の「確認」のコマンドは runbook にある。

- 取り込み: runbook の「新しい取り込み（ingest）」の「デプロイ直後の確認」（構成・権限・取引時間中の取り込み）
- ビューアー: 「4.」の手順 7
- 定期クエリ: 「日次の品質チェックと新旧の突き合わせ（#40）」・「1 時間足（#49）」・「30 日固定の ATM IV（#100）」のそれぞれの「確認（prod の apply の後）」。1 日 1 回のものは翌朝 07 時台の後に見る
- データ: 取引時間中なら、直近 1 時間のスナップショットがある（0 より大きい）。読み方は [data.md の「まず読むもの（テーブル関数）」](data.md#まず読むものテーブル関数)

```sh
gcloud scheduler jobs describe ingest --location $L --project $P --format='value(state)'   # ENABLED
bq query --location=$L --use_legacy_sql=false "SELECT COUNT(*) AS snapshots FROM \`$P.optionchan_tokyo.latest_runs\`(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR), CURRENT_TIMESTAMP())"
```

## 自分の環境に要らないもの

旧系（Cloud Functions・App Engine の取り込みとビューアー）と、元の環境の過去データのための節・設定。新しい環境では読まない・作らない。

- **旧系**: runbook の「監視アラート」（旧系のアラート 2 本）、「サービスアカウント鍵」（旧系の SA の鍵）、「旧系の停止（#14、5a）」。リポジトリの `appengine/`・`functions/`・`functions_py/`・`memo.txt` も旧系
- **新旧の突き合わせ**: runbook の「日次の品質チェックと新旧の突き合わせ（#40）」のうち `legacy-*` の 3 本と、その結果の表 `legacy_compare_daily`・作業用データセット `optionchan_legacy_stage`・`optionchan_legacy_copy`。「1 時間足（#49）」の「旧系との突き合わせ」・「過去分の移行（#12 の後）」も同じ
- **過去データの移行・補正・穴埋め**: runbook の「過去データの移行（#12）」・「過去データの補正（#4、3b）」・「DataCube の歩み値での穴埋め（#111）」・「DataCube の歩み値での行の作成（#114）」（購入した DataCube のデータを使う）、「欠けの一覧の表 data_gaps（#123）」（旧系の時代の欠け。表は空でもビューアーは動く）。「1 時間足（#49）」の「mid IV の過去分（#98）」と「30 日固定の ATM IV（#100）」の「過去分」も、過去のデータを入れないなら要らない
- **一度きりの取り込み・撤去**: runbook の「Terraform」の「アラートの取り込み（#10、一度だけ）」と secret `TF_ALERT_POLICY_IDS`。workflow は secret が無ければ空の map（`{}`）を渡すので、無くてよい（`.github/workflows/ci.yml`・`.github/workflows/deploy.yml` の plan・apply の step）。「取引カレンダー（#63）」の「TOML と #66 の仕組みの撤去（#75、一度だけ）」も要らない

`legacy_stopped` の値（未確認）: 旧系の無い環境では、旧系向けのアラート 2 本（`monitoring/m1-liveness.json`・`monitoring/m2-errors.json`。旧系の関数を見る）と `legacy-*` の定期クエリ 3 本は意味が無く、`legacy-stage` は旧系のデータセット `optionchan` を読むので毎日失敗して新系の M2 が鳴る見込み。`legacy_stopped = true` にするとアラートは作られず定期クエリは無効になるが、作業用データセットと定期クエリの定義は作られる。既定値は false で、`tests/test_infra.py` が既定 false を検査しているので、既定値を変えると CI の `pytest` が落ちる。新しい環境でどうするのがよいか（true にしてテストも直すか、旧系向けの定義を消すか）と、旧系のデータセットが無い環境でこれらの作成が通るかは確かめていない。

## 後片付け

全体を消す手順は runbook に無い。データを持つ資源（データセット・表・バケット・state のバケット・SA）は `prevent_destroy` や `deletion_policy = "PREVENT"`・`deletion_protection` で、Terraform からは消せないようにしてある（runbook の「新しい取り込み（ingest）」の「切り戻し」）。止める・一部を消す手順は次の節にある。

- 取り込みを止める: runbook の「新しい取り込み（ingest）」の「停止・再開」（`ingest_paused`）。計算資源（Scheduler・Cloud Run・テーブル関数）を消すのは同じ節の「切り戻し」の R3
- アラート: 「新しい取り込みの監視（#35）」の「削除・切り戻し」
- 定期クエリ: 「日次の品質チェックと新旧の突き合わせ（#40）」・「1 時間足（#49）」・「30 日固定の ATM IV（#100）」のそれぞれの「停止・削除」
- ビューアー: 「ビューアー（#13）」の「切り戻し・撤去」（OAuth クライアントの削除を含む）
- 予算・クエリ量の上限: 「コスト」（Terraform の管理外なので、消すなら手で）

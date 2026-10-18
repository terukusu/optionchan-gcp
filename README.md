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
  * 時間外・休業日の判定: 立会時間はコードの定数（`settings.py`）、休業日は BigQuery の取引カレンダー（ビュー `trading_calendar_latest`）だけを正として読む（#63・#75。JPX から毎日自動で取り込む）。読めないときは直前の値、一度も読めていなければ休業日なしで ERROR。`INGEST_DRY_RUN=1` では読まない（休業日なし）。docs/runbook.md の「取引カレンダー（#63）」

* apps/viewer
  * 新しいビューアー（#13 / #52。Python 3.13・uv・Flask）。Cloud Run `viewer`（asia-northeast1）に IAP（Cloud Run への直接設定）の後ろでデプロイする（#53）。アプリに認証は無いので、IAP なし・`allUsers` で公開しない。デプロイ・オーナーの手作業（OAuth クライアント）・確認は docs/runbook.md の「ビューアー（#13）」。ページ `/` `/smile` `/atm_iv` と CSV の API `/smile_data` `/atm_iv_data?d=&n=` は現行（appengine/ + HTTP 関数）と同じ URL・同じ CSV の形式で（旧型の ATM 推移 `/atm`・`/atm_data` は移さない。#58）、BigQuery の新系（`optionchan_tokyo` の最新版のテーブル関数と 1 時間足 `atm_iv`）を直接読む
  * `/atm_iv` は足（1分・5分・15分・1時間・日足）を切り替えられるローソク足（#68）。期間の選択は無く、チャートの表示範囲（スクロール・拡縮）で読む範囲が決まり、端に近づくとその外側を追加で読む。足・限月を変えても表示範囲を保つ。読み込み中はチャート上に表示し、失敗したら再試行できる（#77）。データは `/atm_iv_bars?tf=&n=&start=&end=`（`tf` は `1m` `5m` `15m` `1h` `1d`。`start`・`end` は UTC の UNIX 秒で両方か両方なし（なしは足ごとの既定の範囲）、1 回の上限は分足 3 日（31 日前まで）・1 時間足 100 日・日足 400 日、`end` は now に縮める。読んだ範囲を応答のヘッダ `X-Bars-Start`・`X-Bars-End` で返す。CSV は `/atm_iv_data` と同じ列で値の無い足を除く。費用の見積もりは docs/runbook.md の「ビューアー（#13）」の「コスト」）。分足は最新版の ATM の put から 1 分足を作り（5・15 分足はアプリで束ねる）、1 時間足・日足は `/atm_iv_data` と同じ 1 時間足から。日足は取引日（夜間は次の取引日。休業日は取引カレンダー `trading_calendar_latest`）ごとで、画面の日付も取引日（#124）。IV は mid IV（気配の中値 =（bid_iv + ask_iv）/ 2。`&iv=mid`。画面の既定）か約定 IV（`iv` なしの既定）を選べる。mid は分足が最新版の ATM の put から（#93）、1 時間足・日足が mid の 1 時間足 `atm_mid_iv`（定期クエリが作る。2018 年まで作り直し済み。#98）から
  * 限月は 0・1（今日から見て何番目の限月）と「期近つなぎ」（`&n=roll`。#101）。期近つなぎは期近の ATM IV を限月の乗り換えでつないだ長期のチャートで、日足なら 2018-10 まで遡れる。乗り換えの規則: 足の取引日（夜間は次の取引日）が期近の最終取引日の 5 営業日前（残存営業日数 = その取引日より後で最終取引日以下の営業日の数が 5 以下）に達したら 2 限月へ乗り換える（設定 `ROLL_BUSINESS_DAYS`。営業日は土日・取引カレンダーの休業日を除く日で、祝日取引の日は営業日）。乗り換えは取引日の境目（夜間の始まりの 17:00 JST）に来る。選んだ限月の値が無い足・IV が 0 以下の足は除き、期近へ戻さない（その時間は線が途切れる）。`n=roll` の CSV は 5 列の後ろに 6 列目 `last_trading_day`（その足の限月の最終取引日 `YYYY-MM-DD`）を足し、画面は乗り換えた足に印（例: 「12月限」）を付け、凡例にカーソルの足の限月を出す。期間・上限・`iv` の組み合わせは `n=0` と同じ（`/atm_iv_data` は `n=roll` を受け付けない）。残存の短い期近は IV が高く出るので、乗り換えの所に段差が出る（補正はしない）
  * 限月の選択肢には「30日固定」（`&n=cm`。#100）もある。限月ごとにフォワードの位置で補間した ATM IV から、満期 30 日に補間した IV（下の「30 日固定の ATM IV」）で、ATM の行使価格の乗り換え・限月の乗り換えによる段差が無い。値は取り込み 1 回ごとに 1 つで、分足は 1 分 1 本、1 時間足・日足はそれを束ねたもの。表 `atm_cm_iv`（定期クエリ `atm-cm` が 1 時間ごとに足し、毎朝 07:45 の回で前日分を作り直す（#117）。2018-10 まで作り直し済み）と、まだ表に入っていない分（最大 3 日）を生の気配から同じ SQL で計算したものをつなぐ。気配の中値から作るので `iv`（約定 / mid）は使わない（画面は IV の選択を無効にする）。CSV・期間・上限は `n=0` と同じ。表示はローソク足と折れ線（終値を結ぶ）を切り替えられる
  * 配色は既定がダーク。各ページの上の「ライト表示」「ダーク表示」で切り替え、選んだものはブラウザごとに覚える（`static/theme.js`。`localStorage`）。チャートの色もテーマに合わせて塗り直す
  * 「日付（JST）」の欄で日付を選ぶと（他の選択と同じく選んだ時点で。打ち込みは打ち終わってから）、選んでいる足のままその日のまわりへ移動する（その日の 12:00 JST を中心に、分足は ±2〜18 時間・1 時間足は ±3 日・日足は ±60 日を読み直す。そこから左右へスクロールすると続きを読む）。1 時間足でも遠い過去（2020 年など）を見られる。分足は直近 31 日までのまま
  * 足が 0 本のときはチャートの真ん中に理由と、表示できる選び方のボタンを出す。限月 0・1 は今日から数えた限月なので、過去の日付へ移動するとその限月の足が無い（「期近つなぎで表示」）。mid は気配の無い期間がある（「約定で表示」）。移動した範囲に足が無ければ、それより前を探しに行かずに止まる（遠くの足が出て、移動した日付から外れないように）
  * 過去データの欠け（#110。docs/data-gaps.md）を印・帯で出す（#123）。ページを開いたときに `/data_gaps`（表 `data_gaps` の区間。ヘッダなし CSV `prev_at,next_at,cause,status`、時刻は UTC の UNIX 秒、`prev_at` 昇順。値が変わらず保存しなかった区間 `no_change` かつ `none` は出さない。表が空なら空の 200）を 1 回だけ読み、約定（限月 0・1・期近つなぎ）では #114 で約定から作った区間の足の背景に帯、作れなかった区間と mid・30日固定のすべての区間（作った行に気配が無い）は足の下に ■ を付け、凡例にカーソルの足の区間（種類と期間）、足が 0 本の説明に区間の期間を出す。表は所有者が runbook の「欠けの一覧の表 data_gaps（#123）」の手順で入れる（区間の時刻はコードに持たない）。読めなければ帯・印を出さずに今どおり描く
  * `/smile` はスマイルカーブ（#79）。気配の IV から作った曲線（既定は OTM 合成: ATM より下は put・ATM 以上は call の mid IV =（bid_iv + ask_iv）/ 2 を線、bid〜ask の IV をひげ、約定の IV を点。call・put 別にも切り替えられる）を、行使価格の数値の軸に描く。初めの横の範囲は描いている限月の行使価格の全体（#127）。ホイール・ドラッグ・ピンチ・スワイプで拡大縮小・スクロール（サーバーに要求しない）、カーソル位置の行使価格の値をチャートの上に出す。時点は「最新」・前後のステップ（±1分〜±1日）・日時の入力で動かし、「この時点を固定」で 2 時点を重ねて比べる。データは `/smile_snapshot?at=`（`at` は UTC の UNIX 秒、省略で最新、未来は now に縮める、2010-01-01 より前は 400。`at` 以前 14 日で最新の完了したスナップショット（`&dir=next` は `at` 以後 14 日で最初のもの、無ければ `at` 以前で最新。「+」方向の移動が取引の切れ目を 1 回で越える。#80）の 1 時点を、1 行目がメタ（時刻・限月ごとの最終取引日と ATM）、2 行目から限月 × 行使価格 × call / put の縦持ちの CSV で返す。無ければ空の 200）。曲線の規則（IV の 0 は欠損・mid IV・OTM 合成）は `smile.py`。1 行目の 6 列目 `source` は、そのスナップショットの行の `ingest_run_id` が `legacy-fill114-` で始まれば `fill114`（#114 で約定から作った時点。画面のグラフのすぐ上に「この時点は取得できなかったため、別に購入した約定データから作り直しています。売り気配・買い気配は無く、点（約定の IV）だけを表示しています」を出す。#123・#127）、それ以外は空。1 ページで読む時点は 20 まで（費用は docs/runbook.md の「ビューアー（#13）」の「コスト」）。`/smile_data` は旧ビューアーとの見比べ（#54）のために形式を変えずに残す
  * 設定は環境変数: `GCP_PROJECT`（必須）、`BQ_DATASET`（既定 `optionchan_tokyo`）、`BQ_LOCATION`（既定 `asia-northeast1`）、`MAX_BYTES_BILLED`（既定 1 GiB）、`DAILY_BYTES_BILLED`（既定 4 GiB。JST の 1 日にこのプロセスで課金されたバイト数の上限。超えたら API は 429。#95）。SQL は `src/optionchan_viewer/sql/` にだけ置き、値はクエリパラメータで渡す
  * ブラウザ用ライブラリ（Chart.js（`/smile`）・TradingView Lightweight Charts（`/atm_iv`））は `package-lock.json` で固定し、イメージのビルドで同梱する（CDN を使わない）

* schema
  * BigQueryのスキーマ。`schema/tokyo/` は新しい取り込みのテーブル（東京）の定義と設定（`tables.json`）と、日次チェックの結果のテーブル（`quality_daily`・`legacy_compare_daily`、#40）。`schema/ohlc/atm_ohlc.json` は東京の 1 時間足の 3 表（#49）と mid IV の `atm_mid_iv`（#98）の列（旧系の `atm_*.json` と同じ。取り込みのイメージに入る `schema/tokyo/` には置かない）

* sql
  * BigQuery の SQL。`sql/latest/` はスナップショットごとに最新の完了版を選ぶテーブル関数の本文（Terraform が読む。使い方は docs/runbook.md の「最新版の読み方」）
  * `sql/quality/`（日次の品質チェック）・`sql/legacy/`（旧系の前日分を東京へ運んで新旧を突き合わせる）は BigQuery Data Transfer Service の定期クエリの本文（#40。Terraform が読む。確認・再実行は docs/runbook.md の「日次の品質チェックと新旧の突き合わせ（#40）」）
  * `sql/legacy_import/` は旧系の過去データを東京へ移すときに手で流す SQL（コピーの検証・移行・移行の検証、#12。Terraform は読まない。手順は docs/runbook.md の「過去データの移行（#12）」）
  * `sql/ohlc/` は 1 時間足（ATM の IV・プレミアム・行使価格の OHLC、#49）。`atm_hourly.sql` は定期クエリの本文（Terraform が読む）、他は旧系との突き合わせ・過去分の移行で手で流す SQL（docs/runbook.md の「1 時間足（#49）」）
  * `sql/cm/` は 30 日固定の ATM IV（#100）。`atm_cm.sql` は定期クエリの本文（Terraform が読む）、`cm_backfill.sql` は過去分の作り直しで手で流す SQL（docs/runbook.md の「30 日固定の ATM IV（#100）」）。計算（`-- cm:begin`〜`-- cm:end`）はビューアーの `sql/atm_cm.sql` と一字一句同じ

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

## 30 日固定の ATM IV（#100）

`/atm_iv` の ATM IV（その時点で `is_atm` の put の IV）は、原資産が動いて ATM が隣の行使価格に移るとスキューのぶん段差で跳び、限月の乗り換えでも不連続になる。30 日固定の ATM IV は、フォワード価格の位置で補間した ATM IV を、満期 30 日に補間した値で、どちらの段差も無い（日経VI と同じく 30 日の固定満期の考え方。ただし日経VI とは計算が違い、値も一致しない）。合成した指標で、実在の銘柄の値ではない。計算は `sql/cm/atm_cm.sql` の `-- cm:begin`〜`-- cm:end`（取り込み 1 回 = `created_at` ごと）:

1. **使える気配**: bid・ask が両方 0 より大きく、スプレッド（ask − bid）が中値の 30% 以下。薄い時間帯（深夜 1〜6 時など）の極端に低い bid・高い ask は除く（そのまま使うと mid IV が数ポイント跳ぶ）
2. **フォワード F**（限月ごと）: ATM（`is_atm`。2 本なら小さい方）に近い 5 本の行使価格のうち、call・put の両方に使える気配のあるものの `C − P + K`（put-call パリティ。中値。金利は無視）の中央値。3 本に満たなければ出さない。先物（12 月限）と比べて平均の差 +2 円、ばらつき（標準偏差）28 円（2026-10-05〜16。1 本の行使価格だけだと 110〜140 円）
3. **補間 ATM IV**（限月ごと）: ATM に近い 9 本の行使価格の IV（call・put の mid IV =（bid_iv + ask_iv）/ 2 の平均。片方だけならその値）のうち、F 以下で最大・F より上で最小の 2 本を、`log(K / F)` で線形に補間する。片側が無ければ出さない。call と put の平均にするのは、F が行使価格をまたぐときに使う種類（OTM 側）が入れ替わって段差が出ないようにするため
4. **満期 T**: SQ（最終取引日の次の営業日。土日・取引カレンダーの休業日を飛ばす）の 09:00 JST までの暦日の日数。SQ を過ぎた限月は数えず、残りを最終取引日の順に 1 限月・2 限月とする
5. **30 日の IV**: T1 が 3〜30 日なら、総分散 `σ²T` を T について線形に補間する: `σ30 = √( ((T2 − 30)·σ1²·T1 + (30 − T1)·σ2²·T2) / (T2 − T1) / 30 )`（`method = interp`）。SQ の前後は外挿しない（取り込んでいるのは 2 限月だけで、3 限月が無い）:
   - **SQ の直前（T1 が 3 日未満）は 2 限月の補間 ATM IV**（`method = m2`）。残存の短い限月の IV は不安定なので使わない
   - **SQ の直後（T1 が 30 日以上）は 1 限月の補間 ATM IV**（`method = m1`）。SQ の前の `m2` と同じ限月なので、SQ をまたいでも値がつながる（30 日より短い側へ外挿すると、乗り換えの瞬間に 0.8 ほどの段差が出た）
   - 境目でも値はつながる（T1 = 30 で補間の重みは 1 限月だけ、T1 = 3 で 2 限月がほぼすべて）。どれにも当たらなければ NULL（線が途切れる）
   - この間の値は満期が 30 日より長い（または短い）限月の IV で、厳密には 30 日の値ではない（1 か月の中で SQ の前の 3 日と後の数日）

- 表 `atm_cm_iv` には限月ごとの F・補間 ATM IV・T も残す（検証用。画面には出さない）
- 表は定期クエリ `atm-cm` が毎時 :45 に、表の最後の行 − 2 時間（取り込みの遅れの分の重ね。最大 3 日前）からその時刻までを作り直して足す（新しい取り込みが無い時間は何もしない）。毎朝 07:45 の回は前日 07:00〜当日 07:00 を作り直す（#117。窓・スキャン量は docs/runbook.md の「30 日固定の ATM IV（#100）」）
- 確かめたこと: 1 分ごとの変化の絶対値の平均は、限月ごとの補間 ATM IV で ATM の行使価格が変わらない分 0.048・変わった分 0.067（2026-10-05〜16。スプレッドの条件を入れる前の測定。従来の ATM の put は 0.17・0.40）。30 日の IV の 1 分ごとの変化は平均 0.035・99 パーセンタイル 0.23（2026-09-07〜10-16）。1 を超える変化は週末・立会の切れ目をまたぐものだけ（1 分の間で 1 を超えたのは 1 回）。SQ（09-11・10-09）の前後: 最終取引日の日中の終わり → 夜間の始まりで 26.74 → 27.17・25.99 → 26.15
- 出せる割合（2018-10〜。各月 1 日の抜き出し）: 日中・夜間の前半はほぼ 100%。2021〜2025 年は深夜（1〜6 時）に 2 限月の気配が無い（スプレッドが中値の 65〜200%）ので、その時間は NULL になり、1 日で見ると 72〜100%。2018-10-19 より前は行が無く、欠け 1・3（2022-09-14〜2023-04-25 の全欠けと、2024-08 より前の前日比が大きい間）は #114 で作った行に気配が無いので出せない（過去データの欠けは docs/data-gaps.md）

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

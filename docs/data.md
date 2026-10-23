# データの読み方

新系の取り込み（`apps/ingest`）が書く BigQuery のデータを、別のアプリ・クエリから読む人向けの説明。表・テーブル関数・列・時刻・粒度と、30 日固定の ATM IV の計算をまとめる。用語は [glossary.md](glossary.md)、運用（確認・停止・作り直し）は [runbook.md](runbook.md)、過去データの欠けは [data-gaps.md](data-gaps.md)。

- 列の正は `schema/` の JSON（全列に `description` がある）、表の設定の正は `schema/tokyo/tables.json` と `infra/prod/*.tf`。この文書はそれを読みやすくまとめたもので、食い違ったら元のファイルが正
- クエリの例のプロジェクトは `<PROJECT_ID>`（自分のプロジェクト ID に置き換える）

## データセット

| 項目 | 値 | 定義 |
|---|---|---|
| データセット | `optionchan_tokyo` | `schema/tokyo/tables.json` の `dataset`（`infra/prod/data.tf` が読む） |
| ロケーション | `asia-northeast1`（東京） | 同上 |
| ストレージの課金 | 物理（`PHYSICAL`） | 同上 |
| タイムトラベル | 168 時間 | 同上 |
| 表の既定の有効期限 | なし | 同上 |

- クエリはロケーション `asia-northeast1` で流す（`bq query --location=asia-northeast1`）
- データを持つ表（取り込みの 4 表・`trading_calendar`・DataCube の 2 表）とデータセットは、Terraform の `prevent_destroy` と削除の保護で二重に止めてある（`infra/prod/data.tf`・`calendar.tf`・`datacube.tf`）

## まず読むもの（テーブル関数）

取り込みの生の表（`spot_price`・`future_price`・`option_price`・`ingest_runs`）は、回収・再処理が同じスナップショットを新しい `ingest_run_id` で追記するので、同じスナップショットの行が重複しうる。書き込みの途中で失敗した実行の行（完了記録の無い残骸）も残る。**生の表を直接数えず、次のテーブル関数で読む**（`infra/prod/data.tf`、本文は `sql/latest/`）。

| テーブル関数 | 返すもの |
|---|---|
| `latest_runs(start_at, end_at)` | `snapshot_at` が `[start_at, end_at)` の `ingest_runs` のうち、スナップショットごとに最新の 1 件（`finished_at` の降順、同時刻なら `ingest_run_id` の降順） |
| `spot_price_latest(start_at, end_at)` | `created_at` が `[start_at, end_at)` の `spot_price` のうち、`latest_runs` と同じ選び方の実行の行だけ |
| `future_price_latest(start_at, end_at)` | 同じく `future_price` |
| `option_price_latest(start_at, end_at)` | 同じく `option_price` |

- 引数はどちらも `TIMESTAMP` で必須。範囲は `start_at` を含み `end_at` を含まない
- 列は元の表と同じ（`SELECT t.*`）
- `ingest_runs` に無い実行の行は結合で落ちる（途中で失敗した実行の行は見えない）
- ビューでなくテーブル関数にしているのは、期間を必須にして `option_price` のパーティションの絞り込み（`require_partition_filter`）と `ingest_runs` の全件のスキャンを避けるため（`infra/prod/data.tf` のコメント）
- 確かめ方（行数の一致・重複 0・所属なし 0）は runbook の「新しい取り込み（ingest）」の「最新版の読み方（#34）」

## 表の一覧

すべて `optionchan_tokyo` の中。パーティションの「日」「月」は BigQuery の TIMESTAMP の列なので UTC の日・月で切れる（JST の 1 日は UTC の 2 日にまたがる）。

| 名前 | 中身 | 粒度（1 行） | パーティション | クラスタ | 誰が書くか |
|---|---|---|---|---|---|
| `spot_price` | 日経平均（現物）の値 | スナップショット 1 回 | 日（`created_at`） | なし | Cloud Run `ingest`（毎分の取り込み・毎時の回収）とジョブ `reprocess`（手動の作り直し） |
| `future_price` | 日経225先物の値 | スナップショット 1 回 | 日（`created_at`） | `contract_month` | 同上 |
| `option_price` | 日経225オプションの値（2 限月 × call / put × 行使価格） | スナップショット × 限月 × call / put × 行使価格 | 日（`created_at`）。**`require_partition_filter = true`** | `last_trading_day`・`target_price` | 同上 |
| `ingest_runs` | 取り込み実行の完了記録（3 表に書き終わった後に 1 行） | 取り込み実行 1 回（同じスナップショットに複数ありうる） | 日（`snapshot_at`） | なし | 同上 |
| `latest_runs` | テーブル関数（上の「まず読むもの」） | — | — | — | Terraform（`sql/latest/`） |
| `spot_price_latest` | テーブル関数 | — | — | — | 同上 |
| `future_price_latest` | テーブル関数 | — | — | — | 同上 |
| `option_price_latest` | テーブル関数 | — | — | — | 同上 |
| `atm_iv` | ATM の put の IV（約定 IV）の 1 時間足 | 限月 × 1 時間 | 月（`started_at`） | `last_trading_day` | 定期クエリ `atm-hourly`（毎日 07:40 JST、`sql/ohlc/atm_hourly.sql`） |
| `atm_option_price` | ATM の put のプレミアムの 1 時間足 | 限月 × 1 時間 | 月（`started_at`） | `last_trading_day` | 同上 |
| `atm_target_price` | ATM の行使価格の 1 時間足 | 限月 × 1 時間 | 月（`started_at`） | `last_trading_day` | 同上 |
| `atm_mid_iv` | ATM の put の mid IV の 1 時間足（旧系に無い表） | 限月 × 1 時間 | 月（`started_at`） | `last_trading_day` | 同上 |
| `atm_cm_iv` | 30 日固定の ATM IV と、限月ごとのフォワード・補間 ATM IV・SQ までの日数 | 取り込み 1 回（`created_at`） | 月（`created_at`） | なし | 定期クエリ `atm-cm`（毎時 :45、`sql/cm/atm_cm.sql`） |
| `trading_calendar` | 取引カレンダーの版の履歴（追記のみ） | 版 × 休業日に当たる平日 1 日 | なし | なし | Cloud Run `ingest`（毎日 06:40 JST のリクエスト）と手動のジョブ `calendar-sync` |
| `trading_calendar_latest` | ビュー。`trading_calendar` の最新版（`version_at` が最大の版）の行だけ | 同上 | — | — | Terraform（`sql/calendar/trading_calendar_latest.sql`） |
| `data_gaps` | 過去データの欠けの区間の一覧 | 欠けの区間 1 つ | なし | なし | 所有者が手で入れる（`sql/gaps/gaps.sql` の結果を `bq load --replace`。runbook の「欠けの一覧の表 data_gaps（#123）」） |
| `quality_daily` | 日次の品質チェックの結果（運用用） | 窓の日付 1 日 | なし | なし | 定期クエリ `quality-daily`（毎日 07:15 JST、`sql/quality/quality_daily.sql`） |
| `legacy_compare_daily` | 新旧の突き合わせの結果（運用用。旧系の並行稼働の間だけ） | 窓の日付 1 日 | なし | なし | 定期クエリ `legacy-compare`（毎日 07:30 JST、`sql/legacy/compare.sql`） |
| `dc_option_tick` | J-Quants DataCube の日経225オプションの歩み値（運用用。所有者が購入した非公開のデータ） | 約定 1 件 | 月（`trade_date`）。`require_partition_filter = true` | `contract_month`・`put_call_type`・`exercise_price` | 所有者が月ごとに `bq load --replace`（runbook の「DataCube の歩み値での穴埋め（#111）」） |
| `dc_future_tick` | J-Quants DataCube の日経225先物（ラージ）の歩み値（同上） | 約定 1 件 | 月（`trade_date`）。`require_partition_filter = true` | `contract_month` | 所有者が月ごとに `bq load --replace`（runbook の「DataCube の歩み値での行の作成（#114）」） |

- 定期クエリの時刻・窓は `infra/prod/sq.tf` の冒頭のコメントと各 SQL の冒頭が正。スケジュールは UTC で書いてある（JST − 9 時間）
- `quality_daily`・`legacy_compare_daily`・`dc_option_tick`・`dc_future_tick` はデータを読む人向けではない（品質・移行・穴埋めの記録）。DataCube の 2 表は自己使用のライセンスのデータで、値を第三者へ渡さない・公開しない。自分の環境で作った場合は空（`docs/setup.md` の手順では入れない）
- 取り込みの 4 表には、新系の取り込みの行のほかに、旧系から移した過去の行（`ingest_run_id` が `legacy-` で始まり、`ingest_runs.trigger = legacy_import`。#12）と、DataCube の歩み値から作った行（`ingest_run_id` が `legacy-fill114-` で始まる。#114）がある。どちらも最新版のテーブル関数で同じように読める

## 主な表の列

列名・型・モード・説明は `schema/` の JSON から写した（説明は短くしたものがある）。型の `INTEGER`・`FLOAT`・`BOOLEAN` は BigQuery の `INT64`・`FLOAT64`・`BOOL`。

### option_price

列の正は `schema/tokyo/option_price.json`。

| 列 | 型 | モード | 説明 |
|---|---|---|---|
| `type` | INTEGER | NULLABLE | 1 = call、2 = put（`dc_option_tick.put_call_type` とは逆） |
| `target_price` | INTEGER | REQUIRED | 行使価格 |
| `is_atm` | BOOLEAN | NULLABLE | ATM なら TRUE（ページの ATM の印）。品質チェックの `atm_not_1` は限月 × call / put で 1 行でない組を数える。2 行の版もあり、`sql/cm/atm_cm.sql` は小さい方を使う |
| `price` | INTEGER | NULLABLE | オプションのプレミアム（約定値） |
| `price_time` | DATETIME | NULLABLE | 値がついた時刻（JST の壁時計。ページの表示に年を補ったもの） |
| `diff` | INTEGER | NULLABLE | 前日比 |
| `diff_rate` | FLOAT | NULLABLE | 騰落率 |
| `iv` | FLOAT | NULLABLE | 約定値のインプライド・ボラティリティ（約定 IV） |
| `bid` | INTEGER | NULLABLE | 買い気配（買い呼び値） |
| `bid_volume` | INTEGER | NULLABLE | 買い注文量 |
| `bid_iv` | FLOAT | NULLABLE | 買い気配の IV |
| `ask` | INTEGER | NULLABLE | 売り気配（売り呼び値） |
| `ask_volume` | INTEGER | NULLABLE | 売り注文量 |
| `ask_iv` | FLOAT | NULLABLE | 売り気配の IV |
| `volume` | INTEGER | NULLABLE | 取引高 |
| `positions` | INTEGER | NULLABLE | 建玉残 |
| `quotation` | INTEGER | NULLABLE | 清算値 |
| `quotation_date` | DATE | NULLABLE | 清算日 |
| `delta` | FLOAT | NULLABLE | デルタ |
| `gamma` | FLOAT | NULLABLE | ガンマ |
| `theta` | FLOAT | NULLABLE | セータ |
| `vega` | FLOAT | NULLABLE | ベガ |
| `last_trading_day` | DATE | REQUIRED | 最終取引日（限月の識別に使う） |
| `created_at` | TIMESTAMP | REQUIRED | スナップショットの時刻（1 限月ページの最終更新時刻）。パーティションの列 |
| `page_updated_at` | TIMESTAMP | NULLABLE | この行を取ったページの最終更新時刻。2 限月の行だけ `created_at` と違いうる |
| `price_time_raw` | STRING | NULLABLE | ページの約定日時の表示（`MM/DD HH:MM`、年なし）。`price_time` の元。#4 で補正した移行行（`parser_version = legacy-fix4-1`）は補正前の `price_time` |
| `ingest_run_id` | STRING | NULLABLE | 取り込み実行の ID（UUID4。旧系から移した行は `legacy-<created_at の UTC>`）。`ingest_runs` と結合して最新版を選ぶ |
| `parser_version` | STRING | NULLABLE | 解析コードの版 |
| `source_object` | STRING | NULLABLE | 元の生 HTML の GCS のオブジェクト（バケット・名前・generation） |

- 1 スナップショットの限月は 2 つ（1 限月・2 限月。品質チェックの `months_not_2` が検査）
- 最新版のキーは（`created_at`・`type`・`target_price`・`last_trading_day`）（品質チェックの `dup_keys` と runbook の確認のクエリ）

### future_price

列の正は `schema/tokyo/future_price.json`。最新版ではスナップショット 1 回 1 行（キーは `created_at`）。

| 列 | 型 | モード | 説明 |
|---|---|---|---|
| `price` | FLOAT | NULLABLE | 現在値 |
| `price_time` | DATETIME | NULLABLE | 値がついた時刻（JST の壁時計） |
| `diff` | FLOAT | NULLABLE | 前日比 |
| `diff_rate` | FLOAT | NULLABLE | 騰落率 |
| `hv` | FLOAT | NULLABLE | ヒストリカル・ボラティリティ |
| `contract_month` | DATE | REQUIRED | 限月 |
| `created_at` | TIMESTAMP | REQUIRED | スナップショットの時刻。パーティションの列 |
| `page_updated_at` | TIMESTAMP | NULLABLE | この行を取ったページの最終更新時刻 |
| `price_time_raw` | STRING | NULLABLE | ページの約定日時の表示（`option_price` と同じ） |
| `ingest_run_id` | STRING | NULLABLE | 取り込み実行の ID（`option_price` と同じ） |
| `parser_version` | STRING | NULLABLE | 解析コードの版 |
| `source_object` | STRING | NULLABLE | 元の生 HTML |

### spot_price

列の正は `schema/tokyo/spot_price.json`。最新版ではスナップショット 1 回 1 行（キーは `created_at`）。

| 列 | 型 | モード | 説明 |
|---|---|---|---|
| `price` | FLOAT | NULLABLE | 現在値 |
| `price_time` | DATETIME | NULLABLE | 値がついた時刻（JST の壁時計） |
| `diff` | FLOAT | NULLABLE | 前日比 |
| `diff_rate` | FLOAT | NULLABLE | 騰落率 |
| `hv` | FLOAT | NULLABLE | ヒストリカル・ボラティリティ |
| `created_at` | TIMESTAMP | REQUIRED | スナップショットの時刻。パーティションの列 |
| `page_updated_at` | TIMESTAMP | NULLABLE | この行を取ったページの最終更新時刻 |
| `price_time_raw` | STRING | NULLABLE | ページの約定日時の表示（`option_price` と同じ） |
| `ingest_run_id` | STRING | NULLABLE | 取り込み実行の ID（`option_price` と同じ） |
| `parser_version` | STRING | NULLABLE | 解析コードの版 |
| `source_object` | STRING | NULLABLE | 元の生 HTML |

### ingest_runs

列の正は `schema/tokyo/ingest_runs.json`。3 表に書き終わった実行だけに 1 行ある。

| 列 | 型 | モード | 説明 |
|---|---|---|---|
| `ingest_run_id` | STRING | REQUIRED | 取り込み実行の ID（UUID4。旧系から移した行は `legacy-<snapshot_at の UTC>`） |
| `snapshot_at` | TIMESTAMP | REQUIRED | スナップショットの時刻（= 3 表の `created_at`）。パーティションの列 |
| `page2_updated_at` | TIMESTAMP | NULLABLE | 2 限月ページの最終更新時刻 |
| `source_object` | STRING | REQUIRED | 元の生 HTML（旧系から移した行は `legacy:optionchan`） |
| `source_generation` | INTEGER | NULLABLE | 元の生 HTML の GCS の generation |
| `parser_version` | STRING | REQUIRED | 解析コードの版（旧系から移した行は `legacy`） |
| `trigger` | STRING | REQUIRED | 起動のきっかけ（`scheduler` / `recovery` / `reprocess`。旧系から移した行は `legacy_import`） |
| `rows_spot` | INTEGER | REQUIRED | `spot_price` に書いた行数 |
| `rows_future` | INTEGER | REQUIRED | `future_price` に書いた行数 |
| `rows_option` | INTEGER | REQUIRED | `option_price` に書いた行数 |
| `started_at` | TIMESTAMP | REQUIRED | 実行の開始時刻（実時刻） |
| `finished_at` | TIMESTAMP | REQUIRED | 3 表への書き込みが終わった時刻（実時刻）。最新版の選択はこの降順 |

### trading_calendar

列の正は `schema/tokyo/trading_calendar.json`。ふつうはビュー `trading_calendar_latest`（同じ列）を読む。1 行は「業務規程上の休業日に当たる平日 1 日」で、土日の行は無い。

| 列 | 型 | モード | 説明 |
|---|---|---|---|
| `version_at` | TIMESTAMP | REQUIRED | 版の作成時刻（取り込みの時刻）。版の識別子。最新版は最大の版 |
| `calendar_date` | DATE | REQUIRED | 休業日に当たる平日（国民の祝日・休日と、年末年始の 12/31・1/1・1/2・1/3）。1 版の中で一意 |
| `is_trading_day` | BOOLEAN | REQUIRED | true = 祝日取引を実施する（取引日）、false = 休業 |
| `status` | STRING | REQUIRED | `confirmed`（確定）/ `planned`（予定） |
| `day_type` | STRING | REQUIRED | `national_holiday`（国民の祝日・休日）/ `year_end`（年末年始の 12/31・1/2・1/3） |
| `name` | STRING | REQUIRED | JPX の表の名称（例: 勤労感謝の日） |
| `note` | STRING | NULLABLE | 実施有無の欄が参照する JPX の注記の本文。無ければ NULL |
| `source` | STRING | REQUIRED | `jpx_table`（JPX の表）/ `rule`（平日の 1/1。規則で足す）/ `history`（過去年の埋め戻し） |
| `covered_until` | DATE | REQUIRED | 版の値（全行で同じ）。確定の最終日。これより後の行は予定 |
| `listed_until` | DATE | REQUIRED | 版の値。JPX の表の最終年の 12/31。これより後の平日は未知 |
| `content_hash` | STRING | REQUIRED | 版の値。版の内容の SHA-256（16 進）。行の欠け・重複の検知に使える |
| `accepted` | STRING | REQUIRED | 版の値。`auto`（検証を通った）/ `override`（手動の `--accept` で越えた） |

- 取引日 = 土日と `is_trading_day = false` の日を除いた日（`docs/architecture/target.md` の [3.2](architecture/target.md#32-データbigquery) の「取引カレンダー（#63）」）
- 掲載範囲は版の `MIN(calendar_date)` の年の 1/1 〜 `listed_until`。その外の平日は未知

### 1 時間足（atm_iv・atm_option_price・atm_target_price・atm_mid_iv）

列の正は `schema/ohlc/atm_ohlc.json`（4 表で同じ列）。最新版の ATM の put（`is_atm AND type = 2`）から、限月 × 1 時間ごとに作る（`sql/ohlc/atm_hourly.sql`）。

| 列 | 型 | モード | 説明 |
|---|---|---|---|
| `open` | FLOAT | NULLABLE | 始値（その 1 時間で `created_at` が最も早い NULL でない値） |
| `high` | FLOAT | NULLABLE | 高値 |
| `low` | FLOAT | NULLABLE | 安値 |
| `close` | FLOAT | NULLABLE | 終値（その 1 時間で `created_at` が最も遅い NULL でない値） |
| `last_trading_day` | DATE | REQUIRED | 最終取引日（限月） |
| `time_frame` | INTEGER | REQUIRED | 時間枠（秒）。いつも 3600 |
| `started_at` | TIMESTAMP | REQUIRED | 開始時刻（UTC の正時 = JST の正時）。パーティションの列 |

- 表ごとの値: `atm_iv` は `iv`、`atm_option_price` は `price`、`atm_target_price` は `target_price`、`atm_mid_iv` は mid IV（`bid_iv`・`ask_iv` が両方 0 より大きいときの（`bid_iv` + `ask_iv`）/ 2）
- キーは（`last_trading_day`・`time_frame`・`started_at`）。値が全部 NULL の 1 時間も行は作る（OHLC が NULL）
- 1 時間の中で ATM の行使価格が変わっても分けない（`atm_target_price` の高値・安値に出る）
- 窓は毎日 07:40 JST の回が前日 07:00〜当日 07:00 JST を置き換える。当日の分はまだ無い（ビューアーの分足は最新版から直接作る）。runbook の「1 時間足（#49）」

### atm_cm_iv

列の正は `schema/cm/atm_cm_iv.json`。取り込み 1 回 1 行。計算は下の「30 日固定の ATM IV の計算」。

| 列 | 型 | モード | 説明 |
|---|---|---|---|
| `created_at` | TIMESTAMP | REQUIRED | 取り込み（スナップショット）の時刻。パーティション（月）の列 |
| `iv30` | FLOAT | NULLABLE | 30 日固定の ATM IV（%）。出せなければ NULL |
| `method` | STRING | NULLABLE | `iv30` の出し方: `m1`（1 限月だけ）/ `interp`（2 限月の補間）/ `m2`（2 限月だけ）。`iv30` が NULL なら NULL |
| `m1_last_trading_day` | DATE | NULLABLE | 1 限月（SQ 前で最終取引日が最も早い限月）の最終取引日 |
| `m1_days` | FLOAT | NULLABLE | 1 限月の SQ（最終取引日の次の営業日の 09:00 JST）までの暦日の日数 |
| `m1_forward` | FLOAT | NULLABLE | 1 限月のフォワード（3 本未満は NULL） |
| `m1_iv` | FLOAT | NULLABLE | 1 限月の補間 ATM IV（%） |
| `m2_last_trading_day` | DATE | NULLABLE | 2 限月の最終取引日 |
| `m2_days` | FLOAT | NULLABLE | 2 限月の SQ までの暦日の日数 |
| `m2_forward` | FLOAT | NULLABLE | 2 限月のフォワード |
| `m2_iv` | FLOAT | NULLABLE | 2 限月の補間 ATM IV（%） |

## 時刻

| 列 | 型 | 中身 |
|---|---|---|
| `created_at`（取り込みの 3 表・`atm_cm_iv`）・`snapshot_at`（`ingest_runs`） | TIMESTAMP | スナップショットの時刻 = 1 限月ページの最終更新時刻（ページの表示時刻を時点にしたもの）。TIMESTAMP なので BigQuery は UTC で表示する。JST の日時は `DATETIME(created_at, 'Asia/Tokyo')` |
| `page_updated_at` | TIMESTAMP | その行を取ったページの更新時刻。2 限月の行だけ `created_at` と違いうる（`ingest_runs.page2_updated_at` も同じ） |
| `price_time` | DATETIME | 値がついた時刻。**JST の壁時計**（タイムゾーンを持たない）。ページの `MM/DD HH:MM` の表示に年を補ったもの |
| `price_time_raw` | STRING | `price_time` の元の表示（年なし） |
| `started_at`・`finished_at`（`ingest_runs`） | TIMESTAMP | 取り込みの実行の実時刻 |
| `started_at`（1 時間足） | TIMESTAMP | 足の開始（UTC の正時 = JST の正時） |
| `version_at`（`trading_calendar`） | TIMESTAMP | 版を取り込んだ時刻 |

- **約 15 分遅れ**: JPX のページ自体が約 15 分遅れのデータで、ページの時刻（`created_at`）は相場の時刻、取り込みの実時刻はその約 15 分後（`docs/architecture/target.md` 3.1、`apps/ingest/src/optionchan_ingest/settings.py` の `SESSION_GRACE_MINUTES` のコメント）。直近の約 15 分の相場は、まだ表に無い
- **立会時間**（JST）: 日中 8:45〜15:45、夜間 17:00〜翌 6:00（`apps/ingest/src/optionchan_ingest/settings.py` の `SESSION_*`）。取り込みは立会の終わりの後も 30 分（`SESSION_GRACE_MINUTES`）続ける。時間外・休業日は取り込まない
- **取引日**: 土日と取引カレンダーの休業日（`is_trading_day = false`）を除いた日。夜間は次の取引日に属する（金曜・休業日の前日の夜間は休み明けの取引日）。ビューアーは JST + 7 時間の日付から土日・休業日を飛ばした最初の日をその時刻の取引日にしている（`apps/viewer/src/optionchan_viewer/bars.py` の `trading_day`）
- 運用の窓（品質チェック・1 時間足・突き合わせ）は `[前日 07:00, 当日 07:00)` JST の 1 日で、窓の日付は開始日（= 日中立会の日付。その後の夜間を含む）。取引日とは数え方が違う

## IV の種類

| 種類 | 列・作り方 | 使う所 |
|---|---|---|
| 約定 IV | `option_price.iv`（約定値の IV） | 1 時間足 `atm_iv`、ビューアーの `iv` なしの既定 |
| mid IV | （`bid_iv` + `ask_iv`）/ 2。両方 0 より大きいときだけ | 1 時間足 `atm_mid_iv`、ビューアーの `iv=mid`、スマイルカーブの線 |
| 30 日固定の ATM IV | `atm_cm_iv.iv30`（気配の中値から作る。下の「30 日固定の ATM IV の計算」） | ビューアーの限月「30日固定」 |

- IV の 0 以下は欠損として扱う（気配の IV が 0 の行がある。ビューアーの `apps/viewer/src/optionchan_viewer/smile.py` の規則）。平均などを取るときは 0 を除く
- `atm_cm_iv` の IV は %（schema の `description`）。`option_price` の IV の列の単位は schema に書いていない（`atm_cm_iv` は `bid_iv`・`ask_iv` をそのまま使って % としている）
- ATM IV（その時点の `is_atm` の put の IV）は、ATM が隣の行使価格に移るときと限月の乗り換えで段差が出る。段差の無い系列が要るときは 30 日固定を使う

## 30 日固定の ATM IV の計算

ATM IV（その時点で `is_atm` の put の IV）は、原資産が動いて ATM が隣の行使価格に移るとスキューのぶん段差で跳び、限月の乗り換えでも不連続になる。30 日固定の ATM IV は、フォワード価格の位置で補間した ATM IV を、満期 30 日に補間した値で、どちらの段差も無い（日経VI と同じく 30 日の固定満期の考え方。ただし日経VI とは計算が違い、値も一致しない）。合成した指標で、実在の銘柄の値ではない。計算は `sql/cm/atm_cm.sql` の `-- cm:begin`〜`-- cm:end`（取り込み 1 回 = `created_at` ごと。最新版 `option_price_latest` の全行使価格の気配から）:

1. **使える気配**: bid・ask が両方 0 より大きく（`bid > 0 AND ask >= bid`）、スプレッド（ask − bid）が中値の 30% 以下。薄い時間帯（深夜 1〜6 時など）の極端に低い bid・高い ask は除く（そのまま使うと mid IV が数ポイント跳ぶ）
2. **フォワード F**（限月ごと）: ATM（`is_atm`。2 本なら小さい方）に近い 5 本の行使価格のうち、call・put の両方に使える気配のあるものの `C − P + K`（put-call パリティ。中値。金利は無視）の中央値（偶数本なら真ん中の 2 つの平均）。3 本に満たなければ出さない。先物（12 月限）と比べて平均の差 +2 円、ばらつき（標準偏差）28 円（2026-10-05〜16。1 本の行使価格だけだと 110〜140 円）
3. **補間 ATM IV**（限月ごと）: ATM に近い 9 本の行使価格の IV（call・put の mid IV =（bid_iv + ask_iv）/ 2 の平均。片方だけならその値。mid IV は使える気配で `bid_iv`・`ask_iv` が両方 0 より大きいときだけ）のうち、F 以下で最大・F より上で最小の 2 本を、`log(K / F)` で線形に補間する。片側が無ければ出さない。call と put の平均にするのは、F が行使価格をまたぐときに使う種類（OTM 側）が入れ替わって段差が出ないようにするため
4. **満期 T**: SQ（最終取引日の次の営業日。土日と `trading_calendar_latest` の全行を飛ばす。祝日取引の実施日（`is_trading_day = true`）も業務規程上は休業日で SQ にならない。#149）の 09:00 JST までの暦日の日数。SQ を過ぎた限月は数えず、残りを最終取引日の順に 1 限月・2 限月とする（ATM の印・F・IV が無い限月も数える）
5. **30 日の IV**: T1 が 3〜30 日なら、総分散 `σ²T` を T について線形に補間する: `σ30 = √( ((T2 − 30)·σ1²·T1 + (30 − T1)·σ2²·T2) / (T2 − T1) / 30 )`（`method = interp`）。SQ の前後は外挿しない（取り込んでいるのは 2 限月だけで、3 限月が無い）:
   - **SQ の直前（T1 が 3 日未満）は 2 限月の補間 ATM IV**（`method = m2`）。残存の短い限月の IV は不安定なので使わない
   - **SQ の直後（T1 が 30 日以上）は 1 限月の補間 ATM IV**（`method = m1`）。SQ の前の `m2` と同じ限月なので、SQ をまたいでも値がつながる（30 日より短い側へ外挿すると、乗り換えの瞬間に 0.8 ほどの段差が出た）
   - 境目でも値はつながる（T1 = 30 で補間の重みは 1 限月だけ、T1 = 3 で 2 限月がほぼすべて）。どれにも当たらなければ NULL（線が途切れる）
   - この間の値は満期が 30 日より長い（または短い）限月の IV で、厳密には 30 日の値ではない（1 か月の中で SQ の前の 3 日と後の数日）

- 表 `atm_cm_iv` には限月ごとの F・補間 ATM IV・T も残す（検証用。ビューアーの画面には出さない）
- 表は定期クエリ `atm-cm` が毎時 :45 に、表の最後の行 − 2 時間（取り込みの遅れの分の重ね。最大 3 日前）からその時刻までを作り直して足す（新しい取り込みが無い時間は何もしない）。毎朝 07:45 の回は前日 07:00〜当日 07:00 を作り直す（#117。窓・スキャン量は `docs/runbook.md` の「30 日固定の ATM IV（#100）」）。重ねの 2 時間より遅れて終わった取り込みは、翌朝 07:45 の回まで古い値のまま
- 同じ計算（`-- cm:begin`〜`-- cm:end`）は `sql/cm/cm_backfill.sql`（過去分の作り直し）とビューアーの `apps/viewer/src/optionchan_viewer/sql/atm_cm.sql`（表にまだ無い直近の分を補う）にもあり、一字一句同じ（`tests/test_cm.py` が検査）
- 確かめたこと: 1 分ごとの変化の絶対値の平均は、限月ごとの補間 ATM IV で ATM の行使価格が変わらない分 0.048・変わった分 0.067（2026-10-05〜16。スプレッドの条件を入れる前の測定。従来の ATM の put は 0.17・0.40）。30 日の IV の 1 分ごとの変化は平均 0.035・99 パーセンタイル 0.23（2026-09-07〜10-16）。1 を超える変化は週末・立会の切れ目をまたぐものだけ（1 分の間で 1 を超えたのは 1 回）。SQ（2 回）の前後の、最終取引日の日中の終わり → 夜間の始まりの変化は 0.5 未満
- 出せる割合（2018-10〜。各月 1 日の抜き出し）: 日中・夜間の前半はほぼ 100%。2021〜2025 年は深夜（1〜6 時）に 2 限月の気配が無い（スプレッドが中値の 65〜200%）ので、その時間は NULL になり、1 日で見ると 72〜100%。2018-10-19 より前は行が無く、欠け 1・3（2022-09-14〜2023-04-25 の全欠けと、2024-08 より前の前日比が大きい間）は #114 で作った行に気配が無いので出せない（下の「過去データの欠け」）

## 過去データの欠け

旧系（2018-10〜2026-10）の解析の不具合などで、過去のデータに欠けがある。欠けごとの原因・期間・今の状態は [data-gaps.md](data-gaps.md)。

- 欠けの区間の一覧は表 `data_gaps`（列の正は `schema/tokyo/data_gaps.json`）。1 行が 1 区間で、欠けているのは分の終わりが `prev_at` と `next_at` の間（両端を含まない）のもの。`cause`（`bug_format` = 欠け 1、`bug_comma` = 欠け 3、`no_change` = 値が変わらず保存しなかった、`unknown`）と `status`（`none` / `filled` / `partial`。#114 で作った行の割合）を持つ
- #114 で DataCube の歩み値から作った行（`ingest_run_id` が `legacy-fill114-` で始まる）は約定の列だけで、気配（`bid`・`ask` とその IV）が無い。mid IV・30 日固定はその区間で出ない
- 表は所有者が手で入れる（runbook の「欠けの一覧の表 data_gaps（#123）」）。自分の環境では空

## クエリの例

`<PROJECT_ID>` を自分のプロジェクト ID に、日時を読みたい範囲に置き換える。ロケーションは `asia-northeast1`（`bq query --location=asia-northeast1 --use_legacy_sql=false`）。日時は例で、その範囲にデータがあるとは限らない。

最新版を 1 時間ぶん読む（ATM の put。JST の 2026-10-14 10:00〜11:00）:

```sql
SELECT
  DATETIME(created_at, 'Asia/Tokyo') AS snapshot_jst,
  last_trading_day,
  target_price,
  iv,
  IF(bid_iv > 0 AND ask_iv > 0, (bid_iv + ask_iv) / 2, NULL) AS mid_iv
FROM `<PROJECT_ID>.optionchan_tokyo.option_price_latest`(
  TIMESTAMP '2026-10-14 10:00:00+09:00', TIMESTAMP '2026-10-14 11:00:00+09:00')
WHERE is_atm AND type = 2
ORDER BY created_at, last_trading_day
```

ある時点のスマイル（その時点以前で最新のスナップショットの全行使価格。期間は前 1 日に絞る）:

```sql
WITH snap AS (
  SELECT MAX(snapshot_at) AS snap_at
  FROM `<PROJECT_ID>.optionchan_tokyo.latest_runs`(
    TIMESTAMP '2026-10-13 14:00:00+09:00', TIMESTAMP '2026-10-14 14:00:00+09:00')
)
SELECT
  o.last_trading_day,
  o.target_price,
  IF(o.type = 1, 'call', 'put') AS call_put,
  o.is_atm,
  IF(o.bid_iv > 0, o.bid_iv, NULL) AS bid_iv,
  IF(o.ask_iv > 0, o.ask_iv, NULL) AS ask_iv,
  IF(o.bid_iv > 0 AND o.ask_iv > 0, (o.bid_iv + o.ask_iv) / 2, NULL) AS mid_iv,
  IF(o.iv > 0, o.iv, NULL) AS iv
FROM `<PROJECT_ID>.optionchan_tokyo.option_price_latest`(
  TIMESTAMP '2026-10-13 14:00:00+09:00', TIMESTAMP '2026-10-14 14:00:00+09:00') AS o
JOIN snap ON o.created_at = snap.snap_at
ORDER BY o.last_trading_day, o.target_price, o.type
```

1 時間足（ATM の put の mid IV。JST の 1 週間）:

```sql
SELECT
  DATETIME(started_at, 'Asia/Tokyo') AS started_jst,
  last_trading_day,
  open,
  high,
  low,
  close
FROM `<PROJECT_ID>.optionchan_tokyo.atm_mid_iv`
WHERE started_at >= TIMESTAMP '2026-10-05 00:00:00+09:00' AND started_at < TIMESTAMP '2026-10-12 00:00:00+09:00'
ORDER BY last_trading_day, started_at
```

- 限月を 1 つにするときは `last_trading_day` で絞る（1 時間足は同じ時刻に 2 限月の行がある）

## 費用の注意

- **パーティションの列で絞る**。取り込みの 3 表は `created_at`、`ingest_runs` は `snapshot_at`、1 時間足は `started_at`、`atm_cm_iv` は `created_at`。絞らないと全期間（2018-10〜）を読む
- `option_price` は `require_partition_filter = true` で、`created_at` の絞り込みが無いクエリはエラーになる（`dc_option_tick`・`dc_future_tick` も `trade_date` で同じ）
- 最新版のテーブル関数は期間の引数が必須。中で `created_at`・`snapshot_at` を絞るので、期間に応じたパーティションだけを読む
- パーティションは UTC の日なので、JST の 1 日（00:00〜24:00）は UTC の 2 日のパーティションを読む。`option_price` は列が多く 1 日のパーティションが大きい（`sql/cm/atm_cm.sql` の冒頭のコメントに dry run の値がある）。要る列だけを `SELECT` する（列指向なので読む列の分だけ課金される）
- 先に `bq query --dry_run` で読むバイト数を見る。プロジェクトのクエリ量の上限・予算は `docs/runbook.md` の最初の節（[コスト](runbook.md#コスト)）

## 列の契約

- `trading_calendar` の列・型・モードは、オーナーが承認した契約（`apps/ingest/tests/test_schema.py` の `TRADING_CALENDAR` が `schema/tokyo/trading_calendar.json` と一致することを検査する。別のアプリ（シミュレーター）の契約テストも同じファイルを照合する）。変えるときは読み手と合意してから。列の追加は NULLABLE で末尾に。改名・削除・型の変更はしない（`docs/architecture/target.md` の 3.2「取引カレンダー（#63）」）
- 取り込みの 3 表（`spot_price`・`future_price`・`option_price`）は、旧系の列（`schema/*.json`）の名前・型・モードを変えない（`apps/ingest/tests/test_schema.py` の `test_existing_columns_unchanged`）。新系で足した列（`ingest_run_id`・`parser_version`・`source_object`・`page_updated_at`・`price_time_raw`）は NULLABLE
- 列を変えるときは、読む側（ビューアー・別のアプリ）に知らせてから。`schema/` を変えると Terraform の差分になり、`schema/tokyo/` は取り込みのイメージにも入る（`docs/development.md`）

## 作業用のデータセット（旧系）

`infra/prod/sq.tf` は `optionchan_tokyo` のほかに、旧系との突き合わせ（#40）の作業用のデータセットを 2 つ作る。**読まない**（毎日作り直す途中のデータで、表は 7 日で消える）。

| データセット | ロケーション | 中身 |
|---|---|---|
| `optionchan_legacy_stage` | US | 旧系の前日分の作業用（定期クエリ `legacy-stage` が作る） |
| `optionchan_legacy_copy` | asia-northeast1 | その東京へのコピー（`legacy-copy` の宛先。`legacy-compare` が読む） |

- 旧系の停止（#14）で定期クエリを止め（`var.legacy_stopped`）、旧系の削除（#24）で消す。旧系のデータセット `optionchan`（US）は Terraform の管理の外
- 自分の環境では旧系が無いので要らない（`docs/setup.md`）

## 別のプロジェクトから読むときの権限

別のプロジェクト・アプリ（の SA）から `optionchan_tokyo` を読むには、次の 2 つが要る。

- データセット `optionchan_tokyo` の `roles/bigquery.dataViewer`（表・ビュー・テーブル関数を読む）。データセット単位で付けると、その中の表とテーブル関数の両方に効く
- クエリを流すプロジェクト（読む側の、課金されるプロジェクト）の `roles/bigquery.jobUser`（クエリのジョブを作る）

今の付与は Terraform にある: データセット単位は `infra/prod/iam.tf`（`sa-ingest` は `roles/bigquery.dataEditor`、`sa-viewer` は `roles/bigquery.dataViewer`）と `infra/prod/sq.tf`（定期クエリの `sa-sq-dts` は `roles/bigquery.dataViewer` と結果の表だけの `roles/bigquery.dataEditor`）、プロジェクト単位の `roles/bigquery.jobUser` は `infra/bootstrap/`（`ingest.tf`・`viewer.tf`・`sq.tf`）。新しい読み手を足すときも同じ形で Terraform に書く（`infra/prod/data.tf` のコメントのとおり、データセットの `access` ブロックは書かない）。

- 公開の URL・`allUsers` への付与は作らない
- DataCube の 2 表（`dc_option_tick`・`dc_future_tick`）は自己使用のライセンスのデータで、データセット単位の付与はこの 2 表にも及ぶ。別のプロジェクトへ付けるときは、その読み手に渡してよいかを先に確かめる

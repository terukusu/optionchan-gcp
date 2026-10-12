# 契約テンプレ: Cloud Functions エントリポイント（Pub/Sub / GCS / HTTP トリガ）

対象: `functions/main.py` の `download_jpx`（Pub/Sub）/ `load_jpx_into_bq`（GCS finalize）/ `atm_data`・`smile_data`（HTTP）、`functions_py/timeframe/main.py` の `insert_ohlc`（Pub/Sub）、`functions_py/atm_iv/main.py` の `atm_iv_data`（HTTP）。純粋関数と違い **副作用が本体** なので、副作用を全部列挙することが契約の中心になる。

## 記入項目

| 項目 | 書き方 |
|---|---|
| トリガ | Pub/Sub トピック / GCS バケットとイベント / HTTP。スケジュール（Cloud Scheduler の cron と TZ） |
| 入力 | `data` / `context` / `request` のどこから何を読むか。想定外の入力（空メッセージ・対象外のファイル名・不正なパラメータ）の扱い |
| 認証 | HTTP なら `check_auth`（`auth.txt` の共通パスワードとの照合）。失敗時の応答 |
| 副作用リスト | 書き込み先を **すべて** 列挙: GCS オブジェクト名パターン、BigQuery テーブル（INSERT / load）、Datastore のキー、外部 HTTP（JPX） |
| 戻り値 / 応答 | HTTP ならステータス・Content-Type・本文の形（CSV / JSON の列）。後方互換が要る消費者（`appengine/static/*.js`） |
| 冪等性・再実行 | 同じイベントが再配信・再実行されたときに何が二重になるか（重複行・重複ファイル）。防ぐならその方法 |
| 実行時間・リソース | timeout / memory に収まるか。BigQuery のスキャン量（パーティション条件） |
| 失敗時 | 例外を投げたとき何が起きるか（Pub/Sub の再試行設定の有無、データ欠損になるか）。ログに何を出すか |
| ランタイム | runtime（python37 なら廃止済み。移行の要否） |

## 記入テンプレ

```markdown
### `<関数名>` (`<ファイルパス>`) — 新規 / 改修

- trigger: <Pub/Sub topic / GCS bucket+event / HTTP> / schedule: <cron, TZ>
- input: <読む値> / 想定外入力: <無視してログ / 400 / 例外>
- auth: <check_auth の有無と失敗時応答 / 対象外>
- 副作用:
  1. GCS `optionchan/<パターン>` に書き込み
  2. BigQuery `optionchan.<table>` に <INSERT / load>
  3. Datastore `<kind>/<key>` を更新
- response: <HTTP の形 / なし>
- 冪等性: <再配信時の挙動>
- 実行時間・スキャン量: <見積もり>
- 失敗時: <挙動とログ>
- runtime: <python311 / python37 → 移行要否>
- 改修の場合の差分:
  - 現契約 (デプロイ済みソースとコードから抽出): <現状の副作用と分岐>
  - 変更点: <追加 / 削除した副作用・分岐>
```

## 観察可能な AC への落とし方

ローカルでは GCP クライアントを使う部分を実行できないので、**判定ロジックを純粋関数に切り出して pytest** にし、副作用はデプロイ後の確認（`bq query` / `gcloud logging read` / `gsutil ls`）で AC にする。

| 契約 | 観察可能な AC |
|---|---|
| 想定外のファイル名を無視 | `REGEX_PRICE_FILE にマッチしない名前では table_name が None`（切り出した関数の pytest） |
| HTTP 認証 | `Authorization ヘッダ無しで 401/403 を返す`（デプロイ後に `curl -s -o /dev/null -w '%{http_code}'` で確認。パスワードは表示しない） |
| BigQuery への INSERT | `デプロイ後 30 分 (取引時間中) の option_price の COUNT(*) > 0`（`bq query`、パーティション条件付き） |
| 失敗時ログ | `gcloud logging read 'severity>=ERROR AND resource.labels.function_name="<FN>"' --freshness=10m` が 0 件 |
| 冪等性 | `同じ created_at の行が重複しない`（`SELECT created_at, COUNT(*) ... GROUP BY ... HAVING COUNT(*) > <期待値>` が 0 行） |

副作用の **列挙漏れ** は Phase 2 の本番影響視点・スコープ分解視点が block する対象。

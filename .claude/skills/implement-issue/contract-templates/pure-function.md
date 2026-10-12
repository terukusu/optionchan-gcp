# 契約テンプレ: 純粋関数（パーサー・変換・集計ロジック）

対象: 副作用を持たず、同じ入力に対して常に同じ値を返す関数。例: `functions/jpx_loader.py` の `parse_jpx_html` / `parse_option`、価格文字列の正規表現パース、IV・OHLC の計算、`appengine` / 表示用関数のデータ整形。

I/O（`load_html_from_web` の HTTP 取得、GCS / BigQuery / Datastore アクセス、`datetime.now()`）が混ざっている関数は、**I/O 部分と変換部分を分けてから**変換部分にこのテンプレを使う（分けられないなら `cloud-function.md` を使う）。

## 記入項目

| 項目 | 書き方 | 例 |
|---|---|---|
| シグネチャ | 引数と戻り値の型（型ヒントが無ければ実際に渡る型） | `parse_jpx_html(html: str) -> JpxOptionPriceInfo` |
| 事前条件 (pre) | 呼び出し側が保証すべき条件と、破られたときの挙動 | HTML が JPX の価格ページであること / 構造が違えば `None` を返す or 例外 |
| 事後条件 (post) | 戻り値が満たす性質を **入力との関係** で書く | call / put の件数は HTML 上の行数と一致する |
| 不変条件 | 常に成り立つ性質 | `created_at` は JST の tz-aware datetime |
| 欠損値の扱い | `-` や空欄、取引時間外の表示をどう値に落とすか | 価格が `-` なら `None`（0 にしない） |
| 純粋性 | I/O・現在時刻・乱数・グローバル状態を触らない | 現在時刻が要るなら引数で受ける |

## 記入テンプレ

```markdown
### `<関数名>` (`<ファイルパス>`) — 新規 / 改修

- signature: `<型>`
- pre:
  - <条件> (破ったとき: <例外の型 / 戻り値>)
- post:
  - <入力と戻り値の関係>
- invariant:
  - <常に成り立つ性質>
- 欠損値: <`-` / 空欄 / 取引時間外の扱い>
- purity: 副作用なし / 例外: <あれば>
- 改修の場合の差分:
  - 現契約 (コードから抽出): <現状の挙動。fixture があればその出力>
  - 変更点: <追加 / 緩和 / 強化した条件>
  - 後方互換: 戻り値の形・BigQuery に入る値が変わるか (変わるなら schema/*.json と既存データへの影響)
```

## 観察可能な AC への落とし方

契約 1 行 → pytest 1 本 → AC 1 行、の対応を保つ。HTML 等の入力は `tests/fixtures/` に保存した実物を使う（JPX へのアクセスは fixture 取得時の 1 回だけ）。

| 契約 | 観察可能な AC |
|---|---|
| post「call / put の件数」 | `tests/fixtures/jpx/nkopm_1.html を parse すると call_option_list が N 件` |
| 欠損値 `-` → `None` | `価格が "-" の行の price は None` |
| invariant tz-aware | `どの fixture でも created_at.tzinfo is not None` |
| pre 違反（構造変化） | `空 HTML を渡すと <例外 / None>`（JPX のページ構造変化時の挙動を固定する） |

**書いてはいけない AC**: 「正しくパースできる」「適切に処理される」。

"""取り込みの設定を 1 か所にまとめる。

データセット名・バケット名・生 HTML の形式・立会時間などユーザー確認で変わりうる値は、ここと
schema/tokyo/*.json だけを直せば差し替えられるようにする。
"""

import json
import os
from dataclasses import dataclass
from datetime import time
from pathlib import Path

# イメージ内ではリポジトリ直下の構成を /app に置く（apps/ingest/Dockerfile）。
# ローカルではこのファイルから 4 つ上（リポジトリ直下）を使う
_REPO_ROOT = Path(__file__).resolve().parents[4]
_APP_ROOT = Path("/app") if Path("/app/schema").is_dir() else _REPO_ROOT

DEFAULT_SCHEMA_DIR = _APP_ROOT / "schema" / "tokyo"

# データを書くテーブル（書き込み順）と完了記録のテーブル
DATA_TABLES = ("spot_price", "future_price", "option_price")
RUNS_TABLE = "ingest_runs"

# 生 HTML: gzip した JSON の封筒。名前は 1 限月ページの更新時刻（JST）
RAW_OBJECT_FORMAT = "raw/%Y/%m/%d/%Y%m%dT%H%M.json.gz"
RAW_ENVELOPE_FORMAT = 1
RAW_CONTENT_TYPE = "application/json"

# JPX（1 限月・2 限月）。ヘッダが無いと 400 になる（現行の functions/jpx_loader.py と同じ値）
JPX_URLS = {
    1: "https://svc.qri.jp/jpx/nkopm/",
    2: "https://svc.qri.jp/jpx/nkopm/1",
}
JPX_HEADERS = {
    "Referer": "https://svc.qri.jp/jpx/nkopm/2",
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
}

# --- 期限・タイムアウト・リトライ（Scheduler は毎分・リトライ 0。次の分のリクエストと重ならないようにする） ---
# リクエスト 1 回の全体の期限。Cloud Run のリクエストタイムアウト（50 秒、11b）と gunicorn の --timeout 50 より短く、
# 毎分の起動間隔（60 秒）に余裕を残す。超えたら次の呼び出しに入らず ERROR deadline_exceeded で終える
REQUEST_DEADLINE_SECONDS = 45
# 残りがこれ未満なら次の外部呼び出しに入らない
MIN_REMAINING_SECONDS = 2
# JPX と GCS の呼び出しは別スレッドで実行し、timeout + CALL_GRACE_SECONDS で待つのをやめて deadline_exceeded にする
# （requests の読み取りタイムアウトは「バイトが届かない時間」の上限で、少しずつ届く応答や再試行の最後の 1 回は
#   timeout を超えうるため。待つのをやめた呼び出しはスレッドに残り、下の各タイムアウトで終わる）。
# BigQuery は gRPC の deadline が開始から応答までの全体にかかるので、スレッドに出さない
CALL_GRACE_SECONDS = 1
# JPX: 1 回 15 秒（残り時間で頭打ち）を接続（最大 5 秒）と読み取りに配分し、合計が timeout を超えないようにする。
# 接続は要求ごとに張り直す（#88。io_jpx.fetch_page）
JPX_TIMEOUT_SECONDS = 15
JPX_CONNECT_TIMEOUT_SECONDS = 5
# JPX の取り直し（#88・#130）: HTTP 5xx・接続エラー・タイムアウト（io_jpx の JpxTransient）のときだけ、1 リクエストで
# 最大 len(JPX_RETRY_WAITS_SECONDS) 回（1 限月・2 限月の合計）、回ごとに伸ばした待ち（3 → 5 → 8 秒。リクエスト全体の
# 通し番号で選ぶ）の後に取り直す。4xx・空ページは取り直さない（次の分に任せる）。
# 期限: 取り直しの前ごとに、予算 = 残り − 今回の待ち − JPX_RETRY_RESERVE_SECONDS（取り直しの後の GCS・BigQuery の分。
# JPX の後の処理は実測 p50 2.0 / p99 2.7 / 最大 3.7 秒。10-17 以降の ingested。#130）が JPX_RETRY_MIN_TIMEOUT_SECONDS
# 以上のときだけ取り直し、取り直しのタイムアウトは min(JPX_TIMEOUT_SECONDS, 予算)。足りなければ待たずに止める。
# 8 秒目の即時の 502 は 11・16・24 秒目頃に取り直し（最後の取り直しは 1 回目の失敗から 15 秒以上後）、
# 15 秒の読み取りタイムアウトの後（1 回目でも取り直しでも）は予算が無いので取り直さない。
# 待ち: 毎晩 04:00・04:20 JST の 502 は分の 8 秒目に即時に返り、3 秒後の取り直しも 502、次の分は取れていた。
# 旧系の失敗は同じ分の 2〜11 秒目に散らばる（#88・#130 の調査）。
# FETCH_AT_SECOND + 待ちの合計 + JPX_RETRY_MIN_TIMEOUT_SECONDS + JPX_RETRY_RESERVE_SECONDS ≤ REQUEST_DEADLINE_SECONDS
# に保つ（8 + 16 + 5 + 15 = 44。即時の 502 が続けば上限の回数まで取り直せる。test_app で検査）
JPX_RETRY_WAITS_SECONDS = (3, 5, 8)
JPX_RETRY_RESERVE_SECONDS = 15
JPX_RETRY_MIN_TIMEOUT_SECONDS = 5
# GCS: timeout（10 秒、残り時間で頭打ち）を (接続, 読み取り) の組で渡す。
# 存在確認（読み取り・冪等）は 1 回 GCS_EXISTS_ATTEMPT_TIMEOUT_SECONDS 秒の試行を、最後の試行が timeout 内に
# 終わる範囲でだけ再試行する。作成はリトライしない（再送で自分の作成に 412 を受けて skipped_locked になるのを避ける。
# 失敗したら次の分に再試行される）
GCS_CONNECT_TIMEOUT_SECONDS = 5
GCS_TIMEOUT_SECONDS = 10
GCS_EXISTS_ATTEMPT_TIMEOUT_SECONDS = 4
# JPX の取得を分の頭からこの秒（分内の秒）まで遅らせる（#38）。JPX のページは分単位の更新時刻が変わった後、
# 分の頭の数秒で中身が書き換わる（1〜2 秒目の取得は書き換え途中の版、4〜10 秒目は書き換え後の版だった）。
# 取引時間中だけ 1 限月ページの取得の前に待ち、到着が既にこの秒を過ぎていれば待たない。待ちも全体の期限に数える。
# 待ちも課金対象のインスタンス時間になる（10 秒だと月の vCPU 秒が無料枠を超えうる。target.md 7.2）。
# 環境変数 INGEST_FETCH_AT_SECOND で上書きできる（0 で待たない。0〜MAX_FETCH_AT_SECOND）
FETCH_AT_SECOND = 8.0
# 上限: 待ち 20 秒 + JPX 15 秒でも期限 45 秒の内側に残りがある値
MAX_FETCH_AT_SECOND = 20.0
# BigQuery Storage Write API: テーブルごとに 1 回、開始から応答まで 15 秒（残り時間で頭打ち）。
# リトライなし（失敗は write_failed、毎時の回収（recovery.py）が拾う）
BQ_APPEND_TIMEOUT_SECONDS = 15

# --- 取りこぼしの回収・日次の照合・再処理（#34、target.md 3.1） ---
# 毎時 0 分（JST。Scheduler の予定時刻、無ければ実時刻）のリクエストで、通常の処理の後に
# 同じ期限（45 秒）の残りで回収する。
# 対象: 名前の時刻が直近 RECOVERY_LOOKBACK_HOURS 時間の生 HTML のうち、ingest_runs に完了記録が無く、
# 作成から RECOVERY_MIN_AGE_SECONDS 秒以上たったもの（実行中の取り込み（最大 50 秒）と重ならない）を古い順に最大
# RECOVERY_MAX_ITEMS 件。1 件を始めるのは残りが RECOVERY_MIN_REMAINING_SECONDS 秒以上のときだけ
# （取引時間中は #38 の待ち 8 秒 + 通常の処理の後なので、残りはおよそ 30 秒）
RECOVERY_LOOKBACK_HOURS = 6
RECOVERY_MIN_AGE_SECONDS = 180
RECOVERY_MAX_ITEMS = 10
RECOVERY_MIN_REMAINING_SECONDS = 15
# 日次の照合: この JST の時:分のリクエストで、回収の後に前日のこの時刻〜当日のこの時刻の生 HTML と完了記録を数える
RECONCILE_AT = (7, 0)
# 照合の回は、照合（一覧 + クエリ）の分の時間を回収に使わせない
RECONCILE_RESERVE_SECONDS = 15
# 照合・再処理のログに名前を載せる上限
RECONCILE_LOG_MAX = 20
# 再処理ジョブ（Cloud Run ジョブ reprocess）の期限。タスクのタイムアウト 3600 秒より前に止めて残りの件数を出す
REPROCESS_DEADLINE_SECONDS = 3300
# 再処理で 1 件を始める残り時間の下限（1 件の最悪 = 読み出し 10 秒 + 追記 15 秒 × 4）
REPROCESS_MIN_REMAINING_SECONDS = 70
# 完了記録のクエリ（BigQuery jobs.query の REST）。リトライなし（次の回に任せる）
BQ_QUERY_TIMEOUT_SECONDS = 15
BQ_CONNECT_TIMEOUT_SECONDS = 5
# 生 HTML の一覧・読み出し（読み取りなので、最後の試行が timeout 内に終わる範囲で再試行する。1 回の試行の上限）
GCS_LIST_TIMEOUT_SECONDS = 10
GCS_READ_TIMEOUT_SECONDS = 10
GCS_READ_ATTEMPT_TIMEOUT_SECONDS = 5

# 1 限月あたりの行使価格の下限（構造の検査。実測の最小は 1 スナップショット 84 行 = 2 限月 × 2 種別 × 21）
MIN_STRIKES = 10

# 鮮度のしきい値（分）。min(実時刻, セッション終了) − max(ページ更新時刻, セッション開始)
STALE_DAY_MINUTES = 30
STALE_NIGHT_MINUTES = 40

# 本文の凍結（#88）: 取引時間中（ページの更新時刻が立会の [開始, 終了)）に、option の本文
# （snapshot.OPTION_BODY_COLUMNS）が直前のスナップショットと同じことがこの回数続いたら WARNING body_frozen。
# ヘッダの更新時刻だけ進み本文が止まる形（2026-10-14 18:52〜19:27 JST の 36 回）は鮮度（stale）では見えない。
# 2026-10-13〜10-15 の取引時間中は、凍結を除くと最長 1 回（静かな時間の 1 分）。寄り前・引け後は数えない。
# 品質チェック（sql/quality/quality_daily.sql の body_frozen）も同じ回数・列・時間帯（tests/test_sql.py が一致を検査）
BODY_FROZEN_REPEATS = 3

# --- 取引カレンダーの固定の値（#75。休業日・covered_until は BigQuery の最新版だけが正で、ここには持たない） ---
# 立会時間（JST の実時刻）。出典: 大阪取引所「祝日取引」のページの「取引時間/取引日」（HOLIDAY_TRADING_URL）。
# 日中 8:45〜15:45、夜間 17:00〜翌 6:00。毎日の取り込み（calendar_sync.py）でページの表と照合し、違えば
# ERROR calendar_sessions_changed（ここを直して出し直す）
SESSION_DAY_OPEN = time(8, 45)
SESSION_DAY_CLOSE = time(15, 45)
SESSION_NIGHT_OPEN = time(17, 0)
SESSION_NIGHT_CLOSE = time(6, 0)
# ページ時刻の約 15 分遅れ + 余裕。セッション終了後もこの分数だけ取り込みを続ける
SESSION_GRACE_MINUTES = 30
# ここに書いた取引日は、直前の夜間セッションが無いものとして扱う（臨時の休止用。通常は空）
NO_NIGHT_SESSION_BEFORE: frozenset = frozenset()

# 取引カレンダーの期限の通知: 1 日 1 回（この JST の時:分に来たリクエストだけ）。
# covered_until の CALENDAR_WARN_DAYS 日前から WARNING calendar_expiring、過ぎたら ERROR calendar_outdated
CALENDAR_NOTICE_AT = (9, 0)
CALENDAR_WARN_DAYS = 14

# --- 取引カレンダーの自動取り込み（#63。calendar_sync.py、runbook「取引カレンダー（#63）」） ---
# 出典: JPX「祝日取引」のページ（祝日取引実施日の表）と、検証用の内閣府「国民の祝日」CSV
HOLIDAY_TRADING_URL = "https://www.jpx.co.jp/derivatives/rules/holidaytrading/index.html"
HOLIDAYS_CSV_URL = "https://www8.cao.go.jp/chosei/shukujitsu/syukujitsu.csv"
# 版を追記するテーブルと、最新版を返すビュー（optionchan_tokyo。列は schema/tokyo/trading_calendar.json）
CALENDAR_TABLE = "trading_calendar"
CALENDAR_VIEW = "trading_calendar_latest"
# 毎日この JST の時:分のリクエスト（時間外。夜間の余裕 06:30 の後、DTS の 07:02 の前）で、通常の処理の後に
# 同じ期限の残りで取り込む（Scheduler を足さない）。失敗しても応答は変えない
CALENDAR_SYNC_AT = (6, 40)
# JPX のページ・祝日 CSV の取得 1 回の上限（リトライなし。翌日か手動のジョブに任せる）
CALENDAR_HTTP_TIMEOUT_SECONDS = 10
# 最新版の読み込み（起動時と毎時 0 分）の上限。起動時はこの分だけ取り込みの期限を短くする
CALENDAR_LOAD_TIMEOUT_SECONDS = 5
# 起動直後（プロセス内に直前の版が無い）に、14 日以内の休業日の増加を比べる BigQuery の 1 つ前の版を読む上限
# （#75。失敗・期限なら比べずに WARNING calendar_previous_unavailable）。起動時の期限はこの分も足す
CALENDAR_PREVIOUS_TIMEOUT_SECONDS = 3
# 起動時の読み込みに失敗した（一度も読めていない）とき、次のリクエストで読み直すまでの秒数（毎時 0 分を待たない）
CALENDAR_RETRY_SECONDS = 300
# 版の追記の上限
CALENDAR_WRITE_TIMEOUT_SECONDS = 10
# 手動のジョブ（calendar-sync）の期限
CALENDAR_JOB_DEADLINE_SECONDS = 300


@dataclass(frozen=True)
class Settings:
    project: str | None
    dataset: str
    raw_bucket: str
    schema_dir: Path
    dry_run: bool
    fetch_at_second: float = FETCH_AT_SECOND
    bq_location: str = ""  # クエリのジョブを実行する場所（データセットの場所）

    @classmethod
    def from_env(cls, env=None):
        env = os.environ if env is None else env
        dry_run = env.get("INGEST_DRY_RUN", "") in ("1", "true")
        raw_bucket = env.get("RAW_BUCKET", "")
        if not raw_bucket and not dry_run:
            raise RuntimeError("RAW_BUCKET が未設定")
        schema_dir = Path(env.get("SCHEMA_DIR", DEFAULT_SCHEMA_DIR))
        return cls(
            project=env.get("GCP_PROJECT") or None,
            dataset=env.get("BQ_DATASET", "optionchan_tokyo"),
            raw_bucket=raw_bucket or "dry-run",
            schema_dir=schema_dir,
            dry_run=dry_run,
            fetch_at_second=_fetch_at_second(env.get("INGEST_FETCH_AT_SECOND", "")),
            bq_location=env.get("BQ_LOCATION") or dataset_location(schema_dir),
        )


def dataset_location(schema_dir: Path) -> str:
    """データセットの場所（schema/tokyo/tables.json の dataset.location。Terraform もここを読む）"""
    with open(Path(schema_dir) / "tables.json", encoding="utf-8") as f:
        return json.load(f)["dataset"]["location"]


def _fetch_at_second(text: str) -> float:
    if text == "":
        return FETCH_AT_SECOND
    try:
        value = float(text)
    except ValueError:
        raise RuntimeError(f"INGEST_FETCH_AT_SECOND が数値でない: {text!r}") from None
    if not 0 <= value <= MAX_FETCH_AT_SECOND:  # NaN もここで弾く
        raise RuntimeError(f"INGEST_FETCH_AT_SECOND が範囲外（0〜{MAX_FETCH_AT_SECOND:g}）: {text!r}")
    return value

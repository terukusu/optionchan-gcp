"""取り込みの設定を 1 か所にまとめる。

データセット名・バケット名・生 HTML の形式などユーザー確認で変わりうる値は、ここと
schema/tokyo/*.json・config/trading_calendar.toml だけを直せば差し替えられるようにする。
"""

import os
from dataclasses import dataclass
from pathlib import Path

# イメージ内ではリポジトリ直下の構成を /app に置く（apps/ingest/Dockerfile）。
# ローカルではこのファイルから 4 つ上（リポジトリ直下）を使う
_REPO_ROOT = Path(__file__).resolve().parents[4]
_APP_ROOT = Path("/app") if Path("/app/config").is_dir() else _REPO_ROOT

DEFAULT_SCHEMA_DIR = _APP_ROOT / "schema" / "tokyo"
DEFAULT_CALENDAR_PATH = _APP_ROOT / "config" / "trading_calendar.toml"

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
# JPX: リトライなし（次の分に任せる）。1 回 15 秒（残り時間で頭打ち）を接続（最大 5 秒）と読み取りに配分し、
# 合計が timeout を超えないようにする
JPX_TIMEOUT_SECONDS = 15
JPX_CONNECT_TIMEOUT_SECONDS = 5
# GCS: timeout（10 秒、残り時間で頭打ち）を (接続, 読み取り) の組で渡す。
# 存在確認（読み取り・冪等）は 1 回 GCS_EXISTS_ATTEMPT_TIMEOUT_SECONDS 秒の試行を、最後の試行が timeout 内に
# 終わる範囲でだけ再試行する。作成はリトライしない（再送で自分の作成に 412 を受けて skipped_locked になるのを避ける。
# 失敗したら次の分に再試行される）
GCS_CONNECT_TIMEOUT_SECONDS = 5
GCS_TIMEOUT_SECONDS = 10
GCS_EXISTS_ATTEMPT_TIMEOUT_SECONDS = 4
# BigQuery Storage Write API: テーブルごとに 1 回、開始から応答まで 15 秒（残り時間で頭打ち）。
# リトライなし（失敗は write_failed、回収は 11c）
BQ_APPEND_TIMEOUT_SECONDS = 15

# 1 限月あたりの行使価格の下限（構造の検査。実測の最小は 1 スナップショット 84 行 = 2 限月 × 2 種別 × 21）
MIN_STRIKES = 10

# 鮮度のしきい値（分）。min(実時刻, セッション終了) − max(ページ更新時刻, セッション開始)
STALE_DAY_MINUTES = 30
STALE_NIGHT_MINUTES = 40

# 取引カレンダーの期限の通知: 1 日 1 回（この JST の時:分に来たリクエストだけ）。
# covered_until の CALENDAR_WARN_DAYS 日前から WARNING calendar_expiring、過ぎたら ERROR calendar_outdated
CALENDAR_NOTICE_AT = (9, 0)
CALENDAR_WARN_DAYS = 14


@dataclass(frozen=True)
class Settings:
    project: str | None
    dataset: str
    raw_bucket: str
    calendar_path: Path
    schema_dir: Path
    dry_run: bool

    @classmethod
    def from_env(cls, env=None):
        env = os.environ if env is None else env
        dry_run = env.get("INGEST_DRY_RUN", "") in ("1", "true")
        raw_bucket = env.get("RAW_BUCKET", "")
        if not raw_bucket and not dry_run:
            raise RuntimeError("RAW_BUCKET が未設定")
        return cls(
            project=env.get("GCP_PROJECT") or None,
            dataset=env.get("BQ_DATASET", "optionchan_tokyo"),
            raw_bucket=raw_bucket or "dry-run",
            calendar_path=Path(env.get("CALENDAR_PATH", DEFAULT_CALENDAR_PATH)),
            schema_dir=Path(env.get("SCHEMA_DIR", DEFAULT_SCHEMA_DIR)),
            dry_run=dry_run,
        )

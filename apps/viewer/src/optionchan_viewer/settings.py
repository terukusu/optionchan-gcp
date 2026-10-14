"""ビューアーの設定（環境変数）。

プロジェクト ID は必須（既定値なし。リポジトリにオーナー個人の設定を入れないため）。
データセット・ロケーション・上限は汎用の既定値を持つ。
"""

import os
import re
from dataclasses import dataclass
from datetime import UTC, datetime

# BigQuery のプロジェクト ID / データセット名として許す形（SQL の識別子に埋めるので厳密に検査する）
# ドメイン付きのプロジェクト ID（example.com:my-project）も受け付ける。バッククォート・空白・ドットの連続等は拒否
_PROJECT_RE = re.compile(r"^(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)*:)?"
                         r"[a-z][a-z0-9-]{4,28}[a-z0-9]$")
_DATASET_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,1023}$")
_LOCATION_RE = re.compile(r"^[A-Za-z0-9-]{2,40}$")

GiB = 1024**3

# 探す範囲・読む期間の上限（リクエストの値に依らない固定値）
SEARCH_DAYS = 14  # 最新のスナップショット・1 時間足を探す範囲（年末年始の休場を含む）
ATM_IV_MONTH_DAYS = 10  # /atm_iv_data の限月を決める範囲（現行の t1 と同じ）
ATM_IV_FILL_MAX_DAYS = 3  # /atm_iv_data で 1 時間足になっていない分を生データから補う最大の日数
D_DEFAULT, D_MIN, D_MAX = 7, 1, 100  # /atm_iv_data の d（日数）
N_DEFAULT, N_VALUES = 0, (0, 1)  # /atm_iv_data・/atm_iv_bars の n（何番目の限月）
# /atm_iv_bars の足（#68）: 名前 → 秒。1m・5m・15m は 1 分足（sql/atm_iv_minute.sql）から、
# 1h・1d は 1 時間足（/atm_iv_data と同じ）から
TIMEFRAMES = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "1d": 86400}
TF_DEFAULT = "1h"
# /atm_iv_bars の期間（#77。画面に期間の選択は無く、チャートの表示範囲で決まる）: 足 → (既定, 1 回の最大, 遡れる上限)。
#   既定 = start・end を省いたときの日数（分足は最新のスナップショットの、1h・1d は now の、
#     その日数前の UTC の 0 時〜now）
#   1 回の最大 = end − start の上限（日）。分足は生データ（option_price_latest。1 日 約 90 MiB）を読むので、
#     日の境目に揃えない 3 日でも 4 日分のパーティション（約 360 MiB）で 1 クエリ 512 MiB 以下（#68 AC 6）。
#     1h・1d は 1 時間足（atm_iv。小さい）を読む
#   遡れる上限 = start が now の何日前の UTC の 0 時まで許すか（分足だけ。長期間の分足は集計済みテーブルが前提で、
#     スコープ外）
BARS_RANGE = {
    "1m": (1, 3, 31),
    "5m": (1, 3, 31),
    "15m": (1, 3, 31),
    "1h": (10, 100, None),
    "1d": (100, 400, None),
}
BARS_EPOCH = datetime(2010, 1, 1, tzinfo=UTC)  # start の下限（これより前のデータは無い）
# 日足: 取引日の 1 時間足は 00:00 JST の前（前の取引日の夜間。連休の前の夜間を含む）から始まり、日中（当日）で終わる。
# [start, end) に 00:00 JST が入る取引日を完全に作るため、1 時間足を start の 10 日前〜end の 1 日後まで読む
DAILY_LOOKBEHIND_DAYS = 10
HOLIDAYS_AHEAD_DAYS = 15  # 日足の取引日の判定に読む休業日の先（今日の夜間が属する休み明けの日まで。年末年始を含む）
QUERY_TIMEOUT_SECONDS = 30  # BigQuery の結果を待つ上限（gunicorn の --timeout 60 より短く）


@dataclass(frozen=True)
class Settings:
    project: str
    dataset: str = "optionchan_tokyo"
    location: str = "asia-northeast1"
    maximum_bytes_billed: int = GiB

    def __post_init__(self):
        if not _PROJECT_RE.match(self.project):
            raise ValueError("GCP_PROJECT の形式が不正")
        if not _DATASET_RE.match(self.dataset):
            raise ValueError("BQ_DATASET の形式が不正")
        if not _LOCATION_RE.match(self.location):
            raise ValueError("BQ_LOCATION の形式が不正")
        if not 0 < self.maximum_bytes_billed <= 10 * GiB:
            raise ValueError("MAX_BYTES_BILLED は 1〜10 GiB のバイト数")


def from_env(env=None) -> Settings:
    env = os.environ if env is None else env
    project = env.get("GCP_PROJECT", "")
    if not project:
        raise RuntimeError("環境変数 GCP_PROJECT（BigQuery のプロジェクト ID）が必要")
    kwargs = {"project": project}
    if env.get("BQ_DATASET"):
        kwargs["dataset"] = env["BQ_DATASET"]
    if env.get("BQ_LOCATION"):
        kwargs["location"] = env["BQ_LOCATION"]
    if env.get("MAX_BYTES_BILLED"):
        kwargs["maximum_bytes_billed"] = int(env["MAX_BYTES_BILLED"])
    return Settings(**kwargs)

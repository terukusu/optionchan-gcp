"""ビューアーの設定（環境変数）。

プロジェクト ID は必須（既定値なし。リポジトリにオーナー個人の設定を入れないため）。
データセット・ロケーション・上限は汎用の既定値を持つ。
"""

import os
import re
from dataclasses import dataclass

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
# /atm_iv_bars の d（日数）: 足 → (既定, 最小, 最大)。分足は生データを読むので、スキャン量
# （1 日 約 90 MiB × (d + 1) 日分のパーティション）が 512 MiB 以下になる 3 日まで（#68 AC 6）。
# 1h・1d は /atm_iv_data と同じ上限
BARS_D = {"1m": (1, 1, 3), "5m": (1, 1, 3), "15m": (1, 1, 3), "1h": (10, 1, 100), "1d": (100, 1, 100)}
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

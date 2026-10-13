"""行 → CSV の純粋関数（現行の HTTP 関数の CSV の形式の契約を持つ）。

共通: 改行 \\n、ヘッダ行なし、値の無いセル（NULL）は空。値は「数値として同値」（pandas の to_csv の表記
（25000.0 等）までは合わせない）。画面の JS は split("\\n") → split(",") で読み、空セルを null にする。
"""

import math
from datetime import UTC, datetime

SMILE_COLUMNS = (
    "target_price",
    "o1_call_iv",
    "o1_call_price_time",
    "o1_put_iv",
    "o1_put_price_time",
    "o2_call_iv",
    "o2_call_price_time",
    "o2_put_iv",
    "o2_put_price_time",
)
ATM_COLUMNS = ("time", "target_price", "iv", "price")
ATM_IV_COLUMNS = ("open", "high", "low", "close", "started_at")


def cell(value) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        raise TypeError("BOOL は CSV に出さない")
    if isinstance(value, float):
        return "" if math.isnan(value) else repr(value)
    if isinstance(value, int):
        return str(value)
    if isinstance(value, datetime):
        # pandas の to_csv と同じ表記（YYYY-MM-DD HH:MM:SS+00:00）。JS は Date.parse で読む
        if value.tzinfo is None:
            raise ValueError("naive な datetime は出さない")
        return value.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S+00:00")
    raise TypeError(f"未対応の型: {type(value).__name__}")


def line(values) -> str:
    return ",".join(cell(v) for v in values) + "\n"


def smile_csv(snapshot_at: datetime, atm_target_price: int, rows: list[dict]) -> str:
    """1 行目 `<スナップショットの UNIX 秒>,<1 限月 put の ATM の行使価格>`、以降 SMILE_COLUMNS（行使価格昇順）"""
    head = line([int(snapshot_at.timestamp()), atm_target_price])
    return head + "".join(line([r[c] for c in SMILE_COLUMNS]) for r in rows)


def atm_csv(rows: list[dict]) -> str:
    """1 行目 `<最終行の time>`、以降 ATM_COLUMNS（time 昇順。time は 5 分に切り捨てた UNIX 秒）"""
    head = line([rows[-1]["time"]])
    return head + "".join(line([r[c] for c in ATM_COLUMNS]) for r in rows)


def atm_iv_csv(rows: list[dict]) -> str:
    """ヘッダ・メタ行なし、ATM_IV_COLUMNS（started_at 昇順）"""
    return "".join(line([r[c] for c in ATM_IV_COLUMNS]) for r in rows)

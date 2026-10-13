"""足の束ね（/atm_iv_bars の 5 分・15 分足と日足、#68）。純粋関数。

行は {"open","high","low","close","started_at"}（started_at は aware な UTC。sql/atm_iv_minute.sql・sql/atm_iv.sql の
行と同じ形）。
束ねる意味は sql/ohlc/atm_hourly.sql の bars と同じ: open / close = 時間順で最初 / 最後の値、high / low = 最大 / 最小。
値の無い足（high が NULL = iv が全部 NULL）は使わず、行の無い区間の足は作らない（取引の無い時間は詰める）。
"""

from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

JST = ZoneInfo("Asia/Tokyo")
# 夜間（17:00〜翌 06:59 JST）を次の日に寄せるずらし: JST + 7 時間の日付が取引日の候補
_SESSION_SHIFT = timedelta(hours=7)


def drop_empty(rows: list[dict]) -> list[dict]:
    """値の無い足（high が NULL）を除く（順は保つ）"""
    return [r for r in rows if r["high"] is not None]


def _merge(rows: list[dict], started_at: datetime) -> dict:
    # rows は時間順・値のある足だけ（1 本以上）
    return {
        "open": rows[0]["open"],
        "high": max(r["high"] for r in rows),
        "low": min(r["low"] for r in rows),
        "close": rows[-1]["close"],
        "started_at": started_at,
    }


def _group(rows: list[dict], key) -> list[tuple]:
    """値のある足を時間順に key ごとにまとめる（key は時刻について単調）"""
    groups = []
    for r in sorted(drop_empty(rows), key=lambda r: r["started_at"]):
        k = key(r["started_at"])
        if groups and groups[-1][0] == k:
            groups[-1][1].append(r)
        else:
            groups.append((k, [r]))
    return groups


def resample(rows: list[dict], seconds: int) -> list[dict]:
    """足を seconds 秒（300・900・3600 等。60 の倍数で 3600 を割り切る）の足に束ねる。
    区間は UNIX 秒で切る（= JST の正時から）"""
    if seconds <= 0 or seconds % 60 or 3600 % seconds:
        raise ValueError("seconds は 60 の倍数で 3600 を割り切る値にする")

    def key(t):
        return int(t.timestamp()) // seconds * seconds

    return [_merge(rs, datetime.fromtimestamp(k, UTC)) for k, rs in _group(rows, key)]


def trading_day(t: datetime, holidays) -> date:
    """t（aware）が属する取引日。JST + 7 時間の日付から土日・休業日（holidays）を飛ばした最初の日。
    日中（09:00〜16:59）はその日、夜間（17:00〜翌 06:59）は次の取引日（金曜・休業日の前日の夜間は休み明けの日）"""
    d = (t.astimezone(JST) + _SESSION_SHIFT).date()
    while d.weekday() >= 5 or d in holidays:
        d += timedelta(days=1)
    return d


def daily(rows: list[dict], holidays, after: date) -> list[dict]:
    """足を取引日ごとの日足に束ねる。after 以下の取引日（読んだ範囲の先頭で欠けている日）は出さない。
    started_at = その取引日の 00:00 JST（UTC で表す）"""
    out = []
    for d, rs in _group(rows, lambda t: trading_day(t, holidays)):
        if d > after:
            out.append(_merge(rs, datetime.combine(d, time(0), JST).astimezone(UTC)))
    return out

"""足の束ね（/atm_iv_bars の 5 分・15 分足と日足、#68）と期近つなぎの限月の乗り換え（n=roll、#101）。純粋関数。

行は {"open","high","low","close","started_at"}（started_at は aware な UTC。sql/atm_iv_minute.sql・sql/atm_iv.sql の
行と同じ形）。n=roll の行は last_trading_day（date。sql/atm_iv_months.sql 等）も持ち、束ねるときは限月ごとに分ける。
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
    # rows は時間順・値のある足だけ（1 本以上）。限月の列（n=roll）があれば写す（無い行の出力は #68 のまま）
    out = {
        "open": rows[0]["open"],
        "high": max(r["high"] for r in rows),
        "low": min(r["low"] for r in rows),
        "close": rows[-1]["close"],
        "started_at": started_at,
    }
    if "last_trading_day" in rows[0]:
        out["last_trading_day"] = rows[0]["last_trading_day"]
    return out


def _group(rows: list[dict], key) -> list[tuple]:
    """値のある足を時間順に key(行) ごとにまとめる（並びは各まとまりの最初の足の時刻順）"""
    groups = {}
    for r in sorted(drop_empty(rows), key=lambda r: r["started_at"]):
        groups.setdefault(key(r), []).append(r)
    return list(groups.items())


def resample(rows: list[dict], seconds: int) -> list[dict]:
    """足を seconds 秒（300・900・3600 等。60 の倍数で 3600 を割り切る）の足に束ねる。
    区間は UNIX 秒で切る（= JST の正時から）"""
    if seconds <= 0 or seconds % 60 or 3600 % seconds:
        raise ValueError("seconds は 60 の倍数で 3600 を割り切る値にする")

    def key(r):
        # 違う限月（n=roll）の足は同じ区間でも 1 本にしない
        return int(r["started_at"].timestamp()) // seconds * seconds, r.get("last_trading_day")

    return [_merge(rs, datetime.fromtimestamp(k, UTC)) for (k, _), rs in _group(rows, key)]


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
    for (d, _), rs in _group(rows, lambda r: (trading_day(r["started_at"], holidays), r.get("last_trading_day"))):
        if d > after:
            out.append(_merge(rs, datetime.combine(d, time(0), JST).astimezone(UTC)))
    return out


def business_days_after(d: date, last: date, holidays, limit: int | None = None) -> int:
    """d より後で last 以下の営業日（土日でなく、休業日（holidays）でもない日）の数（残存営業日数。last <= d なら 0）。
    祝日取引の日・カレンダーの掲載範囲外の平日は営業日（holidays に無い）。limit を渡したら limit で数えるのを止める"""
    count = 0
    while d < last and (limit is None or count < limit):
        d += timedelta(days=1)
        if d.weekday() < 5 and d not in holidays:
            count += 1
    return count


def roll_month(candidates, d: date, holidays, n: int) -> date | None:
    """取引日 d に使う限月: 候補（最終取引日。None は無視）のうち残存営業日数が n より多い最小のもの。
    期近の残存が n 営業日以下になった取引日から 2 限月（満了した限月・最終取引日の当日は選ばない）。無ければ None"""
    for last in sorted({c for c in candidates if c is not None}):
        if business_days_after(d, last, holidays, n + 1) > n:
            return last
    return None


def roll(rows: list[dict], holidays, n: int) -> list[dict]:
    """期近つなぎ（#101）: 全限月の足から、取引日ごとに roll_month の限月の足だけを残す（started_at 昇順）。
    候補はその取引日の行（値の無い行も含む）の限月。選んだ限月の値の無い足・IV が 0 以下（low <= 0）の足は除き、
    期近へ戻さない（その時間は線が途切れる）。乗り換えは取引日の境目（夜間の始まり）に来る"""
    days = [(trading_day(r["started_at"], holidays), r) for r in rows]
    candidates = {}
    for d, r in days:
        candidates.setdefault(d, set()).add(r["last_trading_day"])
    months = {d: roll_month(c, d, holidays, n) for d, c in candidates.items()}
    out = [r for d, r in days
           if r["last_trading_day"] == months[d] and r["high"] is not None and r["low"] > 0]
    return sorted(out, key=lambda r: r["started_at"])

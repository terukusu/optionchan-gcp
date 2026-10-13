"""取引カレンダーの読み込みとセッションの判定（純粋関数）。

取引時間・grace・no_night_session_before はイメージの config/trading_calendar.toml。
休業日（closed_dates）と covered_until は BigQuery の最新版（#63、calendar_sync.py）で置き換え、
読めないときは TOML の値を使う（with_closed_dates）。
"""

import tomllib
from dataclasses import dataclass, replace
from datetime import date, datetime, time, timedelta
from pathlib import Path

from .parser import JST

DAY = "day"
NIGHT = "night"

_TOP_KEYS = {"covered_until", "grace_minutes", "closed_dates", "no_night_session_before"}
_SESSION_KEYS = {"day_open", "day_close", "night_open", "night_close"}


@dataclass(frozen=True)
class Calendar:
    covered_until: date
    closed_dates: frozenset[date]
    no_night_session_before: frozenset[date]
    day_open: time
    day_close: time
    night_open: time
    night_close: time
    grace: timedelta

    def is_trading_day(self, d: date) -> bool:
        """土日・closed_dates 以外は取引日。

        closed_dates は covered_until より後の日付（JPX の「予定」の休業日）も常に適用する。
        covered_until より後で closed_dates に無い平日は、休業日かどうか分からないので取引日として扱う
        （取り込みを止めない側に倒す。呼び出し側は calendar_outdated を出し、鮮度判定をしない）
        """
        return d.weekday() < 5 and d not in self.closed_dates

    def next_trading_day(self, d: date) -> date:
        d += timedelta(days=1)
        while not self.is_trading_day(d):
            d += timedelta(days=1)
        return d

    def is_outdated(self, now: datetime) -> bool:
        """now が covered_until を過ぎた（未知の平日を取引日として判定している）"""
        return _jst(now).date() > self.covered_until

    def days_left(self, now: datetime) -> int:
        """covered_until までの日数（JST の日付。当日は 0、過ぎたら負）"""
        return (self.covered_until - _jst(now).date()).days


@dataclass(frozen=True)
class Session:
    kind: str  # DAY / NIGHT
    trade_date: date  # 取引日（夜間は次の取引日）
    start: datetime  # セッションの開始（JST aware）
    end: datetime  # セッションの終了（余裕を含まない立会の終了時刻。JST aware）


def _parse_time(text: str) -> time:
    hour, minute = text.split(":")
    return time(int(hour), int(minute))


def load_calendar(path: str | Path) -> Calendar:
    with open(path, "rb") as f:
        data = tomllib.load(f)
    # キーの置き場所の誤り（[sessions] の後に closed_dates を書く等）を黙って無視しない
    top = set(data) - {"sessions"}
    if top != _TOP_KEYS:
        raise ValueError(f"trading calendar keys: {sorted(top)} (expected {sorted(_TOP_KEYS)})")
    sessions = data["sessions"]
    if set(sessions) != _SESSION_KEYS:
        raise ValueError(f"trading calendar [sessions] keys: {sorted(sessions)}")
    return Calendar(
        covered_until=data["covered_until"],
        closed_dates=frozenset(data["closed_dates"]),
        no_night_session_before=frozenset(data["no_night_session_before"]),
        day_open=_parse_time(sessions["day_open"]),
        day_close=_parse_time(sessions["day_close"]),
        night_open=_parse_time(sessions["night_open"]),
        night_close=_parse_time(sessions["night_close"]),
        grace=timedelta(minutes=int(data["grace_minutes"])),
    )


def with_closed_dates(cal: Calendar, closed_dates, covered_until: date) -> Calendar:
    """休業日と covered_until だけを置き換えたカレンダー（取引時間などはそのまま）"""
    return replace(cal, closed_dates=frozenset(closed_dates), covered_until=covered_until)


def sessions_of(cal: Calendar) -> dict[str, time]:
    return {
        "day_open": cal.day_open,
        "day_close": cal.day_close,
        "night_open": cal.night_open,
        "night_close": cal.night_close,
    }


def _jst(now: datetime) -> datetime:
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    return now.astimezone(JST)


def _at(d: date, t: time) -> datetime:
    return datetime.combine(d, t, tzinfo=JST)


def session_at(now: datetime, cal: Calendar) -> Session | None:
    """now（実時刻）が取り込む時間帯なら Session、時間外なら None。

    - 日中: 取引日 D の [day_open, day_close + grace]
    - 夜間: 取引日 P の [night_open, 翌日 night_close + grace]。取引日は P の次の取引日 D
      （D が no_night_session_before にあれば夜間なし）
    """
    now = _jst(now)
    today = now.date()

    if cal.is_trading_day(today):
        start, end = _at(today, cal.day_open), _at(today, cal.day_close)
        if start <= now <= end + cal.grace:
            return Session(DAY, today, start, end)

    # 夜間の開始日 P は今日（夕方以降）か前日（翌朝）
    for p in (today, today - timedelta(days=1)):
        if not cal.is_trading_day(p):
            continue
        d = cal.next_trading_day(p)
        if d in cal.no_night_session_before:
            continue
        start = _at(p, cal.night_open)
        end = _at(p + timedelta(days=1), cal.night_close)
        if start <= now <= end + cal.grace:
            return Session(NIGHT, d, start, end)
    return None

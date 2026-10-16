"""取引カレンダーとセッションの判定（純粋関数）。

取引時間・grace・no_night_session_before は settings の定数。休業日（closed_dates）と covered_until は
BigQuery の最新版（#63、calendar_sync.CalendarHolder）だけが正で、コードには持たない（#75）。
版を一度も読めていないときは休業日なし（土日以外はすべて取引日）・covered_until なしの値（build_calendar() の既定）。
"""

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta

from . import settings
from .parser import JST

DAY = "day"
NIGHT = "night"


@dataclass(frozen=True)
class Calendar:
    covered_until: date | None  # 版の確定の最終日。版を読めていなければ None（休業日が分からない）
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
        """休業日を判定できない（未知の平日を取引日として判定している）: now が covered_until を過ぎたか、
        版を読めていない（covered_until が None）"""
        return self.covered_until is None or _jst(now).date() > self.covered_until

    def days_left(self, now: datetime) -> int | None:
        """covered_until までの日数（JST の日付。当日は 0、過ぎたら負）。版を読めていなければ None"""
        if self.covered_until is None:
            return None
        return (self.covered_until - _jst(now).date()).days


@dataclass(frozen=True)
class Session:
    kind: str  # DAY / NIGHT
    trade_date: date  # 取引日（夜間は次の取引日）
    start: datetime  # セッションの開始（JST aware）
    end: datetime  # セッションの終了（余裕を含まない立会の終了時刻。JST aware）


def build_calendar(closed_dates=(), covered_until: date | None = None) -> Calendar:
    """settings の取引時間に休業日と covered_until を合わせたカレンダー。引数なしは版を読めていないときの値"""
    return Calendar(
        covered_until=covered_until,
        closed_dates=frozenset(closed_dates),
        no_night_session_before=frozenset(settings.NO_NIGHT_SESSION_BEFORE),
        day_open=settings.SESSION_DAY_OPEN,
        day_close=settings.SESSION_DAY_CLOSE,
        night_open=settings.SESSION_NIGHT_OPEN,
        night_close=settings.SESSION_NIGHT_CLOSE,
        grace=timedelta(minutes=settings.SESSION_GRACE_MINUTES),
    )


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


def in_session(t: datetime, cal: Calendar) -> bool:
    """t（ページの更新時刻など）が立会の時間 [開始, 終了) の中か（余裕の時間帯・終了時刻は含めない）。
    本文の凍結の判定（#88、snapshot.BodyWatch）が使う。寄り前・引け後のページは本文が動かないので数えない"""
    session = session_at(t, cal)
    return session is not None and session.start <= _jst(t) < session.end

from datetime import UTC, date, datetime, time, timedelta

import pytest
from calgen import CAL

from optionchan_ingest import settings
from optionchan_ingest.calendar import DAY, NIGHT, build_calendar, session_at
from optionchan_ingest.parser import jst


@pytest.fixture(scope="module")
def cal():
    return CAL


def test_fixed_values_are_constants():
    """#75: 立会時間・余裕・夜間なしの日は settings の定数（休業日・covered_until は BigQuery の版だけ）"""
    default = build_calendar()
    assert (default.day_open, default.day_close) == (time(8, 45), time(15, 45))
    assert (default.night_open, default.night_close) == (time(17, 0), time(6, 0))
    assert default.grace == timedelta(minutes=30)
    assert default.no_night_session_before == frozenset()
    assert settings.NO_NIGHT_SESSION_BEFORE == frozenset()


def test_without_version_every_weekday_is_trading_day():
    """版を一度も読めていない: 休業日なし（土日以外はすべて取引日）。休業日が分からないので期限切れと同じ扱い"""
    default = build_calendar()
    assert default.closed_dates == frozenset()
    assert default.covered_until is None
    assert session_at(jst(2026, 11, 23, 10, 0), default).kind == DAY  # 版では休業日
    assert session_at(jst(2026, 12, 31, 10, 0), default).kind == DAY
    assert session_at(jst(2026, 10, 17, 10, 0), default) is None  # 土曜
    assert default.is_outdated(jst(2026, 10, 14, 10, 0))
    assert default.days_left(jst(2026, 10, 14, 10, 0)) is None


# (実時刻 JST, 期待: None か (kind, 取引日, セッション開始))
CASES = [
    # 日中の開始・終了 + 余裕（30 分）
    ("0844", jst(2026, 10, 14, 8, 44), None),
    ("0845", jst(2026, 10, 14, 8, 45), (DAY, date(2026, 10, 14), jst(2026, 10, 14, 8, 45))),
    ("1615", jst(2026, 10, 14, 16, 15), (DAY, date(2026, 10, 14), jst(2026, 10, 14, 8, 45))),
    ("1616", jst(2026, 10, 14, 16, 16), None),
    ("1659", jst(2026, 10, 14, 16, 59), None),
    # 夜間: 17:00 開始、日付またぎ、06:00 + 30 分まで
    ("1700", jst(2026, 10, 14, 17, 0), (NIGHT, date(2026, 10, 15), jst(2026, 10, 14, 17, 0))),
    ("2359", jst(2026, 10, 14, 23, 59), (NIGHT, date(2026, 10, 15), jst(2026, 10, 14, 17, 0))),
    ("0001", jst(2026, 10, 15, 0, 1), (NIGHT, date(2026, 10, 15), jst(2026, 10, 14, 17, 0))),
    ("0630", jst(2026, 10, 15, 6, 30), (NIGHT, date(2026, 10, 15), jst(2026, 10, 14, 17, 0))),
    ("0631", jst(2026, 10, 15, 6, 31), None),
    # 金曜夜間 → 土曜 06:00 まで（取引日は翌月曜）
    ("fri_night", jst(2026, 10, 16, 22, 0), (NIGHT, date(2026, 10, 19), jst(2026, 10, 16, 17, 0))),
    ("sat_0600", jst(2026, 10, 17, 6, 0), (NIGHT, date(2026, 10, 19), jst(2026, 10, 16, 17, 0))),
    ("sat_0700", jst(2026, 10, 17, 7, 0), None),
    ("sat_noon", jst(2026, 10, 17, 12, 0), None),
    # 日曜の夕方は時間外（月曜の前の夜間は無い）
    ("sun_1700", jst(2026, 10, 18, 17, 0), None),
    ("mon_0300", jst(2026, 10, 19, 3, 0), None),
    ("mon_0845", jst(2026, 10, 19, 8, 45), (DAY, date(2026, 10, 19), jst(2026, 10, 19, 8, 45))),
    # 祝日取引（平日の祝日は取引日。2026-11-03 文化の日）
    ("holiday_trading", jst(2026, 11, 3, 10, 0), (DAY, date(2026, 11, 3), jst(2026, 11, 3, 8, 45))),
    ("holiday_eve_night", jst(2026, 11, 2, 20, 0), (NIGHT, date(2026, 11, 3), jst(2026, 11, 2, 17, 0))),
    # 2026-11-23（祝日取引なし）: 日中・夜間とも無し。金曜夜間の取引日は火曜
    ("1123_day", jst(2026, 11, 23, 10, 0), None),
    ("1123_night", jst(2026, 11, 23, 20, 0), None),
    ("1120_fri_night", jst(2026, 11, 20, 20, 0), (NIGHT, date(2026, 11, 24), jst(2026, 11, 20, 17, 0))),
    # 年末年始: 大納会 12/30 の夜間は実施（取引日は大発会 1/4）、12/31 日中・1/1 は休み
    ("1230_night", jst(2026, 12, 30, 20, 0), (NIGHT, date(2027, 1, 4), jst(2026, 12, 30, 17, 0))),
    ("1231_0600", jst(2026, 12, 31, 6, 0), (NIGHT, date(2027, 1, 4), jst(2026, 12, 30, 17, 0))),
    ("1231_day", jst(2026, 12, 31, 10, 0), None),
    ("1231_night", jst(2026, 12, 31, 20, 0), None),
    ("0101", jst(2027, 1, 1, 10, 0), None),
    ("0103_night", jst(2027, 1, 3, 20, 0), None),
    ("0104_day", jst(2027, 1, 4, 9, 0), (DAY, date(2027, 1, 4), jst(2027, 1, 4, 8, 45))),
]


@pytest.mark.parametrize("now, expected", [pytest.param(n, e, id=i) for i, n, e in CASES])
def test_session_at(cal, now, expected):
    session = session_at(now, cal)
    if expected is None:
        assert session is None
    else:
        assert (session.kind, session.trade_date, session.start) == expected


def test_session_at_accepts_utc(cal):
    # UTC 2026-10-14 00:00 = JST 09:00
    session = session_at(datetime(2026, 10, 14, 0, 0, tzinfo=UTC), cal)
    assert session.kind == DAY


def test_session_at_rejects_naive(cal):
    with pytest.raises(ValueError):
        session_at(datetime(2026, 10, 14, 9, 0), cal)


def test_covered_until_exceeded(cal):
    """covered_until より後: closed_dates に書いた日（JPX の「予定」の休業日）は休み、書いていない平日は取引日"""
    after = cal.covered_until.replace(year=cal.covered_until.year + 1)
    assert cal.is_outdated(jst(after.year, after.month, after.day, 10, 0))
    assert not cal.is_outdated(jst(2026, 10, 14, 10, 0))
    assert cal.covered_until < date(2027, 12, 31)
    assert session_at(jst(2027, 12, 31, 10, 0), cal) is None  # closed_dates（予定）は常に使う
    assert session_at(jst(2027, 9, 20, 10, 0), cal) is None
    # 未知の平日（例 2028-01-03（月）は年始休業日だが未記載）は取引日として扱う。期限切れの通知で気付いて足す
    assert session_at(jst(2028, 1, 3, 10, 0), cal).kind == DAY
    assert session_at(jst(2028, 1, 8, 10, 0), cal) is None  # 土曜


def test_days_left(cal):
    d = cal.covered_until
    assert cal.days_left(jst(d.year, d.month, d.day, 23, 0)) == 0
    after = d + timedelta(days=1)
    assert cal.days_left(jst(after.year, after.month, after.day, 0, 0)) == -1


@pytest.mark.parametrize(
    "now, end",
    [
        (jst(2026, 10, 14, 16, 0), jst(2026, 10, 14, 15, 45)),
        (jst(2026, 10, 15, 6, 10), jst(2026, 10, 15, 6, 0)),
        (jst(2026, 10, 16, 22, 0), jst(2026, 10, 17, 6, 0)),
    ],
)
def test_session_end_excludes_grace(cal, now, end):
    assert session_at(now, cal).end == end


def test_no_night_session_before(monkeypatch):
    monkeypatch.setattr(settings, "NO_NIGHT_SESSION_BEFORE", frozenset({date(2026, 10, 15)}))
    custom = build_calendar(CAL.closed_dates, CAL.covered_until)
    assert session_at(jst(2026, 10, 14, 20, 0), custom) is None
    assert session_at(jst(2026, 10, 15, 20, 0), custom).kind == NIGHT

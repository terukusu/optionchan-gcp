"""足の束ね・取引日の検査（#68 AC 5 のうち CI で動くもの）。BigQuery は使わない（純粋関数）。"""

from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from optionchan_viewer import bars

JST = ZoneInfo("Asia/Tokyo")
# 2026-10-09 は金曜、10-12（月）はスポーツの日（休業日）
HOLIDAYS = frozenset({date(2026, 10, 12)})


def jst(day, hour, minute=0):
    return datetime(2026, 10, day, hour, minute, tzinfo=JST).astimezone(UTC)


def row(at, o, h, low, c):
    return {"open": o, "high": h, "low": low, "close": c, "started_at": at}


def naive_ohlc(values):
    """1 本の足の素朴な集計（時間順の値の並び。None は飛ばす）"""
    vals = [v for v in values if v is not None]
    return (vals[0], max(vals), min(vals), vals[-1]) if vals else None


# 1 分足（API の 1 分足と同じ形。値の無い分は high が None）。日中の終わり 16:14 と夜間の始まり 17:00 の空きを含む
MINUTES = [
    row(jst(13, 15, 58), 18.0, 18.5, 17.9, 18.2),
    row(jst(13, 15, 59), 18.2, 18.3, 18.1, 18.1),
    row(jst(13, 16, 0), None, None, None, None),  # 全部 NULL の分
    row(jst(13, 16, 1), 18.4, 18.4, 18.0, 18.0),
    row(jst(13, 16, 4), 18.0, 19.0, 17.5, 18.6),  # 途中で ATM の行使価格が変わった分（1 分足の段階で束ね済み）
    row(jst(13, 16, 5), 18.6, 18.7, 18.6, 18.7),  # 5 分の境目
    row(jst(13, 16, 14), 18.7, 18.9, 18.7, 18.8),  # 日中の最後
    row(jst(13, 17, 0), 19.0, 19.2, 18.9, 19.1),  # 夜間の最初（15 分・1 時間の境目）
    row(jst(13, 17, 14), 19.1, 19.1, 19.0, 19.0),
    row(jst(13, 17, 15), 19.0, 19.3, 19.0, 19.3),
]


def test_drop_empty():
    rows = bars.drop_empty(MINUTES)
    assert len(rows) == len(MINUTES) - 1
    assert all(r["high"] is not None for r in rows)
    assert [r["started_at"] for r in rows] == sorted(r["started_at"] for r in rows)


@pytest.mark.parametrize("seconds", [300, 900, 3600])
def test_resample_matches_naive(seconds):
    got = bars.resample(MINUTES, seconds)
    # 素朴な集計: 区間ごとに 1 分足の open・close の時間順の並びと high・low の並びから作る
    groups = {}
    for r in MINUTES:
        key = int(r["started_at"].timestamp()) // seconds * seconds
        groups.setdefault(key, []).append(r)
    want = []
    for key in sorted(groups):
        rs = [r for r in groups[key] if r["high"] is not None]
        if not rs:
            continue
        o = naive_ohlc([r["open"] for r in rs])[0]
        c = naive_ohlc([r["close"] for r in rs])[3]
        want.append(row(datetime.fromtimestamp(key, UTC), o, max(r["high"] for r in rs),
                        min(r["low"] for r in rs), c))
    assert got == want


def test_resample_5m_boundaries_and_gap():
    got = bars.resample(MINUTES, 300)
    assert [r["started_at"] for r in got] == [jst(13, 15, 55), jst(13, 16, 0), jst(13, 16, 5), jst(13, 16, 10),
                                              jst(13, 17, 0), jst(13, 17, 10), jst(13, 17, 15)]
    # 16:00 の足: 16:00（全部 NULL）は飛ばし、16:01 の open・16:04 の close
    assert got[1] == row(jst(13, 16, 0), 18.4, 19.0, 17.5, 18.6)
    # 16:15〜16:59 の足は作らない（入力に無い時間）
    assert not [r for r in got if jst(13, 16, 15) <= r["started_at"] < jst(13, 17, 0)]


def test_resample_15m():
    got = bars.resample(MINUTES, 900)
    assert got == [
        row(jst(13, 15, 45), 18.0, 18.5, 17.9, 18.1),
        row(jst(13, 16, 0), 18.4, 19.0, 17.5, 18.8),
        row(jst(13, 17, 0), 19.0, 19.2, 18.9, 19.0),
        row(jst(13, 17, 15), 19.0, 19.3, 19.0, 19.3),
    ]


def test_resample_all_null_bucket_is_dropped():
    rows = [row(jst(13, 10, 0), None, None, None, None), row(jst(13, 10, 3), None, None, None, None),
            row(jst(13, 10, 5), 20.0, 20.0, 20.0, 20.0)]
    assert bars.resample(rows, 300) == [row(jst(13, 10, 5), 20.0, 20.0, 20.0, 20.0)]
    assert bars.resample([], 900) == []


@pytest.mark.parametrize("seconds", [0, 30, 90, 420, 7200, 86400])
def test_resample_rejects_bad_seconds(seconds):
    with pytest.raises(ValueError):
        bars.resample(MINUTES, seconds)


@pytest.mark.parametrize("at,want", [
    (jst(13, 9, 0), date(2026, 10, 13)),  # 日中
    (jst(13, 16, 14), date(2026, 10, 13)),  # 日中の終わり
    (jst(13, 17, 0), date(2026, 10, 14)),  # 夜間 → 次の日
    (jst(13, 23, 59), date(2026, 10, 14)),
    (jst(14, 0, 0), date(2026, 10, 14)),  # 夜間の日付が変わった後 → その日
    (jst(14, 6, 14), date(2026, 10, 14)),
    (jst(9, 17, 0), date(2026, 10, 13)),  # 金曜の夜間 → 土日・休業日（月）を飛ばして火曜
    (jst(10, 5, 0), date(2026, 10, 13)),  # 土曜の未明（金曜の夜間の続き）
    (jst(9, 15, 0), date(2026, 10, 9)),  # 金曜の日中
])
def test_trading_day(at, want):
    assert bars.trading_day(at, HOLIDAYS) == want


def test_trading_day_holiday_trading_is_trading_day():
    # 祝日取引の実施日（休業日の表に無い平日）は取引日。その前日の夜間はその日に入る
    assert bars.trading_day(jst(11, 17, 0), frozenset()) == date(2026, 10, 12)
    assert bars.trading_day(jst(12, 10, 0), frozenset()) == date(2026, 10, 12)


def midnight(day):
    return datetime(2026, 10, day, tzinfo=JST).astimezone(UTC)


HOURS = [
    row(jst(8, 15, 0), 21.0, 21.0, 21.0, 21.0),  # 木曜の日中（先頭の欠けた日）
    row(jst(8, 17, 0), 22.0, 23.0, 21.5, 22.5),  # 木曜の夜間 → 金曜
    row(jst(9, 9, 0), 22.5, 24.0, 22.0, 23.0),  # 金曜の日中
    row(jst(9, 16, 0), None, None, None, None),  # 全部 NULL の時間
    row(jst(9, 17, 0), 23.0, 23.5, 20.0, 21.0),  # 金曜の夜間 → 休み明け（火曜。月曜は休業日）
    row(jst(10, 5, 0), 21.0, 22.0, 21.0, 21.5),
    row(jst(13, 9, 0), 21.5, 21.5, 19.0, 19.5),  # 火曜の日中
    row(jst(13, 17, 0), None, None, None, None),  # 全部 NULL の時間だけの日（水曜）は作らない
]


def test_daily():
    got = bars.daily(HOURS, HOLIDAYS, after=date(2026, 10, 8))
    assert got == [
        row(midnight(9), 22.0, 24.0, 21.5, 23.0),
        row(midnight(13), 23.0, 23.5, 19.0, 19.5),
    ]
    # started_at はその日の 00:00 JST（UTC で前日の 15:00）
    assert got[0]["started_at"] == datetime(2026, 10, 8, 15, tzinfo=UTC)


def test_daily_drops_days_up_to_after():
    # after 以下の取引日（読んだ範囲の先頭で欠けている日）は出さない
    assert [r["started_at"] for r in bars.daily(HOURS, HOLIDAYS, after=date(2026, 10, 9))] == [midnight(13)]
    assert len(bars.daily(HOURS, HOLIDAYS, after=date(2026, 10, 7))) == 3


def test_daily_without_holidays_puts_friday_night_on_monday():
    got = bars.daily(HOURS, frozenset(), after=date(2026, 10, 8))
    assert [r["started_at"] for r in got] == [midnight(9), midnight(12), midnight(13)]
    assert got[1] == row(midnight(12), 23.0, 23.5, 20.0, 21.5)


def test_daily_is_resample_compatible():
    # 日中だけの日の日足は 1 時間足を束ねたもの（open = 最初、close = 最後、high / low = 最大 / 最小）
    rows = [row(jst(13, hr, 0), 20.0 + hr / 10, 21.0 + hr, 19.0 - hr / 10, 20.5) for hr in range(9, 16)]
    [got] = bars.daily(rows, HOLIDAYS, after=date(2026, 10, 12))
    assert got == row(midnight(13), rows[0]["open"], rows[-1]["high"], rows[-1]["low"], rows[-1]["close"])
    assert got["started_at"] + timedelta(hours=9) == datetime(2026, 10, 13, tzinfo=UTC)

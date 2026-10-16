"""足の束ね・取引日の検査（#68 AC 5 のうち CI で動くもの）。BigQuery は使わない（純粋関数）。"""

from datetime import UTC, date, datetime, time, timedelta
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


# ---- 期近つなぎ（n=roll、#101） ----

NOV, DEC, MAR = date(2026, 11, 12), date(2026, 12, 10), date(2027, 3, 11)  # 11 月限（木）・12 月限・3 月限


def at(month, day, hour, year=2026):
    return datetime(year, month, day, hour, tzinfo=JST).astimezone(UTC)


def mrow(t, month, v=20.0, low=None):
    """限月付きの足（sql/atm_iv_months.sql の行の形）。v が None なら値の無い足"""
    if v is None:
        return {"open": None, "high": None, "low": None, "close": None, "last_trading_day": month, "started_at": t}
    return {"open": v, "high": v + 1, "low": v - 1 if low is None else low, "close": v, "last_trading_day": month,
            "started_at": t}


def both(times, months=(NOV, DEC)):
    """各時刻に限月の行をそろえる（期近 = 20.0、2 限月 = 30.0、期先 = 40.0）"""
    return [mrow(t, m, 20.0 + 10 * i) for t in times for i, m in enumerate(months)]


def picked(rows):
    return [(r["started_at"], r["last_trading_day"]) for r in rows]


@pytest.mark.parametrize("d,last,holidays,limit,want", [
    (NOV, NOV, frozenset(), None, 0),  # 当日は 0
    (date(2026, 11, 13), NOV, frozenset(), None, 0),  # 満了した限月
    (date(2026, 11, 5), NOV, frozenset(), None, 5),  # 金・月・火・水・木
    (date(2026, 11, 4), NOV, frozenset(), None, 6),
    (date(2026, 11, 6), NOV, frozenset(), None, 4),  # 土日を数えない
    (date(2026, 11, 4), NOV, frozenset({date(2026, 11, 10)}), None, 5),  # 休業日を数えない
    (date(2026, 11, 2), date(2026, 11, 4), frozenset(), None, 2),  # 祝日取引の日（11-03。休業日の表に無い）は営業日
    (date(2026, 11, 2), date(2026, 11, 4), frozenset({date(2026, 11, 3)}), None, 1),
    (date(2026, 10, 1), DEC, frozenset(), 6, 6),  # limit で打ち切る
    (date(2026, 11, 9), NOV, frozenset(), 6, 3),
])
def test_business_days_after(d, last, holidays, limit, want):
    assert bars.business_days_after(d, last, holidays, limit) == want


@pytest.mark.parametrize("d,want", [
    (date(2026, 11, 4), NOV),  # r = 6
    (date(2026, 11, 5), DEC),  # r = 5 → 2 限月
    (NOV, DEC),  # 最終取引日の当日（r = 0）
    (date(2026, 11, 13), DEC),  # 満了した限月は候補にあっても選ばない
    (date(2026, 12, 4), None),  # 12 月限も残存 4 で、次の限月が候補に無い
])
def test_roll_month(d, want):
    assert bars.roll_month([NOV, DEC, None], d, frozenset(), 5) == want


def test_roll_month_three_months_picks_second():
    # 3 限月の時期（2018〜2019）: 乗り換えの後は 2 番目に小さい限月（期先の四半期限月へ飛ばない）
    assert bars.roll_month([MAR, DEC, NOV], date(2026, 11, 5), frozenset(), 5) == DEC
    assert bars.roll_month([MAR, DEC, NOV], date(2026, 11, 4), frozenset(), 5) == NOV


def test_roll_boundary_is_night_session():
    # 取引日 11-05（r = 5）から 12 月限。夜間は前日 11-04 の 17 時から 11-05 の取引日
    times = [at(11, 3, 15), at(11, 4, 10), at(11, 4, 15), at(11, 4, 17), at(11, 4, 23), at(11, 5, 10)]
    got = bars.roll(both(times), frozenset(), 5)
    assert picked(got) == [(times[0], NOV), (times[1], NOV), (times[2], NOV), (times[3], DEC), (times[4], DEC),
                           (times[5], DEC)]
    assert [r["close"] for r in got] == [20.0, 20.0, 20.0, 30.0, 30.0, 30.0]


def test_roll_friday_night_goes_to_monday():
    # L = 11-16（月）: 乗り換えの取引日は 11-09（月、r = 5）。前の金曜 11-06 の 17 時からの夜間が 2 限月
    last = date(2026, 11, 16)
    times = [at(11, 6, 15), at(11, 6, 17), at(11, 7, 2), at(11, 9, 10)]
    got = bars.roll(both(times, (last, DEC)), frozenset(), 5)
    assert picked(got) == [(times[0], last), (times[1], DEC), (times[2], DEC), (times[3], DEC)]


def test_roll_holiday_moves_switch_one_day_earlier():
    times = [at(11, 3, 15), at(11, 3, 17), at(11, 4, 15), at(11, 4, 17)]
    # 休業日なし: 11-04 の日中まで期近
    assert picked(bars.roll(both(times), frozenset(), 5)) == [(times[0], NOV), (times[1], NOV), (times[2], NOV),
                                                              (times[3], DEC)]
    # 11-10（火）が休業日: 5 営業日前が 1 日早まり、11-03 の 17 時（取引日 11-04）から
    assert picked(bars.roll(both(times), frozenset({date(2026, 11, 10)}), 5)) == [
        (times[0], NOV), (times[1], DEC), (times[2], DEC), (times[3], DEC)]


def test_roll_holiday_trading_day_counts():
    # 祝日取引の日（11-03。休業日の集合に入らない）を営業日に数える。休業日にすると乗り換えが 1 日早まる
    times = [at(10, 28, 15), at(10, 28, 17)]
    holidays = frozenset({date(2026, 11, 3)})
    last = date(2026, 11, 5)  # 10-29 から 5 営業日（30・2・3・4・5）
    assert picked(bars.roll(both(times, (last, DEC)), frozenset(), 5)) == [(times[0], last), (times[1], DEC)]
    assert picked(bars.roll(both(times, (last, DEC)), holidays, 5)) == [(times[0], DEC), (times[1], DEC)]


def test_roll_year_end_holidays():
    # 1 月限（L = 2026-01-08）→ 2 月限: 年末年始（12-31〜01-02）の休業をはさみ、取引日 2025-12-29（r = 5）から。
    # 2025-12-26（金）の日中までが 1 月限、その 17 時からの夜間が 2 月限
    jan, feb = date(2026, 1, 8), date(2026, 2, 12)
    holidays = frozenset({date(2025, 12, 31), date(2026, 1, 1), date(2026, 1, 2)})
    times = [at(12, 26, 10, 2025), at(12, 26, 15, 2025), at(12, 26, 17, 2025), at(12, 29, 10, 2025),
             at(12, 30, 17, 2025)]
    got = bars.roll(both(times, (jan, feb)), holidays, 5)
    assert picked(got) == [(times[0], jan), (times[1], jan), (times[2], feb), (times[3], feb), (times[4], feb)]
    # 休業日を知らないと乗り換えが遅れる（取引日 12-29 も 1 月限）
    assert picked(bars.roll(both(times, (jan, feb)), frozenset(), 5))[3] == (times[3], jan)


def test_roll_three_months():
    times = [at(11, 4, 15), at(11, 4, 17)]
    got = bars.roll(both(times, (NOV, DEC, MAR)), frozenset(), 5)
    assert picked(got) == [(times[0], NOV), (times[1], DEC)]


def test_roll_null_second_month_does_not_fall_back():
    # 2 限月の値が NULL の足は出ない（同じ時刻の期近に値があっても戻らない）。前後の足は 2 限月
    times = [at(11, 4, 17), at(11, 4, 18), at(11, 4, 19)]
    rows = [mrow(times[0], NOV), mrow(times[0], DEC, 30.0),
            mrow(times[1], NOV), mrow(times[1], DEC, None),
            mrow(times[2], NOV), mrow(times[2], DEC, 31.0)]
    assert picked(bars.roll(rows, frozenset(), 5)) == [(times[0], DEC), (times[2], DEC)]


def test_roll_drops_non_positive_iv():
    # IV が 0 以下の足（close = 0・low = 0 を含む）は出ない
    times = [at(11, 2, 10), at(11, 2, 11), at(11, 2, 12)]
    rows = [mrow(times[0], NOV, 20.0), mrow(times[1], NOV, 20.0, low=0.0),
            {**mrow(times[2], NOV, 0.0), "high": 0.0, "low": 0.0}, mrow(times[0], DEC, 30.0)]
    assert picked(bars.roll(rows, frozenset(), 5)) == [(times[0], NOV)]


def test_roll_on_last_trading_day_uses_second_month():
    times = [at(11, 12, 10), at(11, 13, 10)]
    # 満了後の行（11-13 の 11 月限）は来ないが、来ても選ばない
    assert picked(bars.roll(both(times), frozenset(), 5)) == [(times[0], DEC), (times[1], DEC)]


@pytest.mark.parametrize("n,switch", [
    (0, date(2026, 11, 12)),  # 最終取引日の当日から
    (5, date(2026, 11, 5)),
    (10, date(2026, 10, 29)),  # 10 営業日前（11-03 は営業日）
])
def test_roll_n_days_setting(n, switch):
    days = [date(2026, 10, 27) + timedelta(days=i) for i in range(17)]
    days = [d for d in days if d.weekday() < 5]
    times = [datetime.combine(d, time(10), JST).astimezone(UTC) for d in days]
    got = bars.roll(both(times), frozenset(), n)
    first_dec = next(r["started_at"] for r in got if r["last_trading_day"] == DEC)
    assert first_dec.astimezone(JST).date() == switch
    assert all(r["last_trading_day"] == NOV for r in got if r["started_at"] < first_dec)


def test_roll_output_is_sorted_and_unique():
    times = [at(11, 4, 17), at(11, 4, 15), at(11, 4, 16)]
    got = bars.roll(both(times), frozenset(), 5)
    ts = [r["started_at"] for r in got]
    assert ts == sorted(set(ts))


def test_daily_of_roll():
    # 日足: 取引日ごとに 1 本で last_trading_day = M(D)。乗り換えの取引日（11-05）の日足には前の営業日（11-04）の
    # 夜間（2 限月）だけが入り、期近の足が混ざらない
    times = [at(11, 4, 9), at(11, 4, 15), at(11, 4, 17), at(11, 4, 23), at(11, 5, 10)]
    rows = both(times)
    rows[4]["high"] = 99.0  # 11-04 17 時の 11 月限（選ばれない）の高値
    got = bars.daily(bars.roll(rows, frozenset(), 5), frozenset(), after=date(2026, 11, 3))
    assert [(r["started_at"].astimezone(JST).date(), r["last_trading_day"]) for r in got] == [
        (date(2026, 11, 4), NOV), (date(2026, 11, 5), DEC)]
    assert got[1]["open"] == 30.0 and got[1]["high"] == 31.0 and got[1]["close"] == 30.0


@pytest.mark.parametrize("seconds", [300, 900])
def test_resample_does_not_merge_months(seconds):
    # 同じ区間の別の分で限月が違う足は 1 本にしない（束ねた足は限月を保つ）
    t = datetime(2026, 11, 4, 8, 0, tzinfo=UTC)
    rows = [mrow(t, NOV, 20.0), mrow(t + timedelta(minutes=1), DEC, 30.0), mrow(t + timedelta(minutes=2), DEC, 31.0)]
    got = bars.resample(rows, seconds)
    assert [(r["started_at"], r["last_trading_day"], r["open"], r["close"]) for r in got] == [
        (t, NOV, 20.0, 20.0), (t, DEC, 30.0, 31.0)]


def test_resample_without_month_has_same_keys():
    # n=0/1 の行（last_trading_day の列が無い）の出力は今と同じ dict（キーが増えない）
    for r in bars.resample(MINUTES, 300) + bars.daily(HOURS, HOLIDAYS, after=date(2026, 10, 8)):
        assert set(r) == {"open", "high", "low", "close", "started_at"}

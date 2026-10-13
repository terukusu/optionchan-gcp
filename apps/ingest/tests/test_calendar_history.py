"""取引カレンダーの過去年の埋め戻し（#67）: xlsx の解析、過去年の行、版への追加、手動ジョブの --backfill-from。

fixture:
- holiday_trading_days.xlsx: JPX「祝日取引実施日（確定）一覧」（2026-10-14 取得）からシートと共有文字列だけを残し、
  行を 2026 年末までに絞ったもの
- syukujitsu_2018_2025.csv: 内閣府の祝日 CSV（CP932）の 2018〜2025 年の行
"""

from datetime import date, timedelta

import pytest
from conftest import FIXTURES, REPO_ROOT
from test_calendar_sync import CSV as CSV_2026
from test_calendar_sync import FIXTURE, VERSION_AT, FakeSync, events, loaded, logs, sync
from xlsxgen import read_rows, xlsx_bytes

from optionchan_ingest import calendar_sync
from optionchan_ingest import jpx_calendar as jc
from optionchan_ingest.calendar import load_calendar
from optionchan_ingest.calendar_sync import CalendarFetchFailed, CalendarHolder, SyncDeps
from optionchan_ingest.parser import jst

XLSX = (FIXTURES / "holiday_trading_days.xlsx").read_bytes()
CSV_PAST = (FIXTURES / "syukujitsu_2018_2025.csv").read_bytes()
# 内閣府の CSV は 1 ファイルに全年が入る（見出しは 1 行）
CSV_ALL = CSV_PAST + CSV_2026.split(b"\n", 1)[1]
HOLIDAYS = jc.parse_holidays_csv(CSV_ALL)
HELD = jc.parse_holiday_trading_xlsx(XLSX)
START, END = date(2018, 1, 1), date(2026, 1, 1)
XLSX_URL = "https://www.jpx.co.jp/derivatives/rules/holidaytrading/nlsgeu000006hweb-att/List_of_Finalized_Holiday_Trading_Days_J.xlsx"
CAL = load_calendar(REPO_ROOT / "config" / "trading_calendar.toml")


def fixture_version():
    return jc.build_version(jc.parse_jpx_page(FIXTURE))


def history(held=HELD, holidays=HOLIDAYS, start=START, end=END):
    return jc.history_rows(holidays, held, start, end)


def backfilled():
    return jc.with_history(fixture_version(), history())


# ---- SQ 日・取引最終日（読み手の契約: 版の全行を休業日として繰り上げる） ----


def is_business_day(d: date, rows) -> bool:
    return d.weekday() < 5 and d not in rows


def previous_business_day(d: date, rows) -> date:
    d -= timedelta(days=1)
    while not is_business_day(d, rows):
        d -= timedelta(days=1)
    return d


def sq_date(year: int, month: int, rows) -> date:
    """第 2 金曜日。休業日（版の全行）なら前の営業日"""
    first = date(year, month, 1)
    d = first + timedelta(days=(4 - first.weekday()) % 7 + 7)
    return d if is_business_day(d, rows) else previous_business_day(d, rows)


def last_trading_day(year: int, month: int, rows) -> date:
    return previous_business_day(sq_date(year, month, rows), rows)


def test_sq_2023_08_from_version_rows():
    """AC2: 2023-08 限は SQ 日 8/11（山の日。祝日取引は実施）が 8/10 に繰り上がり、取引最終日は 8/9"""
    version = backfilled()
    rows = {r.calendar_date for r in version.rows}  # is_trading_day に関係なく全行
    assert version.by_date[date(2023, 8, 11)].is_trading_day is True
    assert sq_date(2023, 8, rows) == date(2023, 8, 10)
    assert last_trading_day(2023, 8, rows) == date(2023, 8, 9)
    # is_trading_day = false だけを休業日にすると再現できない（祝日取引の実施日も業務規程上は休業日）
    closed = version.closed_dates
    assert sq_date(2023, 8, closed) == date(2023, 8, 11)
    # 祝日取引の開始前の例: 2019-05 限（第 2 金曜 5/10 は平日）・2020-07 限（第 2 金曜 7/10）は繰り上がらない
    assert sq_date(2019, 5, rows) == date(2019, 5, 10)
    assert last_trading_day(2019, 5, rows) == date(2019, 5, 9)
    # 2019-05 の前の 10 連休（4/27〜5/6）をまたぐ: 5/7 の前の営業日は 4/26
    assert previous_business_day(date(2019, 5, 7), rows) == date(2019, 4, 26)


# ---- xlsx ----


def test_parse_xlsx_fixture():
    """実物の形: 見出し 祝日取引の対象日・名称・実施有無、日付はシリアル値、文字列は共有文字列（ふりがなは除く）"""
    assert HELD[0] == jc.HeldDay(jc.HOLIDAY_TRADING_START, "秋分の日", True)
    assert HELD[-1] == jc.HeldDay(date(2026, 12, 31), "年末休業日", False)
    assert len(HELD) == 69
    by_day = {h.day: h for h in HELD}
    assert by_day[date(2023, 1, 9)] == jc.HeldDay(date(2023, 1, 9), "成人の日", False)
    assert by_day[date(2024, 2, 12)].name == "建国記念の日の振替休日"
    assert by_day[date(2023, 8, 11)].held is True


def test_xlsx_url_from_page():
    assert jc.xlsx_url(FIXTURE, "https://www.jpx.co.jp/derivatives/rules/holidaytrading/index.html") == XLSX_URL
    with pytest.raises(jc.CalendarParseError, match="0 個"):
        jc.xlsx_url(b"<html><body><a href='x.pdf'>x</a></body></html>", "https://example.com/")


@pytest.mark.parametrize(
    "data, match",
    [
        (b"not a zip", "zip"),
        (xlsx_bytes([], headers=("日付", "名称", "実施有無")), "見出し"),
        (xlsx_bytes([]), "行が無い"),
        (xlsx_bytes([(date(2022, 9, 23), "秋分の日", True)], held_text={date(2022, 9, 23): "実施"}), "実施有無"),
        (xlsx_bytes([(date(2022, 9, 23), "秋分の日", True)], sheets=2), "シート"),
        (
            xlsx_bytes([(date(2022, 10, 10), "スポーツの日", True), (date(2022, 9, 23), "秋分の日", True)]),
            "昇順",
        ),
    ],
)
def test_parse_xlsx_errors(data, match):
    with pytest.raises(jc.CalendarParseError, match=match):
        jc.parse_holiday_trading_xlsx(data)


def test_xlsxgen_round_trip():
    """テスト用の組み立て（xlsxgen）が fixture と同じ行になる（以下の検査の前提）"""
    assert jc.parse_holiday_trading_xlsx(xlsx_bytes(read_rows(XLSX))) == HELD


# ---- 過去年の行 ----


def test_history_rows_2018_2025():
    rows = history()
    by_date = {r.calendar_date: r for r in rows}
    assert len(rows) == 136
    assert {r.source for r in rows} == {jc.SOURCE_HISTORY}
    assert {r.status for r in rows} == {jc.CONFIRMED}
    assert all(r.calendar_date.weekday() < 5 for r in rows)
    # AC1: 平日の国民の祝日・休日と 12/31・1/1・1/2・1/3 がすべてある
    expected = {d for d in HOLIDAYS if START <= d < END and d.weekday() < 5} | {
        date(y, m, dd)
        for y in range(2018, 2026)
        for m, dd in ((12, 31), (1, 1), (1, 2), (1, 3))
        if date(y, m, dd).weekday() < 5
    }
    assert set(by_date) == expected
    # 祝日取引の開始前はすべて休業
    before = [r for r in rows if r.calendar_date < jc.HOLIDAY_TRADING_START]
    assert all(not r.is_trading_day and r.note == jc.NOTE_BEFORE_HOLIDAY_TRADING for r in before)
    assert by_date[date(2019, 10, 22)].name == "休日（祝日扱い）"  # 即位礼正殿の儀
    assert by_date[date(2020, 7, 24)].name == "スポーツの日"  # 東京五輪の移動
    assert by_date[date(2019, 1, 2)].day_type == jc.YEAR_END
    assert by_date[date(2019, 1, 1)].day_type == jc.NATIONAL_HOLIDAY
    assert by_date[date(2022, 9, 19)].is_trading_day is False  # 開始の直前（敬老の日）
    # 臨時休場は無い（2020-10-01 の東証のシステム障害は現物だけ。大阪取引所のデリバティブは取引あり）
    assert date(2020, 10, 1) not in by_date


def test_history_rows_match_xlsx():
    """AC1: 祝日取引の開始以後の行の日付・実施有無・名称は xlsx と一致する（1/1 は xlsx に載らないので休業）"""
    rows = [r for r in history() if r.calendar_date >= jc.HOLIDAY_TRADING_START]
    xlsx = {h.day: h for h in HELD if h.day < END}
    assert {r.calendar_date for r in rows} - set(xlsx) == {date(2024, 1, 1), date(2025, 1, 1)}
    for r in rows:
        if (r.calendar_date.month, r.calendar_date.day) == (1, 1):
            assert (r.is_trading_day, r.name, r.note) == (False, "元日", None)
            continue
        h = xlsx[r.calendar_date]
        assert (r.is_trading_day, r.name, r.note) == (h.held, h.name, None)
    # 2023-01-02 は CSV では「休日」（振替休日）だが年始休業日
    by_date = {r.calendar_date: r for r in rows}
    assert (by_date[date(2023, 1, 2)].day_type, by_date[date(2023, 1, 2)].name) == (jc.YEAR_END, "年始休業日")


def test_xlsx_matches_current_version():
    """xlsx の 2026 年の行は、今のページから作った版（確定の行）と一致する"""
    assert jc.held_days_mismatch(fixture_version(), HELD) == []
    changed = [h if h.day != date(2026, 11, 23) else jc.HeldDay(h.day, h.name, True) for h in HELD]
    assert jc.held_days_mismatch(fixture_version(), changed) == ["2026-11-23: xlsx True / 版 False"]


def _without(day):
    return [h for h in HELD if h.day != day]


@pytest.mark.parametrize(
    "held, holidays, match",
    [
        (_without(date(2023, 8, 11)), HOLIDAYS, "xlsx に無い"),
        (sorted([*HELD, jc.HeldDay(date(2023, 8, 14), "x", True)], key=lambda h: h.day), HOLIDAYS, "CSV"),
        (
            [h if h.day != date(2024, 12, 31) else jc.HeldDay(h.day, h.name, True) for h in HELD],
            HOLIDAYS,
            "12/31",
        ),
        (sorted([*HELD, jc.HeldDay(date(2025, 1, 1), "元日", False)], key=lambda h: h.day), HOLIDAYS, "1/1"),
        (HELD[1:], HOLIDAYS, "最初の日"),
        (HELD, {d: n for d, n in HOLIDAYS.items() if d.year != 2020}, "2020"),
    ],
)
def test_history_rows_errors(held, holidays, match):
    with pytest.raises(jc.CalendarParseError, match=match):
        history(held=held, holidays=holidays)


def test_history_rows_before_start_needs_no_xlsx_rows():
    rows = jc.history_rows(HOLIDAYS, [], date(2018, 1, 1), date(2022, 1, 1))
    assert all(not r.is_trading_day for r in rows)
    with pytest.raises(ValueError):
        jc.history_rows(HOLIDAYS, HELD, date(2018, 2, 1), END)


# ---- 版・検証・ingest ----


def test_with_history_only_before_listed_range():
    version = backfilled()
    assert version.listed_from == START
    assert version.covered_until == fixture_version().covered_until
    assert version.listed_until == fixture_version().listed_until
    assert [r for r in version.rows if r.calendar_date >= END] == list(fixture_version().rows)
    with pytest.raises(ValueError):
        jc.with_history(
            fixture_version(), [jc.DayRow(date(2026, 1, 2), False, "confirmed", "year_end", "x", None, "history")]
        )


def test_backfill_passes_validation():
    """過去年の追加は前の版の掲載範囲より前の行の追加だけなので past_changed に当たらない"""
    ref = jc.reference_from_version(fixture_version())
    problems = jc.validate(backfilled(), jc.parse_jpx_page(FIXTURE), HOLIDAYS, ref, date(2026, 10, 15))
    assert problems == []


def test_past_rows_other_than_history_are_still_changes():
    """掲載範囲より前でも source = history でない行の追加・既存の行の変更は past_changed"""
    ref = jc.reference_from_version(fixture_version())
    jpx_row = jc.DayRow(date(2025, 11, 24), False, jc.CONFIRMED, jc.NATIONAL_HOLIDAY, "x", None, jc.SOURCE_JPX)
    version = jc.with_history(fixture_version(), [jpx_row])
    rules = [p.rule for p in jc.validate(version, jc.parse_jpx_page(FIXTURE), HOLIDAYS, ref, date(2026, 10, 15))]
    assert rules == ["past_changed"]
    # 前の版にある過去年の行が消える（引き継げない）のも past_changed
    ref2 = jc.reference_from_version(backfilled())
    problems = jc.validate(fixture_version(), jc.parse_jpx_page(FIXTURE), HOLIDAYS, ref2, date(2026, 10, 15))
    assert [p.rule for p in problems] == ["past_changed"]


def test_backfilled_rows_round_trip_and_ingest_sanity():
    """ingest は history の行を読める。1 年 25 日超・14 日以内の増加に当たらない"""
    version = backfilled()
    loaded_ = jc.from_rows(jc.to_rows(version, VERSION_AT, jc.ACCEPTED_AUTO))
    assert loaded_.version == version
    assert jc.ingest_sanity(loaded_, CAL.closed_dates, date(2026, 10, 15)) == []
    per_year = {}
    for d in version.closed_dates:
        per_year[d.year] = per_year.get(d.year, 0) + 1
    assert max(per_year.values()) == per_year[2019] == 20 <= jc.MAX_CLOSED_PER_YEAR
    # 前の版（2026〜）を使っている ingest が埋め戻した版に切り替えても、今日以降の判定は同じ
    holder = CalendarHolder(CAL, load=None)
    holder.apply(loaded(fixture_version()), {}, "test", jst(2026, 10, 15, 9, 0))
    before = holder.current
    holder.apply(
        jc.LoadedVersion(VERSION_AT + timedelta(days=1), version, jc.ACCEPTED_AUTO), {}, "test", jst(2026, 10, 15, 9, 0)
    )
    assert holder.version_at == VERSION_AT + timedelta(days=1)
    assert {d for d in holder.current.closed_dates if d >= date(2026, 10, 15)} == {
        d for d in before.closed_dates if d >= date(2026, 10, 15)
    }
    assert not holder.current.is_trading_day(date(2019, 5, 1))


def test_toml_ignores_history_rows():
    text = (REPO_ROOT / "config" / "trading_calendar.toml").read_text(encoding="utf-8")
    assert jc.render_toml(text, backfilled(), date(2026, 10, 15)) == jc.render_toml(
        text, fixture_version(), date(2026, 10, 15)
    )


# ---- run_sync / 手動ジョブ ----


class FakeBackfill(FakeSync):
    def __init__(self, xlsx=XLSX, xlsx_error=None, **kwargs):
        kwargs.setdefault("csv", CSV_ALL)
        super().__init__(**kwargs)
        self.xlsx = xlsx
        self.xlsx_error = xlsx_error

    def fetch_url(self, url, timeout):
        self.calls.append(("url", url))
        if self.xlsx_error:
            raise self.xlsx_error
        return self.xlsx

    def deps(self, base=CAL):
        d = super().deps(base)
        return SyncDeps(
            d.base, d.load_latest, d.fetch_page, d.fetch_holidays, d.append_rows, d.clock, fetch_url=self.fetch_url
        )


def test_backfill_writes_one_version(capsys):
    fake = FakeBackfill(latest=loaded())
    result = sync(fake, backfill_from=START)
    assert result.outcome == "updated"
    assert fake.calls == ["load", "page", "csv", ("url", XLSX_URL), ("append", "trading_calendar")]
    [(_, rows)] = fake.appended
    assert len(rows) == 36 + 136
    assert {r["accepted"] for r in rows} == {jc.ACCEPTED_AUTO}
    assert result.loaded.version == backfilled()
    [record] = [r for r in logs(capsys) if r["event"] == "calendar_updated"]
    assert record["history_rows"] == 136
    assert record["history_from"] == "2018-01-01" and record["history_until"] == "2025-12-31"
    assert record["added_closed"] == [] and record["removed_closed"] == []

    # 次の日の取り込み（--backfill-from なし）は過去年の行を引き継ぐので変化なし
    again = FakeBackfill(latest=result.loaded)
    assert sync(again).outcome == "unchanged"
    # 埋まっていれば何も足さない
    again = FakeBackfill(latest=result.loaded)
    assert sync(again, backfill_from=START).outcome == "unchanged"
    assert again.calls == ["load", "page"]
    assert ("INFO", "calendar_backfill_skipped") in events(logs(capsys))


def test_backfill_dry_run_lists_rows(capsys):
    fake = FakeBackfill(latest=loaded())
    assert sync(fake, backfill_from=date(2023, 1, 1), dry_run=True).outcome == "dry_run"
    assert fake.appended == []
    [record] = [r for r in logs(capsys) if r["event"] == "calendar_sync_dry_run"]
    assert record["history_rows"] == len(record["history_days"]) == 49
    assert "2023-08-11 祝日取引 山の日" in record["history_days"]
    assert "2023-01-09 休業 成人の日" in record["history_days"]


@pytest.mark.parametrize(
    "kwargs, stage",
    [
        ({"xlsx_error": CalendarFetchFailed("HTTP 404")}, "fetch_history"),
        ({"xlsx": b"<html>"}, "parse_history"),
        ({"xlsx": xlsx_bytes([(d, n, h) for d, n, h in read_rows(XLSX) if d != date(2024, 7, 15)])}, "parse_history"),
        (
            {"xlsx": xlsx_bytes([(d, n, d == date(2026, 11, 23) or h) for d, n, h in read_rows(XLSX)])},
            "parse_history",
        ),
        ({"page": FIXTURE.replace("確定）一覧</a>".encode(), b"</a>")}, "parse"),
        ({"csv_error": CalendarFetchFailed("timeout")}, "fetch_holidays"),
    ],
)
def test_backfill_failures_do_not_write(capsys, kwargs, stage):
    fake = FakeBackfill(latest=loaded(), **kwargs)
    assert sync(fake, backfill_from=START).outcome == "failed"
    assert fake.appended == []
    [record] = [r for r in logs(capsys) if r["event"] == "calendar_sync_failed"]
    assert record["stage"] == stage


def test_main_backfill(capsys):
    def run(argv, fake):
        return calendar_sync.main(argv, deps=fake.deps(), base_path=REPO_ROOT / "config" / "trading_calendar.toml")

    fake = FakeBackfill(latest=loaded())
    assert run(["--dry-run", "--backfill-from=2018-01-01"], fake) == 0
    assert fake.appended == []
    fake = FakeBackfill(latest=loaded())
    assert run(["--backfill-from", "2018-01-01"], fake) == 0
    assert len(fake.appended[0][1]) == 172
    for argv in (["--backfill-from=2018-10-01"], ["--backfill-from=x"], ["--print-toml", "--backfill-from=2018-01-01"]):
        assert run(argv, FakeBackfill()) == 2

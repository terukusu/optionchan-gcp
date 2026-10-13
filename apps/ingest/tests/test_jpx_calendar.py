"""取引カレンダーの自動取り込み（#63）の純粋関数。

JPX のページ・祝日 CSV の解析、版の組み立て、検証、BigQuery の行・TOML
"""

import tomllib
from dataclasses import replace
from datetime import UTC, date, datetime, time, timedelta

import pytest
from calgen import page_html, row, standard_rows
from conftest import FIXTURES, REPO_ROOT

from optionchan_ingest import bq_proto
from optionchan_ingest import jpx_calendar as jc
from optionchan_ingest.calendar import load_calendar

TODAY = date(2026, 10, 14)
TOML_PATH = REPO_ROOT / "config" / "trading_calendar.toml"
CAL = load_calendar(TOML_PATH)
FIXTURE = (FIXTURES / "holiday_trading.html").read_bytes()
CSV = (FIXTURES / "syukujitsu_2026_2027.csv").read_bytes()
HOLIDAYS = jc.parse_holidays_csv(CSV)
# 2028 年（合成。テストで翌年の予定の公表を作るため）
HOLIDAYS_2028 = {
    date(2028, 1, 1): "元日",
    date(2028, 1, 10): "成人の日",
    date(2028, 2, 11): "建国記念の日",
    date(2028, 2, 23): "天皇誕生日",
    date(2028, 3, 20): "春分の日",
    date(2028, 4, 29): "昭和の日",
    date(2028, 5, 3): "憲法記念日",
    date(2028, 5, 4): "みどりの日",
    date(2028, 5, 5): "こどもの日",
    date(2028, 7, 17): "海の日",
    date(2028, 8, 11): "山の日",
    date(2028, 9, 18): "敬老の日",
    date(2028, 9, 22): "秋分の日",
    date(2028, 10, 9): "スポーツの日",
    date(2028, 11, 3): "文化の日",
    date(2028, 11, 23): "勤労感謝の日",
}
BCP = (date(2026, 11, 23), date(2027, 9, 20))


def fixture_version():
    return jc.build_version(jc.parse_jpx_page(FIXTURE))


def toml_reference():
    return jc.reference_from_calendar(CAL.closed_dates, CAL.covered_until)


def std_page(years=(2026, 2027), confirmed_until=date(2027, 5, 31), closed=BCP, holidays=None, **kwargs):
    rows = standard_rows(holidays or HOLIDAYS, list(years), confirmed_until, closed)
    return jc.parse_jpx_page(page_html(rows, **kwargs))


def rules(problems):
    return sorted(p.rule for p in problems)


# ---- 実ページの抜粋（2026-10-14 取得） ----


def test_parse_fixture():
    page = jc.parse_jpx_page(FIXTURE)
    assert page.years == (2026, 2027)
    assert page.intro_years == (2026, 2027)
    assert len(page.rows) == 34
    assert page.sessions == {
        "day_open": time(8, 45),
        "day_close": time(15, 45),
        "night_open": time(17, 0),
        "night_close": time(6, 0),
    }
    by_day = {r.day: r for r in page.rows}
    bcp = by_day[date(2026, 11, 23)]
    assert (bcp.name, bcp.held, bcp.status) == ("勤労感謝の日", False, jc.CONFIRMED)
    assert "BCPテスト" in bcp.note
    assert by_day[date(2026, 9, 22)].name == "休日"  # 「休日（注5）」の注記の参照を除く
    assert by_day[date(2026, 9, 22)].note is None  # 実施有無の欄の注記だけを持つ
    assert by_day[date(2027, 7, 19)].status == jc.PLANNED


def test_fixture_matches_current_toml():
    """照合（#63）: 今のページから作った版の今日以降の休業日・covered_until は、手で写した TOML と一致する"""
    version = fixture_version()
    assert {d for d in version.closed_dates if d >= TODAY} == set(CAL.closed_dates)
    assert version.covered_until == CAL.covered_until == date(2027, 6, 30)
    assert version.listed_until == date(2027, 12, 31)
    # 1/1 は表に載らないので規則で足す（2026-01-01 木・2027-01-01 金）
    rule_rows = [r for r in version.rows if r.source == jc.SOURCE_RULE]
    assert [(r.calendar_date, r.is_trading_day, r.day_type) for r in rule_rows] == [
        (date(2026, 1, 1), False, jc.NATIONAL_HOLIDAY),
        (date(2027, 1, 1), False, jc.NATIONAL_HOLIDAY),
    ]
    by_date = version.by_date
    assert by_date[date(2026, 12, 31)].day_type == jc.YEAR_END
    assert by_date[date(2026, 1, 2)].day_type == jc.YEAR_END
    assert by_date[date(2026, 5, 6)].day_type == jc.NATIONAL_HOLIDAY  # 憲法記念日の振替休日


def test_fixture_passes_validation_against_toml():
    problems = jc.validate(fixture_version(), jc.parse_jpx_page(FIXTURE), HOLIDAYS, toml_reference(), TODAY)
    assert problems == []


def test_generated_page_matches_fixture():
    """テスト用の組み立て（calgen）が実ページと同じ休業日になる（以下の検査の前提）"""
    generated = jc.build_version(std_page())
    assert generated.closed_dates == fixture_version().closed_dates
    assert generated.covered_until == date(2027, 6, 30)


def test_render_toml_keeps_current_file():
    text = TOML_PATH.read_text(encoding="utf-8")
    rendered = jc.render_toml(text, fixture_version(), TODAY)
    data = tomllib.loads(rendered)
    assert set(data["closed_dates"]) == set(CAL.closed_dates)
    assert data["covered_until"] == CAL.covered_until
    assert data["sessions"] == tomllib.loads(text)["sessions"]
    # 名称と確定/予定のコメント
    assert "  2027-09-20, # 敬老の日（予定。祝日取引を実施しない）" in rendered
    assert "  2027-01-01, # 元日（確定）" in rendered


def test_render_toml_after_update(tmp_path):
    version = jc.build_version(std_page(confirmed_until=date(2027, 12, 31)))
    rendered = jc.render_toml(TOML_PATH.read_text(encoding="utf-8"), version, date(2026, 12, 20))
    path = tmp_path / "cal.toml"
    path.write_text(rendered, encoding="utf-8")
    cal = load_calendar(path)  # 読み込みのキーの検査も通る
    assert cal.covered_until == date(2027, 12, 31)
    assert date(2026, 11, 23) not in cal.closed_dates  # 過去日は書かない


# ---- TOML の差分（#66） ----


def test_toml_change_none_when_same():
    change = jc.toml_change(TOML_PATH.read_text(encoding="utf-8"), fixture_version(), TODAY)
    assert not change.changed and not change.toml_newer
    assert change.added == () and change.removed == ()


def test_toml_change_ignores_past_dates():
    # 2026-11-23 が過ぎると生成した TOML からは消えるが、差分には数えない（祝日が過ぎるたびに PR を出さない）
    text = TOML_PATH.read_text(encoding="utf-8")
    change = jc.toml_change(text, fixture_version(), date(2026, 11, 24))
    assert not change.changed
    assert "2026-11-23" not in change.text


def test_toml_change_added_removed_and_covered_until():
    text = TOML_PATH.read_text(encoding="utf-8")
    # 2027-09-20 が実施するに変わり、2027-07-19（海の日）が実施しないに変わり、下半期が確定
    version = jc.build_version(
        std_page(confirmed_until=date(2027, 12, 31), closed=(date(2026, 11, 23), date(2027, 7, 19)))
    )
    change = jc.toml_change(text, version, TODAY)
    assert change.changed and not change.toml_newer
    assert [d for d, _ in change.added] == [date(2027, 7, 19)]
    assert change.added[0][1].startswith("海の日（確定")
    assert change.removed == ((date(2027, 9, 20), "敬老の日（BCP テストのため実施しない。予定）"),)
    assert change.covered_until == (date(2027, 6, 30), date(2027, 12, 31))
    assert set(tomllib.loads(change.text)["closed_dates"]) == {d for d in version.closed_dates if d >= TODAY}

    title, body = jc.toml_pr_text(change, datetime(2026, 12, 1, 21, 40, tzinfo=UTC), TODAY)
    assert title.startswith("取引カレンダーの TOML を BigQuery の最新版に合わせる")
    assert title.endswith("（休業日 +1、休業日 -1、covered_until 2027-06-30 → 2027-12-31）")
    assert "- covered_until: 2027-06-30 → 2027-12-31\n" in body
    assert "  - 2027-07-19（海の日（確定" in body
    assert "  - 2027-09-20（敬老の日（BCP テストのため実施しない。予定））" in body
    assert "version_at 2026-12-01T21:40:00+00:00" in body


def test_toml_change_covered_until_only():
    text = TOML_PATH.read_text(encoding="utf-8").replace("covered_until = 2027-06-30", "covered_until = 2027-05-31")
    change = jc.toml_change(text, fixture_version(), TODAY)
    assert change.changed and change.added == () and change.removed == ()
    title, body = jc.toml_pr_text(change, VERSION_AT_FOR_TOML, TODAY)
    assert title.endswith("（covered_until 2027-05-31 → 2027-06-30）")
    assert "  - なし" in body


def test_toml_change_toml_newer():
    # 手で TOML の covered_until を延ばした（緊急回避）ときは版に合わせない
    text = TOML_PATH.read_text(encoding="utf-8").replace("covered_until = 2027-06-30", "covered_until = 2027-12-31")
    change = jc.toml_change(text, fixture_version(), TODAY)
    assert change.toml_newer


VERSION_AT_FOR_TOML = datetime(2026, 10, 14, 21, 40, tzinfo=UTC)


# ---- 祝日 CSV ----


def test_parse_holidays_csv_cp932():
    assert len(HOLIDAYS) == 35
    assert HOLIDAYS[date(2027, 3, 22)] == "休日"
    assert HOLIDAYS[date(2026, 11, 23)] == "勤労感謝の日"


def test_parse_holidays_csv_utf8():
    data = CSV.decode("cp932").encode("utf-8")
    assert jc.parse_holidays_csv(data) == HOLIDAYS


@pytest.mark.parametrize(
    "data",
    [
        b"",
        "date,name\r\n2026/1/1,元日\r\n".encode("cp932"),
        "国民の祝日・休日月日,国民の祝日・休日名称\r\n2026-01-01,元日\r\n".encode("cp932"),
        "国民の祝日・休日月日,国民の祝日・休日名称\r\n2026/1/1,元日\r\n".encode("cp932"),  # 行が少ない
        b"\xff\xfe\x00",
    ],
)
def test_parse_holidays_csv_errors(data):
    with pytest.raises(jc.CalendarParseError):
        jc.parse_holidays_csv(data)


# ---- ページの形の誤り（CalendarParseError。前のカレンダーを保つ） ----


def _replace(old, new, count=1):
    text = FIXTURE.decode("utf-8")
    assert text.count(old) >= count, old
    return text.replace(old, new, count).encode("utf-8")


@pytest.mark.parametrize(
    "data",
    [
        pytest.param(b"", id="empty"),
        pytest.param(b"\xff\xfe<html>", id="not_utf8"),
        pytest.param(_replace("祝日取引実施日</span></h2>", "祝日取引の実施日</span></h2>"), id="heading"),
        pytest.param(_replace("<th>実施有無</th>", "<th>実施</th>"), id="header"),
        pytest.param(_replace('"a-center a-middle">金</td>', '"a-center a-middle">木</td>'), id="weekday"),
        pytest.param(_replace(">実施する</td>", ">実施予定</td>"), id="held"),
        pytest.param(_replace(">確定</td>", ">確認中</td>"), id="status"),
        pytest.param(_replace(">1月12日</td>", ">1月32日</td>"), id="bad_date"),
        pytest.param(_replace(">1月12日</td>", ">1/12</td>"), id="date_format"),
        pytest.param(_replace(">2026年<br> </td>", ">令和8年</td>"), id="year"),
        pytest.param(_replace('<td class="a-center a-middle">確定</td>', ""), id="columns"),
    ],
)
def test_parse_errors(data):
    with pytest.raises(jc.CalendarParseError):
        jc.build_version(jc.parse_jpx_page(data))


def test_parse_error_without_table_rows():
    rows_html = page_html([row(date(2026, 1, 12), "成人の日")])
    text = rows_html.decode("utf-8")
    start, end = text.index("  <tr>\n    <td"), text.index("</table>")
    with pytest.raises(jc.CalendarParseError):
        jc.parse_jpx_page((text[:start] + text[end:]).encode("utf-8"))


def test_no_confirmed_rows():
    with pytest.raises(jc.CalendarParseError):
        jc.build_version(std_page(confirmed_until=date(2025, 12, 31)))


def test_sessions_unreadable_is_none():
    page = jc.parse_jpx_page(page_html(standard_rows(HOLIDAYS, [2026], date(2026, 12, 31)), sessions=("終日", "なし")))
    assert page.sessions is None
    assert jc.sessions_mismatch(None, CAL_SESSIONS) is not None


CAL_SESSIONS = {
    "day_open": time(8, 45),
    "day_close": time(15, 45),
    "night_open": time(17, 0),
    "night_close": time(6, 0),
}


def test_sessions_mismatch():
    page = jc.parse_jpx_page(
        page_html(standard_rows(HOLIDAYS, [2026], date(2026, 12, 31)), sessions=("8:45∼15:30", "17:00∼翌6:00"))
    )
    assert jc.sessions_mismatch(page.sessions, CAL_SESSIONS) == "day_close: page 15:30 / config 15:45"
    assert jc.sessions_mismatch(jc.parse_jpx_page(FIXTURE).sessions, CAL_SESSIONS) is None


# ---- 検証（形・祝日・年末年始。上書きできない） ----


def _validate(rows, today=TODAY, holidays=None, reference=None, **kwargs):
    page = jc.parse_jpx_page(page_html(rows, **kwargs))
    return jc.validate(jc.build_version(page), page, holidays or HOLIDAYS, reference, today)


def _std_rows(**kwargs):
    kwargs.setdefault("closed", BCP)
    return standard_rows(HOLIDAYS, [2026, 2027], date(2027, 5, 31), **kwargs)


def _without(rows, day):
    return [r for r in rows if r[0] != day]


def test_validate_standard_page():
    assert _validate(_std_rows()) == []


def test_years_mismatch():
    assert rules(_validate(_std_rows(), intro_years=[2027, 2028])) == ["years_mismatch"]


def test_current_year_missing():
    rows = [r for r in _std_rows() if r[0].year == 2027]
    assert "current_year_missing" in rules(_validate(rows, intro_years=[2027]))


def test_row_count():
    rows = [r for r in _std_rows() if r[0].year == 2027 or r[0].month >= 11]
    problems = rules(_validate(rows))
    assert "row_count" in problems
    assert set(problems) == {"row_count", "holiday_missing", "year_end_missing"}


def test_status_order():
    rows = _std_rows()
    rows = [r if r[0] != date(2027, 2, 11) else row(r[0], r[1], confirmed=False) for r in rows]
    assert rules(_validate(rows)) == ["status_order"]


def test_weekend_row():
    rows = _std_rows() + [row(date(2026, 1, 3), "年始休業日", held=False)]  # 土曜
    assert "weekend" in rules(_validate(rows))


@pytest.mark.parametrize("day", [date(2026, 12, 31), date(2027, 12, 31), date(2026, 1, 2)])
def test_year_end_missing(day):
    assert rules(_validate(_without(_std_rows(), day))) == ["year_end_missing"]


def test_year_end_held():
    rows = [r if r[0] != date(2026, 12, 31) else row(r[0], r[1], held=True) for r in _std_rows()]
    assert rules(_validate(rows)) == ["year_end_held"]


def test_jan1_held():
    rows = _std_rows() + [row(date(2027, 1, 1), "元日", held=True)]
    assert "jan1_held" in rules(_validate(rows))


def test_not_holiday():
    rows = _std_rows() + [row(date(2026, 11, 24), "休日", held=False)]
    assert rules(_validate(rows)) == ["not_holiday"]


def test_holiday_missing():
    assert rules(_validate(_without(_std_rows(), date(2026, 11, 3)))) == ["holiday_missing"]


def test_year_without_csv_planned_is_accepted():
    """祝日 CSV にまだ無い年は「予定」なら照合を飛ばして受け入れる（WARNING は calendar_sync）"""
    rows = _std_rows() + standard_rows(HOLIDAYS_2028, [2028], date(2027, 12, 31))
    assert _validate(rows, today=date(2027, 2, 10)) == []


def test_year_without_csv_confirmed_is_overridable():
    rows = standard_rows(HOLIDAYS, [2026, 2027], date(2027, 12, 31), BCP)
    rows += standard_rows(HOLIDAYS_2028, [2028], date(2028, 6, 30))
    problems = _validate(rows, today=date(2027, 2, 10))
    assert rules(problems) == ["unverified_confirmed"]
    assert problems[0].overridable


# ---- 前の版との差分（--accept で上書きできる） ----


def _ref(version):
    return jc.reference_from_version(version)


def test_unchanged_has_no_problems():
    version = fixture_version()
    assert jc.validate(version, jc.parse_jpx_page(FIXTURE), HOLIDAYS, _ref(version), TODAY) == []


def test_past_changed():
    prev = fixture_version()
    rows = [r if r[0] != date(2026, 3, 20) else row(r[0], r[1], held=False, note=6) for r in _std_rows()]
    problems = _validate(rows, reference=_ref(prev))
    assert rules(problems) == ["past_changed"]
    assert all(p.overridable for p in problems)


def test_past_not_checked_against_toml():
    """TOML は過去日を書かないので、初回（版が無い）は過去日の違いを見ない"""
    assert _validate(_std_rows(), reference=toml_reference()) == []


def test_covered_until_regressed():
    prev = replace(fixture_version(), covered_until=date(2027, 12, 31))
    assert rules(_validate(_std_rows(), reference=_ref(prev))) == ["covered_until_regressed"]


def test_listed_until_regressed():
    prev = jc.build_version(std_page(years=(2026, 2027, 2028), holidays={**HOLIDAYS, **HOLIDAYS_2028}))
    problems = _validate(_std_rows(), reference=_ref(prev))
    assert "listed_until_regressed" in rules(problems)


def test_confirmed_changed():
    prev = fixture_version()
    rows = [r if r[0] != date(2026, 11, 3) else row(r[0], r[1], confirmed=False) for r in _std_rows()]
    assert "confirmed_changed" in rules(_validate(rows, reference=_ref(prev)))


def test_too_many_changes():
    prev = fixture_version()
    closed = BCP + (date(2027, 7, 19), date(2027, 8, 11), date(2027, 9, 23))
    assert _validate(_std_rows(closed=closed), reference=_ref(prev)) == []  # 3 件まで
    closed += (date(2027, 10, 11),)
    assert rules(_validate(_std_rows(closed=closed), reference=_ref(prev))) == ["too_many_changes"]


def test_toml_reference_counts_changes():
    closed = (date(2027, 7, 19), date(2027, 8, 11))  # BCP の 2 日が消え、2 日増える = 4 件
    assert rules(_validate(_std_rows(closed=closed), reference=toml_reference())) == ["too_many_changes"]


def test_closes_soon_rejects_new_closed_day():
    """14 日以内に新しい休業日（誤りなら取引日を取りこぼす）は受け入れない"""
    assert jc.CLOSE_GUARD_DAYS == 14
    prev = fixture_version()
    closed = BCP + (date(2026, 11, 3),)
    problems = _validate(_std_rows(closed=closed), today=date(2026, 10, 20), reference=_ref(prev))
    assert rules(problems) == ["closes_soon", "confirmed_changed"]
    # 15 日前なら closes_soon にならない（確定の日の変化は別の問題として残る）
    problems = _validate(_std_rows(closed=closed), today=date(2026, 10, 19), reference=_ref(prev))
    assert rules(problems) == ["confirmed_changed"]
    # 予定の日なら 15 日前は受け入れる
    closed = BCP + (date(2027, 7, 19),)
    assert _validate(_std_rows(closed=closed), today=date(2027, 7, 4), reference=_ref(prev)) == []
    problems = _validate(_std_rows(closed=closed), today=date(2027, 7, 5), reference=_ref(prev))
    assert rules(problems) == ["closes_soon"]


def test_new_trading_day_soon_is_accepted():
    """休業日が取引日に変わる（取りこぼさない側）は 14 日以内でも受け入れる（確定の変化は別の問題）"""
    prev_rows = _std_rows(closed=BCP + (date(2027, 9, 23),))
    prev = jc.build_version(jc.parse_jpx_page(page_html(prev_rows)))
    rows = _std_rows()
    assert _validate(rows, today=date(2027, 9, 20), reference=_ref(prev)) == []


def test_next_year_publication_is_not_counted_as_changes():
    """2 月の翌年の予定の公表: 前の版の掲載範囲より後の休業日は変更に数えず、過去年の行は引き継ぐ"""
    prev = fixture_version()
    holidays = {**HOLIDAYS, **HOLIDAYS_2028}
    rows = standard_rows(holidays, [2027, 2028], date(2027, 5, 31), closed=(date(2027, 9, 20),))
    page = jc.parse_jpx_page(page_html(rows))
    version = jc.build_version(page, prev)
    today = date(2027, 2, 10)
    assert jc.validate(version, page, holidays, _ref(prev), today) == []
    assert version.listed_until == date(2028, 12, 31)
    assert date(2026, 11, 23) in version.closed_dates  # 2026 年の行を引き継ぐ
    assert version.by_date[date(2026, 11, 23)] == prev.by_date[date(2026, 11, 23)]
    assert date(2028, 12, 29) not in version.closed_dates
    # 2028 年は 1/1・1/2・12/31 が土日で、平日の休業日は増えない（1/3（月）は行があり、合成の表では実施する）
    assert jc.closed_diff(version, _ref(prev), today) == {"added_closed": [], "removed_closed": []}
    assert date(2028, 1, 3) in version.by_date


def test_december_publication_extends_covered_until():
    prev = fixture_version()
    page = std_page(confirmed_until=date(2027, 12, 31))
    version = jc.build_version(page, prev)
    assert version.covered_until == date(2027, 12, 31)
    assert jc.validate(version, page, HOLIDAYS, _ref(prev), date(2026, 12, 20)) == []
    assert version.content_hash != prev.content_hash


def test_half_year_end():
    assert jc.half_year_end(date(2027, 5, 5)) == date(2027, 6, 30)
    assert jc.half_year_end(date(2027, 6, 30)) == date(2027, 6, 30)
    assert jc.half_year_end(date(2027, 7, 19)) == date(2027, 12, 31)


# ---- BigQuery の行 ----

VERSION_AT = datetime(2026, 10, 13, 21, 40, tzinfo=UTC)


def test_rows_round_trip():
    version = fixture_version()
    rows = jc.to_rows(version, VERSION_AT, jc.ACCEPTED_AUTO)
    assert len(rows) == len(version.rows) == 36
    assert {r["content_hash"] for r in rows} == {version.content_hash}
    loaded = jc.from_rows(rows)
    assert loaded.version == version
    assert loaded.version.content_hash == version.content_hash
    assert (loaded.version_at, loaded.accepted) == (VERSION_AT, jc.ACCEPTED_AUTO)


def test_rows_match_schema_and_encode():
    schema = bq_proto.load_schema(REPO_ROOT / "schema" / "tokyo", "trading_calendar")
    rows = jc.to_rows(fixture_version(), VERSION_AT, jc.ACCEPTED_OVERRIDE)
    assert set(rows[0]) == {c["name"] for c in schema}
    descriptor = bq_proto.schema_to_descriptor(schema, "trading_calendar")
    proto = bq_proto.rows_to_proto(descriptor, schema, rows)
    back = bq_proto.proto_to_rows(descriptor, schema, proto)
    assert back[0]["calendar_date"] == rows[0]["calendar_date"]
    assert back[0]["version_at"] == VERSION_AT
    assert [r["is_trading_day"] for r in back] == [r["is_trading_day"] for r in rows]


def test_content_hash_ignores_version_at():
    a = fixture_version()
    b = jc.build_version(jc.parse_jpx_page(FIXTURE))
    assert a.content_hash == b.content_hash


def test_from_rows_empty():
    assert jc.from_rows([]) is None


def _bq_rows():
    return jc.to_rows(fixture_version(), VERSION_AT, "auto")


@pytest.mark.parametrize(
    "change",
    [
        lambda rows: rows[0].update(version_at=datetime(2026, 1, 1, tzinfo=UTC)),
        lambda rows: rows[0].update(covered_until=date(2027, 12, 31)),
        lambda rows: rows[0].update(calendar_date=date(2026, 1, 3)),  # 土曜
        lambda rows: rows[0].update(status="maybe"),
        lambda rows: rows[0].update(day_type="other"),
        lambda rows: rows[0].update(is_trading_day="false"),
        lambda rows: rows[1].update(calendar_date=rows[0]["calendar_date"]),
        lambda rows: [r.update(accepted="manual") for r in rows],
        lambda rows: [r.update(covered_until=date(2028, 6, 30)) for r in rows],
        # content_hash の照合: 行の欠け・同じ version_at の別の版の行の混入・名称の書き換え
        lambda rows: rows.pop(),
        lambda rows: rows.append({**rows[0], "calendar_date": date(2026, 11, 24), "is_trading_day": False}),
        lambda rows: rows[3].update(name="x"),
        lambda rows: [r.update(content_hash="0" * 64) for r in rows],
    ],
)
def test_from_rows_rejects_bad_versions(change):
    rows = _bq_rows()
    change(rows)
    with pytest.raises(jc.CalendarDataError):
        jc.from_rows(rows)


# ---- 12 月の表の切り替え（翌年・翌々年） ----


def test_december_accepts_next_two_years():
    """12 月は表が翌年・翌々年に切り替わっていてもよい（今年の残りは前の版から引き継ぐ）"""
    holidays = {**HOLIDAYS, **HOLIDAYS_2028}
    rows = standard_rows(holidays, [2027, 2028], date(2027, 12, 31), closed=(date(2027, 9, 20),))
    page = jc.parse_jpx_page(page_html(rows))
    prev = fixture_version()
    version = jc.build_version(page, prev)
    assert jc.validate(version, page, holidays, _ref(prev), date(2026, 12, 20)) == []
    assert date(2026, 12, 31) in version.closed_dates  # 引き継ぎ
    # 12 月以外は今年が要る
    assert "current_year_missing" in rules(jc.validate(version, page, holidays, _ref(prev), date(2026, 11, 30)))


# ---- ingest 側の検査 ----


def _loaded(version, accepted=jc.ACCEPTED_AUTO):
    return jc.LoadedVersion(VERSION_AT, version, accepted)


def test_ingest_sanity_accepts_current():
    assert jc.ingest_sanity(_loaded(fixture_version()), CAL.closed_dates, TODAY) == []


def test_ingest_sanity_rejects_new_closed_day_within_14_days():
    version = jc.build_version(std_page(closed=BCP + (date(2026, 11, 3),)))
    today = date(2026, 10, 20)  # 11/3 の 14 日前
    problems = jc.ingest_sanity(_loaded(version), CAL.closed_dates, today)
    assert len(problems) == 1 and problems[0].startswith("closes_soon")
    # 15 日前は見ない。手動で受け入れた版（override）は確認済みなので見ない
    assert jc.ingest_sanity(_loaded(version), CAL.closed_dates, date(2026, 10, 19)) == []
    assert jc.ingest_sanity(_loaded(version, jc.ACCEPTED_OVERRIDE), CAL.closed_dates, today) == []
    # 今使っている値に既にある休業日は増えたことにならない
    assert jc.ingest_sanity(_loaded(version), CAL.closed_dates | {date(2026, 11, 3)}, today) == []


def test_ingest_sanity_rejects_too_many_closed_days():
    version = fixture_version()
    extra = tuple(
        jc.DayRow(d, False, jc.PLANNED, jc.NATIONAL_HOLIDAY, "x", None, jc.SOURCE_JPX)
        for d in (date(2027, 3, 1) + timedelta(days=i) for i in range(60))
        if d.weekday() < 5
    )
    big = replace(version, rows=tuple(sorted(version.rows + extra, key=lambda r: r.calendar_date)))
    for accepted in (jc.ACCEPTED_AUTO, jc.ACCEPTED_OVERRIDE):
        problems = jc.ingest_sanity(_loaded(big, accepted), big.closed_dates, TODAY)
        assert [p.split(":")[0] for p in problems] == ["too_many_closed"]

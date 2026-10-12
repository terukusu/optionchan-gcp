import json
from dataclasses import asdict
from datetime import date, datetime, timedelta
from unittest import mock

import pytest
from conftest import FIXTURES, read_fixture
from htmlgen import page_html

from optionchan_ingest import parser
from optionchan_ingest.parser import (
    CALL,
    JST,
    PUT,
    JpxUnavailable,
    ParseError,
    build_price_time,
    jst,
    parse_page,
    parse_updated_at,
)

LEGACY = FIXTURES / "legacy"


def legacy_rows(name):
    with open(LEGACY / name, encoding="utf-8") as f:
        return [json.loads(line) for line in f]


def to_legacy_json(value):
    """新パーサーの値を旧系の JSON（to_json）と同じ表現にする"""
    if isinstance(value, datetime):
        return value.astimezone(JST).replace(tzinfo=None).isoformat()  # DATETIME（JST・オフセットなし）
    if isinstance(value, date):
        return value.isoformat()
    return value


def new_rows(page):
    rows = [asdict(page.spot), asdict(page.future)] + [asdict(o) for o in page.options]
    out = []
    for row in rows:
        converted = {k: to_legacy_json(v) for k, v in row.items()}
        converted["created_at"] = page.updated_at.isoformat()
        out.append(converted)
    return out


def assert_common_columns_equal(actual, expected, ignore=()):
    assert len(actual) == len(expected)
    for i, (a, e) in enumerate(zip(actual, expected, strict=True)):
        for key, value in e.items():
            if key in ignore:
                continue
            assert a[key] == value, (i, key)
            assert type(a[key]) is type(value), (i, key)


# --- 旧パーサーとの一致（AC 3） ---


@pytest.mark.parametrize("name", ["page1", "page2", "yearend"])
def test_matches_legacy_golden(name):
    page = parse_page(read_fixture(f"{name}.html"))
    assert_common_columns_equal(new_rows(page), legacy_rows(f"{name}.jsonl"))


def test_sq_month_contract_month_fixed():
    """旧パーサーは SQ 月の 1 日〜限月交代に先物の限月を翌年にする（2025-12 の実データでも発生）。新は表示の年"""
    page = parse_page(read_fixture("sq_month.html"))
    golden = legacy_rows("sq_month.jsonl")
    assert golden[1]["contract_month"] == "2027-12-01"  # 旧の誤り
    assert page.future.contract_month == date(2026, 12, 1)
    assert_common_columns_equal(new_rows(page), golden, ignore=("contract_month",))


def test_yearend_quotation_date_differs_from_legacy_run_later():
    """旧パーサーを 2029 年に動かすと清算日が 2028-12-30 になる。新は更新時刻から決めるので 2026-12-30"""
    page = parse_page(read_fixture("yearend.html"))
    golden = legacy_rows("yearend_now2029.jsonl")
    assert {r["quotation_date"] for r in golden[2:]} == {"2028-12-30"}
    assert {o.quotation_date for o in page.options} == {date(2026, 12, 30)}
    assert_common_columns_equal(new_rows(page), golden, ignore=("quotation_date",))


# --- 清算日の年は更新時刻から（AC 4） ---


def test_quotation_year_from_updated_at_not_now():
    html = read_fixture("yearend.html")

    class FarFuture(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2031, 6, 1, tzinfo=tz)

    with mock.patch.object(parser, "datetime", FarFuture):
        page = parse_page(html)
    assert page.updated_at == jst(2027, 1, 4, 8, 46)
    assert page.quotation_date == date(2026, 12, 30)
    assert all(o.quotation_date == date(2026, 12, 30) for o in page.options)


def test_quotation_same_day_is_kept():
    page = parse_page(page_html(updated_at="2026/10/09 16:00", quotation="10/09"))
    assert page.quotation_date == date(2026, 10, 9)


def test_quotation_future_month_day_goes_to_previous_year():
    page = parse_page(page_html(updated_at="2026/01/05 08:46", quotation="12/30"))
    assert page.quotation_date == date(2025, 12, 30)


# --- 空ページ・構造違い（AC 5） ---


@pytest.mark.parametrize("html", [read_fixture("empty.html"), read_fixture("empty_rows.html"), "", b"  \n"])
def test_empty_page_is_unavailable(html):
    with pytest.raises(JpxUnavailable):
        parse_page(html)


def test_updated_at_of_empty_page_is_unavailable():
    with pytest.raises(JpxUnavailable):
        parse_updated_at(read_fixture("empty.html"))


@pytest.mark.parametrize(
    "html",
    [
        pytest.param(read_fixture("mismatch.html"), id="greek_rows_mismatch"),
        pytest.param(page_html(last_trading_day="11/12"), id="bad_last_trading_day"),
        pytest.param(page_html(future_label="期近"), id="no_contract_month"),
        pytest.param(page_html(updated_at="2026-10-12 10:46"), id="bad_updated_at"),
        pytest.param(page_html(quotation="-"), id="no_quotation_date"),
        pytest.param(page_html().replace("日経225先物", "なにか"), id="no_future_row"),
        pytest.param(page_html().replace("日経平均株価", "なにか"), id="no_spot_row"),
        pytest.param(page_html(rows={40000: (["abc"] * 8, ["abc"] * 8)}), id="bad_cell"),
    ],
)
def test_structure_change_is_parse_error(html):
    with pytest.raises(ParseError):
        parse_page(html)


# --- 値の解釈 ---


def test_page1_values():
    page = parse_page(read_fixture("page1.html"))
    assert page.updated_at == jst(2026, 10, 12, 10, 46)
    assert page.updated_at.utcoffset() == timedelta(hours=9)
    assert page.spot.price == 39876.54
    assert page.spot.price_time == jst(2026, 10, 9, 15, 45)
    assert page.spot.price_time_raw == "10/09 15:45"
    assert page.future.price == 40000 - 130
    assert isinstance(page.future.price, int)
    assert page.future.price_time_raw == "10/12 10:45"
    assert page.future.contract_month == date(2026, 12, 1)
    assert page.last_trading_day == date(2026, 11, 12)

    calls = [o for o in page.options if o.type == CALL]
    puts = [o for o in page.options if o.type == PUT]
    # 全 call → 全 put の順、行使価格は表示順
    assert [o.type for o in page.options] == [CALL] * 3 + [PUT] * 3
    assert [o.target_price for o in calls] == [39500, 40000, 40500]
    assert [o.is_atm for o in calls] == [False, True, False]
    assert [o.is_atm for o in puts] == [False, True, False]
    assert calls[0].price == 1000
    assert calls[0].price_time == jst(2026, 10, 12, 10, 40)
    assert calls[0].price_time_raw == "10/12 10:40"
    assert (calls[0].ask, calls[0].ask_volume, calls[0].bid, calls[0].bid_volume) == (1020, 5, 1000, 3)
    assert (calls[0].ask_iv, calls[0].bid_iv) == (25.0, 24.0)
    assert puts[0].delta == -0.5


def test_missing_values_are_none_not_zero():
    page = parse_page(read_fixture("page1.html"))
    dash = [o for o in page.options if o.target_price == 40500]
    assert len(dash) == 2
    for o in dash:
        for name in (
            "price",
            "price_time",
            "price_time_raw",
            "diff",
            "diff_rate",
            "iv",
            "bid",
            "bid_volume",
            "bid_iv",
            "ask",
            "ask_volume",
            "ask_iv",
            "volume",
            "positions",
            "quotation",
            "delta",
            "gamma",
            "theta",
            "vega",
        ):
            assert getattr(o, name) is None, name
    page2 = parse_page(read_fixture("page2.html"))
    assert (page2.spot.price, page2.spot.price_time, page2.spot.price_time_raw, page2.spot.diff) == (None,) * 4


def test_bytes_input():
    page = parse_page(read_fixture("page1.html").encode("utf-8"))
    assert page.updated_at == jst(2026, 10, 12, 10, 46)


# --- 約定日時の年の補完（#2 と同じ規則） ---


@pytest.mark.parametrize(
    "updated_at, display, expected",
    [
        (jst(2026, 10, 12, 10, 46), (10, 12, 10, 40), jst(2026, 10, 12, 10, 40)),
        (jst(2026, 10, 13, 0, 30), (10, 12, 23, 59), jst(2026, 10, 12, 23, 59)),
        (jst(2027, 1, 4, 8, 46), (12, 30, 15, 45), jst(2026, 12, 30, 15, 45)),
        (jst(2026, 12, 31, 23, 59), (1, 1, 0, 0), jst(2027, 1, 1, 0, 0)),
        (jst(2028, 3, 1, 9, 0), (2, 29, 15, 45), jst(2028, 2, 29, 15, 45)),
    ],
)
def test_build_price_time(updated_at, display, expected):
    actual = build_price_time(updated_at, *display)
    assert actual == expected
    assert actual.utcoffset() == timedelta(hours=9)


def test_build_price_time_invalid():
    with pytest.raises(ValueError):
        build_price_time(jst(2026, 10, 12), 2, 30, 10, 0)


def test_fixtures_match_generator():
    """tests/fixtures/*.html は fixtures/generate.py の出力のまま（手で直したら golden と食い違う）"""
    import importlib.util

    spec = importlib.util.spec_from_file_location("fixture_generate", FIXTURES / "generate.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    for name, html in module.FIXTURES.items():
        assert read_fixture(name) == html, name


@pytest.mark.parametrize(
    "label, expected",
    [
        ("26年12月限", date(2026, 12, 1)),
        ("2026年12月限", date(2026, 12, 1)),
        ("2027年3月限", date(2027, 3, 1)),
        ("27年03月限", date(2027, 3, 1)),
    ],
)
def test_future_contract_month_two_or_four_digit_year(label, expected):
    page = parse_page(page_html(future_label=label))
    assert page.future.contract_month == expected


@pytest.mark.parametrize("label", ["99年12月限", "2031年12月限", "2024年12月限", "026年12月限", "26年13月限"])
def test_future_contract_month_out_of_range_is_parse_error(label):
    # 更新時刻（2026-10-12）の年 −1〜+3 の範囲外・桁数違い・不正な月は形式変更として扱う
    with pytest.raises(ParseError):
        parse_page(page_html(future_label=label))

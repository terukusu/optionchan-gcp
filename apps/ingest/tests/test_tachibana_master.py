from datetime import date

import pytest
from tachibana_fakes import fixture_json

from optionchan_ingest.parser import CALL, PUT
from optionchan_ingest.sources.tachibana import master as tm

TODAY = date(2031, 3, 3)


def option_master():
    return fixture_json("master_op.json")["aCLMStkIssueMstOp"]


def future_master():
    return fixture_json("master_sak.json")["aCLMStkIssueMstSak"]


def by_code(items):
    return {i["sIssueCode"]: i for i in items}


def by_name(items):
    return {i["sIssueName"]: i for i in items}


def test_select_options_nearest_two_months_of_target_product():
    options = tm.select_options(option_master(), TODAY)
    source = by_code(option_master())
    assert {o.last_trading_day for o in options} == {date(2031, 3, 13), date(2031, 4, 10)}
    for o in options:
        item = source[o.code]
        assert item["sSakOpSyouhin"] == "18"
        assert item["sGensisanCode"].strip() == ""  # オプションの原資産コードは空白（資料の例）でも取る
        assert isinstance(o.target_price, int)
        assert o.type in (CALL, PUT)
        assert o.type == (CALL if item["sPutCall"] == "7" else PUT)
        assert o.contract_month == date(int(item["sGengetu"][:4]), int(item["sGengetu"][4:]), 1)
    assert len(options) == 2 * 2 * 5
    calls = [o for o in options if o.type == CALL]
    puts = [o for o in options if o.type == PUT]
    assert len(calls) == len(puts)
    # 並び: 取引最終日 → コール（行使価格の昇順）→ プット（行使価格の昇順）
    first = options[:10]
    assert [(o.type, o.target_price) for o in first] == [(CALL, s) for s in range(1000, 1500, 100)] + [
        (PUT, s) for s in range(1000, 1500, 100)
    ]
    assert all(o.last_trading_day == date(2031, 3, 13) for o in first)


def test_select_options_excludes_other_products_underlyings_weeklies_minis_and_past_months():
    codes = {o.code for o in tm.select_options(option_master(), TODAY)}
    excluded = {
        "架空オプション別商品",  # 商品区分 05
        "架空オプション別原資産",  # 原資産コード 999
        "架空オプション週次",  # 最終売買日が第 2 金曜日の前の週に無い
        "架空オプションミニC1200",  # 名前に「ミニ」
        "架空オプション過去",  # 最終売買日が today より前
    }
    names = {i["sIssueName"] for i in option_master()}
    assert excluded <= names
    for item in option_master():
        if item["sIssueName"] in excluded:
            assert item["sIssueCode"] not in codes
    assert tm.OPTION_PRODUCTS == frozenset({"18"})


def test_last_trading_day_today_is_kept():
    options = tm.select_options(option_master(), date(2031, 3, 13))
    assert {o.last_trading_day for o in options} == {date(2031, 3, 13), date(2031, 4, 10)}
    options = tm.select_options(option_master(), date(2031, 3, 14))
    assert {o.last_trading_day for o in options} == {date(2031, 4, 10), date(2031, 5, 8)}


def test_strike_range_includes_both_ends():
    options = tm.select_options(option_master(), TODAY, strike_range=(1100, 1300))
    assert {o.target_price for o in options} == {1100, 1200, 1300}
    assert len(options) == 3 * 2 * 2


def test_months_argument():
    options = tm.select_options(option_master(), TODAY, months=3)
    assert len({o.last_trading_day for o in options}) == 3
    with pytest.raises(ValueError):
        tm.select_options(option_master(), TODAY, months=0)
    with pytest.raises(ValueError):
        tm.select_options(option_master(), TODAY, strike_range=(1300, 1100))


@pytest.mark.parametrize(
    ("underlying", "selected"),
    [(" ", True), ("", True), ("101", True), ("999", False)],
)
def test_option_underlying_blank_or_nikkei225_is_kept(underlying, selected):
    # fixture の原資産が空白の行（別原資産の 999 の行は除く）の原資産コードを置き換える
    items = [{**i, "sGensisanCode": underlying} for i in option_master() if i["sGensisanCode"].strip() == ""]
    if selected:
        options = tm.select_options(items, TODAY)
        assert len(options) == 2 * 2 * 5
    else:
        with pytest.raises(tm.MasterError, match="option months"):
            tm.select_options(items, TODAY)


@pytest.mark.parametrize(
    ("name", "name_en", "selected"),
    [
        ("架空オプションC1200", "TEST OPTION", True),
        ("架空オプションミニC1200", "TEST OPTION", False),
        ("架空オプションC1200", "TEST OPTION MINI", False),
        ("架空オプションC1200", "test option mini", False),
    ],
)
def test_option_name_with_mini_is_excluded(name, name_en, selected):
    items = option_master()
    target = next(i for i in items if i["sIssueName"] == "架空オプションC1200" and i["sLastBaibaiDay"] == "20310313")
    target["sIssueName"], target["sIssueNameEizi"] = name, name_en
    codes = {o.code for o in tm.select_options(items, TODAY)}
    assert (target["sIssueCode"] in codes) is selected
    assert tm.EXCLUDED_NAME_WORDS == ("ミニ", "MINI")


@pytest.mark.parametrize(
    ("day", "monthly"),
    [
        # 2031-03 の第 2 金曜日は 14 日。月次の最終売買日（前の営業日）は範囲 [7, 14) に入る
        (date(2031, 3, 13), True),
        (date(2031, 3, 7), True),
        (date(2031, 3, 14), False),  # SQ 日そのもの
        (date(2031, 3, 6), False),  # 第 1 金曜日の前（週次）
        (date(2031, 4, 10), True),  # 第 2 金曜日 11 日の前日
        (date(2031, 5, 8), True),  # 第 2 金曜日 9 日の前日
        (date(2031, 5, 7), True),  # 第 2 金曜日が休日のときの水曜日
        (date(2031, 5, 15), False),  # 第 3 金曜日の前（週次）
        (date(2031, 5, 1), False),  # 第 1 金曜日の前（週次）
    ],
)
def test_is_monthly_last_trading_day(day, monthly):
    assert tm.is_monthly_last_trading_day(day) is monthly


@pytest.mark.parametrize(
    ("nearest_state", "expected_name", "expected_day"),
    [
        ("9", "架空先物2", date(2031, 4, 10)),  # 停止中 → 次の限月
        (" ", "架空先物1", date(2031, 3, 13)),  # 通常
        ("0", "架空先物1", date(2031, 3, 13)),  # 解除
        ("", "架空先物2", date(2031, 4, 10)),  # 未知の値は取らない側に倒す
        ("1", "架空先物2", date(2031, 4, 10)),
    ],
)
def test_select_future_skips_suspended_nearest(nearest_state, expected_name, expected_day):
    items = future_master()
    nearest = by_name(items)["架空先物1"]
    assert nearest["sBaibaiTeisiC"] == "9"  # fixture の一番近い限月は停止中
    nearest["sBaibaiTeisiC"] = nearest_state
    future = tm.select_future(items, TODAY)
    assert by_code(items)[future.code]["sIssueName"] == expected_name
    assert future.last_trading_day == expected_day
    assert future.contract_month == date(expected_day.year, expected_day.month, 1)
    assert tm.NOT_SUSPENDED == frozenset({" ", "0"})


def test_select_future_excludes_mini_and_other_underlying():
    codes = {i["sIssueName"]: i["sIssueCode"] for i in future_master()}
    future = tm.select_future(future_master(), date(2031, 3, 4))  # 週次の日付でも 19・999 の行は取らない
    assert future.code not in (codes["架空先物ミニ"], codes["架空先物（別原資産）"])
    # 先物の原資産が 101 でなく空白でも通す（別原資産の 999 の行はそのまま）
    items = [{**i, "sGensisanCode": " "} if i["sGensisanCode"] == "101" else i for i in future_master()]
    assert tm.select_future(items, TODAY).code == future.code


@pytest.mark.parametrize(
    ("call", "match"),
    [
        (lambda: tm.select_options(option_master(), date(2031, 4, 11)), "option months"),
        (lambda: tm.select_options(option_master(), date(2031, 6, 1)), "option months"),
        (lambda: tm.select_options(option_master(), TODAY, strike_range=(5000, 6000)), "strike_range"),
        (lambda: tm.select_future(future_master(), date(2031, 7, 1)), "no future"),
        (lambda: tm.select_future([], TODAY), "no future"),
    ],
)
def test_nothing_to_select_is_master_error(call, match):
    with pytest.raises(tm.MasterError, match=match) as e:
        call()
    assert "9000" not in str(e.value)  # 銘柄コードの一覧を文に入れない


def test_bad_strike_is_master_error():
    items = option_master()
    items[0]["sKousiPrice"] = "2000.500000"
    with pytest.raises(tm.MasterError, match="sKousiPrice"):
        tm.select_options(items, TODAY)


def test_missing_field_is_master_error():
    items = option_master()
    del items[0]["sIssueCode"]
    with pytest.raises(tm.MasterError, match="sIssueCode"):
        tm.select_options(items, TODAY)


def test_duplicate_key_is_master_error():
    items = option_master()
    target = next(i for i in items if i["sLastBaibaiDay"] == "20310313" and i["sSakOpSyouhin"] in tm.OPTION_PRODUCTS)
    items.append({**target, "sIssueCode": "900099999"})
    with pytest.raises(tm.MasterError, match="duplicate"):
        tm.select_options(items, TODAY)


def test_two_futures_on_same_day_is_master_error():
    items = future_master()
    target = next(i for i in items if i["sIssueName"] == "架空先物2")
    items.append({**target, "sIssueCode": "900099998"})
    with pytest.raises(tm.MasterError):
        tm.select_future(items, TODAY)


def test_select_and_codes():
    selection = tm.select(option_master(), future_master(), TODAY)
    codes = selection.codes()
    assert codes[0] == tm.SPOT_CODE == "101"
    assert codes[1] == selection.future.code
    assert codes[2:] == [o.code for o in selection.options]


def test_parse_helpers():
    assert tm.parse_yyyymmdd("20310313") == date(2031, 3, 13)
    assert tm.parse_yyyymmdd("00000000") is None
    assert tm.parse_yyyymmdd("") is None
    assert tm.parse_contract_month("203104") == date(2031, 4, 1)
    assert tm.parse_strike("2000.000000") == 2000
    assert tm.parse_strike("1000") == 1000
    for text in ("2031031", "20311345", "abcdefgh"):
        with pytest.raises(tm.MasterError):
            tm.parse_yyyymmdd(text)
    for text in ("2031", "203113"):
        with pytest.raises(tm.MasterError):
            tm.parse_contract_month(text)
    for text in ("x", "NaN", "Infinity", "1.5"):
        with pytest.raises(tm.MasterError):
            tm.parse_strike(text)

import json
from datetime import UTC, date, datetime

import pytest
from conftest import REPO_ROOT
from tachibana_fakes import fixture_json, market_item

from optionchan_ingest.bq_proto import load_schema, proto_to_rows, rows_to_proto, schema_to_descriptor
from optionchan_ingest.parser import CALL, JST, PUT
from optionchan_ingest.snapshot import RunMeta
from optionchan_ingest.sources.tachibana import master as tm
from optionchan_ingest.sources.tachibana import parser as tp

SCHEMA_DIR = REPO_ROOT / "schema" / "tokyo"
TODAY = date(2031, 3, 3)
CREATED_AT = datetime(2031, 3, 3, 1, 0, 7, tzinfo=UTC)  # 10:00:07 JST
RUN = RunMeta(ingest_run_id="run-test-1", parser_version=tp.PARSER_VERSION, source_object="gs://test-bucket/x#1")
TABLES = ("spot_price", "future_price", "option_price")


def selection():
    return tm.select(
        fixture_json("master_op.json")["aCLMStkIssueMstOp"],
        fixture_json("master_sak.json")["aCLMStkIssueMstSak"],
        TODAY,
    )


def prices(sel, overrides=None):
    overrides = overrides or {}
    values = {
        "pDPP": "12",
        "tDPP:T": "09:58",
        "pDYWP": "+3",
        "pDYRP": "33.33",
        "pQBP": "11",
        "pBV": "5",
        "pQAP": "13",
        "pAV": "7",
        "pDV": "100",
    }
    out = [market_item(sel.spot_code, {**values, "pDPP": "1234.56", "pDYWP": "-1.5", "pDYRP": "-0.12"})]
    out.append(market_item(sel.future.code, {**values, "pDPP": "1230.0", "pDYWP": "+10", "pDYRP": "0.82"}))
    out += [market_item(o.code, {**values, **overrides.get(o.code, {})}) for o in sel.options]
    return out


def schema_columns(table):
    return {c["name"] for c in json.loads((SCHEMA_DIR / f"{table}.json").read_text(encoding="utf-8"))}


def test_shape_and_columns_match_schema():
    sel = selection()
    rows = tp.build_rows(prices(sel), sel, CREATED_AT, RUN)
    assert set(rows) == set(TABLES)
    assert len(rows["spot_price"]) == 1
    assert len(rows["future_price"]) == 1
    assert len(rows["option_price"]) == len(sel.options) == 20
    for table in TABLES:
        for row in rows[table]:
            assert set(row) == schema_columns(table)  # 部分集合より強い（全列を持つ）


def test_required_columns_and_values():
    sel = selection()
    rows = tp.build_rows(prices(sel), sel, CREATED_AT, RUN)
    for row, o in zip(rows["option_price"], sel.options, strict=True):
        assert row["type"] == o.type
        assert row["target_price"] == o.target_price
        assert row["last_trading_day"] == o.last_trading_day
        assert row["created_at"] is not None
        assert row["price"] == 12 and isinstance(row["price"], int)
        assert row["diff"] == 3
        assert row["diff_rate"] == 33.33
        assert (row["bid"], row["bid_volume"], row["ask"], row["ask_volume"], row["volume"]) == (11, 5, 13, 7, 100)
        assert row["price_time"] == datetime(2031, 3, 3, 9, 58, tzinfo=JST)
    assert [r["type"] for r in rows["option_price"][:10]] == [CALL] * 5 + [PUT] * 5
    future = rows["future_price"][0]
    assert future["contract_month"] == date(2031, 4, 1)
    assert future["price"] == 1230.0 and isinstance(future["price"], float)
    assert future["diff"] == 10.0
    spot = rows["spot_price"][0]
    assert (spot["price"], spot["diff"], spot["diff_rate"]) == (1234.56, -1.5, -0.12)
    for row in (future, spot):
        assert row["created_at"] is not None
        assert isinstance(row["diff"], float)


def test_null_columns():
    sel = selection()
    rows = tp.build_rows(prices(sel), sel, CREATED_AT, RUN)
    assert tp.FIELD_POSITIONS is None
    for row in rows["option_price"]:
        for col in tp.OPTION_NULL_COLUMNS:
            assert row[col] is None
        assert row["positions"] is None
    for col in tp.FUTURE_NULL_COLUMNS:
        assert rows["future_price"][0][col] is None
    for col in tp.SPOT_NULL_COLUMNS:
        assert rows["spot_price"][0][col] is None
    assert {"is_atm", "iv", "price_time_raw", "page_updated_at", "quotation"} <= set(tp.OPTION_NULL_COLUMNS)


@pytest.mark.parametrize("empty", ["", " "])
def test_empty_values_are_none(empty):
    sel = selection()
    target = sel.options[0].code
    blank = dict.fromkeys(["pDPP", "tDPP:T", "pDYWP", "pDYRP", "pQBP", "pBV", "pQAP", "pAV", "pDV"], empty)
    rows = tp.build_rows(prices(sel, {target: blank}), sel, CREATED_AT, RUN)
    row = rows["option_price"][0]
    for col in ("price", "price_time", "diff", "diff_rate", "bid", "bid_volume", "ask", "ask_volume", "volume"):
        assert row[col] is None
    assert rows["option_price"][1]["price"] == 12
    assert tp.EMPTY_VALUES == frozenset({""})


@pytest.mark.parametrize("value", ["-", " - "])
def test_dash_is_not_empty_but_parse_error(value):
    # 資料では空の値は "" だけ。"-" は数値でないので TachibanaParseError にして気づけるようにする
    sel = selection()
    with pytest.raises(tp.TachibanaParseError, match="number"):
        tp.build_rows(prices(sel, {sel.options[0].code: {"pDPP": value}}), sel, CREATED_AT, RUN)


def test_missing_field_is_none():
    sel = selection()
    items = prices(sel)
    del items[2]["pQBP"]
    rows = tp.build_rows(items, sel, CREATED_AT, RUN)
    assert rows["option_price"][0]["bid"] is None


def test_created_at_and_meta_are_same_for_all_rows():
    sel = selection()
    rows = tp.build_rows(prices(sel), sel, CREATED_AT, RUN)
    all_rows = [r for t in TABLES for r in rows[t]]
    assert {r["created_at"] for r in all_rows} == {CREATED_AT}
    for r in all_rows:
        assert (r["ingest_run_id"], r["parser_version"], r["source_object"]) == (
            "run-test-1",
            "tachibana-1",
            "gs://test-bucket/x#1",
        )


@pytest.mark.parametrize(
    ("text", "created_at", "expected"),
    [
        # 夜間の 0 時以降に取った 0 時以降の時刻はその日（夜間の始まりから見て翌日）
        ("00:30", datetime(2031, 3, 4, 0, 45, tzinfo=JST), datetime(2031, 3, 4, 0, 30, tzinfo=JST)),
        # 0 時前の時刻は前日
        ("23:55", datetime(2031, 3, 4, 0, 45, tzinfo=JST), datetime(2031, 3, 3, 23, 55, tzinfo=JST)),
        ("15:45", datetime(2031, 3, 3, 15, 50, tzinfo=JST), datetime(2031, 3, 3, 15, 45, tzinfo=JST)),
        # 取得時刻より少し後（許容 5 分以内）は当日
        ("15:52", datetime(2031, 3, 3, 15, 50, tzinfo=JST), datetime(2031, 3, 3, 15, 52, tzinfo=JST)),
        ("15:56", datetime(2031, 3, 3, 15, 50, tzinfo=JST), datetime(2031, 3, 2, 15, 56, tzinfo=JST)),
        # created_at が UTC でも JST の日付で補う（UTC 15:10 = JST 翌日 00:10）
        ("00:05", datetime(2031, 3, 3, 15, 10, tzinfo=UTC), datetime(2031, 3, 4, 0, 5, tzinfo=JST)),
        # 金曜の夜 → 土曜の朝
        ("05:59", datetime(2031, 3, 8, 6, 1, tzinfo=JST), datetime(2031, 3, 8, 5, 59, tzinfo=JST)),
        ("", datetime(2031, 3, 3, 9, 1, tzinfo=JST), None),
        (None, datetime(2031, 3, 3, 9, 1, tzinfo=JST), None),
    ],
)
def test_complete_price_time(text, created_at, expected):
    value = tp.complete_price_time(text, created_at)
    assert value == expected
    if value is not None:
        assert value.tzinfo is JST


@pytest.mark.parametrize("text", ["9:5:x", "2031/03/03 09:00", "25:00", "abc", "09:00:05"])
def test_complete_price_time_rejects_unknown_format(text):
    # 時刻は HH:MM だけ（資料で確かめた）。秒付き・日付付きは通さない
    with pytest.raises(tp.TachibanaParseError):
        tp.complete_price_time(text, CREATED_AT)
    assert tp.PRICE_TIME_FORMATS == ("%H:%M",)


def test_naive_created_at_is_rejected():
    sel = selection()
    with pytest.raises(ValueError):
        tp.build_rows(prices(sel), sel, datetime(2031, 3, 3, 10, 0), RUN)
    with pytest.raises(ValueError):
        tp.complete_price_time("10:00", datetime(2031, 3, 3, 10, 0))


@pytest.mark.parametrize(("value", "message"), [("12.5", "integer"), ("abc", "number"), ("NaN", "finite")])
def test_bad_integer_is_parse_error(value, message):
    sel = selection()
    with pytest.raises(tp.TachibanaParseError, match=message):
        tp.build_rows(prices(sel, {sel.options[0].code: {"pDPP": value}}), sel, CREATED_AT, RUN)


def test_missing_and_duplicate_issues_are_parse_errors():
    sel = selection()
    items = prices(sel)
    with pytest.raises(tp.TachibanaParseError, match="missing in prices: 1"):
        tp.build_rows(items[:-1], sel, CREATED_AT, RUN)
    with pytest.raises(tp.TachibanaParseError, match="duplicate"):
        tp.build_rows(items + [items[-1]], sel, CREATED_AT, RUN)
    with pytest.raises(tp.TachibanaParseError, match="sIssueCode"):
        tp.build_rows(items + [{"pDPP": "1"}], sel, CREATED_AT, RUN)


def test_unselected_issues_are_ignored():
    sel = selection()
    rows = tp.build_rows(prices(sel) + [market_item("900088888")], sel, CREATED_AT, RUN)
    assert len(rows["option_price"]) == len(sel.options)


def test_market_columns():
    assert tp.MARKET_COLUMNS[:2] == ("pDPP", "tDPP:T")
    assert None not in tp.MARKET_COLUMNS


def test_rows_encode_to_bigquery_types():
    sel = selection()
    rows = tp.build_rows(prices(sel), sel, CREATED_AT, RUN)
    for table in TABLES:
        schema = load_schema(SCHEMA_DIR, table)
        desc = schema_to_descriptor(schema, table)
        decoded = proto_to_rows(desc, schema, rows_to_proto(desc, schema, rows[table]))
        assert len(decoded) == len(rows[table])
        assert decoded[0]["created_at"] == CREATED_AT

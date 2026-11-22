"""JSCC の VaR 証拠金計算用パラメータファイルの解析・1 枚の証拠金の計算（#164。jscc.py の契約）。

入力は jsccgen で組み立てた合成のデータ（実ファイルを置かない・切り出さない）
"""

import json
import math
from datetime import date
from decimal import Decimal

import pytest
from conftest import REPO_ROOT
from jsccgen import SCENARIOS, Bpf, Inst, all_instruments_var, default_instruments, future, listing_html, var_parameter

from optionchan_ingest import jscc
from optionchan_ingest.jscc import BpfReader, JsccFormatError

DAY = date(2026, 11, 20)
SCHEMA = REPO_ROOT / "schema" / "tokyo"


def read(bpf: Bpf, group="NK225", business_date=DAY):
    reader = BpfReader(bpf.lines(), product_group=group, business_date=business_date)
    return list(reader), reader


def schema_names(table):
    with open(SCHEMA / f"{table}.json", encoding="utf-8") as f:
        return [c["name"] for c in json.load(f)]


# ---- parse_listing ----

PAGE = "https://example.test/jscc/listed-derivatives/weekday/index.html"


def test_parse_listing_maps_files_to_dates():
    html = listing_html(
        [
            "BPF_IDX_20261120_1600.zip",
            "VaRParameter_20261120_1600.csv",
            "All-Instruments-VaR_20261120_1600.csv",
            "BPF_IDX_20261119_1600.zip",
            # 版・種類の違うものは入らない
            "BPF_IDX_20261120_1100.zip",
            "BPF_JGB_20261120_1600.zip",
            "APF_IDX_20261120_1600.zip",
            "archive.html",
            "BPF_IDX_20261332_1600.zip",  # 日付として不正
            "BPF_IDX_20261118_1600.csv",  # 拡張子が違う
        ]
    )
    got = jscc.parse_listing(html, PAGE)
    base = "https://example.test/jscc/listed-derivatives/weekday/"
    assert got == {
        date(2026, 11, 20): {
            "bpf": base + "BPF_IDX_20261120_1600.zip",
            "parameter": base + "VaRParameter_20261120_1600.csv",
            "margin": base + "All-Instruments-VaR_20261120_1600.csv",
        },
        date(2026, 11, 19): {"bpf": base + "BPF_IDX_20261119_1600.zip"},
    }


def test_parse_listing_ignores_other_hosts_and_paths():
    html = listing_html(
        [],
        extra_hrefs=[
            "https://evil.test/BPF_IDX_20261120_1600.zip",
            "//evil.test/BPF_IDX_20261120_1600.zip",
            "../BPF_IDX_20261120_1600.zip",
            "sub/BPF_IDX_20261120_1600.zip",
            "BPF_IDX_20261120_1600.zip?x=1",
            "BPF_IDX_20261120_1600.zip#a",
            "/BPF_IDX_20261120_1600.zip",
        ],
    )
    assert jscc.parse_listing(html, PAGE) == {}


def test_parse_listing_other_shape_is_empty():
    assert jscc.parse_listing("<html><body>maintenance</body></html>", PAGE) == {}
    assert jscc.parse_listing("", PAGE) == {}


def test_parse_listing_other_version():
    html = listing_html(["BPF_IDX_20261120_1100.zip", "BPF_IDX_20261120_1600.zip"])
    got = jscc.parse_listing(html, PAGE, version="1100")
    assert got == {DAY: {"bpf": "https://example.test/jscc/listed-derivatives/weekday/BPF_IDX_20261120_1100.zip"}}


# ---- BpfReader ----


def test_reader_returns_only_group_instruments():
    insts, reader = read(Bpf())
    assert [i.instrument_code for i in insts] == ["100000001", "100000002", "100000003", "100000004"]
    assert {i.product_group_code for i in insts} == {"NK225"}
    assert reader.summary.instruments == 4
    assert reader.summary.file_type == "S"
    assert reader.summary.scenarios == SCENARIOS
    for inst, src in zip(insts, default_instruments(), strict=False):
        assert list(inst.pnl) == src.values()
        assert len(inst.pnl) == len(SCENARIOS)


def test_reader_fields():
    insts, _ = read(Bpf())
    put, weekly, fut = insts[1], insts[2], insts[3]
    assert put.contract_type == "OOP" and put.put_call_type == "P"
    assert put.strike_price == Decimal("29500.0000")
    assert put.price == 24.0
    assert put.contract_size == 1000
    assert put.aggregation_group == "IDX_IDX"
    assert put.calculation_method_flag == "H"
    assert weekly.product_code == "NK225W2" and weekly.contract_period_code == "2026122"
    assert fut.contract_type == "FUT"
    assert fut.put_call_type is None and fut.strike_price is None and fut.underlying_period_code is None


def test_reader_summary_none_until_done():
    reader = BpfReader(Bpf().lines(), product_group="NK225", business_date=DAY)
    it = iter(reader)
    next(it)
    assert reader.summary is None
    assert reader.scenarios == SCENARIOS  # 最初の銘柄を返した時点で並びは分かる
    list(it)
    assert reader.summary is not None


def test_reader_accepts_lines_without_newline():
    lines = [line.rstrip("\r\n") for line in Bpf().lines()]
    insts = list(BpfReader(lines, product_group="NK225", business_date=DAY))
    assert len(insts) == 4


def test_reader_other_group():
    insts, reader = read(Bpf(), group="TOPIX")
    assert [i.instrument_code for i in insts] == ["200000001"]


def test_reader_group_without_instruments_fails():
    with pytest.raises(JsccFormatError):
        read(Bpf(), group="NONE")


def fails(bpf, match=None, **kwargs):
    with pytest.raises(JsccFormatError, match=match):
        read(bpf, **kwargs)


def test_reader_date_mismatch_fails():
    fails(Bpf(date_text="20261119"), match="DATE")
    fails(Bpf(), business_date=date(2026, 11, 19), match="DATE")


def test_reader_file_type_fails():
    fails(Bpf(file_type="T"), match="File Type")


def test_reader_date_record_count_fails():
    fails(Bpf(extra_date_record=True), match="DATE")
    lines = [line for line in Bpf().lines() if not line.startswith("0,")]
    with pytest.raises(JsccFormatError, match="DATE"):
        list(BpfReader(lines, product_group="NK225", business_date=DAY))


def test_reader_scenario_order_differs_fails():
    swapped = (SCENARIOS[1], SCENARIOS[0]) + SCENARIOS[2:]
    instruments = default_instruments()
    instruments[2] = Inst("100000003", scenarios=swapped)
    fails(Bpf(instruments=instruments), match="並び")


def test_reader_scenario_count_differs_fails():
    instruments = default_instruments()
    instruments[1] = Inst("100000002", scenarios=SCENARIOS[:-1])
    fails(Bpf(instruments=instruments), match="並び")
    instruments = default_instruments()
    instruments[1] = Inst("100000002", scenarios=SCENARIOS + (("20240107", "S"),))
    fails(Bpf(instruments=instruments), match="並び")


def test_reader_non_contiguous_hspl_fails():
    n = len(SCENARIOS)
    blocks = [("100000001", 0, n), ("100000002", 0, n), ("100000001", 0, n), ("100000003", 0, n), ("100000004", 0, n)]
    fails(Bpf(hspl_blocks=blocks), match="連続")
    # 途中で切れて離れたところに続きがある形は、切れた所で並び（本数）の違いになる
    blocks = [("100000001", 0, n), ("100000002", 0, 3), ("100000003", 0, n), ("100000002", 3, n), ("100000004", 0, n)]
    fails(Bpf(hspl_blocks=blocks), match="並び")


def test_reader_missing_hspl_fails():
    n = len(SCENARIOS)
    blocks = [("100000001", 0, n), ("100000002", 0, n), ("100000004", 0, n)]
    fails(Bpf(hspl_blocks=blocks), match="HSPL")


def test_reader_missing_price_fails():
    fails(Bpf(omit_price=("100000002",)), match="PRICE")
    instruments = default_instruments()
    instruments[1].price = ""
    fails(Bpf(instruments=instruments), match="PRICE")


def test_reader_other_group_missing_price_is_ignored():
    insts, _ = read(Bpf(omit_price=("200000001",)))
    assert len(insts) == 4


def test_reader_scenario_type_fails():
    scen = SCENARIOS[:-1] + (("20240106", "X"),)
    instruments = [Inst("100000001", scenarios=scen)]
    fails(Bpf(instruments=instruments), match="Scenario Type")


def test_reader_method_flag_fails():
    instruments = default_instruments()
    instruments[0].flag = "A"
    fails(Bpf(instruments=instruments), match="計算方式")


def test_reader_instrument_after_hspl_fails():
    fails(Bpf(instrument_after_hspl=Inst("100000009")), match="HSPL の後")


@pytest.mark.parametrize(
    "bad, match",
    [
        ("1,INSTRUMENT,100000009,NK225,NK225E,OOP,C,202612,202612,30000,1000,IDX_IDX", "列"),
        ("2,PRICE,100000001,1.0,9", "列"),
        ("3,HSPL,100000001,20240101,H", "列"),
        ("3,HSPL,100000001,20240101,H,", "Unit PnL"),
        ("3,HSPL,100000001,20240101,H,1.5", "Unit PnL"),
        ("1,INSTRUMENT,100000009,NK225,NK225E,OOP,C,202612,202612,30000,x,IDX_IDX,H", "Contract Size"),
        ("2,PRICE,100000001,abc", "Price"),
        ("1,INSTRUMENT,100000009,NK225,NK225E,OOP,C,202612,202612,3x,1000,IDX_IDX,H", "Strike"),
    ],
)
def test_reader_bad_values_fail(bad, match):
    lines = Bpf().lines()
    kind = bad[0]
    # 同じレコードのブロックの最後に差し込む（HSPL は最初の銘柄の先頭を置き換える）
    if kind == "3":
        idx = next(i for i, line in enumerate(lines) if line.startswith("3,HSPL,100000001,"))
        lines[idx] = bad + "\r\n"
    else:
        idx = max(i for i, line in enumerate(lines) if line.startswith(kind + ","))
        lines.insert(idx + 1, bad + "\r\n")
    with pytest.raises(JsccFormatError, match=match) as e:
        list(BpfReader(lines, product_group="NK225", business_date=DAY))
    assert "行" in str(e.value)  # 行番号が入る


def test_reader_error_message_has_no_values():
    instruments = default_instruments()
    instruments[1].price = "123456.789"
    lines = Bpf(instruments=instruments).lines()
    idx = next(i for i, line in enumerate(lines) if line.startswith("2,PRICE,100000002"))
    lines[idx] = "2,PRICE,100000002,123456.7x9\r\n"
    with pytest.raises(JsccFormatError) as e:
        list(BpfReader(lines, product_group="NK225", business_date=DAY))
    assert "123456" not in str(e.value)


def test_reader_does_not_keep_all_pnl():
    """返した銘柄の pnl を reader が持ち続けない（全銘柄の pnl を溜めない）"""
    reader = BpfReader(Bpf().lines(), product_group="NK225", business_date=DAY)
    for inst in reader:
        assert not any(v is inst.pnl for v in vars(reader).values())


# ---- VaRParameter ----


def test_parse_var_parameter_all_rows():
    records = jscc.parse_var_parameter(var_parameter())
    assert [r.line_no for r in records] == list(range(1, 7))
    assert [r.record_id for r in records] == [0, 0, 0, 1, 1, 2]
    r0 = records[1]
    assert (r0.record_name, r0.level, r0.aggregation_group) == ("VAR", "L02", "IDX_IDX")
    assert r0.confidence_level == 97.5 and r0.stress_scenario_number == 2
    assert r0.params == () and r0.parent_aggregation_group is None and r0.type is None
    r1 = records[3]
    assert r1.params == (("a", 0.5), ("b", 0.25))
    assert r1.parent_aggregation_group is None and r1.type == "OFFSET_LIMIT"
    assert r1.confidence_level is None and r1.stress_scenario_number is None
    assert records[4].params == () and records[4].parent_aggregation_group == "IDX"
    assert records[5].params == (("a", 1.0),)


@pytest.mark.parametrize(
    "text",
    [
        "",
        "#only comment\r\n",
        "0,VAR,L01,IDX,97.5\r\n",  # 列が足りない
        "0,VAR,L01,IDX,97.5,2,9\r\n",  # 列が多い
        "0,VAR,L01,IDX,x,2\r\n",
        "0,VAR,L01,IDX,97.5,x\r\n",
        "1,HSRATIO,L01,IDX,,OFFSET_LIMIT,2,a,0.5\r\n",  # 値が足りない
        "1,HSRATIO,L01,IDX,,OFFSET_LIMIT,1,a,0.5,b,0.2\r\n",  # 個数より多い値
        "1,HSRATIO,L01,IDX,,OFFSET_LIMIT,1,a,x\r\n",
        "1,HSRATIO,L01,IDX,,OFFSET_LIMIT,x\r\n",
        "5,OTHER,L01,IDX,97.5,2\r\n",
        "0,VAR,L01,,97.5,2\r\n",  # Aggregation Group が空
    ],
)
def test_parse_var_parameter_errors(text):
    with pytest.raises(JsccFormatError):
        jscc.parse_var_parameter(text)


def test_stress_params():
    records = jscc.parse_var_parameter(var_parameter())
    assert jscc.stress_params(records, "IDX_IDX") == (97.5, 2)
    assert jscc.stress_params(records, "JGB") == (99.0, 3)
    with pytest.raises(JsccFormatError):
        jscc.stress_params(records, "NONE")
    dup = jscc.parse_var_parameter(var_parameter(extra=["0,VAR,L02,IDX_IDX,99.0,2"]))
    with pytest.raises(JsccFormatError):
        jscc.stress_params(dup, "IDX_IDX")


# ---- margin_one_lot（手で計算した値） ----

H = [-10, -20, 5, 7]
S = [-100, -1, 3]
HIST = [True] * 4 + [False] * 3


def test_margin_contract_example():
    """X = {-10, -20, 5, 7, -100, -1}、t = 6 × 0.5 = 3、k = 3、ES = (100 + 20 + 10) / 3 = 43.33… → 44"""
    assert jscc.margin_one_lot(H + S, HIST, side=1, confidence=50, n_stress=2) == 44


def test_margin_uses_only_worst_stress():
    """S は損失の大きい方から n_stress 本だけ（-100 だけ。-1・3 は入らない）。X = {-10,-20,5,7,-100}、t = 2.5、k = 2
    ES = -(-100 - 20 + 0.5 × -10) / 2.5 = 125 / 2.5 = 50"""
    assert jscc.margin_one_lot(H + S, HIST, side=1, confidence=50, n_stress=1) == 50


def test_margin_short_side():
    """売り: v = -pnl = H {10, 20, -5, -7}・S {100, 1, -3} → S の小さい 2 本 {-3, 1}。X = {-7,-5,-3,1,10,20}
    t = 3、ES = (7 + 5 + 3) / 3 = 5"""
    assert jscc.margin_one_lot(H + S, HIST, side=-1, confidence=50, n_stress=2) == 5


def test_margin_rounds_up_fraction():
    """t が整数でない: X = 6 本、confidence 75 → t = 1.5、k = 1。
    ES = -(-100 + 0.5 × -20) / 1.5 = 110 / 1.5 = 73.33… → 74"""
    assert jscc.margin_one_lot(H + S, HIST, side=1, confidence=75, n_stress=2) == 74


def test_margin_exact_integer_not_rounded_up():
    """ES がちょうど整数なら切り上げない（浮動小数の誤差で 1 円ずれない）。97.5% は t = n / 40"""
    pnl = [-40] * 40 + [0] * 2
    hist = [True] * 40 + [False] * 2
    # X = 40 本の -40 と S の 2 本の 0（n = 42、t = 1.05、k = 1）→ ES = -(-40 + 0.05 × -40) / 1.05 = 40
    assert jscc.margin_one_lot(pnl, hist, side=1, confidence=97.5, n_stress=2) == 40


def test_margin_small_t():
    """t < 1（k = 0）: ES = -x_0"""
    pnl = [-7, 1, 2, 3]
    hist = [True, True, True, False]
    assert jscc.margin_one_lot(pnl, hist, side=1, confidence=97.5, n_stress=1) == 7


def test_margin_negative_es_is_zero():
    """OP の買い（シナリオ後の評価額はすべて正）: ES は負 → 0"""
    pnl = [5, 6, 7, 8, 9, 10, 11]
    assert jscc.margin_one_lot(pnl, HIST, side=1, confidence=50, n_stress=2) == 0


def test_margin_floor():
    """OP の売りの下限（清算値段 × Contract Size）。ES の方が大きければ ES"""
    assert jscc.margin_one_lot(H + S, HIST, side=-1, confidence=50, n_stress=2, floor=24000) == 24000
    assert jscc.margin_one_lot(H + S, HIST, side=-1, confidence=50, n_stress=2, floor=3) == 5


def test_margin_permutation_invariant():
    order = [6, 2, 0, 5, 1, 4, 3]
    pnl = [(H + S)[i] for i in order]
    hist = [HIST[i] for i in order]
    assert jscc.margin_one_lot(pnl, hist, side=1, confidence=50, n_stress=2) == 44


@pytest.mark.parametrize(
    "kwargs",
    [
        {"pnl": [1, 2], "is_hist": [True]},
        {"is_hist": [False] * 7},
        {"n_stress": 4},
        {"confidence": 0},
        {"confidence": 100},
        {"side": 0},
    ],
)
def test_margin_invalid_args(kwargs):
    args = {"pnl": H + S, "is_hist": HIST, "side": 1, "confidence": 50, "n_stress": 2, **kwargs}
    with pytest.raises(ValueError):
        jscc.margin_one_lot(args.pop("pnl"), args.pop("is_hist"), **args)


def test_instrument_margins_floor_only_for_option_short():
    """OP の売りだけ ceil(清算値段 × Contract Size) を下限にする。先物・買いは下限なし"""
    pnl = [-1, 0, 1, 0, 0, 0, 0]
    option = Inst("1", price="24.5", size=1000, pnl=pnl)
    fut = future("2", price="24.5", size=1000, pnl=pnl)
    insts, _ = read(Bpf(instruments=[option, fut]))
    hist = [t == "H" for _, t in SCENARIOS]
    assert jscc.instrument_margins(insts[0], hist, 50, 2) == (24500, 1)
    assert jscc.instrument_margins(insts[1], hist, 50, 2) == (1, 1)
    assert math.ceil(Decimal("24.5") * 1000) == 24500


# ---- All-Instruments-VaR・照合 ----


def test_parse_all_instruments_var():
    text = all_instruments_var({"100000001": (100, 0), "100000002": (5, 7)})
    assert jscc.parse_all_instruments_var(text, "NK225") == {"100000001": (100, 0), "100000002": (5, 7)}
    assert jscc.parse_all_instruments_var("﻿" + text, "NK225") == {"100000001": (100, 0), "100000002": (5, 7)}
    assert jscc.parse_all_instruments_var(text, "TOPIX") == {"900000001": (123, 456)}


@pytest.mark.parametrize(
    "text",
    [
        "Instrument Code,Product Group Code\r\n1,NK225\r\n",
        all_instruments_var({"1": (1, 2)}, extra_lines=["1,NK225,NK225E,Option,C,202612,30000,3,4"]),
        all_instruments_var({"1": (1, 2)}, extra_lines=["2,NK225,NK225E,Option,C,202612,30000,x,4"]),
        "",
    ],
)
def test_parse_all_instruments_var_errors(text):
    with pytest.raises(JsccFormatError):
        jscc.parse_all_instruments_var(text, "NK225")


def test_compare_margins():
    computed = {"a": (1, 2), "b": (3, 4), "c": (5, 6), "e": (0, 0)}
    reference = {"a": (1, 2), "b": (3, 5), "c": (9, 9), "d": (1, 1)}
    check = jscc.compare_margins(computed, reference)
    assert (check.matched, check.mismatched, check.ref_only, check.bpf_only) == (3, 3, 1, 1)
    assert check.mismatched_codes == ("b", "c")
    assert check.matched + check.mismatched == 2 * 3


def test_compare_margins_codes_capped():
    computed = {f"{i:03d}": (1, 1) for i in range(30)}
    reference = {f"{i:03d}": (2, 2) for i in range(30)}
    check = jscc.compare_margins(computed, reference)
    assert check.mismatched == 60
    assert check.mismatched_codes == tuple(f"{i:03d}" for i in range(20))


# ---- ロード用の行 ----


def test_scenario_row_matches_schema():
    insts, _ = read(Bpf())
    row = jscc.scenario_row(insts[1], business_date=DAY, file_version="1600", run_id="r1", reference=(5, 0))
    assert list(row) == schema_names("jscc_var_scenario")
    assert row["business_date"] == "2026-11-20"
    assert row["strike_price"] == "29500"
    assert row["pnl"] == default_instruments()[1].values()
    assert (row["margin_short_jscc"], row["margin_long_jscc"]) == (5, 0)
    assert row["price"] == 24.0
    json.dumps(row)  # NDJSON にできる
    fut = jscc.scenario_row(insts[3], business_date=DAY, file_version="1600", run_id="r1", reference=None)
    assert fut["strike_price"] is None and fut["put_call_type"] is None
    assert fut["margin_short_jscc"] is None and fut["margin_long_jscc"] is None


def test_strike_text():
    insts, _ = read(Bpf(instruments=[Inst("1", strike="22.5000"), Inst("2", strike="22000")]))
    assert [
        jscc.scenario_row(i, business_date=DAY, file_version="1600", run_id="r", reference=None)["strike_price"]
        for i in insts
    ] == ["22.5", "22000"]


def test_scenario_set_row_matches_schema():
    _, reader = read(Bpf())
    row = jscc.scenario_set_row(reader.summary, business_date=DAY, file_version="1600", run_id="r1")
    assert list(row) == schema_names("jscc_var_scenario_set")
    assert row["scenarios"][0] == {"scenario_id": "20240101", "scenario_type": "H"}
    assert row["scenarios"][4] == {"scenario_id": "20240101", "scenario_type": "S"}
    assert (row["scenarios_h"], row["scenarios_s"]) == (4, 3)
    assert row["file_type"] == "S"
    json.dumps(row)


def test_parameter_rows_match_schema():
    records = jscc.parse_var_parameter(var_parameter())
    rows = jscc.parameter_rows(records, business_date=DAY, file_version="1600", run_id="r1")
    assert len(rows) == len(records)
    for row in rows:
        assert list(row) == schema_names("jscc_var_parameter")
    assert rows[3]["params"] == [{"name": "a", "value": 0.5}, {"name": "b", "value": 0.25}]
    assert rows[0]["params"] == []
    json.dumps(rows)

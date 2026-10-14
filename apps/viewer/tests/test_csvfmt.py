"""CSV の形式（現行の HTTP 関数の契約）の純粋関数の検査（#52 AC 2）。"""

from datetime import UTC, date, datetime, timedelta, timezone

import pytest

from optionchan_viewer import csvfmt


def test_cell():
    assert csvfmt.cell(None) == ""
    assert csvfmt.cell(25000) == "25000"
    assert csvfmt.cell(18.5) == "18.5"
    assert csvfmt.cell(float("nan")) == ""
    assert csvfmt.cell(1760335200) == "1760335200"
    # pandas の to_csv（tz-aware の datetime64）と同じ表記
    assert csvfmt.cell(datetime(2026, 10, 13, 7, 0, tzinfo=UTC)) == "2026-10-13 07:00:00+00:00"
    jst = timezone(timedelta(hours=9))
    assert csvfmt.cell(datetime(2026, 10, 13, 16, 0, tzinfo=jst)) == "2026-10-13 07:00:00+00:00"
    # 日付（/smile_snapshot の最終取引日）
    assert csvfmt.cell(date(2026, 11, 12)) == "2026-11-12"
    with pytest.raises(ValueError):
        csvfmt.cell(datetime(2026, 10, 13))
    with pytest.raises(TypeError):
        csvfmt.cell(True)


@pytest.mark.parametrize("value", [0.1, 18.25, 1e-7, 123456789.125, 20.0])
def test_float_is_numerically_equal(value):
    # 値の契約は「数値として同値」（pandas の表記までは合わせない）
    assert float(csvfmt.cell(value)) == value


def smile_row(tp, **kw):
    row = dict.fromkeys(csvfmt.SMILE_COLUMNS)
    row["target_price"] = tp
    row.update(kw)
    return row


def test_smile_csv():
    at = datetime(2026, 10, 13, 5, 59, tzinfo=UTC)
    rows = [
        smile_row(24750, o1_put_iv=20.5, o1_put_price_time=1760335080, o2_put_iv=19.0),
        smile_row(25000, o1_call_iv=17.25, o1_call_price_time=1760335140, o1_put_iv=18.0, o1_put_price_time=1760335100,
                  o2_call_iv=16.5, o2_call_price_time=1760330000, o2_put_iv=17.5, o2_put_price_time=1760331000),
    ]
    text = csvfmt.smile_csv(at, 25000, rows)
    lines = text.split("\n")
    assert lines[0] == f"{int(at.timestamp())},25000"
    # 9 列: target_price, o1_call_iv, o1_call_price_time, o1_put_iv, o1_put_price_time, o2_call_*, o2_put_*
    assert lines[1] == "24750,,,20.5,1760335080,,,19.0,"
    assert lines[2] == "25000,17.25,1760335140,18.0,1760335100,16.5,1760330000,17.5,1760331000"
    assert all(len(line.split(",")) == 9 for line in lines[1:-1])
    assert text.endswith("\n") and lines[-1] == ""


def test_atm_iv_csv():
    rows = [
        {"open": 18.0, "high": 19.5, "low": 17.75, "close": 19.0, "started_at": datetime(2026, 10, 12, 0, tzinfo=UTC)},
        {"open": None, "high": None, "low": None, "close": None, "started_at": datetime(2026, 10, 12, 1, tzinfo=UTC)},
    ]
    assert csvfmt.atm_iv_csv(rows) == (
        "18.0,19.5,17.75,19.0,2026-10-12 00:00:00+00:00\n,,,,2026-10-12 01:00:00+00:00\n"
    )
    assert csvfmt.atm_iv_csv([]) == ""


def test_snapshot_csv():
    # /smile_snapshot（#79）: メタ行 + SNAPSHOT_COLUMNS の縦持ち
    at = datetime(2026, 10, 13, 5, 59, tzinfo=UTC)
    months = {1: {"last_trading_day": date(2026, 11, 12), "atm": 25000},
              2: {"last_trading_day": date(2026, 12, 10), "atm": None}}
    rows = [
        {"month": 1, "type": 2, "target_price": 24750, "iv": None, "price": 150, "price_time": 1760335080,
         "bid_iv": 20.0, "ask_iv": 21.0, "bid": 145, "ask": 155, "mid_iv": 20.5, "otm": 1},
        {"month": 2, "type": 1, "target_price": 25000, "iv": 17.25, "price": None, "price_time": None,
         "bid_iv": None, "ask_iv": 18.0, "bid": None, "ask": 610, "mid_iv": None, "otm": 0},
    ]
    text = csvfmt.snapshot_csv(at, months, rows)
    lines = text.split("\n")
    assert lines[0] == f"{int(at.timestamp())},2026-11-12,25000,2026-12-10,"
    assert lines[1] == "1,2,24750,,150,1760335080,20.0,21.0,145,155,20.5,1"
    assert lines[2] == "2,1,25000,17.25,,,,18.0,,610,,0"
    assert all(len(line.split(",")) == len(csvfmt.SNAPSHOT_COLUMNS) == 12 for line in lines[1:-1])
    assert text.endswith("\n") and lines[-1] == ""

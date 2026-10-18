"""スマイルカーブの曲線の規則（smile.py、#79 AC 5）の検査。"""

from datetime import date

import pytest

from optionchan_viewer import smile

M1, M2 = date(2026, 11, 12), date(2026, 12, 10)
CALL, PUT = 1, 2


def row(month, typ, strike, iv=None, bid_iv=None, ask_iv=None, is_atm=False, price=None, price_time=None, bid=None,
        ask=None):
    return {"month": month, "type": typ, "target_price": strike, "last_trading_day": M1 if month == 1 else M2,
            "is_atm": is_atm, "iv": iv, "price": price, "price_time": price_time, "bid_iv": bid_iv,
            "ask_iv": ask_iv, "bid": bid, "ask": ask}


# 1 限月の ATM は 25000（put の is_atm）、2 限月の ATM は 25250。2 限月の値は 50 台（混ざったら値で分かる）
ROWS = [
    row(1, PUT, 24750, iv=None, bid_iv=20.0, ask_iv=21.0),  # iv が NULL
    row(1, CALL, 24750, iv=17.0, bid_iv=16.0, ask_iv=None),  # bid_iv だけ
    row(1, PUT, 25000, iv=19.0, bid_iv=18.5, ask_iv=19.5, is_atm=True, price=260, price_time=1760335140, bid=255,
        ask=265),  # ATM ちょうど
    row(1, CALL, 25000, iv=18.0, bid_iv=17.5, ask_iv=18.25, is_atm=True),
    row(1, PUT, 25250, iv=None, bid_iv=None, ask_iv=18.0),  # ask_iv だけ
    row(1, CALL, 25250, iv=0.0, bid_iv=0.0, ask_iv=17.0),  # iv・bid_iv が 0（欠損）
    row(2, PUT, 25000, iv=55.0, bid_iv=54.0, ask_iv=56.0),
    row(2, CALL, 25000, iv=None, bid_iv=53.0, ask_iv=55.0),
    row(2, PUT, 25250, iv=None, bid_iv=52.0, ask_iv=54.0, is_atm=True),
    row(2, CALL, 25250, iv=None, bid_iv=51.0, ask_iv=53.0, is_atm=True),
    row(2, PUT, 25500, iv=None, bid_iv=50.0, ask_iv=52.0, is_atm=True),  # is_atm が 2 本: 最小（25250）を使う
]


def by_key(rows):
    return {(r["month"], r["target_price"], r["type"]): r for r in rows}


def test_atm_by_month():
    assert smile.atm_by_month(ROWS) == {1: 25000, 2: 25250}
    # call の is_atm は使わない。put の is_atm が無い限月は None
    assert smile.atm_by_month([r for r in ROWS if not (r["type"] == PUT and r["month"] == 2)]) == {1: 25000, 2: None}


@pytest.mark.parametrize("v,want", [(None, None), (0.0, None), (-1.0, None), (0.01, 0.01), (18.5, 18.5)])
def test_positive_iv(v, want):
    assert smile.positive_iv(v) == want


@pytest.mark.parametrize("bid_iv,ask_iv,want", [
    (18.5, 19.5, 19.0), (17.5, 18.25, 17.875),
    (16.0, None, None), (None, 18.0, None), (0.0, 17.0, None), (17.0, 0.0, None), (None, None, None),
])
def test_mid_iv(bid_iv, ask_iv, want):
    assert smile.mid_iv(bid_iv, ask_iv) == want


def test_snapshot_rows_months():
    months, _ = smile.snapshot_rows(ROWS)
    assert months == {1: {"last_trading_day": M1, "atm": 25000}, 2: {"last_trading_day": M2, "atm": 25250}}


def test_otm_composite():
    _, rows = smile.snapshot_rows(ROWS)
    rows = by_key(rows)
    otm = sorted(k for k, r in rows.items() if r["otm"] == 1)
    # 1 限月（ATM 25000）: K < ATM は put、K >= ATM（ATM ちょうどを含む）は call
    # 2 限月（ATM 25250）: 限月ごとの ATM で決める（1 限月の ATM を使わない）
    assert otm == [(1, 24750, PUT), (1, 25000, CALL), (1, 25250, CALL),
                   (2, 25000, PUT), (2, 25250, CALL)]
    # ATM ちょうどの put は OTM 合成に使わない（値は返す）
    assert rows[(1, 25000, PUT)]["otm"] == 0 and rows[(1, 25000, PUT)]["mid_iv"] == 19.0
    # OTM 合成の 1 本は行使価格ごとに 1 行だけ
    for m in (1, 2):
        strikes = [k[1] for k in otm if k[0] == m]
        assert len(strikes) == len(set(strikes))


def test_otm_without_atm_is_empty():
    rows = [r for r in ROWS if not (r["type"] == PUT and r["is_atm"] and r["month"] == 2)]
    months, out = smile.snapshot_rows(rows)
    assert months[2]["atm"] is None
    assert all(r["otm"] == 0 for r in out if r["month"] == 2)
    assert any(r["otm"] == 1 for r in out if r["month"] == 1)


def test_mid_and_missing():
    _, rows = smile.snapshot_rows(ROWS)
    rows = by_key(rows)
    # mid は両方ある行使価格だけ (bid_iv + ask_iv) / 2
    assert rows[(1, 24750, PUT)]["mid_iv"] == 20.5
    assert rows[(1, 25000, CALL)]["mid_iv"] == 17.875
    assert rows[(2, 25500, PUT)]["mid_iv"] == 51.0
    # 片方だけ・0 の行使価格には mid が無い（片方の値は残す）
    assert rows[(1, 24750, CALL)]["mid_iv"] is None and rows[(1, 24750, CALL)]["bid_iv"] == 16.0
    assert rows[(1, 25250, PUT)]["mid_iv"] is None and rows[(1, 25250, PUT)]["ask_iv"] == 18.0
    r = rows[(1, 25250, CALL)]
    assert r["mid_iv"] is None and r["bid_iv"] is None and r["ask_iv"] == 17.0 and r["iv"] is None
    # iv の NULL はそのまま、他の列は SQL の値
    assert rows[(1, 24750, PUT)]["iv"] is None
    atm_put = rows[(1, 25000, PUT)]
    assert (atm_put["iv"], atm_put["price"], atm_put["price_time"], atm_put["bid"], atm_put["ask"]) == (
        19.0, 260, 1760335140, 255, 265)


def test_months_do_not_mix():
    _, rows = smile.snapshot_rows(ROWS)
    # 1 限月の値は 10〜30、2 限月は 50 台
    for r in rows:
        for c in ("iv", "bid_iv", "ask_iv", "mid_iv"):
            if r[c] is not None:
                assert (10 <= r[c] <= 30) if r["month"] == 1 else (50 <= r[c] < 60), (r, c)
    assert len([r for r in rows if r["month"] == 1]) == 6
    assert len([r for r in rows if r["month"] == 2]) == 5


def test_order_and_columns():
    _, rows = smile.snapshot_rows(list(reversed(ROWS)))
    keys = [(r["month"], r["target_price"], r["type"]) for r in rows]
    assert keys == sorted(keys)
    assert set(rows[0]) == {"month", "type", "target_price", "iv", "price", "price_time", "bid_iv", "ask_iv", "bid",
                            "ask", "mid_iv", "otm"}


def test_empty():
    months, rows = smile.snapshot_rows([])
    assert months == {1: {"last_trading_day": None, "atm": None}, 2: {"last_trading_day": None, "atm": None}}
    assert rows == []


# ---- 1 行目の source（#123）。実行 ID は合成 ----

@pytest.mark.parametrize("ids,want", [
    (["legacy-fill114-20000101T000000Z"], "fill114"),
    ([None, "legacy-fill114-20000101T000100Z"], "fill114"),
    # 旧系の取り込みの形・新系の形・NULL・行なし
    (["legacy-20000101T000000Z"], None),
    (["legacy-fill114"], None),
    (["00000000-0000-0000-0000-000000000000"], None),
    ([None], None),
    ([], None),
])
def test_snapshot_source(ids, want):
    assert smile.snapshot_source([{"ingest_run_id": i} for i in ids]) == want

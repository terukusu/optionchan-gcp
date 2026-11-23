"""JSCC のシナリオの r・g の当てはめ（#168）: jscc_factor の純粋関数（データは jsccgen の合成。実ファイルの値なし）"""

import json
import math
import random
from datetime import date, timedelta

import pytest
from conftest import REPO_ROOT
from jsccgen import FACTOR_NODES, Bpf, b76, factor_month, future, many_scenarios, var_parameter

from optionchan_ingest import jscc, jscc_factor, settings
from optionchan_ingest.jscc_factor import (
    FactorCollector,
    black76,
    factor_rows,
    fit_ridge,
    hat_index,
    horizon_days,
    implied_vol,
    quantile,
    sq_date,
    t_years,
)

BD = date(2026, 11, 20)  # 金曜
HOL = frozenset({date(2026, 11, 23), date(2026, 12, 31), date(2027, 1, 1), date(2027, 1, 11)})
RATE = 0.005


def schema_columns():
    with open(REPO_ROOT / "schema" / "tokyo" / "jscc_var_scenario_factor.json", encoding="utf-8") as f:
        return [c["name"] for c in json.load(f)]


# ---- 時間 ----


def test_sq_date_monthly_second_friday():
    assert sq_date("NK225E", "202612", HOL) == date(2026, 12, 11)
    assert sq_date("NK225F", "202703", HOL) == date(2027, 3, 12)
    assert sq_date("NK225MCF", "202701", HOL) == date(2027, 1, 8)


def test_sq_date_holiday_goes_back():
    # 第 2 金曜が休業 → 前の営業日。続けて休業ならさらに前
    assert sq_date("NK225E", "202701", HOL | {date(2027, 1, 8)}) == date(2027, 1, 7)
    assert sq_date("NK225E", "202701", HOL | {date(2027, 1, 8), date(2027, 1, 7)}) == date(2027, 1, 6)
    # 祝日取引の日もカレンダーの行なので営業日でない（holidays は行の全部）
    assert sq_date("NK225W2", "2027012", HOL | {date(2027, 1, 8)}) == date(2027, 1, 7)


def test_sq_date_weekly():
    assert sq_date("NK225W4", "2026114", HOL) == date(2026, 11, 27)  # 4 番目の金曜
    assert sq_date("NK225W1", "2026121", HOL) == date(2026, 12, 4)
    assert sq_date("NK225C1", "2026121", HOL) == date(2026, 12, 2)  # 1 番目の水曜
    assert sq_date("NK225C4", "2026124", HOL) == date(2026, 12, 23)
    assert sq_date("NK225W2", "2026122", HOL) == sq_date("NK225E", "202612", HOL)  # W2 = 月次の SQ


@pytest.mark.parametrize(
    "product, period",
    [
        ("NK225W5", "2027025"),  # 2027-02 の金曜は 4 回
        ("NK225C5", "2026115"),  # 2026-11 の水曜は 4 回
        ("NK225W2", "2026114"),  # 商品の数字と n が違う
        ("NK225W2", "202612"),  # 週次の商品に 6 桁
        ("NK225E", "2026122"),  # 月次の商品に 7 桁
        ("NK225E", "20261"),
        ("NK225E", "2026AB"),
        ("NK225E", "202613"),
        ("NK225E", ""),
    ],
)
def test_sq_date_none(product, period):
    assert sq_date(product, period, HOL) is None


def test_sq_date_is_business_day():
    rnd = random.Random(3)
    for _ in range(200):
        holidays = frozenset(date(2026, 1, 1) + timedelta(days=rnd.randrange(1500)) for _ in range(200))
        y, m = rnd.randrange(2026, 2030), rnd.randrange(1, 13)
        for product, period in [("NK225E", f"{y}{m:02d}"), ("NK225W3", f"{y}{m:02d}3"), ("NK225C2", f"{y}{m:02d}2")]:
            d = sq_date(product, period, holidays)
            assert d is not None and d.weekday() < 5 and d not in holidays


def test_horizon_days():
    assert horizon_days(date(2026, 11, 20), HOL) == 5  # 金曜・月曜が休業 → 火・水
    assert horizon_days(date(2026, 11, 16), HOL) == 2
    assert horizon_days(date(2026, 11, 19), HOL) == 5  # 木 → 金・(月休業)・火
    assert horizon_days(date(2026, 12, 30), HOL) == 6  # 12-31・1-1 休業、1-2・1-3 は土日
    assert horizon_days(date(2026, 11, 20), HOL, n=1) == 4


def test_t_years():
    assert t_years(date(2026, 12, 11), BD) == pytest.approx((21 - 6.75 / 24) / 365, abs=1e-15)
    assert t_years(BD, BD) < 0


# ---- Black-76・IV ----


def test_black76_matches_reference_and_parity():
    rnd = random.Random(1)
    for _ in range(300):
        F, K = rnd.uniform(20000, 60000), rnd.uniform(15000, 70000)
        T, s = rnd.uniform(1 / 365, 2), rnd.uniform(0.05, 1.5)
        c, p = black76(True, F, K, T, s, RATE), black76(False, F, K, T, s, RATE)
        assert c == pytest.approx(b76(True, F, K, T, s, RATE), rel=1e-9, abs=1e-9)
        assert p == pytest.approx(b76(False, F, K, T, s, RATE), rel=1e-9, abs=1e-9)
        assert c - p == pytest.approx(math.exp(-RATE * T) * (F - K), abs=1e-7)


def test_black76_expired_is_intrinsic():
    assert black76(True, 50000, 49000, 0.0, 0.2, RATE) == 1000
    assert black76(False, 50000, 49000, -0.01, 0.2, RATE) == 0
    assert black76(False, 48000, 49000, -0.01, 0.2, RATE) == 1000
    assert black76(True, 50000, 49000, 0.5, 0.0, RATE) == pytest.approx(math.exp(-RATE * 0.5) * 1000)


def _bisect_iv(is_call, F, K, T, price, rate, it=60):
    a, b = 1e-4, 5.0
    for _ in range(it):
        m = 0.5 * (a + b)
        if b76(is_call, F, K, T, m, rate) < price:
            a = m
        else:
            b = m
    return 0.5 * (a + b)


def test_implied_vol_round_trip_and_bisection():
    rnd = random.Random(2)
    n = 0
    for _ in range(500):
        is_call = rnd.random() < 0.5
        F, T = rnd.uniform(30000, 60000), rnd.uniform(2 / 365, 1.5)
        K = F * math.exp(rnd.uniform(-0.4, 0.2))
        s = rnd.uniform(0.08, 1.2)
        price = black76(is_call, F, K, T, s, RATE)
        lo, hi = black76(is_call, F, K, T, 1e-4, RATE), black76(is_call, F, K, T, 5.0, RATE)
        if not (lo + 1e-9 < price < hi) or price < 1:
            continue
        guess = rnd.choice([None, s * rnd.uniform(0.5, 2), 7.0])
        got = implied_vol(is_call, F, K, T, price, RATE, guess)
        assert got is not None and 1e-4 < got < 5
        assert abs(black76(is_call, F, K, T, got, RATE) - price) < 1e-9 * max(1, price)
        assert got == pytest.approx(s, abs=1e-7)
        assert abs(got - _bisect_iv(is_call, F, K, T, price, RATE)) < 1e-8
        n += 1
    assert n > 300


@pytest.mark.parametrize(
    "is_call, F, K, T, price",
    [
        (True, 50000, 45000, 0.1, 4990.0),  # 本源的価値（割引後 約 4997.5）より下
        (True, 50000, 55000, 0.1, 50000.0),  # 上限（σ = 5）以上
        (False, 50000, 55000, 0.1, 1e-12),  # σ の下限の値以下
        (True, 50000, 55000, 0.0, 10.0),  # T ≤ 0
        (True, 50000, 55000, 0.1, float("nan")),
        (True, -1.0, 55000, 0.1, 10.0),
    ],
)
def test_implied_vol_none(is_call, F, K, T, price):
    assert implied_vol(is_call, F, K, T, price, RATE) is None


# ---- 折れ線・リッジ・分位点 ----


@pytest.mark.parametrize(
    "z, expected",
    [
        (-6.0, (0, 0.0)),
        (-5.0, (0, 0.0)),
        (-4.5, (0, 0.5)),
        (-1.25, (4, 0.5)),
        (0.0, (6, 1.0)),  # 節点ちょうどは左の区間の右端（searchsorted の左と同じ）
        (0.25, (7, 0.5)),
        (3.0, (11, 1.0)),
        (9.0, (11, 1.0)),
    ],
)
def test_hat_index(z, expected):
    j, w = hat_index(z, FACTOR_NODES)
    assert (j, w) == pytest.approx(expected)


def _dense_solve(points, n, ridge):
    a = [[ridge if r == c else 0.0 for c in range(n)] for r in range(n)]
    b = [0.0] * n
    for j, w, y in points:
        row = [0.0] * n
        row[j] += 1 - w
        row[j + 1] += w
        for r in range(n):
            b[r] += row[r] * y
            for c in range(n):
                a[r][c] += row[r] * row[c]
    for i in range(n):  # 部分枢軸選択つきの Gauss 消去
        p = max(range(i, n), key=lambda r: abs(a[r][i]))
        a[i], a[p], b[i], b[p] = a[p], a[i], b[p], b[i]
        for r in range(i + 1, n):
            f = a[r][i] / a[i][i]
            for c in range(i, n):
                a[r][c] -= f * a[i][c]
            b[r] -= f * b[i]
    x = [0.0] * n
    for i in range(n - 1, -1, -1):
        x[i] = (b[i] - sum(a[i][c] * x[c] for c in range(i + 1, n))) / a[i][i]
    return x


def test_fit_ridge_matches_dense():
    rnd = random.Random(4)
    for _ in range(50):
        points = []
        for _ in range(rnd.randrange(0, 60)):
            j, w = hat_index(rnd.uniform(-6, 4), FACTOR_NODES)
            points.append((j, w, rnd.gauss(0, 0.2)))
        got = fit_ridge(points, 13, 1e-4)
        want = _dense_solve(points, 13, 1e-4)
        assert max(abs(x - y) for x, y in zip(got, want, strict=True)) < 1e-9


def test_fit_ridge_empty_and_exact():
    assert fit_ridge([], 13, 1e-4) == [0.0] * 13
    g = [0.1 * m - 0.3 for m in range(13)]
    points = []
    for z in [x / 10 for x in range(-50, 31)]:
        j, w = hat_index(z, FACTOR_NODES)
        points.append((j, w, (1 - w) * g[j] + w * g[j + 1]))
    assert fit_ridge(points, 13, 1e-12) == pytest.approx(g, abs=1e-9)


def test_quantile():
    assert quantile([], 0.5) is None
    assert quantile([3.0], 0.9) == 3.0
    assert quantile([4.0, 1.0, 3.0, 2.0], 0.5) == pytest.approx(2.5)
    assert quantile([4.0, 1.0, 3.0, 2.0], 0.9) == pytest.approx(3.7)  # np.percentile の既定（線形）
    assert quantile([1.0, 2.0], 0.0) == 1.0 and quantile([1.0, 2.0], 1.0) == 2.0


# ---- factor_rows ----


def collect(instruments, business_date=BD):
    """合成の銘柄を BPF の行にして読み、(collector, scenarios, stress, reference) を返す"""
    reader = jscc.BpfReader(
        Bpf(business_date=business_date, instruments=instruments).lines(),
        product_group="NK225",
        business_date=business_date,
    )
    records = jscc.parse_var_parameter(var_parameter())
    collector = FactorCollector()
    reference = {}
    stress = {}
    for inst in reader:
        is_hist = [t == "H" for _, t in reader.scenarios]
        stress.setdefault(inst.aggregation_group, jscc.stress_params(records, inst.aggregation_group))
        reference[inst.instrument_code] = jscc.instrument_margins(inst, is_hist, *stress[inst.aggregation_group])
        collector.add(inst)
    return collector, reader.scenarios, stress, reference


def run(instruments, business_date=BD, holidays=HOL, check=lambda: None, reference=None):
    collector, scenarios, stress, ref = collect(instruments, business_date)
    return factor_rows(
        collector,
        scenarios=scenarios,
        stress=stress,
        reference=ref if reference is None else reference,
        business_date=business_date,
        holidays=holidays,
        file_version="1600",
        run_id="run-1",
        rate=RATE,
        check=check,
    )


def assert_row_shape(row, n):
    assert list(row) == schema_columns()
    json.dumps(row, allow_nan=False)  # NaN・inf が無い
    assert len(row["r"]) == n
    if row["g"]:
        assert len(row["g"]) == 13 * n and row["nodes"] == list(FACTOR_NODES)
    else:
        assert row["nodes"] == []
        assert row["n_fit"] is row["fit_rmse"] is row["check_n"] is None
        assert row["check_median_rel"] is row["check_p90_rel"] is None


def g_at(row, j, m):
    return row["g"][j * 13 + m]


def identified(month, j):
    """シナリオ j で当てはめに使える点が十分に載る節点（重みの和 0.5 以上）"""
    return [m for m, w in enumerate(month.node_weight[j]) if w >= 0.5]


SCEN = many_scenarios()


@pytest.fixture(scope="module")
def recovered():
    month = factor_month(scenarios=SCEN, seed=11)
    result = run(month.instruments)
    return month, result


def test_recovers_r_and_g(recovered):
    """先物のある限月: 既知の r・g から作った pnl から r・g が戻る（AC2）"""
    month, result = recovered
    [row] = result.rows
    assert_row_shape(row, len(SCEN))
    assert (row["product_code"], row["contract_period_code"], row["forward_source"]) == ("NK225E", "202612", "NK225F")
    assert row["forward"] == month.forward
    assert row["t_years"] == pytest.approx(month.t0, abs=1e-15)
    assert row["horizon_days"] == 5 and row["rate"] == RATE
    assert row["r"] == pytest.approx(month.r, abs=1e-9)
    assert row["atm_iv"] == pytest.approx(month.atm_iv, rel=1e-9)
    assert row["n_fit"] == len(month.used)
    assert min(len(identified(month, j)) for j in range(len(SCEN))) >= 6
    worst = max(abs(g_at(row, j, m) - month.g[j][m]) for j in range(len(SCEN)) for m in identified(month, j))
    assert worst < 1e-3
    assert row["fit_rmse"] < 1e-4
    assert result.fitted == 1 and result.skipped == {} and result.empty_scenarios == 0


def test_check_columns(recovered):
    """同じ生成器の pnl なので、当て直した 1 枚の売りは All-Instruments-VaR（合成の基準）に近い"""
    month, result = recovered
    [row] = result.rows
    assert row["check_n"] == len(month.used)
    assert 0 <= row["check_median_rel"] <= row["check_p90_rel"]
    assert row["check_median_rel"] < 0.01
    assert row["check_p90_rel"] < 0.05


def test_check_without_reference_is_null():
    month = factor_month(seed=12)
    result = run(month.instruments, reference={})
    [row] = result.rows
    assert row["g"] and row["check_n"] == 0
    assert row["check_median_rel"] is None and row["check_p90_rel"] is None


def test_parity_forward_when_no_future():
    """先物の無い限月: F₀・F′ をパリティで（AC3）"""
    month = factor_month(future_product=None, period="202705", sq=date(2027, 5, 14), scenarios=SCEN, seed=13)
    [row] = run(month.instruments).rows
    assert_row_shape(row, len(SCEN))
    assert row["forward_source"] == "parity"
    assert row["forward"] == pytest.approx(month.forward, rel=1e-6)
    assert row["r"] == pytest.approx(month.r, abs=1e-6)
    assert row["g"]


def test_future_priority():
    month = factor_month(future_product="NK225MF", fut_cs=100, seed=14)
    other = future("599999999", product="NK225MCF", period="202612", size=10, price="1.0", pnl=[0] * 7)
    [row] = run([*month.instruments, other]).rows
    assert row["forward_source"] == "NK225MF" and row["forward"] == month.forward
    big = future("599999998", product="NK225F", period="202612", size=1000, price="51000.0", pnl=[0] * 7)
    [row] = run([*month.instruments, other, big]).rows
    assert row["forward_source"] == "NK225F" and row["forward"] == 51000.0
    assert row["r"] == [0.0] * 7


def test_t1_expired_has_r_only():
    """h の間に SQ（T₁ ≤ 0）: r だけ、g・nodes は空、質の列は NULL（AC3）"""
    bd = date(2026, 12, 9)  # 水曜。2 営業日後 = 12-11（SQ）
    month = factor_month(business_date=bd, sq=date(2026, 12, 11), h_days=2, seed=15)
    result = run(month.instruments, business_date=bd)
    [row] = result.rows
    assert_row_shape(row, 7)
    assert row["horizon_days"] == 2 and row["t_years"] > 0
    assert row["r"] == pytest.approx(month.r, abs=1e-9)
    assert row["g"] == [] and row["atm_iv"] == pytest.approx(month.atm_iv, rel=1e-9)
    assert result.skipped == {jscc_factor.T1_EXPIRED: ["NK225E:202612"]}
    assert result.fitted == 0


def _used_atm_code(month):
    """使う OP のうち値の大きい（ATM に近い）もの"""
    prices = {i.code: float(i.price) for i in month.instruments}
    return max(month.used, key=lambda c: prices[c])


def test_floor_points_are_excluded():
    """1 円の下限の点は当てはめにも fit_rmse にも入らない。入れると戻らない（AC3）"""
    base = factor_month(scenarios=SCEN, seed=16)
    code = _used_atm_code(base)
    overrides = {(code, 3): 1.0, (code, 5): 1.0}
    month = factor_month(scenarios=SCEN, seed=16, overrides=overrides)
    [row] = run(month.instruments).rows
    for j in (3, 5):
        assert max(abs(g_at(row, j, m) - month.g[j][m]) for m in identified(month, j)) < 1e-3
    assert row["fit_rmse"] < 1e-4
    # 下限のすぐ上（1.001 円）なら点として使われ、当てはめが崩れる
    bad = factor_month(scenarios=SCEN, seed=16, overrides={(code, 3): 1.001})
    [row] = run(bad.instruments).rows
    assert max(abs(g_at(row, 3, m) - bad.g[3][m]) for m in identified(bad, 3)) > 1e-2
    assert row["fit_rmse"] > 1e-3


def test_uninvertible_points_are_excluded():
    base = factor_month(scenarios=SCEN, seed=17)
    code = _used_atm_code(base)
    month = factor_month(scenarios=SCEN, seed=17, overrides={(code, 2): 10 * base.forward})  # σ = 5 の値より上
    [row] = run(month.instruments).rows
    assert max(abs(g_at(row, 2, m) - month.g[2][m]) for m in identified(month, 2)) < 1e-3
    assert row["fit_rmse"] < 1e-4


def test_few_options_has_empty_g():
    month = factor_month(strikes=[47500, 48000, 52000, 52500], seed=18)
    result = run(month.instruments)
    [row] = result.rows
    assert_row_shape(row, 7)
    assert row["g"] == [] and row["atm_iv"] is not None
    assert result.skipped == {jscc_factor.FEW_OPTIONS: ["NK225E:202612"]}


def test_future_only_month():
    fut = future("599999997", product="NK225F", period="202803", size=1000, price="50000.0", pnl=[1000] * 7)
    month = factor_month(seed=19)
    result = run([*month.instruments, fut])
    rows = {(r["product_code"], r["contract_period_code"]): r for r in result.rows}
    row = rows[("NK225F", "202803")]
    assert_row_shape(row, 7)
    assert row["forward_source"] == "NK225F" and row["forward"] == 50000.0
    assert row["r"] == pytest.approx([math.log(1 + 1 / 50000)] * 7, abs=1e-15)
    assert row["g"] == [] and row["atm_iv"] is None
    assert row["t_years"] == pytest.approx(t_years(date(2028, 3, 10), BD))
    assert result.skipped == {}  # 先物だけの限月は当てはめの対象ではない


def test_weekly_rows():
    w4 = factor_month(
        product="NK225W4",
        period="2026114",
        sq=date(2026, 11, 27),
        future_product=None,
        seed=20,
        cs=100,
        code_start=510000001,
    )
    c1 = factor_month(
        product="NK225C1",
        period="2026121",
        sq=date(2026, 12, 2),
        future_product=None,
        seed=21,
        cs=100,
        code_start=520000001,
    )
    month = factor_month(seed=22)
    result = run([*month.instruments, *w4.instruments, *c1.instruments])
    keys = [(r["product_code"], r["contract_period_code"]) for r in result.rows]
    assert keys == [("NK225C1", "2026121"), ("NK225E", "202612"), ("NK225W4", "2026114")]  # 一意で、その順
    rows = dict(zip(keys, result.rows, strict=True))
    for key, m in ((("NK225W4", "2026114"), w4), (("NK225C1", "2026121"), c1)):
        row = rows[key]
        assert_row_shape(row, 7)
        assert row["forward_source"] == "parity"
        assert row["t_years"] == pytest.approx(m.t0, abs=1e-15)
        assert row["r"] == pytest.approx(m.r, abs=1e-6)
        assert row["g"]
    assert result.fitted == 3


def test_no_parity_pair_has_no_row():
    month = factor_month(future_product=None, period="202705", sq=date(2027, 5, 14), seed=23)
    calls = [i for i in month.instruments if i.put_call == "C"]
    result = run(calls)
    assert result.rows == []
    assert result.skipped == {jscc_factor.NO_PARITY: ["NK225E:202705"]}


def test_no_sq_has_no_row():
    month = factor_month(product="NK225W2", period="2026114", future_product=None, seed=24)
    result = run(month.instruments)
    assert result.rows == []
    assert result.skipped == {jscc_factor.NO_SQ: ["NK225W2:2026114"]}


def test_holiday_sq_moves_t():
    month = factor_month(sq=date(2026, 12, 10), seed=25)
    [row] = run(month.instruments, holidays=HOL | {date(2026, 12, 11)}).rows
    assert row["t_years"] == pytest.approx(t_years(date(2026, 12, 10), BD))


def test_collector_keeps_only_needed():
    month = factor_month(seed=26)
    other = future("599999996", product="NK225VF", period="202612", size=1000, price="20.0")
    mini = month.instruments[1]
    mini_other = factor_month(product="NK225ME", seed=27, code_start=530000001).instruments[1]
    collector, *_ = collect([*month.instruments, other, mini_other])
    assert set(collector.futures["202612"]) == {"NK225F"}
    kept = collector.options[("NK225E", "202612")]
    assert all(i.price > 1 for i in kept)
    assert len(kept) == sum(1 for i in month.instruments[1:] if float(i.price) > 1)
    assert ("NK225ME", "202612") not in collector.option_periods
    assert mini.code in {i.instrument_code for i in kept} or float(mini.price) <= 1


def test_check_callback_per_month():
    calls = []
    a = factor_month(seed=28)
    b = factor_month(period="202701", sq=date(2027, 1, 8), seed=29, code_start=540000001)
    run([*a.instruments, *b.instruments], check=lambda: calls.append(1))
    assert len(calls) == 2

    def stop():
        raise RuntimeError("deadline")

    with pytest.raises(RuntimeError):
        run(a.instruments, check=stop)


def test_no_values_in_skipped():
    month = factor_month(strikes=[47500, 52500], seed=30)
    result = run(month.instruments)
    text = json.dumps(result.skipped)
    assert "50000" not in text and "47500" not in text
    assert settings.JSCC_FACTOR_MIN_OPTIONS == 5

"""JSCC のシナリオから限月ごとの先物のリターン r と IV の対数変化 g(z) を当てはめる（#168。純粋関数）。

1 行 = 営業日 × 版 × 商品 × 限月（schema/tokyo/jscc_var_scenario_factor.json）。手順の正は docs/data.md の
「JSCC のシナリオの r・g の当てはめ（#168）」。要点:
- F₀・F′_j: 同じ限月の先物（settings.JSCC_FACTOR_FUTURES の順）の清算値段と F₀ + pnl_j / contract_size。先物の無い
  月次の限月と週次はプットコール・パリティ（ATM の近くの 3 行使価格の中央値）。r_j = ln(F′_j / F₀)
- T₀ = (SQ の 09:00 − 営業日の 15:45) / 365、h = 営業日から 2 営業日後までの暦日、T₁ = T₀ − h / 365
- σ₀ = 清算値段の IV、σ′_ij = シナリオ後の 1 単位の値 pnl_ij / contract_size の (F′_j, T₁) での IV。
  y_ij = ln σ′_ij − ln σ₀_i を z の折れ線（hat の基底）にリッジ付きの最小二乗で当てはめる（正規方程式は 3 重対角）
- check_*: 基準の状態で当て直した 1 枚の売りの ES と All-Instruments-VaR の 1 枚の売りの相対誤差

numpy は使わない。時計・I/O を持たない（期限は呼び出し側の check で見る）。ログ・例外のメッセージに値を入れない
（商品・限月コードと件数まで）
"""

import bisect
import heapq
import math
import statistics
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import date, timedelta

from . import jscc, settings

IV_MIN = 1e-4
IV_MAX = 5.0
IV_LOWER_MARGIN = 1e-9
IV_PRICE_TOL = 1e-12
IV_WIDTH_TOL = 1e-12
IV_MAX_ITER = 100
IV_DEFAULT_GUESS = 0.3
SQ_HOUR_OFFSET_DAYS = 6.75 / 24  # SQ の 09:00 − 営業日の 15:45
PARITY_STRIKES = 3
FLOOR_UNIT_VALUE = 1.0  # OP の 1 単位の値の下限（円）。これ以下の点は当てはめに使わない
_SQRT2 = math.sqrt(2.0)
_SQRT2PI = math.sqrt(2.0 * math.pi)
FRIDAY, WEDNESDAY = 4, 2

# 行が無い・g が空の理由（FactorResult.skipped のキー）
NO_SQ = "no_sq"  # SQ が決まらない（限月コードの形・n 番目の曜日が無い）
EXPIRED = "expired"  # T₀ ≤ 0
NO_PARITY = "no_parity"  # パリティの組が基準かどれかのシナリオで無い
NONFINITE_R = "nonfinite_r"  # r に非有限（F′ ≤ 0 など）
T1_EXPIRED = "t1_expired"  # T₁ ≤ 0（h の間に SQ）→ g 空
FEW_OPTIONS = "few_options"  # 使う OP が settings.JSCC_FACTOR_MIN_OPTIONS 本未満 → g 空
NONFINITE_G = "nonfinite_g"  # g・質の列に非有限 → g 空
NO_ROW_REASONS = (NO_SQ, EXPIRED, NO_PARITY, NONFINITE_R)
EMPTY_G_REASONS = (T1_EXPIRED, FEW_OPTIONS, NONFINITE_G)


# ---- 時間 ----


def is_business_day(d: date, holidays: frozenset[date]) -> bool:
    return d.weekday() < 5 and d not in holidays


def previous_business_day(d: date, holidays: frozenset[date]) -> date:
    """d が営業日でなければ前の営業日へ戻す"""
    while not is_business_day(d, holidays):
        d -= timedelta(days=1)
    return d


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date | None:
    first = date(year, month, 1)
    d = first + timedelta(days=(weekday - first.weekday()) % 7 + 7 * (n - 1))
    return d if d.month == month else None


def sq_date(product_code: str, contract_period_code: str, holidays: frozenset[date]) -> date | None:
    """SQ の日。6 桁（YYYYMM。月次の OP・先物）→ 第 2 金曜、7 桁（YYYYMMn）の NK225Wn → n 番目の金曜・NK225Cn →
    n 番目の水曜。営業日でなければ前の営業日。決まらない（形が違う・n 番目が月に無い・n が商品と違う）なら None"""
    code = contract_period_code or ""
    if not code.isdigit() or len(code) not in (6, 7):
        return None
    year, month = int(code[:4]), int(code[4:6])
    if not 1 <= month <= 12:
        return None
    weekly = product_code[:-1] if len(product_code) > 1 and product_code[-1].isdigit() else None
    if len(code) == 6:
        if weekly in ("NK225W", "NK225C"):
            return None
        d = _nth_weekday(year, month, FRIDAY, 2)
    else:
        n = int(code[6])
        if weekly not in ("NK225W", "NK225C") or product_code[-1] != code[6] or n < 1:
            return None
        d = _nth_weekday(year, month, FRIDAY if weekly == "NK225W" else WEDNESDAY, n)
    if d is None:
        return None
    return previous_business_day(d, holidays)


def horizon_days(business_date: date, holidays: frozenset[date], n: int = 2) -> int:
    """business_date から n 番目の営業日までの暦日"""
    d, k = business_date, 0
    while k < n:
        d += timedelta(days=1)
        k += is_business_day(d, holidays)
    return (d - business_date).days


def t_years(sq: date, business_date: date) -> float:
    """(SQ の 09:00 − 営業日の 15:45) / 365 日"""
    return ((sq - business_date).days - SQ_HOUR_OFFSET_DAYS) / 365


# ---- Black-76・IV ----


def black76(is_call: bool, F: float, K: float, T: float, sigma: float, rate: float) -> float:
    """1 単位の値。T ≤ 0・sigma ≤ 0 は割引（e^{−rate·max(T,0)}）した本源的価値"""
    df = math.exp(-rate * max(T, 0.0))
    if T <= 0 or sigma <= 0:
        return df * max(0.0, (F - K) if is_call else (K - F))
    sd = sigma * math.sqrt(T)
    d1 = math.log(F / K) / sd + 0.5 * sd
    d2 = d1 - sd
    if is_call:
        return df * (F * 0.5 * math.erfc(-d1 / _SQRT2) - K * 0.5 * math.erfc(-d2 / _SQRT2))
    return df * (K * 0.5 * math.erfc(d2 / _SQRT2) - F * 0.5 * math.erfc(d1 / _SQRT2))


def implied_vol(
    is_call: bool, F: float, K: float, T: float, price: float, rate: float, guess: float | None = None
) -> float | None:
    """price の Black-76 の IV（Newton + 二分の安全策。区間 [IV_MIN, IV_MAX]）。
    price が (black76(IV_MIN) + 1e-9, black76(IV_MAX)) の外・F・K・T が正でない・非有限・IV_MAX_ITER 回で
    収束しない（価格の差・区間の幅が許容に入らない）なら None"""
    if not (F > 0 and K > 0 and T > 0 and math.isfinite(F) and math.isfinite(K) and math.isfinite(price)):
        return None
    lo_price = black76(is_call, F, K, T, IV_MIN, rate)
    hi_price = black76(is_call, F, K, T, IV_MAX, rate)
    if not lo_price + IV_LOWER_MARGIN < price < hi_price:
        return None
    df = math.exp(-rate * T)
    sqt = math.sqrt(T)
    lk = math.log(F / K)
    a, b = IV_MIN, IV_MAX
    if guess is not None and a < guess < b:
        s = guess
    else:
        s = max(IV_DEFAULT_GUESS, _SQRT2PI / sqt * price / (df * F))  # Brenner の近似と 0.3 の大きい方
        if not a < s < b:
            s = 0.5 * (a + b)
    tol = IV_PRICE_TOL * max(1.0, price)
    for _ in range(IV_MAX_ITER):
        sd = s * sqt
        d1 = lk / sd + 0.5 * sd
        d2 = d1 - sd
        if is_call:
            p = df * (F * 0.5 * math.erfc(-d1 / _SQRT2) - K * 0.5 * math.erfc(-d2 / _SQRT2))
        else:
            p = df * (K * 0.5 * math.erfc(d2 / _SQRT2) - F * 0.5 * math.erfc(d1 / _SQRT2))
        f = p - price
        if f < 0:
            a = s
        else:
            b = s
        if abs(f) < tol or b - a < IV_WIDTH_TOL:
            return s
        vega = df * F * math.exp(-0.5 * d1 * d1) / _SQRT2PI * sqt
        n = s - f / vega if vega > 0 else 0.5 * (a + b)
        s = n if a < n < b else 0.5 * (a + b)
    return None


# ---- 折れ線・リッジ ----


def hat_index(z: float, nodes: Sequence[float]) -> tuple[int, float]:
    """z の折れ線の基底: (左の節点の添字 j, 右の重み w)。値は (1 − w)·g[j] + w·g[j+1]。z は端で切る"""
    zc = min(max(z, nodes[0]), nodes[-1])
    j = min(max(bisect.bisect_left(nodes, zc) - 1, 0), len(nodes) - 2)
    return j, (zc - nodes[j]) / (nodes[j + 1] - nodes[j])


def fit_ridge(points: Sequence[tuple[int, float, float]], n_nodes: int, ridge: float) -> list[float]:
    """(BᵀB + ridge·I) g = Bᵀy の解。points は (j, w, y)（B の行は j に 1 − w、j + 1 に w）。
    BᵀB は 3 重対角なので Thomas 法。点が無ければ全 0"""
    diag = [ridge] * n_nodes
    off = [0.0] * (n_nodes - 1)  # (m, m + 1)
    rhs = [0.0] * n_nodes
    for j, w, y in points:
        u = 1.0 - w
        diag[j] += u * u
        diag[j + 1] += w * w
        off[j] += u * w
        rhs[j] += u * y
        rhs[j + 1] += w * y
    # 前進消去（対称・正定値なので枢軸選択なしで安定）
    c = [0.0] * n_nodes
    d = [0.0] * n_nodes
    c[0] = off[0] / diag[0] if n_nodes > 1 else 0.0
    d[0] = rhs[0] / diag[0]
    for m in range(1, n_nodes):
        den = diag[m] - off[m - 1] * c[m - 1]
        if m < n_nodes - 1:
            c[m] = off[m] / den
        d[m] = (rhs[m] - off[m - 1] * d[m - 1]) / den
    g = [0.0] * n_nodes
    g[-1] = d[-1]
    for m in range(n_nodes - 2, -1, -1):
        g[m] = d[m] - c[m] * g[m + 1]
    return g


def interp(x: float, xs: Sequence[float], ys: Sequence[float]) -> float:
    """xs（昇順）の折れ線の x の値。範囲の外は端の値"""
    if x <= xs[0]:
        return ys[0]
    if x >= xs[-1]:
        return ys[-1]
    i = bisect.bisect_right(xs, x)
    x0, x1 = xs[i - 1], xs[i]
    if x1 == x0:
        return ys[i]
    return ys[i - 1] + (ys[i] - ys[i - 1]) * (x - x0) / (x1 - x0)


def quantile(xs: Sequence[float], q: float) -> float | None:
    """線形補間の分位点（0 ≤ q ≤ 1）。空なら None"""
    if not xs:
        return None
    s = sorted(xs)
    pos = q * (len(s) - 1)
    lo = math.floor(pos)
    hi = min(lo + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (pos - lo)


# ---- ストリームから残す銘柄 ----


class FactorCollector:
    """BPF の銘柄のうち当てはめに要るものだけを残す（BpfInstrument の参照。pnl はコピーしない）:
    先物（settings.JSCC_FACTOR_FUTURES）の全部と、対象の OP（settings.JSCC_FACTOR_PRODUCTS）で清算値段 > 1 円のもの"""

    def __init__(self):
        self.futures: dict[str, dict[str, jscc.BpfInstrument]] = {}  # 限月 → 商品 → 銘柄
        self.options: dict[tuple[str, str], list[jscc.BpfInstrument]] = {}  # (商品, 限月) → 銘柄
        self.option_periods: set[tuple[str, str]] = set()  # 清算値段によらず出てきた (商品, 限月)
        self.groups: set[str] = set()

    def add(self, inst: jscc.BpfInstrument) -> None:
        if inst.contract_type == "FUT":
            if inst.product_code in settings.JSCC_FACTOR_FUTURES:
                self.futures.setdefault(inst.contract_period_code, {})[inst.product_code] = inst
            return
        if inst.product_code not in settings.JSCC_FACTOR_PRODUCTS:
            return
        key = (inst.product_code, inst.contract_period_code)
        self.option_periods.add(key)
        if inst.price > FLOOR_UNIT_VALUE and inst.strike_price is not None and inst.put_call_type in ("C", "P"):
            self.options.setdefault(key, []).append(inst)
            self.groups.add(inst.aggregation_group)


@dataclass
class FactorResult:
    rows: list[dict]
    fitted: int  # g が空でない行
    skipped: dict[str, list[str]] = field(default_factory=dict)  # 理由 → ["商品:限月", ...]
    empty_scenarios: int = 0  # 点が 0 個で g_j = 0 にしたシナリオ（全限月の合計）


# ---- 当てはめ ----


def _parity_forward(pairs: list[tuple[float, float, float]], df: float) -> float | None:
    """pairs: (K, C, P)。C・P とも 1 円より大きい組のうち |C − P| が小さい 3 本の K + (C − P)/df の中央値"""
    ok = [(abs(c - p), k, c, p) for k, c, p in pairs if c > FLOOR_UNIT_VALUE and p > FLOOR_UNIT_VALUE]
    if not ok:
        return None
    best = heapq.nsmallest(PARITY_STRIKES, ok)
    return statistics.median(k + (c - p) / df for _, k, c, p in best)


def _parity(options: list[jscc.BpfInstrument], T0: float, T1: float, n: int, rate: float):
    """(F₀, [F′_j]) か None（基準かどれかのシナリオで組が無い）"""
    by_strike: dict[float, dict[str, jscc.BpfInstrument]] = {}
    for inst in options:
        by_strike.setdefault(float(inst.strike_price), {})[inst.put_call_type] = inst
    pairs = [(k, d["C"], d["P"]) for k, d in sorted(by_strike.items()) if "C" in d and "P" in d]
    if not pairs:
        return None
    f0 = _parity_forward([(k, c.price, p.price) for k, c, p in pairs], math.exp(-rate * T0))
    if f0 is None:
        return None
    df1 = math.exp(-rate * max(T1, 0.0))
    scaled = [(k, c.pnl, c.contract_size, p.pnl, p.contract_size) for k, c, p in pairs]
    fwd = []
    for j in range(n):
        f = _parity_forward([(k, cp[j] / cs, pp[j] / ps) for k, cp, cs, pp, ps in scaled], df1)
        if f is None:
            return None
        fwd.append(f)
    return f0, fwd


def _key(product: str, period: str) -> str:
    return f"{product}:{period}"


def _fit_month(
    options: list[jscc.BpfInstrument],
    *,
    F0: float,
    fwd: list[float],
    T0: float,
    T1: float,
    rate: float,
    is_hist: list[bool],
    stress: dict[str, tuple[float, int]],
    reference: dict[str, tuple[int, int]],
    short_floor: bool = True,
) -> dict:
    """σ₀・z・g・質の列。戻り値は {atm_iv, g, nodes, n_fit, fit_rmse, check_*, reason, empty}"""
    nodes = settings.JSCC_FACTOR_NODES
    nn = len(nodes)
    out = {
        "atm_iv": None,
        "nodes": [],
        "g": [],
        "n_fit": None,
        "fit_rmse": None,
        "check_n": None,
        "check_median_rel": None,
        "check_p90_rel": None,
        "reason": None,
        "empty": 0,
    }
    used = []  # (inst, is_call, K, σ₀, k)
    for inst in options:
        K = float(inst.strike_price)
        is_call = inst.put_call_type == "C"
        otm = K > F0 if is_call else K <= F0
        if not otm or inst.price < settings.JSCC_FACTOR_MIN_PRICE:
            continue
        s0 = implied_vol(is_call, F0, K, T0, inst.price, rate)
        if s0 is None:
            continue
        used.append((inst, is_call, K, s0, math.log(K / F0)))
    if used:
        used.sort(key=lambda u: (u[4], u[1]))
        atm = math.exp(interp(0.0, [u[4] for u in used], [math.log(u[3]) for u in used]))
        if math.isfinite(atm):
            out["atm_iv"] = atm
    if T1 <= 0:
        out["reason"] = T1_EXPIRED
        return out
    if len(used) < settings.JSCC_FACTOR_MIN_OPTIONS or out["atm_iv"] is None:
        out["reason"] = FEW_OPTIONS
        return out

    scale = out["atm_iv"] * math.sqrt(T0)
    basis = [hat_index(u[4] / scale, nodes) for u in used]
    ln_s0 = [math.log(u[3]) for u in used]
    n = len(fwd)
    g: list[float] = []
    sq_sum = 0.0
    n_points = 0
    empty = 0
    for j in range(n):
        Fj = fwd[j]
        points = []
        for i, (inst, is_call, K, s0, _) in enumerate(used):
            v = inst.pnl[j] / inst.contract_size
            if v <= FLOOR_UNIT_VALUE:
                continue
            s1 = implied_vol(is_call, Fj, K, T1, v, rate, s0)
            if s1 is None:
                continue
            jj, w = basis[i]
            points.append((jj, w, math.log(s1) - ln_s0[i]))
        if not points:
            empty += 1
        gj = fit_ridge(points, nn, settings.JSCC_FACTOR_RIDGE)
        for jj, w, y in points:
            e = y - ((1.0 - w) * gj[jj] + w * gj[jj + 1])
            sq_sum += e * e
        n_points += len(points)
        g.extend(gj)
    rmse = math.sqrt(sq_sum / n_points) if n_points else None
    if not all(math.isfinite(x) for x in g) or (rmse is not None and not math.isfinite(rmse)):
        out["reason"] = NONFINITE_G
        return out

    # check: 基準の状態（F₀・σ₀）に r・g を当てた 1 枚の売りの ES と All-Instruments-VaR の 1 枚の売り
    rel = []
    growth = [Fj / F0 for Fj in fwd]
    for i, (inst, is_call, K, s0, _) in enumerate(used):
        ref = reference.get(inst.instrument_code)
        if ref is None or ref[0] is None or ref[0] <= 0:
            continue
        jj, w = basis[i]
        cs = inst.contract_size
        values = []
        for j in range(n):
            gz = (1.0 - w) * g[j * nn + jj] + w * g[j * nn + jj + 1]
            v = black76(is_call, F0 * growth[j], K, T1, s0 * math.exp(gz), rate)
            values.append(max(1, math.floor(v + 0.5)) * cs)
        confidence, n_stress = stress[inst.aggregation_group]
        es = jscc.margin_one_lot(
            values,
            is_hist,
            side=-1,
            confidence=confidence,
            n_stress=n_stress,
            floor=jscc.option_short_floor(inst) if short_floor else 0,
        )
        rel.append(abs(es - ref[0]) / ref[0])
    out.update(
        nodes=list(nodes),
        g=g,
        n_fit=len(used),
        fit_rmse=rmse,
        check_n=len(rel),  # 0 なら check_median_rel・check_p90_rel は NULL
        check_median_rel=quantile(rel, 0.5),
        check_p90_rel=quantile(rel, 0.9),
        empty=empty,
    )
    return out


def factor_rows(
    collector: FactorCollector,
    *,
    scenarios: Sequence[tuple[str, str]],
    stress: dict[str, tuple[float, int]],
    reference: dict[str, tuple[int, int]],
    business_date: date,
    holidays: frozenset[date],
    file_version: str,
    run_id: str,
    rate: float,
    check: Callable[[], None] = lambda: None,
) -> FactorResult:
    """限月ごとの行（schema/tokyo/jscc_var_scenario_factor.json の列と 1 対 1。(product_code, contract_period_code)
    の順）。行が無い・g が空の限月は skipped に理由ごと。check は限月ごとに呼ぶ（期限。例外はそのまま上げる）"""
    n = len(scenarios)
    is_hist = [t == "H" for _, t in scenarios]
    h = horizon_days(business_date, holidays, settings.JSCC_FACTOR_HORIZON_BUSINESS_DAYS)
    monthly = settings.JSCC_FACTOR_MONTHLY_PRODUCT
    result = FactorResult(rows=[], fitted=0)

    def skip(reason: str, product: str, period: str) -> None:
        result.skipped.setdefault(reason, []).append(_key(product, period))

    groups: list[tuple[str, str, str | None]] = []  # (商品, 限月, 先物の商品 / None)
    monthly_periods = {period for product, period in collector.option_periods if product == monthly}
    for product, period in sorted(collector.option_periods):
        fut = None
        if product == monthly:
            futs = collector.futures.get(period, {})
            fut = next((p for p in settings.JSCC_FACTOR_FUTURES if p in futs), None)
        groups.append((product, period, fut))
    for period, futs in sorted(collector.futures.items()):
        if period not in monthly_periods:
            fut = next(p for p in settings.JSCC_FACTOR_FUTURES if p in futs)
            groups.append((fut, period, fut))
    groups.sort(key=lambda x: (x[0], x[1]))

    for product, period, fut in groups:
        check()
        sq = sq_date(product, period, holidays)
        if sq is None:
            skip(NO_SQ, product, period)
            continue
        T0 = t_years(sq, business_date)
        if T0 <= 0:
            skip(EXPIRED, product, period)
            continue
        T1 = T0 - h / 365
        options = collector.options.get((product, period), [])
        if fut is not None:
            finst = collector.futures[period][fut]
            F0 = finst.price
            fwd = [F0 + v / finst.contract_size for v in finst.pnl]
            source = fut
        else:
            par = _parity(options, T0, T1, n, rate)
            if par is None:
                skip(NO_PARITY, product, period)
                continue
            F0, fwd = par
            source = "parity"
        if not (math.isfinite(F0) and F0 > 0 and all(math.isfinite(f) and f > 0 for f in fwd)):
            skip(NONFINITE_R, product, period)
            continue
        r = [math.log(f / F0) for f in fwd]
        if len(r) != n or not all(math.isfinite(x) for x in r):
            skip(NONFINITE_R, product, period)
            continue
        row = {
            "business_date": business_date.isoformat(),
            "file_version": file_version,
            "jscc_run_id": run_id,
            "product_code": product,
            "contract_period_code": period,
            "forward_source": source,
            "forward": F0,
            "t_years": T0,
            "horizon_days": h,
            "rate": rate,
            "atm_iv": None,
            "nodes": [],
            "r": r,
            "g": [],
            "n_fit": None,
            "fit_rmse": None,
            "check_n": None,
            "check_median_rel": None,
            "check_p90_rel": None,
        }
        if product in settings.JSCC_FACTOR_PRODUCTS:
            fit = _fit_month(
                options,
                F0=F0,
                fwd=fwd,
                T0=T0,
                T1=T1,
                rate=rate,
                is_hist=is_hist,
                stress=stress,
                reference=reference,
                short_floor=business_date >= settings.JSCC_OPTION_SHORT_FLOOR_FROM,
            )
            if fit["reason"]:
                skip(fit["reason"], product, period)
            result.empty_scenarios += fit.pop("empty")
            fit.pop("reason")
            row.update(fit)
            if row["g"]:
                result.fitted += 1
        result.rows.append(row)
    return result

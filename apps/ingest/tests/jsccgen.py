"""テスト用の JSCC の VaR 証拠金計算用パラメータファイル（#164）の組み立て。値はすべて合成（実ファイルを切り出さない）。

- BPF_IDX（zip の中の CSV）: レコード 0 DATE → 1 INSTRUMENT → 2 PRICE → 3 HSPL → 6 SFR → 7 ASPL の順。
  ブロックの前に # の見出し行、改行は CRLF（仕様の形）
- VaRParameter: レコード 0（VAR）・1（HSRATIO）・2（ASRATIO）
- All-Instruments-VaR: 見出し 1 行 + 銘柄ごとの 1 枚の売り・買い
- 一覧の HTML（Apache 風の <a href="ファイル名">）
"""

import io
import math
import random
import zipfile
from dataclasses import dataclass, field
from datetime import date
from statistics import NormalDist

BPF_HEADERS = {
    "0": "#Record ID,Record Name,Date,File Type",
    "1": (
        "#Record ID,Record Name,Instrument Code,Product Group Code,Product Code,Contract Type,Put/Call Type,"
        "Contract Period Code,Underlying Period Code,Strike Price,Contract Size,Aggregation Group,"
        "Calculation Method Flag"
    ),
    "2": "#Record ID,Record Name,Instrument Code,Price",
    "3": "#Record ID,Record Name,Instrument Code,Scenario ID,Scenario Type,Unit PnL",
    "6": "#Record ID,Record Name,Product Group Code,Spread Fluctuation Risk",
    "7": "#Record ID,Record Name,Instrument Code,Scenario ID,Unit PnL,Scenario Delta",
}

# シナリオの並び（H が先、S が後。H と S で同じ ID があるのは実ファイルと同じ）
SCENARIOS = (
    ("20240101", "H"),
    ("20240102", "H"),
    ("20240103", "H"),
    ("20240104", "H"),
    ("20240101", "S"),
    ("20240105", "S"),
    ("20240106", "S"),
)


@dataclass
class Inst:
    code: str
    group: str = "NK225"
    product: str = "NK225E"
    contract_type: str = "OOP"
    put_call: str = "C"
    period: str = "202612"
    underlying: str = "202612"
    strike: str = "30000.0000"
    size: int = 1000
    agg: str = "IDX_IDX"
    flag: str = "H"
    price: str = "10.0"
    pnl: list[int] | None = None  # None ならシナリオの数だけ合成
    scenarios: tuple | None = None  # None なら SCENARIOS

    def instrument_line(self):
        return ",".join(
            [
                "1",
                "INSTRUMENT",
                self.code,
                self.group,
                self.product,
                self.contract_type,
                self.put_call,
                self.period,
                self.underlying,
                self.strike,
                str(self.size),
                self.agg,
                self.flag,
            ]
        )

    def values(self):
        scen = self.scenarios or SCENARIOS
        if self.pnl is not None:
            return list(self.pnl)
        seed = sum(ord(c) for c in self.code)
        return [(seed * (i + 3)) % 97 - 40 for i in range(len(scen))]


def future(code, **kw):
    kw.setdefault("product", "NK225F")
    return Inst(code, contract_type="FUT", put_call="", underlying="", strike="", **kw)


def default_instruments():
    """NK225 の 4 銘柄（OP の C・P・週次・先物）と、他のグループの 2 銘柄（うち 1 つは計算方式 A）"""
    return [
        Inst("100000001", put_call="C"),
        Inst("100000002", put_call="P", strike="29500.0000", price="24.0"),
        Inst("100000003", product="NK225W2", period="2026122", strike="31250.0000"),
        future("100000004", size=1000, price="38000.0"),
        Inst("200000001", group="TOPIX", product="TOPIXE", agg="IDX_IDX"),
        Inst("200000002", group="JGB", product="JGBL", contract_type="FUT", put_call="", flag="A", agg="JGB"),
    ]


@dataclass
class Bpf:
    business_date: date = date(2026, 11, 20)
    instruments: list[Inst] = field(default_factory=default_instruments)
    date_text: str | None = None  # DATE レコードの日付（None なら business_date）
    file_type: str = "S"
    extra_date_record: bool = False
    # HSPL のブロック（銘柄コード, 開始の添字, 終わりの添字）。None なら計算方式 H の銘柄ごとに全シナリオ
    hspl_blocks: list[tuple[str, int, int]] | None = None
    omit_price: tuple = ()
    instrument_after_hspl: Inst | None = None

    def lines(self):
        ymd = self.date_text or self.business_date.strftime("%Y%m%d")
        out = [BPF_HEADERS["0"], f"0,DATE,{ymd},{self.file_type}"]
        if self.extra_date_record:
            out.append(f"0,DATE,{ymd},{self.file_type}")
        out.append(BPF_HEADERS["1"])
        out += [i.instrument_line() for i in self.instruments]
        out.append(BPF_HEADERS["2"])
        out += [f"2,PRICE,{i.code},{i.price or ''}" for i in self.instruments if i.code not in self.omit_price]
        out.append(BPF_HEADERS["3"])
        by_code = {i.code: i for i in self.instruments}
        blocks = self.hspl_blocks
        if blocks is None:
            blocks = [(i.code, 0, len(i.scenarios or SCENARIOS)) for i in self.instruments if i.flag == "H"]
        for code, start, end in blocks:
            inst = by_code[code]
            scen = inst.scenarios or SCENARIOS
            values = inst.values()
            for k in range(start, end):
                sid, stype = scen[k]
                out.append(f"3,HSPL,{code},{sid},{stype},{values[k]}")
        if self.instrument_after_hspl is not None:
            out.append(self.instrument_after_hspl.instrument_line())
        out.append(BPF_HEADERS["6"])
        out.append("6,SFR,JGB,100")
        out.append(BPF_HEADERS["7"])
        out += [f"7,ASPL,{i.code},#01,5,0.5" for i in self.instruments if i.flag == "A"]
        return [line + "\r\n" for line in out]

    def csv_text(self):
        return "".join(self.lines())

    def zip_bytes(self, member=None, extra_members=()):
        name = member or f"BPF_IDX_{self.business_date:%Y%m%d}_1600.csv"
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
            z.writestr(name, self.csv_text().encode("utf-8"))
            for extra in extra_members:
                z.writestr(extra, b"x")
        return buf.getvalue()


def var_parameter(confidence="97.5", stress="2", extra=()):
    """VaRParameter の CSV（IDX_IDX の行を含む）"""
    lines = [
        "#Record ID,Record Name,Level,Aggregation Group,Confidence Level,Stress Scenario Number",
        f"0,VAR,L01,IDX,{confidence},{stress}",
        f"0,VAR,L02,IDX_IDX,{confidence},{stress}",
        "0,VAR,L01,JGB,99.0,3",
        "#Record ID,Record Name,Level,Aggregation Group,Parent Aggregation Group,Type,Number of Param,"
        "Param Name 01,Param Value 01,Param Name 02,Param Value 02",
        "1,HSRATIO,L01,IDX,,OFFSET_LIMIT,2,a,0.5,b,0.25",
        "1,HSRATIO,L02,IDX_IDX,IDX,GROUP,0,,,,",
        "#Record ID,Record Name,Level,Aggregation Group,Parent Aggregation Group,Type,Number of Param,"
        "Param Name 01,Param Value 01,Param Name 02,Param Value 02",
        "2,ASRATIO,L01,JGB,,OFFSET_LIMIT,1,a,1.0,,",
        *extra,
    ]
    return "".join(line + "\r\n" for line in lines)


AIV_HEADER = (
    "Instrument Code,Product Group Code,Product Code,Product Type,Put/Call,Contract Month,Strike Price,"
    "Margin for 1 Short,Margin for 1 Long"
)


def all_instruments_var(margins: dict[str, tuple[int, int]], group="NK225", extra_lines=()):
    lines = [AIV_HEADER]
    for code, (short, long) in margins.items():
        lines.append(f"{code},{group},NK225E,Option,C,202612,30000,{short},{long}")
    lines.append("900000001,TOPIX,TOPIXF,Future,-,202612,-,123,456")
    lines += list(extra_lines)
    return "".join(line + "\r\n" for line in lines)


def listing_html(names, extra_hrefs=()):
    rows = "".join(f'<tr><td><a href="{n}">{n}</a></td><td>2026-11-20 16:05</td></tr>\n' for n in names)
    extra = "".join(f'<a href="{h}">x</a>\n' for h in extra_hrefs)
    return f"<html><body><table>\n{rows}</table>\n{extra}</body></html>"


def day_names(d: date, version="1600"):
    ymd = d.strftime("%Y%m%d")
    return [
        f"BPF_IDX_{ymd}_{version}.zip",
        f"VaRParameter_{ymd}_{version}.csv",
        f"All-Instruments-VaR_{ymd}_{version}.csv",
    ]


# ---- シナリオの r・g の当てはめ（#168）の合成: 既知の r・g から Black-76 で pnl を作る限月 ----

FACTOR_NODES = (-5.0, -4.0, -3.0, -2.0, -1.5, -1.0, -0.5, 0.0, 0.5, 1.0, 1.5, 2.0, 3.0)


def _ncdf(x):
    return NormalDist().cdf(x)


def b76(is_call, F, K, T, sigma, rate):
    """テスト側の Black-76（statistics.NormalDist。本体の実装とは別に書く）"""
    df = math.exp(-rate * max(T, 0.0))
    if T <= 0 or sigma <= 0:
        return df * max(0.0, (F - K) if is_call else (K - F))
    sd = sigma * math.sqrt(T)
    d1 = math.log(F / K) / sd + 0.5 * sd
    d2 = d1 - sd
    if is_call:
        return df * (F * _ncdf(d1) - K * _ncdf(d2))
    return df * (K * _ncdf(-d2) - F * _ncdf(-d1))


def _interp(x, xs, ys):
    if x <= xs[0]:
        return ys[0]
    if x >= xs[-1]:
        return ys[-1]
    for i in range(1, len(xs)):
        if x <= xs[i]:
            return ys[i - 1] + (ys[i] - ys[i - 1]) * (x - xs[i - 1]) / (xs[i] - xs[i - 1])
    raise AssertionError


def _hat(z, nodes=FACTOR_NODES):
    """z の折れ線の重み（節点ごと）。端で切る"""
    z = min(max(z, nodes[0]), nodes[-1])
    w = [0.0] * len(nodes)
    for m in range(len(nodes) - 1):
        if nodes[m] <= z <= nodes[m + 1]:
            t = (z - nodes[m]) / (nodes[m + 1] - nodes[m])
            w[m], w[m + 1] = 1 - t, t
            return w
    raise AssertionError


@dataclass
class FactorMonth:
    """合成した限月: 銘柄と、作るのに使った正解（r・g・F₀・σ_ATM・使う OP・節点ごとの重みの和）"""

    instruments: list
    product: str
    period: str
    forward: float
    r: list
    g: list  # シナリオごとに節点 13 個
    atm_iv: float
    used: list  # 使う OP の銘柄コード
    node_weight: list  # シナリオごと・節点ごとの、下限より上の点の hat の重みの和（小さい節点は当てはめで決まらない）
    t0: float
    t1: float


def factor_month(
    *,
    business_date=date(2026, 11, 20),
    sq=date(2026, 12, 11),
    h_days=5,
    product="NK225E",
    period="202612",
    future_product="NK225F",
    F0=50000.0,
    strikes=None,
    scenarios=SCENARIOS,
    seed=1,
    rate=0.005,
    cs=1000,
    fut_cs=1000,
    code_start=500000001,
    r_scale=0.03,
    g_level=0.1,
    g_noise=0.02,
    smile=(-1.6094379124341003, -0.8, 1.5),  # ln σ₀(k) = a + b·k + c·k²（a = ln 0.2）
    overrides=None,
):
    """既知の r（先物の pnl から）・g（シナリオごとに節点の値を乱数）で OP の pnl を Black-76 から作る。
    清算値段は丸めない（σ₀ が正確に戻る）。OP の pnl は 1 単位の値を 1/cs 円の刻みに丸め、1 円を下限にする。
    future_product=None なら先物を作らない（F′ はパリティで戻る）。overrides: {(銘柄コード, シナリオの添字): 1 単位の値}
    で点の値を置き換える（1 円の下限・逆算できない値の除外の確かめ）"""
    rnd = random.Random(seed)
    n = len(scenarios)
    t0 = ((sq - business_date).days - 6.75 / 24) / 365
    t1 = t0 - h_days / 365
    if strikes is None:
        strikes = [round(F0 * (0.70 + 0.005 * i) / 250) * 250 for i in range(91)]
    insts = []
    codes = iter(range(code_start, code_start + 10**6))
    if future_product:
        fut_pnl = [round(F0 * rnd.gauss(0, r_scale) * fut_cs) for _ in range(n)]
        fwd = [F0 + v / fut_cs for v in fut_pnl]
        insts.append(
            future(
                str(next(codes)),
                product=future_product,
                period=period,
                size=fut_cs,
                price=repr(F0),
                pnl=fut_pnl,
                scenarios=scenarios,
            )
        )
    else:
        fwd = [F0 * math.exp(rnd.gauss(0, r_scale)) for _ in range(n)]
    r = [math.log(f / F0) for f in fwd]
    g = []
    for _ in range(n):
        level, slope = rnd.gauss(0, g_level), rnd.gauss(0, g_level / 2)
        g.append([level + slope * x / 3 + rnd.gauss(0, g_noise) for x in FACTOR_NODES])
    a, b, c = smile

    def sigma0(K):
        k = math.log(K / F0)
        return math.exp(a + b * k + c * k * k)

    options = []
    for K in sorted(set(strikes)):
        for pc in ("C", "P"):
            is_call = pc == "C"
            p0 = max(1.0, b76(is_call, F0, K, t0, sigma0(K), rate))
            options.append((str(next(codes)), is_call, K, p0))
    used = [(code, K) for code, is_call, K, p0 in options if (K > F0 if is_call else K <= F0) and p0 >= 5]
    used.sort(key=lambda u: u[1])
    ks = [math.log(K / F0) for _, K in used]
    atm = math.exp(_interp(0.0, ks, [math.log(sigma0(K)) for _, K in used])) if used else float("nan")
    z = {code: math.log(K / F0) / (atm * math.sqrt(t0)) for code, K in used}
    node_weight = [[0.0] * len(FACTOR_NODES) for _ in range(n)]
    overrides = overrides or {}
    for code, is_call, K, p0 in options:
        zi = z.get(code, math.log(K / F0) / (atm * math.sqrt(t0)) if used else 0.0)
        hw = _hat(zi)
        pnl = []
        for j in range(n):
            gz = sum(w * gm for w, gm in zip(hw, g[j], strict=True))
            v = b76(is_call, fwd[j], K, t1, sigma0(K) * math.exp(gz), rate)
            units = overrides.get((code, j), max(1.0, round(v * cs) / cs))
            pnl.append(round(units * cs))
            if code in z and units > 1:
                for m, w in enumerate(hw):
                    node_weight[j][m] += w
        insts.append(
            Inst(
                code,
                product=product,
                put_call="C" if is_call else "P",
                period=period,
                underlying=period[:6],
                strike=f"{K:.4f}",
                size=cs,
                price=repr(p0),
                pnl=pnl,
                scenarios=scenarios,
            )
        )
    return FactorMonth(
        instruments=insts,
        product=product,
        period=period,
        forward=F0,
        r=r,
        g=g,
        atm_iv=atm,
        used=[code for code, _ in used],
        node_weight=node_weight,
        t0=t0,
        t1=t1,
    )


def many_scenarios(n_h=40, n_s=6):
    """シナリオの並び（H n_h 本 + S n_s 本）"""
    return tuple((f"2024{i:04d}", "H") for i in range(n_h)) + tuple((f"2025{i:04d}", "S") for i in range(n_s))

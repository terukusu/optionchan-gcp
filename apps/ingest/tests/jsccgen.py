"""テスト用の JSCC の VaR 証拠金計算用パラメータファイル（#164）の組み立て。値はすべて合成（実ファイルを切り出さない）。

- BPF_IDX（zip の中の CSV）: レコード 0 DATE → 1 INSTRUMENT → 2 PRICE → 3 HSPL → 6 SFR → 7 ASPL の順。
  ブロックの前に # の見出し行、改行は CRLF（仕様の形）
- VaRParameter: レコード 0（VAR）・1（HSRATIO）・2（ASRATIO）
- All-Instruments-VaR: 見出し 1 行 + 銘柄ごとの 1 枚の売り・買い
- 一覧の HTML（Apache 風の <a href="ファイル名">）
"""

import io
import zipfile
from dataclasses import dataclass, field
from datetime import date

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

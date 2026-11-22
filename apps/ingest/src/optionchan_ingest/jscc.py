"""JSCC の VaR 証拠金計算用パラメータファイル（#164）の解析と、1 枚の証拠金の計算（純粋関数。I/O は jscc_sync.py）。

ファイル（JSCC の「VaR 証拠金計算用パラメータファイル」。仕様は JSCC の GUI アプリケーション仕様書）:
- BPF_IDX_YYYYMMDD_<版>.zip: 1 メンバーの CSV。レコード 0 DATE → 1 INSTRUMENT → 2 PRICE → 3 HSPL（銘柄 × シナリオの
  1 枚あたりの値）→ 6 SFR → 7 ASPL の順で、ブロックの前に # の見出し行。展開すると 800MB 超なので行を順に読む
- VaRParameter_YYYYMMDD_<版>.csv: Aggregation Group ごとの信頼水準・ストレスのシナリオの本数（レコード 0）と
  相殺の係数（レコード 1・2）
- All-Instruments-VaR_YYYYMMDD_<版>.csv: 銘柄ごとの 1 枚の売り・買いの証拠金（照合の基準）

1 枚の証拠金（margin_one_lot）: v = 売買の向き × 値、H の全部 + S の損失の大きい方から n 本の期待ショートフォール
（信頼水準 97.5% なら損失の大きい方から 2.5%）を円に切り上げ、0 未満は 0。OP の売りは清算値段 × Contract Size を
下回らない（2026-11-16〜20 の All-Instruments-VaR と全件一致。Phase 2 の実測）。

例外は JsccFormatError（ValueError の派生）だけ。メッセージに価格・損益の値を入れない（行番号・列名・銘柄コードまで）
"""

import csv
import io
import math
import re
from array import array
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from fractions import Fraction
from urllib.parse import urljoin


class JsccFormatError(ValueError):
    """ファイルの形・値が想定と違う"""


# ---- 一覧のページ ----

KIND_BY_PREFIX = {
    "BPF_IDX": ("bpf", "zip"),
    "VaRParameter": ("parameter", "csv"),
    "All-Instruments-VaR": ("margin", "csv"),
}
_HREF_RE = re.compile(r"""<a\s[^>]*?href\s*=\s*["']([^"']*)["']""", re.I)
_NAME_RE = re.compile(r"^(BPF_IDX|VaRParameter|All-Instruments-VaR)_(\d{8})_(\d{4})\.(zip|csv)$")


def parse_listing(html: str, page_url: str, version: str = "1600") -> dict[date, dict[str, str]]:
    """一覧のページ → {営業日: {"bpf" / "parameter" / "margin": 絶対 URL}}。

    href がファイル名だけ（page_url と同じディレクトリ）で、名前が 3 種類のどれかの形・版が version のものだけ。
    形の違うページは空の dict（例外にしない）
    """
    out: dict[date, dict[str, str]] = {}
    for href in _HREF_RE.findall(html or ""):
        m = _NAME_RE.match(href)  # / ? # : を含むもの（絶対 URL・別のディレクトリ）はここで外れる
        if not m:
            continue
        prefix, ymd, ver, ext = m.groups()
        kind, expected_ext = KIND_BY_PREFIX[prefix]
        if ver != version or ext != expected_ext:
            continue
        try:
            day = date(int(ymd[:4]), int(ymd[4:6]), int(ymd[6:]))
        except ValueError:
            continue
        out.setdefault(day, {})[kind] = urljoin(page_url, href)
    return out


# ---- BPF ----

INSTRUMENT_COLUMNS = 13
PRICE_COLUMNS = 4
HSPL_COLUMNS = 6
DATE_COLUMNS = 4
FILE_TYPE = "S"
METHOD_HIST = "H"
SCENARIO_TYPES = ("H", "S")


@dataclass(frozen=True)
class BpfInstrument:
    instrument_code: str
    product_group_code: str
    product_code: str
    contract_type: str
    put_call_type: str | None
    contract_period_code: str
    underlying_period_code: str | None
    strike_price: Decimal | None
    contract_size: int
    aggregation_group: str
    calculation_method_flag: str
    price: float
    pnl: array  # array('q')。シナリオの並び（BpfSummary.scenarios）の順
    price_text: str = ""  # 清算値段の元の文字列（OP の売りの下限を Decimal で計算する）


@dataclass(frozen=True)
class BpfSummary:
    file_type: str
    scenarios: tuple[tuple[str, str], ...]  # (Scenario ID, Scenario Type)
    instruments: int


def _fail(line_no: int, reason: str):
    raise JsccFormatError(f"{line_no} 行目: {reason}")


class BpfReader:
    """BPF の CSV の行を順に読み、product_group の銘柄を 1 つずつ返す（HSPL の出現順）。

    読み終わると summary が入る（それまでは None）。scenarios は最初の銘柄を返した時点で入る。
    持つのは product_group の INSTRUMENT・PRICE の表と今の 1 銘柄の値だけ（全銘柄の値を溜めない）
    """

    def __init__(self, lines: Iterable[str], *, product_group: str, business_date: date):
        self._lines = lines
        self._group = product_group
        self._date = business_date
        self.summary: BpfSummary | None = None
        self.scenarios: tuple[tuple[str, str], ...] | None = None

    def __iter__(self) -> Iterator[BpfInstrument]:
        group = self._group
        attrs: dict[str, tuple] = {}  # 銘柄コード → INSTRUMENT の列（product_group だけ）
        prices: dict[str, tuple[float, str]] = {}
        file_type = None
        hspl_started = False
        done: set[str] = set()
        cur = None
        cur_pnl = None
        scen_ids: list[str] | None = None
        scen_types: list[str] | None = None
        first_code = None
        k = 0
        count = 0
        line_no = 0

        def finish(code, pnl, end_line):
            if pnl is None:
                return None
            if scen_ids is not None and code != first_code and len(pnl) != len(scen_ids):
                _fail(end_line, f"銘柄 {code} の HSPL のシナリオの並びが最初の銘柄と違う（本数）")
            if code not in prices:
                _fail(end_line, f"銘柄 {code} の PRICE が無い")
            a = attrs[code]
            price, price_text = prices[code]
            return BpfInstrument(*a, price=price, pnl=pnl, price_text=price_text)

        for raw in self._lines:
            line_no += 1
            line = raw.rstrip("\r\n")
            if not line or line[0] == "#":
                continue
            rid = line[0]
            if rid == "3" and line.startswith("3,"):
                hspl_started = True
                if file_type is None:
                    _fail(line_no, "DATE レコードが無い")
                head = line.split(",", 3)
                if len(head) < 4:
                    _fail(line_no, "HSPL の列の数が違う")
                code = head[2]
                if code != cur:
                    if code not in attrs:
                        continue
                    if cur is not None:
                        inst = finish(cur, cur_pnl, line_no)
                        done.add(cur)
                        if self.scenarios is None:
                            self.scenarios = tuple(zip(scen_ids, scen_types, strict=True))
                        count += 1
                        cur_pnl = None
                        yield inst
                    if code in done:
                        _fail(line_no, f"銘柄 {code} の HSPL が連続していない")
                    cur = code
                    cur_pnl = array("q")
                    k = 0
                    if first_code is None:
                        first_code = code
                        scen_ids, scen_types = [], []
                rest = head[3].split(",")
                if len(rest) != 3:
                    _fail(line_no, "HSPL の列の数が違う")
                sid, stype, value = rest
                if stype not in SCENARIO_TYPES:
                    _fail(line_no, f"Scenario Type が H・S 以外（銘柄 {code}）")
                try:
                    cur_pnl.append(int(value))
                except (ValueError, OverflowError):
                    _fail(line_no, f"Unit PnL が整数でない（銘柄 {code}）")
                if code == first_code:
                    scen_ids.append(sid)
                    scen_types.append(stype)
                elif k >= len(scen_ids) or scen_ids[k] != sid or scen_types[k] != stype:
                    _fail(line_no, f"銘柄 {code} の HSPL のシナリオの並びが最初の銘柄と違う")
                k += 1
                continue
            parts = line.split(",")
            if rid == "0" and parts[0] == "0":
                if file_type is not None:
                    _fail(line_no, "DATE レコードが 2 つ以上")
                if len(parts) != DATE_COLUMNS:
                    _fail(line_no, "DATE レコードの列の数が違う")
                if parts[2] != self._date.strftime("%Y%m%d"):
                    _fail(line_no, "DATE レコードの日付がファイル名の営業日と違う")
                if parts[3] != FILE_TYPE:
                    _fail(line_no, f"File Type が {FILE_TYPE} でない")
                file_type = parts[3]
            elif parts[0] == "1":
                if hspl_started:
                    _fail(line_no, "INSTRUMENT が HSPL の後に出た")
                if file_type is None:
                    _fail(line_no, "DATE レコードが無い")
                if len(parts) != INSTRUMENT_COLUMNS:
                    _fail(line_no, "INSTRUMENT の列の数が違う")
                if parts[3] != group:
                    continue
                if parts[2] in attrs:
                    _fail(line_no, f"銘柄 {parts[2]} の INSTRUMENT が 2 つ以上")
                attrs[parts[2]] = self._instrument(parts, line_no)
            elif parts[0] == "2":
                if hspl_started:
                    _fail(line_no, "PRICE が HSPL の後に出た")
                if len(parts) != PRICE_COLUMNS:
                    _fail(line_no, "PRICE の列の数が違う")
                code = parts[2]
                if code not in attrs:
                    continue
                text = parts[3]
                if not text:
                    _fail(line_no, f"銘柄 {code} の PRICE が空")
                try:
                    price = float(Decimal(text))
                except (InvalidOperation, ValueError):
                    _fail(line_no, f"Price が読めない（銘柄 {code}）")
                if not math.isfinite(price):
                    _fail(line_no, f"Price が読めない（銘柄 {code}）")
                if code in prices:
                    _fail(line_no, f"銘柄 {code} の PRICE が 2 つ以上")
                prices[code] = (price, text)
            # 6（SFR）・7（ASPL）とほかのレコードは読み飛ばす
        if cur is not None:
            inst = finish(cur, cur_pnl, line_no)
            done.add(cur)
            if self.scenarios is None:
                self.scenarios = tuple(zip(scen_ids, scen_types, strict=True))
            count += 1
            cur_pnl = None
            yield inst
        if file_type is None:
            _fail(line_no, "DATE レコードが無い")
        missing = sorted(set(attrs) - done)
        if missing:
            _fail(line_no, f"HSPL の無い銘柄がある（{len(missing)} 件。先頭 {missing[:5]}）")
        if count == 0:
            _fail(line_no, f"Product Group {group} の銘柄が無い")
        self.summary = BpfSummary(file_type=file_type, scenarios=self.scenarios, instruments=count)

    def _instrument(self, p: list[str], line_no: int) -> tuple:
        code = p[2]
        if p[12] != METHOD_HIST:
            _fail(line_no, f"銘柄 {code} の計算方式が {METHOD_HIST} でない（HSPL を持たない）")
        try:
            size = int(p[10])
        except ValueError:
            _fail(line_no, f"Contract Size が読めない（銘柄 {code}）")
        strike = None
        if p[9]:
            try:
                strike = Decimal(p[9])
            except InvalidOperation:
                _fail(line_no, f"Strike Price が読めない（銘柄 {code}）")
            if not strike.is_finite():
                _fail(line_no, f"Strike Price が読めない（銘柄 {code}）")
        return (code, p[3], p[4], p[5], p[6] or None, p[7], p[8] or None, strike, size, p[11], p[12])


# ---- VaRParameter ----


@dataclass(frozen=True)
class ParamRecord:
    line_no: int
    record_id: int
    record_name: str
    level: str
    aggregation_group: str
    parent_aggregation_group: str | None
    type: str | None
    confidence_level: float | None
    stress_scenario_number: int | None
    params: tuple[tuple[str, float], ...]


def _lines(text: str) -> Iterator[tuple[int, list[str]]]:
    """# の見出し行・空行を除いた行を (1 始まりの番号, 列) で"""
    n = 0
    for raw in (text or "").lstrip("﻿").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        n += 1
        yield n, line.split(",")


def parse_var_parameter(text: str) -> list[ParamRecord]:
    records = []
    for n, p in _lines(text):
        if p[0] not in ("0", "1", "2"):
            _fail(n, "Record ID が 0・1・2 以外")
        rid = int(p[0])
        if rid == 0:
            if len(p) != 6:
                _fail(n, "レコード 0 の列の数が違う")
            if not p[3]:
                _fail(n, "Aggregation Group が空")
            try:
                conf, stress = float(p[4]), int(p[5])
            except ValueError:
                _fail(n, "Confidence Level・Stress Scenario Number が読めない")
            if not math.isfinite(conf):
                _fail(n, "Confidence Level が読めない")
            records.append(ParamRecord(n, 0, p[1], p[2], p[3], None, None, conf, stress, ()))
            continue
        if len(p) < 7:
            _fail(n, f"レコード {rid} の列の数が足りない")
        if not p[3]:
            _fail(n, "Aggregation Group が空")
        try:
            count = int(p[6])
        except ValueError:
            _fail(n, "Number of Param が読めない")
        values = p[7:]
        if count < 0 or len(values) < 2 * count:
            _fail(n, "Number of Param と値の数が合わない")
        if any(v.strip() for v in values[2 * count :]):
            _fail(n, "Number of Param より多い値がある")
        params = []
        for i in range(count):
            name, value = values[2 * i], values[2 * i + 1]
            try:
                v = float(value)
            except ValueError:
                _fail(n, f"Param Value が読めない（{name}）")
            if not name or not math.isfinite(v):
                _fail(n, "Param Name が空か、Param Value が読めない")
            params.append((name, v))
        records.append(ParamRecord(n, rid, p[1], p[2], p[3], p[4] or None, p[5] or None, None, None, tuple(params)))
    if not records:
        raise JsccFormatError("VaRParameter に行が無い")
    return records


def stress_params(records: list[ParamRecord], aggregation_group: str) -> tuple[float, int]:
    """(信頼水準（% 表記）, ストレスの本数)。レコード 0 の aggregation_group の行がちょうど 1 つ"""
    rows = [r for r in records if r.record_id == 0 and r.aggregation_group == aggregation_group]
    if len(rows) != 1:
        raise JsccFormatError(f"VaRParameter のレコード 0 に Aggregation Group {aggregation_group} が {len(rows)} 行")
    return rows[0].confidence_level, rows[0].stress_scenario_number


# ---- 1 枚の証拠金 ----


def margin_one_lot(
    pnl: Sequence[int], is_hist: Sequence[bool], *, side: int, confidence: float, n_stress: int, floor: int = 0
) -> int:
    """1 枚の証拠金（円）= max(0, ceil(ES), floor)。端数は分数で計算する（浮動小数の誤差で 1 円ずれない）"""
    if len(pnl) != len(is_hist):
        raise ValueError("pnl と is_hist の長さが違う")
    if side not in (-1, 1):
        raise ValueError("side は -1（売り）か 1（買い）")
    if not 0 < confidence < 100:
        raise ValueError("confidence は 0 より大きく 100 より小さい")
    hist = [side * v for v, h in zip(pnl, is_hist, strict=True) if h]
    stress = sorted(side * v for v, h in zip(pnl, is_hist, strict=True) if not h)
    if not hist:
        raise ValueError("H のシナリオが無い")
    if n_stress < 0 or len(stress) < n_stress:
        raise ValueError("S のシナリオが n_stress 本より少ない")
    xs = sorted(hist + stress[:n_stress])
    n = len(xs)
    # confidence は 1/10000 % 単位の有理数として扱う（97.5 → 1 − 0.975 = 1/40）
    tail = 1 - Fraction(Decimal(repr(confidence)).quantize(Decimal("0.0001"))) / 100
    t = n * tail
    k = math.floor(t)
    total = Fraction(sum(xs[:k]))
    if t > k:
        total += (t - k) * xs[k]
    es = -total / t
    return max(0, math.ceil(es), floor)


def option_short_floor(inst: BpfInstrument) -> int:
    """OP の売りの下限 = ceil(清算値段 × Contract Size)。先物は 0"""
    if inst.contract_type == "FUT":
        return 0
    return math.ceil(Decimal(inst.price_text or repr(inst.price)) * inst.contract_size)


def instrument_margins(
    inst: BpfInstrument, is_hist: Sequence[bool], confidence: float, n_stress: int
) -> tuple[int, int]:
    """(1 枚の売り, 1 枚の買い)。OP の売りだけ清算値段 × Contract Size を下限にする"""
    short = margin_one_lot(
        inst.pnl, is_hist, side=-1, confidence=confidence, n_stress=n_stress, floor=option_short_floor(inst)
    )
    long = margin_one_lot(inst.pnl, is_hist, side=1, confidence=confidence, n_stress=n_stress)
    return short, long


# ---- All-Instruments-VaR・照合 ----

AIV_COLUMNS = ("Instrument Code", "Product Group Code", "Margin for 1 Short", "Margin for 1 Long")


def parse_all_instruments_var(text: str, product_group: str) -> dict[str, tuple[int, int]]:
    """銘柄コード → (1 枚の売り, 1 枚の買い)。Product Group Code = product_group の行だけ"""
    reader = csv.reader(io.StringIO((text or "").lstrip("﻿")))
    header = next(reader, None)
    if header is None or not set(AIV_COLUMNS) <= set(header):
        raise JsccFormatError("All-Instruments-VaR の見出しが違う")
    idx = [header.index(c) for c in AIV_COLUMNS]
    out: dict[str, tuple[int, int]] = {}
    for n, row in enumerate(reader, start=2):
        if not row:
            continue
        if len(row) != len(header):
            _fail(n, "All-Instruments-VaR の列の数が違う")
        code, group, short, long = (row[i] for i in idx)
        if group != product_group:
            continue
        try:
            value = (int(short), int(long))
        except ValueError:
            _fail(n, f"All-Instruments-VaR の値が読めない（銘柄 {code}）")
        if code in out:
            _fail(n, f"All-Instruments-VaR に銘柄 {code} が 2 行")
        out[code] = value
    return out


MISMATCH_CODES_MAX = 20


@dataclass(frozen=True)
class MarginCheck:
    matched: int
    mismatched: int
    ref_only: int
    bpf_only: int
    mismatched_codes: tuple[str, ...]


def compare_margins(computed: dict[str, tuple[int, int]], reference: dict[str, tuple[int, int]]) -> MarginCheck:
    """両方にある銘柄の売り・買いを別々に数える。片側だけの銘柄は不一致に数えない"""
    matched = mismatched = 0
    codes = []
    for code in sorted(set(computed) & set(reference)):
        same = sum(a == b for a, b in zip(computed[code], reference[code], strict=True))
        matched += same
        mismatched += 2 - same
        if same != 2:
            codes.append(code)
    return MarginCheck(
        matched=matched,
        mismatched=mismatched,
        ref_only=len(set(reference) - set(computed)),
        bpf_only=len(set(computed) - set(reference)),
        mismatched_codes=tuple(codes[:MISMATCH_CODES_MAX]),
    )


# ---- ロード用の行（schema/tokyo/jscc_*.json の列と 1 対 1） ----


def _decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value.normalize(), "f")


def scenario_row(
    inst: BpfInstrument, *, business_date: date, file_version: str, run_id: str, reference: tuple[int, int] | None
) -> dict:
    return {
        "business_date": business_date.isoformat(),
        "file_version": file_version,
        "jscc_run_id": run_id,
        "instrument_code": inst.instrument_code,
        "product_group_code": inst.product_group_code,
        "product_code": inst.product_code,
        "contract_type": inst.contract_type,
        "put_call_type": inst.put_call_type,
        "contract_period_code": inst.contract_period_code,
        "underlying_period_code": inst.underlying_period_code,
        "strike_price": _decimal_text(inst.strike_price),
        "contract_size": inst.contract_size,
        "aggregation_group": inst.aggregation_group,
        "calculation_method_flag": inst.calculation_method_flag,
        "price": inst.price,
        "pnl": inst.pnl.tolist(),
        "margin_short_jscc": reference[0] if reference else None,
        "margin_long_jscc": reference[1] if reference else None,
    }


def scenario_set_row(summary: BpfSummary, *, business_date: date, file_version: str, run_id: str) -> dict:
    return {
        "business_date": business_date.isoformat(),
        "file_version": file_version,
        "jscc_run_id": run_id,
        "file_type": summary.file_type,
        "scenarios": [{"scenario_id": sid, "scenario_type": stype} for sid, stype in summary.scenarios],
        "scenarios_h": sum(1 for _, t in summary.scenarios if t == "H"),
        "scenarios_s": sum(1 for _, t in summary.scenarios if t == "S"),
    }


def parameter_rows(records: list[ParamRecord], *, business_date: date, file_version: str, run_id: str) -> list[dict]:
    return [
        {
            "business_date": business_date.isoformat(),
            "file_version": file_version,
            "jscc_run_id": run_id,
            "line_no": r.line_no,
            "record_id": r.record_id,
            "record_name": r.record_name,
            "level": r.level,
            "aggregation_group": r.aggregation_group,
            "parent_aggregation_group": r.parent_aggregation_group,
            "type": r.type,
            "confidence_level": r.confidence_level,
            "stress_scenario_number": r.stress_scenario_number,
            "params": [{"name": name, "value": value} for name, value in r.params],
        }
        for r in records
    ]

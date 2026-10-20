"""JPX の日経225オプションのページを解析する（純粋関数。now()・I/O を使わない）。

functions/jpx_loader.parse_jpx_html の後継。値の解釈は旧パーサーと同じにし、次の 2 点だけ直す:
- 清算日の年: 旧は datetime.now().year。新はページの更新時刻の年（未来になるなら前年）
- 先物の限月: 旧は「限月の 1 日が更新時刻より前なら翌年」にしており、SQ のある月の 1 日〜限月交代まで
  翌年の限月になる（例: 2025-12-01 の「25年12月限」が 2026-12-01）。新は表示の年（YY）をそのまま使う
"""

import re
from dataclasses import dataclass, field
from datetime import date, datetime
from zoneinfo import ZoneInfo

from pyquery import PyQuery

JST = ZoneInfo("Asia/Tokyo")

CALL = 1
PUT = 2

REGEX_PRICE = re.compile(r"([\d.]+)\((\d{2})/(\d{2}) (\d{2}):(\d{2})\)")
REGEX_OPTION_PRICE = re.compile(r"([0-9.]+)\s*(\d{2})/(\d{2}) (\d{2}):(\d{2})")
REGEX_ORDER = re.compile(r"([\d\-]+)\s*\(([\d\-]+)\)\s*([\d\-]+)\s*\(([\d\-]+)\)")
REGEX_DIFF = re.compile(r"([+\-\d.]+)\s*([+\-\d.]+)%")
REGEX_ORDER_IV = re.compile(r"(?:-|([\d.]+)%)\s*(?:-|([\d.]+)%)")
REGEX_CONTRACT_MONTH = re.compile(r"(\d+)年(\d+)月")
REGEX_MONTH_DAY = re.compile(r"(\d+)/(\d+)")
REGEX_UPDATED_AT = re.compile(r"^\d{4}/\d{2}/\d{2} \d{2}:\d{2}$")
REGEX_LAST_TRADING_DAY = re.compile(r"^\d{4}/\d{2}/\d{2}$")


class JpxUnavailable(Exception):
    """JPX のページが空・取得できない（4 時台の空ページ、接続断など）。次の分に任せる"""


class JpxTransient(JpxUnavailable):
    """一時的な取得の失敗（HTTP 5xx・接続エラー・タイムアウト）。同じ分の中で上限まで取り直す（#130、app._fetch_jpx）"""


class ParseError(Exception):
    """ページの構造が想定と違う（形式変更など）。生 HTML を残して parse_failed にする"""


@dataclass
class SpotRow:
    price: float | None
    price_time: datetime | None
    price_time_raw: str | None
    diff: float | None
    diff_rate: float | None
    hv: float | None


@dataclass
class FutureRow:
    price: int | None
    price_time: datetime | None
    price_time_raw: str | None
    diff: int | None
    diff_rate: float | None
    hv: float | None
    contract_month: date


@dataclass
class OptionRow:
    type: int
    target_price: int
    is_atm: bool
    price: int | None
    price_time: datetime | None
    price_time_raw: str | None
    diff: int | None
    diff_rate: float | None
    iv: float | None
    bid: int | None
    bid_volume: int | None
    bid_iv: float | None
    ask: int | None
    ask_volume: int | None
    ask_iv: float | None
    volume: int | None
    positions: int | None
    quotation: int | None
    quotation_date: date
    delta: float | None
    gamma: float | None
    theta: float | None
    vega: float | None
    last_trading_day: date


@dataclass
class ParsedPage:
    updated_at: datetime
    spot: SpotRow
    future: FutureRow
    quotation_date: date
    last_trading_day: date
    # 旧パーサーと同じく全 call（行使価格の表示順）→ 全 put の順
    options: list[OptionRow] = field(default_factory=list)


def build_price_time(updated_at: datetime, month: int, day: int, hour: int, minute: int) -> datetime:
    """表示の約定日時（年なし）に、前年・当年・翌年のうち更新時刻に最も近い年を補う（旧 build_price_time と同じ規則）"""
    base = updated_at.astimezone(JST)
    candidates = []
    for year in (base.year - 1, base.year, base.year + 1):
        try:
            candidates.append(datetime(year, month, day, hour, minute, tzinfo=JST))
        except ValueError:
            continue
    if not candidates:
        raise ValueError(f"invalid price time: {month:02d}/{day:02d} {hour:02d}:{minute:02d}")
    return min(candidates, key=lambda x: abs(x - base))


def _doc(html: str | bytes) -> PyQuery:
    if isinstance(html, bytes):
        html = html.decode("utf-8", errors="replace")
    if not html.strip():
        raise JpxUnavailable("empty body")
    return PyQuery(html, parser="html")


def _updated_at(q: PyQuery) -> datetime:
    text = q.find(".update-time").find("dd").text().strip()
    if not text:
        raise JpxUnavailable("update-time is empty")
    if not REGEX_UPDATED_AT.match(text):
        raise ParseError(f"unexpected update-time: {text!r}")
    return datetime.strptime(text, "%Y/%m/%d %H:%M").replace(tzinfo=JST)


def parse_updated_at(html: str | bytes) -> datetime:
    """ページの最終更新時刻（JST aware）だけを取り出す。空なら JpxUnavailable"""
    return _updated_at(_doc(html))


def _none_or(conv, text: str):
    text = text.strip()
    if text in ("", "-"):
        return None
    return conv(text)


def _price_with_time(text: str, regex, conv, updated_at):
    m = regex.search(text)
    if m is None:
        return None, None, None
    month, day, hour, minute = (int(x) for x in m.group(2, 3, 4, 5))
    raw = f"{m.group(2)}/{m.group(3)} {m.group(4)}:{m.group(5)}"
    return conv(m.group(1)), build_price_time(updated_at, month, day, hour, minute), raw


def _price_row(price_info: PyQuery, label: str):
    rows = price_info.find("tr").filter(lambda i, e: PyQuery(e).find("td").eq(0).text().find(label) > -1)
    if len(rows) != 1:
        raise ParseError(f"priceInfo の {label} の行が {len(rows)} 行")
    tds = rows.find("td")
    if len(tds) < 5:
        raise ParseError(f"priceInfo の {label} の列が {len(tds)} 列")
    return [tds.eq(i).text() for i in range(5)]


def _parse_spot(price_info: PyQuery, updated_at: datetime) -> SpotRow:
    cells = _price_row(price_info, "日経平均株価")
    price, price_time, raw = _price_with_time(cells[1].replace(",", ""), REGEX_PRICE, float, updated_at)
    return SpotRow(
        price=price,
        price_time=price_time,
        price_time_raw=raw,
        diff=_none_or(float, cells[2].replace(",", "")),
        diff_rate=_none_or(float, cells[3].replace("%", "").replace(",", "")),
        hv=_none_or(float, cells[4].replace("%", "").replace(",", "")),
    )


def _parse_future(price_info: PyQuery, updated_at: datetime) -> FutureRow:
    cells = _price_row(price_info, "先物")
    price, price_time, raw = _price_with_time(cells[1].replace(",", ""), REGEX_PRICE, int, updated_at)
    m = REGEX_CONTRACT_MONTH.search(cells[0])
    if m is None:
        raise ParseError(f"先物の限月が読めない: {cells[0]!r}")
    # 表示は「26年12月限」（2 桁）。4 桁（「2026年12月限」）に変わっても読めるようにし、
    # 更新時刻の年 −1〜+3 の範囲外は形式変更として ParseError にする（4026 年などを通さない）
    year_text, month = m.group(1), int(m.group(2))
    if len(year_text) == 2:
        year = 2000 + int(year_text)
    elif len(year_text) == 4:
        year = int(year_text)
    else:
        raise ParseError(f"先物の限月の年が読めない: {cells[0]!r}")
    base_year = updated_at.astimezone(JST).year
    if not base_year - 1 <= year <= base_year + 3:
        raise ParseError(f"先物の限月の年が範囲外: {cells[0]!r}")
    try:
        contract_month = date(year, month, 1)
    except ValueError as e:
        raise ParseError(f"先物の限月が不正: {cells[0]!r}") from e
    return FutureRow(
        price=price,
        price_time=price_time,
        price_time_raw=raw,
        diff=_none_or(int, cells[2].replace(",", "")),
        diff_rate=_none_or(float, cells[3].replace("%", "").replace(",", "")),
        hv=_none_or(float, cells[4].replace("%", "").replace(",", "")),
        contract_month=contract_month,
    )


def _quotation_date(q: PyQuery, updated_at: datetime) -> date:
    text = q.find(".price-info-header").find("tr").eq(1).find("th").eq(0).text()
    m = REGEX_MONTH_DAY.search(text)
    if m is None:
        raise ParseError(f"清算日が読めない: {text!r}")
    month, day = int(m.group(1)), int(m.group(2))
    base = updated_at.astimezone(JST).date()
    for year in (base.year, base.year - 1):
        try:
            d = date(year, month, day)
        except ValueError:
            continue
        if d <= base:
            return d
    raise ParseError(f"清算日が不正: {text!r}")


def _option(
    option_type: int,
    target_price: int,
    is_atm: bool,
    cells: list[str],
    greeks: list[str],
    quotation_date: date,
    last_trading_day: date,
    updated_at: datetime,
) -> OptionRow:
    # cells: 現在値, 前日比, IV, 売気配(数量) 買気配(数量), 売気配IV 買気配IV, 取引高, 建玉残, 清算値
    price, price_time, raw = _price_with_time(cells[0], REGEX_OPTION_PRICE, int, updated_at)

    diff = diff_rate = None
    m = REGEX_DIFF.search(cells[1])
    if m is not None:
        diff = _none_or(int, m.group(1))
        diff_rate = _none_or(float, m.group(2))

    ask = ask_volume = bid = bid_volume = None
    m = REGEX_ORDER.search(cells[3])
    if m is not None:
        ask, ask_volume, bid, bid_volume = (_none_or(int, x) for x in m.group(1, 2, 3, 4))

    ask_iv = bid_iv = None
    m = REGEX_ORDER_IV.search(cells[4])
    if m is not None:
        ask_iv = float(m.group(1)) if m.group(1) is not None else None
        bid_iv = float(m.group(2)) if m.group(2) is not None else None

    return OptionRow(
        type=option_type,
        target_price=target_price,
        is_atm=is_atm,
        price=price,
        price_time=price_time,
        price_time_raw=raw,
        diff=diff,
        diff_rate=diff_rate,
        iv=_none_or(float, cells[2].replace("%", "")),
        bid=bid,
        bid_volume=bid_volume,
        bid_iv=bid_iv,
        ask=ask,
        ask_volume=ask_volume,
        ask_iv=ask_iv,
        volume=_none_or(int, cells[5]),
        positions=_none_or(int, cells[6]),
        quotation=_none_or(int, cells[7]),
        quotation_date=quotation_date,
        delta=_none_or(float, greeks[0]),
        gamma=_none_or(float, greeks[1]),
        theta=_none_or(float, greeks[2]),
        vega=_none_or(float, greeks[3]),
        last_trading_day=last_trading_day,
    )


def parse_page(html: str | bytes) -> ParsedPage:
    """JPX の価格ページを解析する。

    空ページ（更新時刻が空・オプションの行が 0 行）は JpxUnavailable、それ以外の構造違いは ParseError。
    """
    q = _doc(html)
    updated_at = _updated_at(q)

    scroll = q.find(".price-info-scroll")
    row_elems = scroll.find(".row-num")
    if len(row_elems) == 0:
        raise JpxUnavailable("no option rows")

    price_info = q.find("#priceInfo")
    spot = _parse_spot(price_info, updated_at)
    future = _parse_future(price_info, updated_at)
    quotation_date = _quotation_date(q, updated_at)

    ltd_text = q.find(".date-table.last-tradingday").find("dd").text().strip()
    if not REGEX_LAST_TRADING_DAY.match(ltd_text):
        raise ParseError(f"取引最終日が読めない: {ltd_text!r}")
    last_trading_day = datetime.strptime(ltd_text, "%Y/%m/%d").date()

    rows = [[PyQuery(td).text().strip().replace(",", "") for td in PyQuery(e).find("td")] for e in row_elems]
    greeks = [
        [PyQuery(td).text().replace(",", "") for td in PyQuery(e).find("table").find("td")]
        for e in scroll.find(".greek")
    ]
    if len(rows) != len(greeks):
        raise ParseError(f"row-num {len(rows)} 行とギリシャ指標 {len(greeks)} 行が一致しない")

    calls, puts = [], []
    for row, greek in zip(rows, greeks, strict=True):
        if len(row) != 17:
            raise ParseError(f"row-num の列が {len(row)} 列（17 列のはず）")
        if len(greek) != 8:
            raise ParseError(f"ギリシャ指標が {len(greek)} 個（8 個のはず）")
        m = re.search(r"([0-9]+)", row[8])
        if m is None:
            raise ParseError(f"行使価格が読めない: {row[8]!r}")
        target_price = int(m.group(1))
        # 表示は「A T M」（間は NBSP）。空白類を除いて探す
        is_atm = "ATM" in re.sub(r"\s", "", row[8])
        try:
            calls.append(
                _option(
                    CALL,
                    target_price,
                    is_atm,
                    list(reversed(row[:8])),
                    greek[:4],
                    quotation_date,
                    last_trading_day,
                    updated_at,
                )
            )
            puts.append(
                _option(PUT, target_price, is_atm, row[-8:], greek[-4:], quotation_date, last_trading_day, updated_at)
            )
        except ValueError as e:
            raise ParseError(f"行使価格 {target_price} の値が読めない: {e}") from e

    return ParsedPage(
        updated_at=updated_at,
        spot=spot,
        future=future,
        quotation_date=quotation_date,
        last_trading_day=last_trading_day,
        options=calls + puts,
    )


def jst(year, month, day, hour=0, minute=0) -> datetime:
    """テスト・呼び出し側の補助: JST aware の datetime"""
    return datetime(year, month, day, hour, minute, tzinfo=JST)

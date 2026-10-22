"""立花の銘柄マスタから、毎分取得する銘柄を選ぶ（純粋関数。now()・I/O を使わない）。

- オプション（CLMStkGetIssueMstOp）: 日経225オプション（月次）のうち、取引最終日が基準日以降の近い 2 限月の
  全行使価格のコール・プット（行使価格の範囲で絞れる）
- 先物（CLMStkGetIssueMstSak）: 日経225先物（ラージ）の売買停止でない最も近い限月
- 指数: 日経平均（101）

限月は取引最終日（sLastBaibaiDay）ごとにまとめる（snapshot.validate_rows と同じく取引最終日を限月のキーにする）。
基準日 today は取引日（夜間は次の取引日。calendar.Session.trade_date と同じ考え）で、呼ぶ側が渡す。

資料で確かめたこと: 商品区分（sSakOpSyouhin）は 18 = 日経225、19 = 日経225ミニ（先物マスタとオプションマスタで同じ
一覧）。売買停止（sBaibaiTeisiC）は " " 通常・"0" 解除・"9" 停止中。先物の例の原資産コード（sGensisanCode）は 101。
推定（口座で確かめる）: 日経225オプションの商品区分は 18（資料の例では TOPIX のオプションが TOPIX 先物と同じ 05
なので）。オプションの原資産コードは資料の例では空白なので、空白か 101 なら通し、日経225 かどうかは商品区分で判断する
（18 の推定が違えば該当なしの MasterError になり、止まる側に倒れる）。週次のオプションは最終売買日が第 2 金曜日の前の
1 週間に無いことで、ミニのオプションは銘柄名で外す。
"""

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation

from ...parser import CALL, PUT

# 原資産コード（sGensisanCode）の日経平均（資料の先物の例）
UNDERLYING_NIKKEI225 = "101"
# CLMStkGetIssueMstIndex の日経平均の銘柄コード（資料の例）
SPOT_CODE = "101"
# 日経225オプションの商品区分（sSakOpSyouhin）。18 と推定（口座で確かめる）。ミニ（19）はここに入れないことで除く
OPTION_PRODUCTS = frozenset({"18"})
# 日経225先物（ラージ）の商品区分（資料: 18 = 日経225、19 = 日経225ミニ）
FUTURE_PRODUCTS = frozenset({"18"})
# sBaibaiTeisiC の「売買停止でない」値（資料: " " 通常、"0" 解除、"9" 停止中）。生の値で比べる（strip しない。
# "" や未知の値は取らない側に倒す）
NOT_SUSPENDED = frozenset({" ", "0"})
# sPutCall（資料の記載: 5 = プット、7 = コール）
PUT_CALL = {"7": CALL, "5": PUT}
# ミニのオプションを銘柄名（sIssueName・sIssueNameEizi）で外す語（推定。英字は大文字に揃えて比べる）
EXCLUDED_NAME_WORDS = ("ミニ", "MINI")
# 日付の未設定（sKenrikousiLastDay などの 00000000）
_UNSET_DATES = frozenset({"", "00000000"})


class MasterError(ValueError):
    """該当なし・必要な項目の欠け・形の誤り（文に銘柄名・コードの一覧は入れない。件数・項目名だけ）"""


@dataclass(frozen=True)
class OptionIssue:
    code: str
    type: int  # CALL / PUT
    target_price: int
    last_trading_day: date
    contract_month: date  # sGengetu の月の 1 日


@dataclass(frozen=True)
class FutureIssue:
    code: str
    contract_month: date  # sGengetu の月の 1 日
    last_trading_day: date


@dataclass(frozen=True)
class Selection:
    spot_code: str
    future: FutureIssue
    # 取引最終日 → コール（行使価格の昇順）→ プット（行使価格の昇順）の順
    options: tuple[OptionIssue, ...]

    def codes(self) -> list[str]:
        """時価を取る銘柄コード（get_market_price に渡す）: 指数、先物、オプションの順"""
        return [self.spot_code, self.future.code, *(o.code for o in self.options)]


def parse_yyyymmdd(text: str) -> date | None:
    """YYYYMMDD → date。空と 00000000 は None"""
    if text in _UNSET_DATES:
        return None
    if len(text) != 8 or not text.isdigit():
        raise MasterError("date is not YYYYMMDD")
    try:
        return datetime.strptime(text, "%Y%m%d").date()
    except ValueError:
        raise MasterError("date is not YYYYMMDD") from None


def parse_contract_month(text: str) -> date:
    """YYYYMM → その月の 1 日"""
    if len(text) != 6 or not text.isdigit():
        raise MasterError("sGengetu is not YYYYMM")
    try:
        return date(int(text[:4]), int(text[4:]), 1)
    except ValueError:
        raise MasterError("sGengetu is not YYYYMM") from None


def parse_strike(text: str) -> int:
    """sKousiPrice（"2000.000000"）→ 2000。整数でなければ MasterError"""
    try:
        value = Decimal(text)
    except InvalidOperation:
        raise MasterError("sKousiPrice is not a number") from None
    if not value.is_finite() or value != value.to_integral_value():
        raise MasterError("sKousiPrice is not an integer")
    return int(value)


def is_monthly_last_trading_day(d: date) -> bool:
    """d が月次（SQ が第 2 金曜日）の最終売買日になりうる日か。

    第 2 金曜日を F として F - 7 日 <= d < F。月次の最終売買日は SQ の前の営業日（休日が続いても F - 7 より前には
    ならない）。週次は第 1 金曜日の前（F - 8 以前）か第 3 金曜日以降の前（F + 6 前後）なので外れる。休日の表は要らない
    """
    first = date(d.year, d.month, 1)
    second_friday = first + timedelta(days=(4 - first.weekday()) % 7 + 7)
    return second_friday - timedelta(days=7) <= d < second_friday


def _require(item: Mapping[str, str], name: str) -> str:
    value = item.get(name)
    if not isinstance(value, str) or not value:
        raise MasterError(f"missing {name}")
    return value


def _last_trading_day(item: Mapping[str, str]) -> date:
    value = parse_yyyymmdd(_require(item, "sLastBaibaiDay"))
    if value is None:
        raise MasterError("sLastBaibaiDay is unset")
    return value


def _is_nikkei225_underlying(item: Mapping[str, str]) -> bool:
    """原資産コードが空白（オプションの例）か 101（先物の例）なら通す。それ以外（別の原資産）は外す"""
    code = item.get("sGensisanCode")
    if not isinstance(code, str):
        return False
    code = code.strip()
    return code == "" or code == UNDERLYING_NIKKEI225


def _has_excluded_name(item: Mapping[str, str]) -> bool:
    names = (item.get("sIssueName"), item.get("sIssueNameEizi"))
    return any(isinstance(n, str) and any(w in n.upper() for w in EXCLUDED_NAME_WORDS) for n in names)


def select_options(
    master: Iterable[Mapping[str, str]],
    today: date,
    *,
    months: int = 2,
    strike_range: tuple[int, int] | None = None,
) -> tuple[OptionIssue, ...]:
    """日経225オプション（OPTION_PRODUCTS・原資産が空白か 101・sPutCall が 5/7・月次の最終売買日・名前がミニでない）の、
    取引最終日が today 以降の近い months 限月。

    strike_range=(lo, hi) は両端を含む。限月が months 個に満たない・範囲で限月の銘柄が無くなる・
    同じ (type, target_price, last_trading_day) が 2 つあるとき（ミニがすり抜けたときなど）は MasterError
    """
    if months < 1:
        raise ValueError("months must be positive")
    if strike_range is not None and strike_range[0] > strike_range[1]:
        raise ValueError("strike_range must be (low, high)")
    issues = []
    for item in master:
        if item.get("sSakOpSyouhin") not in OPTION_PRODUCTS or not _is_nikkei225_underlying(item):
            continue
        option_type = PUT_CALL.get(item.get("sPutCall"))
        if option_type is None:
            continue
        last = _last_trading_day(item)
        if last < today or not is_monthly_last_trading_day(last) or _has_excluded_name(item):
            continue
        issues.append(
            OptionIssue(
                code=_require(item, "sIssueCode"),
                type=option_type,
                target_price=parse_strike(_require(item, "sKousiPrice")),
                last_trading_day=last,
                contract_month=parse_contract_month(_require(item, "sGengetu")),
            )
        )
    days = sorted({i.last_trading_day for i in issues})[:months]
    if len(days) < months:
        raise MasterError(f"option months: {len(days)} (need {months})")
    selected = [
        i
        for i in issues
        if i.last_trading_day in days and (strike_range is None or strike_range[0] <= i.target_price <= strike_range[1])
    ]
    found = {i.last_trading_day for i in selected}
    if len(found) < months:
        raise MasterError(f"option months in strike_range: {len(found)} (need {months})")
    keys = {(i.type, i.target_price, i.last_trading_day) for i in selected}
    if len(keys) != len(selected):
        raise MasterError(f"duplicate (type, target_price, last_trading_day): {len(selected) - len(keys)}")
    return tuple(sorted(selected, key=lambda i: (i.last_trading_day, i.type, i.target_price)))


def select_future(master: Iterable[Mapping[str, str]], today: date) -> FutureIssue:
    """日経225先物（FUTURE_PRODUCTS・原資産が空白か 101・売買停止でない）の、取引最終日が today 以降で最も近い限月。

    無い・最も近い取引最終日の銘柄が 2 つあるときは MasterError
    """
    candidates = []
    for item in master:
        if item.get("sSakOpSyouhin") not in FUTURE_PRODUCTS or not _is_nikkei225_underlying(item):
            continue
        if item.get("sBaibaiTeisiC") not in NOT_SUSPENDED:
            continue
        last = _last_trading_day(item)
        if last < today:
            continue
        candidates.append(
            FutureIssue(
                code=_require(item, "sIssueCode"),
                contract_month=parse_contract_month(_require(item, "sGengetu")),
                last_trading_day=last,
            )
        )
    if not candidates:
        raise MasterError("no future")
    nearest = min(c.last_trading_day for c in candidates)
    found = [c for c in candidates if c.last_trading_day == nearest]
    if len(found) != 1:
        raise MasterError(f"futures on the nearest last trading day: {len(found)}")
    return found[0]


def select(
    option_master: Iterable[Mapping[str, str]],
    future_master: Iterable[Mapping[str, str]],
    today: date,
    *,
    months: int = 2,
    strike_range: tuple[int, int] | None = None,
) -> Selection:
    return Selection(
        spot_code=SPOT_CODE,
        future=select_future(future_master, today),
        options=select_options(option_master, today, months=months, strike_range=strike_range),
    )

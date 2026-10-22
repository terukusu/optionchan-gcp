"""立花の銘柄マスタから、毎分取得する銘柄を選ぶ（純粋関数。now()・I/O を使わない）。

- オプション（CLMStkGetIssueMstOp）: 日経225オプション（月次）のうち、取引最終日が基準日以降の近い 2 限月の
  全行使価格のコール・プット（行使価格の範囲で絞れる）
- 先物（CLMStkGetIssueMstSak）: 日経225先物の売買停止でない最も近い限月
- 指数: 日経平均（101）

限月は取引最終日（sLastBaibaiDay）ごとにまとめる（snapshot.validate_rows と同じく取引最終日を限月のキーにする）。
基準日 today は取引日（夜間は次の取引日。calendar.Session.trade_date と同じ考え）で、呼ぶ側が渡す。
"""

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

from ...parser import CALL, PUT

# 原資産コード（sGensisanCode）の日経平均（資料の例）
UNDERLYING_NIKKEI225 = "101"
# CLMStkGetIssueMstIndex の日経平均の銘柄コード（資料の例）
SPOT_CODE = "101"
# 日経225オプション（月次）の商品区分（sSakOpSyouhin）。未確認。
# 実在の値と取り違えないよう、口座で確かめるまでは実際のマスタに一致しない仮の値にしてある（実のマスタでは
# 該当なしの MasterError になり、黙って別の商品を選ばない）。週次・ミニはここに入れないことで除く（見分け方も未確認）
OPTION_PRODUCTS = frozenset({"UNCONFIRMED-NK225-OP"})
# 日経225先物の商品区分（資料の例 18 = 日経225）。ラージかミニかは未確認（JPX の future_price はラージと推定）
FUTURE_PRODUCTS = frozenset({"18"})
# sBaibaiTeisiC の「売買停止でない」値。未確認
NOT_SUSPENDED = frozenset({"0"})
# sPutCall（資料の記載: 5 = プット、7 = コール）
PUT_CALL = {"7": CALL, "5": PUT}
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


def select_options(
    master: Iterable[Mapping[str, str]],
    today: date,
    *,
    months: int = 2,
    strike_range: tuple[int, int] | None = None,
) -> tuple[OptionIssue, ...]:
    """日経225オプション（OPTION_PRODUCTS・原資産 101・sPutCall が 5/7）の、取引最終日が today 以降の近い months 限月。

    strike_range=(lo, hi) は両端を含む。限月が months 個に満たない・範囲で限月の銘柄が無くなる・
    同じ (type, target_price, last_trading_day) が 2 つあるときは MasterError
    """
    if months < 1:
        raise ValueError("months must be positive")
    if strike_range is not None and strike_range[0] > strike_range[1]:
        raise ValueError("strike_range must be (low, high)")
    issues = []
    for item in master:
        if item.get("sSakOpSyouhin") not in OPTION_PRODUCTS or item.get("sGensisanCode") != UNDERLYING_NIKKEI225:
            continue
        option_type = PUT_CALL.get(item.get("sPutCall"))
        if option_type is None:
            continue
        last = _last_trading_day(item)
        if last < today:
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
    """日経225先物（FUTURE_PRODUCTS・原資産 101・売買停止でない）の、取引最終日が today 以降で最も近い限月。

    無い・最も近い取引最終日の銘柄が 2 つあるときは MasterError
    """
    candidates = []
    for item in master:
        if item.get("sSakOpSyouhin") not in FUTURE_PRODUCTS or item.get("sGensisanCode") != UNDERLYING_NIKKEI225:
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

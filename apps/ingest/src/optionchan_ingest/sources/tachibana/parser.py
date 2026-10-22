"""立花の時価の応答と選んだ銘柄から、spot_price・future_price・option_price の行を作る
（純粋関数。now()・I/O を使わない）。

出力は snapshot.build_rows と同じ形（表名 → 列名 → Python 値の dict のリスト）。各行は schema/tokyo/<表>.json の
列をすべてキーに持つ（NULL の列も None で持つ）。

NULL にする列: option の is_atm・iv・bid_iv・ask_iv・ギリシャ指標（段 1b で気配から計算）、quotation・quotation_date
（清算値に当たる項目が未確認）、price_time_raw（列の意味が JPX のページの表示 MM/DD HH:MM なので入れない。元の値は
生の応答に残る）、page_updated_at（ページの更新時刻に当たるものが無い）、future・spot の hv。
is_atm が NULL なので snapshot.validate_rows（ATM が 1 行あることを求める）には通さない（段 1a/1b）。
"""

from collections.abc import Iterable, Mapping
from datetime import datetime, time, timedelta
from decimal import Decimal, InvalidOperation

from ...parser import JST
from ...snapshot import RunMeta
from .master import Selection

# 解析コードの版（段 1a で RunMeta.parser_version に入れる値。この Issue の build_rows は RunMeta の値をそのまま使う）
PARSER_VERSION = "tachibana-1"

# 時価の FD 項目コード。資料の例にあるのは pDPP（現在値）・tDPP:T（現在値の時刻）だけ。
# ほかは一般に知られている名前で、未確認（口座で確かめる）。None は「項目なし → 列は NULL」
FIELD_PRICE = "pDPP"
FIELD_PRICE_TIME = "tDPP:T"
FIELD_DIFF = "pDYWP"  # 前日比。未確認
FIELD_DIFF_RATE = "pDYRP"  # 騰落率。未確認
FIELD_BID = "pQBP"  # 買気配。未確認
FIELD_BID_VOLUME = "pBV"  # 買気配の数量。未確認
FIELD_ASK = "pQAP"  # 売気配。未確認
FIELD_ASK_VOLUME = "pAV"  # 売気配の数量。未確認
FIELD_VOLUME = "pDV"  # 出来高。未確認
FIELD_POSITIONS: str | None = None  # 建玉。コードが未確認なので取らない（列は NULL）

# get_market_price の columns に渡す項目
MARKET_COLUMNS: tuple[str, ...] = tuple(
    c
    for c in (
        FIELD_PRICE,
        FIELD_PRICE_TIME,
        FIELD_DIFF,
        FIELD_DIFF_RATE,
        FIELD_BID,
        FIELD_BID_VOLUME,
        FIELD_ASK,
        FIELD_ASK_VOLUME,
        FIELD_VOLUME,
        FIELD_POSITIONS,
    )
    if c is not None
)

# 値が無いことの表し方。未確認（空文字と "-" を NULL にする）。項目そのものが応答に無いときも NULL
EMPTY_VALUES = frozenset({"", "-"})
# 現在値の時刻（tDPP:T）の書式。未確認（時刻だけの値と推定。日付は complete_price_time で補う）
PRICE_TIME_FORMATS = ("%H:%M:%S", "%H:%M")
# 約定時刻が取得時刻より後に見えるのを許す幅（時計のずれ）。これを超えたら前日の時刻とみなす
PRICE_TIME_FUTURE_TOLERANCE = timedelta(minutes=5)

OPTION_NULL_COLUMNS = (
    "is_atm",
    "iv",
    "bid_iv",
    "ask_iv",
    "delta",
    "gamma",
    "theta",
    "vega",
    "quotation",
    "quotation_date",
    "price_time_raw",
    "page_updated_at",
)
FUTURE_NULL_COLUMNS = ("hv", "price_time_raw", "page_updated_at")
SPOT_NULL_COLUMNS = ("hv", "price_time_raw", "page_updated_at")


class TachibanaParseError(ValueError):
    """時価の応答が想定と違う（数値でない・整数の列に小数・時刻の書式・銘柄の欠け・重複）。文に値は入れない"""


def _require_aware(dt: datetime, name: str) -> None:
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")


def _text(item: Mapping[str, str], code: str | None) -> str | None:
    if code is None:
        return None
    value = item.get(code)
    if value is None:
        return None
    if not isinstance(value, str):
        raise TachibanaParseError(f"{code}: not a string")
    value = value.strip()
    return None if value in EMPTY_VALUES else value


def _decimal(text: str, code: str) -> Decimal:
    try:
        value = Decimal(text)
    except InvalidOperation:
        raise TachibanaParseError(f"{code}: not a number") from None
    if not value.is_finite():
        raise TachibanaParseError(f"{code}: not a finite number")
    return value


def _int(item: Mapping[str, str], code: str | None) -> int | None:
    """INTEGER の列。整数でなければ TachibanaParseError（小数を黙って切り捨てない）"""
    text = _text(item, code)
    if text is None:
        return None
    value = _decimal(text, code)
    if value != value.to_integral_value():
        raise TachibanaParseError(f"{code}: not an integer")
    return int(value)


def _float(item: Mapping[str, str], code: str | None) -> float | None:
    text = _text(item, code)
    return None if text is None else float(_decimal(text, code))


def _parse_time(text: str) -> time:
    for fmt in PRICE_TIME_FORMATS:
        try:
            return datetime.strptime(text, fmt).time()
        except ValueError:
            continue
    raise TachibanaParseError(f"{FIELD_PRICE_TIME}: unsupported time format")


def complete_price_time(text: str | None, created_at: datetime) -> datetime | None:
    """時刻だけの値（PRICE_TIME_FORMATS）に日付を補い、JST の aware な datetime にする。空は None。

    created_at（取得時刻）の JST の日付を付け、created_at + PRICE_TIME_FUTURE_TOLERANCE より後になるなら前日にする。
    約定時刻は取得時刻より前なので、夜間の 0 時以降に取った 0 時以降の時刻はその日（夜間の始まりから見て翌日）、
    0 時前の時刻は前日になる。金曜の夜 → 土曜の朝の夜間にもそのまま使える

    制限: 約定がその日か前日のときだけ正しい。月曜の日中の始まり前後に土曜の朝（夜間の終わり）の約定（例 05:30）を
    取ると月曜 05:30 になり、薄い行使価格で何日も約定が無いときも日付がずれる。時価の応答に日付の項目があるかは
    未確認なので、段 2 で口座の応答を確かめる（日付が無ければ段 1a/1b で古い price_time の扱いを決める）
    """
    _require_aware(created_at, "created_at")
    if text is None or text.strip() in EMPTY_VALUES:
        return None
    clock = _parse_time(text.strip())
    base = created_at.astimezone(JST)
    value = datetime.combine(base.date(), clock, tzinfo=JST)
    if value > created_at + PRICE_TIME_FUTURE_TOLERANCE:
        value = datetime.combine(base.date() - timedelta(days=1), clock, tzinfo=JST)
    return value


def _index(prices: Iterable[Mapping[str, str]]) -> dict[str, Mapping[str, str]]:
    by_code: dict[str, Mapping[str, str]] = {}
    duplicates = 0
    for item in prices:
        code = item.get("sIssueCode")
        if not isinstance(code, str) or not code:
            raise TachibanaParseError("missing sIssueCode")
        if code in by_code:
            duplicates += 1
        by_code[code] = item
    if duplicates:
        raise TachibanaParseError(f"duplicate sIssueCode: {duplicates}")
    return by_code


def build_rows(
    prices: Iterable[Mapping[str, str]], selection: Selection, created_at: datetime, run: RunMeta
) -> dict[str, list[dict]]:
    """時価（aCLMMfdsMarketPrice の要素）と選んだ銘柄から 3 テーブルの行を作る。created_at は全行同じ（取得の時刻）。

    選んだ銘柄が prices に無い・sIssueCode が重複するときは TachibanaParseError。選んでいない銘柄は無視する。
    option の行の並びは selection.options の順
    """
    _require_aware(created_at, "created_at")
    by_code = _index(prices)
    missing = [c for c in selection.codes() if c not in by_code]
    if missing:
        raise TachibanaParseError(f"selected issues missing in prices: {len(missing)}")
    meta = {
        "ingest_run_id": run.ingest_run_id,
        "parser_version": run.parser_version,
        "source_object": run.source_object,
    }

    def common(item: Mapping[str, str]) -> dict:
        return {
            "price_time": complete_price_time(_text(item, FIELD_PRICE_TIME), created_at),
            "created_at": created_at,
            "page_updated_at": None,
            "price_time_raw": None,
            **meta,
        }

    def float_prices(item: Mapping[str, str]) -> dict:
        return {
            "price": _float(item, FIELD_PRICE),
            "diff": _float(item, FIELD_DIFF),
            "diff_rate": _float(item, FIELD_DIFF_RATE),
            "hv": None,
            **common(item),
        }

    spot = by_code[selection.spot_code]
    future = by_code[selection.future.code]
    options = []
    for o in selection.options:
        item = by_code[o.code]
        options.append(
            {
                "type": o.type,
                "target_price": o.target_price,
                "last_trading_day": o.last_trading_day,
                "price": _int(item, FIELD_PRICE),
                "diff": _int(item, FIELD_DIFF),
                "diff_rate": _float(item, FIELD_DIFF_RATE),
                "bid": _int(item, FIELD_BID),
                "bid_volume": _int(item, FIELD_BID_VOLUME),
                "ask": _int(item, FIELD_ASK),
                "ask_volume": _int(item, FIELD_ASK_VOLUME),
                "volume": _int(item, FIELD_VOLUME),
                "positions": _int(item, FIELD_POSITIONS),
                **{c: None for c in OPTION_NULL_COLUMNS},
                **common(item),
            }
        )
    return {
        "spot_price": [float_prices(spot)],
        "future_price": [{**float_prices(future), "contract_month": selection.future.contract_month}],
        "option_price": options,
    }

from enum import Enum
from dataclasses import dataclass, field

from dataclasses_json import dataclass_json
from datetime import datetime
from pytz import timezone
from marshmallow import fields
from my_logging import getLogger


TZ_JST = timezone('Asia/Tokyo')
TZ_UTC = timezone('UTC')

log = getLogger(__name__)


# タイムゾーン未指定のdatetimeはJSTとしてシリアライズ、
# タイムゾーン未指定の日付文字列はUTCとして aware な datetime に
# デシリアライズするための marshmallow の fields.DateTime のサブクラス。
class MyDateTime(fields.DateTime):

    def _serialize(self, value, attr, obj, **kwargs):
        if value is not None and (value.tzinfo is None or value.tzinfo.utcoffset(value) is None):
            # native -> aware
            value = TZ_JST.localize(value)

        return super()._serialize(value, attr, obj, **kwargs)

    def _deserialize(self, value, attr, data, **kwargs):

        if value is None:
            return super()._deserialize(value, attr, data, **kwargs)

        d = super()._deserialize(value, attr, data, **kwargs)

        if d.tzinfo is None or d.tzinfo.utcoffset(d) is None:

            # native -> aware
            d = TZ_UTC.localize(d)

        return d


# オプション種別
class OptionType(Enum):
    CALL = 1
    PUT = 2


# dataclasses_json の from_json で使うデシリアライザ
# タイムゾーン未指定の日付文字列はJSTとして扱う
def __deserialize_datetime_json(str):
    if str is None:
        return None

    d = datetime.fromisoformat(str)

    if d.tzinfo is None or d.tzinfo.utcoffset(d) is None:
        # タイムゾーン情報が無い場合はJST決め打ち
        d = TZ_JST.localize(d)

    return d


# BigQueryのTIMESTAMP型カラムに格納するフィールド
# TIMESTAMP型はタイムゾーン情報も格納できるので素直にisoフォーマット文字列へ変換
timestamp_field_metadata = {'dataclasses_json': {
            'encoder': lambda x: datetime.isoformat(x) if x is not None else None,
            'decoder': datetime.fromisoformat,
        }}

# BigQueryのDATETIME型カラムに格納する型。
# タイムゾーン情報を保存できないのでJST(+09:00)に決め打ちで変換してからisoフォーマットへ変換
datetime_field_metadata = {'dataclasses_json': {
    'encoder': lambda x: datetime.isoformat(x.astimezone(TZ_JST))[:-6] if x is not None else None,
    'decoder': __deserialize_datetime_json,
    'mm_field': MyDateTime(format='iso', allow_none=True)
}}

# BigQueryのDATE型カラムに格納する型。
# タイムゾーン情報を保存できないのでJST(+09:00)に決め打ち、
# 時刻は 00:00:00 に決め打ちで変換してからisoフォーマットへ変換。
date_field_metadata = {'dataclasses_json': {
    'encoder': lambda x: datetime.isoformat(x.astimezone(TZ_JST))[:10] if x is not None else None,
    'decoder': __deserialize_datetime_json,
    'mm_field': MyDateTime(format='iso', allow_none=True)
}}

# オプション種別EnumをJSONにシリアライズするための
option_type_field_metadata = {'dataclasses_json': {
    'encoder': lambda x: x.value if x is not None else None,
    'decoder': lambda x: OptionType(x) if x is not None else None,
}}


@dataclass_json
@dataclass
class SpotPrice:
    price: float = None
    price_time: datetime = field(default=None, metadata=datetime_field_metadata)
    diff: float = None
    diff_rate: float = None
    hv: float = None
    created_at: datetime = field(default=None, metadata=timestamp_field_metadata)


@dataclass_json
@dataclass
class FuturePrice:
    price: float = None
    price_time: datetime = field(default=None, metadata=datetime_field_metadata)
    diff: float = None
    diff_rate: float = None
    hv: float = None
    contract_month: datetime = field(default=None, metadata=date_field_metadata)
    created_at: datetime = field(default=None, metadata=timestamp_field_metadata)


@dataclass_json
@dataclass
class OptionPrice:
    type: OptionType = field(default=None, metadata=option_type_field_metadata)
    target_price: int = None
    is_atm: bool = False
    price: int = None
    price_time: datetime = field(default=None, metadata=datetime_field_metadata)
    diff: int = None
    diff_rate: float = None
    iv: float = None
    bid: int = None
    bid_volume: int = None
    bid_iv: float = None
    ask: int = None
    ask_volume: int = None
    ask_iv: float = None
    volume: int = None
    positions: int = None
    quotation: int = None
    quotation_date: datetime = field(default=None, metadata=date_field_metadata)
    delta: float = None
    gamma: float = None
    theta: float = None
    vega: float = None
    last_trading_day: datetime = field(default=None, metadata=date_field_metadata)
    created_at: datetime = field(default=None, metadata=timestamp_field_metadata)

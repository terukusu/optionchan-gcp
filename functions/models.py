from enum import Enum
from dataclasses import dataclass, field

from dataclasses_json import dataclass_json
from datetime import datetime
from pytz import timezone


# オプション種別
class OptionType(Enum):
    CALL = 1
    PUT = 2


TZ_JST = timezone('Asia/Tokyo')

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
            'decoder': lambda x: TZ_JST.localize(datetime.fromisoformat(x)),
        }}

# BigQueryのDATE型カラムに格納する型。
# タイムゾーン情報を保存できないのでJST(+09:00)に決め打ち、
# 時刻は 00:00:00 に決め打ちで変換してからisoフォーマットへ変換。
date_field_metadata = {'dataclasses_json': {
            'encoder': lambda x: datetime.isoformat(x.astimezone(TZ_JST))[:10] if x is not None else None,
            'decoder': lambda x: TZ_JST.localize(datetime.fromisoformat(x)),
        }}

# オプション種別EnumをJSONにシリアライズするための
option_type_field_metadata = {'dataclasses_json': {
            'encoder': lambda x: x.value if x is not None else None,
            'decoder': lambda x: OptionType(x) if x is not None else None,
        }}


@dataclass_json
@dataclass
class SpotPrice:
    price: float
    price_time: datetime = field(metadata=datetime_field_metadata)
    diff: float
    diff_rate: float
    hv: float
    created_at: datetime = field(metadata=timestamp_field_metadata)


@dataclass_json
@dataclass
class FuturePrice:
    price: float
    price_time: datetime = field(metadata=datetime_field_metadata)
    diff: float
    diff_rate: float
    hv: float
    contract_month: datetime = field(metadata=date_field_metadata)
    created_at: datetime = field(metadata=timestamp_field_metadata)


@dataclass_json
@dataclass
class OptionPrice:
    type: OptionType = field(metadata=option_type_field_metadata)
    target_price: int
    is_atm: bool = field(init=False, default=False)
    price: int = field(init=False, default=None)
    price_time: datetime = field(metadata=datetime_field_metadata, init=False, default=None)
    diff: int = field(init=False, default=None)
    diff_rate: float = field(init=False, default=None)
    iv: float = field(init=False, default=None)
    bid: int = field(init=False, default=None)
    bid_volume: int = field(init=False, default=None)
    bid_iv: float = field(init=False, default=None)
    ask: int = field(init=False, default=None)
    ask_volume: int = field(init=False, default=None)
    ask_iv: float = field(init=False, default=None)
    volume: int = field(init=False, default=None)
    positions: int = field(init=False, default=None)
    quotation: int = field(init=False, default=None)
    quotation_date: datetime = field(metadata=date_field_metadata, init=False, default=None)
    delta: float = field(init=False, default=None)
    gamma: float = field(init=False, default=None)
    theta: float = field(init=False, default=None)
    vega: float = field(init=False, default=None)
    last_trading_day: datetime = field(metadata=date_field_metadata)
    created_at: datetime = field(metadata=timestamp_field_metadata)

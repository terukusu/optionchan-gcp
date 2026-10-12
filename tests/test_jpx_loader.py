import json
import os
from datetime import datetime, timedelta

import pytest
from pytz import timezone

from jpx_loader import TZ_JST, build_price_time, parse_jpx_html, parse_option
from models import OptionType

FIXTURE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fixtures', 'jpx')


def jst(year, month, day, hour=0, minute=0):
    return TZ_JST.localize(datetime(year, month, day, hour, minute))


def assert_jst_datetime(actual, expected):
    assert actual == expected
    # 値だけでなく JST の aware datetime であること (JSON 化で JST に変換されるため必須)
    assert actual.utcoffset() == timedelta(hours=9)
    assert actual.replace(tzinfo=None) == expected.replace(tzinfo=None)


# (created_at, ページ表示の MM/DD HH:MM, 期待する約定日時)
PRICE_TIME_CASES = [
    pytest.param(jst(2026, 10, 12, 10, 46), '10/12 10:40', jst(2026, 10, 12, 10, 40), id='same_day'),
    pytest.param(jst(2026, 10, 12, 10, 46), '10/10 05:09', jst(2026, 10, 10, 5, 9), id='two_days_ago'),
    pytest.param(jst(2026, 10, 13, 0, 30), '10/12 23:59', jst(2026, 10, 12, 23, 59), id='night_session_cross_midnight'),
    pytest.param(jst(2027, 1, 4, 8, 46), '12/30 15:45', jst(2026, 12, 30, 15, 45), id='year_boundary'),
    pytest.param(jst(2026, 10, 12, 10, 46), '10/12 10:47', jst(2026, 10, 12, 10, 47), id='one_minute_after_update'),
    pytest.param(jst(2026, 10, 12, 23, 59), '10/13 00:00', jst(2026, 10, 13, 0, 0), id='display_next_day'),
    pytest.param(jst(2026, 12, 31, 23, 59), '01/01 00:00', jst(2027, 1, 1, 0, 0), id='display_next_year'),
]


def split_display(display):
    # 'MM/DD HH:MM' -> (month, day, hour, minute)
    date_str, time_str = display.split(' ')
    month, day = map(int, date_str.split('/'))
    hour, minute = map(int, time_str.split(':'))
    return month, day, hour, minute


@pytest.mark.parametrize('created_at, display, expected', PRICE_TIME_CASES)
def test_build_price_time(created_at, display, expected):
    actual = build_price_time(created_at, *split_display(display))
    assert_jst_datetime(actual, expected)


def test_build_price_time_uses_jst_date_of_created_at():
    # UTC 2026-10-12 01:46 = JST 2026-10-12 10:46
    created_at = timezone('UTC').localize(datetime(2026, 10, 12, 1, 46))
    actual = build_price_time(created_at, 10, 12, 10, 40)
    assert_jst_datetime(actual, jst(2026, 10, 12, 10, 40))


def test_build_price_time_skips_nonexistent_leap_day():
    # 2/29 は閏年の候補だけが有効 (前年 2027・翌年 2029 は平年なので候補から外れる)
    actual = build_price_time(jst(2028, 3, 1, 9, 0), 2, 29, 15, 45)
    assert_jst_datetime(actual, jst(2028, 2, 29, 15, 45))


def test_build_price_time_invalid_date_raises():
    with pytest.raises(ValueError):
        build_price_time(jst(2026, 10, 12, 10, 46), 2, 30, 10, 0)


def make_option_info(created_at, price_text):
    # parse_jpx_html が parse_option に渡す 18 要素 (カンマ除去・strip 済み) を組み立てる。値は架空
    return [
        created_at,
        OptionType.CALL,
        '40000',
        False,
        price_text,
        '-10 -0.98%',
        '24.50%',
        '1020 (5) 1000 (3)',
        '25.00% 24.00%',
        '30',
        '1200',
        '1010',
        jst(2026, 10, 9),
        '0.5000',
        '0.0001',
        '-10.0000',
        '40.0000',
        '2026/11/12',
    ]


@pytest.mark.parametrize('created_at, display, expected', PRICE_TIME_CASES)
def test_parse_option_price_time(created_at, display, expected):
    option = parse_option(make_option_info(created_at, '1000 \n' + display))
    assert option.price == 1000
    assert_jst_datetime(option.price_time, expected)
    # price_time 以外は従来どおり
    assert option.created_at == created_at
    assert option.target_price == 40000
    assert option.diff == -10
    assert option.iv == 24.5


def test_parse_option_no_price():
    option = parse_option(make_option_info(jst(2026, 10, 12, 10, 46), '- \n-'))
    assert option.price is None
    assert option.price_time is None


def test_parse_option_price_time_json():
    # BigQuery の DATETIME に入る値: JST・オフセットなし
    option = parse_option(make_option_info(jst(2026, 10, 12, 10, 46), '1000 10/10 05:09'))
    assert json.loads(option.to_json())['price_time'] == '2026-10-10T05:09:00'


@pytest.fixture(scope='module')
def jpx_info():
    with open(os.path.join(FIXTURE_DIR, 'nkopm_minimal.html'), mode='r', encoding='UTF-8') as f:
        return parse_jpx_html(f.read())


def test_parse_jpx_html_spot_future_price_time(jpx_info):
    assert_jst_datetime(jpx_info.created_at, jst(2026, 10, 12, 10, 46))
    assert jpx_info.spot_price.price == 39876.54
    assert_jst_datetime(jpx_info.spot_price.price_time, jst(2026, 10, 9, 15, 45))
    assert jpx_info.future_price.price == 39870
    assert_jst_datetime(jpx_info.future_price.price_time, jst(2026, 10, 12, 10, 45))


def test_parse_jpx_html_diff_with_comma(jpx_info):
    assert jpx_info.spot_price.diff == -1234.56
    assert jpx_info.spot_price.diff_rate == -3.0
    assert jpx_info.spot_price.hv == 20.0
    assert jpx_info.future_price.diff == -1230
    assert isinstance(jpx_info.future_price.diff, int)
    assert jpx_info.future_price.diff_rate == -2.99
    assert jpx_info.future_price.hv == 19.5


def test_parse_jpx_html_option_price_time(jpx_info):
    assert len(jpx_info.call_option_list) == 1
    assert len(jpx_info.put_option_list) == 1

    call = jpx_info.call_option_list[0]
    assert call.target_price == 40000
    assert call.price == 1000
    assert_jst_datetime(call.price_time, jst(2026, 10, 10, 5, 9))

    put = jpx_info.put_option_list[0]
    assert put.target_price == 40000
    assert put.price == 1100
    assert_jst_datetime(put.price_time, jst(2026, 10, 9, 22, 42))

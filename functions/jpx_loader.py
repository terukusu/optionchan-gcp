"""
日本取引所グループのウェブサイトから日経225オプションの価格をダウンロードするためのモジュールです。
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from pytz import timezone
from typing import List
import re
import requests

from pyquery import PyQuery as pq

from models import OptionPrice, OptionType, FuturePrice, SpotPrice
from my_logging import getLogger


# JPXのHTMLに含まれるオプション価格情報を表すデータクラス
@dataclass
class JpxOptionPriceInfo:
    spot_price: SpotPrice
    future_price: FuturePrice
    call_option_list: List[OptionPrice]
    put_option_list: List[OptionPrice]
    created_at: datetime


REGEX_PRICE = re.compile(r'([\d\.]+)\(\d{2}/\d{2} (\d{2}:\d{2})\)')
REGEX_OPTION_PRICE = re.compile(r'([0-9\.]+)\s*\d{2}/\d{2} (\d{2}:\d{2})')
REGEX_ORDER = re.compile(r'([\d\-]+)\s*\(([\d\-]+)\)\s*([\d\-]+)\s*\(([\d\-]+)\)')
REGEX_DIFF = re.compile(r'([+\-\d\.]+)\s*([+\-\d\.]+)%')
REGEX_ORDER_IV = re.compile(r'(?:\-|([\d\.]+)%)\s*(?:\-|([\d\.]+)%)')
REGEX_CONTRACT_MONTH = re.compile(r'(\d+)年(\d+)月')
TZ_JST = timezone('Asia/Tokyo')


# 期近、次限月、更に先
JPX_URL_NEARBY_1ST = 'https://svc.qri.jp/jpx/nkopm/'
JPX_URL_NEARBY_2ND = 'https://svc.qri.jp/jpx/nkopm/1'
JPX_URL_NEARBY_3RD = 'https://svc.qri.jp/jpx/nkopm/2'

log = getLogger(__name__)


def load_html_from_file(file_path):
    # ローカルファイルWebからHTMLをロード
    log.debug('loading jpx html from file: %s', file_path)

    with open(file_path, mode="r", encoding='UTF-8') as f:
        html = f.read()

    return html


def load_html_from_web(url):
    # WebからHTMLをロード

    headers = {
        'Referer': 'https://svc.qri.jp/jpx/nkopm/2',
        'Pragma': 'no-cache',
        'Cache-Control': 'no-cache'
    }

    response = requests.get(url, headers=headers)

    html = response.content

    return html

#
# オプションに関する情報を更に細かく解析します
#
# option_info の内容
#  0: 更新日時
#  1: CALL or PUT
#  2: ターゲット価格
#  3: ATMかどうか
#  4: 現在値
#  5: 前日比
#  6: IV
#  7: 売気配(数量) 買気配(数量)
#  8: 売気配IV 買気配IV
#  9: 取引高
# 10: 建玉残
# 11: 清算値
# 12: 清算日
# 13: デルタ
# 14: ガンマ
# 15: セータ
# 16: ベガ
# 17: 取引最終日
def parse_option(option_info):
    # option_infoを順番に走査するためのカーソル
    seq = iter(range(len(option_info)))

    # 更新日時
    created_at = option_info[next(seq)]

    # call or put
    option_type = option_info[next(seq)]

    # ターゲット価格
    target_price = int(option_info[next(seq)])

    # ATMかどうか
    is_atm = option_info[next(seq)]

    # price
    m = REGEX_OPTION_PRICE.search(option_info[next(seq)])

    price = None
    price_time = None

    if m is not None:
        price = int(m.group(1))
        price_time_str = m.group(2)
        [pt_hour, pt_minute] = list(map(lambda x: int(x), price_time_str.split(':')))
        pt_year = created_at.year
        pt_month = created_at.month
        pt_day = created_at.day
        price_time = TZ_JST.localize(datetime(pt_year, pt_month, pt_day, pt_hour, pt_minute))
        if price_time > created_at:
            #  未来時刻ということは日マタギということなので日付を1日戻しておく
            price_time -= timedelta(days=1)

    # 前日比
    m = REGEX_DIFF.search(option_info[next(seq)])
    diff = None
    diff_rate = None

    if m is not None:
        diff_str = m.group(1)
        diff = int(diff_str) if diff_str != '-' else None

        diff_rate_str = m.group(2)
        diff_rate = float(diff_rate_str) if diff_rate_str != '-' else None

    # IV
    iv_str = option_info[next(seq)].replace('%', '')
    iv = float(iv_str) if iv_str != '-' else None

    # 売気配, 買気配
    m = REGEX_ORDER.search(option_info[next(seq)])

    bid = None
    bid_volume = None
    ask = None
    ask_volume = None

    if m is not None:
        ask_str = m.group(1)
        ask = int(ask_str) if ask_str != '-' else None

        ask_volume_str = m.group(2)
        ask_volume = int(ask_volume_str) if ask_volume_str != '-' else None

        bid_str = m.group(3)
        bid = int(bid_str) if bid_str != '-' else None

        bid_volume_str = m.group(4)
        bid_volume = int(bid_volume_str) if bid_volume_str != '-' else None

    # 売気配IV, 買気配IV
    m = REGEX_ORDER_IV.search(option_info[next(seq)])
    ask_iv_str = m.group(1)
    ask_iv = float(ask_iv_str) if ask_iv_str is not None else None
    bid_iv_str = m.group(2)
    bid_iv = float(bid_iv_str) if bid_iv_str is not None else None

    # 出来高
    volume_str = option_info[next(seq)]
    volume = int(volume_str) if volume_str != '-' else None

    # 建玉数
    positions_str = option_info[next(seq)]
    positions = int(positions_str) if positions_str != '-' else None

    # 清算値
    quotation_str = option_info[next(seq)]
    quotation = int(quotation_str) if quotation_str != '-' else None

    # 精算日
    quotation_date = option_info[next(seq)]

    # delta
    delta_str = option_info[next(seq)]
    delta = float(delta_str) if delta_str != '-' else None

    # gamma
    gamma_str = option_info[next(seq)]
    gamma = float(gamma_str) if gamma_str != '-' else None

    # theta
    theta_str = option_info[next(seq)]
    theta = float(theta_str) if theta_str != '-' else None

    # vega
    vega_str = option_info[next(seq)]
    vega = float(vega_str) if vega_str != '-' else None

    # 取引最終日
    last_trading_day_str = option_info[next(seq)]
    last_trading_day = TZ_JST.localize(datetime.strptime(last_trading_day_str, "%Y/%m/%d"))

    option = OptionPrice(
        type=option_type,
        target_price=target_price,
        last_trading_day=last_trading_day,
        created_at=created_at
    )

    option.is_atm = is_atm
    option.price = price
    option.price_time = price_time
    option.diff = diff
    option.diff_rate = diff_rate
    option.iv = iv
    option.bid = bid
    option.bid_volume = bid_volume
    option.bid_iv = bid_iv
    option.ask = ask
    option.ask_volume = ask_volume
    option.ask_iv = ask_iv
    option.volume = volume
    option.positions = positions
    option.quotation = quotation
    option.quotation_date = quotation_date
    option.delta = delta
    option.gamma = gamma
    option.theta = theta
    option.vega = vega

    return option


# JPX形式のHTMLを解析しします
def parse_jpx_html(html):

    q = pq(html, parser='html')

    # 更新時刻
    created_at_str = q.find('.update-time').find('dd').text()
    created_at = TZ_JST.localize(datetime.strptime(created_at_str, "%Y/%m/%d %H:%M"))

    price_info = q.find('#priceInfo')

    # 現物価格情報
    spot_price_web = price_info.find('tr').filter(lambda idx, e: pq(e).find('td').eq(0).text().find('日経平均株価') > -1)
    spot_price_str = spot_price_web.find('td').eq(1).text().replace(',', '')
    m = REGEX_PRICE.search(spot_price_str)
    spot_price = None
    spot_price_time = None

    if m is not None:
        spot_price = float(m.group(1))
        spot_price_time_str = m.group(2)
        [spt_hour, spt_minute] = list(map(lambda x: int(x), spot_price_time_str.split(':')))
        spt_year = created_at.year
        spt_month = created_at.month
        spt_day = created_at.day
        spot_price_time = TZ_JST.localize(datetime(spt_year, spt_month, spt_day, spt_hour, spt_minute))

        if spot_price_time > created_at:
            # 未来日ということは日マタギなので１日戻しておく
            spot_price_time -= timedelta(days=1)

    spot_price_diff_str = spot_price_web.find('td').eq(2).text()
    spot_price_diff = float(spot_price_diff_str) if spot_price_diff_str != '-' else None

    spot_price_diff_rate_str = spot_price_web.find('td').eq(3).text().replace('%', '')
    spot_price_diff_rate = float(spot_price_diff_rate_str) if spot_price_diff_rate_str != '-' else None

    spot_price_hv_str = spot_price_web.find('td').eq(4).text().replace('%', '')
    spot_price_hv = float(spot_price_hv_str) if spot_price_hv_str != '-' else None

    spot_price_info = SpotPrice(
        spot_price,
        spot_price_time,
        spot_price_diff,
        spot_price_diff_rate,
        spot_price_hv,
        created_at,
    )

    # 先物価格情報
    future_price_web = price_info.find('tr').filter(lambda idx, e: pq(e).find('td').eq(0).text().find('先物') > -1)
    future_price_str = future_price_web.find('td').eq(1).text().replace(',', '')

    future_price = None
    future_price_time = None

    m = REGEX_PRICE.search(future_price_str)
    if m is not None:
        future_price = int(m.group(1))
        future_price_time_str = m.group(2)
        [fpt_hour, fpt_minute] = list(map(lambda x: int(x), future_price_time_str.split(':')))
        fpt_year = created_at.year
        fpt_month = created_at.month
        fpt_day = created_at.day
        future_price_time = TZ_JST.localize(datetime(fpt_year, fpt_month, fpt_day, fpt_hour, fpt_minute))
        if future_price_time > created_at:
            # 未来日ということは日マタギなので１日戻しておく
            future_price_time -= timedelta(days=1)

    future_price_diff_str = future_price_web.find('td').eq(2).text()
    future_price_diff = int(future_price_diff_str) if future_price_diff_str != '-' else None

    future_price_diff_rate_str = future_price_web.find('td').eq(3).text().replace('%', '')
    future_price_diff_rate = float(future_price_diff_rate_str) if future_price_diff_rate_str != '-' else None

    future_price_hv_str = future_price_web.find('td').eq(4).text().replace('%', '')
    future_price_hv = float(future_price_hv_str) if future_price_hv_str != '-' else None

    # 先物の限月
    m = REGEX_CONTRACT_MONTH.search(future_price_web.find('td').eq(0).text())
    future_contract_year = int(m.group(1)) + int(created_at.year - created_at.year % 1000)
    future_contract_month = int(m.group(2))
    future_contract_date = TZ_JST.localize(datetime(future_contract_year, future_contract_month, 1))
    if future_contract_date < created_at:
        # 過去の日付になってしまうということは限月が来年のものということなので1年プラスしておく
        future_contract_date = TZ_JST.localize(datetime(future_contract_year + 1, future_contract_month, 1))

    future_price_info = FuturePrice(
        future_price,
        future_price_time,
        future_price_diff,
        future_price_diff_rate,
        future_price_hv,
        future_contract_date,
        created_at,
    )

    # 精算日(SQではない)
    quotation_date_text = q.find('.price-info-header').find('tr').eq(1).find('th').eq(0).text()
    m = re.search(r'(\d+)/(\d+)', quotation_date_text)
    qd_year = datetime.now().year
    qd_month = int(m.group(1))
    qd_day = int(m.group(2))
    qd = TZ_JST.localize(datetime(qd_year, qd_month, qd_day))
    if qd > created_at:
        # 年またぎ考慮
        qd = TZ_JST.localize(datetime(qd_year - 1, qd_month, qd_day))

    quotation_date = qd

    # print('quotation date: {}'.format(quotation_date))

    # 取引最終日
    last_trading_day_str = q.find('.date-table.last-tradingday').find('dd').text()

    # print('last trading day: {}'.format(last_trading_day_str))

    # オプション情報のHTMLからテキストで情報を抽出
    option_price_info = q.find('.price-info-scroll')

    # 価格, iv etc
    row_text_list = []
    option_price_info.find('.row-num').each(lambda idx, e: row_text_list.append(
        list(pq(e).find('td').map(lambda idx2, e: pq(e).text().strip().replace(',', '')))))

    # ギリシャ指標
    greeks_text_list = []
    option_price_info.find(".greek").each(lambda idx, e: greeks_text_list.append(
        list(pq(e).find('table').find('td').map(lambda idx2, e: pq(e).text().replace(',', '')))))

    call_option_list = []
    put_option_list = []

    for i in range(len(row_text_list)):
        row = row_text_list[i]
        greeks = greeks_text_list[i]

        target_info = row[8]
        is_atm = (target_info.find('A T M') >= 0)
        target_price = re.search('([0-9]+)', target_info).group(1)

        call_info = [created_at, OptionType.CALL, target_price, is_atm]
        call_info.extend(list(reversed(row[:8])))
        call_info.append(quotation_date)
        call_info.extend(greeks[:4])
        call_info.append(last_trading_day_str)

        call_option = parse_option(call_info)
        call_option_list.append(call_option)

        put_info = [created_at, OptionType.PUT, target_price, is_atm]
        put_info.extend(row[-8:])
        put_info.append(quotation_date)
        put_info.extend(greeks[-4:])
        put_info.append(last_trading_day_str)

        put_option = parse_option(put_info)
        put_option_list.append(put_option)

    result = JpxOptionPriceInfo(spot_price_info, future_price_info, call_option_list, put_option_list, created_at)

    return result


# 直近 1限月の価格情報を取得します
def load_jpx_nearby_month():
    html = load_html_from_web(JPX_URL_NEARBY_1ST)
    return parse_jpx_html(html)


# 直近 2限月の価格情報を取得します
def load_jpx_nearby_month_2nd():
    html = load_html_from_web(JPX_URL_NEARBY_2ND)
    return parse_jpx_html(html)


# 直近 次次限月の先物限月のオプション価格報を取得します
def load_jpx_nearby_month_3rd():
    html = load_html_from_web(JPX_URL_NEARBY_3RD)
    return parse_jpx_html(html)

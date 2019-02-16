import pandas as pd

from google.cloud import bigquery
from google.cloud import datastore
from datetime import datetime, timedelta
from pytz import timezone

import models
from my_logging import getLogger
from config import Config

log = getLogger(__name__)
config = Config()

TZ_JST = timezone('Asia/Tokyo')
TZ_UTC = timezone('UTC')


# これだけは Cloud Datastore からデータとってくるやつ。
def find_latest_future_price():
    client = datastore.Client()

    with client.transaction():
        key = client.key(config.gcp_ds_kind, config.gcp_ds_key_id)
        latest_future_price = client.get(key)

    if latest_future_price is None:
        return None
    latest_future_price_json = models.FuturePrice.schema().dump(latest_future_price)
    future_price = models.FuturePrice.schema().load(latest_future_price_json)

    return future_price


#
# 最新先物価格のcreated_atと同時刻のcreated_atを持つオプション価格のリストを取得します。
# 返ってくるカラム
# target_price                   int64
# o1_call_iv                   float64
# o1_call_price_time    datetime64[ns]
# o1_put_iv                    float64
# o1_put_price_time     datetime64[ns]
# o1_put_is_atm                 object
# o2_call_iv                   float64
# o2_call_price_time    datetime64[ns]
# o2_put_iv                    float64
# o2_put_price_time     datetime64[ns]
# o2_put_is_atm                 object
def find_option_price_by_created_at(created_at):

    client = bigquery.Client()
    created_at_str = datetime.isoformat(created_at)
    table = f'{config.gcp_bq_dataset_name}.option_price'

    query = (f'''
        WITH t AS(
            SELECT max(last_trading_day) l_max,  min(last_trading_day)  l_min
            FROM {table} WHERE created_at = "{created_at_str}"
        )
        SELECT target_price,
            MAX(CASE WHEN last_trading_day = (SELECT l_min FROM t) AND type = 1 THEN iv END) AS o1_call_iv,
            MAX(CASE WHEN last_trading_day = (SELECT l_min FROM t) AND type = 1 THEN price_time END) AS o1_call_price_time,
            MAX(CASE WHEN last_trading_day = (SELECT l_min FROM t) AND type = 2 THEN iv END) AS o1_put_iv,
            MAX(CASE WHEN last_trading_day = (SELECT l_min FROM t) AND type = 2 THEN price_time END) AS o1_put_price_time,
            MAX(CASE WHEN last_trading_day = (SELECT l_min FROM t) AND type = 2 THEN is_atm END) AS o1_put_is_atm,
            MAX(CASE WHEN last_trading_day = (SELECT l_max FROM t) AND type = 1 THEN iv END) AS o2_call_iv,
            MAX(CASE WHEN last_trading_day = (SELECT l_max FROM t) AND type = 1 THEN price_time END) AS o2_call_price_time,
            MAX(CASE WHEN last_trading_day = (SELECT l_max FROM t) AND type = 2 THEN iv END) AS o2_put_iv,
            MAX(CASE WHEN last_trading_day = (SELECT l_max FROM t) AND type = 2 THEN price_time END) AS o2_put_price_time,
            MAX(CASE WHEN last_trading_day = (SELECT l_max FROM t) AND type = 2 THEN is_atm END) AS o2_put_is_atm
        FROM {table} WHERE created_at = "{created_at_str}" GROUP BY target_price ORDER BY target_price'''
    )

    query_job = client.query(query)
    rows = query_job.result()

    df = rows.to_dataframe()

    return df


# target_date: この日の前７日間のデータを返す. aware なものを渡してください。
def find_recent_iv_and_price_of_atm_options(target_date):

    last_trading_day_from = target_date.astimezone(TZ_JST).strftime('%Y-%m-%d')
    created_at_from = (target_date - timedelta(days=7)).isoformat()
    table = f'{config.gcp_bq_dataset_name}.option_price'

    query = (f'''
    WITH t1 AS (
        SELECT min(last_trading_day) AS l_min FROM `{table}`
        WHERE last_trading_day >= "{last_trading_day_from}"
    ),
    t2 AS (
        SELECT
            target_price, iv, price, TIMESTAMP_SECONDS(CAST(TRUNC(UNIX_SECONDS(created_at)/300) AS INT64) * 300) AS time 
        FROM
            `{table}`
        WHERE
            created_at > "{created_at_from}" AND last_trading_day=(SELECT l_min FROM t1) AND is_atm = TRUE
    )
    SELECT
        UNIX_SECONDS(time) AS time, ROUND(AVG(target_price), 1) AS target_price, 
        ROUND(AVG(iv), 1) AS iv, ROUND(AVG(price), 1) AS price
    FROM t2 GROUP BY time ORDER BY time
    ''')

    result = __do_bq_query(query)
    return result


# 文字列でqueryを受け取って、pandas.DataFrameを返す
def __do_bq_query(query):
    client = bigquery.Client()
    query_job = client.query(query)
    rows = query_job.result()

    return rows.to_dataframe()

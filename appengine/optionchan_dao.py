from google.cloud import bigquery
from google.cloud import datastore
from datetime import datetime

import models
from my_logging import getLogger
from config import Config

log = getLogger(__name__)
config = Config()


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

    # 値リストのリストにしちゃう
    return rows.to_dataframe()

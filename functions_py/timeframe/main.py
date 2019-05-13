from google.cloud import bigquery

import config
from my_logging import getLogger

log = getLogger(__name__)
config = config.Config()


def insert_ohlc_iv():
    from_table = f'{config.gcp_bq_dataset_name}.option_price'
    to_table = f'{config.gcp_bq_dataset_name}.atm_iv'

    query = (f'''
        CREATE TEMP FUNCTION DT() AS (CURRENT_TIMESTAMP());

        INSERT `{to_table}` (open, high, low, close, last_trading_day, time_frame, started_at) 
        
        WITH t1 as (
            SELECT  
                (array_agg(iv IGNORE NULLS ORDER BY created_at ASC))[OFFSET(0)] open,
                MAX(iv) high,
                MIN(iv) low,
                (array_agg(iv IGNORE NULLS ORDER BY created_at DESC))[OFFSET(0)] close,
                last_trading_day,
                3600 time_frame,
                TIMESTAMP_SECONDS(CAST(TRUNC(UNIX_SECONDS(created_at)/3600) AS INT64) * 3600) AS started_at
            FROM `{from_table}`
            WHERE is_atm=True AND type=2 AND created_at >= TIMESTAMP_TRUNC(TIMESTAMP_ADD(DT(), INTERVAL -1 DAY), DAY) AND created_at < TIMESTAMP_TRUNC(DT(), DAY)
            GROUP BY last_trading_day, started_at
        )        
        SELECT * FROM t1;
    ''')

    client = bigquery.Client()
    query_job = client.query(query)
    query_job.result()


def insert_ohlc_option_price():
    from_table = f'{config.gcp_bq_dataset_name}.option_price'
    to_table = f'{config.gcp_bq_dataset_name}.atm_option_price'

    query = (f'''
        CREATE TEMP FUNCTION DT() AS (CURRENT_TIMESTAMP());

        INSERT `{to_table}` (open, high, low, close, last_trading_day, time_frame, started_at) 

        WITH t1 as (
            SELECT  
                (array_agg(price IGNORE NULLS ORDER BY created_at ASC))[OFFSET(0)] open,
                MAX(price) high,
                MIN(price) low,
                (array_agg(price IGNORE NULLS ORDER BY created_at DESC))[OFFSET(0)] close,
                last_trading_day,
                3600 time_frame,
                TIMESTAMP_SECONDS(CAST(TRUNC(UNIX_SECONDS(created_at)/3600) AS INT64) * 3600) AS started_at
            FROM `{from_table}`
            WHERE is_atm=True AND type=2 AND created_at >= TIMESTAMP_TRUNC(TIMESTAMP_ADD(DT(), INTERVAL -1 DAY), DAY) AND created_at < TIMESTAMP_TRUNC(DT(), DAY)
            GROUP BY last_trading_day, started_at
        )        
        SELECT * FROM t1;
    ''')

    client = bigquery.Client()
    query_job = client.query(query)
    query_job.result()


def insert_ohlc_target_price():
    from_table = f'{config.gcp_bq_dataset_name}.option_price'
    to_table = f'{config.gcp_bq_dataset_name}.atm_target_price'

    query = (f'''
        CREATE TEMP FUNCTION DT() AS (CURRENT_TIMESTAMP());

        INSERT `{to_table}` (open, high, low, close, last_trading_day, time_frame, started_at) 

        WITH t1 as (
            SELECT  
                (array_agg(target_price IGNORE NULLS ORDER BY created_at ASC))[OFFSET(0)] open,
                MAX(target_price) high,
                MIN(target_price) low,
                (array_agg(target_price IGNORE NULLS ORDER BY created_at DESC))[OFFSET(0)] close,
                last_trading_day,
                3600 time_frame,
                TIMESTAMP_SECONDS(CAST(TRUNC(UNIX_SECONDS(created_at)/3600) AS INT64) * 3600) AS started_at
            FROM `{from_table}`
            WHERE is_atm=True AND type=2 AND created_at >= TIMESTAMP_TRUNC(TIMESTAMP_ADD(DT(), INTERVAL -1 DAY), DAY) AND created_at < TIMESTAMP_TRUNC(DT(), DAY)
            GROUP BY last_trading_day, started_at
        )        
        SELECT * FROM t1;
    ''')

    client = bigquery.Client()
    query_job = client.query(query)
    query_job.result()


# entry point of Cloud Functions
# trigger = pubsub
def insert_ohlc(data, context):
    insert_ohlc_iv()
    insert_ohlc_option_price()
    insert_ohlc_target_price()


if __name__ == '__main__':
    insert_ohlc()

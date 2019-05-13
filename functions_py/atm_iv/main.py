from flask import make_response
from google.cloud import bigquery

# my modules
import config
from my_logging import getLogger

log = getLogger(__name__)
config = config.Config()

with open('auth.txt', 'r') as f:
    cf_token = f.readline().rstrip('\r\n')


# entry point of Cloud Functions
# trigger = http
def atm_iv_data(request):
    if not check_auth(request):
        return 'Forbidden', 403

    num_days = request.args.get('d', default='7')

    df = query_atm_iv(num_days)

    # CSV化
    atm_iv_csv = df.to_csv(index=False, header=False)

    res = make_response(atm_iv_csv, 200)
    res.headers['Content-type'] = 'text/csv; charset=utf-8'
    return res


def query_atm_iv(num_days):
    table_iv = f'{config.gcp_bq_dataset_name}.atm_iv'
    table_option_price = f'{config.gcp_bq_dataset_name}.option_price'

    query = (f'''
        WITH t1 AS (
            SELECT
                MIN(last_trading_day) as last_trading_day
            FROM
                {table_iv}
            WHERE
                started_at > TIMESTAMP_TRUNC(TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL -10 DAY), DAY)
                AND last_trading_day >=  CURRENT_DATE('Asia/Tokyo')
        ), t2 AS (
            SELECT
                open,high,low,close, started_at
            FROM
                {table_iv}
            WHERE
                last_trading_day=(SELECT t1.last_trading_day FROM t1)
                AND started_at >= TIMESTAMP_TRUNC(TIMESTAMP_ADD(CURRENT_TIMESTAMP() , INTERVAL -{num_days} DAY), DAY)
        ), t3 AS (
            -- まだ時間足集計が保存されていない分(=UTCでの当日分)の時間足を集計する
            SELECT  
                (array_agg(iv IGNORE NULLS ORDER BY created_at ASC))[OFFSET(0)] open,
                MAX(iv) high,
                MIN(iv) low,
                (array_agg(iv IGNORE NULLS ORDER BY created_at DESC))[OFFSET(0)] close,
                TIMESTAMP_SECONDS(CAST(TRUNC(UNIX_SECONDS(created_at)/3600) AS INT64) * 3600) AS started_at
            FROM
                {table_option_price}
            WHERE
                created_at >= TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)
                AND is_atm=True AND type=2 AND last_trading_day=(SELECT t1.last_trading_day FROM t1)
            GROUP BY started_at
        )
        SELECT
            open, high, low, close, started_at
        FROM
            t2

        UNION ALL

        SELECT
            open, high, low, close, started_at
        FROM
            t3
        ORDER BY started_at
    ''')

    client = bigquery.Client()
    query_job = client.query(query)
    rows = query_job.result()
    df = rows.to_dataframe()

    return df


def check_auth(request):
    auth_header = request.headers.get("Authorization")

    req_cf_token = None

    if auth_header is not None:
        req_cf_token = auth_header.split(' ')[1]

    return req_cf_token == cf_token


if __name__ == '__main__':
    from flask import Flask, request

    app = Flask(__name__)


    @app.route('/')
    def index():
        return atm_iv_data(request)


    app.run('127.0.0.1', 8000, debug=True)

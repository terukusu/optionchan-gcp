from flask import make_response
from google.cloud import bigquery

# my modules
import config
from my_logging import getLogger

log = getLogger(__name__)
config = config.Config()

with open('auth.txt', 'r') as f:
    cf_token = f.readline().rstrip('\r\n')


def query_atm_iv():
    table = f'{config.gcp_bq_dataset_name}.atm_iv'

    query = (f'''
        WITH t1 AS (
            SELECT
                MIN(last_trading_day) as last_trading_day
            FROM
                {table}
            WHERE
                started_at > TIMESTAMP_TRUNC(TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL -7 DAY), DAY)
                AND last_trading_day >=  CURRENT_DATE('Asia/Tokyo')
        )
        SELECT
            open,high,low,close, started_at
        FROM
            {table}
        WHERE
            last_trading_day=(SELECT t1.last_trading_day FROM t1)
            AND started_at >= TIMESTAMP_TRUNC(TIMESTAMP_ADD(CURRENT_TIMESTAMP() , INTERVAL -14 DAY), DAY)
    ''')

    client = bigquery.Client()
    query_job = client.query(query)
    rows = query_job.result()
    df = rows.to_dataframe()

    return df


# entry point of Cloud Functions
# trigger = http
def atm_iv(request):
    if not check_auth(request):
        return 'Forbidden', 403

    df = query_atm_iv()

    # CSV化
    atm_iv_csv = df.to_csv(index=False, header=False)

    res = make_response(atm_iv_csv, 200)
    res.headers['Content-type'] = 'text/csv; charset=utf-8'
    return res


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
        return atm_iv(request)


    app.run('127.0.0.1', 8000, debug=True)
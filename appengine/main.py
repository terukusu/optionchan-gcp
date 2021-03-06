# 3rd party library
import requests

from flask import Flask, render_template, make_response, request

# my modules
from config import Config

from my_logging import getLogger

config = Config()
log = getLogger(__name__)
app = Flask(__name__)

# TODO Config に逃がす
with open('auth.txt', 'r') as f:
    cf_token = f.readline().rstrip('\r\n')


@app.route('/')
def hello():
    return render_template('index.html')


@app.route('/smile')
def smile():
    return render_template('smile.html')


# スマイルカーブ用のデータをCSVで返す
@app.route('/smile_data')
def smile_data():
    url = f'{config.gcp_cf_url_base}/smile_data'
    option_list_csv = load_content_from_cloud_functions(url)

    res = make_response(option_list_csv, 200)
    res.headers['Content-type'] = 'text/csv; charset=utf-8'
    return res


@app.route('/atm')
def amt():
    return render_template('atm.html')


# ATM IV推移用のデータをCSVで返す
@app.route('/atm_data')
def atm_data():
    url = f'{config.gcp_cf_url_base}/atm_data'
    option_list_csv = load_content_from_cloud_functions(url)

    res = make_response(option_list_csv, 200)
    res.headers['Content-type'] = 'text/csv; charset=utf-8'
    return res


@app.route('/atm_iv')
def atm_iv():
    return render_template('atm_iv.html')


# ATM IV推移用のデータをCSVで返す
@app.route('/atm_iv_data')
def atm_iv_data():
    num_days = request.args.get('d', default='7')
    n_th_contract_month = request.args.get('n', default='0')
    url = f'{config.gcp_cf_url_base}/atm_iv_data?d={num_days}&n={n_th_contract_month}'
    option_list_csv = load_content_from_cloud_functions(url)

    res = make_response(option_list_csv, 200)
    res.headers['Content-type'] = 'text/csv; charset=utf-8'
    return res


# Cloud Functions からコンテンツをロードする
def load_content_from_cloud_functions(url):

    # TODO 認証する

    headers = {
        'Pragma': 'no-cache',
        'Cache-Control': 'no-cache',
        'Authorization': f'Bearer {cf_token}'
    }

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        raise Exception(f'status code from HTTP Cloud Functions is invalid: {response.status_code}')

    return response.content


if __name__ == '__main__':
    # for local dev
    app.run(host='127.0.0.1', port=8080, debug=True)

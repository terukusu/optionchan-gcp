# 3rd party library
from flask import Flask, render_template, make_response

import optionchan_dao as od

from my_logging import getLogger

log = getLogger(__name__)
app = Flask(__name__)


@app.route('/')
def hello():
    return render_template('index.html')


@app.route('/smile')
def smile():
    return render_template('smile.html')


# スマイルカーブ用のデータをCSVで返す
@app.route('/smile_data')
def smile_data():
    future = od.find_latest_future_price()
    df = od.find_option_price_by_created_at(future.created_at)

    log.debug(f'number of matched options: {len(df)}')

    # ATMの行使価格を検索
    o1_atm = df[df['o1_put_is_atm'] == True]['target_price'].iloc[0]

    # 不要カラム削除
    df = df.drop(['o1_put_is_atm', 'o2_put_is_atm'], axis=1)

    # CSV化
    line1 = f'{int(future.created_at.timestamp())},{o1_atm}\n'
    option_list_csv = line1 + df.to_csv(index=False, header=False, date_format='%s')

    res = make_response(option_list_csv, 200)
    res.headers['Content-type'] = 'text/csv; charset=utf-8'
    return res


if __name__ == '__main__':
    # for local dev
    app.run(host='127.0.0.1', port=8080, debug=True)

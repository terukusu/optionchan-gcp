import base64
import gzip
import re

from dataclasses import asdict
from datetime import datetime

# 3rd party modules
import pandas as pd

from flask import make_response
from google.cloud import bigquery
from google.cloud import datastore
from google.cloud import storage
from pytz import timezone

# my modules
import config, jpx_loader, optionchan_dao as od
from my_logging import getLogger

log = getLogger(__name__)
config = config.Config()

# TODO Config に逃がす
with open('auth.txt', 'r') as f:
    cf_token = f.readline().rstrip('\r\n')

TZ_JST = timezone('Asia/Tokyo')
TZ_UTC = timezone('UTC')

# 価格情報を格納するGCS上のファイル名のパターン
REGEX_PRICE_FILE = re.compile(r'((?:spot|future|option)_price)_\d+.json.gz')


# entry point of Cloud Functions
# trigger = http
# スマイルカーブ用のデータをCSVで返す
def smile_data(request):

    if not check_auth(request):
        return 'Forbidden', 403

    future = od.find_latest_future_price()
    df = od.find_option_price_by_created_at(future.created_at)

    log.debug(f'number of matched options: {len(df)}')

    # ATMの行使価格を検索
    o1_atm = df[df['o1_put_is_atm'] == True]['target_price'].iloc[0]

    # 不要カラム削除
    df = df.drop(['o1_put_is_atm', 'o2_put_is_atm'], axis=1)

    # 取引時刻カラムの日付は native JST なのでタイムゾーンを付加してからUnixtimeに変換
    def apply_tz(x):
        return int(x.tz_localize(TZ_JST).timestamp()) if not pd.isnull(x) else x
    df['o1_call_price_time'] = df['o1_call_price_time'].apply(apply_tz, )
    df['o2_call_price_time'] = df['o2_call_price_time'].apply(apply_tz)
    df['o1_put_price_time'] = df['o1_put_price_time'].apply(apply_tz)
    df['o2_put_price_time'] = df['o2_put_price_time'].apply(apply_tz)

    # CSV化
    line1 = f'{int(future.created_at.timestamp())},{o1_atm}\n'
    option_list_csv = line1 + df.to_csv(index=False, header=False)

    res = make_response(option_list_csv, 200)
    res.headers['Content-type'] = 'text/csv; charset=utf-8'
    return res


# entry point of Cloud Functions
# trigger = http
# ATM IV推移用のデータをCSVで返す
def atm_data(request):

    if not check_auth(request):
        return 'Forbidden', 403

    today = datetime.now(TZ_JST)

    df = od.find_recent_iv_and_price_of_atm_options(today)
    log.debug(f'number of matched record: {len(df)}')

    latest_created_at_str = str(df['time'].iloc[-1])

    # CSV化
    line1 = f'{latest_created_at_str}\n'
    option_list_csv = line1 + df.to_csv(index=False, header=False)

    res = make_response(option_list_csv, 200)
    res.headers['Content-type'] = 'text/csv; charset=utf-8'
    return res


# entry point of Cloud Functions
# trigger = bucket
# filename format: <tablename>_yyyymmddhhmmssSSS.json
def load_jpx_into_bq(data, context):
    event_type = context.event_type

    if event_type != 'google.storage.object.finalize':
        log.info(f'not target event_type. skipping...: event_type={event_type}')
        return

    bucket = data['bucket']
    name = data['name']
    time_created = data['timeCreated']

    log.debug('file received: bucket={}, name={}, timeCreated={}'.format(bucket, name, time_created))

    m = REGEX_PRICE_FILE.match(name)
    if m is None:
        log.debug('not price info file: {}'.format(name))
        # 価格情報ファイルじゃない
        return

    table_name = m.groups()[0]
    log.debug('table_name: {}'.format(table_name))

    json_on_gcs_into_bq(bucket, name, table_name)


# entry point of Cloud Functions
# trigger = pubsub
def download_jpx(data, context):

    if 'data' in data:
        topic = base64.b64decode(data['data']).decode('utf-8')
    else:
        topic = 'empty_topic'

    log.debug('topice received: topic={}!'.format(topic))

    # 1限月をDL
    jpx1 = jpx_loader.load_jpx_nearby_month()

    # 2限月をDL
    jpx2 = jpx_loader.load_jpx_nearby_month_2nd()

    created_at = jpx1.created_at

    is_changed = jpx1.future_price.price_time is not None and update_prev_future_price_if_changed(jpx1.future_price)

    # 先物価格が値が動いてない場合は処理スキップ
    if not is_changed:
        if jpx1.future_price.price_time is not None:
            log_price_time = jpx1.future_price.price_time.isoformat()
        else:
            log_price_time = 'None'

        log.debug('future_price is not changed. '
                  f'skipping..: price_time={log_price_time}')
        return

    # created_at は1限月にDLしたものに統一する
    # 1限月、２限月合わせてとある時刻のスナップショットとして扱うため
    for op in jpx2.call_option_list:
        op.created_at = created_at

    for op in jpx2.put_option_list:
        op.created_at = created_at

    # JSON化
    spot_price_json = jpx1.spot_price.to_json()
    future_price_json = jpx1.future_price.to_json()

    option_price_json = '\n'.join(map(lambda x: x.to_json(), jpx1.call_option_list))
    option_price_json += '\n'
    option_price_json += '\n'.join(map(lambda x: x.to_json(), jpx1.put_option_list))
    option_price_json += '\n'
    option_price_json += '\n'.join(map(lambda x: x.to_json(), jpx2.call_option_list))
    option_price_json += '\n'
    option_price_json += '\n'.join(map(lambda x: x.to_json(), jpx2.put_option_list))

    # Cloud Storageへアップロード
    suffix = created_at.strftime('%Y%m%d%H%M%S')

    upload_to_gcs_from_string(spot_price_json, 'spot_price_{}.json'.format(suffix))
    upload_to_gcs_from_string(future_price_json, 'future_price_{}.json'.format(suffix))
    upload_to_gcs_from_string(option_price_json, 'option_price_{}.json'.format(suffix))


# Cloud Storage へアップロード
# コンテンツはgzip圧縮して、.gz を末尾に付加したファイル名でアップロードします。
def upload_to_gcs_from_string(data, filename):
    client = storage.Client()
    bucket = client.get_bucket(config.gcp_cs_bucket_name)

    gzipped_data = gzip.compress(data.encode('utf-8'))
    file_blob = bucket.blob('{}.gz'.format(filename))
    file_blob.content_encoding = 'gzip'
    file_blob.upload_from_string(gzipped_data, content_type='application/json')


# 先物の最新取引時刻が前回のものより新しければ保存します。
# 新しかった場合は True を返します。
# future_price は aware を渡して。
def update_prev_future_price_if_changed(future_price):
    client = datastore.Client()

    with client.transaction():
        key = client.key(config.gcp_ds_kind, config.gcp_ds_key_id)
        prev_future_price = client.get(key)

        if prev_future_price is not None:
            # 時分が同じならば同じ時刻とみなす。
            # 土日は24時間以上価格が動かないが、JPXのサイトには時分表示しか無く
            # 何日の時刻なのか正確に判定できないため。
            prev_future_price_hhmm = prev_future_price['price_time'].astimezone(TZ_JST).strftime('%H:%M')
            future_price_hhmm = future_price.price_time.astimezone(TZ_JST).strftime('%H:%M')

            if prev_future_price_hhmm == future_price_hhmm:
                # 値動きなし。
                return False

        if prev_future_price is None:
            prev_future_price = datastore.Entity(key)

        prev_future_price.update(asdict(future_price))
        client.put(prev_future_price)

    return True


def json_on_gcs_into_bq(bucket_name, file_name, table_name):

    uri = 'gs://%s/%s' % (bucket_name, file_name)

    client = bigquery.Client()

    table_fqn = '{}.{}.{}'.format(config.gcp_project_id, config.gcp_bq_dataset_name, table_name)

    dataset_ref = client.dataset(config.gcp_bq_dataset_name)
    client.get_dataset(dataset_ref)

    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = False
    job_config.create_disposition = 'CREATE_NEVER'
    job_config.source_format = 'NEWLINE_DELIMITED_JSON'

    try:
        load_job = client.load_table_from_uri(uri, table_fqn, job_config=job_config)
        log.debug('Load job: {} [{}]'.format(load_job.job_id,table_fqn))
    except Exception as e:
        log.error('Failed to create load job: {}'.format(e))
        raise e


def check_auth(request):
    auth_header = request.headers.get("Authorization")

    req_cf_token = None

    if auth_header is not None:
        req_cf_token = auth_header.split(' ')[1]

    return req_cf_token == cf_token


def main():
    log.debug('main() is called.')
    pass


if __name__ == '__main__':
    main()

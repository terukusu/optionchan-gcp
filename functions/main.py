import base64
import gzip
import re

from datetime import datetime, timedelta
from pytz import timezone

# 3rd party modules
from google.cloud import bigquery
from google.cloud import storage

# my modules
import jpx_loader
from my_logging import getLogger

log = getLogger(__name__)

# GCP info
GCP_PROJECT_ID = 'optionchan-222710'
GCS_BUCKET_NAME = 'jikken'
BQ_DATASET_NAME = 'jikken'

TZ_JST = timezone('Asia/Tokyo')
TZ_UTC = timezone('UTC')

# 価格情報を格納するGCS上のファイル名のパターン
REGEX_PRICE_FILE = re.compile(r'((?:spot|future|option)_price)_\d+.json.gz')


#
# 既にBigQueryに指定時刻(created_at)の先物価格が格納されているかを取得します。
# price_time はタイムゾーンが JST な aware なdatetime
#
def is_exists_future_price(price_time):
    client = bigquery.Client()

    table_fqn = '{}.future_price'.format(BQ_DATASET_NAME)
    price_time_str = price_time.strftime('%Y-%m-%d %H:%M:%S')

    # price_time の前後１時間のcreated_atの範囲で検索。
    # (つまり価格情報の記録頻度は１時間以上空けてはダメー)
    from_date = (price_time - timedelta(hours=1)).isoformat()
    to_date = (price_time + timedelta(hours=1)).isoformat()

    query = (
        'SELECT count(*) > 0 as is_exists FROM `{}` '
        'WHERE created_at >= "{}" AND created_at < "{}" AND price_time="{}"')\
        .format(table_fqn, from_date, to_date, price_time_str)

    query_job = client.query(query)
    rows = query_job.result()
    is_exists = next(iter(rows)).is_exists

    return is_exists


def json_on_gcs_into_bq(bucket_name, file_name, table_name):

    uri = 'gs://%s/%s' % (bucket_name, file_name)

    client = bigquery.Client()

    table_fqn = '{}.{}.{}'.format(GCP_PROJECT_ID, GCS_BUCKET_NAME, table_name)

    dataset_ref = client.dataset(BQ_DATASET_NAME)
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


# entry point of Cloud Functions
# trigger = bucket
# filename format: <tablename>_yyyymmddhhmmssSSS.json
def load_jpx_into_bq(data, context):
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
    future_price_time = jpx1.future_price.price_time

    # ↓ 小さなクエリでも最小単位の10MB課金されるのでダメー

    # BigQueryに先物価格が既に存在する(＝値が動いてない＝取引時間外)場合は処理スキップ
    # is_exists = is_exists_future_price(future_price_time)
    #
    # if is_exists:
    #     log.debug('future_price already exists. '
    #               'skipping..: price_time={}'.format(future_price_time.isoformat()))
    #     return

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
    bucket = client.get_bucket(GCS_BUCKET_NAME)

    gzipped_data = gzip.compress(data.encode('utf-8'))
    file_blob = bucket.blob('{}.gz'.format(filename))
    file_blob.content_encoding = 'gzip'
    file_blob.upload_from_string(gzipped_data, content_type='application/json')


def main():
    log.debug('main() is called.')
    pass


if __name__ == '__main__':
    main()

import base64
import gzip
import re

from dataclasses import asdict
from pytz import timezone

# 3rd party modules
from google.cloud import bigquery
from google.cloud import datastore
from google.cloud import storage

# my modules
import jpx_loader, config
from my_logging import getLogger

log = getLogger(__name__)
config = config.Config()

TZ_JST = timezone('Asia/Tokyo')
TZ_UTC = timezone('UTC')

# 価格情報を格納するGCS上のファイル名のパターン
REGEX_PRICE_FILE = re.compile(r'((?:spot|future|option)_price)_\d+.json.gz')


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

    is_changed = update_prev_future_price_if_changed(jpx1.future_price)

    # 先物価格が値が動いてない場合は処理スキップ
    if not is_changed:
        log.debug('future_price is not changed. '
                  'skipping..: price_time={}'.format(jpx1.future_price.price_time.isoformat()))
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
def update_prev_future_price_if_changed(future_price):
    client = datastore.Client()

    with client.transaction():
        key = client.key(config.gcp_ds_kind, config.gcp_ds_key_id)
        prev_future_price = client.get(key)

        if prev_future_price is not None and future_price.price_time == prev_future_price['price_time']:
            # 値動きなし
            return False

        if prev_future_price is None:
            prev_future_price = datastore.Entity(key)

        prev_future_price.update(asdict(future_price))
        client.put(prev_future_price)

    return True


def main():
    log.debug('main() is called.')
    pass


if __name__ == '__main__':
    main()

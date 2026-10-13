"""アラートポリシーの条件を過去の指標に当てて、発火回数を数える。

- 旧系 (m1-liveness.json / m2-errors.json): Cloud Functions の execution_count
- 新系の M1 (ingest-m1-liveness.json、#35): Cloud Run ingest の request_count の 2xx。データは新系のデプロイ (2026-10-13) 以降
- 新系の M2 (ingest-m2-errors.json) はログ一致なので、ここでは当てない (runbook のコマンドで件数を見る)

読み取りのみ (Monitoring API の timeSeries.list)。標準ライブラリだけで動く。リポジトリ直下で実行する:

    CLOUDSDK_ACTIVE_CONFIG_NAME=<config> GCP_PROJECT_ID=<PROJECT_ID> python3 -I monitoring/backtest.py [--days 42]

プロジェクトは --project か環境変数 GCP_PROJECT_ID で渡す（既定値は持たない）。

出力は判定ごとの要約 1 行と期間だけ (生の時系列・トークンは出さない)。
終了コード: 発火なし 0、発火あり 1、API エラー 2。
"""
import argparse
import json
import os
import re
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
API = 'https://monitoring.googleapis.com/v3/projects/{project}/timeSeries'
EXECUTION_COUNT = 'cloudfunctions.googleapis.com/function/execution_count'
REQUEST_COUNT = 'run.googleapis.com/request_count'
MAX_DAYS = 42  # Monitoring の保持期間 (6 週間)


# ---- 判定 (純粋関数) ----

def max_gap_minutes(minutes):
    """件数のある分 (datetime) の、隣どうしの間隔の最大 (分)。2 点未満は 0。"""
    ordered = sorted(minutes)
    gaps = [(b - a) // timedelta(minutes=1) for a, b in zip(ordered, ordered[1:])]
    return max(gaps, default=0)


def _window_sums(counts, window_minutes):
    """各点 t を始点とする窓 [t, t + window) の合計を (t, 合計) で返す。"""
    keys = sorted(counts)
    window = timedelta(minutes=window_minutes)
    total = 0
    end = 0
    for start, t in enumerate(keys):
        while end < len(keys) and keys[end] < t + window:
            total += counts[keys[end]]
            end += 1
        yield t, total
        total -= counts[t]


def max_window_sum(counts, window_minutes):
    """長さ window_minutes の移動窓 (t <= u < t + window) の合計の最大。"""
    return max((s for _, s in _window_sums(counts, window_minutes)), default=0)


def first_window_over(counts, window_minutes, threshold):
    """合計が threshold を超える最初の窓の開始時刻。無ければ None。"""
    for t, s in _window_sums(counts, window_minutes):
        if s > threshold:
            return t
    return None


def _seconds_to_minutes(value):
    return int(value.rstrip('s')) // 60


def _function_names(filter_):
    m = re.search(r'function_name = (one_of\(([^)]*)\)|"[^"]*")', filter_)
    return re.findall(r'"([^"]+)"', m.group(1))


def thresholds(m1, m2):
    """ポリシー JSON から判定に使う値を取り出す。"""
    absent = m1['conditions'][0]['conditionAbsent']
    [function] = _function_names(absent['filter'])
    result = {'m1': {'function': function, 'minutes': _seconds_to_minutes(absent['duration'])}, 'm2': []}
    for c in m2['conditions']:
        cond = c['conditionThreshold']
        result['m2'].append({
            'functions': _function_names(cond['filter']),
            'minutes': _seconds_to_minutes(cond['aggregations'][0]['alignmentPeriod']),
            'threshold': cond['thresholdValue'],
        })
    return result


def ingest_threshold(policy):
    """新系 M1 (request_count のしきい値) から、サービス名・応答クラス・窓 (分) を取り出す。"""
    cond = policy['conditions'][0]['conditionThreshold']
    service = re.search(r'resource\.labels\.service_name = "([^"]+)"', cond['filter']).group(1)
    code_class = re.search(r'metric\.labels\.response_code_class = "([^"]+)"', cond['filter']).group(1)
    return {
        'service': service,
        'response_code_class': code_class,
        'minutes': _seconds_to_minutes(cond['aggregations'][0]['alignmentPeriod']),
    }


# ---- API ----

class ApiError(Exception):
    pass


def access_token():
    try:
        r = subprocess.run(['gcloud', 'auth', 'print-access-token'], capture_output=True, text=True, check=True)
    except (OSError, subprocess.CalledProcessError) as e:
        raise ApiError('gcloud auth print-access-token failed') from e
    return r.stdout.strip()


def _rfc3339(dt):
    return dt.strftime('%Y-%m-%dT%H:%M:%SZ')


def _list_points(project, params):
    """timeSeries.list を全ページ読み、(系列, 点) を返す。"""
    token = access_token()
    params = dict(params, pageSize='100000')
    while True:
        url = API.format(project=project) + '?' + urllib.parse.urlencode(params)
        req = urllib.request.Request(url, headers={'Authorization': f'Bearer {token}'})
        try:
            with urllib.request.urlopen(req, timeout=120) as res:
                body = json.load(res)
        except urllib.error.HTTPError as e:
            try:
                message = json.load(e).get('error', {}).get('message', '')
            except ValueError:
                message = ''
            raise ApiError(f'HTTP {e.code}: {message[:200]}') from None
        except urllib.error.URLError as e:
            raise ApiError(f'request failed: {e.reason}') from None
        for series in body.get('timeSeries', []):
            for point in series.get('points', []):
                yield series, point
        token_next = body.get('nextPageToken')
        if not token_next:
            return
        params['pageToken'] = token_next


def _point_minute(point):
    # 点は [endTime - 60s, endTime) の件数。開始の分で数える
    return datetime.strptime(point['interval']['endTime'][:16], '%Y-%m-%dT%H:%M') - timedelta(minutes=1)


def _interval(start, end):
    return {
        'interval.startTime': _rfc3339(start),
        'interval.endTime': _rfc3339(end),
        'aggregation.alignmentPeriod': '60s',
        'aggregation.perSeriesAligner': 'ALIGN_DELTA',
    }


def fetch_counts(project, functions, start, end):
    """(function_name, status) -> {分 (UTC, naive datetime): 件数}。0 の点は捨てる。"""
    names = ', '.join(f'"{f}"' for f in functions)
    params = {
        'filter': f'resource.type = "cloud_function" AND metric.type = "{EXECUTION_COUNT}"'
                  f' AND resource.labels.function_name = one_of({names})',
        **_interval(start, end),
    }
    counts = defaultdict(dict)
    for series, point in _list_points(project, params):
        key = (series['resource']['labels']['function_name'], series['metric']['labels'].get('status', ''))
        n = int(point['value'].get('int64Value', 0))
        if n == 0:
            continue
        minute = _point_minute(point)
        counts[key][minute] = counts[key].get(minute, 0) + n
    return counts


def fetch_request_minutes(project, service, code_class, start, end):
    """service の応答クラス code_class の件数がある分 (UTC, naive datetime) の集合。リビジョンは合算。"""
    params = {
        'filter': f'resource.type = "cloud_run_revision" AND metric.type = "{REQUEST_COUNT}"'
                  f' AND resource.labels.service_name = "{service}"'
                  f' AND metric.labels.response_code_class = "{code_class}"',
        **_interval(start, end),
    }
    return {_point_minute(point) for _, point in _list_points(project, params)
            if int(point['value'].get('int64Value', 0)) > 0}


def merge(counts, function, include):
    """function の系列のうち include(status) が真のものを分ごとに合計する。"""
    merged = defaultdict(int)
    for (f, status), points in counts.items():
        if f == function and include(status):
            for minute, n in points.items():
                merged[minute] += n
    return dict(merged)


# ---- main ----

def _fmt(dt):
    return dt.strftime('%Y-%m-%d %H:%M UTC')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--project', default=os.environ.get('GCP_PROJECT_ID') or None,
                        help='GCP プロジェクト ID（既定: 環境変数 GCP_PROJECT_ID）')
    parser.add_argument('--days', type=int, default=MAX_DAYS)
    args = parser.parse_args(argv)
    if not args.project:
        parser.error('--project か環境変数 GCP_PROJECT_ID でプロジェクト ID を渡す')
    days = max(1, min(args.days, MAX_DAYS))

    with open(os.path.join(HERE, 'm1-liveness.json'), encoding='utf-8') as f:
        m1 = json.load(f)
    with open(os.path.join(HERE, 'm2-errors.json'), encoding='utf-8') as f:
        m2 = json.load(f)
    with open(os.path.join(HERE, 'ingest-m1-liveness.json'), encoding='utf-8') as f:
        ingest_m1 = json.load(f)
    t = thresholds(m1, m2)
    ti = ingest_threshold(ingest_m1)
    functions = sorted({t['m1']['function']} | {f for c in t['m2'] for f in c['functions']})

    end = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    start = end - timedelta(days=days)
    try:
        counts = fetch_counts(args.project, functions, start, end)
        ingest_minutes = fetch_request_minutes(args.project, ti['service'], ti['response_code_class'], start, end)
    except ApiError as e:
        print(f'error: {e}', file=sys.stderr)
        return 2

    print(f'period: {_fmt(start)} - {_fmt(end)} ({days} days)')
    fires = 0

    f, minutes = t['m1']['function'], t['m1']['minutes']
    runs = sorted(merge(counts, f, lambda s: True))
    gap = max_gap_minutes(runs)
    long_gaps = [a for a, b in zip(runs, runs[1:]) if (b - a) // timedelta(minutes=1) >= minutes]
    fires += len(long_gaps)
    print(f'M1 {f}: max gap {gap} min (fires if >= {minutes}) -> {len(long_gaps)} fires')
    if long_gaps:
        print(f'  first gap after: {_fmt(long_gaps[0])}')

    for c in t['m2']:
        for f in c['functions']:
            failures = merge(counts, f, lambda s: s != 'ok')
            worst = max_window_sum(failures, c['minutes'])
            n = 1 if worst > c['threshold'] else 0
            fires += n
            print(f'M2 {f}: max failures in {c["minutes"]} min = {worst} '
                  f'(fires if > {c["threshold"]}) -> {">= 1" if n else "0"} fires')
            if n:
                print(f'  first window: {_fmt(first_window_over(failures, c["minutes"], c["threshold"]))}')

    # 新系 M1: 15 分の窓で 2xx が 0 件 = 2xx のある分どうしの間隔が 15 分を超える (間に 15 分の空白)
    s, minutes = ti['service'], ti['minutes']
    runs = sorted(ingest_minutes)
    gap = max_gap_minutes(runs)
    long_gaps = [a for a, b in zip(runs, runs[1:]) if (b - a) // timedelta(minutes=1) > minutes]
    fires += len(long_gaps)
    since = f', since {_fmt(runs[0])}' if runs else ', no data'
    print(f'M1 ingest (service {s}, {ti["response_code_class"]}): max gap {gap} min (fires if > {minutes}{since}) '
          f'-> {len(long_gaps)} fires')
    if long_gaps:
        print(f'  first gap after: {_fmt(long_gaps[0])}')
    return 1 if fires else 0


if __name__ == '__main__':
    sys.exit(main())

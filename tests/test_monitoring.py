import importlib.util
import json
import os
from datetime import datetime, timedelta

import pytest

MONITORING_DIR = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'monitoring'))
RUNBOOK_URL_PREFIX = 'https://github.com/terukusu/optionchan-gcp/blob/master/docs/runbook.md#'
EXECUTION_COUNT = 'cloudfunctions.googleapis.com/function/execution_count'
NOT_OK = 'NOT metric.labels.status = "ok"'


def load_policy(name):
    with open(os.path.join(MONITORING_DIR, name), encoding='utf-8') as f:
        return json.load(f)


def load_backtest():
    # import 時に gcloud・API を呼ばないこと (main() でだけ呼ぶ)
    spec = importlib.util.spec_from_file_location('backtest', os.path.join(MONITORING_DIR, 'backtest.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope='module')
def m1():
    return load_policy('m1-liveness.json')


@pytest.fixture(scope='module')
def m2():
    return load_policy('m2-errors.json')


@pytest.fixture(scope='module')
def backtest():
    return load_backtest()


@pytest.mark.parametrize('name, prefix', [
    ('m1-liveness.json', 'optionchan: M1 '),
    ('m2-errors.json', 'optionchan: M2 '),
])
def test_policy_common(name, prefix):
    policy = load_policy(name)
    assert policy['displayName'].startswith(prefix)
    # 通知チャネルは作成時に --notification-channels で渡す (ID をリポジトリに置かない)
    assert 'notificationChannels' not in policy
    assert policy['enabled'] is True
    assert policy['combiner'] == 'OR'
    links = policy['documentation']['links']
    assert links
    for link in links:
        assert link['url'].startswith(RUNBOOK_URL_PREFIX)


def test_m1_liveness(m1):
    assert len(m1['conditions']) == 1
    cond = m1['conditions'][0]['conditionAbsent']
    assert cond['duration'] == '900s'
    assert 'function_name = "download_jpx"' in cond['filter']
    assert EXECUTION_COUNT in cond['filter']
    # status ごとの系列のまま absence にすると、まれな crash 系列の欠落で発火する
    assert 'status' not in cond['filter']
    [agg] = cond['aggregations']
    assert agg['crossSeriesReducer'] == 'REDUCE_SUM'
    assert agg['groupByFields'] == ['resource.label.function_name']


def test_m2_errors(m2):
    conditions = m2['conditions']
    assert len(conditions) == 2
    for c in conditions:
        cond = c['conditionThreshold']
        assert EXECUTION_COUNT in cond['filter']
        assert NOT_OK in cond['filter']
        [agg] = cond['aggregations']
        assert agg['alignmentPeriod'] == '900s'
        assert agg['perSeriesAligner'] == 'ALIGN_SUM'
        assert agg['crossSeriesReducer'] == 'REDUCE_SUM'
        assert agg['groupByFields'] == ['resource.label.function_name']
        assert cond['comparison'] == 'COMPARISON_GT'

    ingest, ohlc = (c['conditionThreshold'] for c in conditions)
    # 15 分で 3 件以上 (> 2)
    assert '"download_jpx"' in ingest['filter'] and '"load_jpx_into_bq"' in ingest['filter']
    assert ingest['thresholdValue'] == 2
    # 1 件以上 (> 0)
    assert 'function_name = "insert_ohlc"' in ohlc['filter']
    assert ohlc['thresholdValue'] == 0


def utc(hour, minute):
    return datetime(2026, 10, 12, hour, minute)


def test_max_gap_minutes(backtest):
    assert backtest.max_gap_minutes([]) == 0
    assert backtest.max_gap_minutes([utc(0, 0)]) == 0
    assert backtest.max_gap_minutes([utc(0, m) for m in range(10)]) == 1
    # 順不同でも隣どうしの間隔で数える
    minutes = [utc(0, 30), utc(0, 0), utc(0, 1), utc(0, 16), utc(0, 31)]
    assert backtest.max_gap_minutes(minutes) == 15


def test_max_window_sum(backtest):
    assert backtest.max_window_sum({}, 15) == 0
    # 窓は t <= u < t + 15。15 分離れた 2 点は同じ窓に入らない
    assert backtest.max_window_sum({utc(0, 0): 2, utc(0, 15): 2}, 15) == 2
    assert backtest.max_window_sum({utc(0, 0): 2, utc(0, 14): 2}, 15) == 4
    # 離れた 2 群
    counts = {utc(0, 0): 1, utc(0, 5): 1, utc(3, 0): 1, utc(3, 1): 1, utc(3, 2): 1}
    assert backtest.max_window_sum(counts, 15) == 3
    assert backtest.first_window_over(counts, 15, threshold=2) == utc(3, 0)
    assert backtest.first_window_over(counts, 15, threshold=3) is None


def test_thresholds_from_policies(backtest, m1, m2):
    t = backtest.thresholds(m1, m2)
    assert t['m1'] == {'function': 'download_jpx', 'minutes': 15}
    assert t['m2'] == [
        {'functions': ['download_jpx', 'load_jpx_into_bq'], 'minutes': 15, 'threshold': 2},
        {'functions': ['insert_ohlc'], 'minutes': 15, 'threshold': 0},
    ]

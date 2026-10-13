import importlib.util
import json
import os
import re
from datetime import datetime, timedelta

import pytest

MONITORING_DIR = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'monitoring'))
RUNBOOK = os.path.normpath(os.path.join(MONITORING_DIR, '..', 'docs', 'runbook.md'))
EXECUTION_COUNT = 'cloudfunctions.googleapis.com/function/execution_count'
NOT_OK = 'NOT metric.labels.status = "ok"'
REQUEST_COUNT = 'run.googleapis.com/request_count'
INFRA_MONITORING_TF = os.path.normpath(os.path.join(MONITORING_DIR, '..', 'infra', 'prod', 'monitoring.tf'))


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
def ingest_m1():
    return load_policy('ingest-m1-liveness.json')


@pytest.fixture(scope='module')
def ingest_m2():
    return load_policy('ingest-m2-errors.json')


@pytest.fixture(scope='module')
def backtest():
    return load_backtest()


@pytest.mark.parametrize('name, prefix', [
    ('m1-liveness.json', 'optionchan: M1 '),
    ('m2-errors.json', 'optionchan: M2 '),
    ('ingest-m1-liveness.json', 'optionchan: M1 ingest '),
    ('ingest-m2-errors.json', 'optionchan: M2 ingest '),
])
def test_policy_common(name, prefix):
    policy = load_policy(name)
    assert policy['displayName'].startswith(prefix)
    # 通知チャネルは作成時に --notification-channels で渡す (ID をリポジトリに置かない)
    assert 'notificationChannels' not in policy
    assert policy['enabled'] is True
    assert policy['combiner'] == 'OR'
    # runbook へは特定のリポジトリの絶対 URL でなく、リポジトリ相対の節の名前で案内する（誰でも clone して使う前提。#55）
    doc = policy['documentation']
    assert 'links' not in doc
    m = re.search(r'リポジトリの `docs/runbook.md` の「([^」]+)」節の「([^」]+)」', doc['content'])
    assert m, name
    section, subsection = m.groups()
    with open(RUNBOOK, encoding='utf-8') as f:
        headings = [line.rstrip('\n') for line in f if line.startswith('#')]
    # 節（##）と、その中の小節（###）が runbook にあること
    start = headings.index(f'## {section}')
    rest = headings[start + 1:]
    end = next((i for i, h in enumerate(rest) if h.startswith('## ')), len(rest))
    assert f'### {subsection}' in rest[:end], (name, section, subsection)
    # displayName の上限は 100 文字。ログ一致の条件は Logging の通知ルールとしても作られ、その名前は
    # ポリシーと条件の displayName をつないだものになるらしく、合わせて 100 文字を超えると作成が 400
    # （`display_name` must not be more than 100 characters）、更新は「Recompilation of log match condition failed」
    # でポリシーが無効になる（2026-10-14。35 + 80 文字で失敗、35 + 46 文字で成功）。余裕を見て合計 95 文字以内にする
    assert len(policy['displayName']) <= 100
    for c in policy['conditions']:
        assert len(c['displayName']) <= 100, (name, c['displayName'])
        if 'conditionMatchedLog' in c:
            assert len(policy['displayName']) + len(c['displayName']) <= 95, (name, c['displayName'])


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


def test_display_names_unique():
    # runbook の削除・一時停止は表示名の完全一致で 1 本を選ぶ
    names = [load_policy(n)['displayName'] for n in sorted(os.listdir(MONITORING_DIR)) if n.endswith('.json')]
    assert len(names) == 4
    assert len(set(names)) == len(names)


def test_terraform_reads_all_policies():
    # monitoring/*.json はすべて monitoring.tf の alert_policies から読まれる (置いただけで作られない、を防ぐ)
    with open(INFRA_MONITORING_TF, encoding='utf-8') as f:
        tf = f.read()
    for name in sorted(os.listdir(MONITORING_DIR)):
        if name.endswith('.json'):
            assert f'/monitoring/{name}"' in tf, name
    assert 'paused_with_ingest = ["ingest_m1"]' in tf


def test_ingest_m1_liveness(ingest_m1):
    [c] = ingest_m1['conditions']
    cond = c['conditionThreshold']
    f = cond['filter']
    assert REQUEST_COUNT in f
    assert 'resource.type = "cloud_run_revision"' in f
    assert 'resource.labels.service_name = "ingest"' in f
    # 時間外の 204 も 2xx。403 (invoker の権限切れ)・5xx だけが続くときも発火させる
    assert 'metric.labels.response_code_class = "2xx"' in f
    # 欠落条件にしない: request_count は最後のリクエストの後も 0 の点を出し続け、欠落とみなされない
    assert 'conditionAbsent' not in c
    # データが無い (Scheduler・サービスの停止) ときも発火
    assert cond['evaluationMissingData'] == 'EVALUATION_MISSING_DATA_ACTIVE'
    assert cond['comparison'] == 'COMPARISON_LT'
    assert cond['thresholdValue'] == 1
    assert cond['duration'] == '60s'  # EVALUATION_MISSING_DATA_ACTIVE は 0 より大きい duration が必要（#35 の初回 apply で 400）
    [agg] = cond['aggregations']
    assert agg['alignmentPeriod'] == '900s'
    assert agg['perSeriesAligner'] == 'ALIGN_SUM'
    # リビジョンごとの系列を合算する (デプロイで系列が変わっても途切れない)
    assert agg['crossSeriesReducer'] == 'REDUCE_SUM'
    assert agg['groupByFields'] == ['resource.label.service_name']


def test_ingest_m2_errors(ingest_m2):
    [c] = ingest_m2['conditions']
    f = c['conditionMatchedLog']['filter']
    assert 'resource.type="cloud_run_revision" AND resource.labels.service_name="ingest"' in f
    assert 'resource.type="cloud_run_job" AND resource.labels.job_name="reprocess"' in f
    # 取引カレンダーの手動の取り込み（#63）
    assert 'resource.type="cloud_run_job" AND resource.labels.job_name="calendar-sync"' in f
    # 定期クエリ（BigQuery Data Transfer Service、#40）の失敗。プロジェクトの DTS は品質チェックと突き合わせの 4 本だけ
    assert 'OR resource.type="bigquery_dts_config")' in f
    assert 'severity>=ERROR' in f
    # 除外は recovery_failed だけ (毎時単発で次の 0 分に回復。続けば 07:00 の reconcile_failed / raw_unrecovered)
    assert f.count('NOT ') == 1
    assert 'NOT jsonPayload.event="recovery_failed"' in f
    # gunicorn の起動行 (stderr のテキスト、severity DEFAULT) やリクエストログの 5xx も含めるため、event の有無で絞らない
    assert 'jsonPayload.event:*' not in f
    strategy = ingest_m2['alertStrategy']
    # ログ一致の条件は通知の頻度の上限が必須
    assert strategy['notificationRateLimit']['period'] == '3600s'
    assert strategy['autoClose'] == '1800s'
    assert strategy['notificationPrompts'] == ['OPENED']


def test_ingest_threshold(backtest, ingest_m1):
    assert backtest.ingest_threshold(ingest_m1) == {'service': 'ingest', 'response_code_class': '2xx', 'minutes': 15}

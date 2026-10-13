"""sql/latest/（最新版の選択のテーブル関数、#34）の静的検査。

- 各 SQL が infra/prod/data.tf の google_bigquery_routine.latest の for_each のキーと 1 対 1 で、Terraform から読まれている
- 期間の引数 start_at / end_at で snapshot_at（データの表は created_at も）を [start_at, end_at) に絞っている
  （option_price の require_partition_filter と ingest_runs の全件スキャンを避ける）
- スナップショットごとに finished_at・ingest_run_id の降順で 1 件を選ぶ
- プロジェクト名・データセット名を直書きしない（Terraform の templatefile が入れる）
BigQuery での実行の確認は bq query --dry_run（Phase 3 の手順、PR に記録）。
"""
import glob
import os
import re

import pytest

ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
SQL_DIR = os.path.join(ROOT, 'sql', 'latest')
DATA_TF = os.path.join(ROOT, 'infra', 'prod', 'data.tf')
DATA_TABLES = ('spot_price', 'future_price', 'option_price')
PICK_LATEST = re.compile(
    r'QUALIFY\s+ROW_NUMBER\(\)\s+OVER\s*\(\s*PARTITION\s+BY\s+(?:r\.)?snapshot_at\s+'
    r'ORDER\s+BY\s+(?:r\.)?finished_at\s+DESC\s*,\s*(?:r\.)?ingest_run_id\s+DESC\s*\)\s*=\s*1',
    re.I,
)


def read(path):
    with open(path, encoding='utf-8') as f:
        return f.read()


def body(text):
    """コメントを除いた本文"""
    return '\n'.join(line for line in text.splitlines() if not line.lstrip().startswith('--'))


SQL_FILES = {os.path.splitext(os.path.basename(p))[0]: read(p) for p in sorted(glob.glob(os.path.join(SQL_DIR, '*.sql')))}


def routine_keys():
    text = read(DATA_TF)
    m = re.search(r'resource\s+"google_bigquery_routine"\s+"latest"\s*\{(.*?)\n\}', text, re.S)
    assert m, 'google_bigquery_routine.latest が無い'
    block = m.group(1)
    keys = re.search(r'for_each\s*=\s*toset\(\[([^\]]*)\]\)', block)
    assert keys, 'for_each が toset([...]) でない'
    return block, re.findall(r'"([^"]+)"', keys.group(1))


def test_sql_files_match_terraform():
    block, keys = routine_keys()
    assert sorted(keys) == sorted(SQL_FILES) == sorted(['latest_runs', *(f'{t}_latest' for t in DATA_TABLES)])
    assert 'sql/latest/${each.key}.sql' in block
    assert 'TABLE_VALUED_FUNCTION' in block
    # 引数は start_at・end_at の 2 つで TIMESTAMP
    assert re.findall(r'name\s*=\s*"([^"]+)"', block) == ['start_at', 'end_at']
    assert block.count('typeKind = "TIMESTAMP"') == 2


@pytest.mark.parametrize('name', sorted(SQL_FILES))
def test_filters_by_period_and_picks_latest(name):
    sql = body(SQL_FILES[name])
    assert re.search(r'snapshot_at\s*>=\s*start_at\s+AND\s+(?:r\.)?snapshot_at\s*<\s*end_at', sql), name
    assert PICK_LATEST.search(sql), name
    assert '`${project}.${dataset}.ingest_runs`' in sql


@pytest.mark.parametrize('table', DATA_TABLES)
def test_data_tables_filter_created_at_and_join_runs(table):
    sql = body(SQL_FILES[f'{table}_latest'])
    assert f'`${{project}}.${{dataset}}.{table}` AS t' in sql
    assert re.search(r't\.created_at\s*>=\s*start_at\s+AND\s+t\.created_at\s*<\s*end_at', sql), table
    # ingest_runs にある実行の行だけ（残骸を落とす）: 内部結合
    assert re.search(r'\)\s*AS\s+r\s+USING\s*\(\s*ingest_run_id\s*\)', sql), table
    assert not re.search(r'\b(LEFT|RIGHT|FULL|CROSS)\b', sql, re.I), table


@pytest.mark.parametrize('name', sorted(SQL_FILES))
def test_no_hardcoded_names(name):
    sql = SQL_FILES[name]
    assert 'optionchan_tokyo' not in sql
    assert not re.search(r'optionchan-\d', sql)
    # 参照するテーブルはすべて ${project}.${dataset}. で始まる
    for ref in re.findall(r'`([^`]+)`', body(sql)):
        assert ref.startswith('${project}.${dataset}.'), ref

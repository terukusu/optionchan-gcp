"""30 日固定の ATM IV（#100）の静的検査。

- 計算（-- cm:begin〜end）は定期クエリ sql/cm/atm_cm.sql・過去分の作り直し sql/cm/cm_backfill.sql・ビューアーの
  sql/atm_cm.sql で一字一句同じ
- 結果の列の順は schema/cm/atm_cm_iv.json と同じ（INSERT は列の位置で入る）
- 書くのは atm_cm_iv だけで、窓 / 範囲の中を置き換える。生データは窓 / 範囲で絞る
- 定期クエリは毎時（#117）: 7 時台の回は前日 07:00〜当日 07:00 の作り直し、他の回は表の最後の行 − 2 時間（上限 3 日）からの増分で、
  新しい取り込みが無ければ何もしない
- infra/prod/sq.tf のテーブル・テーブル単位の付与・DTS（スケジュールは tests/test_infra.py、窓は tests/test_sql.py）
BigQuery での実行の確認は tests/test_cm_bq.py（OPTIONCHAN_BQ_TEST=1）。
"""
import json
import os
import re

import pytest

ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
SQL_DIR = os.path.join(ROOT, 'sql', 'cm')
SQ_TF = os.path.join(ROOT, 'infra', 'prod', 'sq.tf')
SCHEMA = os.path.join(ROOT, 'schema', 'cm', 'atm_cm_iv.json')
VIEWER_CM = os.path.join(ROOT, 'apps', 'viewer', 'src', 'optionchan_viewer', 'sql', 'atm_cm.sql')
# SQL → 窓 / 範囲の変数
PERIOD = {'atm_cm.sql': ('win_start', 'win_end'), 'cm_backfill.sql': ('start_at', 'end_at')}
# SQL → 読む表（定期クエリは窓の始まりを決めるのに atm_cm_iv の最後の行と ingest_runs の新しい取り込みを読む。#117）
READS = {
    'atm_cm.sql': ['atm_cm_iv', 'ingest_runs', 'option_price_latest', 'trading_calendar_latest'],
    'cm_backfill.sql': ['atm_cm_iv', 'option_price_latest', 'trading_calendar_latest'],
}


def read(path):
    with open(path, encoding='utf-8') as f:
        return f.read()


def body(text):
    """コメントを除いた本文"""
    return '\n'.join(line for line in text.splitlines() if not line.lstrip().startswith('--'))


def between(text, begin, end):
    m = re.search(rf'^{re.escape(begin)}\n(.*?)^{re.escape(end)}$', text, re.S | re.M)
    assert m, (begin, end)
    return m.group(1)


SQL = {name: read(os.path.join(SQL_DIR, name)) for name in PERIOD}


def test_files():
    assert sorted(os.listdir(SQL_DIR)) == sorted(PERIOD)


def test_cm_identical():
    """定期クエリ・過去分の作り直し・ビューアーの補う分は同じ計算（片方だけ直すと表と画面の値が食い違う）"""
    cm = between(SQL['atm_cm.sql'], '-- cm:begin', '-- cm:end')
    assert between(SQL['cm_backfill.sql'], '-- cm:begin', '-- cm:end') == cm
    assert between(read(VIEWER_CM), '-- cm:begin', '-- cm:end') == cm
    # 入力は quote（最新版の気配）と holiday（休業日）の 2 つの CTE だけ。表を直接読まない
    assert '`' not in cm
    assert set(re.findall(r'\b(?:FROM|JOIN)\s+(\w{2,})', cm)) - {n for n in re.findall(r'^(\w+) AS \(', cm, re.M)} == \
        {'quote', 'holiday'}
    # 定数（docs/data.md の「30 日固定の ATM IV の計算」）
    for expr in ('(ask - bid) * 2 <= 0.3 * (ask + bid)', 'rn <= 5', 'HAVING COUNT(*) >= 3', 'rn <= 9',
                 'LN(fwd / lo.k) / LN(hi.k / lo.k)', '(m2_days - 30)', '(30 - m1_days)', 'SQRT(var30 / 30)',
                 'm1_days >= 30', 'm1_days >= 3', 'm1_days < 3', "TIME(9, 0, 0)", 'EXTRACT(DAYOFWEEK FROM d) NOT IN (1, 7)'):
        assert expr in cm, expr


@pytest.mark.parametrize('name', sorted(PERIOD))
def test_result_matches_schema(name):
    text = SQL[name]
    for marker in ('source:begin', 'source:end', 'result:begin', 'result:end', 'cm:begin', 'cm:end'):
        assert len(re.findall(rf'^-- {marker}$', text, re.M)) == 1, marker
    line = lambda marker: re.search(rf'^-- {marker}$', text, re.M).start()  # noqa: E731
    assert line('result:begin') < line('source:begin') < line('source:end') < line('cm:begin') < line('cm:end')
    assert '-- cm:end\nSELECT * FROM cm\n-- result:end' in text
    cols = json.loads(read(SCHEMA))
    for c in cols:
        assert c['description'] and c['mode'] in ('NULLABLE', 'REQUIRED'), c['name']
    final = re.search(r'^cm AS \(\n  SELECT\n(.*?)\n  FROM cm_var\n\)', between(text, '-- cm:begin', '-- cm:end'),
                      re.S | re.M).group(1)
    names = [re.split(r'\s+AS\s+', item.strip().rstrip(','))[-1] for item in re.split(r',\n(?=    \w)', final)]
    assert names == [c['name'] for c in cols]


@pytest.mark.parametrize('name', sorted(PERIOD))
def test_reads_period_and_replaces_only_atm_cm_iv(name):
    start, end = PERIOD[name]
    text = body(SQL[name])
    refs = re.findall(r'`\$\{project\}\.\$\{dataset\}\.(\w+)`(\([^()]*\))?', text)
    assert sorted(set(t for t, _ in refs)) == READS[name]
    assert ('option_price_latest', f'({start}, {end})') in refs
    assert f"WHERE calendar_date >= DATE({start}, 'Asia/Tokyo') AND calendar_date < DATE_ADD(DATE({end}, " in text
    writes = re.findall(r'\b(?:INSERT INTO|MERGE|DELETE FROM|UPDATE|TRUNCATE TABLE|CREATE OR REPLACE TABLE|DROP)\s+`([^`]+)`',
                        text)
    assert writes == ['${project}.${dataset}.atm_cm_iv'] * 2
    # 窓 / 範囲の中だけを 1 つのトランザクションで置き換える。結果が無ければ何もしない
    assert re.search(
        r'IF EXISTS \(SELECT 1 FROM result\) THEN\s+BEGIN TRANSACTION;\s+'
        rf'DELETE FROM `\$\{{project\}}\.\$\{{dataset\}}\.atm_cm_iv` WHERE created_at >= {start} AND created_at < {end};\s+'
        r'INSERT INTO `\$\{project\}\.\$\{dataset\}\.atm_cm_iv` SELECT \* FROM result;\s+COMMIT TRANSACTION;\s+END IF;',
        text)


def test_hourly_window():
    """毎時の定期クエリ（#117）の窓の決め方。値の確認は tests/test_cm_bq.py"""
    text = body(SQL['atm_cm.sql'])
    # 7 時台（JST）の回は前日 07:00〜当日 07:00 の作り直し（窓は tests/test_sql.py の HOURLY_WINDOW_DECLARE と同じ）。
    # 他の回は @run_time まで、始まりは最大 3 日前
    assert "DECLARE rebuild BOOL DEFAULT EXTRACT(HOUR FROM DATETIME(@run_time, 'Asia/Tokyo')) = 7;" in text
    assert ("DECLARE win_end TIMESTAMP DEFAULT IF(rebuild, TIMESTAMP(DATETIME(DATE(@run_time, 'Asia/Tokyo'), TIME '07:00:00'), "
            "'Asia/Tokyo'), @run_time);") in text
    assert 'DECLARE win_start TIMESTAMP DEFAULT TIMESTAMP_SUB(win_end, INTERVAL IF(rebuild, 1, 3) DAY);' in text
    # 増分の回: 窓の中（3 日の範囲）の snapshot で前の回の後に終わった取り込みが無ければ何もしない（option_price を読まない）。
    # あれば始まりを表の最後の行（win_end より前）− 2 時間まで縮める（3 日の上限より前にはしない）
    guard = re.search(
        r'IF NOT rebuild THEN\s+IF NOT EXISTS \(\s+SELECT 1\s+FROM `\$\{project\}\.\$\{dataset\}\.ingest_runs`\s+'
        r'WHERE snapshot_at >= win_start AND snapshot_at < win_end '
        r'AND finished_at >= TIMESTAMP_SUB\(@run_time, INTERVAL 65 MINUTE\)\s+\) THEN\s+RETURN;\s+END IF;\s+'
        r'SET win_start = GREATEST\(IFNULL\(TIMESTAMP_SUB\(\(\s+SELECT MAX\(created_at\)\s+'
        r'FROM `\$\{project\}\.\$\{dataset\}\.atm_cm_iv`\s+WHERE created_at >= win_start AND created_at < win_end\s+'
        r'\), INTERVAL 2 HOUR\), win_start\), win_start\);\s+END IF;', text)
    assert guard
    # 順序: DECLARE → 未来の窓の ERROR（手動実行。何もしない回より先）→ 増分の窓 → 計算 → 置き換え。DECLARE は表を読まない
    future = text.index('IF win_end > CURRENT_TIMESTAMP() THEN')
    assert text.rindex('DECLARE ') < future < guard.start() < text.index('CREATE TEMP TABLE result AS') \
        < text.index('BEGIN TRANSACTION')
    assert '`' not in text[:future]
    # 窓を変えるのはこの SET だけ（置き換えの DELETE は縮めた窓で行う）
    assert re.findall(r'\bSET (\w+)', text) == ['win_start']


def test_backfill_range_is_checked():
    text = SQL['cm_backfill.sql']
    assert re.search(r'IF start_at IS NULL OR end_at IS NULL OR start_at >= end_at THEN\s+SELECT ERROR\(', text)
    assert re.search(r'IF end_at > CURRENT_TIMESTAMP\(\) OR TIMESTAMP_TRUNC\(start_at, HOUR\) != start_at '
                     r'OR TIMESTAMP_TRUNC\(end_at, HOUR\) != end_at THEN\s+SELECT ERROR\(', text)


def tf_block(kind, name):
    m = re.search(rf'^resource\s+"{kind}"\s+"{name}"\s*\{{(.*?)^\}}', read(SQ_TF), re.S | re.M)
    assert m, (kind, name)
    return m.group(1)


def test_terraform():
    table = tf_block('google_bigquery_table', 'atm_cm_iv')
    assert 'dataset_id               = google_bigquery_dataset.tokyo.dataset_id' in table
    assert 'table_id                 = "atm_cm_iv"' in table
    assert 'schema/cm/atm_cm_iv.json' in table
    assert re.search(r'^\s*deletion_protection\s*=\s*true$', table, re.M)
    assert re.search(r'time_partitioning \{\s*type\s*=\s*"MONTH"\s*field\s*=\s*"created_at"\s*\}', table)
    grant = tf_block('google_bigquery_table_iam_member', 'sq_atm_cm_editor')
    assert 'table_id   = google_bigquery_table.atm_cm_iv.table_id' in grant
    assert 'role       = "roles/bigquery.dataEditor"' in grant
    assert 'member     = "serviceAccount:${local.sa_sq}"' in grant
    dts = tf_block('google_bigquery_data_transfer_config', 'atm_cm')
    assert 'display_name         = "optionchan atm-cm"' in dts
    assert 'location             = local.bq.dataset.location' in dts
    assert 'templatefile("${path.module}/../../sql/cm/atm_cm.sql"' in dts
    assert 'google_bigquery_table_iam_member.sq_atm_cm_editor' in dts
    # 本番の DTS が読むのは atm_cm.sql だけ（cm_backfill.sql は手で流す）
    assert re.findall(r'templatefile\("\$\{path\.module\}/\.\./\.\./sql/cm/(\w+\.sql)"', read(SQ_TF)) == ['atm_cm.sql']

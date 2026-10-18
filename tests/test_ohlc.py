"""1 時間足（#49、target.md 3.2）の静的検査。

- 新系の 3 表の列（schema/ohlc/atm_ohlc.json）が旧系の schema/atm_*.json と同じ（名前・型・モード・順）
- infra/prod/sq.tf のテーブル 4（3 表 + mid IV の atm_mid_iv、#98）・テーブル単位の付与・DTS（スケジュール等は tests/test_infra.py、窓・期間は tests/test_sql.py）
- sql/ohlc/ の名前は ${...} だけ。表は期間で絞る。1 時間足の計算（-- bars:begin〜end）は定期クエリと旧系の同値の確認で一字一句同じ
- 定期クエリは ATM の put だけを最新版から読み、3 表を窓の中だけ置き換える。移行は追記だけで、範囲を C 以下に限る
BigQuery での実行の確認は tests/test_ohlc_bq.py（OPTIONCHAN_BQ_TEST=1）。
"""
import json
import os
import re

import pytest

ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
SQL_DIR = os.path.join(ROOT, 'sql', 'ohlc')
SQ_TF = os.path.join(ROOT, 'infra', 'prod', 'sq.tf')
VIEWER_ATM_IV = os.path.join(ROOT, 'apps', 'viewer', 'src', 'optionchan_viewer', 'sql', 'atm_iv.sql')
VIEWER_ATM_IV_MONTHS = os.path.join(ROOT, 'apps', 'viewer', 'src', 'optionchan_viewer', 'sql', 'atm_iv_months.sql')
TABLES = ('atm_iv', 'atm_option_price', 'atm_target_price')
# 定期クエリが書く表: 旧系と同じ 3 表 + mid IV（#98。旧系に無い。列は同じ）
HOURLY_TABLES = TABLES + ('atm_mid_iv',)
VIEWER_SQL = os.path.join(ROOT, 'apps', 'viewer', 'src', 'optionchan_viewer', 'sql')
# SQL → 置換する名前（atm_hourly は Terraform、他は runbook の render が入れる）
NAMES = {
    'atm_hourly.sql': {'project', 'dataset'},
    'legacy_check.sql': {'project', 'legacy_dataset'},
    'export.sql': {'project', 'source_dataset'},
    'compare.sql': {'project', 'dataset'},
    'import.sql': {'project', 'dataset', 'import_dataset'},
    'fingerprint.sql': {'project', 'source_dataset'},
    'mid_backfill.sql': {'project', 'dataset'},
}
# 窓 / 期間の条件（表ごとに読む・書く箇所の直後に来る）
PERIOD = {
    'atm_hourly.sql': ('win_start', 'win_end'),
    'legacy_check.sql': ('start_at', 'end_at'),
    'compare.sql': ('start_at', 'end_at'),
    'import.sql': ('start_at', 'end_at'),
    'export.sql': ('@start_at', '@end_at'),
    'fingerprint.sql': ('@start_at', '@end_at'),
    'mid_backfill.sql': ('start_at', 'end_at'),
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


SQL = {name: read(os.path.join(SQL_DIR, name)) for name in NAMES}


def schema(path):
    return [(c['name'], c['type'], c.get('mode', 'NULLABLE')) for c in json.loads(read(path))]


def test_files():
    assert sorted(os.listdir(SQL_DIR)) == sorted(NAMES)


def test_schema_same_as_legacy():
    tokyo = schema(os.path.join(ROOT, 'schema', 'ohlc', 'atm_ohlc.json'))
    for t in TABLES:
        assert schema(os.path.join(ROOT, 'schema', f'{t}.json')) == tokyo, t
    assert [c[0] for c in tokyo] == ['open', 'high', 'low', 'close', 'last_trading_day', 'time_frame', 'started_at']
    for c in json.loads(read(os.path.join(ROOT, 'schema', 'ohlc', 'atm_ohlc.json'))):
        assert c['description'] and c['mode'] in ('NULLABLE', 'REQUIRED'), c['name']


def tf_block(kind, name):
    text = read(SQ_TF)
    m = re.search(rf'^resource\s+"{kind}"\s+"{name}"\s*\{{(.*?)^\}}', text, re.S | re.M)
    assert m, (kind, name)
    return m.group(1)


def test_terraform_tables():
    block = tf_block('google_bigquery_table', 'atm_ohlc')
    assert sorted(re.findall(r'^\s*(\w+)\s*=\s*"ATM', block, re.M)) == sorted(HOURLY_TABLES)
    assert 'dataset_id               = google_bigquery_dataset.tokyo.dataset_id' in block
    assert 'schema/ohlc/atm_ohlc.json' in block
    assert re.search(r'^\s*clustering\s*=\s*\["last_trading_day"\]$', block, re.M)
    assert re.search(r'^\s*deletion_protection\s*=\s*true$', block, re.M)
    assert re.search(r'time_partitioning \{\s*type\s*=\s*"MONTH"\s*field\s*=\s*"started_at"\s*\}', block)


def test_terraform_grants_and_dts():
    grant = tf_block('google_bigquery_table_iam_member', 'sq_atm_ohlc_editor')
    assert 'for_each = google_bigquery_table.atm_ohlc' in grant
    assert 'role       = "roles/bigquery.dataEditor"' in grant
    assert 'member     = "serviceAccount:${local.sa_sq}"' in grant
    dts = tf_block('google_bigquery_data_transfer_config', 'atm_hourly')
    assert 'display_name         = "optionchan atm-hourly"' in dts
    assert 'location             = local.bq.dataset.location' in dts
    assert 'sql/ohlc/atm_hourly.sql' in dts
    assert 'google_bigquery_table_iam_member.sq_atm_ohlc_editor' in dts
    # 本番の DTS が読むのは atm_hourly.sql だけ（他は手で流す）
    tf = read(SQ_TF)
    assert re.findall(r'templatefile\("\$\{path\.module\}/\.\./\.\./sql/ohlc/(\w+\.sql)"', tf) == ['atm_hourly.sql']


@pytest.mark.parametrize('name', sorted(NAMES))
def test_names_are_placeholders(name):
    text = SQL[name]
    assert set(re.findall(r'\$\{(\w+)\}', text)) == NAMES[name]
    assert not re.search(r'optionchan[_-]\w+', body(text))
    for ref in re.findall(r'`([^`]+)`', body(text)):
        assert re.match(r'\$\{project\}\.\$\{\w+\}\.\w+$', ref), (name, ref)


@pytest.mark.parametrize('name', sorted(NAMES))
def test_tables_filtered_by_period(name):
    start, end = PERIOD[name]
    text = body(SQL[name])
    refs = list(re.finditer(r'`\$\{project\}\.\$\{(\w+)\}\.(\w+)`(\([^()]*\))?', text))
    assert refs, name
    for m in refs:
        dataset, table, args = m.groups()
        following = text[m.end():m.end() + 400]
        if table.endswith('_latest'):
            assert args == f'({start}, {end})', (name, table, args)
        elif table == 'ingest_runs':
            # import.sql の C（新系の最初のスナップショット）だけ。1 分 1 行の小さい表
            assert name == 'import.sql' and re.match(r"\s*WHERE trigger != 'legacy_import'", following), (name, following)
        elif table == 'option_price':
            assert re.match(rf'\s*WHERE created_at >= {start} AND created_at < {end}', following), (name, following)
        elif re.match(r'\s*AS t\n', following):
            # MERGE の書き込み先: ON の条件で窓 / 範囲に限る
            assert re.search(rf'AND t\.started_at >= {start} AND t\.started_at < {end}', following), (name, table)
        else:
            assert table in HOURLY_TABLES, (name, table)
            assert re.match(rf'\s*WHERE started_at >= {start} AND started_at < {end}', following), (name, table, following)


def test_bars_identical():
    """定期クエリと旧系の同値の確認は同じ 1 時間足の計算を使う（片方だけ直すと同値の確認が意味を失う）"""
    hourly = between(SQL['atm_hourly.sql'], '-- bars:begin', '-- bars:end')
    check = between(SQL['legacy_check.sql'], '-- bars:begin', '-- bars:end')
    assert hourly.rstrip().rstrip(',') == check.rstrip().rstrip(',')
    # ビューアー（#52）の /atm_iv_data が「まだ 1 時間足になっていない分」を補う計算も同じ
    viewer = between(read(VIEWER_ATM_IV), '-- bars:begin', '-- bars:end')
    assert hourly.rstrip().rstrip(',') == viewer.rstrip().rstrip(',')
    # 期近つなぎ（#101）の全限月の 1 時間足も同じ
    months = between(read(VIEWER_ATM_IV_MONTHS), '-- bars:begin', '-- bars:end')
    assert hourly.rstrip().rstrip(',') == months.rstrip().rstrip(',')
    # 旧系の insert_ohlc と同じ意味: ATM の put、UTC の正時、3600、open は昇順・close は降順の最初の NULL でない値
    assert 'TIMESTAMP_TRUNC(created_at, HOUR) AS started_at' in hourly
    assert '3600 AS time_frame' in hourly
    assert 'GROUP BY last_trading_day, started_at' in hourly
    for v in ('iv', 'price', 'target_price'):
        assert re.search(rf'ARRAY_AGG\({v} IGNORE NULLS ORDER BY created_at, target_price(, {v})?\)\[SAFE_OFFSET\(0\)\]', hourly), v
        assert re.search(rf'ARRAY_AGG\({v} IGNORE NULLS ORDER BY created_at DESC, target_price DESC(, {v} DESC)?\)\[SAFE_OFFSET\(0\)\]', hourly), v
        assert f'MAX({v})' in hourly and f'MIN({v})' in hourly
    for name in ('atm_hourly.sql', 'legacy_check.sql'):
        assert re.search(r'\n\s*AND is_atm AND type = 2\n|\n\s*WHERE is_atm AND type = 2\n', SQL[name]), name
    assert re.search(r'\n\s*WHERE is_atm AND type = 2\n', read(VIEWER_ATM_IV))


def test_mid_bars_identical():
    """mid IV の 1 時間足（#98）: 定期クエリ・過去分の作り直し・ビューアーの補う分（1 限月・期近つなぎ）は同じ計算"""
    hourly = between(SQL['atm_hourly.sql'], '-- mid_bars:begin', '-- mid_bars:end')
    assert between(SQL['mid_backfill.sql'], '-- mid_bars:begin', '-- mid_bars:end') == hourly
    for name in ('atm_mid.sql', 'atm_mid_months.sql'):
        assert between(read(os.path.join(VIEWER_SQL, name)), '-- mid_bars:begin', '-- mid_bars:end') == hourly, name
    # mid = bid_iv・ask_iv が両方 0 より大きいときの平均（ビューアーの分足 sql/atm_mid_minute.sql と同じ規則）
    rule = 'IF(bid_iv > 0 AND ask_iv > 0, (bid_iv + ask_iv) / 2, NULL)'
    assert rule in hourly and rule in read(os.path.join(VIEWER_SQL, 'atm_mid_minute.sql'))
    # 足の切り方・OHLC の式・並べ方は iv と同じ（iv を mid に替えたもの）
    bars = between(SQL['atm_hourly.sql'], '-- bars:begin', '-- bars:end')
    for expr in ('ARRAY_AGG({v} IGNORE NULLS ORDER BY created_at, target_price, {v})[SAFE_OFFSET(0)] AS {v}_open',
                 'MAX({v}) AS {v}_high', 'MIN({v}) AS {v}_low',
                 'ARRAY_AGG({v} IGNORE NULLS ORDER BY created_at DESC, target_price DESC, {v} DESC)[SAFE_OFFSET(0)] AS {v}_close'):
        assert expr.format(v='iv') in bars and expr.format(v='mid') in hourly, expr
    assert 'TIMESTAMP_TRUNC(created_at, HOUR) AS started_at' in hourly and 'GROUP BY last_trading_day, started_at' in hourly
    # 定期クエリは bars と mid_bars を同じキーでつなぐ（同じ ATM の行から作るので 1 対 1）
    assert 'SELECT * FROM bars JOIN mid_bars USING (last_trading_day, started_at)\n-- result:end' in SQL['atm_hourly.sql']
    for name in ('atm_hourly.sql', 'mid_backfill.sql'):
        assert re.search(r'\n\s*WHERE is_atm AND type = 2\n', SQL[name]), name


def test_backfill_limited_to_iv_keys():
    """過去分の作り直し（#122）: mid の足は範囲の中の atm_iv の 1 時間足のキーに限る（atm_iv の過去分は旧系から移した値で、
    UTC 23 時台などの足が無い。最新版から作った mid の足だけが多くならないように）"""
    text = SQL['mid_backfill.sql']
    iv_keys = re.search(r'\niv_keys AS \((.*?)\n\),\n', text, re.S)
    assert iv_keys, 'iv_keys CTE'
    assert iv_keys.group(1) == ('\n  SELECT DISTINCT last_trading_day, started_at'
                                '\n  FROM `${project}.${dataset}.atm_iv`'
                                '\n  WHERE started_at >= start_at AND started_at < end_at AND time_frame = 3600')
    # source の外に置く（tests/test_ohlc_bq.py は source を固定値に差し替え、atm_iv は一時テーブルにする）
    assert text.index('\n-- source:end\n') < iv_keys.start() < text.index('\n-- mid_bars:begin\n')
    assert 'SELECT *, 3600 AS time_frame FROM mid_bars JOIN iv_keys USING (last_trading_day, started_at)\n-- result:end' in text
    # 何もしない条件（IF EXISTS）は JOIN の後の bars に掛かる: 範囲の中に atm_iv のキーが無ければ mid を消さない
    assert text.index('\n-- result:end\n') < text.index('IF EXISTS (SELECT 1 FROM bars) THEN')


def merges(text):
    return re.findall(r'MERGE `\$\{project\}\.\$\{dataset\}\.(\w+)` AS t\n(.*?);', text, re.S)


def test_hourly_merges_replace_window():
    text = SQL['atm_hourly.sql']
    found = merges(text)
    assert [t for t, _ in found] == list(HOURLY_TABLES)
    prefix = {'atm_iv': 'iv', 'atm_option_price': 'price', 'atm_target_price': 'target_price', 'atm_mid_iv': 'mid'}
    # 過去分の作り直し（#98）は atm_mid_iv だけを同じ形で範囲の中を置き換える
    backfill = merges(SQL['mid_backfill.sql'])
    assert [t for t, _ in backfill] == ['atm_mid_iv']
    assert backfill[0][1] == found[-1][1].replace('win_start', 'start_at').replace('win_end', 'end_at')
    assert re.search(r'IF EXISTS \(SELECT 1 FROM bars\) THEN\s+BEGIN TRANSACTION;.*COMMIT TRANSACTION;\s+END IF;',
                     SQL['mid_backfill.sql'], re.S)
    for table, merge in found:
        p = prefix[table]
        assert f'SELECT {p}_open AS open, {p}_high AS high, {p}_low AS low, {p}_close AS close, ' in merge
        assert 'ON t.last_trading_day = s.last_trading_day AND t.time_frame = s.time_frame AND t.started_at = s.started_at' in merge
        assert 'WHEN MATCHED THEN UPDATE SET open = s.open, high = s.high, low = s.low, close = s.close' in merge
        assert ('WHEN NOT MATCHED BY SOURCE AND t.started_at >= win_start AND t.started_at < win_end '
                'AND t.time_frame = 3600 THEN DELETE') in merge
    # ATM の行が無い窓では何もしない（データの無い過去の日の手動実行で消さない）。4 表は 1 つのトランザクション
    assert re.search(r'IF EXISTS \(SELECT 1 FROM bars\) THEN\s+BEGIN TRANSACTION;.*COMMIT TRANSACTION;\s+END IF;', text, re.S)


def test_import_appends_only_before_cutover():
    text = body(SQL['import.sql'])
    found = merges(text)
    assert [t for t, _ in found] == list(TABLES)
    for table, merge in found:
        assert f"USING (SELECT * FROM src WHERE tbl = '{table}' AND versions = 1) AS s" in merge
        assert 'WHEN NOT MATCHED THEN INSERT' in merge
        assert 'UPDATE' not in merge and 'DELETE' not in merge
    assert not re.search(r'\b(DELETE FROM|UPDATE `|TRUNCATE)\b', text)
    assert re.search(r'IF cutover IS NULL OR end_at > cutover THEN\s+SELECT ERROR\(', text)
    assert text.index('IF cutover IS NULL') < text.index('CREATE TEMP TABLE src') < text.index('BEGIN TRANSACTION')


@pytest.mark.parametrize('name', ['legacy_check.sql', 'compare.sql', 'import.sql', 'mid_backfill.sql'])
def test_range_is_checked(name):
    assert re.search(r'IF start_at IS NULL OR end_at IS NULL OR start_at >= end_at THEN\s+SELECT ERROR\(', SQL[name])


@pytest.mark.parametrize('name', ['legacy_check.sql', 'compare.sql', 'import.sql', 'export.sql', 'fingerprint.sql',
                                  'mid_backfill.sql'])
def test_manual_sql_reads_only(name):
    """手で流す SQL のうち書き込むのは import.sql（新系の 3 表への追記）と mid_backfill.sql（atm_mid_iv の作り直し、#98）だけ"""
    text = body(SQL[name])
    writes = re.findall(r'\b(?:INSERT INTO|MERGE|DELETE FROM|UPDATE|TRUNCATE TABLE|CREATE OR REPLACE TABLE|DROP)\s+`([^`]+)`', text)
    if name == 'import.sql':
        assert writes == [f'${{project}}.${{dataset}}.{t}' for t in TABLES]
    elif name == 'mid_backfill.sql':
        assert writes == ['${project}.${dataset}.atm_mid_iv']
    else:
        assert writes == []

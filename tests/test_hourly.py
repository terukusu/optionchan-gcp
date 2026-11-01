"""銘柄ごとの 1 時間足（#161）の静的検査。

- 表の列（schema/tokyo/option_price_hourly.json）は公開のインターフェース（シミュレーターが固定コミットの schema と照合する）。
  (name, type, mode) の並びを固定値と比べる（apps/ingest/tests/test_schema.py の TRADING_CALENDAR と同じ流儀）
- 計算（-- bars:begin〜end）は定期クエリ sql/hourly/option_price_hourly.sql と過去分の作り直し sql/hourly/hourly_backfill.sql で一字一句同じ。
  入力は source の CTE（src = option_price_latest の 13 列）だけで、option_price を直接読まない
- 書くのは option_price_hourly だけで、窓 / 範囲の中を置き換える。表の読み書き（MAX・DELETE）は限月の範囲 [ltd_lo, ltd_hi) で絞る
  （パーティションは last_trading_day の月で require_partition_filter）
- 定期クエリは毎時（atm_cm と同じ形）: 7 時台の回は前日 07:00〜当日 07:00 の作り直し、他の回は表の最後の足 − 2 時間（上限は 3 日前の正時）
  からの増分で、新しい取り込みが無ければ何もしない。過去分の範囲は JST の 07:00 ちょうど
- infra/prod/sq.tf の表・付与・DTS（スケジュールは tests/test_infra.py、窓の宣言は tests/test_sql.py）
BigQuery での実行の確認は tests/test_hourly_bq.py（OPTIONCHAN_BQ_TEST=1）。
"""
import json
import os
import re

import pytest

ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
SQL_DIR = os.path.join(ROOT, 'sql', 'hourly')
SQ_TF = os.path.join(ROOT, 'infra', 'prod', 'sq.tf')
SCHEMA = os.path.join(ROOT, 'schema', 'tokyo', 'option_price_hourly.json')
TABLES_JSON = os.path.join(ROOT, 'schema', 'tokyo', 'tables.json')
NAMES = ('option_price_hourly.sql', 'hourly_backfill.sql')
# SQL → 読む表（定期クエリは窓の始まりを決めるのに表の最後の足と ingest_runs の新しい取り込みを読む）
READS = {
    'option_price_hourly.sql': ['ingest_runs', 'option_price_hourly', 'option_price_latest'],
    'hourly_backfill.sql': ['option_price_hourly', 'option_price_latest'],
}

# 列（名前・型・モード）。公開のインターフェースなので、変えるときは読み手（シミュレーター）に知らせてから（docs/data.md の「列の契約」）
HOURLY_SCHEMA = [
    ('started_at', 'TIMESTAMP', 'REQUIRED'),
    ('last_trading_day', 'DATE', 'REQUIRED'),
    ('target_price', 'INTEGER', 'REQUIRED'),
    ('type', 'INTEGER', 'REQUIRED'),
    ('mid_open', 'FLOAT', 'NULLABLE'),
    ('mid_high', 'FLOAT', 'NULLABLE'),
    ('mid_low', 'FLOAT', 'NULLABLE'),
    ('mid_close', 'FLOAT', 'NULLABLE'),
    ('bid_close', 'INTEGER', 'NULLABLE'),
    ('ask_close', 'INTEGER', 'NULLABLE'),
    ('bid_iv_close', 'FLOAT', 'NULLABLE'),
    ('ask_iv_close', 'FLOAT', 'NULLABLE'),
    ('trade_open', 'INTEGER', 'NULLABLE'),
    ('trade_high', 'INTEGER', 'NULLABLE'),
    ('trade_low', 'INTEGER', 'NULLABLE'),
    ('trade_close', 'INTEGER', 'NULLABLE'),
    ('iv_close', 'FLOAT', 'NULLABLE'),
    ('volume', 'INTEGER', 'NULLABLE'),
    ('positions', 'INTEGER', 'NULLABLE'),
    ('snapshots', 'INTEGER', 'NULLABLE'),
    ('last_at', 'TIMESTAMP', 'NULLABLE'),
]
HOURLY_COLUMNS = [name for name, _, _ in HOURLY_SCHEMA]
# 元の行の列（option_price_latest から読む 13 列）
SOURCE_COLUMNS = ['created_at', 'last_trading_day', 'target_price', 'type', 'bid', 'ask', 'bid_iv', 'ask_iv', 'price', 'price_time',
                  'iv', 'volume', 'positions']


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
TABLE = '`${project}.${dataset}.option_price_hourly`'
LTD = 'last_trading_day >= ltd_lo AND last_trading_day < ltd_hi'


def test_files():
    assert sorted(os.listdir(SQL_DIR)) == sorted(NAMES)


def test_schema_is_fixed():
    """列の並び・型・モードは固定（キーの 4 列だけ REQUIRED）。どの列にも説明がある"""
    cols = json.loads(read(SCHEMA))
    assert [(c['name'], c['type'], c['mode']) for c in cols] == HOURLY_SCHEMA
    for c in cols:
        assert c['description'], c['name']
        assert len(c['description']) <= 1024, c['name']
    # ROWS は GoogleSQL の予約語（列名にすると読み手がバッククォートで囲む必要がある）。足に入ったスナップショットの数は snapshots
    assert 'rows' not in HOURLY_COLUMNS


def test_schema_describes_bars_and_windows():
    """2 つの足の切り方の違い・連続売買の窓の 3 時代・中値の条件・同じ立会の約定だけを数えることを列の説明に書く（読み手が schema だけで分かる）"""
    desc = {c['name']: c['description'] for c in json.loads(read(SCHEMA))}
    assert 'created_at の足' in desc['started_at'] and 'price_time の足' in desc['started_at']
    for name in ('mid_open', 'volume', 'positions', 'snapshots', 'last_at'):
        assert 'created_at（UTC の正時に切り捨て）' in desc[name], name
    assert 'price_time（JST の約定の分）を JST の正時に切り捨て' in desc['trade_open']
    assert 'created_at の足と違う' in desc['trade_open']
    assert '同じ立会' in desc['trade_open']
    for text in ('sd < 2021-09-21 は日中 9:00〜15:10・夜間 16:30〜翌 5:25',
                 '2021-09-21 ≤ sd < 2024-11-05 は日中 8:45〜15:10・夜間 16:30〜翌 5:55',
                 'sd ≥ 2024-11-05 は日中 8:45〜15:40・夜間 17:00〜翌 5:55',
                 'bid > 0・ask > 0・bid ≤ ask'):
        assert text in desc['mid_open'], text
    assert 'パーティション' in desc['last_trading_day']


def test_bars_identical():
    """定期クエリと過去分の作り直しは同じ計算（片方だけ直すと、過去分と毎時の足が食い違う）"""
    bars = between(SQL['option_price_hourly.sql'], '-- bars:begin', '-- bars:end')
    assert between(SQL['hourly_backfill.sql'], '-- bars:begin', '-- bars:end') == bars
    # 入力は src（最新版の行）だけ。表を直接読まない。窓の変数は win_start だけを使う（約定の足の下限）
    assert '`' not in bars
    defined = set(re.findall(r'^(\w+) AS \(', bars, re.M))
    assert set(re.findall(r'\b(?:FROM|JOIN)\s+(\w{2,})', bars)) - defined == {'src'}
    assert 'win_start' in bars and 'win_end' not in bars and '@' not in bars


def test_bars_rules():
    """中値の条件・連続売買の窓（3 時代、[始まり, 終わり)。07:00 JST 起点の時刻）・同じ立会の約定・2 つの足の切り方"""
    bars = body(between(SQL['option_price_hourly.sql'], '-- bars:begin', '-- bars:end'))
    assert "DATETIME_SUB(DATETIME(created_at, 'Asia/Tokyo'), INTERVAL 7 HOUR) AS s" in bars
    assert 'DATETIME_SUB(price_time, INTERVAL 7 HOUR) AS ps' in bars
    assert 'bid > 0 AND ask > 0 AND bid <= ask AND CASE' in bars
    windows = re.findall(r"WHEN sd < DATE '([\d-]+)' THEN\s+(.*?)\n|ELSE\s+(.*?)\n", bars)
    times = [re.findall(r"st (>=|<) TIME '(\d\d:\d\d):00'", w[1] or w[2]) for w in windows]
    assert [w[0] for w in windows] == ['2021-09-21', '2024-11-05', '']
    # JST − 7 時間: 01:45 = 8:45、02:00 = 9:00、08:10 = 15:10、08:40 = 15:40、09:30 = 16:30、10:00 = 17:00、22:25 = 5:25、22:55 = 5:55
    assert times == [
        [('>=', '02:00'), ('<', '08:10'), ('>=', '09:30'), ('<', '22:25')],
        [('>=', '01:45'), ('<', '08:10'), ('>=', '09:30'), ('<', '22:55')],
        [('>=', '01:45'), ('<', '08:40'), ('>=', '10:00'), ('<', '22:55')],
    ]
    # 気配・数の列は created_at の UTC の正時、約定の列は price_time の JST の正時。同じ立会（JST の 07:00・16:00 で切る）の行だけ
    assert 'TIMESTAMP_TRUNC(created_at, HOUR) AS started_at' in bars
    assert "TIMESTAMP_TRUNC(TIMESTAMP(price_time, 'Asia/Tokyo'), HOUR) AS started_at" in bars
    assert "AND DATE(ps) = sd AND (TIME(ps) < TIME '09:00:00') = (st < TIME '09:00:00')" in bars
    assert 'WHERE started_at >= win_start' in bars
    assert 'COUNT(DISTINCT created_at) AS snapshots' in bars
    assert 'IFNULL(q.snapshots, 0) AS snapshots' in bars
    assert 'FULL OUTER JOIN hourly_trade_bars AS t USING (started_at, last_trading_day, target_price, type)' in bars


@pytest.mark.parametrize('name', NAMES)
def test_result_matches_schema(name):
    text = SQL[name]
    for marker in ('source:begin', 'source:end', 'result:begin', 'result:end', 'bars:begin', 'bars:end'):
        assert len(re.findall(rf'^-- {marker}$', text, re.M)) == 1, marker
    line = lambda marker: re.search(rf'^-- {marker}$', text, re.M).start()  # noqa: E731
    assert line('result:begin') < line('source:begin') < line('source:end') < line('bars:begin') < line('bars:end')
    assert '-- bars:end\nSELECT * FROM hourly_bars\n-- result:end' in text
    # 結果の列の並びは schema と同じ
    final = re.search(r'^hourly_bars AS \(\n  SELECT\n(.*?)\n  FROM hourly_quote_bars', between(text, '-- bars:begin', '-- bars:end'),
                      re.S | re.M).group(1)
    names = [re.split(r'\s+AS\s+', item.strip().rstrip(','))[-1].split('.')[-1] for item in final.split(',\n')]
    assert names == HOURLY_COLUMNS
    # INSERT は列の名前を並べる
    [insert] = re.findall(r'INSERT INTO `\$\{project\}\.\$\{dataset\}\.option_price_hourly`\n\s+\(([^()]*)\)\n\s+SELECT \* FROM result;',
                          text)
    assert [c.strip() for c in insert.split(',')] == HOURLY_COLUMNS
    # 元の行は最新版の 13 列（type が NULL の行は使わない。表の type は REQUIRED）
    source = between(text, '-- source:begin', '-- source:end')
    select = re.search(r'^  SELECT (.*)$', source, re.M).group(1)
    assert [c.strip() for c in select.split(',')] == SOURCE_COLUMNS
    assert source.rstrip().endswith('WHERE type IS NOT NULL\n),')


@pytest.mark.parametrize('name', NAMES)
def test_reads_latest_only(name):
    """option_price は option_price_latest を通してだけ読む（直接参照 0）。最新版は窓 / 範囲で絞る"""
    text = body(SQL[name])
    refs = re.findall(r'`\$\{project\}\.\$\{dataset\}\.(\w+)`(\([^()]*\))?', text)
    assert sorted(set(t for t, _ in refs)) == READS[name]
    assert ('option_price_latest', '(win_start, win_end)') in refs
    assert not re.search(r'\boption_price\b(?!_)', text)


@pytest.mark.parametrize('name', NAMES)
def test_replaces_only_option_price_hourly(name):
    text = body(SQL[name])
    writes = re.findall(r'\b(?:INSERT INTO|MERGE|DELETE FROM|UPDATE|TRUNCATE TABLE|CREATE OR REPLACE TABLE|DROP)\s+`([^`]+)`', text)
    assert writes == ['${project}.${dataset}.option_price_hourly'] * 2
    # 窓 / 範囲の中だけを 1 つのトランザクションで置き換える。結果が無ければ何もしない。限月の範囲で絞る（パーティションの絞り込み）
    assert re.search(
        r'IF EXISTS \(SELECT 1 FROM result\) THEN\s+BEGIN TRANSACTION;\s+'
        rf'DELETE FROM {re.escape(TABLE)}\s+WHERE {LTD} AND started_at >= win_start AND started_at < win_end;\s+'
        rf'INSERT INTO {re.escape(TABLE)}\s+\([^()]*\)\s+SELECT \* FROM result;\s+COMMIT TRANSACTION;\s+END IF;',
        text)
    # 限月の範囲の外の行があれば書く前に ERROR（DELETE の範囲から漏れて重複しないように）
    prefix = name.removesuffix('.sql')
    guard = re.search(
        r"IF EXISTS \(\s+SELECT 1 FROM result\s+WHERE last_trading_day < DATE\(started_at, 'Asia/Tokyo'\) "
        r"OR last_trading_day >= DATE_ADD\(DATE\(started_at, 'Asia/Tokyo'\), INTERVAL 1 YEAR\)\s+\) THEN\s+"
        rf"SELECT ERROR\(FORMAT\('{prefix}: last_trading_day out of range", text)
    assert guard and guard.start() < text.index('BEGIN TRANSACTION')
    assert text.index('CREATE TEMP TABLE result AS') < guard.start()
    # 限月の範囲は最初の窓から: [窓の始まりの JST の日付, 窓の終わりの 1 年後)
    assert "DECLARE ltd_lo DATE DEFAULT DATE(win_start, 'Asia/Tokyo');" in text
    assert "DECLARE ltd_hi DATE DEFAULT DATE_ADD(DATE(win_end, 'Asia/Tokyo'), INTERVAL 1 YEAR);" in text
    # 表を読む・書く文はどれも限月で絞る
    for m in re.finditer(rf'(?:FROM|INTO) {re.escape(TABLE)}', text):
        following = text[m.end():m.end() + 80]
        assert following.lstrip().startswith(('WHERE ' + LTD, '(started_at')), following


def test_hourly_window():
    """毎時の定期クエリの窓の決め方（atm_cm.sql と同じ形。増分の始まりは正時に切り捨てる）。値の確認は tests/test_hourly_bq.py"""
    text = body(SQL['option_price_hourly.sql'])
    assert "DECLARE rebuild BOOL DEFAULT EXTRACT(HOUR FROM DATETIME(@run_time, 'Asia/Tokyo')) = 7;" in text
    assert ("DECLARE win_end TIMESTAMP DEFAULT IF(rebuild, TIMESTAMP(DATETIME(DATE(@run_time, 'Asia/Tokyo'), TIME '07:00:00'), "
            "'Asia/Tokyo'), @run_time);") in text
    assert ('DECLARE win_start TIMESTAMP DEFAULT IF(rebuild, TIMESTAMP_SUB(win_end, INTERVAL 1 DAY), '
            'TIMESTAMP_TRUNC(TIMESTAMP_SUB(win_end, INTERVAL 3 DAY), HOUR));') in text
    guard = re.search(
        r'IF NOT rebuild THEN\s+IF NOT EXISTS \(\s+SELECT 1\s+FROM `\$\{project\}\.\$\{dataset\}\.ingest_runs`\s+'
        r'WHERE snapshot_at >= win_start AND snapshot_at < win_end '
        r'AND finished_at >= TIMESTAMP_SUB\(@run_time, INTERVAL 65 MINUTE\)\s+\) THEN\s+RETURN;\s+END IF;\s+'
        r'SET win_start = GREATEST\(IFNULL\(TIMESTAMP_SUB\(\(\s+SELECT MAX\(started_at\)\s+'
        rf'FROM {re.escape(TABLE)}\s+WHERE {LTD} AND started_at >= win_start AND started_at < win_end\s+'
        r'\), INTERVAL 2 HOUR\), win_start\), win_start\);\s+END IF;', text)
    assert guard
    future = text.index('IF win_end > CURRENT_TIMESTAMP() THEN')
    assert re.search(r"IF win_end > CURRENT_TIMESTAMP\(\) THEN\s+SELECT ERROR\(FORMAT\('option_price_hourly: win_end %t is in the future",
                     text)
    assert text.rindex('DECLARE ') < future < guard.start() < text.index('CREATE TEMP TABLE result AS') \
        < text.index('BEGIN TRANSACTION')
    assert '`' not in text[:future]
    assert re.findall(r'\bSET (\w+)', text) == ['win_start']


def test_backfill_range_is_checked():
    """過去分の範囲: NULL・逆順・未来は ERROR。端はどちらも JST の 07:00（日中・夜間の途中で切ると約定の足が境目で欠ける）"""
    text = SQL['hourly_backfill.sql']
    assert 'DECLARE win_start TIMESTAMP DEFAULT @start_at;\nDECLARE win_end TIMESTAMP DEFAULT @end_at;\n' in text
    assert re.search(r'IF win_start IS NULL OR win_end IS NULL OR win_start >= win_end THEN\s+SELECT ERROR\(', text)
    assert re.search(r"IF win_end > CURRENT_TIMESTAMP\(\) OR TIME\(DATETIME\(win_start, 'Asia/Tokyo'\)\) != TIME '07:00:00'\s+"
                     r"OR TIME\(DATETIME\(win_end, 'Asia/Tokyo'\)\) != TIME '07:00:00' THEN\s+SELECT ERROR\(", text)
    assert '@run_time' not in text
    # 最後に要約を 1 行
    assert text.rstrip().endswith('SELECT COUNT(*) AS bars, COUNTIF(mid_close IS NOT NULL) AS with_mid, '
                                  'COUNTIF(trade_close IS NOT NULL) AS with_trade FROM result;')


def tf_block(kind, name):
    m = re.search(rf'^resource\s+"{kind}"\s+"{name}"\s*\{{(.*?)^\}}', read(SQ_TF), re.S | re.M)
    assert m, (kind, name)
    return m.group(1)


def test_terraform():
    table = tf_block('google_bigquery_table', 'option_price_hourly')
    assert 'dataset_id               = google_bigquery_dataset.tokyo.dataset_id' in table
    assert 'table_id                 = "option_price_hourly"' in table
    assert 'schema                   = file("${path.module}/../../schema/tokyo/option_price_hourly.json")' in table
    assert re.search(r'time_partitioning \{\s*type\s*=\s*"MONTH"\s*field\s*=\s*"last_trading_day"\s*\}', table)
    assert 'clustering               = ["target_price", "type", "started_at"]' in table
    assert 'require_partition_filter = true' in table
    assert re.search(r'^\s*deletion_protection\s*=\s*true$', table, re.M)
    # パーティション・クラスタの列は表にあり、キーの列（REQUIRED）
    cols = {c['name']: c['mode'] for c in json.loads(read(SCHEMA))}
    for c in ('last_trading_day', 'target_price', 'type', 'started_at'):
        assert cols[c] == 'REQUIRED', c
    # 取り込みの表ではない（tables.json の for_each・apps/ingest の検査に入れない）
    assert 'option_price_hourly' not in read(TABLES_JSON)
    # 付与は表単位の dataEditor 1 件だけ（読み取りは既存のデータセット単位の dataViewer）
    grant = tf_block('google_bigquery_table_iam_member', 'sq_option_hourly_editor')
    assert 'table_id   = google_bigquery_table.option_price_hourly.table_id' in grant
    assert 'role       = "roles/bigquery.dataEditor"' in grant
    assert 'member     = "serviceAccount:${local.sa_sq}"' in grant
    assert len(re.findall(r'google_bigquery_table\.option_price_hourly\.table_id', read(SQ_TF))) == 1
    dts = tf_block('google_bigquery_data_transfer_config', 'option_hourly')
    assert 'display_name         = "optionchan option-hourly"' in dts
    assert 'location             = local.bq.dataset.location' in dts
    assert 'templatefile("${path.module}/../../sql/hourly/option_price_hourly.sql"' in dts
    assert ('depends_on = [google_bigquery_dataset_iam_member.sq_tokyo_viewer, '
            'google_bigquery_table_iam_member.sq_option_hourly_editor]') in dts
    # 本番の DTS が読むのは option_price_hourly.sql だけ（hourly_backfill.sql は手で流す）
    assert re.findall(r'templatefile\("\$\{path\.module\}/\.\./\.\./sql/hourly/(\w+\.sql)"', read(SQ_TF)) == ['option_price_hourly.sql']

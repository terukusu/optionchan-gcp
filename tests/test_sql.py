"""sql/latest/（最新版の選択のテーブル関数、#34）の静的検査。

- 各 SQL が infra/prod/data.tf の google_bigquery_routine.latest の for_each のキーと 1 対 1 で、Terraform から読まれている
- 期間の引数 start_at / end_at で snapshot_at（データの表は created_at も）を [start_at, end_at) に絞っている
  （option_price の require_partition_filter と ingest_runs の全件スキャンを避ける）
- スナップショットごとに finished_at・ingest_run_id の降順で 1 件を選ぶ
- プロジェクト名・データセット名を直書きしない（Terraform の templatefile が入れる）
BigQuery での実行の確認は bq query --dry_run（Phase 3 の手順、PR に記録）。
"""
import glob
import json
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


# ---- 日次の品質チェック・新旧の突き合わせ（#40）・1 時間足（#49）。BigQuery で動かす検査は tests/test_sql_bq.py・tests/test_ohlc_bq.py ----

SQ_TF = os.path.join(ROOT, 'infra', 'prod', 'sq.tf')
SCHEMA_DIR = os.path.join(ROOT, 'schema', 'tokyo')
# SQL → (Terraform の templatefile に渡す名前, 結果のテーブル, 日付の列)
SCHEDULED = {
    'sql/quality/quality_daily.sql': ({'project', 'dataset'}, 'quality_daily', 'window_date'),
    'sql/legacy/stage.sql': ({'project', 'legacy_dataset', 'stage_dataset'}, None, None),
    'sql/legacy/compare.sql': ({'project', 'dataset', 'copy_dataset', 'fail_on_unexplained', 'fail_from'}, 'legacy_compare_daily', 'compare_date'),
    # 1 時間足（#49）。結果は atm_* の 3 表への MERGE（検査は tests/test_ohlc.py）
    'sql/ohlc/atm_hourly.sql': ({'project', 'dataset'}, None, None),
    # 30 日固定の ATM IV（#100）。結果は atm_cm_iv の窓の置き換え（検査は tests/test_cm.py）
    'sql/cm/atm_cm.sql': ({'project', 'dataset'}, None, None),
}
WINDOW_DECLARE = (
    "DECLARE win_end TIMESTAMP DEFAULT TIMESTAMP(DATETIME(DATE(@run_time, 'Asia/Tokyo'), TIME '07:00:00'), 'Asia/Tokyo');\n"
    'DECLARE win_start TIMESTAMP DEFAULT TIMESTAMP_SUB(win_end, INTERVAL 1 DAY);'
)


def sq(path):
    return read(os.path.join(ROOT, path))


def top_level_items(select_list):
    """SELECT の列のリストを、かっこの外のカンマで分ける"""
    items, depth, cur = [], 0, ''
    for ch in select_list:
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
        if ch == ',' and depth == 0:
            items.append(cur)
            cur = ''
        else:
            cur += ch
    items.append(cur)
    return [i.strip() for i in items if i.strip()]


def result_columns(text):
    """-- result:begin 〜 -- result:end の最後の SELECT の列名（AS の別名か列名そのもの）"""
    block = re.search(r'^-- result:begin\n(.*?)^-- result:end$', text, re.S | re.M).group(1)
    m = re.search(r'\nSELECT\n(.*)\nFROM \w+\n$', block, re.S)
    assert m, '結果の SELECT が見つからない'
    return [re.split(r'\s+AS\s+', item)[-1].strip() for item in top_level_items(body(m.group(1)))]


def test_scheduled_sql_read_by_terraform():
    tf = read(SQ_TF)
    for path, (names, _, _) in SCHEDULED.items():
        call = re.search(r'templatefile\("\$\{path\.module\}/\.\./\.\./' + re.escape(path) + r'", \{(.*?)\}\)', tf, re.S)
        assert call, path
        assert set(re.findall(r'^\s*(\w+)\s*=', call.group(1), re.M)) == names, path
        # SQL の ${...} はすべて Terraform が渡す名前（未定義だと templatefile がエラーになる）
        assert set(re.findall(r'\$\{(\w+)\}', sq(path))) == names, path


@pytest.mark.parametrize('path', sorted(SCHEDULED))
def test_scheduled_sql_window_and_names(path):
    text = sq(path)
    # 窓は @run_time（DTS の予定時刻）の JST の日付から。手動実行で未来の窓を指定したら止める
    assert WINDOW_DECLARE in text
    assert re.search(r'IF win_end > CURRENT_TIMESTAMP\(\) THEN\s+SELECT ERROR\(', text)
    assert 'optionchan_tokyo' not in text and 'optionchan_legacy' not in text
    assert not re.search(r'optionchan-\d', text)
    for ref in re.findall(r'`([^`]+)`', body(text)):
        assert re.match(r'\$\{project\}\.\$\{\w+\}\.\w+$', ref), ref


@pytest.mark.parametrize('path', sorted(SCHEDULED))
def test_scheduled_sql_filters_period(path):
    """生テーブルは created_at / snapshot_at を窓で絞り、テーブル関数には窓（か窓の少し前から少し後まで）を渡す"""
    text = body(sq(path))
    for m in re.finditer(r'`\$\{project\}\.\$\{(\w+)\}\.(\w+)`(\((?:[^()]|\([^()]*\))*\))?', text):
        dataset, table, args = m.groups()
        if dataset == 'stage_dataset':
            continue  # stage.sql が作る作業用テーブル（書き込み先と、その件数の数え直し）
        if table == 'trading_calendar_latest':
            # 休業日のカレンダー（ビュー。1 年 十数行）は日付で絞る（30 日固定の SQ の日、#100）
            following = text[m.end():m.end() + 120]
            assert re.match(r"\s*WHERE calendar_date >= DATE\(win_start, 'Asia/Tokyo'\) AND calendar_date < ", following), (path, following)
            continue
        if table.endswith('_latest') or table == 'latest_runs':
            assert args and re.match(
                r'\((win_start|TIMESTAMP_SUB\(win_start, INTERVAL \d+ (DAY|HOUR)\)), '
                r'(win_end|TIMESTAMP_ADD\(win_end, INTERVAL \d+ HOUR\))\)$', args), (table, args)
        elif table in ('spot_price', 'future_price', 'option_price', 'ingest_runs'):
            col = 'snapshot_at' if table == 'ingest_runs' else 'created_at'
            following = text[m.end():m.end() + 120]
            assert re.match(rf'\s*WHERE {col} >= win_start AND {col} < win_end', following), (path, table, following)


@pytest.mark.parametrize('path', [p for p, (_, t, _) in SCHEDULED.items() if t])
def test_result_matches_schema_and_merge(path):
    names, table, date_col = SCHEDULED[path]
    text = sq(path)
    for marker in ('source:begin', 'source:end', 'result:begin', 'result:end'):
        assert len(re.findall(rf'^-- {marker}$', text, re.M)) == 1, marker
    with open(os.path.join(SCHEMA_DIR, f'{table}.json'), encoding='utf-8') as f:
        schema = json.load(f)
    # MERGE の INSERT ROW は位置で入るので、結果の列の順番がスキーマと同じであること
    assert result_columns(text) == [c['name'] for c in schema]
    for c in schema:
        assert c['description'] and c['mode'] in ('NULLABLE', 'REQUIRED', 'REPEATED'), c['name']
    merge = re.search(r'MERGE `\$\{project\}\.\$\{dataset\}\.(\w+)` AS t\nUSING result AS s\nON FALSE\n'
                      r'WHEN NOT MATCHED BY SOURCE AND t\.(\w+) = DATE\(win_start, \'Asia/Tokyo\'\) THEN DELETE\n'
                      r'WHEN NOT MATCHED THEN INSERT ROW;', text)
    assert merge and merge.groups() == (table, date_col)


def test_errors_after_recording():
    quality = sq('sql/quality/quality_daily.sql')
    compare = sq('sql/legacy/compare.sql')
    # 記録（MERGE）の後に違反・説明できない差分で失敗させる（新系の M2 で通知）
    assert re.search(r'MERGE .*IF \(SELECT violations FROM result\) > 0 THEN\s+SELECT ERROR\(FORMAT\(\'quality: ', quality, re.S)
    # 説明できない差分で失敗させるのは切り替え（infra/prod/sq.tf の local）が true で、窓の日付が下限以降のときだけ（#61。
    # 判定は結果の fail_on_unexplained 列に入れ、tests/test_sql_bq.py が固定値で確かめる）。コピーの欠けは常に失敗
    assert 'DECLARE fail_on_unexplained BOOL DEFAULT ${fail_on_unexplained};' in compare
    assert "DECLARE fail_from DATE DEFAULT DATE '${fail_from}';" in compare
    assert "fail_on_unexplained AND DATE(win_start, 'Asia/Tokyo') >= fail_from AS fail_on_unexplained," in compare
    assert re.search(r'MERGE .*IF \(SELECT fail_on_unexplained AND unexplained > 0 FROM result\) THEN\s+SELECT ERROR\(', compare, re.S)
    assert re.search(r'MERGE .*IF NOT \(SELECT copy_complete FROM result\) THEN\s+SELECT ERROR\(', compare, re.S)
    assert "'legacy_compare: unexplained " in compare
    tf = read(SQ_TF)
    assert re.search(r'^\s*legacy_compare_fail_on_unexplained = true$', tf, re.M)
    assert re.search(r'^\s*legacy_compare_fail_from\s+= "\d{4}-\d{2}-\d{2}"$', tf, re.M)
    assert 'fail_on_unexplained = local.legacy_compare_fail_on_unexplained' in tf
    assert re.search(r'fail_from\s+= local\.legacy_compare_fail_from', tf)
    # コピーが今回の窓のものでなければ、記録の前に止める
    guard = compare.index('staged_window` WHERE window_start = win_start AND window_end = win_end')
    assert guard < compare.index('CREATE TEMP TABLE result') < compare.index('MERGE ')


def test_stage_writes_only_stage_dataset():
    text = body(sq('sql/legacy/stage.sql'))
    writes = re.findall(r'(?:CREATE OR REPLACE TABLE|INSERT INTO|MERGE|DELETE FROM|UPDATE|TRUNCATE TABLE)\s+`([^`]+)`', text)
    # 書くのは作業用データセットの 4 表だけ。旧データセット（legacy_dataset）は FROM で読むだけ
    assert writes == ['${project}.${stage_dataset}.' + t for t in ('spot_price', 'future_price', 'option_price', 'staged_window')]
    assert len(re.findall(r'FROM `\$\{project\}\.\$\{legacy_dataset\}\.(\w+)`', text)) == 3


# ---- 品質チェックの本文の凍結（#88）: 取り込み（apps/ingest）と同じ回数・列・立会の時間 ----

INGEST_SETTINGS = os.path.join(ROOT, 'apps', 'ingest', 'src', 'optionchan_ingest', 'settings.py')
INGEST_SNAPSHOT = os.path.join(ROOT, 'apps', 'ingest', 'src', 'optionchan_ingest', 'snapshot.py')


def ingest_time(name):
    """settings.py の SESSION_* = time(h, m) を 'HH:MM:00' にする（このテストは apps/ingest を import しない）"""
    h, m = re.search(rf'^{name} = time\((\d+), (\d+)\)$', read(INGEST_SETTINGS), re.M).groups()
    return f'{int(h):02d}:{int(m):02d}:00'


def test_quality_body_frozen_matches_ingest():
    quality = sq('sql/quality/quality_daily.sql')
    snap = re.search(r'^body_snap AS \(\n(.*?)^\),$', quality, re.S | re.M).group(1)
    # 立会の [開始, 終了)
    t = "TIME(created_at, 'Asia/Tokyo')"
    day_open, day_close = ingest_time('SESSION_DAY_OPEN'), ingest_time('SESSION_DAY_CLOSE')
    night_open, night_close = ingest_time('SESSION_NIGHT_OPEN'), ingest_time('SESSION_NIGHT_CLOSE')
    assert f"({t} >= TIME '{day_open}' AND {t} < TIME '{day_close}')" in snap
    assert f"OR {t} >= TIME '{night_open}'" in snap
    assert f"OR {t} < TIME '{night_close}' AS in_session" in snap
    # 本文の列（snapshot.OPTION_BODY_COLUMNS と同じ並び）
    cols = re.search(r'OPTION_BODY_KEY = \(([^)]*)\)\nOPTION_BODY_COLUMNS = OPTION_BODY_KEY \+ \(([^)]*)\)',
                     read(INGEST_SNAPSHOT)).groups()
    ingest_cols = re.findall(r'"(\w+)"', cols[0] + cols[1])
    struct = re.search(r'TO_JSON_STRING\(STRUCT\(([^)]*)\)\)', snap).group(1)
    assert [c.strip() for c in struct.split(',')] == ingest_cols
    # 回数
    repeats = re.search(r'^BODY_FROZEN_REPEATS = (\d+)$', read(INGEST_SETTINGS), re.M).group(1)
    assert f'(SELECT COUNTIF(repeats >= {repeats}) FROM body_streak) AS body_frozen' in quality
    # 違反に数え、列は末尾（INSERT ROW は列の位置で入れる）
    assert '+ price_time_future + price_time_too_old + sunday + body_frozen AS violations,' in quality
    schema = json.load(open(os.path.join(SCHEMA_DIR, 'quality_daily.json'), encoding='utf-8'))
    assert schema[-1]['name'] == 'body_frozen' and schema[-1]['mode'] == 'NULLABLE'


# ---- 取引カレンダーの最新版のビュー（#63） ----

CALENDAR_SQL = os.path.join(ROOT, 'sql', 'calendar', 'trading_calendar_latest.sql')
CALENDAR_TF = os.path.join(ROOT, 'infra', 'prod', 'calendar.tf')


def test_calendar_view_read_by_terraform():
    tf = read(CALENDAR_TF)
    assert 'sql/calendar/trading_calendar_latest.sql' in tf
    assert re.search(r'table_id\s*=\s*"trading_calendar_latest"', tf)
    assert 'use_legacy_sql = false' in tf


def test_calendar_view_picks_max_version_with_hash():
    sql = body(read(CALENDAR_SQL))
    assert 'optionchan_tokyo' not in sql and not re.search(r'optionchan-\d', sql)
    for ref in re.findall(r'`([^`]+)`', sql):
        assert ref == '${project}.${dataset}.trading_calendar', ref
    # content_hash も返す（ingest が再計算して照合する）
    assert re.search(r'^SELECT \*$', sql, re.M)
    assert re.search(r'WHERE version_at = \(SELECT MAX\(version_at\) FROM `[^`]+`\)', sql)

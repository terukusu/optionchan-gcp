"""sql/legacy_import/（過去データの移行、#12）の静的検査と、sql/legacy/compare.sql の移行行の除外。

- 参照する表はすべて `${project}.${名前}.<表>`（名前は runbook の render が入れる）で、プロジェクト・データセットを直書きしない
- フィンガープリント（STRUCT）と INSERT の旧スキーマの列が schema/<表>.json と同じ順。INSERT の列は schema/tokyo/<表>.json にあり、
  ingest_runs は全列を順に入れる
- option_price（require_partition_filter）とその他の表は、読む・消す箇所をすべて created_at / snapshot_at の範囲で絞る
- insert.sql の文の順（範囲の検査 → 残骸の削除 → 移す T → データの表 → ingest_runs）と、id の書式が insert / verify で同じ
BigQuery での実行の確認は tests/test_legacy_import_bq.py（OPTIONCHAN_BQ_TEST=1）。
"""
import json
import os
import re

import pytest

ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
SQL_DIR = os.path.join(ROOT, 'sql', 'legacy_import')
TABLES = ('spot_price', 'future_price', 'option_price')
RUN_ID = "FORMAT('legacy-%s', FORMAT_TIMESTAMP('%Y%m%dT%H%M%SZ', {}, 'UTC'))"
# SQL → 置換する名前
NAMES = {
    'fingerprint.sql': {'project', 'source_dataset'},
    'insert.sql': {'project', 'dataset', 'import_dataset'},
    'verify.sql': {'project', 'dataset', 'import_dataset'},
}


def read(path):
    with open(path, encoding='utf-8') as f:
        return f.read()


def body(text):
    """コメントを除いた本文"""
    return '\n'.join(line for line in text.splitlines() if not line.lstrip().startswith('--'))


def columns(path):
    return [c['name'] for c in json.loads(read(path))]


SQL = {name: read(os.path.join(SQL_DIR, name)) for name in NAMES}
LEGACY = {t: columns(os.path.join(ROOT, 'schema', f'{t}.json')) for t in TABLES}
TOKYO = {t: columns(os.path.join(ROOT, 'schema', 'tokyo', f'{t}.json')) for t in (*TABLES, 'ingest_runs')}


def names_list(text):
    return [re.sub(r'^\w+\.', '', c.strip()) for c in text.split(',') if c.strip()]


def test_files():
    assert sorted(os.listdir(SQL_DIR)) == sorted(NAMES)


@pytest.mark.parametrize('name', sorted(NAMES))
def test_names_are_placeholders(name):
    text = SQL[name]
    assert set(re.findall(r'\$\{(\w+)\}', text)) == NAMES[name]
    # 本文にデータセット名を直書きしない（コメントの説明は可。'legacy:optionchan' は完了記録の値）
    assert not re.search(r'optionchan_\w+', body(text))
    assert not re.search(r'optionchan-\d', text)
    for ref in re.findall(r'`([^`]+)`', body(text)):
        assert re.match(r'\$\{project\}\.\$\{\w+\}\.\w+$', ref), ref


@pytest.mark.parametrize('name', sorted(NAMES))
def test_fingerprint_columns_match_legacy_schema(name):
    """フィンガープリントの STRUCT は旧スキーマの全列を schema/<表>.json の順に（US と東京で同じ値になるように）"""
    structs = re.findall(r'FARM_FINGERPRINT\(TO_JSON_STRING\(STRUCT\((.*?)\)\)\)', body(SQL[name]), re.S)
    if name == 'insert.sql':
        assert structs == []
        return
    expected = [LEGACY[t] for t in TABLES] * (2 if name == 'verify.sql' else 1)
    assert [names_list(s) for s in structs] == expected


@pytest.mark.parametrize('name', ['verify.sql'])
def test_verify_sources_read_legacy_columns(name):
    text = body(SQL[name])
    for t in TABLES:
        m = re.search(rf'legacy_{t.split("_")[0]} AS \(\s*SELECT(.*?)FROM `\$\{{project\}}\.\$\{{import_dataset\}}\.{t}`', text, re.S)
        assert m and names_list(m.group(1)) == LEGACY[t], t
        m = re.search(rf'tokyo_{t.split("_")[0]} AS \(\s*SELECT(.*?)FROM `\$\{{project\}}\.\$\{{dataset\}}\.{t}`', text, re.S)
        assert m and names_list(m.group(1)) == LEGACY[t] + ['ingest_run_id'], t


def test_insert_columns():
    text = body(SQL['insert.sql'])
    inserts = re.findall(r'INSERT INTO `\$\{project\}\.\$\{dataset\}\.(\w+)` \((.*?)\)\nSELECT(.*?)\n(?:FROM|JOIN)', text, re.S)
    assert [t for t, _, _ in inserts] == [*TABLES, 'ingest_runs']
    for table, cols, select in inserts:
        cols = names_list(cols)
        if table == 'ingest_runs':
            # 完了記録は全列を順に（REQUIRED の列を落とさない）
            assert cols == TOKYO['ingest_runs']
        else:
            # 旧スキーマの列 + ingest_run_id。追加列（page_updated_at・price_time_raw・parser_version・source_object）は NULL のまま
            assert cols == LEGACY[table] + ['ingest_run_id']
            assert set(cols) <= set(TOKYO[table])
            assert names_list(select) == LEGACY[table] + ['ingest_run_id']


def test_insert_order_and_guards():
    text = body(SQL['insert.sql'])
    pos = [text.index(s) for s in (
        'IF start_at IS NULL OR end_at IS NULL OR start_at >= end_at THEN',
        'IF checked_until IS NULL OR end_at > checked_until THEN',
        'DELETE FROM `${project}.${dataset}.spot_price`',
        'DELETE FROM `${project}.${dataset}.future_price`',
        'DELETE FROM `${project}.${dataset}.option_price`',
        'CREATE TEMP TABLE targets AS',
        'IF EXISTS (SELECT 1 FROM targets GROUP BY ingest_run_id HAVING COUNT(*) > 1) THEN',
        'INSERT INTO `${project}.${dataset}.spot_price`',
        'INSERT INTO `${project}.${dataset}.future_price`',
        'INSERT INTO `${project}.${dataset}.option_price`',
        'INSERT INTO `${project}.${dataset}.ingest_runs`',
    )]
    assert pos == sorted(pos)
    assert 'SELECT MAX(window_end) FROM `${project}.${dataset}.quality_daily`' in text
    # 残骸の削除は legacy- の行で、範囲内の完了記録に無いものだけ（新系の行は消さない）
    deletes = re.findall(r'DELETE FROM `[^`]+`\n(WHERE .*?);', text, re.S)
    assert len(deletes) == 3
    for d in deletes:
        assert "AND STARTS_WITH(ingest_run_id, 'legacy-')" in d
        assert 'AND ingest_run_id NOT IN (' in d
    # 完了記録の値
    assert ("'legacy:optionchan', CAST(NULL AS INT64), 'legacy', 'legacy_import',\n"
            '  rows_spot, rows_future, rows_option, snapshot_at, snapshot_at') in text


def test_run_id_format_is_shared():
    assert RUN_ID.format('s.created_at') in SQL['insert.sql']
    assert RUN_ID.format('snapshot_at') in SQL['verify.sql']
    assert RUN_ID.format('created_at') in SQL['verify.sql']


@pytest.mark.parametrize('name', sorted(NAMES))
def test_tables_filtered_by_range(name):
    """表を読む・消す箇所はすべて期間で絞る（option_price の require_partition_filter、全件スキャンの防止）"""
    text = body(SQL[name])
    start, end = ('@start_at', '@end_at') if name == 'fingerprint.sql' else ('start_at', 'end_at')
    refs = list(re.finditer(r'`\$\{project\}\.\$\{(\w+)\}\.(\w+)`( AS \w+)?', text))
    assert refs
    for m in refs:
        dataset, table, alias = m.groups()
        if table == 'quality_daily':
            continue  # 1 日 1 行の小さい表（範囲の上限の検査）
        if text[m.start() - len('INSERT INTO '):m.start()] == 'INSERT INTO ':
            continue  # 書き込み先
        col = 'snapshot_at' if table == 'ingest_runs' else 'created_at'
        prefix = f'{alias.split()[-1]}.' if alias else ''
        following = text[m.end():m.end() + 200]
        assert re.search(rf'WHERE {prefix}{col} >= {start} AND {prefix}{col} < {end}', following), (name, table, following)


def test_compare_excludes_legacy_import_rows():
    """新旧の突き合わせの新系側（最新版のテーブル関数 4 つ）から移行行を除く"""
    text = body(read(os.path.join(ROOT, 'sql', 'legacy', 'compare.sql')))
    for cte in ('new_runs', 'new_spot', 'new_future', 'new_option'):
        m = re.search(rf'^{cte} AS \((.*?)^\),', text, re.S | re.M)
        assert m, cte
        assert re.search(r"\)\n  WHERE NOT STARTS_WITH\(ingest_run_id, 'legacy-'\)\n$", m.group(1)), cte

"""sql/tick_fill/（DataCube の歩み値での欠け 2 の穴埋め、#111）と歩み値の表の静的検査。

- 参照する表はすべて `${project}.${dataset}.<表>`（名前は runbook の render が入れる）で、プロジェクト・データセットを直書きしない
- 補完の版（fill_version）と欠け 2 の境目（gap_start・gap_end）が fill / verify / rollback / accuracy で同じ
- accuracy.sql の推定（関数と、source 以外の結果の SELECT）が fill.sql と同じ（一致率を測った推定と、書き込む推定が同じ）。関数は verify も同じ
- 表を読む・書く箇所はすべてパーティション列で絞る（option_price は created_at、dc_option_tick は trade_date。require_partition_filter）
- fill.sql の UPDATE は未補完の旧系の NULL の行（volume あり）だけに当たり、price・price_time・parser_version だけを書く。rollback.sql は
  補完の印のある旧系の行だけを戻す。書き込む列は schema/tokyo/option_price.json にある
- 歩み値の表の列（schema/tokyo/dc_option_tick.json）が CSV の 13 列の並び。Terraform の表は tests/test_infra.py が見る
BigQuery での実行の確認は tests/test_tick_fill_bq.py（OPTIONCHAN_BQ_TEST=1）。
"""
import json
import os
import re

import pytest

ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
SQL_DIR = os.path.join(ROOT, 'sql', 'tick_fill')
NAMES = ('accuracy.sql', 'fill.sql', 'rollback.sql', 'verify.sql')
FILL_VERSION = "DECLARE fill_version STRING DEFAULT 'legacy-fill111-1';"
GAP = ("DECLARE gap_start TIMESTAMP DEFAULT TIMESTAMP '2023-04-25 04:42:00+00';\n"
       "DECLARE gap_end TIMESTAMP DEFAULT TIMESTAMP '2023-05-08 07:31:00+00';")
TICK_SCHEMA = os.path.join(ROOT, 'schema', 'tokyo', 'dc_option_tick.json')
# CSV の 13 列（並びは 26 ファイルとも同じ。列名は版で違うので、表の列名はこちらで決める）
TICK_COLUMNS = [
    ('trade_date', 'DATE'), ('execution_date', 'DATE'), ('index_type', 'STRING'), ('put_call_type', 'INTEGER'),
    ('security_code', 'STRING'), ('execution_time', 'TIME'), ('trade_price', 'NUMERIC'), ('price_type', 'STRING'),
    ('trade_volume', 'INTEGER'), ('seq_no', 'INTEGER'), ('contract_month', 'STRING'), ('exercise_price', 'NUMERIC'),
    ('cash_futures_type', 'INTEGER'),
]


def read(path):
    with open(path, encoding='utf-8') as f:
        return f.read()


def body(text):
    """コメントを除いた本文"""
    return '\n'.join(line for line in text.splitlines() if not line.lstrip().startswith('--'))


def block(text, name):
    m = re.search(rf'^-- {name}:begin\n(.*?)^-- {name}:end$', text, re.S | re.M)
    assert m, name
    return m


SQL = {name: read(os.path.join(SQL_DIR, name)) for name in NAMES}
OPTION = {c['name'] for c in json.loads(read(os.path.join(ROOT, 'schema', 'tokyo', 'option_price.json')))}


def test_files():
    assert sorted(os.listdir(SQL_DIR)) == sorted(NAMES)


@pytest.mark.parametrize('name', NAMES)
def test_names_are_placeholders(name):
    text = SQL[name]
    assert set(re.findall(r'\$\{(\w+)\}', text)) == {'project', 'dataset'}
    assert not re.search(r'optionchan_\w+', body(text))
    assert not re.search(r'optionchan-\d', text)
    for ref in re.findall(r'`([^`]+)`', body(text)):
        assert re.match(r'\$\{project\}\.\$\{dataset\}\.\w+$', ref), ref


@pytest.mark.parametrize('name', NAMES)
def test_fill_version_is_shared(name):
    text = body(SQL[name])
    assert text.count(FILL_VERSION) == 1
    assert 'legacy-fill111' not in text.replace(FILL_VERSION, '').replace("LIKE 'legacy-fill111-%'", '')
    if name in ('fill.sql', 'verify.sql'):
        assert GAP in text


def test_accuracy_uses_same_estimate_as_fill():
    fill, acc = SQL['fill.sql'], SQL['accuracy.sql']
    for name in ('accuracy.sql', 'verify.sql'):
        assert block(fill, 'functions').group(1) == block(SQL[name], 'functions').group(1), name

    def without_source(text):
        result = block(text, 'result').group(1)
        m = block(result, 'source')
        return result[:m.start()] + result[m.end():]

    assert without_source(fill) == without_source(acc)
    # 歩み値の読み方（source の tick）も同じ
    tick = re.compile(r'^tick AS \(.*?^\),$', re.S | re.M)
    src = [block(block(t, 'result').group(1), 'source').group(1) for t in (fill, acc)]
    assert tick.search(src[0]).group(0) == tick.search(src[1]).group(0)


def test_fill_order_and_guards():
    text = body(SQL['fill.sql'])
    pos = [text.index(s) for s in (
        'IF start_at IS NULL OR end_at IS NULL OR start_at >= end_at THEN',
        'IF start_at < gap_start OR end_at > gap_end THEN',
        "IF IFNULL(mode, '') NOT IN ('dryrun', 'apply') THEN",
        'IF calendar_from IS NULL OR calendar_from > tick_from THEN',
        'IF ARRAY_LENGTH(IFNULL(missing_new_year, [])) > 0 THEN',
        'IF other_versions > 0 THEN',
        'IF tick_duplicates > 0 THEN',
        'CREATE TEMP FUNCTION is_open(',
        'CREATE TEMP TABLE est AS',
        'IF ARRAY_LENGTH(IFNULL(missing_days, [])) > 0 THEN',
        "IF mode = 'apply' THEN",
        'UPDATE `${project}.${dataset}.option_price` AS t',
    )]
    assert pos == sorted(pos)
    assert "AND parser_version LIKE 'legacy-fill111-%' AND parser_version != fill_version" in text
    assert 'GROUP BY trade_date, security_code, seq_no\n    HAVING COUNT(*) > 1' in text


def test_fill_writes_only_matched_rows():
    result = block(SQL['fill.sql'], 'result').group(1)
    assert "IF(e.basis = 'matched', CAST(e.m.trade_price AS INT64), NULL) AS new_price" in result
    assert "IF(e.basis = 'matched', DATETIME_TRUNC(e.m.exec_at, MINUTE), NULL) AS new_price_time" in result
    # 対象は volume がある行だけ（ページの表示が空の行は埋めない）
    src = block(result, 'source').group(1)
    assert "STARTS_WITH(ingest_run_id, 'legacy-') AND volume IS NOT NULL" in src
    assert '((price IS NULL AND price_time IS NULL AND parser_version IS NULL) OR parser_version = fill_version)' in src


def test_fill_updates_only_unfilled_legacy_rows():
    text = body(SQL['fill.sql'])
    updates = re.findall(r'UPDATE `\$\{project\}\.\$\{dataset\}\.(\w+)` AS t\n  (SET .*?)\n  (FROM .*?);', text, re.S)
    assert len(updates) == 1
    table, set_clause, rest = updates[0]
    assert table == 'option_price'
    assert set_clause == 'SET price = e.new_price, price_time = e.new_price_time, parser_version = fill_version'
    assert {'price', 'price_time', 'parser_version'} <= OPTION
    assert "FROM est WHERE basis = 'matched'" in rest
    for cond in ('t.created_at >= start_at AND t.created_at < end_at',
                 "STARTS_WITH(t.ingest_run_id, 'legacy-')",
                 't.price IS NULL AND t.price_time IS NULL AND t.volume IS NOT NULL AND t.parser_version IS NULL'):
        assert cond in rest, cond
    # 推定の行（キーごとに 1 つ）とは、キーの列すべてで対応づける（1 行に 2 つの推定が当たらない）
    for k in ('created_at', 'last_trading_day', 'type', 'target_price', 'volume'):
        assert f'e.{k} = t.{k}' in rest, k
        assert k in re.search(r'SELECT DISTINCT (.*?)\n', rest).group(1)


def test_rollback_restores_only_marked_legacy_rows():
    text = body(SQL['rollback.sql'])
    updates = re.findall(r'UPDATE `\$\{project\}\.\$\{dataset\}\.(\w+)`\n(SET .*?)\n(WHERE .*?);', text, re.S)
    assert [t for t, _, _ in updates] == ['option_price']
    _, set_clause, where = updates[0]
    assert set_clause == 'SET price = NULL, price_time = NULL, parser_version = NULL'
    assert 'created_at >= start_at AND created_at < end_at' in where
    assert "STARTS_WITH(ingest_run_id, 'legacy-') AND parser_version = fill_version" in where


@pytest.mark.parametrize('name', NAMES)
def test_tables_filtered_by_partition(name):
    """表を読む・書く箇所はすべてパーティション列で絞る（require_partition_filter、全件スキャンの防止）"""
    text = body(SQL[name])
    refs = list(re.finditer(r'`\$\{project\}\.\$\{dataset\}\.(\w+)`( AS \w+)?', text))
    assert refs
    for m in refs:
        table, alias = m.groups()
        if table == 'trading_calendar_latest':
            continue  # 休業日の一覧（数百行のビュー）
        prefix = f'{alias.split()[-1]}.' if alias else ''
        following = text[m.end():m.end() + 400]
        if table == 'dc_option_tick':
            pattern = rf'WHERE {prefix}trade_date >= tick_from AND {prefix}trade_date <= tick_to'
        else:
            assert table == 'option_price', table
            pattern = rf'WHERE {prefix}created_at >= start_at AND {prefix}created_at < end_at'
        assert re.search(pattern, following), (name, table, following)


def test_tick_range_is_shared():
    for name in ('accuracy.sql', 'fill.sql'):
        text = body(SQL[name])
        assert "DECLARE tick_from DATE DEFAULT DATE(start_at, 'Asia/Tokyo');" in text
        assert "DECLARE tick_to DATE DEFAULT DATE_ADD(DATE(end_at, 'Asia/Tokyo'), INTERVAL 14 DAY);" in text


def test_tick_schema_matches_csv():
    cols = json.loads(read(TICK_SCHEMA))
    assert [(c['name'], c['type']) for c in cols] == TICK_COLUMNS
    assert all(c['mode'] == 'REQUIRED' and c['description'] for c in cols)


def test_tick_columns_used_exist():
    used = set()
    for name in ('accuracy.sql', 'fill.sql'):
        m = re.search(r'^tick AS \(\n.*?SELECT (.*?)\n', SQL[name], re.S | re.M)
        used |= {c.strip() for c in m.group(1).split(',')}
    assert used <= {c for c, _ in TICK_COLUMNS}


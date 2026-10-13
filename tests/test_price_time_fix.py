"""sql/price_time_fix/（過去データの price_time の補正、#4）の静的検査。

- 参照する表はすべて `${project}.${dataset}.<表>`（名前は runbook の render が入れる）で、プロジェクト・データセットを直書きしない
- 補正の版（fix_version）と元の値の書式（'%F %T'）が fix / verify / rollback / accuracy で同じ
- accuracy.sql の推定（関数と、source 以外の結果の SELECT）が fix.sql と同じ（一致率を測った推定と、書き込む推定が同じ）
- 表を読む・書く箇所はすべて created_at（ingest_runs は snapshot_at、テーブル関数は引数）の範囲で絞る（option_price の require_partition_filter）
- fix.sql の UPDATE は未補正の旧系の行（legacy-、@fixed_at より前、price_time あり）だけに当たり、元の値・版を残す。rollback.sql は
  補正の印のある旧系の行だけを戻す。書き込む列は schema/tokyo/<表>.json にある
BigQuery での実行の確認は tests/test_price_time_fix_bq.py（OPTIONCHAN_BQ_TEST=1）。
"""
import json
import os
import re

import pytest

ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
SQL_DIR = os.path.join(ROOT, 'sql', 'price_time_fix')
TABLES = ('spot_price', 'future_price', 'option_price')
NAMES = ('accuracy.sql', 'fix.sql', 'rollback.sql', 'verify.sql')
FIX_VERSION = "DECLARE fix_version STRING DEFAULT 'legacy-fix4-1';"


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


def columns(path):
    return {c['name'] for c in json.loads(read(path))}


SQL = {name: read(os.path.join(SQL_DIR, name)) for name in NAMES}
TOKYO = {t: columns(os.path.join(ROOT, 'schema', 'tokyo', f'{t}.json')) for t in TABLES}


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
def test_fix_version_and_raw_format_are_shared(name):
    text = body(SQL[name])
    assert text.count(FIX_VERSION) == 1
    assert 'legacy-fix4' not in text.replace(FIX_VERSION, '').replace("LIKE 'legacy-fix4-%'", '')
    # 元の値は '%F %T'（YYYY-MM-DD HH:MM:SS）で書き、読む
    formats = re.findall(r"(?:FORMAT|PARSE)_DATETIME\('([^']*)'", text)
    assert set(formats) <= {'%F %T'}
    if name != 'accuracy.sql':
        assert formats


def test_accuracy_uses_same_estimate_as_fix():
    fix, acc = SQL['fix.sql'], SQL['accuracy.sql']
    # 立会の判定の関数は fix・accuracy・verify で同じ
    for name in ('accuracy.sql', 'verify.sql'):
        assert block(fix, 'functions').group(1) == block(SQL[name], 'functions').group(1), name

    def without_source(text):
        result = block(text, 'result').group(1)
        m = block(result, 'source')
        return result[:m.start()] + result[m.end():]

    assert without_source(fix) == without_source(acc)


def test_fix_order_and_guards():
    text = body(SQL['fix.sql'])
    pos = [text.index(s) for s in (
        'IF start_at IS NULL OR end_at IS NULL OR fixed_at IS NULL OR start_at >= end_at THEN',
        'IF end_at > fixed_at THEN',
        "IF IFNULL(mode, '') NOT IN ('dryrun', 'apply') THEN",
        'IF IFNULL(require_calendar, TRUE) AND (calendar_from IS NULL',
        'IF IFNULL(require_calendar, TRUE) AND ARRAY_LENGTH(IFNULL(missing_new_year, [])) > 0 THEN',
        'IF other_versions > 0 THEN',
        'CREATE TEMP FUNCTION is_open(',
        'CREATE TEMP TABLE est AS',
        "IF mode = 'apply' THEN",
        'UPDATE `${project}.${dataset}.spot_price` AS t',
        'UPDATE `${project}.${dataset}.future_price` AS t',
        'UPDATE `${project}.${dataset}.option_price` AS t',
    )]
    assert pos == sorted(pos)
    # 文脈は範囲の 100 日前から（約定の始まりを範囲の外で見つける）
    assert 'DECLARE ctx_start TIMESTAMP DEFAULT TIMESTAMP_SUB(start_at, INTERVAL 100 DAY);' in text
    # 別の版の補正が残っていれば止める
    assert "WHERE parser_version LIKE 'legacy-fix4-%' AND parser_version != fix_version" in text


def test_fix_changes_only_determined_rows():
    """日付が一意に決まらない行（determined 以外）は、約定の始まり以外の行も含めて元のまま"""
    result = block(SQL['fix.sql'], 'result').group(1)
    assert "IF(s.basis = 'determined', s.a, r.b) AS new_price_time" in result
    assert 'IFNULL(s.a, r.b) AS new_price_time' not in result


def test_fix_updates_only_unfixed_legacy_rows():
    text = body(SQL['fix.sql'])
    updates = re.findall(r'UPDATE `\$\{project\}\.\$\{dataset\}\.(\w+)` AS t\n  (SET .*?)\n  (FROM est AS e\n.*?);', text, re.S)
    assert [t for t, _, _ in updates] == list(TABLES)
    for table, set_clause, where in updates:
        assert set_clause == ("SET price_time = e.new_price_time, price_time_raw = FORMAT_DATETIME('%F %T', t.price_time), "
                              'parser_version = fix_version')
        assert {'price_time', 'price_time_raw', 'parser_version'} <= TOKYO[table]
        for cond in ('t.created_at >= start_at AND t.created_at < end_at',
                     "t.created_at < fixed_at AND STARTS_WITH(t.ingest_run_id, 'legacy-')",
                     't.parser_version IS NULL AND t.price_time IS NOT NULL',
                     "e.basis = 'determined'",
                     f"e.tbl = '{table}' AND e.created_at = t.created_at AND e.b = t.price_time"):
            assert cond in where, (table, cond)
        # 推定の行（重複をまとめたキー）とは、表の行のキーの列すべてで対応づける（1 行に 2 つの推定が当たらない）
        keys = {'spot_price': ['price'], 'future_price': ['contract_month', 'price'],
                'option_price': ['last_trading_day', 'type', 'target_price', 'price', 'volume']}[table]
        for k in keys:
            assert re.search(rf'\be\.{k}\b', where) and re.search(rf'\bt\.{k}\b', where), (table, k)


def test_rollback_restores_only_marked_legacy_rows():
    text = body(SQL['rollback.sql'])
    updates = re.findall(r'UPDATE `\$\{project\}\.\$\{dataset\}\.(\w+)`\n(SET .*?)\n(WHERE .*?);', text, re.S)
    assert [t for t, _, _ in updates] == list(TABLES)
    for table, set_clause, where in updates:
        assert set_clause == ("SET price_time = SAFE.PARSE_DATETIME('%F %T', price_time_raw), price_time_raw = NULL, "
                              'parser_version = NULL')
        assert "STARTS_WITH(ingest_run_id, 'legacy-') AND parser_version = fix_version" in where
        assert "SAFE.PARSE_DATETIME('%F %T', price_time_raw) IS NOT NULL" in where


@pytest.mark.parametrize('name', NAMES)
def test_tables_filtered_by_range(name):
    """表を読む・書く箇所はすべて期間で絞る（option_price の require_partition_filter、全件スキャンの防止）"""
    text = body(SQL[name])
    refs = list(re.finditer(r'`\$\{project\}\.\$\{dataset\}\.(\w+)`(\(([^)]*)\))?( AS \w+)?', text))
    assert refs
    for m in refs:
        table, _, args, alias = m.groups()
        if table == 'trading_calendar_latest':
            continue  # 休業日の一覧（数百行のビュー）
        if table.endswith('_latest'):
            assert args in ('ctx_start, end_at', 'start_at, end_at'), (name, table, args)
            continue
        col = 'snapshot_at' if table == 'ingest_runs' else 'created_at'
        prefix = f'{alias.split()[-1]}.' if alias else ''
        following = text[m.end():m.end() + 300]
        assert re.search(rf'WHERE {prefix}{col} >= (start_at|ctx_start) AND {prefix}{col} < end_at', following), (
            name, table, following)


def test_estimate_reads_legacy_columns_only():
    """推定の入力（source）は旧系にもある列と、補正の印の列だけ"""
    src = block(block(SQL['fix.sql'], 'result').group(1), 'source').group(1)
    for table in TABLES:
        m = re.search(rf'SELECT(.*?)FROM `\$\{{project\}}\.\$\{{dataset\}}\.{table}`', src, re.S)
        assert m, table
        used = set(re.findall(r'\b([a-z_]+)\b', m.group(1))) & TOKYO[table]
        assert used <= {'last_trading_day', 'type', 'target_price', 'contract_month', 'created_at', 'price_time', 'price',
                        'volume', 'price_time_raw', 'parser_version', 'ingest_run_id'}, (table, used)
    # 推定は旧系の行（@fixed_at より前）だけで並べる（新系の行が混ざると約定の始まりがずれる）
    assert src.count("AND created_at < fixed_at AND STARTS_WITH(ingest_run_id, 'legacy-')") == 3

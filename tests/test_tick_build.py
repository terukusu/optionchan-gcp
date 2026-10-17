"""sql/tick_build/（DataCube の歩み値での欠け 1・3 の行の作成、#114）と先物の歩み値の表の静的検査。

- 参照する表はすべて `${project}.${dataset}.<表>`（名前は runbook の render が入れる）で、プロジェクト・データセットを直書きしない
- 作成の版（build_version）・範囲（range_start・range_end）・区間のあき（hole_gap_minutes）が build / verify / rollback / accuracy で同じ
- accuracy.sql の作り方（関数と、source 以外の結果の SELECT）が build.sql と同じ（一致率を測った作り方と、書き込む作り方が同じ）。
  歩み値の読み方（source の opt_tick・fut_tick）も同じ。違うのはスナップショットの候補（cand）と every_minute だけ
- 表を読む・書く箇所はすべてパーティション列で絞る（option_price・future_price は created_at、ingest_runs は snapshot_at、歩み値は trade_date）
- build.sql は INSERT だけ（3 表、まだ作っていないスナップショットだけ、1 つのトランザクション）。UPDATE・DELETE・MERGE は無い。
  rollback.sql の DELETE は作った印のある行だけ。書き込む列は schema/tokyo/ の列の定義にある
- 先物の歩み値の表の列（schema/tokyo/dc_future_tick.json）が CSV の 11 列の並び。Terraform の表は tests/test_infra.py が見る
BigQuery での実行の確認は tests/test_tick_build_bq.py（OPTIONCHAN_BQ_TEST=1）。
"""
import json
import os
import re

import pytest

ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
SQL_DIR = os.path.join(ROOT, 'sql', 'tick_build')
NAMES = ('accuracy.sql', 'build.sql', 'rollback.sql', 'verify.sql')
VERSION = "DECLARE build_version STRING DEFAULT 'legacy-fill114-1';"
RANGE = ("DECLARE range_start TIMESTAMP DEFAULT TIMESTAMP '2018-09-30 15:00:00+00';\n"
         "DECLARE range_end TIMESTAMP DEFAULT TIMESTAMP '2024-08-06 03:25:00+00';\n"
         "DECLARE hole_gap_minutes INT64 DEFAULT 15;")
FUTURE_SCHEMA = os.path.join(ROOT, 'schema', 'tokyo', 'dc_future_tick.json')
# CSV の 11 列（2022-08 以前のファイルは最後の sco_category が無い 10 列。列名は版で違うので、表の列名はこちらで決める）
FUTURE_COLUMNS = [
    ('trade_date', 'DATE'), ('execution_date', 'DATE'), ('index_type', 'STRING'), ('security_code', 'STRING'),
    ('execution_time', 'TIME'), ('trade_price', 'NUMERIC'), ('price_type', 'STRING'), ('trade_volume', 'INTEGER'),
    ('seq_no', 'INTEGER'), ('contract_month', 'STRING'), ('sco_category', 'INTEGER'),
]
RUN_ID = "FORMAT('legacy-fill114-%s', FORMAT_TIMESTAMP('%Y%m%dT%H%M%SZ', "


def read(path):
    with open(path, encoding='utf-8') as f:
        return f.read()


def body(text):
    """コメントを除いた本文（JS の関数の中の // コメントは残る）"""
    return '\n'.join(line for line in text.splitlines() if not line.lstrip().startswith('--'))


def block(text, name):
    m = re.search(rf'^-- {name}:begin\n(.*?)^-- {name}:end$', text, re.S | re.M)
    assert m, name
    return m


def cte(text, name):
    m = re.search(rf'^{name} AS \(\n.*?^\),$', text, re.S | re.M)
    assert m, name
    return m.group(0)


SQL = {name: read(os.path.join(SQL_DIR, name)) for name in NAMES}


def schema(table):
    return {c['name'] for c in json.loads(read(os.path.join(ROOT, 'schema', 'tokyo', f'{table}.json')))}


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
def test_version_is_shared(name):
    text = body(SQL[name])
    assert text.count(VERSION) == 1
    # 印の文字列は版の宣言・接頭辞（'legacy-fill114-'）・ingest_run_id の形の 3 つだけ
    rest = text.replace(VERSION, '').replace("'legacy-fill114-'", '').replace("'legacy-fill114-%s'", '')
    assert 'legacy-fill114' not in rest
    if name in ('build.sql', 'verify.sql'):
        assert RANGE in text


def test_run_id_is_shared():
    for name in ('build.sql', 'verify.sql'):
        assert RUN_ID in SQL[name], name
    # ingest_runs の trigger と source_object の値
    assert "'datacube:tick', build_version, 'legacy_fill114'" in SQL['build.sql']
    assert "trigger != 'legacy_fill114'" in SQL['verify.sql']
    assert "trigger = 'legacy_fill114'" in SQL['rollback.sql']


def without_cand(text):
    result = block(text, 'result').group(1)
    src = block(result, 'source')
    return result[:src.start()] + result[src.end():]


def test_accuracy_uses_same_construction_as_build():
    build, acc = SQL['build.sql'], SQL['accuracy.sql']
    assert block(build, 'functions').group(1) == block(acc, 'functions').group(1)
    assert without_cand(build) == without_cand(acc)
    # 歩み値の読み方（source の opt_tick・fut_tick）も同じ。違うのはスナップショットの候補（cand）だけ
    src = [block(block(t, 'result').group(1), 'source').group(1) for t in (build, acc)]
    for name in ('opt_tick', 'fut_tick'):
        assert cte(src[0], name) == cte(src[1], name), name
    assert 'FROM hole_stats AS h' in cte(src[0], 'cand')
    assert 'GENERATE_TIMESTAMP_ARRAY(start_at, TIMESTAMP_SUB(end_at, INTERVAL 1 MINUTE), INTERVAL 1 MINUTE)' in cte(src[1], 'cand')
    assert 'DECLARE every_minute BOOL DEFAULT FALSE;' in build
    assert 'DECLARE every_minute BOOL DEFAULT TRUE;' in acc
    # 歩み値を読む範囲の宣言も同じ
    for decl in ("DECLARE tick_from DATE DEFAULT DATE(start_at, 'Asia/Tokyo');",
                 "DECLARE tick_to DATE DEFAULT DATE_ADD(DATE(end_at, 'Asia/Tokyo'), INTERVAL 14 DAY);",
                 'DECLARE opt_from DATE DEFAULT DATE_SUB(tick_from, INTERVAL 10 DAY);'):
        assert decl in build and decl in acc, decl


def test_build_order_and_guards():
    text = body(SQL['build.sql'])
    pos = [text.index(s) for s in (
        'IF start_at IS NULL OR end_at IS NULL OR start_at >= end_at THEN',
        'IF start_at < range_start OR end_at > range_end THEN',
        "IF IFNULL(mode, '') NOT IN ('dryrun', 'apply') THEN",
        'IF calendar_from IS NULL OR calendar_from > opt_from THEN',
        'IF ARRAY_LENGTH(IFNULL(missing_new_year, [])) > 0 THEN',
        'IF other_versions > 0 THEN',
        'IF tick_duplicates > 0 THEN',
        'IF future_duplicates > 0 THEN',
        'CREATE TEMP FUNCTION is_open(',
        'CREATE TEMP TABLE hole_stats AS',
        'IF ARRAY_LENGTH(IFNULL(mixed_holes, [])) > 0 THEN',
        'IF ARRAY_LENGTH(IFNULL(missing_days, [])) > 0 THEN',
        'CREATE TEMP TABLE built AS',
        'CREATE TEMP TABLE pending AS',
        "IF mode = 'apply' THEN",
        'BEGIN TRANSACTION;',
        'COMMIT TRANSACTION;',
    )]
    assert pos == sorted(pos)
    assert "AND STARTS_WITH(ingest_run_id, 'legacy-fill114-') AND parser_version != build_version" in text
    assert text.count('GROUP BY trade_date, security_code, seq_no\n    HAVING COUNT(*) > 1') == 2
    # 区間: 旧系のスナップショットだけで決め、あきが hole_gap_minutes 以上、range_end より前、範囲と交わる
    assert "AND trigger = 'legacy_import'" in text
    assert ('WHERE TIMESTAMP_DIFF(next_at, prev_at, MINUTE) >= hole_gap_minutes AND prev_at < range_end\n'
            '    AND prev_at < end_at AND next_at > start_at') in text
    assert 'IFNULL(a.active_minutes, 0) >= hole_active_minutes AS qualified' in text
    assert 'WHERE h.qualified AND h.loaded AND c >= start_at AND c < end_at' in text


def test_build_only_inserts():
    text = body(SQL['build.sql'])
    assert not re.search(r'\b(UPDATE|DELETE|MERGE)\b', text)
    inserts = re.findall(r'INSERT INTO `\$\{project\}\.\$\{dataset\}\.(\w+)`\n\s+\((.*?)\)\n\s+SELECT(.*?);', text, re.S)
    assert [t for t, _, _ in inserts] == ['option_price', 'future_price', 'ingest_runs']
    for table, cols, rest in inserts:
        cols = {c.strip() for c in cols.split(',')}
        assert cols <= schema(table), (table, cols - schema(table))
        # まだ作っていないスナップショットだけ
        assert 'JOIN pending AS p USING (created_at)' in rest, table
    # 作る行の印（option・future は parser_version と source_object、ingest_runs は trigger も）
    for table, cols, rest in inserts[:2]:
        assert {'ingest_run_id', 'parser_version', 'source_object'} <= {c.strip() for c in cols.split(',')}
        assert "p.ingest_run_id, build_version, 'datacube:tick'" in rest
    # 気配・ギリシャ指標・前日比は書かない（NULL）
    option_cols = {c.strip() for c in inserts[0][1].split(',')}
    assert not option_cols & {'bid', 'ask', 'bid_iv', 'ask_iv', 'delta', 'gamma', 'theta', 'vega', 'diff', 'diff_rate'}
    # pending は ingest_runs に同じ ingest_run_id が無いスナップショット
    assert ("WHERE ingest_run_id NOT IN (\n  SELECT ingest_run_id FROM `${project}.${dataset}.ingest_runs`\n"
            "  WHERE snapshot_at >= start_at AND snapshot_at < end_at AND STARTS_WITH(ingest_run_id, 'legacy-fill114-')") in text


def test_type_mapping_and_series_rules():
    result = block(SQL['build.sql'], 'result').group(1)
    # option_price.type: 1 = コール・2 = プット。DataCube の put_call_type: 1 = プット・2 = コール
    assert 'CASE r.pc WHEN 2 THEN 1 WHEN 1 THEN 2 END AS type' in result
    assert 'black76_iv(o.pc = 2, ' in result
    assert "AND index_type = '18' AND SAFE_CAST(SUBSTR(contract_month, 5, 2) AS INT64) <= 12" in result
    # 祝日取引の日中の約定と夜間のスナップショット（#111 の fill.sql と同じ規則）
    assert "DATE(x) != trade_date AND TIME(x) >= '08:00:00' AND TIME(x) < '16:30:00' AS holiday_day" in result
    assert 'IF(night, st_night_f, st_all_f) AS st' in result
    # 残存: 約定日時の 6 時間前の日付から SQ まで
    assert 'DATE_DIFF(m.sq, DATE(DATETIME_SUB(o.x, INTERVAL 6 HOUR)), DAY) / 365' in result
    # ATM: 同じ距離なら小さい行使価格
    assert 'ORDER BY ABS(r.k - (CAST(f.st.p AS FLOAT64) + IFNULL(r.off, 0))), r.k LIMIT 1' in result


def test_functions_match_tick_fill():
    """取引日・約定日時・夜間の関数は #111 の fill.sql と同じ定義（取引日の数え方を揃える）"""
    fill = block(read(os.path.join(ROOT, 'sql', 'tick_fill', 'fill.sql')), 'functions').group(1)
    build = block(SQL['build.sql'], 'functions').group(1)
    for name in ('is_open', 'trade_day', 'is_night'):
        pattern = rf'^CREATE TEMP FUNCTION {name}\(.*?\n\)\)?;$'
        assert re.search(pattern, fill, re.S | re.M).group(0) == re.search(pattern, build, re.S | re.M).group(0), name
    pattern = r'^CREATE TEMP FUNCTION exec_at\(.*?\n\);$'
    assert re.search(pattern, fill, re.S | re.M).group(0) == re.search(pattern, build, re.S | re.M).group(0)


def test_rollback_deletes_only_marked_rows():
    text = body(SQL['rollback.sql'])
    deletes = re.findall(r'DELETE FROM `\$\{project\}\.\$\{dataset\}\.(\w+)`\n(WHERE .*?);', text, re.S)
    assert [t for t, _ in deletes] == ['option_price', 'future_price', 'ingest_runs']
    for table, where in deletes:
        assert "STARTS_WITH(ingest_run_id, 'legacy-fill114-') AND parser_version = build_version" in where, table
    assert "AND trigger = 'legacy_fill114'" in deletes[2][1]
    assert not re.search(r'\b(UPDATE|INSERT|MERGE)\b', text)
    assert text.index('BEGIN TRANSACTION;') < text.index('DELETE FROM') < text.index('COMMIT TRANSACTION;')


@pytest.mark.parametrize('name', NAMES)
def test_tables_filtered_by_partition(name):
    """表を読む・書く箇所はすべてパーティション列で絞る（require_partition_filter、全件スキャンの防止）"""
    text = body(SQL[name])
    refs = list(re.finditer(r'(INSERT INTO |DELETE FROM )?`\$\{project\}\.\$\{dataset\}\.(\w+)`( AS \w+)?', text))
    assert refs
    for m in refs:
        verb, table, alias = m.groups()
        if table == 'trading_calendar_latest' or verb == 'INSERT INTO ':
            continue  # 休業日の一覧（数百行のビュー）・書き込み先
        prefix = f'{alias.split()[-1]}.' if alias else ''
        following = text[m.end():m.end() + 300]
        if table in ('dc_option_tick', 'dc_future_tick'):
            pattern = rf'WHERE {prefix}trade_date >= (opt_from|hole_from) AND {prefix}trade_date <= (tick_to|hole_to)'
        elif table == 'ingest_runs':
            pattern = rf'WHERE {prefix}snapshot_at >= '
        else:
            assert table in ('option_price', 'future_price'), table
            pattern = rf'WHERE {prefix}created_at >= start_at AND {prefix}created_at < end_at'
            if name in ('verify.sql', 'rollback.sql'):
                pattern += r' AND TIMESTAMP_TRUNC\(created_at, DAY\) IN UNNEST\(built_days\)'
        assert re.search(pattern, following), (name, table, following)


def test_future_schema_matches_csv():
    cols = json.loads(read(FUTURE_SCHEMA))
    assert [(c['name'], c['type']) for c in cols] == FUTURE_COLUMNS
    # 2022-08 以前のファイルは 10 列（最後の列だけ NULL になる。bq load の --allow_jagged_rows）
    assert [c['mode'] for c in cols] == ['REQUIRED'] * 10 + ['NULLABLE']
    assert all(c['description'] for c in cols)


def test_tick_columns_used_exist():
    src = block(block(SQL['build.sql'], 'result').group(1), 'source').group(1)
    option = {c['name'] for c in json.loads(read(os.path.join(ROOT, 'schema', 'tokyo', 'dc_option_tick.json')))}
    future = {c for c, _ in FUTURE_COLUMNS}
    for name, cols in (('opt_tick', option), ('fut_tick', future)):
        used = re.search(r'SELECT (.*?)\n', cte(src, name)).group(1)
        assert {c.strip() for c in used.split(',')} <= cols, name


def test_verify_checks_listed():
    """verify.sql の 0 になる項目が先頭のコメントと結果の列で同じ"""
    text = SQL['verify.sql']
    zero = ('out_of_hole_runs', 'run_field_errors', 'run_count_errors', 'orphan_rows', 'row_field_errors', 'iv_range_rows',
            'future_trade_rows', 'duplicate_rows', 'atm_errors', 'month_errors')
    head = text[:text.index('DECLARE')]
    result = block(text, 'result').group(1)
    for k in zero:
        assert f'--   {k}' in head, k
        assert re.search(rf'\b{k}\b', result.split('\nSELECT\n')[-1]), k


def test_no_datacube_values_in_repo_files():
    """SQL・docs に DataCube の値（約定値・行使価格の具体値）を書かない: 4 桁以上の数字は日付・時刻・定数だけ"""
    for name in NAMES:
        for n in re.findall(r'(?<![\w.:-])\d{4,}(?![\w:-])', body(SQL[name])):
            assert n in {'1000', '2004', '2018', '2022', '2024'}, (name, n)

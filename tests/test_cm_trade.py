"""sql/cm_trade/（30 日固定の ATM IV の約定 IV からの値、#158）の静的検査。

- 参照する表はすべて `${project}.${dataset}.<表>`（名前は runbook の render が入れる）で、プロジェクト・データセットを直書きしない
- 作り方（-- trade:begin〜end）は fill.sql と accuracy.sql で一字一句同じ。その中の SQ（cm_sq）と、行使価格ごとの IV から 30 日の
  IV まで（cm_smile〜cm）は sql/cm/atm_cm.sql の cm の本文と同じ CTE。入力は quote（#114 の option の行）・fut（先物）・holiday だけ
- accuracy.sql の #114 と同じ作り方の行（関数と、source 以外の結果）は sql/tick_build/accuracy.sql と同じ。違うのは候補（cand）だけ
- fill.sql が書くのは atm_cm_iv だけ。消すのは範囲の basis = 'trade' の行と、結果と同じ時刻の basis が NULL で iv30 が NULL の行だけ。
  足す行は basis = 'trade'、範囲の atm_cm_iv に残った行と同じ時刻は足さない。rollback.sql は範囲の basis = 'trade' だけを消す。
  verify.sql と accuracy.sql は書かない（accuracy.sql はスクリプトの一時テーブルだけ）
- 表・生データは範囲で絞る（option_price_latest・future_price_latest は範囲、atm_cm_iv は created_at、ingest_runs は snapshot_at、
  歩み値は trade_date）
- 範囲（range_start・range_end）は sql/tick_build/build.sql と同じ。約定の窓（trade_window_minutes）は fill.sql と accuracy.sql で同じ
BigQuery での実行の確認は tests/test_cm_trade_bq.py（OPTIONCHAN_BQ_TEST=1）。
"""
import json
import os
import re

import pytest

ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
SQL_DIR = os.path.join(ROOT, 'sql', 'cm_trade')
NAMES = ('accuracy.sql', 'fill.sql', 'rollback.sql', 'verify.sql')
CM = os.path.join(ROOT, 'sql', 'cm', 'atm_cm.sql')
TICK_ACCURACY = os.path.join(ROOT, 'sql', 'tick_build', 'accuracy.sql')
TICK_BUILD = os.path.join(ROOT, 'sql', 'tick_build', 'build.sql')
SCHEMA = os.path.join(ROOT, 'schema', 'cm', 'atm_cm_iv.json')
RANGE = ("DECLARE range_start TIMESTAMP DEFAULT TIMESTAMP '2018-09-30 15:00:00+00';\n"
         "DECLARE range_end TIMESTAMP DEFAULT TIMESTAMP '2024-08-06 03:25:00+00';")
WINDOW = 'DECLARE trade_window_minutes INT64 DEFAULT 120;'
TABLE = '`${project}.${dataset}.atm_cm_iv`'


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
    """CTE 1 つ（最後の CTE は閉じかっこの後にカンマが無い）"""
    m = re.search(rf'^{name} AS \(\n.*?^\),?$', text, re.S | re.M)
    assert m, name
    return m.group(0)


SQL = {name: read(os.path.join(SQL_DIR, name)) for name in NAMES}
TRADE = block(SQL['fill.sql'], 'trade').group(1)


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


def test_trade_identical():
    """作り方は fill.sql と accuracy.sql で同じ（合い方を測った作り方と、書き込む作り方が同じ）"""
    assert block(SQL['accuracy.sql'], 'trade').group(1) == TRADE
    for name in ('fill.sql', 'accuracy.sql'):
        assert body(SQL[name]).count(WINDOW) == 1, name
    # 表を直接読まない。入力は quote・fut・holiday の 3 つの CTE だけ
    assert '`' not in TRADE
    defined = set(re.findall(r'^(\w+) AS \(', TRADE, re.M))
    assert set(re.findall(r'\b(?:FROM|JOIN)\s+(\w{2,})', TRADE)) - defined == {'quote', 'fut', 'holiday'}


def test_trade_reuses_cm_body():
    """満期・30 日の補間の部分は定期クエリの cm の本文と同じ CTE（cm_sq と、cm_smile から cm まで）。cm の本文の気配の部分
    （cm_quote〜cm_day）は使わない"""
    cm = block(read(CM), 'cm').group(1)
    assert cte(TRADE, 'cm_sq') == cte(cm, 'cm_sq')
    tail = TRADE[TRADE.index('cm_smile AS (\n'):]
    assert cm[cm.index('cm_smile AS (\n'):] == tail
    names = re.findall(r'^(\w+) AS \(', TRADE, re.M)
    assert names == ['tr_strike', 'tr_atm', 'tr_near', 'cm_iv', 'cm_sq', 'cm_smile', 'cm_bracket', 'cm_month', 'cm_rank',
                     'cm_pair', 'cm_var', 'cm']
    # cm_iv の列は cm の本文の cm_iv と同じ（cm_smile が読む）
    assert 'SELECT n.created_at, n.last_trading_day, CAST(f.price AS FLOAT64) AS fwd, n.k, n.call_iv, n.put_iv' in TRADE
    assert 'FROM tr_near AS n JOIN fut AS f USING (created_at)\n  WHERE n.rn <= 9' in TRADE
    # 約定 IV: iv > 0 で、スナップショットの時刻（JST）の trade_window_minutes 分前以降。ATM は is_atm（2 つなら小さい方）
    assert "DATETIME_SUB(DATETIME(created_at, 'Asia/Tokyo'), INTERVAL trade_window_minutes MINUTE) AS recent" in TRADE
    assert 'MAX(IF(type = 1 AND iv > 0 AND price_time >= recent, iv, NULL)) AS call_iv' in TRADE
    assert 'MAX(IF(type = 2 AND iv > 0 AND price_time >= recent, iv, NULL)) AS put_iv' in TRADE
    assert 'MIN(IF(atm, k, NULL)) AS k0' in TRADE
    assert 'ORDER BY ABS(k - a.k0), k) AS rn' in TRADE
    for name in ('cm_quote', 'cm_strike', 'cm_atm', 'cm_parity', 'cm_fwd', 'cm_day', 'black76_iv'):
        assert name not in TRADE, name


def test_range_matches_tick_build():
    assert RANGE in read(TICK_BUILD)
    assert body(SQL['fill.sql']).count(RANGE) == 1


def test_fill_guards_and_order():
    text = body(SQL['fill.sql'])
    pos = [text.index(s) for s in (
        'IF start_at IS NULL OR end_at IS NULL OR start_at >= end_at\n'
        '  OR TIMESTAMP_TRUNC(start_at, MINUTE) != start_at OR TIMESTAMP_TRUNC(end_at, MINUTE) != end_at THEN',
        'IF start_at < range_start OR end_at > range_end THEN',
        "IF IFNULL(mode, '') NOT IN ('dryrun', 'apply') THEN",
        'CREATE TEMP TABLE result AS',
        'CREATE TEMP TABLE stored AS',
        "IF mode = 'apply' THEN",
        'BEGIN TRANSACTION;',
        'DELETE FROM',
        'INSERT INTO',
        'COMMIT TRANSACTION;',
    )]
    assert pos == sorted(pos)
    # 結果は cm の 11 列 + 印
    assert "-- trade:end\nSELECT *, 'trade' AS basis FROM cm\n-- result:end" in SQL['fill.sql']


def test_fill_source_is_tick_build_rows():
    """入力は #114 の行（ingest_run_id の接頭辞）の最新版で、範囲で絞る"""
    src = block(block(SQL['fill.sql'], 'result').group(1), 'source').group(1)
    for name, table, cols in (('quote', 'option_price_latest', 'created_at, last_trading_day, type, target_price, is_atm, price_time, iv'),
                              ('fut', 'future_price_latest', 'created_at, price')):
        text = cte(src, name)
        assert f'  SELECT {cols}\n  FROM `${{project}}.${{dataset}}.{table}`(start_at, end_at)\n' \
               "  WHERE STARTS_WITH(ingest_run_id, 'legacy-fill114-')\n" in text, name
    holiday = cte(src, 'holiday')
    assert 'is_trading_day' not in holiday
    assert ("WHERE calendar_date >= DATE_SUB(DATE(start_at, 'Asia/Tokyo'), INTERVAL 1 DAY)\n"
            "    AND calendar_date < DATE_ADD(DATE(end_at, 'Asia/Tokyo'), INTERVAL 120 DAY)") in holiday


def schema_names():
    return [c['name'] for c in json.loads(read(SCHEMA))]


def test_fill_writes_only_trade_rows():
    text = body(SQL['fill.sql'])
    writes = re.findall(r'\b(?:INSERT INTO|MERGE|DELETE FROM|UPDATE|TRUNCATE TABLE|CREATE OR REPLACE TABLE|DROP)\s+`([^`]+)`', text)
    assert writes == ['${project}.${dataset}.atm_cm_iv'] * 2
    assert not re.search(r'\b(UPDATE|MERGE)\b', text)
    # 消すのは範囲の印の行と、結果と同じ時刻の値の無い気配の行だけ（見込みの数え方と同じ条件）
    cond = "(basis = 'trade' OR (basis IS NULL AND iv30 IS NULL AND created_at IN (SELECT created_at FROM result)))"
    assert (f'DELETE FROM {TABLE}\n  WHERE created_at >= start_at AND created_at < end_at\n    AND {cond};') in text
    assert f'WHERE {cond[1:-1]}\n);' in text
    # 足すのは結果（印は 'trade'）のうち、範囲の atm_cm_iv に残った行と同じ時刻でないもの。列は表の 12 列
    m = re.search(rf'INSERT INTO {re.escape(TABLE)}\n\s+\(([^()]*)\)\n  SELECT \* FROM result\n'
                  rf'  WHERE created_at NOT IN \(\n    SELECT created_at FROM {re.escape(TABLE)}\n'
                  r'    WHERE created_at >= start_at AND created_at < end_at\n  \);', text)
    assert m
    assert [c.strip() for c in m.group(1).split(',')] == schema_names()
    assert "SELECT created_at FROM stored WHERE basis IS NULL AND iv30 IS NOT NULL" in text


def test_rollback_deletes_only_trade_rows():
    text = body(SQL['rollback.sql'])
    assert re.findall(r'\b(?:INSERT INTO|MERGE|DELETE FROM|UPDATE)\s+`([^`]+)`', text) == ['${project}.${dataset}.atm_cm_iv']
    assert (f"DELETE FROM {TABLE}\nWHERE created_at >= start_at AND created_at < end_at AND basis = 'trade';") in text


@pytest.mark.parametrize('name', ('verify.sql', 'accuracy.sql'))
def test_read_only(name):
    text = body(SQL[name])
    assert not re.search(r'\b(INSERT|UPDATE|DELETE|MERGE|DROP|TRUNCATE|ALTER|EXECUTE)\b', text)
    # 作るのはスクリプトの一時テーブル・一時関数だけ
    for create in re.findall(r'\bCREATE\b[^\n]*', text):
        assert create.startswith(('CREATE TEMP TABLE', 'CREATE TEMP FUNCTION')), create


def without_cand(text):
    result = block(text, 'result').group(1)
    src = block(result, 'source')
    return result[:src.start()] + result[src.end():]


def test_accuracy_builds_like_tick_build():
    """#114 と同じ作り方の行（一致率を測った sql/tick_build/accuracy.sql と同じ）。候補は 5 分ごとの JST 9:00〜15:00"""
    tick, acc = read(TICK_ACCURACY), SQL['accuracy.sql']
    assert block(tick, 'functions').group(1) == block(acc, 'functions').group(1)
    assert without_cand(tick) == without_cand(acc)
    src = [block(block(t, 'result').group(1), 'source').group(1) for t in (tick, acc)]
    for name in ('opt_tick', 'fut_tick'):
        assert cte(src[0], name) == cte(src[1], name), name
    assert "WHERE MOD(EXTRACT(MINUTE FROM c), 5) = 0 AND TIME(c, 'Asia/Tokyo') BETWEEN '09:00:00' AND '15:00:00'" in \
        cte(src[1], 'cand')
    for decl in ("DECLARE tick_from DATE DEFAULT DATE(start_at, 'Asia/Tokyo');",
                 "DECLARE tick_to DATE DEFAULT DATE_ADD(DATE(end_at, 'Asia/Tokyo'), INTERVAL 14 DAY);",
                 'DECLARE opt_from DATE DEFAULT DATE_SUB(tick_from, INTERVAL 10 DAY);',
                 'DECLARE every_minute BOOL DEFAULT TRUE;'):
        assert decl in tick and decl in acc, decl
    # 作った行を fill.sql の入力の形にする（option は同じ列、先物は値段）
    assert ("quote AS (\n  SELECT created_at, last_trading_day, type, target_price, is_atm, price_time, iv\n  FROM built\n"
            "  WHERE tbl = 'option'\n)") in acc
    assert "fut AS (\n  SELECT created_at, price\n  FROM built\n  WHERE tbl = 'future'\n)" in acc
    # 比べるのは気配からの行だけ
    assert 'WHERE created_at >= start_at AND created_at < end_at AND basis IS NULL' in acc


def test_verify_checks_listed():
    """verify.sql の 0 になる項目が先頭のコメントと結果の列で同じ"""
    text = SQL['verify.sql']
    head = text[:text.index('DECLARE')]
    result = text[text.rindex('\nSELECT\n'):]
    for k in ('duplicate_rows', 'basis_errors', 'outside_rows', 'missing_snapshots', 'iv_range_rows', 'method_errors',
              'forward_errors'):
        assert f'--   {k}' in head, k
        assert re.search(rf'\bAS {k}\b', result), k
    assert "trigger = 'legacy_fill114'" in text


@pytest.mark.parametrize('name', NAMES)
def test_tables_filtered_by_range(name):
    """表を読む・書く箇所はすべて範囲で絞る（パーティションの列か、最新版の関数の引数）"""
    text = body(SQL[name])
    refs = list(re.finditer(r'(INSERT INTO |DELETE FROM )?`\$\{project\}\.\$\{dataset\}\.(\w+)`(\([^()]*\))?', text))
    assert refs
    for m in refs:
        verb, table, args = m.groups()
        following = text[m.end():m.end() + 300]
        if table == 'trading_calendar_latest' or verb == 'INSERT INTO ':
            continue  # 休業日の一覧（数百行のビュー）・書き込み先（INSERT の SELECT は下の atm_cm_iv で見る）
        if table in ('option_price_latest', 'future_price_latest'):
            assert args == '(start_at, end_at)', (name, table)
        elif table == 'atm_cm_iv':
            assert re.match(r'\s+WHERE created_at >= start_at AND created_at < end_at', following), (name, following)
        elif table == 'ingest_runs':
            assert re.match(r'\s+WHERE snapshot_at >= start_at AND snapshot_at < end_at', following), (name, following)
        else:
            assert table in ('dc_option_tick', 'dc_future_tick'), (name, table)
            assert re.match(r'\s+WHERE trade_date >= opt_from AND trade_date <= tick_to', following), (name, following)


def test_no_datacube_values_in_repo_files():
    """SQL に DataCube の値（約定値・行使価格の具体値）を書かない: 4 桁以上の数字は日付・時刻・定数（1 日の秒数）だけ"""
    for name in NAMES:
        for n in re.findall(r'(?<![\w.:-])\d{4,}(?![\w:-])', body(SQL[name])):
            assert n in {'1000', '2004', '2018', '2022', '2024', '86400'}, (name, n)

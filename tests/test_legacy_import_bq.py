"""過去データの移行（#12）の SQL を BigQuery で実際に動かす検査（tests/test_sql_bq.py と同じ流儀）。

CI では動かさない（BigQuery の認証が要る）。手元で `OPTIONCHAN_BQ_TEST=1 python -m pytest -q tests/test_legacy_import_bq.py`。
- 固定値の検査: insert.sql の移す T の選び方（`-- result:begin`〜`-- result:end`）と verify.sql の検証を、`-- source:begin`〜`-- source:end`
  を固定値の CTE に差し替えて SELECT だけ実行する（テーブルを読まない。課金 0 バイト）
- 実テーブルの dry run: 一時データセット（optionchan_import）はまだ無いので、旧スキーマの列を持つ東京の optionchan_legacy_copy で代える
- 書き込みの試験: insert.sql の DELETE・INSERT を、本番の表の代わりにスクリプトの一時テーブルに対して 2 回流し、verify.sql で確かめる
  （本番の表には書かない）。DELETE・INSERT の各文は本番の表に対して 1 文ずつ dry run する（期間の絞り込み）
"""
import os
import re

import pytest

from test_sql_bq import FUTURE, GiB, MiB, OPTION, PROJECT, SPOT, TOKYO, between, bq, cte, literal, read, render

pytestmark = pytest.mark.skipif(os.environ.get('OPTIONCHAN_BQ_TEST') != '1', reason='OPTIONCHAN_BQ_TEST=1 のときだけ')

ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
SQL_DIR = os.path.join(ROOT, 'sql', 'legacy_import')
FINGERPRINT = os.path.join(SQL_DIR, 'fingerprint.sql')
INSERT = os.path.join(SQL_DIR, 'insert.sql')
VERIFY = os.path.join(SQL_DIR, 'verify.sql')
NAMES = {'dataset': 'optionchan_tokyo', 'import_dataset': 'optionchan_legacy_copy'}

# 範囲: 2026-10-12 00:00 UTC 〜 2026-10-14 00:00 UTC（新系は 10-12 23:39 UTC から）
START = '2026-10-12 00:00:00+00'
END = '2026-10-14 00:00:00+00'


def params(start=START, end=END):
    return [f'start_at:TIMESTAMP:{start}', f'end_at:TIMESTAMP:{end}']


def result_query(path, sources):
    """結果の SELECT（source を固定値に差し替え）を、範囲の変数を宣言したスクリプトにする"""
    text = between(read(path), '-- result:begin', '-- result:end').group(1)
    m = between(text, '-- source:begin', '-- source:end')
    text = render(text[:m.start()] + sources + text[m.end():], project=PROJECT, **NAMES)
    return (
        f"DECLARE start_at TIMESTAMP DEFAULT TIMESTAMP '{START}';\n"
        f"DECLARE end_at TIMESTAMP DEFAULT TIMESTAMP '{END}';\n"
        f'{text}'
    )


def ts(hour, minute, day=12):
    return f'2026-10-{day:02d} {hour:02d}:{minute:02d}:00+00'


def run_id(at):
    """insert.sql と同じ書式（'2026-10-12 06:00:00+00' → 'legacy-20261012T060000Z'）"""
    d, t = at[:19].split(' ')
    return f"legacy-{d.replace('-', '')}T{t.replace(':', '')}Z"


# ---- 移す T の選び方（insert.sql） ----

COUNTS = [('created_at', 'TIMESTAMP'), ('n', 'INT64')]
RECORDED = [('snapshot_at', 'TIMESTAMP')]


def run_targets(spot, future, option, recorded):
    sources = (cte('legacy_spot', COUNTS, spot) + cte('legacy_future', COUNTS, future)
               + cte('legacy_option', COUNTS, option) + cte('recorded', RECORDED, recorded))
    rows = bq(result_query(INSERT, sources) + '\nORDER BY snapshot_at')
    return [(r['ingest_run_id'], r['snapshot_at'][:16], int(r['rows_spot']), int(r['rows_future']), int(r['rows_option']))
            for r in rows]


def test_targets_selects_snapshots_without_runs():
    old, partial, dup = ts(6, 0), ts(6, 1), ts(6, 2)          # 新系の稼働前（前日の夜間）
    new = ts(23, 46)                                          # 並行稼働中、新系の記録あり
    missed = ts(23, 47)                                       # 並行稼働中、新系の取りこぼし
    migrated = ts(5, 59)                                      # 前回の実行で移した T（legacy- の記録あり）
    spot = [{'created_at': at, 'n': 1} for at in (old, dup, new, missed, migrated)]
    spot[1]['n'] = 2                                          # 旧系の重複（同じ created_at で 2 行）
    spot.append({'created_at': partial, 'n': 1})              # 先物が無い（旧系の欠け）
    future = [{'created_at': at, 'n': 1} for at in (old, dup, new, missed, migrated)]
    option = [{'created_at': at, 'n': 4} for at in (old, partial, dup, new, missed, migrated)]
    recorded = [{'snapshot_at': new}, {'snapshot_at': migrated}, {'snapshot_at': ts(23, 39)}]
    got = run_targets(spot, future, option, recorded)
    assert got == [
        (run_id(old), old[:16], 1, 1, 4),
        (run_id(partial), partial[:16], 1, 0, 4),
        (run_id(dup), dup[:16], 2, 1, 4),
        (run_id(missed), missed[:16], 1, 1, 4),
    ]
    assert run_id(old) == 'legacy-20261012T060000Z'


def test_targets_empty_when_all_recorded():
    at = ts(6, 0)
    assert run_targets([{'created_at': at, 'n': 1}], [{'created_at': at, 'n': 1}], [{'created_at': at, 'n': 2}],
                       [{'snapshot_at': at}]) == []


# ---- 移行の検証（verify.sql） ----

RUNS = [('ingest_run_id', 'STRING'), ('snapshot_at', 'TIMESTAMP'), ('rows_spot', 'INT64'), ('rows_future', 'INT64'),
        ('rows_option', 'INT64')]
TOKYO_SPOT = SPOT + [('ingest_run_id', 'STRING')]
TOKYO_FUTURE = FUTURE + [('ingest_run_id', 'STRING')]
TOKYO_OPTION = OPTION + [('ingest_run_id', 'STRING')]
CHECKS = ('missing_snapshots', 'overlap_snapshots', 'unknown_legacy_runs', 'bad_run_ids', 'row_mismatches', 'orphan_rows',
          'run_count_mismatches')


def spot_row(at, i=0):
    return {'price': 100.0 + i, 'price_time': '2026-10-12 15:00:00', 'diff': 1.0, 'diff_rate': 0.1, 'hv': 20.0,
            'created_at': at}


def future_row(at, i=0):
    return {**spot_row(at, i), 'price': 200.0 + i, 'contract_month': '2026-12-01'}


def option_row(at, strike, typ=1):
    return {'type': typ, 'target_price': strike, 'is_atm': strike == 60000, 'price': 10, 'price_time': '2026-10-12 15:00:00',
            'diff': 1, 'diff_rate': 0.5, 'iv': 20.0, 'bid': 1, 'bid_volume': 1, 'bid_iv': 19.0, 'ask': 2, 'ask_volume': 1,
            'ask_iv': 21.0, 'volume': 10, 'positions': 100, 'quotation': 5, 'quotation_date': '2026-10-09', 'delta': 0.5,
            'gamma': 0.01, 'theta': -1.0, 'vega': 2.0, 'last_trading_day': '2026-11-12', 'created_at': at}


OLD1, OLD2, NEW, MISSED = ts(6, 0), ts(6, 1), ts(23, 46), ts(23, 47)


def verify_clean():
    """移行の後の正常な状態: 稼働前の 2 つと取りこぼし 1 つを移し、新系の T は新系のまま"""
    d = {k: [] for k in ('legacy_spot', 'legacy_future', 'legacy_option', 'runs', 'tokyo_spot', 'tokyo_future',
                         'tokyo_option')}
    for at in (OLD1, OLD2, NEW, MISSED):
        d['legacy_spot'].append(spot_row(at))
        d['legacy_future'].append(future_row(at))
        d['legacy_option'] += [option_row(at, 60000), option_row(at, 61000)]
    d['legacy_spot'].append(spot_row(OLD2, 1))                # 旧系の重複（2 行とも移す）
    for at in (OLD1, OLD2, MISSED):
        rid = run_id(at)
        n_spot = 2 if at == OLD2 else 1
        d['runs'].append({'ingest_run_id': rid, 'snapshot_at': at, 'rows_spot': n_spot, 'rows_future': 1,
                          'rows_option': 2})
        d['tokyo_spot'] += [{**r, 'ingest_run_id': rid} for r in d['legacy_spot'] if r['created_at'] == at]
        d['tokyo_future'].append({**future_row(at), 'ingest_run_id': rid})
        d['tokyo_option'] += [{**option_row(at, 60000), 'ingest_run_id': rid}, {**option_row(at, 61000), 'ingest_run_id': rid}]
    # 新系（uuid の id）。値は旧系と違ってよい
    d['runs'].append({'ingest_run_id': 'u-new', 'snapshot_at': NEW, 'rows_spot': 1, 'rows_future': 1, 'rows_option': 1})
    d['tokyo_spot'].append({**spot_row(NEW, 5), 'ingest_run_id': 'u-new'})
    d['tokyo_future'].append({**future_row(NEW, 5), 'ingest_run_id': 'u-new'})
    d['tokyo_option'].append({**option_row(NEW, 60000), 'ingest_run_id': 'u-new'})
    d['tokyo_option'].append({**option_row(NEW, 62000), 'ingest_run_id': 'u-orphan'})   # 新系の残骸（数えない）
    return d


def run_verify(d):
    sources = ''.join(cte(name, cols, d[name]) for name, cols in (
        ('legacy_spot', SPOT), ('legacy_future', FUTURE), ('legacy_option', OPTION), ('runs', RUNS),
        ('tokyo_spot', TOKYO_SPOT), ('tokyo_future', TOKYO_FUTURE), ('tokyo_option', TOKYO_OPTION)))
    [row] = bq(result_query(VERIFY, sources))
    return row


def test_verify_clean():
    row = run_verify(verify_clean())
    assert {k: int(row[k]) for k in CHECKS} == dict.fromkeys(CHECKS, 0)
    got = {k: int(row[k]) for k in ('import_snapshots', 'legacy_runs', 'new_snapshots', 'migrated_rows_spot',
                                    'migrated_rows_future', 'migrated_rows_option', 'new_rows_spot', 'new_rows_future',
                                    'new_rows_option')}
    assert got == {'import_snapshots': 4, 'legacy_runs': 3, 'new_snapshots': 1, 'migrated_rows_spot': 4,
                   'migrated_rows_future': 3, 'migrated_rows_option': 6, 'new_rows_spot': 1, 'new_rows_future': 1,
                   'new_rows_option': 2}
    assert row['mismatch_samples'] == []


def test_verify_before_migration():
    """移行の前: 稼働前と取りこぼしの T が missing_snapshots に出る（移す予定の数）。他は 0"""
    d = verify_clean()
    d['runs'] = [r for r in d['runs'] if not r['ingest_run_id'].startswith('legacy-')]
    for t in ('tokyo_spot', 'tokyo_future', 'tokyo_option'):
        d[t] = [r for r in d[t] if not r['ingest_run_id'].startswith('legacy-')]
    row = run_verify(d)
    assert {k: int(row[k]) for k in CHECKS} == {**dict.fromkeys(CHECKS, 0), 'missing_snapshots': 3}


def _missing(d):
    d['runs'] = [r for r in d['runs'] if r['snapshot_at'] != MISSED]
    for t in ('tokyo_spot', 'tokyo_future', 'tokyo_option'):
        d[t] = [r for r in d[t] if r['created_at'] != MISSED]


def _overlap(d):
    d['runs'].append({'ingest_run_id': 'u-dup', 'snapshot_at': OLD1, 'rows_spot': 0, 'rows_future': 0, 'rows_option': 0})


def _unknown(d):
    at = ts(7, 0)
    d['runs'].append({'ingest_run_id': run_id(at), 'snapshot_at': at, 'rows_spot': 0, 'rows_future': 0, 'rows_option': 0})


def _bad_id(d):
    d['tokyo_future'][0]['ingest_run_id'] = run_id(OLD2)    # OLD1 の行に OLD2 の id（両方の記録の行数にも出る）


def _value(d):
    d['tokyo_option'][0]['bid'] = 999


def _dup_row(d):
    d['tokyo_spot'].append(dict(d['tokyo_spot'][0]))       # 同じ行が 2 回（XOR は打ち消し合うが行数・合計で出る）


def _orphan(d):
    d['tokyo_spot'].append({**spot_row(MISSED), 'ingest_run_id': run_id(ts(7, 1)), 'created_at': ts(7, 1)})


def _run_count(d):
    d['runs'][0]['rows_option'] = 3


@pytest.mark.parametrize('expected, mutate', [
    ({'missing_snapshots': 1}, _missing),
    ({'overlap_snapshots': 1}, _overlap),
    ({'unknown_legacy_runs': 1, 'run_count_mismatches': 0}, _unknown),
    ({'bad_run_ids': 1, 'run_count_mismatches': 2}, _bad_id),
    ({'row_mismatches': 1}, _value),
    ({'row_mismatches': 1, 'run_count_mismatches': 1}, _dup_row),
    ({'orphan_rows': 1}, _orphan),
    ({'run_count_mismatches': 1}, _run_count),
])
def test_verify_detects(expected, mutate):
    d = verify_clean()
    mutate(d)
    row = run_verify(d)
    assert {k: int(row[k]) for k in CHECKS} == {**dict.fromkeys(CHECKS, 0), **expected}


def test_verify_samples():
    d = verify_clean()
    _value(d)
    row = run_verify(d)
    assert [(s['tbl'], s['created_at'][:16]) for s in row['mismatch_samples']] == [('option_price', OLD1[:16])]


# ---- 実テーブルの dry run（見積もりバイト数。PR に記録する） ----

def test_dry_run_fingerprint_us():
    sql = render(read(FINGERPRINT), project=PROJECT, source_dataset='optionchan')
    n = bq(sql, location='US', dry_run=True, params=params('2026-10-01 00:00:00+00', '2026-10-08 00:00:00+00'))
    print(f'fingerprint US 7 days: {n} bytes')
    assert 0 < n <= 2 * GiB


def test_dry_run_fingerprint_tokyo():
    sql = render(read(FINGERPRINT), project=PROJECT, source_dataset='optionchan_legacy_copy')
    n = bq(sql, dry_run=True, params=params())
    print(f'fingerprint Tokyo (legacy_copy): {n} bytes')
    assert n <= 500 * MiB


def test_dry_run_targets():
    text = between(read(INSERT), '-- result:begin', '-- result:end').group(1)
    sql = (f"DECLARE start_at TIMESTAMP DEFAULT TIMESTAMP '{START}';\n"
           f"DECLARE end_at TIMESTAMP DEFAULT TIMESTAMP '{END}';\n" + render(text, project=PROJECT, **NAMES))
    n = bq(sql, dry_run=True)
    print(f'targets: {n} bytes')
    assert n <= 100 * MiB


def test_dry_run_verify():
    text = between(read(VERIFY), '-- result:begin', '-- result:end').group(1)
    sql = (f"DECLARE start_at TIMESTAMP DEFAULT TIMESTAMP '{START}';\n"
           f"DECLARE end_at TIMESTAMP DEFAULT TIMESTAMP '{END}';\n" + render(text, project=PROJECT, **NAMES))
    n = bq(sql, dry_run=True)
    print(f'verify: {n} bytes')
    assert n <= GiB


@pytest.mark.parametrize('path, location, names', [
    (FINGERPRINT, 'US', {'source_dataset': 'optionchan'}),
    (INSERT, TOKYO, NAMES),
    (VERIFY, TOKYO, NAMES),
])
def test_dry_run_whole_script(path, location, names):
    """スクリプト全体（DECLARE・IF・DELETE・CREATE TEMP TABLE・INSERT）の構文。スクリプトの dry run は構文だけを見る"""
    sql = render(read(path), project=PROJECT, **names)
    # 見積もりは最初の文のもの（insert は quality_daily の DECLARE、verify は結果の SELECT）なので、通ることだけを見る
    assert bq(sql, location=location, dry_run=True, params=params()) >= 0


# ---- insert.sql の書き込み（DELETE・INSERT）を一時テーブルで実行する ----
# 本番の表の代わりにスクリプトの一時テーブル（セッションの匿名データセット。スクリプトの終わりに消える）を作り、
# insert.sql を 2 回（2 回目は何も入らない）→ verify.sql の順に同じスクリプトで流す。参照の
# `${project}.${dataset}.<表>` を `<表>`、`${project}.${import_dataset}.<表>` を `imp_<表>` に置き換える。

import json as _json
import subprocess as _subprocess

TYPES = {'INTEGER': 'INT64', 'FLOAT': 'FLOAT64', 'BOOLEAN': 'BOOL'}


def schema_cols(path):
    return [(c['name'], TYPES.get(c['type'], c['type'])) for c in _json.loads(read(os.path.join(ROOT, path)))]


def temp_table(name, cols, rows, partition=None):
    # 一時テーブルには OPTIONS（require_partition_filter）を付けられない。期間の絞り込みは test_dry_run_dml_statements で本番の表に対して見る
    struct = ', '.join(f'{c} {t}' for c, t in cols)
    values = ', '.join('STRUCT(' + ', '.join(literal(r.get(c), t) for c, t in cols) + ')' for r in rows)
    opts = f'\nPARTITION BY DATE({partition})' if partition else ''
    return f'CREATE TEMP TABLE {name}{opts} AS SELECT * FROM UNNEST(ARRAY<STRUCT<{struct}>>[{values}]);\n'


def to_temp(text):
    """本番の表への参照を一時テーブルに、DECLARE ... DEFAULT を SET にする（DECLARE はスクリプトの先頭にまとめる）"""
    text = re.sub(r'`\$\{project\}\.\$\{import_dataset\}\.(\w+)`', r'imp_\1', text)
    text = re.sub(r'`\$\{project\}\.\$\{dataset\}\.(\w+)`', r'\1', text)
    declares = re.findall(r'^DECLARE (\w+) (\w+) DEFAULT (.*);$', text, re.M)
    text = re.sub(r'^DECLARE (\w+) (\w+) DEFAULT (.*);$', r'SET \1 = \3;', text, flags=re.M)
    return text, [(n, t) for n, t, _ in declares]


def run_insert_flow(setup, start, end):
    insert, d1 = to_temp(read(INSERT))
    verify, d2 = to_temp(read(VERIFY))
    declares = ''.join(f'DECLARE {n} {t};\n' for n, t in dict(d1 + d2).items())
    script = declares + setup + insert + '\nDROP TABLE targets;\n' + insert + '\n' + verify
    cmd = ['bq', f'--project_id={PROJECT}', f'--location={TOKYO}', 'query', '--use_legacy_sql=false', '--format=json',
           '--nouse_cache', '--maximum_bytes_billed=2147483648', *(f'--parameter={p}' for p in params(start, end))]
    out = _subprocess.run(cmd, input=script, capture_output=True, text=True, timeout=600)
    assert out.returncode == 0, '\n'.join(
        line for line in (out.stdout + out.stderr).splitlines() if 'error' in line.lower() or 'at [' in line)
    results = [r for r in _json.loads(out.stdout[out.stdout.index('['):]) if isinstance(r, list)]
    return results


def test_insert_writes_idempotently_and_verifies():
    """残骸の削除 → 移す T の追記 → 完了記録。2 回目は 0 件。新系の行（記録のある T・新系の残骸）には触れない"""
    old1, old2, partial, new, missed = ts(6, 0), ts(6, 1), ts(6, 2), ts(23, 46), ts(23, 47)
    outside = ts(0, 0, day=14)                                   # 範囲の外（移さない）
    end = ts(22, 0, day=13)
    legacy_spot = [spot_row(at) for at in (old1, old2, new, missed, outside)] + [spot_row(old2, 1)]
    legacy_future = [future_row(at) for at in (old1, old2, new, missed, outside)]
    legacy_option = [option_row(at, k) for at in (old1, old2, partial, new, missed, outside) for k in (60000, 61000)]
    tokyo = {t: schema_cols(f'schema/tokyo/{t}.json') for t in ('spot_price', 'future_price', 'option_price', 'ingest_runs')}
    tokyo_spot = [{**spot_row(new, 5), 'ingest_run_id': 'u-new'},
                  {**spot_row(missed), 'ingest_run_id': run_id(missed)}]      # 前回の途中失敗の残骸（記録なし）
    tokyo_future = [{**future_row(new, 5), 'ingest_run_id': 'u-new'}]
    tokyo_option = [{**option_row(new, 60000), 'ingest_run_id': 'u-new'},
                    {**option_row(new, 62000), 'ingest_run_id': 'u-orphan'}]  # 新系の残骸（消さない）
    runs = [{'ingest_run_id': 'u-new', 'snapshot_at': new, 'source_object': 'gs://b/raw#1', 'parser_version': 'ingest-1',
             'trigger': 'scheduler', 'rows_spot': 1, 'rows_future': 1, 'rows_option': 1, 'started_at': new,
             'finished_at': ts(0, 1, day=13)}]
    setup = (
        temp_table('quality_daily', [('window_end', 'TIMESTAMP')], [{'window_end': end}])
        + temp_table('imp_spot_price', SPOT, legacy_spot)
        + temp_table('imp_future_price', FUTURE, legacy_future)
        + temp_table('imp_option_price', OPTION, legacy_option)
        + temp_table('spot_price', tokyo['spot_price'], tokyo_spot, 'created_at')
        + temp_table('future_price', tokyo['future_price'], tokyo_future, 'created_at')
        + temp_table('option_price', tokyo['option_price'], tokyo_option, 'created_at')
        + temp_table('ingest_runs', tokyo['ingest_runs'], runs, 'snapshot_at')
    )
    first, second, verify = run_insert_flow(setup, START, end)
    assert {k: int(first[0][k]) for k in ('snapshots', 'rows_spot', 'rows_future', 'rows_option')} == {
        'snapshots': 4, 'rows_spot': 4, 'rows_future': 3, 'rows_option': 8}
    assert int(second[0]['snapshots']) == 0
    [row] = verify
    assert {k: int(row[k]) for k in CHECKS} == dict.fromkeys(CHECKS, 0)
    got = {k: int(row[k]) for k in ('import_snapshots', 'legacy_runs', 'new_snapshots', 'migrated_rows_spot',
                                    'migrated_rows_future', 'migrated_rows_option', 'new_rows_spot', 'new_rows_option')}
    assert got == {'import_snapshots': 5, 'legacy_runs': 4, 'new_snapshots': 1, 'migrated_rows_spot': 4,
                   'migrated_rows_future': 3, 'migrated_rows_option': 8, 'new_rows_spot': 1, 'new_rows_option': 2}


def test_dry_run_dml_statements():
    """insert.sql の DELETE・INSERT を 1 文ずつ本番の表に対して dry run する（require_partition_filter の表で期間の絞り込みが効くこと。
    スクリプト全体の dry run は構文しか見ない）。targets は一時テーブルなので、その SELECT（結果の部分）で置き換える"""
    text = render(read(INSERT), project=PROJECT, **NAMES)
    targets = '(' + between(text, '-- result:begin', '-- result:end').group(1) + ')'
    stmts = re.findall(r'^((?:DELETE FROM|INSERT INTO) .*?;)$', text, re.S | re.M)
    assert len(stmts) == 7
    for sql in stmts:
        sql = re.sub(r'\bJOIN targets AS t\b', f'JOIN {targets} AS t', sql)
        sql = re.sub(r'\bFROM targets;', f'FROM {targets};', sql)
        sql = re.sub(r'\b(start_at|end_at)\b', r'@\1', sql)
        assert bq(sql, dry_run=True, params=params()) >= 0, sql[:80]

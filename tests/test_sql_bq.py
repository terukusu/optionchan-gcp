"""日次の品質チェック・新旧の突き合わせ（#40）の SQL を BigQuery で実際に動かす検査。

CI では動かさない（BigQuery の認証が要る）。手元で `OPTIONCHAN_BQ_TEST=1 python -m pytest -q tests/test_sql_bq.py`。
- 固定値の検査: SQL の `-- result:begin`〜`-- result:end`（結果 1 行の SELECT）を取り出し、`-- source:begin`〜`-- source:end`
  （テーブルを読む CTE）を固定値の CTE に差し替えて SELECT だけを実行する（テーブルを読まない。課金 0 バイト）
- 実テーブルの dry run: 差し替えずに `--dry_run` し、見積もりバイト数が上限以下であること（突き合わせはコピーのデータセットの代わりに
  新系のデータセットを使う。旧スキーマの列は新系にもある）
gcloud / bq の構成（アカウント・プロジェクト）は環境変数（CLOUDSDK_ACTIVE_CONFIG_NAME 等）で渡す。
"""
import json
import os
import re
import subprocess

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('OPTIONCHAN_BQ_TEST') != '1', reason='OPTIONCHAN_BQ_TEST=1 のときだけ')

ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
PROJECT = os.environ.get('OPTIONCHAN_BQ_PROJECT', 'optionchan-222710')
TOKYO = 'asia-northeast1'
QUALITY = os.path.join(ROOT, 'sql', 'quality', 'quality_daily.sql')
STAGE = os.path.join(ROOT, 'sql', 'legacy', 'stage.sql')
COMPARE = os.path.join(ROOT, 'sql', 'legacy', 'compare.sql')

# 窓: 2026-10-13 07:00 JST 〜 2026-10-14 07:00 JST
WIN_START = '2026-10-12 22:00:00+00'
WIN_END = '2026-10-13 22:00:00+00'
GiB = 1024 ** 3
MiB = 1024 ** 2


def read(path):
    with open(path, encoding='utf-8') as f:
        return f.read()


def render(text, **names):
    return re.sub(r'\$\{(\w+)\}', lambda m: names[m.group(1)], text)


def between(text, begin, end):
    m = re.search(rf'^{re.escape(begin)}\n(.*?)^{re.escape(end)}$', text, re.S | re.M)
    assert m, (begin, end)
    return m


def result_query(path, sources=None, **names):
    """結果 1 行の SELECT を、窓の変数を宣言したスクリプトにする。sources を渡すと source の CTE を差し替える"""
    body = between(read(path), '-- result:begin', '-- result:end').group(1)
    if sources is not None:
        m = between(body, '-- source:begin', '-- source:end')
        body = body[:m.start()] + sources + body[m.end():]
    body = render(body, project=PROJECT, **names)
    return (
        f"DECLARE win_start TIMESTAMP DEFAULT TIMESTAMP '{WIN_START}';\n"
        f"DECLARE win_end TIMESTAMP DEFAULT TIMESTAMP '{WIN_END}';\n"
        'DECLARE fail_on_unexplained BOOL DEFAULT FALSE;\n'
        f'{body}'
    )


def bq(sql, location=TOKYO, dry_run=False, params=()):
    cmd = ['bq', f'--project_id={PROJECT}', f'--location={location}', 'query', '--use_legacy_sql=false',
           '--format=json', '--nouse_cache', '--maximum_bytes_billed=10485760', *(f'--parameter={p}' for p in params)]
    if dry_run:
        cmd.append('--dry_run')
    out = subprocess.run(cmd, input=sql, capture_output=True, text=True, timeout=300)
    # 失敗時は BigQuery のエラーの行だけを出す（ジョブの JSON にはアカウントのメールアドレスが入るので出さない）
    assert out.returncode == 0, '\n'.join(
        line for line in (out.stdout + out.stderr).splitlines() if 'error' in line.lower() or 'at [' in line)
    if dry_run:
        return int(json.loads(out.stdout)['statistics']['totalBytesProcessed'])
    # スクリプト（DECLARE + SELECT）の結果は [[行, ...]]（最後の文の結果）
    [rows] = json.loads(out.stdout[out.stdout.index('['):])
    return rows


# ---- 固定値の CTE ----

def literal(value, typ):
    if value is None:
        return f'CAST(NULL AS {typ})'
    if typ in ('TIMESTAMP', 'DATETIME', 'DATE'):
        return f"{typ} '{value}'"
    if typ == 'STRING':
        return "'" + str(value).replace("'", "\\'") + "'"
    if typ == 'BOOL':
        return 'TRUE' if value else 'FALSE'
    return repr(value)


def cte(name, columns, rows):
    struct = ', '.join(f'{c} {t}' for c, t in columns)
    values = ', '.join('STRUCT(' + ', '.join(literal(r.get(c), t) for c, t in columns) + ')' for r in rows)
    return f'{name} AS (SELECT * FROM UNNEST(ARRAY<STRUCT<{struct}>>[{values}])),\n'


def ts(minute, hour=0):
    """2026-10-13 hh:mm UTC（JST 09:mm）"""
    return f'2026-10-13 {hour:02d}:{minute:02d}:00+00'


def jst(minute, hour=9, day=13):
    return f'2026-10-{day:02d} {hour:02d}:{minute:02d}:00'


# ---- 品質チェック ----

Q_RUNS = [('ingest_run_id', 'STRING'), ('rows_spot', 'INT64'), ('rows_future', 'INT64'), ('rows_option', 'INT64')]
Q_LATEST = [('snapshot_at', 'TIMESTAMP')]
Q_PRICE = [('created_at', 'TIMESTAMP'), ('page_updated_at', 'TIMESTAMP'), ('price_time', 'DATETIME')]
Q_OPTION = Q_PRICE + [('type', 'INT64'), ('target_price', 'INT64'), ('last_trading_day', 'DATE'), ('is_atm', 'BOOL')]
Q_RAW = [('tbl', 'STRING'), ('ingest_run_id', 'STRING'), ('n', 'INT64')]
MONTHS = ('2026-11-12', '2026-12-10')


def quality_clean():
    data = {k: [] for k in ('runs', 'latest_runs', 'spot_l', 'future_l', 'option_l', 'raw_counts')}
    for i in range(2):
        run, at = f'r{i}', ts(i)
        data['runs'].append({'ingest_run_id': run, 'rows_spot': 1, 'rows_future': 1, 'rows_option': 4})
        data['latest_runs'].append({'snapshot_at': at})
        base = {'created_at': at, 'page_updated_at': at, 'price_time': jst(i)}
        data['spot_l'].append(dict(base))
        data['future_l'].append(dict(base))
        for month in MONTHS:
            for typ in (1, 2):
                data['option_l'].append({**base, 'type': typ, 'target_price': 60000, 'last_trading_day': month,
                                         'is_atm': True})
        data['raw_counts'] += [{'tbl': 'spot', 'ingest_run_id': run, 'n': 1},
                               {'tbl': 'future', 'ingest_run_id': run, 'n': 1},
                               {'tbl': 'option', 'ingest_run_id': run, 'n': 4}]
    return data


def run_quality(data):
    sources = (cte('runs', Q_RUNS, data['runs']) + cte('latest_runs', Q_LATEST, data['latest_runs'])
               + cte('spot_l', Q_PRICE, data['spot_l']) + cte('future_l', Q_PRICE, data['future_l'])
               + cte('option_l', Q_OPTION, data['option_l']) + cte('raw_counts', Q_RAW, data['raw_counts']))
    [row] = bq(result_query(QUALITY, sources, dataset='optionchan_tokyo'))
    return row


ITEMS = ('rowcount_mismatch', 'missing_table', 'dup_keys', 'months_not_2', 'atm_not_1',
         'price_time_future', 'price_time_too_old', 'sunday')


def test_quality_clean():
    row = run_quality(quality_clean())
    assert row['window_date'] == '2026-10-13'
    assert (int(row['runs']), int(row['snapshots']), int(row['option_rows'])) == (2, 2, 8)
    assert {k: int(row[k]) for k in ITEMS + ('orphan_rows', 'violations')} == dict.fromkeys(ITEMS + ('orphan_rows', 'violations'), 0)


def _rowcount(d):
    d['raw_counts'][2]['n'] = 3


def _missing(d):
    d['future_l'].pop()


def _dup(d):
    d['spot_l'].append(dict(d['spot_l'][0]))


def _months(d):
    d['option_l'] = [r for r in d['option_l'] if not (r['created_at'] == ts(1) and r['last_trading_day'] == MONTHS[1])]


def _atm(d):
    d['option_l'][0]['is_atm'] = False


def _future(d):
    d['spot_l'][0]['price_time'] = jst(1)


def _too_old(d):
    d['option_l'][0]['price_time'] = '2026-09-01 10:00:00'


def _sunday(d):
    d['option_l'][0]['price_time'] = '2026-10-11 10:00:00'


@pytest.mark.parametrize('item, mutate', [
    ('rowcount_mismatch', _rowcount), ('missing_table', _missing), ('dup_keys', _dup), ('months_not_2', _months),
    ('atm_not_1', _atm), ('price_time_future', _future), ('price_time_too_old', _too_old), ('sunday', _sunday),
])
def test_quality_detects_each_item(item, mutate):
    data = quality_clean()
    mutate(data)
    row = run_quality(data)
    got = {k: int(row[k]) for k in ITEMS}
    assert got == {k: int(k == item) for k in ITEMS}
    assert int(row['violations']) == 1


def test_quality_orphan_rows_are_not_violations():
    data = quality_clean()
    data['raw_counts'].append({'tbl': 'option', 'ingest_run_id': 'not-in-runs', 'n': 5})
    row = run_quality(data)
    assert (int(row['orphan_rows']), int(row['violations'])) == (5, 0)


# ---- 突き合わせ ----

SPOT = [('price', 'FLOAT64'), ('price_time', 'DATETIME'), ('diff', 'FLOAT64'), ('diff_rate', 'FLOAT64'),
        ('hv', 'FLOAT64'), ('created_at', 'TIMESTAMP')]
FUTURE = SPOT[:5] + [('contract_month', 'DATE'), ('created_at', 'TIMESTAMP')]
OPTION = [('type', 'INT64'), ('target_price', 'INT64'), ('is_atm', 'BOOL'), ('price', 'INT64'),
          ('price_time', 'DATETIME'), ('diff', 'INT64'), ('diff_rate', 'FLOAT64'), ('iv', 'FLOAT64'),
          ('bid', 'INT64'), ('bid_volume', 'INT64'), ('bid_iv', 'FLOAT64'), ('ask', 'INT64'), ('ask_volume', 'INT64'),
          ('ask_iv', 'FLOAT64'), ('volume', 'INT64'), ('positions', 'INT64'), ('quotation', 'INT64'),
          ('quotation_date', 'DATE'), ('delta', 'FLOAT64'), ('gamma', 'FLOAT64'), ('theta', 'FLOAT64'),
          ('vega', 'FLOAT64'), ('last_trading_day', 'DATE'), ('created_at', 'TIMESTAMP')]
RUNS = [('snapshot_at', 'TIMESTAMP'), ('page2_updated_at', 'TIMESTAMP')]
STAGED = [('spot_rows', 'INT64'), ('future_rows', 'INT64'), ('option_rows', 'INT64')]


def spot_v(i):
    return {'price': 100.0 + i, 'price_time': jst(i), 'diff': 1.0, 'diff_rate': 0.1, 'hv': 20.0}


def future_v(i):
    return {**spot_v(i), 'price': 200.0 + i, 'contract_month': '2026-12-01'}


def option_v(i, k, strike=60000):
    """版 i の k 番目の限月（0: 1 限月、1: 2 限月）の call の行"""
    return {'type': 1, 'target_price': strike, 'is_atm': True, 'price': 10 * i + k, 'price_time': jst(i),
            'diff': 1, 'diff_rate': 0.5, 'iv': 20.0 + i, 'bid': 1, 'bid_volume': 1, 'bid_iv': 19.0, 'ask': 2,
            'ask_volume': 1, 'ask_iv': 21.0, 'volume': 10 + i, 'positions': 100, 'quotation': 5,
            'quotation_date': '2026-10-09', 'delta': 0.5, 'gamma': 0.01, 'theta': -1.0, 'vega': 2.0,
            'last_trading_day': MONTHS[k]}


def empty():
    d = {f'{s}_{t}': [] for s in ('legacy', 'new') for t in ('spot', 'future', 'option')}
    d['new_runs'] = []
    return d


def put(d, side, at, spot=None, future=None, options=()):
    if spot is not None:
        d[f'{side}_spot'].append({**spot, 'created_at': at})
    if future is not None:
        d[f'{side}_future'].append({**future, 'created_at': at})
    for o in options:
        d[f'{side}_option'].append({**o, 'created_at': at})


def put_new(d, at, i, page2_at=None, extra=()):
    put(d, 'new', at, spot_v(i), future_v(i), [option_v(i, 0), option_v(i, 1), *extra])
    d['new_runs'].append({'snapshot_at': at, 'page2_updated_at': page2_at or at})


def compare_fixture():
    """新系 T1〜T11（00:00〜00:10 UTC）と、分類ごとの旧系（2026-10-13 の実データの形を含む）"""
    d = empty()
    for n in range(1, 12):
        i = 3 if n == 4 else n                       # T4 は T3 と同じ中身（新系がその分の更新を取れなかった）
        page2 = ts(n) if n == 6 else None            # T6 は 2 限月ページの更新時刻が 1 限月と違う
        extra = [option_v(n, 0, strike=61000)] if n == 5 else []  # T5 にだけある行使価格（直前・直後に無い）
        put_new(d, ts(n - 1), i, page2, extra)
    o = option_v
    put(d, 'legacy', ts(30), spot_v(30), future_v(30), [o(30, 0), o(30, 1)])  # T0: 新系に無い（取りこぼし）
    put(d, 'legacy', ts(0), spot_v(1), future_v(1), [o(1, 0), o(1, 1)])       # T1: 全部一致
    # T2: 実データの形。スポットは新系の直前（T1）、先物・option は T と同じ
    put(d, 'legacy', ts(1), spot_v(1), future_v(2), [o(2, 0), o(2, 1)])
    # T3: 1 限月の行は直前（T2）の版。2 限月の行は約定の列が T と同じで気配だけ違う
    put(d, 'legacy', ts(2), spot_v(3), future_v(3), [o(2, 0), {**o(3, 1), 'bid': 999, 'ask_iv': 9.9}])
    # T4: 新系の T4 が T3 と同じ中身で、旧系のスポット・先物だけ新しい版
    put(d, 'legacy', ts(3), spot_v(40), future_v(40), [o(3, 0), o(3, 1)])
    # T5: 1 限月は is_atm の表示だけ違う（気配と表示の列）。2 限月は約定価格が違う（説明できない）
    put(d, 'legacy', ts(4), spot_v(5), future_v(5), [{**o(5, 0), 'is_atm': False}, {**o(5, 1), 'price': 999}])
    # T6: 2 限月の約定価格が違うが、2 限月ページの更新時刻が違う
    put(d, 'legacy', ts(5), spot_v(6), future_v(6), [o(6, 0), {**o(6, 1), 'price': 999}])
    # T7: 清算日 +1 年・先物の限月 +1 年（旧パーサーの年の補完）
    put(d, 'legacy', ts(6), spot_v(7), {**future_v(7), 'contract_month': '2027-12-01'},
        [{**o(7, 0), 'quotation_date': '2027-10-09'}, o(7, 1)])
    # T8: スポットが新系の T・直前・直後のどれとも違う（説明できない）
    put(d, 'legacy', ts(7), {**spot_v(8), 'price': 999.0}, future_v(8), [o(8, 0), o(8, 1)])
    # T9（00:08）は新系だけ（保存条件の差）
    put(d, 'legacy', ts(9), spot_v(10), future_v(10), [o(10, 0), o(10, 1)])   # T10: 旧系の重複
    d['legacy_spot'].append({**spot_v(10), 'price': 1.0, 'created_at': ts(9)})
    put(d, 'legacy', ts(10), spot_v(11), None, [o(11, 0), o(11, 1)])          # T11: 旧系の欠け（先物なし）
    return d


def run_compare(d, staged=None):
    if staged is None:
        staged = {'spot_rows': len(d['legacy_spot']), 'future_rows': len(d['legacy_future']),
                  'option_rows': len(d['legacy_option'])}
    sources = cte('staged', STAGED, [staged]) + ''.join(cte(name, cols, d[name]) for name, cols in (
        ('legacy_spot', SPOT), ('legacy_future', FUTURE), ('legacy_option', OPTION), ('new_runs', RUNS),
        ('new_spot', SPOT), ('new_future', FUTURE), ('new_option', OPTION)))
    [row] = bq(result_query(COMPARE, sources, dataset='optionchan_tokyo', copy_dataset='optionchan_legacy_copy'))
    return row


def test_compare_classifies():
    row = run_compare(compare_fixture())
    expected = {
        'legacy_snapshots': 11, 'new_snapshots': 11, 'both_snapshots': 10, 'new_only_snapshots': 1,
        'legacy_only_snapshots': 1, 'legacy_dup_snapshots': 1, 'legacy_partial_snapshots': 1,
        'spot_compared': 8, 'spot_version': 1, 'spot_new_stale': 1, 'spot_unexplained': 1,
        'future_compared': 8, 'future_version': 0, 'future_contract_year': 1, 'future_new_stale': 1,
        'future_unexplained': 0,
        'option_compared': 17, 'option_version': 2, 'option_quote_only': 2, 'option_quotation_year': 1,
        'option_page2': 1, 'option_unexplained': 1,
        'unexplained': 3,
        # 先物の約定時刻が直前から変わった新系のスナップショット（T4 は T3 と同じ）10 件、そのうち旧系に無いのは T9
        'legacy_condition_candidates': 10, 'legacy_condition_missed': 1,
    }
    assert {k: int(row[k]) for k in expected} == expected
    assert row['compare_date'] == '2026-10-13'
    assert (row['copy_complete'], row['fail_on_unexplained']) == ('true', 'false')
    samples = sorted((s['kind'], s['created_at'][:16], s.get('detail')) for s in row['unexplained_samples'])
    assert samples == [('legacy_only', '2026-10-13 00:30', None), ('option', '2026-10-13 00:04', '1 60000 2026-12-10'),
                       ('spot', '2026-10-13 00:07', None)]


def test_compare_all_match():
    d = empty()
    for n in range(3):
        put_new(d, ts(n), n)
        put(d, 'legacy', ts(n), spot_v(n), future_v(n), [option_v(n, 0), option_v(n, 1)])
    row = run_compare(d)
    assert int(row['unexplained']) == 0
    assert (int(row['spot_compared']), int(row['option_compared'])) == (3, 6)
    assert row['unexplained_samples'] == []
    assert row['copy_complete'] == 'true'


def test_compare_new_only_key_without_neighbors_is_unexplained():
    """直前・直後のスナップショットが読む範囲に無いとき（連休明けの最初など）は、新系にだけある行を版の差にしない"""
    d = empty()
    put_new(d, ts(0), 1, extra=[option_v(1, 0, strike=61000)])
    put(d, 'legacy', ts(0), spot_v(1), future_v(1), [option_v(1, 0), option_v(1, 1)])
    row = run_compare(d)
    assert (int(row['option_compared']), int(row['option_version']), int(row['option_unexplained'])) == (3, 0, 1)


def test_compare_detects_incomplete_copy():
    d = empty()
    put_new(d, ts(0), 1)
    put(d, 'legacy', ts(0), spot_v(1), future_v(1), [option_v(1, 0), option_v(1, 1)])
    row = run_compare(d, staged={'spot_rows': 1, 'future_rows': 1, 'option_rows': 1000})
    assert row['copy_complete'] == 'false'


# ---- 実テーブルの dry run（見積もりバイト数。PR に記録する） ----

def test_dry_run_quality():
    n = bq(result_query(QUALITY, dataset='optionchan_tokyo'), dry_run=True)
    print(f'quality: {n} bytes')
    assert n <= GiB


def test_dry_run_compare():
    # コピーのデータセットはまだ無いので、旧スキーマの列を持つ新系のデータセットで代える
    sql = result_query(COMPARE, dataset='optionchan_tokyo', copy_dataset='optionchan_tokyo')
    # staged_window もまだ無いので固定値にする
    sql, n = re.subn(r'^staged AS \(.*?^\),\n', lambda m: cte('staged', STAGED, [dict.fromkeys(('spot_rows', 'future_rows', 'option_rows'), 0)]),
                     sql, count=1, flags=re.S | re.M)
    assert n == 1
    n = bq(sql, dry_run=True)
    print(f'compare: {n} bytes')
    assert n <= 1.5 * GiB


def test_dry_run_stage():
    text = render(read(STAGE), project=PROJECT, legacy_dataset='optionchan', stage_dataset='optionchan_legacy_stage')
    selects = re.findall(r'CREATE OR REPLACE TABLE `[^`]+\.(?:spot|future|option)_price` AS\n(SELECT.*?);', text, re.S)
    assert len(selects) == 3
    total = 0
    for sql in selects:
        total += bq(
            f"DECLARE win_start TIMESTAMP DEFAULT TIMESTAMP '{WIN_START}';\n"
            f"DECLARE win_end TIMESTAMP DEFAULT TIMESTAMP '{WIN_END}';\n{sql}",
            location='US', dry_run=True)
    print(f'stage: {total} bytes')
    assert total <= 500 * MiB


@pytest.mark.parametrize('path, location, names', [
    (QUALITY, TOKYO, {'dataset': 'optionchan_tokyo'}),
    (COMPARE, TOKYO, {'dataset': 'optionchan_tokyo', 'copy_dataset': 'optionchan_legacy_copy', 'fail_on_unexplained': 'false'}),
    (STAGE, 'US', {'legacy_dataset': 'optionchan', 'stage_dataset': 'optionchan_legacy_stage'}),
])
def test_dry_run_whole_script(path, location, names):
    """スクリプト全体（DECLARE・IF・CREATE・MERGE・ERROR）の構文。スクリプトの dry run は構文だけを見る（表の有無は見ない）"""
    sql = render(read(path), project=PROJECT, **names)
    assert bq(sql, location=location, dry_run=True, params=['run_time:TIMESTAMP:2026-10-13T23:00:00Z']) == 0

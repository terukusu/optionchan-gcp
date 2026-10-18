"""1 時間足（#49）の SQL を BigQuery で実際に動かす検査（tests/test_sql_bq.py・tests/test_legacy_import_bq.py と同じ流儀）。

CI では動かさない（BigQuery の認証が要る）。手元で `OPTIONCHAN_BQ_TEST=1 python -m pytest -q tests/test_ohlc_bq.py`。
- 固定値の検査: 各 SQL の `-- result:begin`〜`-- result:end` を取り出し、`-- source:begin`〜`-- source:end`（テーブルを読む CTE）を
  固定値の CTE に差し替えて実行する（テーブルを読まない）
- 書き込みの試験: atm_hourly.sql（MERGE）・import.sql を、本番の表の代わりにスクリプトの一時テーブルに対して流す（本番の表には書かない）
- 実テーブルの dry run: 見積もりバイト数（PR に記録する）と、スクリプト全体の構文
"""
import json
import os
import re
import subprocess

import pytest

from test_sql_bq import GiB, MiB, PROJECT, TOKYO, between, bq, cte, literal, read, render

pytestmark = pytest.mark.skipif(
    os.environ.get('OPTIONCHAN_BQ_TEST') != '1' or not PROJECT,
    reason='OPTIONCHAN_BQ_TEST=1 と OPTIONCHAN_BQ_PROJECT のときだけ',
)

ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
SQL_DIR = os.path.join(ROOT, 'sql', 'ohlc')
HOURLY = os.path.join(SQL_DIR, 'atm_hourly.sql')
CHECK = os.path.join(SQL_DIR, 'legacy_check.sql')
EXPORT = os.path.join(SQL_DIR, 'export.sql')
COMPARE = os.path.join(SQL_DIR, 'compare.sql')
IMPORT = os.path.join(SQL_DIR, 'import.sql')
FINGERPRINT = os.path.join(SQL_DIR, 'fingerprint.sql')
TABLES = ('atm_iv', 'atm_option_price', 'atm_target_price')
HOURLY_TABLES = TABLES + ('atm_mid_iv',)   # 定期クエリが書く表（#98 で mid IV を足した）
BACKFILL = os.path.join(SQL_DIR, 'mid_backfill.sql')

M1, M2 = '2026-11-12', '2026-12-10'
ATM = [('created_at', 'TIMESTAMP'), ('last_trading_day', 'DATE'), ('target_price', 'INT64'), ('iv', 'FLOAT64'),
       ('price', 'INT64'), ('bid_iv', 'FLOAT64'), ('ask_iv', 'FLOAT64')]
BAR = [('open', 'FLOAT64'), ('high', 'FLOAT64'), ('low', 'FLOAT64'), ('close', 'FLOAT64'), ('last_trading_day', 'DATE'),
       ('time_frame', 'INT64'), ('started_at', 'TIMESTAMP')]
LEGACY_BAR = [('tbl', 'STRING')] + BAR
# 定期クエリの窓: run_time 2026-10-12 23:00 UTC → [2026-10-11 22:00, 2026-10-12 22:00) UTC
RUN_TIME = '2026-10-12T23:00:00Z'


def ts(hour, minute=0, day=12):
    return f'2026-10-{day:02d} {hour:02d}:{minute:02d}:00+00'


def atm(at, month, strike, iv, price, bid_iv=None, ask_iv=None):
    return {'created_at': at, 'last_trading_day': month, 'target_price': strike, 'iv': iv, 'price': price,
            'bid_iv': bid_iv, 'ask_iv': ask_iv}


def bar(o, h, low, c, month, at):
    return {'open': o, 'high': h, 'low': low, 'close': c, 'last_trading_day': month, 'time_frame': 3600, 'started_at': at}


def num(v):
    return None if v is None else float(v)


def run_script(script, params=(), maximum_bytes=2 * GiB):
    """複数の文のスクリプトを流し、SELECT の結果（空でない行の配列）を順に返す。DML を含むので bq() の 10MB の上限は使わない"""
    cmd = ['bq', f'--project_id={PROJECT}', f'--location={TOKYO}', 'query', '--use_legacy_sql=false', '--format=json',
           '--nouse_cache', f'--maximum_bytes_billed={maximum_bytes}', *(f'--parameter={p}' for p in params)]
    out = subprocess.run(cmd, input=script, capture_output=True, text=True, timeout=600)
    # 失敗時は BigQuery のエラーの行だけを出す（ジョブの JSON にはアカウントのメールアドレスが入るので出さない）
    assert out.returncode == 0, '\n'.join(
        line for line in (out.stdout + out.stderr).splitlines() if 'error' in line.lower() or 'at [' in line)
    text = out.stdout[out.stdout.index('['):] if '[' in out.stdout else '[]'
    # 文ごとの出力のうち行の配列だけ（BEGIN / COMMIT TRANSACTION の空の配列と、作成・件数のメッセージは除く）
    return [r for r in json.loads(text) if isinstance(r, list) and r]


def run_error(script, params=()):
    cmd = ['bq', f'--project_id={PROJECT}', f'--location={TOKYO}', 'query', '--use_legacy_sql=false', '--format=json',
           '--nouse_cache', '--maximum_bytes_billed=104857600', *(f'--parameter={p}' for p in params)]
    out = subprocess.run(cmd, input=script, capture_output=True, text=True, timeout=300)
    assert out.returncode != 0
    # bq はエラーの文を 80 桁ほどで折り返すので、空白をつなげてから探す
    return ' '.join((out.stdout + out.stderr).split())


def with_sources(path, sources, **names):
    """結果の部分（result:begin〜end）の source を固定値に差し替える"""
    text = between(read(path), '-- result:begin', '-- result:end').group(1)
    m = between(text, '-- source:begin', '-- source:end')
    return render(text[:m.start()] + sources + text[m.end():], project=PROJECT, **names)


def temp_table(name, cols, rows):
    struct = ', '.join(f'{c} {t}' for c, t in cols)
    values = ', '.join('STRUCT(' + ', '.join(literal(r.get(c), t) for c, t in cols) + ')' for r in rows)
    return f'CREATE TEMP TABLE {name} AS SELECT * FROM UNNEST(ARRAY<STRUCT<{struct}>>[{values}]);\n'


def split_declares(text):
    """スクリプトを先頭の DECLARE と残り（最初の IF から）に分ける（一時テーブルの作成は DECLARE の後に入れる）"""
    i = text.index('\nIF ') + 1
    assert 'DECLARE' not in text[i:]
    return text[:i], text[i:]


def tables_to_temp(text, prefix=''):
    text = re.sub(r'`\$\{project\}\.\$\{import_dataset\}\.(\w+)`', r'imp_\1', text)
    return re.sub(r'`\$\{project\}\.\$\{(?:dataset|source_dataset)\}\.(\w+)`', prefix + r'\1', text)


def bars_rows(rows):
    return sorted((r['last_trading_day'], r['started_at'][:16]) + tuple(num(r[k]) for k in (
        'iv_open', 'iv_high', 'iv_low', 'iv_close', 'price_open', 'price_high', 'price_low', 'price_close',
        'target_price_open', 'target_price_high', 'target_price_low', 'target_price_close')) for r in rows)


# ---- 1 時間足の計算（atm_hourly.sql の bars。legacy_check.sql も同じ本文） ----

FIXTURE = [
    # M1 の 00 時台: ATM が 60000 → 60500 に変わる（分けない）。iv の NULL は open・close で飛ばす
    # mid（#98）: bid_iv・ask_iv の平均。片方が 0 の行は NULL（open・close で飛ばす）
    atm(ts(0, 5), M1, 60000, 0.20, 100, 0.19, 0.21),
    atm(ts(0, 30), M1, 60000, None, 120, 0.0, 0.25),
    atm(ts(0, 59), M1, 60500, 0.30, 90, 0.28, 0.30),
    # 01:00 ちょうどは次の 1 時間足。気配が無い → mid は NULL の 1 時間足
    atm(ts(1, 0), M1, 60500, 0.25, 95),
    # M2 の 00 時台: iv・price が全部 NULL → 値が NULL の 1 時間足（行は作る。旧系と同じ）
    atm(ts(0, 10), M2, 61000, None, None),
    atm(ts(0, 40), M2, 61000, None, None),
    # M2 の 02 時台: 同じ created_at に 2 行（旧系の重複を移した行・ATM が 2 行の版）→ target_price の順で決める
    atm(ts(2, 10), M2, 61000, 0.40, 300, 0.38, 0.40),
    atm(ts(2, 10), M2, 60500, 0.50, 350, 0.48, 0.52),
]
EXPECTED = sorted([
    (M1, '2026-10-12 00:00', 0.20, 0.30, 0.20, 0.30, 100.0, 120.0, 90.0, 90.0, 60000.0, 60500.0, 60000.0, 60500.0),
    (M1, '2026-10-12 01:00', 0.25, 0.25, 0.25, 0.25, 95.0, 95.0, 95.0, 95.0, 60500.0, 60500.0, 60500.0, 60500.0),
    (M2, '2026-10-12 00:00', None, None, None, None, None, None, None, None, 61000.0, 61000.0, 61000.0, 61000.0),
    (M2, '2026-10-12 02:00', 0.50, 0.50, 0.40, 0.40, 350.0, 350.0, 300.0, 300.0, 60500.0, 61000.0, 60500.0, 61000.0),
])


# mid の 1 時間足（#98）: (限月, started_at, open, high, low, close)
EXPECTED_MID = sorted([
    (M1, '2026-10-12 00:00', 0.20, 0.29, 0.20, 0.29),
    (M1, '2026-10-12 01:00', None, None, None, None),
    (M2, '2026-10-12 00:00', None, None, None, None),
    (M2, '2026-10-12 02:00', 0.50, 0.50, 0.39, 0.39),
])


def mid_rows(rows):
    return sorted((r['last_trading_day'], r['started_at'][:16]) + tuple(
        None if r[k] is None else round(float(r[k]), 6) for k in ('mid_open', 'mid_high', 'mid_low', 'mid_close'))
        for r in rows)


def run_bars(atm_rows):
    # bq() はスクリプト（DECLARE + SELECT）の結果を読むので、使わない変数を 1 つ宣言する
    return bq('DECLARE win_start TIMESTAMP;\n' + with_sources(HOURLY, cte('atm', ATM, atm_rows), dataset='optionchan_tokyo'))


def test_bars_fixed_values():
    rows = run_bars(FIXTURE)
    assert bars_rows(rows) == EXPECTED
    assert mid_rows(rows) == EXPECTED_MID
    assert {r['time_frame'] for r in rows} == {'3600'}


def test_bars_order_does_not_depend_on_input_order():
    rows = run_bars(list(reversed(FIXTURE)))
    assert bars_rows(rows) == EXPECTED


# ---- 定期クエリの MERGE（一時テーブルに書く） ----

def hourly_script(atm_rows, existing, runs=1):
    """atm_hourly.sql を、source を固定値・3 表を一時テーブルにして runs 回流し、最後に 3 表の中身を返すスクリプト"""
    text = render(tables_to_temp(read(HOURLY)), project=PROJECT, dataset='optionchan_tokyo')
    m = between(text, '-- source:begin', '-- source:end')
    text = text[:m.start()] + cte('atm', ATM, atm_rows) + text[m.end():]
    declares, rest = split_declares(text)
    setup = ''.join(temp_table(t, BAR, existing.get(t, [])) for t in HOURLY_TABLES)
    dump = ("SELECT 'atm_iv' AS tbl, * FROM atm_iv UNION ALL SELECT 'atm_option_price', * FROM atm_option_price "
            "UNION ALL SELECT 'atm_target_price', * FROM atm_target_price UNION ALL SELECT 'atm_mid_iv', * FROM atm_mid_iv;\n")
    script = declares + setup + rest + '\n' + dump
    for _ in range(runs - 1):
        script += 'DROP TABLE bars;\n' + rest + '\n' + dump
    return script


def rnd(v):
    return None if v is None else round(float(v), 6)


def table_rows(rows):
    return sorted((r['tbl'], r['last_trading_day'], r['started_at'][:16], rnd(r['open']), rnd(r['high']), rnd(r['low']),
                   rnd(r['close'])) for r in rows)


def test_hourly_merge_is_idempotent_and_replaces_window():
    existing = {
        # 窓の中: 同じキーは更新、計算に無いキーは消す
        'atm_iv': [bar(9, 9, 9, 9, M1, ts(0)), bar(9, 9, 9, 9, M1, ts(5))],
        # 窓の外（前の窓・次の窓）は触れない
        'atm_option_price': [bar(7, 7, 7, 7, M1, ts(21, day=11)), bar(7, 7, 7, 7, M1, ts(22))],
    }
    first, second = run_script(hourly_script(FIXTURE, existing, runs=2), params=[f'run_time:TIMESTAMP:{RUN_TIME}'])
    assert table_rows(first) == table_rows(second)
    got = table_rows(second)
    iv = [r for r in got if r[0] == 'atm_iv']
    assert iv == sorted([('atm_iv', e[0], e[1], *e[2:6]) for e in EXPECTED])
    price = [r for r in got if r[0] == 'atm_option_price']
    assert ('atm_option_price', M1, '2026-10-11 21:00', 7.0, 7.0, 7.0, 7.0) in price
    assert ('atm_option_price', M1, '2026-10-12 22:00', 7.0, 7.0, 7.0, 7.0) in price
    assert len(price) == len(EXPECTED) + 2
    assert len([r for r in got if r[0] == 'atm_target_price']) == len(EXPECTED)
    assert [r for r in got if r[0] == 'atm_mid_iv'] == sorted(('atm_mid_iv', *e) for e in EXPECTED_MID)


def test_hourly_merge_does_nothing_without_atm_rows():
    existing = {t: [bar(1, 2, 0.5, 1.5, M1, ts(3))] for t in HOURLY_TABLES}
    [after] = run_script(hourly_script([], existing), params=[f'run_time:TIMESTAMP:{RUN_TIME}'])
    assert table_rows(after) == sorted((t, M1, '2026-10-12 03:00', 1.0, 2.0, 0.5, 1.5) for t in HOURLY_TABLES)


# ---- mid IV の過去分の作り直し（mid_backfill.sql、#98。一時テーブルに書く） ----

def backfill_script(atm_rows, existing, iv_rows):
    """mid_backfill.sql を、source を固定値・atm_iv（読む、#122）と atm_mid_iv（書く）を一時テーブルにして流すスクリプト"""
    text = render(tables_to_temp(read(BACKFILL)), project=PROJECT, dataset='optionchan_tokyo')
    m = between(text, '-- source:begin', '-- source:end')
    text = text[:m.start()] + cte('atm', ATM, atm_rows) + text[m.end():]
    i = text.index('\nIF ') + 1
    setup = temp_table('atm_iv', BAR, iv_rows) + temp_table('atm_mid_iv', BAR, existing)
    return text[:i] + setup + text[i:] + '\nSELECT * FROM atm_mid_iv;\n'


BACKFILL_PARAMS = [f'start_at:TIMESTAMP:{ts(0)}', f'end_at:TIMESTAMP:{ts(3)}']
# atm_iv の 1 時間足（値は使わない。キーだけを読む）
IV_KEYS = [bar(1, 1, 1, 1, e[0], e[1] + ':00+00') for e in EXPECTED_MID]


def mid_table(rows):
    return sorted((r['last_trading_day'], r['started_at'][:16], rnd(r['open']), rnd(r['high']), rnd(r['low']),
                   rnd(r['close'])) for r in rows)


def test_backfill_same_as_hourly_and_idempotent():
    # 範囲 [00:00, 03:00): 中の既存の足は置き換え、計算に無いキーは消す。範囲の外（03:00）は触れない
    existing = [bar(9, 9, 9, 9, M1, ts(0)), bar(9, 9, 9, 9, M1, ts(2)), bar(7, 7, 7, 7, M1, ts(3))]
    script = backfill_script(FIXTURE, existing, IV_KEYS)
    i = script.index('\nIF ') + 1
    first, *_ = run_script(script, params=BACKFILL_PARAMS)
    assert [int(first[0]['bars']), int(first[0]['with_mid'])] == [4, 2]
    _, after = run_script(script, params=BACKFILL_PARAMS)[-2:]
    got = mid_table(after)
    assert got == sorted(EXPECTED_MID + [(M1, '2026-10-12 03:00', 7.0, 7.0, 7.0, 7.0)])
    # 流し直しても同じ（スクリプトの 2 回目: 表は作り直さず、計算と MERGE だけを繰り返す）
    rest = script[i:].replace('CREATE TEMP TABLE bars', 'DROP TABLE bars;\nCREATE TEMP TABLE bars', 1)
    *_, again = run_script(script + rest, params=BACKFILL_PARAMS)
    assert mid_table(again) == got


# 読まない atm_iv の行（#122）: 範囲の外のキーと、範囲の中の日足（time_frame = 86400）
IV_IGNORED = [bar(1, 1, 1, 1, M1, ts(3)), {**bar(1, 1, 1, 1, M1, ts(1)), 'time_frame': 86400}]
# 範囲の外の既存の mid の足（前日 23:00・03:00）。どの試験でもそのまま残る
MID_OUTSIDE = [bar(6, 6, 6, 6, M1, ts(23, day=11)), bar(7, 7, 7, 7, M1, ts(3))]
MID_OUTSIDE_ROWS = [(M1, '2026-10-11 23:00', 6.0, 6.0, 6.0, 6.0), (M1, '2026-10-12 03:00', 7.0, 7.0, 7.0, 7.0)]


def test_backfill_drops_keys_without_iv():
    """#122: atm_iv に無いキー（M1 の 01:00）の mid の足は作らず、範囲の中の既存の足も消す。範囲の外は残る"""
    iv = [r for r in IV_KEYS if not (r['last_trading_day'] == M1 and r['started_at'].startswith('2026-10-12 01:00'))]
    assert len(iv) == 3
    existing = [bar(9, 9, 9, 9, M1, ts(1)),            # atm_iv に無いキー（作り直しで消える）
                bar(8, 8, 8, 8, M2, ts(1))] + MID_OUTSIDE  # 範囲の中の、どちらにも無いキー（消える）
    result, after = run_script(backfill_script(FIXTURE, existing, iv + IV_IGNORED), params=BACKFILL_PARAMS)
    assert [int(result[0]['bars']), int(result[0]['with_mid'])] == [3, 2]
    expected = [e for e in EXPECTED_MID if e[:2] != (M1, '2026-10-12 01:00')]
    assert mid_table(after) == sorted(expected + MID_OUTSIDE_ROWS)


def test_backfill_does_nothing_without_iv_keys():
    """#122: 範囲の中に atm_iv の 1 時間足のキーが 1 つも無ければ（ATM の行はあっても）何もしない"""
    existing = [bar(9, 9, 9, 9, M1, ts(0)), bar(9, 9, 9, 9, M1, ts(1))] + MID_OUTSIDE
    result, after = run_script(backfill_script(FIXTURE, existing, IV_IGNORED), params=BACKFILL_PARAMS)
    assert [int(result[0]['bars']), int(result[0]['with_mid'])] == [0, 0]
    assert mid_table(after) == sorted([(M1, '2026-10-12 00:00', 9.0, 9.0, 9.0, 9.0),
                                       (M1, '2026-10-12 01:00', 9.0, 9.0, 9.0, 9.0)] + MID_OUTSIDE_ROWS)


@pytest.mark.parametrize('start,end', [(ts(3), ts(0)), (ts(0, 30), ts(3)), ('2099-01-01 00:00:00+00', '2099-01-02 00:00:00+00')])
def test_backfill_rejects_bad_range(start, end):
    text = render(read(BACKFILL), project=PROJECT, dataset='optionchan_tokyo')
    guard = text[:text.index('CREATE TEMP TABLE bars')]
    assert 'mid_backfill: ' in run_error(guard, params=[f'start_at:TIMESTAMP:{start}', f'end_at:TIMESTAMP:{end}'])


def test_hourly_rejects_future_window():
    text = render(read(HOURLY), project=PROJECT, dataset='optionchan_tokyo')
    declares, rest = split_declares(text)
    guard = rest[:rest.index('CREATE TEMP TABLE bars')]
    assert 'is in the future' in run_error(declares + guard, params=['run_time:TIMESTAMP:2099-01-01T00:00:00Z'])


# ---- 旧系の同値の確認（legacy_check.sql） ----

CHECK_START, CHECK_END = ts(0, day=11), ts(0, day=13)


def run_check(atm_rows, legacy_rows):
    sql = with_sources(CHECK, cte('atm', ATM, atm_rows) + cte('legacy', LEGACY_BAR, legacy_rows), legacy_dataset='optionchan')
    rows = bq(f"DECLARE start_at TIMESTAMP DEFAULT TIMESTAMP '{CHECK_START}';\n"
              f"DECLARE end_at TIMESTAMP DEFAULT TIMESTAMP '{CHECK_END}';\n{sql}", location='US')
    return {r['tbl']: r for r in rows}


COUNTS = ('recomputed', 'legacy_keys', 'match', 'late', 'differ', 'legacy_only', 'recomputed_only', 'legacy_dup_keys',
          'legacy_conflict_keys')


def test_check_classifies():
    rows = [atm(ts(0, 5), M1, 60000, 0.2, 100), atm(ts(0, 50), M1, 60000, 0.3, 110),   # 00 時: 一致（旧系は同じ中身の重複）
            atm(ts(1, 5), M1, 60000, 0.2, 100),                                          # 01 時: 値の違い
            atm(ts(2, 5), M1, 60000, 0.2, 100),                                          # 02 時: 計算し直した側だけ
            atm(ts(23, 5, day=11), M1, 60000, 0.2, 100),                                 # 前日 23 時: 旧系に無い（late）
            atm(ts(23, 50, day=11), M2, 61000, 0.4, 200)]                                # 前日 23 時 M2: 旧系は 23:45 までの値（late）
    legacy = []
    for t, (o, h, low, c) in (('atm_iv', (0.2, 0.3, 0.2, 0.3)), ('atm_option_price', (100, 110, 100, 110)),
                              ('atm_target_price', (60000, 60000, 60000, 60000))):
        legacy += [{'tbl': t, **bar(o, h, low, c, M1, ts(0))}] * 2
        legacy.append({'tbl': t, **bar(9, 9, 9, 9, M1, ts(1))})
        legacy.append({'tbl': t, **bar(9, 9, 9, 9, M1, ts(4))})                         # 旧系だけ
        legacy.append({'tbl': t, **bar(1, 1, 1, 1, M2, ts(23, day=11))})
        legacy += [{'tbl': t, **bar(1, 1, 1, 1, M2, ts(6))}, {'tbl': t, **bar(2, 2, 2, 2, M2, ts(6))}]   # 中身の違う重複
    got = run_check(rows, legacy)
    for t in TABLES:
        assert {k: int(got[t][k]) for k in COUNTS} == {
            'recomputed': 5, 'legacy_keys': 5, 'match': 1, 'late': 2, 'differ': 1, 'legacy_only': 1,
            'recomputed_only': 1, 'legacy_dup_keys': 2, 'legacy_conflict_keys': 1}, t
        assert sorted(s['kind'] for s in got[t]['samples']) == ['differ', 'legacy_only', 'recomputed_only']


def test_check_all_match():
    rows = [atm(ts(0, 5), M1, 60000, None, None)]
    legacy = [{'tbl': t, **bar(*v, M1, ts(0))} for t, v in (
        ('atm_iv', (None,) * 4), ('atm_option_price', (None,) * 4), ('atm_target_price', (60000,) * 4))]
    got = run_check(rows, legacy)
    for t in TABLES:
        assert (int(got[t]['match']), int(got[t]['recomputed']), got[t]['samples']) == (1, 1, []), t


# ---- 新旧の突き合わせ（export.sql → compare.sql） ----

def export_json(legacy_rows, start, end):
    """export.sql の 3 表を固定値の一時テーブルにして流し、bars（JSON 文字列）を返す"""
    by = {t: [r for r in legacy_rows if r['tbl'] == t] for t in TABLES}
    script = ''.join(temp_table(t, BAR, by[t]) for t in TABLES) + tables_to_temp(read(EXPORT)) + ';'
    [[row]] = run_script(render(script, project=PROJECT, source_dataset='x'), params=[f'start_at:TIMESTAMP:{start}', f'end_at:TIMESTAMP:{end}'])
    return row['bars'], int(row['rows_'])


def run_compare(legacy_json, new_rows, start, end):
    sql = with_sources(COMPARE, cte('new_rows', LEGACY_BAR, new_rows), dataset='optionchan_tokyo')
    script = (f"DECLARE start_at TIMESTAMP DEFAULT TIMESTAMP '{start}';\n"
              f"DECLARE end_at TIMESTAMP DEFAULT TIMESTAMP '{end}';\n"
              'DECLARE legacy_json STRING DEFAULT @legacy_bars;\n' + sql + ';')
    [rows] = run_script(script, params=[f'legacy_bars:STRING:{legacy_json}'], maximum_bytes=10 * MiB)
    return {r['tbl']: r for r in rows}


def test_export_and_compare():
    start, end = ts(0), ts(0, day=13)
    legacy, new = [], []
    for t in TABLES:
        legacy += [{'tbl': t, **bar(0.1 + 0.2, 1 / 3, None, 7.0, M1, ts(0))}] * 2                     # 一致（旧系は重複）
        legacy.append({'tbl': t, **bar(1, 2, 0.5, 1.5, M1, ts(1))})                                   # 値の違い
        legacy.append({'tbl': t, **bar(1, 1, 1, 1, M1, ts(2))})                                       # 旧系だけ
        legacy.append({'tbl': t, **bar(1, 1, 1, 1, M1, ts(23, day=11))})                              # 範囲の外
        legacy += [{'tbl': t, **bar(1, 1, 1, 1, M2, ts(4))}, {'tbl': t, **bar(2, 2, 2, 2, M2, ts(4))}]  # 中身の違う重複
        new.append({'tbl': t, **bar(0.1 + 0.2, 1 / 3, None, 7.0, M1, ts(0))})
        new.append({'tbl': t, **bar(1, 2.25, 0.5, 1.5, M1, ts(1))})
        new.append({'tbl': t, **bar(1, 1, 1, 1, M1, ts(3))})                                          # 新系だけ
        new.append({'tbl': t, **bar(1, 1, 1, 1, M2, ts(4))})
    legacy_json, n = export_json(legacy, start, end)
    assert n == 5 * 3                                                                                 # 重複は 1 要素
    got = run_compare(legacy_json, new, start, end)
    for t in TABLES:
        assert {k: int(got[t][k]) for k in ('legacy_keys', 'new_keys', 'match', 'differ', 'legacy_only', 'new_only',
                                            'legacy_dup_keys', 'conflict_keys')} == {
            'legacy_keys': 4, 'new_keys': 4, 'match': 1, 'differ': 1, 'legacy_only': 1, 'new_only': 1,
            'legacy_dup_keys': 2, 'conflict_keys': 1}, t
        assert float(got[t]['max_abs_diff']) == 0.25
        assert sorted(s['kind'] for s in got[t]['samples']) == ['differ', 'legacy_only', 'new_only']


# ---- 過去分の移行（import.sql、fingerprint.sql。一時テーブルに書く） ----

RUNS = [('snapshot_at', 'TIMESTAMP'), ('trigger', 'STRING')]
IMPORT_END = ts(22)                                                   # = C（新系の最初の 23:39 を含む窓の始まり）


def import_script(legacy, tokyo, runs=1):
    text = render(tables_to_temp(read(IMPORT)), project=PROJECT, dataset='optionchan_tokyo', import_dataset='x')
    declares, rest = split_declares(text)
    setup = (''.join(temp_table(f'imp_{t}', BAR, [r for r in legacy if r['tbl'] == t]) for t in TABLES)
             + ''.join(temp_table(t, BAR, [r for r in tokyo if r['tbl'] == t]) for t in TABLES)
             + temp_table('ingest_runs', RUNS, [{'snapshot_at': ts(23, 39), 'trigger': 'scheduler'},
                                               {'snapshot_at': ts(5), 'trigger': 'legacy_import'}]))
    # DECLARE の DEFAULT は一時テーブルより前に評価されるので、ingest_runs を読む 2 つは SET で入れ直す
    reset = ("SET first_new = (SELECT MIN(snapshot_at) FROM ingest_runs WHERE trigger != 'legacy_import');\n"
             "SET cutover = TIMESTAMP(DATETIME(DATE(TIMESTAMP_SUB(first_new, INTERVAL 7 HOUR), 'Asia/Tokyo'), "
             "TIME '07:00:00'), 'Asia/Tokyo');\n")
    declares = declares.replace('`', '')
    declares = re.sub(r'DECLARE first_new TIMESTAMP DEFAULT \(.*?\);', 'DECLARE first_new TIMESTAMP;', declares, flags=re.S)
    declares = re.sub(r'DECLARE cutover TIMESTAMP DEFAULT TIMESTAMP\(.*?\);', 'DECLARE cutover TIMESTAMP;', declares, flags=re.S)
    fp = render(tables_to_temp(read(FINGERPRINT)), project=PROJECT, source_dataset='x')
    script = declares + setup + reset + rest
    for _ in range(runs - 1):
        script += '\nDROP TABLE src;\n' + rest
    script += ("\nSELECT 'atm_iv' AS tbl, * FROM atm_iv UNION ALL SELECT 'atm_option_price', * FROM atm_option_price "
               "UNION ALL SELECT 'atm_target_price', * FROM atm_target_price;\n")
    script += fp.replace('@start_at', 'start_at').replace('@end_at', 'end_at') + ';\n'
    script += fp.replace('@start_at', 'start_at').replace('@end_at', 'end_at').replace(' atm_', ' imp_atm_') + ';\n'
    return script


def test_import_collapses_duplicates_skips_conflicts_and_is_idempotent():
    legacy, tokyo = [], []
    for t in TABLES:
        legacy += [{'tbl': t, **bar(1, 2, 0.5, 1.5, M1, ts(0, day=1))}] * 2                       # 同じ中身の重複 → 1 行
        legacy += [{'tbl': t, **bar(None, None, None, None, M1, ts(1, day=1))}]                   # NULL の 1 時間足もそのまま
        legacy += [{'tbl': t, **bar(1, 1, 1, 1, M2, ts(2, day=1))}, {'tbl': t, **bar(2, 2, 2, 2, M2, ts(2, day=1))}]  # 中身の違う重複
        legacy += [{'tbl': t, **bar(3, 3, 3, 3, M1, ts(3, day=1))}]                               # 新系に既にある（上書きしない）
        legacy += [{'tbl': t, **bar(4, 4, 4, 4, M1, ts(23))}]                                     # 範囲の外（C 以降）
        tokyo += [{'tbl': t, **bar(5, 5, 5, 5, M1, ts(3, day=1))}]
    results = run_script(import_script(legacy, tokyo, runs=2), params=[
        f'start_at:TIMESTAMP:{ts(0, day=1)}', f'end_at:TIMESTAMP:{IMPORT_END}'], maximum_bytes=2 * GiB)
    first, second, after, fp_tokyo, fp_import = results
    for t in TABLES:
        [r1] = [r for r in first if r['tbl'] == t]
        assert {k: int(r1[k]) for k in ('candidate_keys', 'inserted', 'skipped_existing', 'conflict_keys',
                                        'dup_rows_collapsed')} == {
            'candidate_keys': 4, 'inserted': 2, 'skipped_existing': 1, 'conflict_keys': 1, 'dup_rows_collapsed': 1}, t
        assert r1['cutover'].startswith('2026-10-12 22:00:00')
        [r2] = [r for r in second if r['tbl'] == t]
        assert (int(r2['inserted']), int(r2['skipped_existing'])) == (0, 3), t
    got = table_rows(after)
    for t in TABLES:
        assert [r[1:] for r in got if r[0] == t] == sorted([
            (M1, '2026-10-01 00:00', 1.0, 2.0, 0.5, 1.5), (M1, '2026-10-01 01:00', None, None, None, None),
            (M1, '2026-10-01 03:00', 5.0, 5.0, 5.0, 5.0)])
    # 移行の検証の形: 新系は rows_ = keys、旧系の fp_clean_* から上書きしなかったキーを除いた分だけ違う
    tokyo_fp = {r['tbl']: r for r in fp_tokyo}
    import_fp = {r['tbl']: r for r in fp_import}
    for t in TABLES:
        assert (int(tokyo_fp[t]['rows_']), int(tokyo_fp[t]['keys']), int(tokyo_fp[t]['conflict_keys'])) == (3, 3, 0)
        assert (int(import_fp[t]['rows_']), int(import_fp[t]['keys']), int(import_fp[t]['dup_keys']),
                int(import_fp[t]['conflict_keys'])) == (6, 4, 2, 1)


def test_fingerprint_clean_matches_after_import():
    """既存キーが無ければ、移行後の新系の fp_clean_* = 旧系の fp_clean_*（runbook の移行の検証の条件）"""
    legacy = []
    for t in TABLES:
        legacy += [{'tbl': t, **bar(1, 2, 0.5, 1.5, M1, ts(0, day=1))}] * 2
        legacy += [{'tbl': t, **bar(1 / 3, None, 0.1 + 0.2, 1.5, M2, ts(1, day=1))}]
        legacy += [{'tbl': t, **bar(1, 1, 1, 1, M2, ts(2, day=1))}, {'tbl': t, **bar(2, 2, 2, 2, M2, ts(2, day=1))}]
    _, _, fp_tokyo, fp_import = run_script(import_script(legacy, []), params=[
        f'start_at:TIMESTAMP:{ts(0, day=1)}', f'end_at:TIMESTAMP:{IMPORT_END}'])
    tokyo_fp = {r['tbl']: r for r in fp_tokyo}
    import_fp = {r['tbl']: r for r in fp_import}
    for t in TABLES:
        assert (tokyo_fp[t]['fp_clean_xor'], tokyo_fp[t]['fp_clean_sum']) == (
            import_fp[t]['fp_clean_xor'], import_fp[t]['fp_clean_sum']), t
        assert tokyo_fp[t]['fp_clean_xor'] == tokyo_fp[t]['fp_all_xor']
        assert int(tokyo_fp[t]['keys']) == int(import_fp[t]['keys']) - int(import_fp[t]['conflict_keys'])


def test_import_rejects_range_after_cutover():
    legacy = [{'tbl': 'atm_iv', **bar(1, 1, 1, 1, M1, ts(0, day=1))}]
    script = import_script(legacy, [])
    out = run_error(script, params=[f'start_at:TIMESTAMP:{ts(0, day=1)}', f'end_at:TIMESTAMP:{ts(23)}'])
    assert 'is after the first window of the new system' in out


# ---- 実テーブルの dry run（見積もりバイト数。PR に記録する）とスクリプト全体の構文 ----

def test_dry_run_hourly_bars():
    """新系の 1 日の窓（2026-10-13 07:00 JST〜）の最新版から 1 時間足を作る SELECT"""
    sql = render(between(read(HOURLY), '-- result:begin', '-- result:end').group(1), project=PROJECT, dataset='optionchan_tokyo')
    n = bq("DECLARE win_start TIMESTAMP DEFAULT TIMESTAMP '2026-10-12 22:00:00+00';\n"
           "DECLARE win_end TIMESTAMP DEFAULT TIMESTAMP '2026-10-13 22:00:00+00';\n" + sql, dry_run=True)
    print(f'atm_hourly bars: {n} bytes')
    assert n <= 500 * MiB


def test_dry_run_mid_backfill_one_year():
    """mid IV の過去分の作り直し（#98）の 1 年分の SELECT（1 年ずつ流す）"""
    sql = render(between(read(BACKFILL), '-- result:begin', '-- result:end').group(1), project=PROJECT,
                 dataset='optionchan_tokyo')
    n = bq("DECLARE start_at TIMESTAMP DEFAULT TIMESTAMP '2025-01-01 00:00:00+00';\n"
           "DECLARE end_at TIMESTAMP DEFAULT TIMESTAMP '2026-01-01 00:00:00+00';\n" + sql, dry_run=True)
    print(f'mid_backfill 2025: {n} bytes')
    assert n <= 10 * GiB


def test_dry_run_legacy_check_30_days():
    sql = render(between(read(CHECK), '-- result:begin', '-- result:end').group(1), project=PROJECT, legacy_dataset='optionchan')
    n = bq("DECLARE start_at TIMESTAMP DEFAULT TIMESTAMP '2026-09-12 00:00:00+00';\n"
           "DECLARE end_at TIMESTAMP DEFAULT TIMESTAMP '2026-10-13 00:00:00+00';\n" + sql, location='US', dry_run=True)
    print(f'legacy_check 30 days: {n} bytes')
    assert n <= GiB


@pytest.mark.parametrize('path', [EXPORT, FINGERPRINT])
def test_dry_run_legacy_reads(path):
    sql = render(read(path), project=PROJECT, source_dataset='optionchan')
    n = bq(sql, location='US', dry_run=True,
           params=['start_at:TIMESTAMP:2018-01-01 00:00:00+00', 'end_at:TIMESTAMP:2026-10-12 22:00:00+00'])
    print(f'{os.path.basename(path)} (US, all): {n} bytes')
    assert n <= 100 * MiB


@pytest.mark.parametrize('path, names, params', [
    (HOURLY, {'dataset': 'optionchan_tokyo'}, ['run_time:TIMESTAMP:2026-10-13T23:00:00Z']),
    (COMPARE, {'dataset': 'optionchan_tokyo'},
     ['start_at:TIMESTAMP:2026-10-13 00:00:00+00', 'end_at:TIMESTAMP:2026-10-14 00:00:00+00', 'legacy_bars:STRING:[]']),
    (IMPORT, {'dataset': 'optionchan_tokyo', 'import_dataset': 'optionchan_import'},
     ['start_at:TIMESTAMP:2018-01-01 00:00:00+00', 'end_at:TIMESTAMP:2026-10-12 22:00:00+00']),
    (CHECK, {'legacy_dataset': 'optionchan'},
     ['start_at:TIMESTAMP:2026-09-12 00:00:00+00', 'end_at:TIMESTAMP:2026-10-13 00:00:00+00']),
    (BACKFILL, {'dataset': 'optionchan_tokyo'},
     ['start_at:TIMESTAMP:2025-01-01 00:00:00+00', 'end_at:TIMESTAMP:2026-01-01 00:00:00+00']),
])
def test_dry_run_whole_script(path, names, params):
    """スクリプト全体（DECLARE・IF・CREATE・トランザクション・MERGE）の構文。スクリプトの dry run は構文だけを見る（表の有無は見ない）"""
    location = 'US' if path == CHECK else TOKYO
    assert bq(render(read(path), project=PROJECT, **names), location=location, dry_run=True, params=params) >= 0

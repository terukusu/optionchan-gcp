"""過去データの補正（#4）の SQL を BigQuery で実際に動かす検査（tests/test_sql_bq.py・tests/test_legacy_import_bq.py と同じ流儀）。

CI では動かさない（BigQuery の認証が要る）。手元で `OPTIONCHAN_BQ_TEST=1 OPTIONCHAN_BQ_PROJECT=<PROJECT_ID> python -m pytest -q tests/test_price_time_fix_bq.py`。
- 推定の固定値の検査: fix.sql の推定（`-- result:begin`〜`-- result:end`）の source を固定値の CTE に差し替え、関数と一緒に SELECT だけ
  実行する（テーブルを読まない。課金 0 バイト）。夜間の日跨ぎ・週末・祝日・月跨ぎ・年跨ぎ・前日以前の約定・表示が更新時刻より先の
  約定・同じ時刻と値段の別の約定・表示が消えた後・取り込みの空白・立会の外・重複・補正済みの行を確かめる
- 書き込みの試験: fix.sql（apply を 2 回）→ verify.sql → rollback.sql → verify.sql を、本番の表の代わりにスクリプトの一時テーブルに
  対して流す（本番の表には書かない）
- 実テーブルの dry run: スクリプト全体の構文と、UPDATE を 1 文ずつ（期間の絞り込み・列）
"""
import json
import os
import re
import subprocess

import pytest

from test_sql_bq import PROJECT, TOKYO, between, bq, cte, literal, read, render

pytestmark = pytest.mark.skipif(
    os.environ.get('OPTIONCHAN_BQ_TEST') != '1' or not PROJECT,
    reason='OPTIONCHAN_BQ_TEST=1 と OPTIONCHAN_BQ_PROJECT のときだけ',
)

ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
SQL_DIR = os.path.join(ROOT, 'sql', 'price_time_fix')
FIX = os.path.join(SQL_DIR, 'fix.sql')
VERIFY = os.path.join(SQL_DIR, 'verify.sql')
ROLLBACK = os.path.join(SQL_DIR, 'rollback.sql')
ACCURACY = os.path.join(SQL_DIR, 'accuracy.sql')
NAMES = {'dataset': 'optionchan_tokyo'}
VERSION = 'legacy-fix4-1'

# 休業日（取引カレンダーの版の行）: 2026-11-23（祝日取引なし）・年末年始は先物・オプションも休み。2026-10-12・11-03 は祝日取引
# （先物・オプションは取引日、東証（spot）は休み）
CLOSED_TRADING = ['2026-11-23', '2026-12-31', '2027-01-01']
CLOSED_CASH = CLOSED_TRADING + ['2026-10-12', '2026-11-03']
START = '2026-10-01 00:00:00+00'
END = '2027-01-10 00:00:00+00'


def dates(ds):
    return '[' + ', '.join(f"DATE '{d}'" for d in ds) + ']'


def functions():
    return between(read(FIX), '-- functions:begin', '-- functions:end').group(1)


# ---- 推定（固定値） ----

SRC = [('tbl', 'STRING'), ('series', 'STRING'), ('last_trading_day', 'DATE'), ('type', 'INT64'), ('target_price', 'INT64'),
       ('contract_month', 'DATE'), ('created_at', 'TIMESTAMP'), ('price_time', 'DATETIME'), ('price', 'FLOAT64'),
       ('volume', 'INT64'), ('price_time_raw', 'STRING'), ('parser_version', 'STRING')]


def row(strike, c, b, price=10.0, volume=1, tbl='option_price', raw=None, version=None):
    """c: スナップショットの JST（'2026-10-13 10:46'）、b: 旧系の保存値（JST の DATETIME。None は表示なし）"""
    if tbl == 'option_price':
        series = f'2026-12-10|1|{strike}'
        keys = {'last_trading_day': '2026-12-10', 'type': 1, 'target_price': strike}
    elif tbl == 'future_price':
        series, keys, volume = '2026-12-01', {'contract_month': '2026-12-01'}, None
    else:
        series, keys, volume = '', {}, None
    return {'tbl': tbl, 'series': series, **keys, 'created_at': f'{c}:00+09', 'price_time': b and f'{b}:00',
            'price': price, 'volume': volume, 'price_time_raw': raw, 'parser_version': version}


def run_estimate(rows):
    result = between(read(FIX), '-- result:begin', '-- result:end').group(1)
    m = between(result, '-- source:begin', '-- source:end')
    result = result[:m.start()] + cte('src', SRC, rows) + result[m.end():]
    sql = (
        f"DECLARE start_at TIMESTAMP DEFAULT TIMESTAMP '{START}';\n"
        f"DECLARE end_at TIMESTAMP DEFAULT TIMESTAMP '{END}';\n"
        "DECLARE ctx_start TIMESTAMP DEFAULT TIMESTAMP_SUB(start_at, INTERVAL 100 DAY);\n"
        f"DECLARE fix_version STRING DEFAULT '{VERSION}';\n"
        f'DECLARE closed_trading ARRAY<DATE> DEFAULT {dates(CLOSED_TRADING)};\n'
        f'DECLARE closed_cash ARRAY<DATE> DEFAULT {dates(CLOSED_CASH)};\n'
        + functions() + render(result, project=PROJECT, **NAMES)
    )
    out = {}
    for r in bq(sql):
        key = (r['tbl'], r['target_price'] and int(r['target_price']), r['created_at'][:16])
        assert key not in out, key
        out[key] = r
    return out


def utc(c):
    """JST 'YYYY-MM-DD HH:MM' → bq の JSON の created_at（UTC）の先頭 16 文字"""
    from datetime import datetime, timedelta
    return (datetime.strptime(c, '%Y-%m-%d %H:%M') - timedelta(hours=9)).strftime('%Y-%m-%d %H:%M')


def got(out, strike, c, tbl='option_price'):
    r = out[(tbl, strike if tbl == 'option_price' else None, utc(c))]
    return r['new_price_time'][:16].replace('T', ' '), r['basis'], int(r['shift_days']), int(r['candidate_shift_days'])


# 曜日: 2026-10-09 金、10-12 月（スポーツの日、祝日取引）、10-13 火、10-16 金、10-17 土、10-19 月、10-20 火、
# 11-02 月、11-20 金、11-23 月（休業）、11-24 火、12-30 水、12-31 木（休業）、2027-01-01 金（休業）、01-04 月
CASES = [
    # 当日の約定（前の表示と違う → 約定の始まり）
    row(1000, '2026-10-13 10:45', '2026-10-13 10:30'),
    row(1000, '2026-10-13 10:46', '2026-10-13 10:40', price=11.0),
    # 夜間の日跨ぎ: 00:30 に 23:59 の約定（旧系も 1 日戻していて正しい）
    row(2000, '2026-10-13 23:58', '2026-10-13 23:50'),
    row(2000, '2026-10-14 00:30', '2026-10-13 23:59', price=11.0),
    # 週末: 土曜朝（金曜の夜間）の約定を月曜に表示（旧系は月曜の日付）
    row(3000, '2026-10-17 05:20', '2026-10-17 05:10'),
    row(3000, '2026-10-17 05:58', '2026-10-17 05:57', price=11.0),
    row(3000, '2026-10-19 08:46', '2026-10-19 05:57', price=11.0),
    # 前の取引日以前の約定を引き継ぐ: 土曜 05:27 の約定を火曜に表示。月曜の夜間にも 05:27 はあるが、表示は土曜から変わっていない
    row(4000, '2026-10-17 05:20', '2026-10-17 05:10'),
    row(4000, '2026-10-17 05:28', '2026-10-17 05:27', price=11.0),
    row(4000, '2026-10-20 10:00', '2026-10-20 05:27', price=11.0),
    # 休業日（11-23）を跨ぐ: 金曜 13:50 の後の約定を火曜に初めて表示 → 11-23 は休み、土日も無いので金曜 14:00
    row(6000, '2026-11-20 13:50', '2026-11-20 13:40'),
    row(6000, '2026-11-24 10:00', '2026-11-23 14:00', price=11.0),
    # 系列の最初（文脈が無い）: 同じ表示でも前の表示が無いので一意に決まらない → 元のまま（候補は金曜で 3 日）
    row(6500, '2026-11-24 10:00', '2026-11-23 14:00'),
    # 月跨ぎ: 月曜朝に 14:00 の約定 → 日曜（11-01）・土曜は日中が無いので金曜 10-30
    row(7000, '2026-10-30 13:50', '2026-10-30 13:40'),
    row(7000, '2026-11-02 09:00', '2026-11-01 14:00', price=11.0),
    # 年跨ぎ: 1/4 に 15:45 の約定 → 1/3 日・1/2 土・1/1・12/31 休業 → 12/30。05:30 の約定 → 12/31 05:30（12/30 の夜間）
    row(8000, '2026-12-30 15:40', '2026-12-30 15:30'),
    row(8000, '2027-01-04 08:50', '2027-01-03 15:45', price=11.0),
    row(8001, '2026-12-31 05:20', '2026-12-31 05:10'),
    row(8001, '2027-01-04 08:50', '2027-01-04 05:30', price=11.0),
    # 表示が更新時刻より先（05:26 の更新で 05:30 の約定。旧系は前日 05:30）→ 当日へ 1 日進める
    row(9000, '2026-10-13 05:20', '2026-10-13 05:19', tbl='future_price'),
    row(9000, '2026-10-13 05:26', '2026-10-12 05:30', price=11.0, tbl='future_price'),
    # 同じ時刻・値段の別の約定（取引高が増えた）は新しい約定。取引高が減った（取引日の切り替え）だけなら同じ約定
    row(10000, '2026-10-13 18:40', '2026-10-13 18:30'),
    row(10000, '2026-10-13 18:48', '2026-10-13 18:47', price=1.0, volume=1),
    row(10000, '2026-10-14 10:00', '2026-10-13 18:47', price=1.0, volume=1),
    row(10000, '2026-10-14 17:01', '2026-10-13 18:47', price=1.0, volume=0),
    row(10000, '2026-10-14 18:48', '2026-10-14 18:47', price=1.0, volume=1),
    # 表示が消えた後（取引日の切り替え）に同じ時刻・値段・取引高の約定 → 新しい約定
    row(11000, '2026-10-13 08:50', '2026-10-13 08:45'),
    row(11000, '2026-10-13 09:01', '2026-10-13 09:00', price=5.0),
    row(11000, '2026-10-14 17:01', None, price=None, volume=None),
    row(11000, '2026-10-15 09:30', '2026-10-15 09:00', price=5.0),
    # 取り込みの空白（前の行が 2 日前）: 前日の候補もありうる → gap（元のまま）
    row(12000, '2026-10-13 10:00', '2026-10-13 09:00'),
    row(12000, '2026-10-15 10:30', '2026-10-15 10:15', price=11.0),
    # 立会の外の時刻の表示（07:00）→ 候補が無いので元のまま
    row(13000, '2026-10-13 10:00', '2026-10-13 07:00'),
    # 休業日（12/31）の取得で直前の約定 → 元のまま。1/4 の同じ表示の行も元のまま（始まりの日付に揃えない）
    row(14000, '2026-12-31 10:00', '2026-12-31 09:59'),
    row(14000, '2027-01-04 09:00', '2027-01-03 09:59'),
    # 重複行（同じスナップショット・同じ表示）は 1 行にまとめて n = 2
    row(16000, '2026-10-13 11:00', '2026-10-13 10:59'),
    row(16000, '2026-10-13 11:00', '2026-10-13 10:59'),
    # spot: 系列の最初で元のまま。候補は東証の休業日（祝日取引の日）を飛ばして金曜 10-09（3 日）
    row(None, '2026-10-12 16:00', '2026-10-12 15:45', price=40000.0, tbl='spot_price'),
    row(None, '2026-10-13 08:50', '2026-10-12 15:45', price=40000.0, tbl='spot_price'),
    # 範囲の前（文脈）の行は返さない
    row(18000, '2026-09-30 10:00', '2026-09-30 09:00'),
]


@pytest.fixture(scope='module')
def estimated():
    return run_estimate(CASES)


@pytest.mark.parametrize('strike, c, expected', [
    (1000, '2026-10-13 10:46', ('2026-10-13 10:40', 'determined', 0, 0)),
    (2000, '2026-10-14 00:30', ('2026-10-13 23:59', 'determined', 0, 0)),
    (3000, '2026-10-17 05:58', ('2026-10-17 05:57', 'determined', 0, 0)),
    (3000, '2026-10-19 08:46', ('2026-10-17 05:57', 'determined', 2, 2)),
    (4000, '2026-10-20 10:00', ('2026-10-17 05:27', 'determined', 3, 3)),
    (6000, '2026-11-24 10:00', ('2026-11-20 14:00', 'determined', 3, 3)),
    (6500, '2026-11-24 10:00', ('2026-11-23 14:00', 'series_start', 0, 3)),
    (7000, '2026-11-02 09:00', ('2026-10-30 14:00', 'determined', 2, 2)),
    (8000, '2027-01-04 08:50', ('2026-12-30 15:45', 'determined', 4, 4)),
    (8001, '2027-01-04 08:50', ('2026-12-31 05:30', 'determined', 4, 4)),
    (10000, '2026-10-14 10:00', ('2026-10-13 18:47', 'determined', 0, 0)),
    (10000, '2026-10-14 17:01', ('2026-10-13 18:47', 'determined', 0, 0)),
    (10000, '2026-10-14 18:48', ('2026-10-14 18:47', 'determined', 0, 0)),
    (11000, '2026-10-15 09:30', ('2026-10-15 09:00', 'determined', 0, 0)),
    (12000, '2026-10-15 10:30', ('2026-10-15 10:15', 'gap', 0, 0)),
    (13000, '2026-10-13 10:00', ('2026-10-13 07:00', 'no_session', 0, 0)),
    (14000, '2026-12-31 10:00', ('2026-12-31 09:59', 'off_session', 0, 0)),
    (14000, '2027-01-04 09:00', ('2027-01-03 09:59', 'off_session', 0, 3)),
])
def test_estimate_option(estimated, strike, c, expected):
    assert got(estimated, strike, c) == expected


def test_estimate_future_ahead_of_page_time(estimated):
    assert got(estimated, None, '2026-10-13 05:26', 'future_price') == ('2026-10-13 05:30', 'determined', -1, -1)


def test_estimate_spot_uses_cash_calendar(estimated):
    assert got(estimated, None, '2026-10-12 16:00', 'spot_price') == ('2026-10-12 15:45', 'series_start', 0, 3)
    assert got(estimated, None, '2026-10-13 08:50', 'spot_price') == ('2026-10-12 15:45', 'series_start', 0, 3)


def test_estimate_rows(estimated):
    # 重複は 1 行（n = 2）、表示の無い行・範囲の前の行は返さない
    assert int(estimated[('option_price', 16000, utc('2026-10-13 11:00'))]['n']) == 2
    assert ('option_price', 11000, utc('2026-10-14 17:01')) not in estimated
    assert not [k for k in estimated if k[1] == 18000]
    assert len(estimated) == len(CASES) - 3


def test_estimate_marked_rows_use_raw():
    """補正済みの行は price_time_raw（元の値）から同じ推定をし、今の値と違えば n_mismatch に数える"""
    rows = [
        row(4000, '2026-10-17 05:20', '2026-10-17 05:10', raw='2026-10-17 05:10:00', version=VERSION),
        row(4000, '2026-10-17 05:28', '2026-10-17 05:27', price=11.0, raw='2026-10-17 05:27:00', version=VERSION),
        # 補正済み（正しい）
        row(4000, '2026-10-20 10:00', '2026-10-17 05:27', price=11.0, raw='2026-10-20 05:27:00', version=VERSION),
        # 補正済み（違う値になっている）
        row(4000, '2026-10-20 10:01', '2026-10-20 05:27', price=11.0, raw='2026-10-20 05:27:00', version=VERSION),
        # 未補正
        row(4000, '2026-10-20 10:02', '2026-10-20 05:27', price=11.0),
    ]
    out = run_estimate(rows)
    for c, mismatch, unmarked in (('2026-10-20 10:00', 0, 0), ('2026-10-20 10:01', 1, 0), ('2026-10-20 10:02', 0, 1)):
        r = out[('option_price', 4000, utc(c))]
        assert r['new_price_time'][:16] == '2026-10-17T05:27', c
        assert r['b'][:16] == '2026-10-20T05:27', c
        assert (int(r['n_mismatch']), int(r['n_unmarked'])) == (mismatch, unmarked), c


# ---- 書き込み（一時テーブル） ----
# 本番の表の代わりにスクリプトの一時テーブルを作り、fix.sql（apply を 2 回）→ verify.sql → rollback.sql → verify.sql を同じスクリプトで
# 流す。参照の `${project}.${dataset}.<表>` を `<表>` に、DECLARE ... DEFAULT を SET にする（DECLARE はスクリプトの先頭にまとめる）。

TYPES = {'INTEGER': 'INT64', 'FLOAT': 'FLOAT64', 'BOOLEAN': 'BOOL'}
FIXED_AT = '2026-10-21 00:00:00+00'


def schema_cols(path):
    return [(c['name'], TYPES.get(c['type'], c['type'])) for c in json.loads(read(os.path.join(ROOT, path)))]


def temp_table(name, cols, rows, partition=None):
    struct = ', '.join(f'{c} {t}' for c, t in cols)
    values = ', '.join('STRUCT(' + ', '.join(literal(r.get(c), t) for c, t in cols) + ')' for r in rows)
    opts = f'\nPARTITION BY DATE({partition})' if partition else ''
    return f'CREATE TEMP TABLE {name}{opts} AS SELECT * FROM UNNEST(ARRAY<STRUCT<{struct}>>[{values}]);\n'


def to_temp(text):
    text = re.sub(r'`\$\{project\}\.\$\{dataset\}\.(\w+)`', r'\1', text)
    declares = re.findall(r'^DECLARE (\w+) (\S+) DEFAULT (.*?);$', text, re.M | re.S)
    text = re.sub(r'^DECLARE (\w+) (\S+) DEFAULT (.*?);$', r'SET \1 = \3;', text, flags=re.M | re.S)
    return text, [(n, t) for n, t, _ in declares]


def tokyo_row(r, run_id, version=None, raw=None):
    return {**r, 'ingest_run_id': run_id, 'parser_version': version, 'price_time_raw': raw}


def flow_tables(other_version=False):
    def opt(strike, c, b, price=10, volume=1, run='legacy'):
        rid = f"legacy-{c.replace('-', '').replace(' ', 'T').replace(':', '')}" if run == 'legacy' else run
        return tokyo_row({'type': 1, 'target_price': strike, 'is_atm': False, 'price': price, 'price_time': b and f'{b}:00',
                          'volume': volume, 'last_trading_day': '2026-12-10', 'created_at': f'{c}:00+09'}, rid)

    option = [
        opt(4000, '2026-10-17 05:20', '2026-10-17 05:10'),
        opt(4000, '2026-10-17 05:28', '2026-10-17 05:27', price=11),
        opt(4000, '2026-10-20 10:00', '2026-10-20 05:27', price=11),
        opt(4000, '2026-10-20 10:00', '2026-10-20 05:27', price=11),        # 重複行（2 行とも直す）
        opt(4000, '2026-10-20 10:01', None, price=None, volume=None),       # 表示なし（触らない）
        opt(4000, '2026-10-21 10:00', '2026-10-21 05:27', price=11),        # @fixed_at 以降（正しい値。触らない）
        opt(5000, '2026-10-20 10:00', '2026-10-18 14:00', run='u-new'),       # 新系の行（触らない）
    ]
    if other_version:   # 別の版の補正が残っている行
        option.append({**opt(6000, '2026-10-20 10:00', '2026-10-20 09:00'), 'parser_version': 'legacy-fix4-0',
                       'price_time_raw': '2026-10-20 09:00:00'})
    spot = [tokyo_row({'price': 39990.0, 'price_time': '2026-10-09 15:29:00', 'created_at': '2026-10-09 15:30:00+09'},
                      'legacy-20261009T063000Z'),     # 系列の最初（元のまま）
            tokyo_row({'price': 40000.0, 'price_time': '2026-10-09 15:45:00', 'created_at': '2026-10-09 15:46:00+09'},
                      'legacy-20261009T064600Z'),
            tokyo_row({'price': 40000.0, 'price_time': '2026-10-12 15:45:00', 'created_at': '2026-10-12 16:00:00+09'},
                      'legacy-20261012T070000Z'),
            tokyo_row({'price': 40000.0, 'price_time': '2026-10-12 15:45:00', 'created_at': '2026-10-13 08:50:00+09'},
                      'legacy-20261012T235000Z')]
    future = [tokyo_row({'price': 39000.0, 'price_time': '2026-10-13 05:19:00', 'contract_month': '2026-12-01',
                         'created_at': '2026-10-13 05:20:00+09'}, 'legacy-20261012T202000Z'),
              tokyo_row({'price': 39010.0, 'price_time': '2026-10-12 05:30:00', 'contract_month': '2026-12-01',
                         'created_at': '2026-10-13 05:26:00+09'}, 'legacy-20261012T202600Z')]
    runs = [{'ingest_run_id': 'u-new', 'snapshot_at': '2026-10-22 00:00:00+00'}]    # 範囲の外の新系の記録
    calendar = [{'calendar_date': d, 'is_trading_day': d not in CLOSED_TRADING} for d in CLOSED_CASH]
    tokyo = {t: schema_cols(f'schema/tokyo/{t}.json') for t in ('spot_price', 'future_price', 'option_price', 'ingest_runs')}
    return (
        temp_table('trading_calendar_latest', [('calendar_date', 'DATE'), ('is_trading_day', 'BOOL')], calendar)
        + temp_table('ingest_runs', tokyo['ingest_runs'], runs, 'snapshot_at')
        + temp_table('spot_price', tokyo['spot_price'], spot, 'created_at')
        + temp_table('future_price', tokyo['future_price'], future, 'created_at')
        + temp_table('option_price', tokyo['option_price'], option, 'created_at')
    )


def run_script(script, params):
    cmd = ['bq', f'--project_id={PROJECT}', f'--location={TOKYO}', 'query', '--use_legacy_sql=false', '--format=json',
           '--nouse_cache', '--maximum_bytes_billed=1073741824', *(f'--parameter={p}' for p in params)]
    out = subprocess.run(cmd, input=script, capture_output=True, text=True, timeout=900)
    assert out.returncode == 0, '\n'.join(
        line for line in (out.stdout + out.stderr).splitlines() if 'error' in line.lower() or 'at [' in line)
    return [r for r in json.loads(out.stdout[out.stdout.index('['):]) if isinstance(r, list)]


def strip_functions(text):
    """同じスクリプトで同名の一時関数は 2 回作れないので、2 つ目以降の SQL からは関数を除く"""
    return text.replace(between(text, '-- functions:begin', '-- functions:end').group(1), '')


def test_fix_apply_is_idempotent_verifiable_and_reversible():
    fix, d1 = to_temp(read(FIX))
    verify, d2 = to_temp(read(VERIFY))
    rollback, d3 = to_temp(read(ROLLBACK))
    declares = ''.join(f'DECLARE {n} {t};\n' for n, t in dict(d1 + d2 + d3).items())
    verify = strip_functions(verify)
    script = (declares + flow_tables() + fix + '\nDROP TABLE est;\n' + strip_functions(fix) + '\n' + verify + '\n'
              + rollback + '\n' + verify)
    params = [f'start_at:TIMESTAMP:{START}', f'end_at:TIMESTAMP:{FIXED_AT}', f'fixed_at:TIMESTAMP:{FIXED_AT}',
              'mode:STRING:apply', 'require_calendar:BOOL:false']
    first, second, verified, restored, verified_after = run_script(script, params)

    def updated(rows):
        r = rows[0]
        return int(r['updated_spot']), int(r['updated_future']), int(r['updated_option'])

    # 1 回目: 一意に決まる（determined）行だけ。spot 3・future 1・option 3（重複を含む）。系列の最初の行（series_start）・
    # 表示なし・@fixed_at 以降・新系の行は書かない
    assert updated(first) == (3, 1, 3)
    pending = {(r['tbl'], r['basis'], int(r['candidate_shift_days'])): int(r['rows_pending']) for r in first}
    assert pending[('option_price', 'determined', 3)] == 2
    assert pending[('future_price', 'determined', -1)] == 1
    assert pending[('spot_price', 'determined', 3)] == 2
    kept = {(r['tbl'], r['basis']): int(r['rows_kept']) for r in first if int(r['rows_kept'])}
    assert kept == {('spot_price', 'series_start'): 1, ('future_price', 'series_start'): 1,
                    ('option_price', 'series_start'): 1}
    # 2 回目: 何も書かない（推定は元の値から同じに計算され、今の値と一致する）
    assert updated(second) == (0, 0, 0)
    assert sum(int(r['rows_pending']) for r in second) == 0
    assert sum(int(r['rows_mismatch']) for r in second) == 0
    checks = ('out_of_scope_rows', 'bad_raw_rows', 'future_rows', 'sunday_changed_rows', 'off_calendar_rows',
              'spot_closed_rows', 'shift_out_of_range_rows')
    by_tbl = {r['tbl']: r for r in verified}
    for tbl, shifted, unmarked, new in (('spot_price', 2, 1, 0), ('future_price', 1, 1, 0), ('option_price', 2, 1, 1)):
        r = by_tbl[tbl]
        assert {k: int(r[k]) for k in checks} == dict.fromkeys(checks, 0), tbl
        assert (int(r['shifted_rows']), int(r['unmarked_rows']), int(r['new_rows'])) == (shifted, unmarked, new), tbl
    assert int(by_tbl['future_price']['forward_rows']) == 1
    # 切り戻し: 補正した行がすべて元の値に戻り、未補正の状態になる
    r = restored[0]
    assert (int(r['restored_spot']), int(r['restored_future']), int(r['restored_option'])) == (3, 1, 3)
    by_tbl = {r['tbl']: r for r in verified_after}
    for tbl, legacy in (('spot_price', 4), ('future_price', 2), ('option_price', 4)):
        r = by_tbl[tbl]
        assert (int(r['legacy_rows']), int(r['unmarked_rows']), int(r['shifted_rows'])) == (legacy, legacy, 0), tbl


def test_fix_dryrun_does_not_write():
    fix, d1 = to_temp(read(FIX))
    verify, d2 = to_temp(read(VERIFY))
    declares = ''.join(f'DECLARE {n} {t};\n' for n, t in dict(d1 + d2).items())
    params = [f'start_at:TIMESTAMP:{START}', f'end_at:TIMESTAMP:{FIXED_AT}', f'fixed_at:TIMESTAMP:{FIXED_AT}',
              'mode:STRING:dryrun', 'require_calendar:BOOL:false']
    summary, verified = run_script(declares + flow_tables() + fix + '\n' + strip_functions(verify), params)
    assert sum(int(r['rows_pending']) for r in summary) == 7
    assert sum(int(r['rows_kept']) for r in summary) == 3
    assert all(int(r['updated_option']) == 0 for r in summary)
    assert sum(int(r['unmarked_rows']) for r in verified) == 10


@pytest.mark.parametrize('params, message, other_version', [
    ({'end_at': '2026-10-22 00:00:00+00'}, 'is after fixed_at', False),
    ({'mode': 'write'}, 'mode must be dryrun or apply', False),
    ({'require_calendar': 'true', 'start_at': '2026-02-01 00:00:00+00'}, 'trading calendar starts at', False),
    # 年始の休業日（1/1〜1/3）の行が無い年（2026 年。一時テーブルのカレンダーは 2026-10 以降の休業日だけ）
    ({'require_calendar': 'true'}, 'trading calendar has no new-year rows (1/1-1/3) for years [2026]', False),
    ({}, '1 rows corrected by another version (not legacy-fix4-1)', True),
])
def test_fix_guards(params, message, other_version):
    fix, d1 = to_temp(read(FIX))
    declares = ''.join(f'DECLARE {n} {t};\n' for n, t in d1)
    p = {'start_at': START, 'end_at': FIXED_AT, 'fixed_at': FIXED_AT, 'mode': 'dryrun', 'require_calendar': 'false', **params}
    types = {'start_at': 'TIMESTAMP', 'end_at': 'TIMESTAMP', 'fixed_at': 'TIMESTAMP', 'mode': 'STRING', 'require_calendar': 'BOOL'}
    cmd = ['bq', f'--project_id={PROJECT}', f'--location={TOKYO}', 'query', '--use_legacy_sql=false', '--format=json',
           '--nouse_cache', '--maximum_bytes_billed=1073741824', *(f'--parameter={k}:{types[k]}:{v}' for k, v in p.items())]
    out = subprocess.run(cmd, input=declares + flow_tables(other_version) + fix, capture_output=True, text=True, timeout=600)
    assert out.returncode != 0
    assert message in out.stdout + out.stderr


# ---- 実テーブルの dry run ----

def fix_params(start='2026-10-01 00:00:00+00', end='2026-10-12 03:25:00+00', mode='dryrun'):
    return [f'start_at:TIMESTAMP:{start}', f'end_at:TIMESTAMP:{end}', f'fixed_at:TIMESTAMP:{end}', f'mode:STRING:{mode}',
            'require_calendar:BOOL:true']


@pytest.mark.parametrize('path, params', [
    (FIX, fix_params()),
    (FIX, fix_params(mode='apply')),
    (VERIFY, fix_params()[:3]),
    (ROLLBACK, fix_params()[:2]),
    (ACCURACY, ['ctx_start:TIMESTAMP:2026-10-12 23:00:00+00', 'start_at:TIMESTAMP:2026-10-13 06:00:00+00',
                'end_at:TIMESTAMP:2026-10-14 00:00:00+00']),
])
def test_dry_run_whole_script(path, params):
    """スクリプト全体の構文（スクリプトの dry run は構文だけを見る。書き込みはしない）"""
    assert bq(render(read(path), project=PROJECT, **NAMES), dry_run=True, params=params) >= 0


def test_dry_run_update_statements():
    """fix.sql・rollback.sql の UPDATE を 1 文ずつ本番の表に対して dry run する（require_partition_filter の表で期間の絞り込みが
    効くこと、列の名前と型）。est は推定の結果と同じ列の空の表で置き換える"""
    est = ("(SELECT * FROM UNNEST(ARRAY<STRUCT<tbl STRING, last_trading_day DATE, type INT64, target_price INT64, "
           "contract_month DATE, created_at TIMESTAMP, b DATETIME, price FLOAT64, volume INT64, new_price_time DATETIME, "
           "basis STRING>>[]))")
    for path, n in ((FIX, 3), (ROLLBACK, 3)):
        text = render(read(path), project=PROJECT, **NAMES)
        stmts = re.findall(r'^ *(UPDATE .*?;)$', text, re.S | re.M)
        assert len(stmts) == n
        for sql in stmts:
            sql = sql.replace('FROM est AS e', f'FROM {est} AS e').replace('fix_version', f"'{VERSION}'")
            sql = re.sub(r'\b(start_at|end_at|fixed_at)\b', r'@\1', sql)
            assert bq(sql, dry_run=True, params=fix_params()[:3]) >= 0, sql[:80]


def test_dry_run_verify_bytes():
    """検証の読み込みの見積もり（東京の 3 表の 5 列。移行の後は旧系の全期間で 約 30GiB の見込み。今は新系の分だけ）。
    立会の判定の一時関数はスクリプトにしか書けず、スクリプトの dry run は見積もりを返さないので、表を読む CTE だけを見積もる"""
    text = between(read(VERIFY), '-- source:begin', '-- source:end').group(1).rstrip().rstrip(',')
    sql = 'WITH ' + re.sub(r'\b(start_at|end_at)\b', r'@\1', render(text, project=PROJECT, **NAMES)) + '\nSELECT COUNT(*) FROM rows_all'
    n = bq(sql, dry_run=True, params=fix_params('2018-01-01 00:00:00+00', '2026-10-12 03:25:00+00')[:2])
    print(f'verify (all years): {n} bytes')
    assert n >= 0

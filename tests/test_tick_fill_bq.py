"""過去データの補完（#111）の SQL を BigQuery で実際に動かす検査（tests/test_price_time_fix_bq.py と同じ流儀）。

CI では動かさない（BigQuery の認証が要る）。手元で `OPTIONCHAN_BQ_TEST=1 OPTIONCHAN_BQ_PROJECT=<PROJECT_ID> python -m pytest -q tests/test_tick_fill_bq.py`。
入力はすべて合成した値（DataCube のデータは使わない。行使価格・値段・数量は架空の値）。
- 推定の固定値の検査: fill.sql の推定（`-- result:begin`〜`-- result:end`）の source（tgt・tick）を固定値の CTE に差し替え、関数と一緒に
  SELECT だけ実行する（テーブルを読まない。課金 0 バイト）。日中・夜間の日跨ぎ・16:30 の取引日の切り替わり・週末・祝日取引の日中と夜間・
  決まらない・合う約定が無い・約定が遅い・同時刻の約定・プット・2022-07 以前の約定日・歩み値の無い取引日・重複行を確かめる
- 書き込みの試験: fill.sql（apply を 2 回）→ verify.sql → rollback.sql → verify.sql を、本番の表の代わりにスクリプトの一時テーブルに
  対して流す（本番の表には書かない）。ガード（範囲・mode・カレンダー・別の版・歩み値の重複・歩み値の無い取引日）で止まること
- 実テーブルの dry run: スクリプト全体の構文と、UPDATE を 1 文ずつ（期間の絞り込み・列）。dc_option_tick がまだ無い（Terraform の
  apply の前）ときは、その参照を同じ列の空の表に置き換える
"""
import json
import os
import re
import subprocess

import pytest

from test_price_time_fix_bq import run_script, schema_cols, strip_functions, temp_table, to_temp
from test_sql_bq import PROJECT, TOKYO, between, bq, cte, read, render

pytestmark = pytest.mark.skipif(
    os.environ.get('OPTIONCHAN_BQ_TEST') != '1' or not PROJECT,
    reason='OPTIONCHAN_BQ_TEST=1 と OPTIONCHAN_BQ_PROJECT のときだけ',
)

ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
SQL_DIR = os.path.join(ROOT, 'sql', 'tick_fill')
FILL = os.path.join(SQL_DIR, 'fill.sql')
VERIFY = os.path.join(SQL_DIR, 'verify.sql')
ROLLBACK = os.path.join(SQL_DIR, 'rollback.sql')
ACCURACY = os.path.join(SQL_DIR, 'accuracy.sql')
NAMES = {'dataset': 'optionchan_tokyo'}
VERSION = 'legacy-fill111-1'
GAP_START = '2023-04-25 04:42:00+00'
GAP_END = '2023-05-08 07:31:00+00'

# 取引カレンダーの行（平日の休業日・祝日取引の日）: 2023-01-02・03 は休業、2023-05-03〜05 は祝日取引（先物・オプションは取引日、
# 現物は休み。この間の取引日（trade_date）は 05-08）
CALENDAR = {'2023-01-02': False, '2023-01-03': False, '2023-05-03': True, '2023-05-04': True, '2023-05-05': True}
CLOSED_TRADING = [d for d, t in CALENDAR.items() if not t]
CLOSED_CASH = list(CALENDAR)
LTD = '2023-06-09'      # 限月（2023-06）。値は架空


def dates(ds):
    return '[' + ', '.join(f"DATE '{d}'" for d in ds) + ']'


def functions():
    return between(read(FILL), '-- functions:begin', '-- functions:end').group(1)


# ---- 推定（固定値） ----

TGT = [('created_at', 'TIMESTAMP'), ('last_trading_day', 'DATE'), ('type', 'INT64'), ('target_price', 'INT64'),
       ('volume', 'INT64'), ('price', 'INT64'), ('price_time', 'DATETIME'), ('diff', 'INT64'), ('parser_version', 'STRING')]
TICK = [('trade_date', 'DATE'), ('execution_date', 'DATE'), ('execution_time', 'TIME'), ('contract_month', 'STRING'),
        ('put_call_type', 'INT64'), ('exercise_price', 'NUMERIC'), ('trade_price', 'NUMERIC'), ('trade_volume', 'INT64'),
        ('seq_no', 'INT64')]


def snap(strike, c, volume, typ=1, ltd=LTD, price=None, price_time=None, diff=None, version=None):
    """c: スナップショットの JST（'2023-04-26 09:15'）"""
    return {'created_at': f'{c}:00+09', 'last_trading_day': ltd, 'type': typ, 'target_price': strike, 'volume': volume,
            'price': price, 'price_time': price_time, 'diff': diff, 'parser_version': version}


def tick(strike, trade_date, at, price, volume, no, pc=2, cm='202306', execution_date=None):
    """at: 約定日時（JST、'2023-04-26 09:10:00.000'）。execution_date を渡すと約定日をその値にする（2022-07 以前の Make_Date）"""
    d, t = at.split(' ')
    return {'trade_date': trade_date, 'execution_date': execution_date or d, 'execution_time': t, 'contract_month': cm,
            'put_call_type': pc, 'exercise_price': strike, 'trade_price': price, 'trade_volume': volume, 'seq_no': no}


TICKS = [
    # 日中: 3 本（数量 2・1・5）
    tick(1000, '2023-04-26', '2023-04-26 09:00:00.000', 100, 2, 1),
    tick(1000, '2023-04-26', '2023-04-26 09:10:30.500', 110, 1, 2),
    tick(1000, '2023-04-26', '2023-04-26 09:20:00.000', 120, 5, 3),
    # 夜間の日跨ぎ（取引日 04-27）
    tick(2000, '2023-04-27', '2023-04-26 23:59:30.000', 50, 1, 1),
    tick(2000, '2023-04-27', '2023-04-27 00:01:10.000', 55, 2, 2),
    # 16:30 の切り替わり: 04-26 の日中と、04-27 の取引日の夜間（累計が戻る）
    tick(3000, '2023-04-26', '2023-04-26 15:00:00.000', 200, 4, 1),
    tick(3000, '2023-04-27', '2023-04-26 16:31:00.000', 210, 1, 1),
    # 週末: 金曜（04-28）の夜間の土曜 02:00 の約定は取引日 05-01（月）
    tick(4000, '2023-05-01', '2023-04-29 02:00:00.000', 300, 3, 1),
    # 祝日取引: 05-02 の夜間・05-03 の日中・05-03 の夜間（取引日はすべて 05-08）
    tick(5000, '2023-05-08', '2023-05-02 20:00:00.000', 400, 2, 1),
    tick(5000, '2023-05-08', '2023-05-03 10:00:00.000', 410, 3, 2),
    tick(5000, '2023-05-08', '2023-05-03 17:00:00.000', 420, 1, 3),
    # 祝日取引の夜間で決まらない: 夜間の累計で合う約定（05-02 20:00）より後に祝日の日中の約定（05-03 11:00）がある
    tick(6000, '2023-05-08', '2023-05-02 20:00:00.000', 500, 2, 1),
    tick(6000, '2023-05-08', '2023-05-03 11:00:00.000', 510, 1, 2),
    # 同時刻の約定はシーケンス No 順
    tick(7000, '2023-04-26', '2023-04-26 10:00:00.000', 601, 1, 6),
    tick(7000, '2023-04-26', '2023-04-26 10:00:00.000', 600, 1, 5),
    # プット（option_price.type = 2 ↔ put_call_type = 1）。同じ行使価格のコールは数量が違う
    tick(8000, '2023-04-26', '2023-04-26 11:00:00.000', 700, 2, 1, pc=1),
    tick(8000, '2023-04-26', '2023-04-26 11:01:00.000', 999, 3, 1, pc=2),
    # 値段が整数でない約定（no_match として数える）
    tick(9000, '2023-04-26', '2023-04-26 12:00:00.000', 12.5, 1, 1),
    # 2022-07 以前: 夜間 02:00 の約定の約定日（Make_Date）は前日（夜間の始まりの日）
    tick(9500, '2022-07-13', '2022-07-13 02:00:00.000', 800, 1, 1, cm='202209', execution_date='2022-07-12'),
]

CASES = [
    snap(1000, '2023-04-26 09:15', 3),
    snap(1000, '2023-04-26 09:25', 8),
    snap(1000, '2023-04-26 09:16', 4),                   # 累計に 4 が無い → no_match
    snap(1000, '2023-04-26 08:55', 8),                   # 合う約定（09:20）が + 10 分より後 → late_trade
    snap(2000, '2023-04-27 00:05', 3),
    snap(3000, '2023-04-26 16:29', 4),
    snap(3000, '2023-04-26 16:32', 1),
    snap(4000, '2023-04-29 05:00', 3),                   # 土曜の朝（取引日 05-01）
    snap(4000, '2023-05-01 08:50', 3),                   # 月曜の日中の前（取引日 05-01）
    snap(5000, '2023-05-03 10:30', 5),                   # 祝日の日中: 全累計（2 + 3）
    snap(5000, '2023-05-03 18:00', 3),                   # 祝日の夜間: 日中を除いた累計（2 + 1）
    snap(6000, '2023-05-03 17:00', 2),                   # 祝日の夜間: 決まらない
    snap(7000, '2023-04-26 10:01', 1),
    snap(7000, '2023-04-26 10:02', 2),
    snap(8000, '2023-04-26 11:05', 2, typ=2),
    snap(9000, '2023-04-26 12:01', 1),
    snap(9500, '2022-07-13 02:30', 1, ltd='2022-09-09'),
    snap(1000, '2023-05-02 10:00', 1),                   # 取引日 05-02 の歩み値が無い → no_tick_day
    snap(1000, '2023-04-26 09:30', 8),                   # 重複行（同じキー 2 行）
    snap(1000, '2023-04-26 09:30', 8),
    # 補完済みの行も同じ推定に入る
    snap(1000, '2023-04-26 09:31', 8, price=120, price_time='2023-04-26 09:20:00', version=VERSION),
]


def run_estimate(snaps, ticks):
    result = between(read(FILL), '-- result:begin', '-- result:end').group(1)
    m = between(result, '-- source:begin', '-- source:end')
    result = result[:m.start()] + cte('tgt', TGT, snaps) + cte('tick', TICK, ticks) + result[m.end():]
    sql = (
        f"DECLARE fill_version STRING DEFAULT '{VERSION}';\n"
        f'DECLARE closed_trading ARRAY<DATE> DEFAULT {dates(CLOSED_TRADING)};\n'
        f'DECLARE closed_cash ARRAY<DATE> DEFAULT {dates(CLOSED_CASH)};\n'
        + functions() + render(result, project=PROJECT, **NAMES)
    )
    out = {}
    for r in bq(sql):
        out.setdefault((int(r['target_price']), r['created_at'][:16]), []).append(r)
    return out


def utc(c):
    """JST 'YYYY-MM-DD HH:MM' → bq の JSON の created_at（UTC）の先頭 16 文字"""
    from datetime import datetime, timedelta
    return (datetime.strptime(c, '%Y-%m-%d %H:%M') - timedelta(hours=9)).strftime('%Y-%m-%d %H:%M')


def got(out, strike, c):
    [r, *_] = out[(strike, utc(c))]
    return (r['basis'], r['new_price'] and int(r['new_price']),
            r['new_price_time'] and r['new_price_time'][:16].replace('T', ' '), r['trade_day'])


@pytest.fixture(scope='module')
def estimated():
    return run_estimate(CASES, TICKS)


@pytest.mark.parametrize('strike, c, expected', [
    (1000, '2023-04-26 09:15', ('matched', 110, '2023-04-26 09:10', '2023-04-26')),
    (1000, '2023-04-26 09:25', ('matched', 120, '2023-04-26 09:20', '2023-04-26')),
    (1000, '2023-04-26 09:16', ('no_match', None, None, '2023-04-26')),
    (1000, '2023-04-26 08:55', ('late_trade', None, None, '2023-04-26')),
    (2000, '2023-04-27 00:05', ('matched', 55, '2023-04-27 00:01', '2023-04-27')),
    (3000, '2023-04-26 16:29', ('matched', 200, '2023-04-26 15:00', '2023-04-26')),
    (3000, '2023-04-26 16:32', ('matched', 210, '2023-04-26 16:31', '2023-04-27')),
    (4000, '2023-04-29 05:00', ('matched', 300, '2023-04-29 02:00', '2023-05-01')),
    (4000, '2023-05-01 08:50', ('matched', 300, '2023-04-29 02:00', '2023-05-01')),
    (5000, '2023-05-03 10:30', ('matched', 410, '2023-05-03 10:00', '2023-05-08')),
    (5000, '2023-05-03 18:00', ('matched', 420, '2023-05-03 17:00', '2023-05-08')),
    (6000, '2023-05-03 17:00', ('ambiguous_holiday', None, None, '2023-05-08')),
    (7000, '2023-04-26 10:01', ('matched', 600, '2023-04-26 10:00', '2023-04-26')),
    (7000, '2023-04-26 10:02', ('matched', 601, '2023-04-26 10:00', '2023-04-26')),
    (8000, '2023-04-26 11:05', ('matched', 700, '2023-04-26 11:00', '2023-04-26')),
    (9000, '2023-04-26 12:01', ('no_match', None, None, '2023-04-26')),
    (9500, '2022-07-13 02:30', ('matched', 800, '2022-07-13 02:00', '2022-07-13')),
    (1000, '2023-05-02 10:00', ('no_tick_day', None, None, '2023-05-02')),
    (1000, '2023-04-26 09:31', ('matched', 120, '2023-04-26 09:20', '2023-04-26')),
])
def test_estimate(estimated, strike, c, expected):
    assert got(estimated, strike, c) == expected


def test_estimate_rows(estimated):
    # 対象の行ごとに 1 行（重複行は 2 行とも同じ値）。補完済みの行は今の値と印をそのまま返す
    assert sum(len(v) for v in estimated.values()) == len(CASES)
    dup = estimated[(1000, utc('2023-04-26 09:30'))]
    assert len(dup) == 2 and {(r['basis'], r['new_price']) for r in dup} == {('matched', '120')}
    [marked] = estimated[(1000, utc('2023-04-26 09:31'))]
    assert (marked['parser_version'], marked['price']) == (VERSION, '120')


def test_estimate_holiday_night_without_later_day_trade():
    """祝日の夜間でも、祝日の日中の約定が照合した約定より前なら決まる（日中を除いた累計）"""
    ticks = [tick(5000, '2023-05-08', '2023-05-03 10:00:00.000', 410, 3, 1),
             tick(5000, '2023-05-08', '2023-05-03 17:00:00.000', 420, 2, 2)]
    out = run_estimate([snap(5000, '2023-05-03 17:30', 2), snap(5000, '2023-05-03 17:31', 5)], ticks)
    assert got(out, 5000, '2023-05-03 17:30') == ('matched', 420, '2023-05-03 17:00', '2023-05-08')
    # 夜間に全累計（3 + 2）は使わない
    assert got(out, 5000, '2023-05-03 17:31')[0] == 'no_match'


# ---- 書き込み（一時テーブル） ----
# 本番の表の代わりにスクリプトの一時テーブルを作り、fill.sql（apply を 2 回）→ verify.sql → rollback.sql → verify.sql を同じスクリプトで
# 流す。参照の `${project}.${dataset}.<表>` を `<表>` に、DECLARE ... DEFAULT を SET にする（DECLARE はスクリプトの先頭にまとめる）。

def option_row(strike, c, volume, run='legacy', typ=1, price=None, price_time=None, diff=None, version=None):
    rid = f"legacy-{c.replace('-', '').replace(' ', 'T').replace(':', '')}" if run == 'legacy' else run
    return {'type': typ, 'target_price': strike, 'is_atm': False, 'price': price, 'price_time': price_time, 'diff': diff,
            'volume': volume, 'last_trading_day': LTD, 'created_at': f'{c}:00+09', 'ingest_run_id': rid,
            'parser_version': version}


def flow_option(other_version=False, missing_day=False):
    rows = [
        option_row(1000, '2023-04-26 09:15', 3, diff=10),          # 書く（110）
        option_row(1000, '2023-04-26 09:25', 8, diff=20),          # 書く（120）
        option_row(1000, '2023-04-26 09:25', 8, diff=20),          # 重複行（2 行とも書く）
        option_row(1000, '2023-04-26 09:26', None),                # 表示が空（volume NULL。触らない）
        option_row(1000, '2023-04-26 09:16', 4),                   # no_match（残す）
        option_row(5000, '2023-05-03 18:00', 3),                   # 祝日の夜間（書く、420）
        option_row(6000, '2023-05-03 17:00', 2),                   # 決まらない（残す）
        option_row(1000, '2023-04-26 09:27', 8, run='u-new'),      # 新系の行（触らない）
        option_row(1000, '2023-05-08 16:40', 1),                   # 欠け 2 の後の行（範囲の外。触らない）
    ]
    if other_version:   # 別の版の補完が残っている行
        rows.append(option_row(1000, '2023-04-26 09:40', 8, price=120, price_time='2023-04-26 09:20:00',
                               version='legacy-fill111-0'))
    if missing_day:     # 取引日 05-02 の歩み値が無い
        rows.append(option_row(1000, '2023-05-02 10:00', 1))
    return rows


def flow_ticks(duplicate=False):
    rows = [{**t, 'index_type': '18', 'security_code': f"1{t['put_call_type']}{t['exercise_price']:07d}",
             'price_type': 'N', 'cash_futures_type': 0}
            for t in TICKS if t['trade_date'] >= '2023-04-25' and isinstance(t['trade_price'], int)]
    # 日経225 以外の指数・Weekly の限月の行（同じ行使価格・種別・取引日）は読まない
    rows.append({**rows[0], 'index_type': '19', 'trade_volume': 7, 'seq_no': 90})
    rows.append({**rows[0], 'contract_month': '202318', 'trade_volume': 7, 'seq_no': 91})
    if duplicate:
        rows.append(dict(rows[0]))
    return rows


def flow_tables(other_version=False, missing_day=False, duplicate=False, calendar=CALENDAR):
    cal = [{'calendar_date': d, 'is_trading_day': t} for d, t in calendar.items()]
    return (
        temp_table('trading_calendar_latest', [('calendar_date', 'DATE'), ('is_trading_day', 'BOOL')], cal)
        + temp_table('option_price', schema_cols('schema/tokyo/option_price.json'),
                     flow_option(other_version, missing_day), 'created_at')
        + temp_table('dc_option_tick', schema_cols('schema/tokyo/dc_option_tick.json'), flow_ticks(duplicate))
    )


def params(start=GAP_START, end=GAP_END, mode='apply'):
    return [f'start_at:TIMESTAMP:{start}', f'end_at:TIMESTAMP:{end}', f'mode:STRING:{mode}']


VERIFY_ZERO = ('out_of_scope_rows', 'null_marked_rows', 'future_rows', 'off_session_rows', 'wrong_trade_day_rows',
               'volume_null_changed_rows', 'base_inconsistent_rows')


def test_fill_apply_is_idempotent_verifiable_and_reversible():
    fill, d1 = to_temp(read(FILL))
    verify, d2 = to_temp(read(VERIFY))
    rollback, d3 = to_temp(read(ROLLBACK))
    declares = ''.join(f'DECLARE {n} {t};\n' for n, t in dict(d1 + d2 + d3).items())
    verify = strip_functions(verify)
    script = (declares + flow_tables() + verify + '\n' + strip_functions(fill) + '\nDROP TABLE est;\n'
              + strip_functions(fill) + '\n' + verify + '\n' + rollback + '\n' + verify)
    # 関数は最初の verify の前に 1 回だけ作る
    script = script.replace(verify, functions() + verify, 1)
    before, first, second, verified, restored, after = run_script(script, params())

    by_basis = {r['basis']: r for r in first}
    # 1 回目: matched の未補完の行だけ（110・120 × 2・420 の 4 行）。no_match・決まらない行は残す
    assert {b: (int(r['rows_total']), int(r['rows_pending']), int(r['rows_kept'])) for b, r in by_basis.items()} == {
        'matched': (4, 4, 0), 'no_match': (1, 0, 1), 'ambiguous_holiday': (1, 0, 1)}
    assert {int(r['updated_option']) for r in first} == {4}
    # 前日比のある書く行（diff 10・20 × 2）は「約定値 − 前日比」= 100 で一定
    assert (int(by_basis['matched']['rows_base_checked']), int(by_basis['matched']['rows_base_inconsistent'])) == (3, 0)
    # 2 回目: 何も書かない（補完済みの行は同じ推定で、今の値と一致する）
    assert {int(r['updated_option']) for r in second} == {0}
    assert sum(int(r['rows_pending']) for r in second) == 0
    assert sum(int(r['rows_mismatch']) for r in second) == 0

    [v0], [v1], [v2] = before, verified, after
    assert {k: int(v1[k]) for k in VERIFY_ZERO} == dict.fromkeys(VERIFY_ZERO, 0)
    assert (int(v0['marked_rows']), int(v0['target_rows'])) == (0, 6)
    assert (int(v1['marked_rows']), int(v1['target_rows']), int(v1['base_checked_rows'])) == (4, 2, 3)
    for k in ('volume_null_rows', 'new_rows'):
        assert int(v0[k]) == int(v1[k]) == int(v2[k]), k
    assert (int(v0['volume_null_rows']), int(v0['new_rows'])) == (1, 1)
    # 切り戻し: 印のある行がすべて NULL に戻る
    assert int(restored[0]['restored_option']) == 4
    assert (int(v2['marked_rows']), int(v2['target_rows'])) == (0, 6)


def test_fill_dryrun_does_not_write():
    fill, d1 = to_temp(read(FILL))
    verify, d2 = to_temp(read(VERIFY))
    declares = ''.join(f'DECLARE {n} {t};\n' for n, t in dict(d1 + d2).items())
    summary, [verified] = run_script(declares + flow_tables() + fill + '\n' + strip_functions(verify),
                                     params(mode='dryrun'))
    assert sum(int(r['rows_pending']) for r in summary) == 4
    assert sum(int(r['rows_kept']) for r in summary) == 2
    assert {int(r['updated_option']) for r in summary} == {0}
    assert (int(verified['marked_rows']), int(verified['target_rows'])) == (0, 6)


def run_guard(tables, p):
    fill, d1 = to_temp(read(FILL))
    declares = ''.join(f'DECLARE {n} {t};\n' for n, t in d1)
    cmd = ['bq', f'--project_id={PROJECT}', f'--location={TOKYO}', 'query', '--use_legacy_sql=false', '--format=json',
           '--nouse_cache', '--maximum_bytes_billed=1073741824', *(f'--parameter={x}' for x in p)]
    out = subprocess.run(cmd, input=declares + tables + fill, capture_output=True, text=True, timeout=600)
    return out.returncode, out.stdout + out.stderr


@pytest.mark.parametrize('kwargs, p, message', [
    ({}, params(start='2023-04-25 04:00:00+00', mode='dryrun'), 'tick_fill: outside the gap (#111)'),
    ({}, params(end='2023-05-08 08:00:00+00', mode='dryrun'), 'tick_fill: outside the gap (#111)'),
    ({}, params(mode='write'), 'mode must be dryrun or apply'),
    ({'calendar': {'2024-01-02': False}}, params(mode='dryrun'), 'trading calendar starts at'),
    ({'calendar': {'2022-01-03': False, '2023-05-03': True}}, params(mode='dryrun'),
     'trading calendar has no new-year rows (1/1-1/3) for years [2023]'),
    ({'other_version': True}, params(), '1 rows filled by another version (not legacy-fill111-1)'),
    ({'duplicate': True}, params(), 'dc_option_tick has 1 duplicated (trade_date, security_code, seq_no)'),
    ({'missing_day': True}, params(), 'dc_option_tick has no rows for trade days [2023-05-02]'),
])
def test_fill_guards(kwargs, p, message):
    code, text = run_guard(flow_tables(**kwargs), p)
    assert code != 0
    assert message in text


def test_fill_guard_missing_day_writes_nothing():
    """歩み値の無い取引日があれば、apply でも UPDATE の前に止まる（同じスクリプトの後の SELECT は流れない）"""
    code, text = run_guard(flow_tables(missing_day=True), params())
    assert code != 0 and 'updated_option' not in text


# ---- 実テーブルの dry run ----

def tick_exists():
    out = subprocess.run(['bq', f'--project_id={PROJECT}', 'show', '--format=json', f"{PROJECT}:{NAMES['dataset']}.dc_option_tick"],
                         capture_output=True, text=True, timeout=120)
    return out.returncode == 0


def real(text):
    """本番の名前にする。dc_option_tick がまだ無いときは、同じ列の空の表に置き換える（option_price などは本番の表）"""
    text = render(text, project=PROJECT, **NAMES)
    if not tick_exists():
        cols = schema_cols('schema/tokyo/dc_option_tick.json')
        empty = '(SELECT * FROM UNNEST(ARRAY<STRUCT<' + ', '.join(f'{c} {t}' for c, t in cols) + '>>[]))'
        text = text.replace(f"`{PROJECT}.{NAMES['dataset']}.dc_option_tick`", empty)
    return text


@pytest.mark.parametrize('path, p', [
    (FILL, params(mode='dryrun')),
    (FILL, params(mode='apply')),
    (VERIFY, params()[:2]),
    (ROLLBACK, params()[:2]),
    (ACCURACY, ['start_at:TIMESTAMP:2023-05-08 07:31:00+00', 'end_at:TIMESTAMP:2023-05-31 07:30:00+00']),
])
def test_dry_run_whole_script(path, p):
    """スクリプト全体の構文（スクリプトの dry run は構文だけを見る。書き込みはしない）"""
    assert bq(real(read(path)), dry_run=True, params=p) >= 0


def test_dry_run_update_statements():
    """fill.sql・rollback.sql の UPDATE を本番の表に対して dry run する（require_partition_filter の表で期間の絞り込みが効くこと、
    列の名前と型）。est は推定の結果と同じ列の空の表で置き換える"""
    est = ("(SELECT * FROM UNNEST(ARRAY<STRUCT<created_at TIMESTAMP, last_trading_day DATE, type INT64, target_price INT64, "
           "volume INT64, new_price INT64, new_price_time DATETIME, basis STRING>>[]))")
    for path in (FILL, ROLLBACK):
        [sql] = re.findall(r'^ *(UPDATE .*?;)$', real(read(path)), re.S | re.M)
        sql = sql.replace('FROM est WHERE', f'FROM {est} WHERE').replace('fill_version', f"'{VERSION}'")
        sql = re.sub(r'\b(start_at|end_at)\b', r'@\1', sql)
        assert bq(sql, dry_run=True, params=params()[:2]) >= 0, sql[:80]


def test_dry_run_source_bytes():
    """推定の入力（source の CTE）の読み込みの見積もり（欠け 2 の範囲・正解の範囲）。スクリプトの dry run は見積もりを返さないので、
    表を読む CTE だけを見積もる（tick_from・tick_to は範囲から計算した値）"""
    for path, start, end in ((FILL, GAP_START, GAP_END), (ACCURACY, '2023-05-08 07:31:00+00', '2023-05-31 07:30:00+00')):
        src = between(between(read(path), '-- result:begin', '-- result:end').group(1),
                      '-- source:begin', '-- source:end').group(1).rstrip().rstrip(',')
        src = re.sub(r'\b(start_at|end_at)\b', r'@\1', real(src)).replace('fill_version', f"'{VERSION}'")
        src = src.replace('tick_from', "DATE(@start_at, 'Asia/Tokyo')").replace(
            'tick_to', "DATE_ADD(DATE(@end_at, 'Asia/Tokyo'), INTERVAL 14 DAY)")
        sql = 'WITH ' + src + '\nSELECT (SELECT COUNT(*) FROM tgt), (SELECT COUNT(*) FROM tick)'
        n = bq(sql, dry_run=True, params=[f'start_at:TIMESTAMP:{start}', f'end_at:TIMESTAMP:{end}'])
        print(f'{os.path.basename(path)} source: {n} bytes')
        assert 0 <= n < 2 * 1024 ** 3


def test_no_datacube_values_in_fixtures():
    """固定値は架空の値（DataCube の行を写していない）: 行使価格は 1000〜9500 の区切りの値、値段は 1000 未満"""
    assert {t['exercise_price'] for t in TICKS} <= {1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 9500}
    assert all(t['trade_price'] < 1000 for t in TICKS)
    assert json.dumps(TICKS)  # 直列化できる（値は Python の固定値だけ）

"""過去データの行の作成（#114）の SQL を BigQuery で実際に動かす検査（tests/test_tick_fill_bq.py と同じ流儀）。

CI では動かさない（BigQuery の認証が要る）。手元で `OPTIONCHAN_BQ_TEST=1 OPTIONCHAN_BQ_PROJECT=<PROJECT_ID> python -m pytest -q tests/test_tick_build_bq.py`。
入力はすべて合成した値（DataCube のデータは使わない。行使価格・値段・数量は架空の値）。
- 作り方の固定値の検査: build.sql の作り方（`-- result:begin`〜`-- result:end`）の source（opt_tick・fut_tick・cand）を固定値の CTE に
  差し替え、関数と一緒に SELECT だけ実行する（テーブルを読まない。課金 0 バイト）。日中・夜間の日跨ぎ（残存の数え方）・限月の切り替わり・
  祝日取引の日中と夜間・約定の無い分・ATM（同じ距離）・裁定の範囲の外・原資産の先物がまだ無い・offset（その日・前の日と合わせる・無いので 0）・
  夜間の寄り付きの先物より先の約定・2022-07 以前の約定日を確かめる。IV は Python で別に計算した Black-76 と比べる
- 書き込みの試験: build.sql（apply を 2 回）→ verify.sql → rollback.sql → verify.sql を、本番の表の代わりにスクリプトの一時テーブルに
  対して流す（本番の表には書かない）。ガード（範囲・mode・カレンダー・別の版・歩み値の重複・月の入れ忘れ・取引日の欠け）で止まること
- 実テーブルの dry run: スクリプト全体の構文。dc_future_tick がまだ無い（Terraform の apply の前）ときは、その参照を同じ列の空の表に置き換える
"""
import json
import math
import os
import re
import subprocess
from datetime import date, datetime, timedelta

import pytest

from test_price_time_fix_bq import schema_cols, temp_table
from test_sql_bq import PROJECT, TOKYO, between, bq, cte, read, render

pytestmark = pytest.mark.skipif(
    os.environ.get('OPTIONCHAN_BQ_TEST') != '1' or not PROJECT,
    reason='OPTIONCHAN_BQ_TEST=1 と OPTIONCHAN_BQ_PROJECT のときだけ',
)

ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
SQL_DIR = os.path.join(ROOT, 'sql', 'tick_build')
BUILD = os.path.join(SQL_DIR, 'build.sql')
VERIFY = os.path.join(SQL_DIR, 'verify.sql')
ROLLBACK = os.path.join(SQL_DIR, 'rollback.sql')
ACCURACY = os.path.join(SQL_DIR, 'accuracy.sql')
NAMES = {'dataset': 'optionchan_tokyo'}
VERSION = 'legacy-fill114-1'

# 取引カレンダーの行: 2023-03-21 は祝日取引（先物・オプションは取引、現物は休み。この日の取引日（trade_date）は 03-22）。
# 年始の行は missing_new_year の検査のため
CALENDAR = {'2022-01-03': False, '2023-01-02': False, '2023-01-03': False, '2023-03-21': True}
CLOSED_TRADING = [d for d, t in CALENDAR.items() if not t]
CLOSED_CASH = list(CALENDAR)
# 限月（このカレンダーで）: 202303 の SQ 03-10・最終取引日 03-09、202304 は 04-14・04-13、202208 は 08-12・08-11
SQ = {'202303': date(2023, 3, 10), '202304': date(2023, 4, 14), '202305': date(2023, 5, 12), '202208': date(2022, 8, 12)}
LTD = {'202303': '2023-03-09', '202304': '2023-04-13', '202305': '2023-05-11', '202208': '2022-08-11'}


def dates(ds):
    return '[' + ', '.join(f"DATE '{d}'" for d in ds) + ']'


def functions():
    return between(read(BUILD), '-- functions:begin', '-- functions:end').group(1)


# ---- Black-76 の IV（Python で別に計算する） ----

def ncdf(x):
    return 0.5 * math.erfc(-x / math.sqrt(2))


def black76(is_call, f, k, t, sigma):
    st = sigma * math.sqrt(t)
    d1 = (math.log(f / k) + 0.5 * st * st) / st
    d2 = d1 - st
    return f * ncdf(d1) - k * ncdf(d2) if is_call else k * ncdf(-d2) - f * ncdf(-d1)


def iv(is_call, price, f, k, days):
    t = days / 365
    lo, hi = 1e-9, 5.0
    for _ in range(200):
        mid = (lo + hi) / 2
        if black76(is_call, f, k, t, mid) < price:
            lo = mid
        else:
            hi = mid
    return round((lo + hi) / 2 * 100, 2)


# ---- 作り方（固定値） ----

OPT = [('trade_date', 'DATE'), ('execution_date', 'DATE'), ('execution_time', 'TIME'), ('contract_month', 'STRING'),
       ('put_call_type', 'INT64'), ('exercise_price', 'NUMERIC'), ('trade_price', 'NUMERIC'), ('trade_volume', 'INT64'),
       ('seq_no', 'INT64')]
FUT = [('trade_date', 'DATE'), ('execution_date', 'DATE'), ('execution_time', 'TIME'), ('contract_month', 'STRING'),
       ('trade_price', 'NUMERIC'), ('seq_no', 'INT64')]
CALL, PUT = 2, 1   # DataCube の put_call_type


def opt(trade_date, at, cm, pc, strike, price, volume, no, execution_date=None):
    """at: 約定日時（JST、'2023-03-08 09:01:30.000'）。execution_date を渡すと約定日をその値にする（2022-07 以前の Make_Date）"""
    d, t = at.split(' ')
    return {'trade_date': trade_date, 'execution_date': execution_date or d, 'execution_time': t, 'contract_month': cm,
            'put_call_type': pc, 'exercise_price': strike, 'trade_price': price, 'trade_volume': volume, 'seq_no': no}


def fut(trade_date, at, cm, price, no, execution_date=None):
    d, t = at.split(' ')
    return {'trade_date': trade_date, 'execution_date': execution_date or d, 'execution_time': t, 'contract_month': cm,
            'trade_price': price, 'seq_no': no}


def parity(trade_date, at, strike, s, f, no):
    """プット・コール・パリティのサンプルが s（= K + C − P − F）になるコールとプットの組（202304、F は原資産の先物の値）"""
    diff = s + f - strike   # C − P
    c = max(diff, 0) + 100
    return [opt(trade_date, at, '202304', CALL, strike, c, 1, no), opt(trade_date, at, '202304', PUT, strike, c - diff, 1, no + 1)]


FUTS = [
    fut('2023-03-08', '2023-03-08 09:00:00.000', '202303', 27000, 1),
    fut('2023-03-08', '2023-03-08 09:05:00.000', '202303', 27100, 2),
    fut('2023-03-08', '2023-03-08 09:07:10.000', '202303', 27050, 3),
    fut('2023-03-08', '2023-03-08 09:00:00.000', '202306', 27200, 1),
    # 夜間（取引日 03-09）: 202306 の寄り付きはオプション（16:30:00.000）より後
    fut('2023-03-09', '2023-03-08 16:30:00.000', '202303', 27300, 1),
    fut('2023-03-09', '2023-03-08 16:30:00.050', '202306', 27250, 1),
    # 03-09 の夜間（取引日 03-10。202303 は満期で、直近の四半期限月は 202306）
    fut('2023-03-10', '2023-03-09 16:30:00.000', '202306', 27400, 1),
    # 祝日取引（取引日 03-22）: 03-20 の夜間・03-21 の日中・03-21 の夜間
    fut('2023-03-22', '2023-03-20 16:30:00.000', '202306', 27000, 1),
    fut('2023-03-22', '2023-03-21 09:00:00.000', '202306', 27010, 2),
    fut('2023-03-22', '2023-03-21 16:30:00.000', '202306', 27020, 3),
    # 2022-07 以前: 夜間 01:59 の約定の約定日（Make_Date）は前日
    fut('2022-07-13', '2022-07-12 01:59:00.000', '202209', 26500, 1),
]
OPTS = [
    # 原資産の先物の約定がまだ無い（202303 の最初の先物は 09:00）
    opt('2023-03-08', '2023-03-08 08:59:00.000', '202303', CALL, 27500, 50, 1, 1),
    opt('2023-03-08', '2023-03-08 09:01:30.000', '202303', CALL, 27000, 300, 2, 1),
    opt('2023-03-08', '2023-03-08 09:03:00.000', '202303', CALL, 27000, 310, 1, 2),
    opt('2023-03-08', '2023-03-08 09:02:00.000', '202303', PUT, 27000, 280, 3, 1),
    # 裁定の範囲の外（F = 27000 で内在価値 1000 より安い）
    opt('2023-03-08', '2023-03-08 09:04:00.000', '202303', CALL, 26000, 900, 1, 1),
    opt('2023-03-08', '2023-03-08 09:08:00.000', '202303', CALL, 27100, 250, 1, 1),
    # 202304（原資産 202306）のパリティ: 03-08 は F ± 500 の 5 組（−40 × 3・−45 × 2 → 中央値 −40）と、範囲の外の 4 組（−100）
    *parity('2023-03-08', '2023-03-08 09:10:00.000', 27000, -40, 27200, 1),
    *parity('2023-03-08', '2023-03-08 09:10:00.000', 27125, -40, 27200, 1),
    *parity('2023-03-08', '2023-03-08 09:10:00.000', 27250, -40, 27200, 1),
    *parity('2023-03-08', '2023-03-08 09:10:00.000', 27375, -45, 27200, 1),
    *parity('2023-03-08', '2023-03-08 09:10:00.000', 27500, -45, 27200, 1),
    *parity('2023-03-08', '2023-03-08 09:10:00.000', 26500, -100, 27200, 1),
    *parity('2023-03-08', '2023-03-08 09:10:00.000', 26625, -100, 27200, 1),
    *parity('2023-03-08', '2023-03-08 09:10:00.000', 27750, -100, 27200, 1),
    *parity('2023-03-08', '2023-03-08 09:10:00.000', 27875, -100, 27200, 1),
    # 夜間（取引日 03-09）の寄り付き: 202306 の先物より先の約定は、前の取引日の最後の先物（27200）
    opt('2023-03-09', '2023-03-08 16:30:00.000', '202304', PUT, 27125, 330, 1, 3),
    # 夜間の日跨ぎ: 01:00 の約定の残存は前の日（03-08）から数える
    opt('2023-03-09', '2023-03-09 01:00:00.000', '202303', CALL, 27000, 420, 5, 1),
    # 03-09 のパリティは 2 組（−45）だけ → 10 日前からのサンプルと合わせて −45
    *parity('2023-03-09', '2023-03-09 10:00:00.000', 27250, -45, 27250, 5),
    *parity('2023-03-09', '2023-03-09 10:00:00.000', 27375, -45, 27250, 5),
    # 03-09 の夜間（取引日 03-10）: 近い 2 限月は 202304・202305
    opt('2023-03-10', '2023-03-09 16:35:00.000', '202304', CALL, 27250, 280, 1, 1),
    # 祝日取引（取引日 03-22）: 03-20 の夜間 2・03-21 の日中 3・03-21 の夜間 1
    opt('2023-03-22', '2023-03-20 20:00:00.000', '202304', CALL, 27000, 300, 2, 1),
    opt('2023-03-22', '2023-03-21 10:00:00.000', '202304', CALL, 27000, 310, 3, 2),
    opt('2023-03-22', '2023-03-21 17:00:00.000', '202304', CALL, 27000, 320, 1, 3),
    # 2022-07 以前: 夜間 02:00 の約定の約定日（Make_Date）は前日
    opt('2022-07-13', '2022-07-12 02:00:00.000', '202208', CALL, 26500, 200, 1, 1, execution_date='2022-07-12'),
]


def minutes(start, n):
    """JST 'YYYY-MM-DD HH:MM' から n 分（スナップショットの候補の created_at）"""
    t0 = datetime.strptime(start, '%Y-%m-%d %H:%M')
    return [(t0 + timedelta(minutes=i)).strftime('%Y-%m-%d %H:%M') for i in range(n)]


CAND = (minutes('2023-03-08 08:55', 35) + minutes('2023-03-08 16:25', 15) + minutes('2023-03-09 00:55', 10)
        + minutes('2023-03-09 09:55', 10) + minutes('2023-03-09 16:25', 15) + minutes('2023-03-20 19:55', 10)
        + minutes('2023-03-21 08:55', 10) + minutes('2023-03-21 09:55', 10) + minutes('2023-03-21 16:25', 10)
        + minutes('2023-03-21 16:55', 10) + minutes('2022-07-13 01:55', 10))


def run_build(opts, futs, cand, every_minute=False):
    result = between(read(BUILD), '-- result:begin', '-- result:end').group(1)
    m = between(result, '-- source:begin', '-- source:end')
    src = (cte('opt_tick', OPT, opts) + cte('fut_tick', FUT, futs)
           + cte('cand', [('c', 'TIMESTAMP')], [{'c': f'{c}:00+09'} for c in cand]))
    sql = (
        f"DECLARE every_minute BOOL DEFAULT {'TRUE' if every_minute else 'FALSE'};\n"
        f'DECLARE closed_trading ARRAY<DATE> DEFAULT {dates(CLOSED_TRADING)};\n'
        f'DECLARE closed_cash ARRAY<DATE> DEFAULT {dates(CLOSED_CASH)};\n'
        + functions() + render(result[:m.start()] + src + result[m.end():], project=PROJECT, **NAMES)
    )
    out = {}
    for r in bq(sql):
        c = jst(r['created_at'])
        if r['tbl'] == 'future':
            key = (c, 'future')
        else:
            key = (c, r['contract_month'][:7].replace('-', ''), int(r['type']), int(r['target_price']))
        assert key not in out, key
        out[key] = r
    return out


def jst(created_at):
    """bq の JSON の created_at（UTC）→ JST 'YYYY-MM-DD HH:MM'"""
    return (datetime.strptime(created_at[:16], '%Y-%m-%d %H:%M') + timedelta(hours=9)).strftime('%Y-%m-%d %H:%M')


@pytest.fixture(scope='module')
def built():
    return run_build(OPTS, FUTS, CAND)


C, P = 1, 2   # option_price.type


def row(out, c, cm, typ, strike):
    r = out[(c, cm, typ, strike)]
    return (int(r['price']) if float(r['price']).is_integer() else float(r['price']), r['price_time'][:16].replace('T', ' '),
            int(r['volume']), r['iv_basis'])


@pytest.mark.parametrize('c, cm, typ, strike, expected', [
    # 原資産の先物がまだ無い
    ('2023-03-08 09:00', '202303', C, 27500, (50, '2023-03-08 08:59', 1, 'no_future')),
    # 日中: 累計と最後の約定
    ('2023-03-08 09:02', '202303', C, 27000, (300, '2023-03-08 09:01', 2, 'ok')),
    ('2023-03-08 09:04', '202303', C, 27000, (310, '2023-03-08 09:03', 3, 'ok')),
    ('2023-03-08 09:04', '202303', P, 27000, (280, '2023-03-08 09:02', 3, 'ok')),
    ('2023-03-08 09:05', '202303', C, 26000, (900, '2023-03-08 09:04', 1, 'out_of_bounds')),
    # パリティの組（27250 のコールは 100）
    ('2023-03-08 09:11', '202304', C, 27250, (100, '2023-03-08 09:10', 1, 'ok')),
    # 夜間（取引日 03-09）: 累計は取引日ごと（03-08 の日中の約定は入らない）
    ('2023-03-08 16:31', '202304', P, 27125, (330, '2023-03-08 16:30', 1, 'ok')),
    ('2023-03-09 01:01', '202303', C, 27000, (420, '2023-03-09 01:00', 5, 'ok')),
    # 限月の切り替わり（取引日 03-10）
    ('2023-03-09 16:36', '202304', C, 27250, (280, '2023-03-09 16:35', 1, 'ok')),
    # 祝日取引: 夜間は祝日の日中を除く、祝日の日中は前の夜間からの全部
    ('2023-03-20 20:01', '202304', C, 27000, (300, '2023-03-20 20:00', 2, 'offset_zero')),
    ('2023-03-21 09:01', '202304', C, 27000, (300, '2023-03-20 20:00', 2, 'offset_zero')),
    ('2023-03-21 10:01', '202304', C, 27000, (310, '2023-03-21 10:00', 5, 'offset_zero')),
    ('2023-03-21 16:31', '202304', C, 27000, (300, '2023-03-20 20:00', 2, 'offset_zero')),
    ('2023-03-21 17:01', '202304', C, 27000, (320, '2023-03-21 17:00', 3, 'offset_zero')),
    # 2022-07 以前の約定日
    ('2022-07-13 02:01', '202208', C, 26500, (200, '2022-07-13 02:00', 1, 'offset_zero')),
])
def test_option_rows(built, c, cm, typ, strike, expected):
    assert row(built, c, cm, typ, strike) == expected


@pytest.mark.parametrize('c, cm, typ, strike, f, k, price, days', [
    ('2023-03-08 09:02', '202303', C, 27000, 27000, 27000, 300, 2),
    ('2023-03-08 09:04', '202303', P, 27000, 27000, 27000, 280, 2),
    # 202304: 原資産 202306（27200）+ offset −40（03-08 のサンプルの中央値）、残存 04-14 − 03-08
    ('2023-03-08 09:11', '202304', C, 27250, 27160, 27250, 100, 37),
    # 夜間の寄り付き: 前の取引日の最後の先物（27200）+ offset −45（03-09 は 2 組だけなので 03-08 と合わせた中央値）
    ('2023-03-08 16:31', '202304', P, 27125, 27155, 27125, 330, 37),
    # 01:00 の約定: 原資産は 16:30 の先物、残存は前の日（03-08）から数える
    ('2023-03-09 01:01', '202303', C, 27000, 27300, 27000, 420, 2),
    ('2023-03-09 16:36', '202304', C, 27250, 27355, 27250, 280, 36),
    # offset が決まらない（10 日前からのサンプルも無い）: 0 にする
    ('2023-03-21 10:01', '202304', C, 27000, 27010, 27000, 310, 24),
    ('2022-07-13 02:01', '202208', C, 26500, 26500, 26500, 200, 31),
])
def test_iv_matches_python(built, c, cm, typ, strike, f, k, price, days):
    r = built[(c, cm, typ, strike)]
    assert abs(float(r['iv']) - iv(typ == C, price, f, k, days)) <= 0.011


def test_iv_null(built):
    for key in [('2023-03-08 09:00', '202303', C, 27500), ('2023-03-08 09:05', '202303', C, 26000)]:
        assert built[key]['iv'] is None, key


def test_last_trading_day_and_months(built):
    assert {r['last_trading_day'] for k, r in built.items() if k[1] == '202303'} == {LTD['202303']}
    assert {r['last_trading_day'] for k, r in built.items() if k[1] == '202304'} == {LTD['202304']}
    # 取引日 03-10 のスナップショットに 202303 の行は無い（満期の後）
    assert not [k for k in built if k[0] == '2023-03-09 16:36' and k[1] == '202303']
    # 取引日 03-09 の行に 03-08 の日中の系列は入らない
    assert ('2023-03-08 16:31', '202303', C, 27000) not in built


def test_atm(built):
    def atm(c, cm):
        return sorted(k[3] for k, r in built.items() if k[0] == c and k[1:2] == (cm,) and r['is_atm'] == 'true')
    # 行使価格が 1 つだけなら、それが ATM
    assert atm('2023-03-08 09:01', '202303') == [27500]
    # F = 27000: 27000（コール・プット）
    assert atm('2023-03-08 09:04', '202303') == [27000, 27000]
    # F = 27050 で 27000 と 27100 が同じ距離 → 小さい方
    assert atm('2023-03-08 09:09', '202303') == [27000, 27000]
    # 202304: F = 27200 − 40 = 27160 → 27125
    assert atm('2023-03-08 09:11', '202304') == [27125, 27125]
    # 原資産の先物がまだ無いスナップショットは ATM なし
    assert atm('2023-03-08 09:00', '202303') == []


def test_future_rows(built):
    def f(c):
        r = built[(c, 'future')]
        return r['contract_month'], int(float(r['price'])), r['price_time'][:16].replace('T', ' ')
    assert f('2023-03-08 09:01') == ('2023-03-01', 27000, '2023-03-08 09:00')
    assert f('2023-03-08 09:06') == ('2023-03-01', 27100, '2023-03-08 09:05')
    assert f('2023-03-08 09:08') == ('2023-03-01', 27050, '2023-03-08 09:07')
    # 夜間（取引日 03-09）の直近の四半期限月は 202303（最終取引日 03-09）。取引日 03-10 からは 202306
    assert f('2023-03-08 16:31') == ('2023-03-01', 27300, '2023-03-08 16:30')
    assert f('2023-03-09 16:31') == ('2023-06-01', 27400, '2023-03-09 16:30')
    assert f('2022-07-13 02:00') == ('2022-09-01', 26500, '2022-07-13 01:59')
    # 原資産の先物がまだ無いスナップショットは future の行なし
    assert ('2023-03-08 09:00', 'future') not in built


def test_snapshots_only_on_trade_minutes(built):
    snaps = {k[0] for k in built}
    # 約定の無い分（09:20・09:12〜）は作らない。候補の外の約定の分（09:10 のパリティは 09:11）は作る
    assert '2023-03-08 09:20' not in snaps and '2023-03-08 09:12' not in snaps
    assert {'2023-03-08 09:00', '2023-03-08 09:01', '2023-03-08 09:02', '2023-03-08 09:11'} <= snaps
    # every_minute（accuracy.sql）はすべての分
    every = run_build(OPTS, FUTS, minutes('2023-03-08 09:10', 5), every_minute=True)
    assert {k[0] for k in every} == set(minutes('2023-03-08 09:10', 5))


# ---- 書き込み（一時テーブル） ----
# 本番の表の代わりにスクリプトの一時テーブルを作り、build.sql（apply を 2 回）→ verify.sql → rollback.sql → verify.sql を同じスクリプトで
# 流す。参照の `${project}.${dataset}.<表>` を `<表>` に、DECLARE ... DEFAULT を SET にする（DECLARE はスクリプトの先頭にまとめる）。

def to_temp(text):
    """test_price_time_fix_bq.to_temp と同じ（型に空白がある DECLARE（ARRAY<STRUCT<...>>）にも対応）"""
    text = re.sub(r'`\$\{project\}\.\$\{dataset\}\.(\w+)`', r'\1', text)
    pattern = r'^DECLARE (\w+) (.+?) DEFAULT (.*?);$'
    declares = re.findall(pattern, text, re.M | re.S)
    text = re.sub(pattern, r'SET \1 = \3;', text, flags=re.M | re.S)
    return text, [(n, t) for n, t, _ in declares]


def run_script(script, params):
    """複数の文のスクリプトを流し、SELECT の結果（空でない行の配列）を順に返す（BEGIN / COMMIT TRANSACTION の空の配列は除く）"""
    cmd = ['bq', f'--project_id={PROJECT}', f'--location={TOKYO}', 'query', '--use_legacy_sql=false', '--format=json',
           '--nouse_cache', '--maximum_bytes_billed=2147483648', *(f'--parameter={p}' for p in params)]
    out = subprocess.run(cmd, input=script, capture_output=True, text=True, timeout=900)
    # 失敗時は BigQuery のエラーの行だけを出す（ジョブの JSON にはアカウントのメールアドレスが入るので出さない）
    assert out.returncode == 0, '\n'.join(
        line for line in (out.stdout + out.stderr).splitlines() if 'error' in line.lower() or 'at [' in line)
    text = out.stdout[out.stdout.index('['):] if '[' in out.stdout else '[]'
    return [r for r in json.loads(text) if isinstance(r, list) and r]


def strip_functions(text):
    return text.replace(between(text, '-- functions:begin', '-- functions:end').group(1), '')


def legacy_run(c):
    """旧系のスナップショット（JST 'YYYY-MM-DD HH:MM'）の ingest_runs の行"""
    t = datetime.strptime(c, '%Y-%m-%d %H:%M') - timedelta(hours=9)
    rid = 'legacy-' + t.strftime('%Y%m%dT%H%M%SZ')
    at = t.strftime('%Y-%m-%d %H:%M:00+00')
    return {'ingest_run_id': rid, 'snapshot_at': at, 'source_object': 'legacy:optionchan', 'parser_version': 'legacy',
            'trigger': 'legacy_import', 'rows_spot': 1, 'rows_future': 1, 'rows_option': 1, 'started_at': at, 'finished_at': at}


# 旧系のスナップショット: 03-08 08:50 → 09:30（区間 A。先物の約定のある分 13）、09:30 → 10:00（約定なし）、10:00 → 10:20（区間 B。
# 約定のある分 2 → 埋めない）、10:20〜10:30 は 1 分ごと（区間なし）、03-31 の区間（歩み値の月の入れ忘れの試験）
LEGACY = (['2023-03-08 08:50', '2023-03-08 09:30', '2023-03-08 10:00', '2023-03-08 10:20']
          + minutes('2023-03-08 10:21', 10) + ['2023-03-14 09:00', '2023-03-14 09:40'])


def flow_futures(duplicate=False, missing_day=False, mixed=False):
    rows = [fut('2023-03-08', f'2023-03-08 09:{m:02d}:00.000', '202303', 27000 + 5 * m, m + 1) for m in range(13)]
    rows += [fut('2023-03-08', '2023-03-08 10:05:00.000', '202303', 27100, 20),
             fut('2023-03-08', '2023-03-08 10:06:00.000', '202303', 27100, 21),
             fut('2023-03-08', '2023-03-08 09:00:00.000', '202306', 27200, 1)]
    if missing_day:   # 03-14 は先物だけある（オプションの歩み値が無い）
        rows += [fut('2023-03-14', f'2023-03-14 09:{m:02d}:00.000', '202303', 27000, m + 1) for m in range(1, 13)]
    if mixed:         # 03-31 → 04-03 の区間（04 月は入れていない）
        rows += [fut('2023-03-31', f'2023-03-31 14:{m:02d}:00.000', '202306', 27000, m + 1) for m in range(1, 13)]
    rows = [{**r, 'index_type': '18', 'security_code': f"16{r['contract_month'][2:]}0018", 'price_type': 'N',
             'trade_volume': 1, 'sco_category': 0} for r in rows]
    if duplicate:
        rows.append(dict(rows[0]))
    return rows


def flow_options(duplicate=False):
    rows = [
        opt('2023-03-08', '2023-03-08 09:01:30.000', '202303', CALL, 27000, 300, 2, 1),
        opt('2023-03-08', '2023-03-08 09:03:00.000', '202303', CALL, 27000, 310, 1, 2),
        opt('2023-03-08', '2023-03-08 09:02:00.000', '202303', PUT, 27000, 280, 3, 1),
        opt('2023-03-08', '2023-03-08 09:40:00.000', '202303', PUT, 27000, 290, 1, 2),   # 区間の外（作らない）
    ]
    rows = [{**r, 'index_type': '18', 'security_code': f"1{r['put_call_type']}{r['exercise_price']:07d}", 'price_type': 'N',
             'cash_futures_type': 0} for r in rows]
    # 日経225 以外の指数・Weekly の限月の行（同じ行使価格・種別・取引日）は読まない
    rows.append({**rows[0], 'index_type': '20', 'trade_volume': 7, 'seq_no': 90})
    rows.append({**rows[0], 'contract_month': '202352', 'trade_volume': 7, 'seq_no': 91})
    if duplicate:
        rows.append(dict(rows[0]))
    return rows


def flow_tables(calendar=CALENDAR, runs_extra=(), **kwargs):
    cal = [{'calendar_date': d, 'is_trading_day': t} for d, t in calendar.items()]
    legacy = [legacy_run(c) for c in LEGACY] + [{**legacy_run('2023-03-31 14:00')}, {**legacy_run('2023-04-03 10:00')}]
    legacy_option = [{'type': 1, 'target_price': 27000, 'is_atm': True, 'price': 300, 'volume': 2, 'last_trading_day': '2023-03-09',
                      'created_at': r['snapshot_at'], 'ingest_run_id': r['ingest_run_id'], 'parser_version': 'legacy-fix4-1'}
                     for r in legacy]
    legacy_future = [{'price': 27000.0, 'contract_month': '2023-03-01', 'created_at': r['snapshot_at'],
                      'ingest_run_id': r['ingest_run_id']} for r in legacy]
    return (
        temp_table('trading_calendar_latest', [('calendar_date', 'DATE'), ('is_trading_day', 'BOOL')], cal)
        + temp_table('ingest_runs', schema_cols('schema/tokyo/ingest_runs.json'), legacy + list(runs_extra), 'snapshot_at')
        + temp_table('option_price', schema_cols('schema/tokyo/option_price.json'), legacy_option, 'created_at')
        + temp_table('future_price', schema_cols('schema/tokyo/future_price.json'), legacy_future, 'created_at')
        + temp_table('dc_option_tick', schema_cols('schema/tokyo/dc_option_tick.json'),
                     flow_options(kwargs.get('duplicate_option', False)))
        + temp_table('dc_future_tick', schema_cols('schema/tokyo/dc_future_tick.json'),
                     flow_futures(kwargs.get('duplicate_future', False), kwargs.get('missing_day', False), kwargs.get('mixed', False)))
    )


START = '2023-03-01 00:00:00+00'
END = '2023-03-10 00:00:00+00'


def params(start=START, end=END, mode='apply'):
    return [f'start_at:TIMESTAMP:{start}', f'end_at:TIMESTAMP:{end}', f'mode:STRING:{mode}']


VERIFY_ZERO = ('out_of_hole_runs', 'run_field_errors', 'run_count_errors', 'orphan_rows', 'row_field_errors', 'iv_range_rows',
               'future_trade_rows', 'duplicate_rows', 'atm_errors', 'month_errors')
DROP = 'DROP TABLE hole_stats;\nDROP TABLE built;\nDROP TABLE built_runs;\nDROP TABLE pending;\n'


def test_build_apply_is_idempotent_verifiable_and_reversible():
    build, d1 = to_temp(read(BUILD))
    verify, d2 = to_temp(read(VERIFY))
    rollback, d3 = to_temp(read(ROLLBACK))
    declares = ''.join(f'DECLARE {n} {t};\n' for n, t in dict(d1 + d2 + d3).items())
    script = (declares + flow_tables() + verify + '\n' + build + '\n' + DROP + strip_functions(build) + '\n' + verify + '\n'
              + rollback + '\n' + verify)
    before, first, second, verified, restored, after = run_script(script, params())

    # 区間 A（08:50 → 09:30）だけを埋める。スナップショットは先物の約定のある 13 分（09:01〜09:13）とオプションの約定の分（09:02・
    # 09:03・09:04 は先物と重なる）。区間 B（約定 2 分）は埋めない
    def total(rows, k):
        return sum(int(r[k]) for r in rows)
    assert {int(r['holes_built']) for r in first} == {1}
    assert {int(r['inserted_runs']) for r in first} == {13}
    assert total([r for r in first if r['tbl'] == 'future'], 'rows_pending') == 13
    # option: 09:02 にコール、09:03 からプット（09:02 の約定）、09:04 からコールの 2 本目 → 09:02〜09:13 の 12 スナップショット ×
    # コール + 09:03〜09:13 の 11 × プット = 23 行
    assert total([r for r in first if r['tbl'] == 'option'], 'rows_pending') == 23
    assert {int(r['inserted_option']) for r in first} == {23}
    assert {int(r['inserted_future']) for r in first} == {13}
    # 2 回目: 何も書かない
    assert {int(r['inserted_runs']) for r in second} == {0}
    assert total(second, 'rows_pending') == 0

    [v0], [v1], [v2] = before, verified, after
    assert {k: int(v1[k]) for k in VERIFY_ZERO} == dict.fromkeys(VERIFY_ZERO, 0)
    assert (int(v1['built_runs']), int(v1['built_option_rows']), int(v1['built_future_rows'])) == (13, 23, 13)
    assert int(v0['built_runs']) == 0 and int(v2['built_runs']) == 0
    for k in ('legacy_runs', 'other_runs'):
        assert int(v0[k]) == int(v1[k]) == int(v2[k]), k
    # 切り戻し: 作った行がすべて消える
    assert (int(restored[0]['deleted_option']), int(restored[0]['deleted_future']), int(restored[0]['deleted_runs'])) == (23, 13, 13)


def test_build_dryrun_does_not_write():
    build, d1 = to_temp(read(BUILD))
    verify, d2 = to_temp(read(VERIFY))
    declares = ''.join(f'DECLARE {n} {t};\n' for n, t in dict(d1 + d2).items())
    summary, [verified] = run_script(declares + flow_tables() + build + '\n' + verify, params(mode='dryrun'))
    assert sum(int(r['rows_pending']) for r in summary) == 36
    assert {int(r['inserted_runs']) for r in summary} == {0}
    assert int(verified['built_runs']) == 0


def run_guard(tables, p):
    build, d1 = to_temp(read(BUILD))
    declares = ''.join(f'DECLARE {n} {t};\n' for n, t in d1)
    cmd = ['bq', f'--project_id={PROJECT}', f'--location={TOKYO}', 'query', '--use_legacy_sql=false', '--format=json',
           '--nouse_cache', '--maximum_bytes_billed=1073741824', *(f'--parameter={x}' for x in p)]
    out = subprocess.run(cmd, input=declares + tables + build, capture_output=True, text=True, timeout=600)
    return out.returncode, ' '.join((out.stdout + out.stderr).split())


OTHER = {**legacy_run('2023-03-08 09:20'), 'ingest_run_id': 'legacy-fill114-20230308T002000Z', 'parser_version': 'legacy-fill114-0',
         'trigger': 'legacy_fill114', 'source_object': 'datacube:tick'}


@pytest.mark.parametrize('kwargs, p, message', [
    ({}, params(start='2018-09-30 14:00:00+00', mode='dryrun'), 'tick_build: outside the legacy range (#114)'),
    ({}, params(end='2024-08-06 04:00:00+00', mode='dryrun'), 'tick_build: outside the legacy range (#114)'),
    ({}, params(mode='write'), 'mode must be dryrun or apply'),
    ({'calendar': {'2024-01-02': False}}, params(mode='dryrun'), 'trading calendar starts at'),
    ({'calendar': {'2022-01-03': False, '2023-03-21': True}}, params(mode='dryrun'),
     'trading calendar has no new-year rows (1/1-1/3) for years [2023]'),
    ({'runs_extra': [OTHER]}, params(), '1 snapshots built by another version (not legacy-fill114-1)'),
    ({'duplicate_option': True}, params(), 'dc_option_tick has 1 duplicated (trade_date, security_code, seq_no)'),
    ({'duplicate_future': True}, params(), 'dc_future_tick has 1 duplicated (trade_date, security_code, seq_no)'),
    ({'missing_day': True}, params(end='2023-03-20 00:00:00+00'),
     'dc_option_tick or dc_future_tick has no rows for trade days [2023-03-14]'),
    ({'mixed': True}, params(end='2023-04-10 00:00:00+00'), 'span loaded and not'),   # bq はエラーを折り返す（not- loaded）
])
def test_build_guards(kwargs, p, message):
    code, text = run_guard(flow_tables(**kwargs), p)
    assert code != 0
    assert message in text


def test_build_guard_writes_nothing():
    """ガードで止まると、apply でも INSERT の前に止まる（同じスクリプトの後の SELECT は流れない）"""
    code, text = run_guard(flow_tables(mixed=True), params(end='2023-04-10 00:00:00+00'))
    assert code != 0 and 'inserted_runs' not in text


# ---- 実テーブルの dry run ----

def table_exists(name):
    out = subprocess.run(['bq', f'--project_id={PROJECT}', 'show', '--format=json', f"{PROJECT}:{NAMES['dataset']}.{name}"],
                         capture_output=True, text=True, timeout=120)
    return out.returncode == 0


def real(text):
    """本番の名前にする。dc_future_tick がまだ無いときは、同じ列の空の表に置き換える（option_price などは本番の表）"""
    text = render(text, project=PROJECT, **NAMES)
    if not table_exists('dc_future_tick'):
        cols = schema_cols('schema/tokyo/dc_future_tick.json')
        empty = '(SELECT * FROM UNNEST(ARRAY<STRUCT<' + ', '.join(f'{c} {t}' for c, t in cols) + '>>[]))'
        text = text.replace(f"`{PROJECT}.{NAMES['dataset']}.dc_future_tick`", empty)
    return text


GAP1 = ['start_at:TIMESTAMP:2022-09-14 06:16:00+00', 'end_at:TIMESTAMP:2023-04-25 04:35:00+00']


@pytest.mark.parametrize('path, p', [
    (BUILD, GAP1 + ['mode:STRING:dryrun']),
    (BUILD, GAP1 + ['mode:STRING:apply']),
    (VERIFY, GAP1),
    (ROLLBACK, GAP1),
    (ACCURACY, ['start_at:TIMESTAMP:2022-08-15 00:00:00+09', 'end_at:TIMESTAMP:2022-08-18 00:00:00+09']),
])
def test_dry_run_whole_script(path, p):
    """スクリプト全体の構文（スクリプトの dry run は構文だけを見る。書き込みはしない）"""
    assert bq(real(read(path)), dry_run=True, params=p) >= 0


def test_no_datacube_values_in_fixtures():
    """固定値は架空の値（DataCube の行を写していない）: 行使価格は 25 円、先物の値は 5 円の刻みの区切りの値、オプションの値段は 1000 未満"""
    assert all(o['exercise_price'] % 25 == 0 for o in OPTS + flow_options())
    assert all(f['trade_price'] % 5 == 0 for f in FUTS + flow_futures())
    assert all(o['trade_price'] < 1000 for o in OPTS + flow_options())

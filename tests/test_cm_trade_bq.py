"""30 日固定の ATM IV の約定 IV からの値（#158）の SQL を BigQuery で実際に動かす検査（tests/test_cm_bq.py と同じ流儀）。

CI では動かさない（BigQuery の認証が要る）。手元で `OPTIONCHAN_BQ_TEST=1 OPTIONCHAN_BQ_PROJECT=<PROJECT_ID> python -m pytest -q tests/test_cm_trade_bq.py`。
入力はすべて合成した値（DataCube のデータは使わない。行使価格・IV・先物の値は架空の値）。
- 作り方の固定値の検査: fill.sql の `-- result:begin`〜`-- result:end` の source（quote・fut・holiday）を固定値の CTE に差し替えて
  SELECT だけ実行する（テーブルを読まない）。期待値は docs/data.md の式を Python で計算したもの
- 書き込みの試験: fill.sql（dryrun・apply を 2 回）→ rollback.sql を、atm_cm_iv をスクリプトの一時テーブルにして流す（本番の表には
  書かない）。範囲・mode のガードで止まること
- 実テーブルの dry run: 結果の SELECT（範囲をクエリパラメータにする）と、本番の表に basis があれば verify.sql・accuracy.sql
"""
import math
import os
import re
from datetime import datetime, timedelta

import pytest

from test_cm_bq import CM_IV, prod_has_basis, table_value
from test_ohlc_bq import run_error, run_script, split_declares, tables_to_temp, temp_table
from test_sql_bq import GiB, MiB, PROJECT, bq, cte, read, render, result_body

pytestmark = pytest.mark.skipif(
    os.environ.get('OPTIONCHAN_BQ_TEST') != '1' or not PROJECT,
    reason='OPTIONCHAN_BQ_TEST=1 と OPTIONCHAN_BQ_PROJECT のときだけ',
)

ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
SQL_DIR = os.path.join(ROOT, 'sql', 'cm_trade')
FILL = os.path.join(SQL_DIR, 'fill.sql')
ROLLBACK = os.path.join(SQL_DIR, 'rollback.sql')
VERIFY = os.path.join(SQL_DIR, 'verify.sql')
ACCURACY = os.path.join(SQL_DIR, 'accuracy.sql')

QUOTE = [('created_at', 'TIMESTAMP'), ('last_trading_day', 'DATE'), ('type', 'INT64'), ('target_price', 'INT64'),
         ('is_atm', 'BOOL'), ('price_time', 'DATETIME'), ('iv', 'FLOAT64')]
FUT = [('created_at', 'TIMESTAMP'), ('price', 'FLOAT64')]
HOLIDAY = [('calendar_date', 'DATE')]
# 限月（架空のカレンダー: 休業日なし）: 2023-02 限の SQ は 02-10（金）、2023-03 限は 03-10（金）の 09:00 JST
M1, M2 = '2023-02-09', '2023-03-09'
T0 = '2023-01-16 00:00:00+00'  # 01-16（月）09:00 JST: SQ まで 25 日・53 日（補間）
STRIKES = range(26000, 30001, 500)
ATM = 28000
FWD = 28100.0


def smile1(k):
    return 20 + (k - ATM) / 500  # 28000 → 20、28500 → 21


def smile2(k):
    return 22 + (k - ATM) / 500


def jst_minus(at, minutes):
    """created_at（UTC）の JST の日時 − minutes 分（DATETIME の文字列）"""
    t = datetime.fromisoformat(at) + timedelta(hours=9) - timedelta(minutes=minutes)
    return t.strftime('%Y-%m-%d %H:%M:%S')


def month_rows(at, month, smile, age=10, atm=ATM):
    """1 限月分の #114 の形の行: call の約定 IV = smile(K) + 0.2、put = smile(K) − 0.2（平均は smile(K)）、約定は age 分前"""
    rows = []
    for k in STRIKES:
        for typ, d in ((1, 0.2), (2, -0.2)):
            rows.append({'created_at': at, 'last_trading_day': month, 'type': typ, 'target_price': k, 'is_atm': k == atm,
                         'price_time': jst_minus(at, age), 'iv': smile(k) + d})
    return rows


def snapshot(at, fwd=FWD):
    return month_rows(at, M1, smile1) + month_rows(at, M2, smile2), [{'created_at': at, 'price': fwd}]


def interp(fwd, s_lo, s_hi, lo, hi):
    return s_lo + (s_hi - s_lo) * math.log(fwd / lo) / math.log(hi / lo)


def iv30(d1, s1, d2, s2):
    return math.sqrt(((d2 - 30) * s1 * s1 * d1 + (30 - d1) * s2 * s2 * d2) / (d2 - d1) / 30)


def num(v):
    return None if v is None else float(v)


def sources(quote, fut, holiday=()):
    return (cte('quote', QUOTE, quote) + cte('fut', FUT, fut or [{'created_at': '1999-01-01 00:00:00+00', 'price': 1.0}])
            + cte('holiday', HOLIDAY, list(holiday) or [{'calendar_date': '1999-01-01'}]))


def run(quote, fut, holiday=()):
    rows = bq('DECLARE trade_window_minutes INT64 DEFAULT 120;\n'
              + result_body(FILL, sources(quote, fut, holiday), dataset='optionchan_tokyo'))
    return {r['created_at'][:16]: r for r in rows}


def test_interp_from_trade_iv_and_future():
    """2 限月の補間: F は先物の値（両方の限月で同じ）、行使価格ごとの IV は call・put の約定 IV の平均"""
    quote, fut = snapshot(T0)
    [row] = run(quote, fut).values()
    assert (row['m1_last_trading_day'], row['m2_last_trading_day']) == (M1, M2)
    assert (num(row['m1_days']), num(row['m2_days'])) == (25.0, 53.0)
    assert num(row['m1_forward']) == num(row['m2_forward']) == FWD
    s1 = interp(FWD, smile1(28000), smile1(28500), 28000, 28500)
    s2 = interp(FWD, smile2(28000), smile2(28500), 28000, 28500)
    assert num(row['m1_iv']) == pytest.approx(s1, abs=1e-9)
    assert num(row['m2_iv']) == pytest.approx(s2, abs=1e-9)
    assert row['method'] == 'interp' and row['basis'] == 'trade'
    assert num(row['iv30']) == pytest.approx(iv30(25, s1, 53, s2), abs=1e-9)


def test_m1_and_m2_like_cm():
    """SQ まで 30 日以上は 1 限月だけ（m1）、3 日未満は 2 限月だけ（m2）。cm の本文と同じ規則"""
    at = '2023-01-10 00:00:00+00'  # 2 月限の SQ まで 31 日
    [row] = run(*snapshot(at)).values()
    assert num(row['m1_days']) == 31.0 and row['method'] == 'm1'
    assert num(row['iv30']) == pytest.approx(num(row['m1_iv']), abs=1e-12)
    at = '2023-02-08 00:00:00+00'  # 2 日
    [row] = run(*snapshot(at)).values()
    assert num(row['m1_days']) == 2.0 and row['method'] == 'm2'
    assert num(row['iv30']) == pytest.approx(num(row['m2_iv']), abs=1e-12)
    # 02-10（金）が休業日なら SQ は 02-13（月）: 5 日 → 補間
    [row] = run(*snapshot(at), [{'calendar_date': '2023-02-10'}]).values()
    assert num(row['m1_days']) == 5.0 and row['method'] == 'interp'


def test_old_trades_are_not_used():
    """約定 IV はスナップショットの 120 分前以降の約定だけ"""
    quote, fut = snapshot(T0)
    # 28000 の約定が 121 分前: 28000 は IV が無く、補間は 27500 と 28500
    old = [dict(r, price_time=jst_minus(T0, 121)) if r['target_price'] == 28000 and r['last_trading_day'] == M1 else r
           for r in quote]
    [row] = run(old, fut).values()
    assert num(row['m1_iv']) == pytest.approx(interp(FWD, smile1(27500), smile1(28500), 27500, 28500), abs=1e-9)
    # 120 分ちょうどは使う
    edge = [dict(r, price_time=jst_minus(T0, 120)) if r['last_trading_day'] == M1 else r for r in quote]
    [row] = run(edge, fut).values()
    assert num(row['m1_iv']) == pytest.approx(interp(FWD, smile1(28000), smile1(28500), 28000, 28500), abs=1e-9)
    # 1 限月の約定がすべて古い: 1 限月の IV・F が無く（cm と同じく IV のある行使価格が無い限月は F も出さない）、補間の 30 日は
    # NULL（行は入る）。2 限月の値は出る
    stale = [dict(r, price_time=jst_minus(T0, 150)) if r['last_trading_day'] == M1 else r for r in quote]
    [row] = run(stale, fut).values()
    assert row['m1_iv'] is None and row['m1_forward'] is None
    assert row['iv30'] is None and row['method'] is None and row['basis'] == 'trade'
    assert row['m2_iv'] is not None


def test_one_side_zero_and_bracket():
    """片方だけの行使価格はその値、iv が 0 の約定は無いのと同じ。F の上に IV のある行使価格が無ければ NULL"""
    quote, fut = snapshot(T0)
    # 28500 の put が無い → 28500 は call の IV（smile + 0.2）。28000 の call の iv が 0 → 28000 は put の IV（smile − 0.2）
    one = [r for r in quote if not (r['last_trading_day'] == M1 and r['target_price'] == 28500 and r['type'] == 2)]
    one = [dict(r, iv=0.0) if r['last_trading_day'] == M1 and r['target_price'] == 28000 and r['type'] == 1 else r
           for r in one]
    [row] = run(one, fut).values()
    assert num(row['m1_iv']) == pytest.approx(
        interp(FWD, smile1(28000) - 0.2, smile1(28500) + 0.2, 28000, 28500), abs=1e-9)
    # F より上の 1 限月の行使価格に約定が無い → 1 限月の IV は NULL
    below = [r for r in quote if not (r['last_trading_day'] == M1 and r['target_price'] > FWD)]
    [row] = run(below, fut).values()
    assert row['m1_iv'] is None and row['iv30'] is None


def test_near_strikes_and_atm():
    """補間に使うのは ATM に近い 9 本だけ。ATM の印が 2 つなら小さい方から数える。印が無い限月は IV を出さない（限月は数える）"""
    fwd = 30200.0
    quote, fut = snapshot(T0, fwd=fwd)
    # 1 限月に 30500 を足す: ATM 28000 から近い 9 本は 26000〜30000 なので、F の上に 9 本の中の行使価格が無い
    extra = [{'created_at': T0, 'last_trading_day': M1, 'type': t, 'target_price': 30500, 'is_atm': False,
              'price_time': jst_minus(T0, 10), 'iv': 30.0} for t in (1, 2)]
    [row] = run(quote + extra, fut).values()
    assert row['m1_iv'] is None
    # 印が 28000 と 28500 の 2 つ: 小さい方（28000）から数えるので同じ
    two = [dict(r, is_atm=r['target_price'] in (28000, 28500)) if r['last_trading_day'] == M1 else r for r in quote + extra]
    [row] = run(two, fut).values()
    assert row['m1_iv'] is None
    # 印が 28500 だけ: 近い 9 本は 26500〜30500 で、30000 と 30500 で補間
    shifted = [dict(r, is_atm=r['target_price'] == 28500) if r['last_trading_day'] == M1 else r for r in quote + extra]
    [row] = run(shifted, fut).values()
    assert num(row['m1_iv']) == pytest.approx(interp(fwd, smile1(30000), 30.0, 30000, 30500), abs=1e-9)
    # 1 限月に印が無い: 1 限月の IV が無い（2 限月を繰り上げない）
    no_atm = [dict(r, is_atm=False) if r['last_trading_day'] == M1 else r for r in quote]
    [row] = run(no_atm, fut).values()
    assert (row['m1_last_trading_day'], row['m2_last_trading_day']) == (M1, M2)
    assert row['m1_iv'] is None and row['m1_forward'] is None and row['iv30'] is None


def test_without_future_is_null_row():
    """先物の行が無いスナップショット: F が無いので IV も 30 日も NULL。行は入る（1 回 1 行）"""
    quote, _ = snapshot(T0)
    q2, f2 = snapshot('2023-01-16 00:05:00+00')
    got = run(quote + q2, f2)
    assert sorted(got) == ['2023-01-16 00:00', '2023-01-16 00:05']
    row = got['2023-01-16 00:00']
    assert row['m1_forward'] is None and row['iv30'] is None and row['method'] is None and row['basis'] == 'trade'
    assert got['2023-01-16 00:05']['method'] == 'interp'


# ---- 書き込み（一時テーブルに書く） ----

START, END = '2023-01-16 00:00:00+00', '2023-01-17 00:00:00+00'


def fill_script(existing, quote, fut, tail='SELECT AS STRUCT * FROM atm_cm_iv ORDER BY created_at'):
    """fill.sql を、source を固定値（範囲で絞る）・atm_cm_iv を一時テーブルにしたスクリプト。最後に表の中身を返す"""
    text = render(tables_to_temp(read(FILL)), project=PROJECT, dataset='optionchan_tokyo')
    m = re.search(r'^-- source:begin\n.*?^-- source:end$', text, re.S | re.M)
    src = (cte('quote_fixed', QUOTE, quote) + cte('fut_fixed', FUT, fut)
           + 'quote AS (SELECT * FROM quote_fixed WHERE created_at >= start_at AND created_at < end_at),\n'
           + 'fut AS (SELECT * FROM fut_fixed WHERE created_at >= start_at AND created_at < end_at),\n'
           + cte('holiday', HOLIDAY, [{'calendar_date': '1999-01-01'}]))
    text = text[:m.start()] + src + text[m.end():]
    declares, rest = split_declares(text)
    return declares + temp_table('atm_cm_iv', CM_IV, existing) + rest + f'\nSELECT ARRAY({tail}) AS after;\n'


def run_fill(mode, existing, quote, fut, start=START, end=END):
    out = run_script(fill_script(existing, quote, fut),
                     params=[f'start_at:TIMESTAMP:{start}', f'end_at:TIMESTAMP:{end}', f'mode:STRING:{mode}'])
    [summary], [after] = out[-2], out[-1]
    counts = {k: int(summary[k]) for k in ('snapshots', 'with_iv30', 'interp', 'm1', 'm2', 'to_delete', 'to_insert',
                                           'conflicts', 'deleted', 'inserted')}
    return counts, [table_value(r) for r in after['after']]


def quote_row(at, iv=None):
    """気配から作った行（basis が NULL）"""
    return {'created_at': at, 'iv30': iv, 'method': 'm1' if iv is not None else None}


def at(minute):
    return f'2023-01-16 00:{minute:02d}:00+00'


def inputs(minutes):
    quote, fut = [], []
    for i, m in enumerate(minutes):
        q, f = snapshot(at(m), fwd=FWD + 10 * i)
        quote += q
        fut += f
    return quote, fut


def by_time(rows):
    return {r['created_at'][:16]: r for r in rows}


def test_fill_apply_is_idempotent_and_keeps_quote_rows():
    # #114 のスナップショット 00:00・00:05・00:10・00:15（範囲の外の前日 23:55 も入力にあるが読まない）
    quote, fut = inputs([0, 5, 10, 15])
    q_out, f_out = snapshot('2023-01-15 23:55:00+00')
    quote, fut = quote + q_out, fut + f_out
    existing = [
        quote_row('2023-01-15 23:00:00+00', 7.0),       # 範囲の外の気配の行（触らない）
        quote_row(at(1), 8.0),                          # 範囲の中の、#114 の時刻でない気配の行（触らない）
        quote_row(at(5)),                               # #114 の時刻の値の無い気配の行（cm_backfill の行。消して足す）
        quote_row(at(10), 9.0),                         # #114 の時刻の値のある気配の行（上書きしない）
        {'created_at': at(20), 'iv30': 1.0, 'basis': 'trade'},  # 前の版の印の行（消す）
    ]
    plan, same = run_fill('dryrun', existing, quote, fut)
    assert plan == {'snapshots': 4, 'with_iv30': 4, 'interp': 4, 'm1': 0, 'm2': 0, 'to_delete': 2, 'to_insert': 3,
                    'conflicts': 1, 'deleted': 0, 'inserted': 0}
    assert [r['created_at'][:16] for r in same] == sorted(r['created_at'][:16] for r in existing)
    first_counts, first = run_fill('apply', existing, quote, fut)
    assert first_counts == dict(plan, deleted=2, inserted=3)
    rows = by_time(first)
    assert sorted(rows) == ['2023-01-15 23:00', '2023-01-16 00:00', '2023-01-16 00:01', '2023-01-16 00:05',
                            '2023-01-16 00:10', '2023-01-16 00:15']
    for t in ('2023-01-16 00:00', '2023-01-16 00:05', '2023-01-16 00:15'):
        assert rows[t]['basis'] == 'trade' and rows[t]['method'] == 'interp' and rows[t]['iv30'] is not None, t
    for t, iv in (('2023-01-15 23:00', 7.0), ('2023-01-16 00:01', 8.0), ('2023-01-16 00:10', 9.0)):
        assert rows[t]['basis'] is None and rows[t]['iv30'] == iv, t
    # F はスナップショットごとの先物の値
    assert rows['2023-01-16 00:05']['m1_forward'] == FWD + 10
    # 2 回目: 印の行を消して同じ行を足す（表は同じ）
    second_counts, second = run_fill('apply', first, quote, fut)
    assert second_counts == dict(plan, to_delete=3, deleted=3, inserted=3)
    assert second == first
    # 切り戻し: 印の行だけが消え、気配の行は残る
    text = render(tables_to_temp(read(ROLLBACK)), project=PROJECT, dataset='optionchan_tokyo')
    declares, rest = split_declares(text)
    out = run_script(declares + temp_table('atm_cm_iv', CM_IV, second) + rest
                     + '\nSELECT ARRAY(SELECT AS STRUCT * FROM atm_cm_iv ORDER BY created_at) AS after;\n',
                     params=[f'start_at:TIMESTAMP:{START}', f'end_at:TIMESTAMP:{END}'])
    [[summary], [after]] = out[-2:]
    assert int(summary['deleted']) == 3
    left = [table_value(r) for r in after['after']]
    assert [r['created_at'][:16] for r in left] == ['2023-01-15 23:00', '2023-01-16 00:01', '2023-01-16 00:10']
    assert all(r['basis'] is None for r in left)


def test_verify_counts():
    """verify.sql の 0 になる項目と引けの日数を、一時テーブルの表で数える（本番の表は読まない）"""
    def row(t, **kw):
        base = {'created_at': t, 'iv30': 20.0, 'method': 'interp', 'm1_last_trading_day': M1, 'm1_days': 25.0,
                'm1_forward': FWD, 'm2_forward': FWD, 'basis': 'trade'}
        return {**base, **kw}
    cm = [row(at(0)),
          row(at(5), method='m1', m2_forward=FWD + 5),                  # method と m1_days が合わない・F が違う
          row(at(7), iv30=600.0),                                       # #114 の時刻でない・iv30 が範囲の外
          row(at(1), basis=None), row(at(1), basis=None),               # 気配の行の重複
          row('2023-01-16 05:40:00+00', basis=None),                    # 14:40 JST の引け（気配）
          row(at(3), basis='other')]                                    # 知らない印
    runs = ([{'snapshot_at': at(m), 'trigger': 'legacy_fill114'} for m in (0, 5, 9)]   # 00:09 は行が無い
            + [{'snapshot_at': at(1), 'trigger': 'scheduler'}, {'snapshot_at': '2023-01-17 00:00:00+00', 'trigger': 'scheduler'},
               {'snapshot_at': '2023-01-14 00:00:00+00', 'trigger': 'scheduler'}])                # 01-14 は土曜
    calendar = [{'calendar_date': '2023-01-02', 'is_trading_day': False}]
    text = render(tables_to_temp(read(VERIFY)), project=PROJECT, dataset='optionchan_tokyo')
    declares, rest = split_declares(text)
    setup = (temp_table('atm_cm_iv', CM_IV, cm)
             + temp_table('ingest_runs', [('snapshot_at', 'TIMESTAMP'), ('trigger', 'STRING')], runs)
             + temp_table('trading_calendar_latest', [('calendar_date', 'DATE'), ('is_trading_day', 'BOOL')], calendar))
    [[got]] = run_script(declares + setup + rest, params=['start_at:TIMESTAMP:2023-01-14 00:00:00+00',
                                                          'end_at:TIMESTAMP:2023-01-18 00:00:00+00'])
    assert {k: int(got[k]) for k in ('duplicate_rows', 'basis_errors', 'outside_rows', 'missing_snapshots', 'iv_range_rows',
                                     'method_errors', 'forward_errors', 'snapshots', 'trade_rows', 'trade_iv30_rows',
                                     'quote_rows', 'trading_days', 'close_days')} == {
        'duplicate_rows': 1, 'basis_errors': 1, 'outside_rows': 1, 'missing_snapshots': 1, 'iv_range_rows': 1,
        'method_errors': 1, 'forward_errors': 1, 'snapshots': 3, 'trade_rows': 3, 'trade_iv30_rows': 3, 'quote_rows': 3,
        'trading_days': 2, 'close_days': 1}
    assert got['short_days'] == ['2023-01-17']


@pytest.mark.parametrize('start,end,mode,message', [
    ('2018-09-29 15:00:00+00', '2018-10-01 15:00:00+00', 'dryrun', 'outside the legacy range'),
    ('2024-08-05 15:00:00+00', '2024-08-06 03:26:00+00', 'dryrun', 'outside the legacy range'),
    (END, START, 'dryrun', 'invalid range'),
    ('2023-01-16 00:00:30+00', END, 'dryrun', 'invalid range'),
    (START, END, 'write', 'mode must be dryrun or apply'),
], ids=['before', 'after', 'reversed', 'not_minute', 'mode'])
def test_fill_guards(start, end, mode, message):
    text = render(read(FILL), project=PROJECT, dataset='optionchan_tokyo')
    declares, rest = split_declares(text)
    guard = rest[:rest.index('CREATE TEMP TABLE result')]
    assert message in run_error(declares + guard, params=[f'start_at:TIMESTAMP:{start}', f'end_at:TIMESTAMP:{end}',
                                                          f'mode:STRING:{mode}'])


# ---- 実テーブルの dry run ----

RANGES = {'A': ('2018-09-30 15:00:00+00', '2022-08-31 15:00:00+00'), 'B': ('2022-08-31 15:00:00+00', '2023-04-30 15:00:00+00'),
          'C': ('2023-04-30 15:00:00+00', '2024-08-06 03:25:00+00')}


def test_dry_run_fill_result():
    """fill.sql の結果の SELECT の見積もり（範囲を変数からクエリパラメータに替える。スクリプトの dry run は見積もりを返さない）。
    範囲 B（欠け 1）。1 日の上限（docs/runbook.md）の中"""
    start, end = RANGES['B']
    text = re.sub(r'\btrade_window_minutes\b', '120', result_body(FILL, dataset='optionchan_tokyo'))
    for name in ('start_at', 'end_at'):
        text = re.sub(rf'\b{name}\b', f'@{name}', text)
    out = bq(text, dry_run=True, params=[f'start_at:TIMESTAMP:{start}', f'end_at:TIMESTAMP:{end}'])
    print(f'dry run fill.sql result [{start}, {end}): {out / MiB:.1f} MiB')
    assert 0 < out < 4 * GiB, out


def test_dry_run_verify_and_accuracy():
    """verify.sql（全範囲）・accuracy.sql（1 窓）の dry run。basis を読むので、本番の表に列を足す（deploy の apply）前は飛ばす"""
    if not prod_has_basis():
        pytest.skip('atm_cm_iv に basis がまだ無い（Terraform の apply の後に流す）')
    for path, start, end in ((VERIFY, RANGES['A'][0], RANGES['C'][1]),
                             (ACCURACY, '2021-11-21 15:00:00+00', '2021-11-26 15:00:00+00')):
        text = render(read(path), project=PROJECT, dataset='optionchan_tokyo')
        bq(text, dry_run=True, params=[f'start_at:TIMESTAMP:{start}', f'end_at:TIMESTAMP:{end}'])

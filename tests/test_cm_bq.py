"""30 日固定の ATM IV（#100）の SQL を BigQuery で実際に動かす検査（tests/test_ohlc_bq.py と同じ流儀）。

CI では動かさない（BigQuery の認証が要る）。手元で `OPTIONCHAN_BQ_TEST=1 OPTIONCHAN_BQ_PROJECT=<PROJECT_ID> python -m pytest -q tests/test_cm_bq.py`。
- 固定値の検査: sql/cm/atm_cm.sql の `-- result:begin`〜`-- result:end` の source（quote・holiday）を固定値の CTE に差し替えて
  実行する（テーブルを読まない）。期待値は docs/data.md の式を Python で計算したもの
- 毎時の定期クエリ（#117）の試験: atm_cm.sql 全体を、source を固定値・atm_cm_iv と ingest_runs をスクリプトの一時テーブルにして
  @run_time ごとに 1 ジョブで流す（本番の表には書かない）。窓の決め方・何もしない回・冪等・増分の積み重ねと 1 日の窓の一致
- 実テーブルの dry run: 見積もりバイト数（PR に記録する）
"""
import json
import math
import os
import re

import pytest

from test_ohlc_bq import run_error, run_script, split_declares, tables_to_temp, temp_table
from test_sql_bq import GiB, MiB, PROJECT, between, bq, cte, read, render, result_body

pytestmark = pytest.mark.skipif(
    os.environ.get('OPTIONCHAN_BQ_TEST') != '1' or not PROJECT,
    reason='OPTIONCHAN_BQ_TEST=1 と OPTIONCHAN_BQ_PROJECT のときだけ',
)

ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
DAILY = os.path.join(ROOT, 'sql', 'cm', 'atm_cm.sql')
BACKFILL = os.path.join(ROOT, 'sql', 'cm', 'cm_backfill.sql')

QUOTE = [('created_at', 'TIMESTAMP'), ('last_trading_day', 'DATE'), ('type', 'INT64'), ('target_price', 'INT64'),
         ('is_atm', 'BOOL'), ('bid', 'INT64'), ('ask', 'INT64'), ('bid_iv', 'FLOAT64'), ('ask_iv', 'FLOAT64')]
HOLIDAY = [('calendar_date', 'DATE')]
M1, M2 = '2026-11-12', '2026-12-10'  # SQ は 11-13（金）・12-11（金）の 09:00 JST
T0 = '2026-10-20 00:00:00+00'  # 10-20 09:00 JST: SQ まで 24 日・52 日（補間）
STRIKES = range(58500, 62001, 500)
ATM = 60000


def month_rows(at, month, fwd, iv_at, atm=ATM, bad_put=None):
    """1 限月分の気配: C − P + K = fwd（put の中値 1000）、call・put の mid IV = iv_at(K)、スプレッドは中値の ±10 円・±0.5。
    bad_put の行使価格の put は bid が極端に低い（スプレッドが中値の 30% を超える）気配にする"""
    rows = []
    for k in STRIKES:
        for typ, mid in ((1, 1000 + fwd - k), (2, 1000)):
            row = {'created_at': at, 'last_trading_day': month, 'type': typ, 'target_price': k, 'is_atm': k == atm,
                   'bid': mid - 10, 'ask': mid + 10, 'bid_iv': iv_at(k) - 0.5, 'ask_iv': iv_at(k) + 0.5}
            if typ == 2 and k == bad_put:
                row.update(bid=1, bid_iv=1.0)
            rows.append(row)
    return rows


def smile1(k):
    return 20 + (k - ATM) / 500  # 60000 → 20、60500 → 21


def smile2(k):
    return 22 + (k - ATM) / 500


def interp(fwd, smile, lo, hi):
    return smile(lo) + (smile(hi) - smile(lo)) * math.log(fwd / lo) / math.log(hi / lo)


def iv30(d1, s1, d2, s2):
    return math.sqrt(((d2 - 30) * s1 * s1 * d1 + (30 - d1) * s2 * s2 * d2) / (d2 - d1) / 30)


def run(quote, holiday=()):
    sources = cte('quote', QUOTE, quote) + cte('holiday', HOLIDAY, list(holiday) or [{'calendar_date': '1999-01-01'}])
    rows = bq('DECLARE win_start TIMESTAMP;\n' + result_body(DAILY, sources, dataset='optionchan_tokyo'))
    return {r['created_at'][:16]: r for r in rows}


def num(v):
    return None if v is None else float(v)


def test_forward_interp_and_30d():
    quote = month_rows(T0, M1, 60200, smile1, bad_put=ATM) + month_rows(T0, M2, 60300, smile2)
    [row] = run(quote).values()
    assert (row['m1_last_trading_day'], row['m2_last_trading_day']) == (M1, M2)
    assert (num(row['m1_days']), num(row['m2_days'])) == (24.0, 52.0)
    # F は C − P + K の中央値（60000 の put は気配が広すぎるので使わないが、他の 4 本で同じ値）
    assert (num(row['m1_forward']), num(row['m2_forward'])) == (60200.0, 60300.0)
    # 60000（20、put は除くので call だけ）と 60500（21）を log(K / F) で補間
    s1 = interp(60200, smile1, 60000, 60500)
    s2 = interp(60300, smile2, 60000, 60500)
    assert num(row['m1_iv']) == pytest.approx(s1, abs=1e-9)
    assert num(row['m2_iv']) == pytest.approx(s2, abs=1e-9)
    assert row['method'] == 'interp'
    assert num(row['iv30']) == pytest.approx(iv30(24, s1, 52, s2), abs=1e-9)


def test_after_sq_uses_m1_only():
    """1 限月の SQ まで 30 日以上（SQ の直後）は 1 限月の IV（外挿しない。SQ の前の 2 限月だけの値とつながる）"""
    at = '2026-10-13 00:00:00+00'  # SQ まで 31 日・59 日
    [row] = run(month_rows(at, M1, 60200, smile1) + month_rows(at, M2, 60300, smile2)).values()
    assert num(row['m1_days']) == 31.0 and row['method'] == 'm1'
    assert num(row['iv30']) == pytest.approx(num(row['m1_iv']), abs=1e-12)


def test_bad_quote_would_change_iv_if_used():
    """スプレッドの条件が効いていること: 60000 の put の極端な気配を使えば補間 IV が変わる（使わないので変わらない）"""
    clean = run(month_rows(T0, M1, 60200, smile1) + month_rows(T0, M2, 60300, smile2))
    dirty = run(month_rows(T0, M1, 60200, smile1, bad_put=ATM) + month_rows(T0, M2, 60300, smile2))
    assert num(clean[T0[:16]]['m1_iv']) == pytest.approx(num(dirty[T0[:16]]['m1_iv']), abs=1e-12)


def test_near_sq_uses_m2_only_and_holiday_moves_sq():
    at = '2026-11-11 00:00:00+00'  # 11-11 09:00 JST: 1 限月の SQ まで 2 日（< 3 日）
    quote = month_rows(at, M1, 60200, smile1) + month_rows(at, M2, 60300, smile2)
    [row] = run(quote).values()
    assert num(row['m1_days']) == 2.0 and row['method'] == 'm2'
    assert num(row['iv30']) == pytest.approx(num(row['m2_iv']), abs=1e-12)
    # 11-13（金）が休業日なら SQ は 11-16（月）: 5 日 → 補間に戻る
    [row] = run(quote, [{'calendar_date': '2026-11-13'}]).values()
    assert num(row['m1_days']) == 5.0 and row['method'] == 'interp'


CALENDAR = [('calendar_date', 'DATE'), ('is_trading_day', 'BOOL')]


@pytest.mark.parametrize('path', [DAILY, BACKFILL], ids=['atm_cm', 'cm_backfill'])
@pytest.mark.parametrize('is_trading_day', [False, True])
def test_holiday_trading_day_is_not_sq(path, is_trading_day):
    """SQ の日は取引カレンダーの全行を休業日として飛ばす（祝日取引の実施日 is_trading_day = true も SQ にならない。
    docs/architecture/target.md 3.2 の契約。#149）。holiday の CTE は SQL のものを使い、表だけを固定値にする"""
    at = '2026-11-11 00:00:00+00'  # 1 限月の SQ（11-13 の金）まで 2 日
    source = between(read(path), '-- source:begin', '-- source:end').group(1)
    [holiday] = re.findall(r'^holiday AS \(\n.*?^\),\n', source, re.S | re.M)
    sources = (cte('cal', CALENDAR, [{'calendar_date': '2026-11-13', 'is_trading_day': is_trading_day}])
               + cte('quote', QUOTE, month_rows(at, M1, 60200, smile1) + month_rows(at, M2, 60300, smile2))
               + holiday.replace('`${project}.${dataset}.trading_calendar_latest`', 'cal'))
    window = ("DECLARE win_start TIMESTAMP DEFAULT TIMESTAMP '2026-11-10 00:00:00+00';\n"
              "DECLARE win_end TIMESTAMP DEFAULT TIMESTAMP '2026-11-12 00:00:00+00';\n"
              "DECLARE start_at TIMESTAMP DEFAULT win_start;\nDECLARE end_at TIMESTAMP DEFAULT win_end;\n")
    [row] = bq(window + result_body(path, sources, dataset='optionchan_tokyo'))
    # どちらでも 11-13 は SQ にならず、SQ は 11-16（月）: 5 日 → 補間
    assert num(row['m1_days']) == 5.0 and row['method'] == 'interp'


def test_missing_forward_or_side_is_null():
    # 1 限月の call に気配が無い → F が出ない → 1 限月の IV も 30 日も NULL（SQ まで 3 日以上なので 2 限月だけにはしない）
    m1 = [dict(r, bid=0) if r['type'] == 1 else r for r in month_rows(T0, M1, 60200, smile1)]
    [row] = run(m1 + month_rows(T0, M2, 60300, smile2)).values()
    assert row['m1_forward'] is None and row['m1_iv'] is None
    assert row['iv30'] is None and row['method'] is None
    assert num(row['m2_iv']) is not None
    # F より上に IV のある行使価格が無い → 補間しない
    m1 = [dict(r, bid_iv=0.0) if r['target_price'] > 60200 else r for r in month_rows(T0, M1, 60200, smile1)]
    [row] = run(m1 + month_rows(T0, M2, 60300, smile2)).values()
    assert num(row['m1_forward']) == 60200.0 and row['m1_iv'] is None and row['iv30'] is None


def test_expired_month_is_not_ranked():
    """SQ を過ぎた限月（最終取引日の夜間の後に残った行）は数えない: 10-08 限の行があっても 1 限月は 11 月限"""
    at = '2026-10-09 08:00:00+00'  # 10-09 17:00 JST（10 月限の SQ 10-09 09:00 の後）
    quote = (month_rows(at, '2026-10-08', 60100, smile1) + month_rows(at, M1, 60200, smile1)
             + month_rows(at, M2, 60300, smile2))
    [row] = run(quote).values()
    assert (row['m1_last_trading_day'], row['m2_last_trading_day']) == (M1, M2)


def test_dry_run_real_tables():
    """実テーブルの見積もり（PR に記録する）: 結果の SELECT の窓 / 範囲を変数からクエリパラメータに替えて dry run する
    （スクリプトの dry run は見積もりを返さない）。定期クエリの 1 日の窓（7 時台の回）・増分の窓（毎時の回。表の最後の行 − 2 時間
    から。#117）と、作り直しの 1 か月"""
    for path, start, end, names, limit in (
            (DAILY, '2026-10-15 22:00:00', '2026-10-16 22:00:00', ('win_start', 'win_end'), 256 * MiB),
            (DAILY, '2026-10-16 03:44:00', '2026-10-16 05:45:00', ('win_start', 'win_end'), 256 * MiB),
            (BACKFILL, '2026-09-01 00:00:00', '2026-10-01 00:00:00', ('start_at', 'end_at'), 4 * GiB)):
        text = result_body(path, dataset='optionchan_tokyo')
        for name in names:
            text = re.sub(rf'\b{name}\b', f'@{name}', text)
        out = bq(text, dry_run=True, params=[f'{names[0]}:TIMESTAMP:{start}', f'{names[1]}:TIMESTAMP:{end}'])
        print(f'dry run {os.path.basename(path)} [{start}, {end}): {out / MiB:.1f} MiB')
        assert 0 < out < limit, out


def test_month_without_atm_flag_is_still_ranked():
    """1 限月に ATM の印が無くても数える（2 限月を 1 限月に繰り上げない）。1 限月の値が無いので 30 日の IV は NULL"""
    m1 = [dict(r, is_atm=False) for r in month_rows(T0, M1, 60200, smile1)]
    [row] = run(m1 + month_rows(T0, M2, 60300, smile2)).values()
    assert (row['m1_last_trading_day'], row['m2_last_trading_day']) == (M1, M2)
    assert row['m1_iv'] is None and row['iv30'] is None


# ---- 毎時の定期クエリ（#117。一時テーブルに書く） ----

SCHEMA = os.path.join(ROOT, 'schema', 'cm', 'atm_cm_iv.json')
with open(SCHEMA, encoding='utf-8') as f:
    CM_IV = [(c['name'], {'FLOAT': 'FLOAT64', 'INTEGER': 'INT64'}.get(c['type'], c['type'])) for c in json.load(f)]
RUNS = [('snapshot_at', 'TIMESTAMP'), ('finished_at', 'TIMESTAMP')]
# 増分の回: 2026-10-13（火）14:45 JST
RUN_TIME = '2026-10-13T05:45:00Z'


def utc(day, hour, minute=0):
    return f'2026-10-{day:02d} {hour:02d}:{minute:02d}:00+00'


def iv_row(at, iv=1.0):
    return {'created_at': at, 'iv30': iv, 'method': 'm1'}


def cm_script(existing, runs, quote=(), upto=False):
    """atm_cm.sql を、source を固定値・atm_cm_iv と ingest_runs を一時テーブルにしたスクリプト。quote は窓で絞る
    （option_price_latest の代わり）。upto なら窓を決めたところで win_start・win_end・rebuild を返す。
    それ以外は最後に窓と表の中身を返す。どちらも何もしない回（RETURN）は出力が無い"""
    text = render(tables_to_temp(read(DAILY)), project=PROJECT, dataset='optionchan_tokyo')
    m = between(text, '-- source:begin', '-- source:end')
    sources = (cte('quote_fixed', QUOTE, list(quote))
               + 'quote AS (SELECT * FROM quote_fixed WHERE created_at >= win_start AND created_at < win_end),\n'
               + cte('holiday', HOLIDAY, [{'calendar_date': '1999-01-01'}]))
    text = text[:m.start()] + sources + text[m.end():]
    declares, rest = split_declares(text)
    setup = temp_table('atm_cm_iv', CM_IV, existing) + temp_table('ingest_runs', RUNS, runs)
    if upto:
        return declares + setup + rest[:rest.index('CREATE TEMP TABLE result')] + 'SELECT win_start, win_end, rebuild;\n'
    return (declares + setup + rest
            + '\nSELECT win_start, win_end, ARRAY(SELECT AS STRUCT * FROM atm_cm_iv ORDER BY created_at) AS after;\n')


def table_value(row):
    """bq の JSON（文字列）を一時テーブルに入れ直せる値にする"""
    return {c: (num(row.get(c)) if t == 'FLOAT64' else row.get(c)) for c, t in CM_IV}


def run_window(run_time, existing, runs):
    """窓の決め方だけを流す。(win_start, win_end, rebuild)（分まで）か、何もしない回は None"""
    out = run_script(cm_script(existing, runs, upto=True), params=[f'run_time:TIMESTAMP:{run_time}'])
    if not out:
        return None
    [[row]] = out
    return row['win_start'][:16], row['win_end'][:16], row['rebuild'] == 'true'


def run_cm(run_time, existing, runs, quote):
    """1 回分を流す。(win_start, win_end, 表の行) か、何もしない回は None"""
    out = run_script(cm_script(existing, runs, quote), params=[f'run_time:TIMESTAMP:{run_time}'])
    if not out:
        return None
    [[row]] = out
    return row['win_start'][:16], row['win_end'][:16], [table_value(r) for r in row['after']]


def fresh(*snapshots):
    """前の回の後に終わった取り込み（finished_at は RUN_TIME の 5 分前）"""
    return [{'snapshot_at': at, 'finished_at': utc(13, 5, 40)} for at in snapshots]


@pytest.mark.parametrize('existing,expected', [
    # 増分: 表の最後の行 − 2 時間から（失敗した回の分もここから埋まる）
    ([iv_row(utc(13, 1)), iv_row(utc(13, 2, 45))], '2026-10-13 00:45'),
    # 3 日の上限: 最後の行 − 2 時間が 3 日より前なら 3 日前から
    ([iv_row(utc(10, 6, 45))], '2026-10-10 05:45'),
    # 表が空 / 3 日より前の行だけ → 3 日前から
    ([], '2026-10-10 05:45'),
    ([iv_row(utc(9, 5))], '2026-10-10 05:45'),
    # 表に run_time より後の行（過去の run_time の手動実行）→ それを除いた最後の行から（窓は空にならない）
    ([iv_row(utc(13, 3)), iv_row(utc(14, 1))], '2026-10-13 01:00'),
], ids=['incremental', 'limit_3_days', 'empty', 'only_old', 'rows_after_run_time'])
def test_hourly_window(existing, expected):
    assert run_window(RUN_TIME, existing, fresh(utc(13, 5, 30))) == (expected, '2026-10-13 05:45', False)


def test_rebuild_window_at_7_jst():
    # 10-14 07:45 JST の回: [10-13 07:00, 10-14 07:00) JST。新しい取り込みが無くても作り直す
    assert run_window('2026-10-13T22:45:00Z', [iv_row(utc(13, 21))], []) == \
        ('2026-10-12 22:00', '2026-10-13 22:00', True)
    # 6 時台・8 時台は増分（表の最後の行 − 2 時間から run_time まで）
    runs = [{'snapshot_at': utc(13, 21), 'finished_at': utc(13, 21, 1)}]
    assert run_window('2026-10-13T21:45:00Z', [iv_row(utc(13, 20))], runs) == \
        ('2026-10-13 18:00', '2026-10-13 21:45', False)
    runs = [{'snapshot_at': utc(13, 23), 'finished_at': utc(13, 23, 1)}]
    assert run_window('2026-10-13T23:45:00Z', [iv_row(utc(13, 21))], runs) == \
        ('2026-10-13 19:00', '2026-10-13 23:45', False)


@pytest.mark.parametrize('runs', [
    # 前の回より前に終わった取り込みだけ（finished_at が run_time − 65 分より前）
    [{'snapshot_at': utc(13, 4, 30), 'finished_at': utc(13, 4, 39)}],
    # 新しく終わった取り込みが 3 日より前の snapshot だけ
    [{'snapshot_at': utc(10, 5), 'finished_at': utc(13, 5, 40)}],
    [],
], ids=['no_new_run', 'only_old_snapshot', 'no_runs'])
def test_hourly_does_nothing_without_new_ingest(runs):
    existing = [iv_row(utc(13, 4))]
    assert run_window(RUN_TIME, existing, runs) is None
    # 全体を流しても表はそのまま（option_price を読まずに終わる）
    assert run_cm(RUN_TIME, existing, runs, snapshot(utc(13, 5))) is None


def test_hourly_rejects_future_run_time():
    text = render(read(DAILY), project=PROJECT, dataset='optionchan_tokyo')
    declares, rest = split_declares(text)
    guard = rest[:rest.index('IF NOT rebuild')]
    assert 'is in the future' in run_error(declares + guard, params=['run_time:TIMESTAMP:2099-01-01T00:45:00Z'])


def snapshot(at, i=0):
    """取り込み 1 回分の気配（i で F・IV を少しずつ変える）"""
    return (month_rows(at, M1, 60200 + 10 * i, lambda k: smile1(k) + 0.1 * i)
            + month_rows(at, M2, 60300 + 10 * i, lambda k: smile2(k) + 0.1 * i))


COLS = [c for c, _ in CM_IV]


def same_rows(got, expected):
    assert [r['created_at'][:16] for r in got] == [r['created_at'][:16] for r in expected]
    for g, e in zip(got, expected, strict=True):
        for c, t in CM_IV[1:]:
            if t == 'FLOAT64' and e[c] is not None:
                assert g[c] == pytest.approx(e[c], abs=1e-9), (e['created_at'], c)
            else:
                assert g[c] == e[c], (e['created_at'], c)


def test_hourly_is_idempotent_and_replaces_window():
    # 表の最後の行 03:00 → 窓 [01:00, 05:45)。窓の中の古い行（計算に無い 02:10 を含む）は置き換え、窓の前の行は残す
    existing = [iv_row(utc(12, 20), 7.0), iv_row(utc(13, 2, 10), 9.0), iv_row(utc(13, 3), 9.0)]
    ats = [utc(13, 0, 40), utc(13, 1, 20), utc(13, 3), utc(13, 4, 20)]
    quote = [r for i, at in enumerate(ats) for r in snapshot(at, i)]
    runs = fresh(*ats)
    start, end, first = run_cm(RUN_TIME, existing, runs, quote)
    assert (start, end) == ('2026-10-13 01:00', '2026-10-13 05:45')
    # 窓の前（00:40 の取り込み・前日の行）は計算しない・消さない
    assert [r['created_at'][:16] for r in first] == ['2026-10-12 20:00', '2026-10-13 01:20', '2026-10-13 03:00',
                                                      '2026-10-13 04:20']
    assert num(first[0]['iv30']) == 7.0 and first[0]['m1_iv'] is None
    # 窓の中は計算の行（10-13 は 1 限月の SQ まで 31 日なので 1 限月の IV。i で値が変わる）
    assert [round(num(r['iv30']), 6) for r in first[1:]] == [round(num(r['m1_iv']), 6) for r in first[1:]]
    assert len({round(num(r['iv30']), 6) for r in first[1:]}) == 3
    # 同じ入力で 2 回目: 窓は表の最後の行（04:20）− 2 時間からになるが、同じ最新版からの計算なので行数・内容は同じ
    start, end, second = run_cm(RUN_TIME, first, runs, quote)
    assert (start, end) == ('2026-10-13 02:20', '2026-10-13 05:45')
    same_rows(second, first)


def test_incremental_runs_match_daily_window():
    """増分の回を毎時重ねて流した結果は、1 日の窓（7 時台の回）で 1 回流した結果と同じ。重ね 2 時間より遅れた取り込みだけは
    7 時台の回まで入らない"""
    # 10-13 09:00〜14:40 JST の 20 分おき。01:00 UTC の取り込みは 100 分遅れ（次の回の重ねで拾う）、
    # 03:00 UTC は 3 時間遅れ（重ねより遅い）
    ats = [utc(13, h, m) for h in range(6) for m in (0, 20, 40)]
    delay = {utc(13, 1): 100, utc(13, 3): 180}
    finished = {at: f'2026-10-13 {int(at[11:13]) + (int(at[14:16]) + delay.get(at, 1)) // 60:02d}:'
                    f'{(int(at[14:16]) + delay.get(at, 1)) % 60:02d}:00+00' for at in ats}
    quote_of = {at: snapshot(at, i) for i, at in enumerate(ats)}

    def inputs(run_time):
        # option_price_latest は書き終わった実行の行だけを返す: finished_at が run_time までの取り込みだけ
        done = [at for at in ats if finished[at] <= run_time.replace('T', ' ').replace('Z', '+00')]
        return [{'snapshot_at': at, 'finished_at': finished[at]} for at in done], [r for at in done for r in quote_of[at]]

    table = []
    for h in range(7):  # 09:45〜15:45 JST の 7 回
        run_time = f'2026-10-13T{h:02d}:45:00Z'
        runs, quote = inputs(run_time)
        out = run_cm(run_time, table, runs, quote)
        assert out, run_time
        table = out[2]
    rebuild_time = '2026-10-13T22:45:00Z'  # 10-14 07:45 JST: [10-13 07:00, 10-14 07:00) JST
    runs, quote = inputs(rebuild_time)
    start, end, daily = run_cm(rebuild_time, [], runs, quote)
    assert (start, end) == ('2026-10-12 22:00', '2026-10-13 22:00')
    assert len(daily) == len(ats)
    same_rows(table, [r for r in daily if r['created_at'][:16] != utc(13, 3)[:16]])
    # 続けて 7 時台の回: 遅れた取り込みも入り、1 日の窓だけの結果と同じ。窓の後の行（07:10 JST）は残る
    after = iv_row(utc(13, 22, 10), 5.0)
    _, _, rebuilt = run_cm(rebuild_time, table + [table_value(after)], runs, quote)
    same_rows(rebuilt, daily + [table_value(after)])

"""30 日固定の ATM IV（#100）の SQL を BigQuery で実際に動かす検査（tests/test_ohlc_bq.py と同じ流儀）。

CI では動かさない（BigQuery の認証が要る）。手元で `OPTIONCHAN_BQ_TEST=1 OPTIONCHAN_BQ_PROJECT=<PROJECT_ID> python -m pytest -q tests/test_cm_bq.py`。
- 固定値の検査: sql/cm/atm_cm.sql の `-- result:begin`〜`-- result:end` の source（quote・holiday）を固定値の CTE に差し替えて
  実行する（テーブルを読まない）。期待値は README の式を Python で計算したもの
- 実テーブルの dry run: 見積もりバイト数（PR に記録する）
"""
import math
import os
import re

import pytest

from test_sql_bq import GiB, MiB, PROJECT, bq, cte, result_body

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
    （スクリプトの dry run は見積もりを返さない）。定期クエリの 1 日の窓と、作り直しの 1 か月"""
    for path, start, end, names, limit in (
            (DAILY, '2026-10-15 22:00:00', '2026-10-16 22:00:00', ('win_start', 'win_end'), 256 * MiB),
            (BACKFILL, '2026-09-01 00:00:00', '2026-10-01 00:00:00', ('start_at', 'end_at'), 4 * GiB)):
        text = result_body(path, dataset='optionchan_tokyo')
        for name in names:
            text = re.sub(rf'\b{name}\b', f'@{name}', text)
        out = bq(text, dry_run=True, params=[f'{names[0]}:TIMESTAMP:{start}', f'{names[1]}:TIMESTAMP:{end}'])
        print(f'dry run {os.path.basename(path)}: {out / MiB:.1f} MiB')
        assert 0 < out < limit, out


def test_month_without_atm_flag_is_still_ranked():
    """1 限月に ATM の印が無くても数える（2 限月を 1 限月に繰り上げない）。1 限月の値が無いので 30 日の IV は NULL"""
    m1 = [dict(r, is_atm=False) for r in month_rows(T0, M1, 60200, smile1)]
    [row] = run(m1 + month_rows(T0, M2, 60300, smile2)).values()
    assert (row['m1_last_trading_day'], row['m2_last_trading_day']) == (M1, M2)
    assert row['m1_iv'] is None and row['iv30'] is None

"""日次の品質チェック・新旧の突き合わせ（#40）の SQL を BigQuery で実際に動かす検査。

CI では動かさない（BigQuery の認証が要る）。手元で `OPTIONCHAN_BQ_TEST=1 OPTIONCHAN_BQ_PROJECT=<PROJECT_ID> python -m pytest -q tests/test_sql_bq.py`。
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

# プロジェクトに既定値は持たない（誰でも clone して自分のプロジェクトで使う前提。#55）
PROJECT = os.environ.get('OPTIONCHAN_BQ_PROJECT', '')
pytestmark = pytest.mark.skipif(
    os.environ.get('OPTIONCHAN_BQ_TEST') != '1' or not PROJECT,
    reason='OPTIONCHAN_BQ_TEST=1 と OPTIONCHAN_BQ_PROJECT のときだけ',
)

ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
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


def result_body(path, sources=None, **names):
    """結果 1 行の SELECT（-- result:begin 〜 -- result:end）。sources を渡すと source の CTE を差し替える"""
    body = between(read(path), '-- result:begin', '-- result:end').group(1)
    if sources is not None:
        m = between(body, '-- source:begin', '-- source:end')
        body = body[:m.start()] + sources + body[m.end():]
    return render(body, project=PROJECT, **names)


def declares(fail_on_unexplained=False, fail_from='2026-10-14'):
    """窓と ERROR の切り替えの変数（compare.sql の DECLARE の代わり。窓の日付は 2026-10-13）"""
    return (
        f"DECLARE win_start TIMESTAMP DEFAULT TIMESTAMP '{WIN_START}';\n"
        f"DECLARE win_end TIMESTAMP DEFAULT TIMESTAMP '{WIN_END}';\n"
        f"DECLARE fail_on_unexplained BOOL DEFAULT {'TRUE' if fail_on_unexplained else 'FALSE'};\n"
        f"DECLARE fail_from DATE DEFAULT DATE '{fail_from}';\n"
    )


def result_query(path, sources=None, fail_on_unexplained=False, fail_from='2026-10-14', **names):
    """結果 1 行の SELECT を、窓の変数を宣言したスクリプトにする"""
    return declares(fail_on_unexplained, fail_from) + result_body(path, sources, **names)


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
    # スクリプト（DECLARE + SELECT）の結果は [[行, ...]]（最後の文の結果）。bq はジョブの警告（「Number of stages in query is
    # high」など）を JSON の途中に書くので、その行を除く
    text = '\n'.join(line for line in out.stdout.splitlines()
                     if not line.startswith(('Warning encountered during job execution', "b'[")))
    [rows] = json.loads(text[text.index('['):])
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
Q_OPTION = Q_PRICE + [('type', 'INT64'), ('target_price', 'INT64'), ('last_trading_day', 'DATE'), ('is_atm', 'BOOL'),
                     ('price', 'INT64'), ('bid', 'INT64'), ('bid_volume', 'INT64'), ('ask', 'INT64'),
                     ('ask_volume', 'INT64'), ('volume', 'INT64'), ('positions', 'INT64')]
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
         'price_time_future', 'price_time_too_old', 'sunday', 'body_frozen')


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


# ---- 本文の凍結（#88） ----

def quality_snapshots(snaps):
    """snaps: [(ページの更新時刻（JST の 'YYYY-MM-DD HH:MM'）, 本文の値)]。本文の値は option の出来高に入れる
    （ほかの項目は違反 0 の形。option の約定時刻は前の取引日の 15:00 で固定）"""
    data = {k: [] for k in ('runs', 'latest_runs', 'spot_l', 'future_l', 'option_l', 'raw_counts')}
    for i, (at_jst, body) in enumerate(snaps):
        run, at = f'r{i}', f'{at_jst}:00+09'
        data['runs'].append({'ingest_run_id': run, 'rows_spot': 1, 'rows_future': 1, 'rows_option': 4})
        data['latest_runs'].append({'snapshot_at': at})
        base = {'created_at': at, 'page_updated_at': at, 'price_time': f'{at_jst}:00'}
        data['spot_l'].append(dict(base))
        data['future_l'].append(dict(base))
        for month in MONTHS:
            for typ in (1, 2):
                # 約定時刻は本文の列なのでスナップショットで変えない（本文の違いは出来高で付ける）
                data['option_l'].append({**base, 'price_time': '2026-10-12 15:00:00', 'type': typ, 'target_price': 60000,
                                         'last_trading_day': month, 'is_atm': True, 'price': 100, 'bid': 95, 'bid_volume': 1, 'ask': 105,
                                         'ask_volume': 2, 'volume': body, 'positions': 1000})
        data['raw_counts'] += [{'tbl': 'spot', 'ingest_run_id': run, 'n': 1},
                               {'tbl': 'future', 'ingest_run_id': run, 'n': 1},
                               {'tbl': 'option', 'ingest_run_id': run, 'n': 4}]
    return data


def same_body(start, minutes, body, day=13):
    """start（'HH:MM'、JST）から minutes 本の毎分のスナップショット。本文はすべて body"""
    h, m = map(int, start.split(':'))
    out = []
    for i in range(minutes):
        t = h * 60 + m + i
        out.append((f'2026-10-{day:02d} {t // 60:02d}:{t % 60:02d}', body))
    return out


def moving(start, minutes, day=13, base=0):
    """毎分本文が変わるスナップショット"""
    return [(t, base + i) for i, (t, _) in enumerate(same_body(start, minutes, None, day))]


def day_2026_10_14_shape(frozen=True):
    """2026-10-14 の窓の形（10-13 07:00〜10-14 07:00 JST に置く）: 寄り前・引け後の本文の止まり（実データの最長: 寄り前 5 回、
    引け後 13 回）、取引時間中の静かな 1 分、夜間の終了直前の 05:58〜06:00、夜間 18:51〜19:27 の凍結（直前と同じが 36 回）。
    frozen=False は凍結の 37 分を毎分変わる本文にした形（10-13〜10-15 の凍結以外の時間帯）"""
    return (
        same_body('08:31', 14, 1)                    # 日中の寄り前 08:31〜08:44（直前と同じ 13 回）
        + moving('08:45', 15, base=100)              # 寄り付き後は毎分変わる
        + same_body('09:00', 2, 200)                 # 静かな 1 分（直前と同じ 1 回）
        + moving('09:02', 3, base=300)
        + moving('15:40', 5, base=400)
        + same_body('15:45', 15, 500)                # 引け後 15:45〜15:59（直前と同じ 14 回）
        + same_body('16:46', 14, 600)                # 夜間の寄り前 16:46〜16:59
        + moving('17:00', 5, base=700)
        + moving('18:46', 5, base=800)
        + (same_body('18:51', 37, 900) if frozen else moving('18:51', 37, base=900))
        + moving('19:28', 3, base=1000)
        + moving('05:50', 8, day=14, base=1100)
        + same_body('05:58', 3, 1200, day=14)        # 05:58〜06:00（06:00 は終了。取引時間中の同じは 1 回）
        + same_body('06:01', 14, 1300, day=14)       # 夜間の引け後 06:01〜06:14
    )


def test_quality_body_frozen_2026_10_14_shape():
    """10-14 の凍結の形は違反（直前と同じ 36 回のうち 3 回目以降の 34）"""
    row = run_quality(quality_snapshots(day_2026_10_14_shape()))
    assert int(row['body_frozen']) == 34
    assert int(row['violations']) == 34
    assert {k: int(row[k]) for k in ITEMS if k != 'body_frozen'} == dict.fromkeys(set(ITEMS) - {'body_frozen'}, 0)


def test_quality_body_frozen_other_hours_are_clean():
    """寄り前・引け後・静かな 1 分・夜間の終了直前は違反にしない"""
    row = run_quality(quality_snapshots(day_2026_10_14_shape(frozen=False)))
    assert (int(row['body_frozen']), int(row['violations'])) == (0, 0)


@pytest.mark.parametrize('repeats, frozen', [(2, 0), (3, 1), (4, 2)])
def test_quality_body_frozen_threshold(repeats, frozen):
    """取引時間中に本文が直前と同じことが N = 3 回続いたら違反、N − 1 回なら違反にしない"""
    snaps = moving('10:00', 3) + same_body('10:03', repeats + 1, 50) + moving('10:30', 3, base=60)
    row = run_quality(quality_snapshots(snaps))
    assert (int(row['body_frozen']), int(row['violations'])) == (frozen, frozen)


@pytest.mark.parametrize('column', ['price', 'bid', 'bid_volume', 'ask', 'ask_volume', 'positions'])
def test_quality_body_frozen_breaks_on_body_change(column):
    """本文の列のどれかが変われば同じ本文と見ない"""
    data = quality_snapshots(same_body('10:00', 6, 50))
    for r in data['option_l']:
        if r['created_at'].startswith('2026-10-13 10:02'):
            r[column] += 1
    row = run_quality(data)
    assert int(row['body_frozen']) == 0  # 10:00〜10:01（1 回）と 10:03〜10:05（2 回）に分かれる


def test_quality_body_frozen_counts_across_page2_rows():
    """2 限月の行だけが変わっても本文の変化（1 限月・2 限月の全行が本文）"""
    data = quality_snapshots(same_body('10:00', 6, 50))
    for r in data['option_l']:
        if r['created_at'].startswith('2026-10-13 10:02') and r['last_trading_day'] == MONTHS[1] and r['type'] == 2:
            r['price'] = 101
    assert int(run_quality(data)['body_frozen']) == 0


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


def compare_sources(d, staged=None):
    if staged is None:
        staged = {'spot_rows': len(d['legacy_spot']), 'future_rows': len(d['legacy_future']),
                  'option_rows': len(d['legacy_option'])}
    return cte('staged', STAGED, [staged]) + ''.join(cte(name, cols, d[name]) for name, cols in (
        ('legacy_spot', SPOT), ('legacy_future', FUTURE), ('legacy_option', OPTION), ('new_runs', RUNS),
        ('new_spot', SPOT), ('new_future', FUTURE), ('new_option', OPTION)))


def run_compare(d, staged=None, **fail):
    [row] = bq(result_query(COMPARE, compare_sources(d, staged), dataset='optionchan_tokyo',
                            copy_dataset='optionchan_legacy_copy', **fail))
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
        'spot_legacy_older': 0, 'future_legacy_older': 0, 'option_legacy_older': 0, 'option_new_behind': 0,
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


# ---- 旧系が古い版・途中の版を読んだ行（legacy_older、#61） ----
# 2026-10-13 14:59 JST（05:59 UTC）の実例の形。新系は 14:44〜14:59 の毎分、旧系は 14:59 の 1 スナップショット

def spot_at(m, pt=None, price=None):
    """14:mm の spot。基準価格（price − diff）は 100"""
    price = 100.0 + m if price is None else price
    return {'price': price, 'price_time': pt or jst(m, 14), 'diff': price - 100.0, 'diff_rate': 0.1, 'hv': 20.0}


def future_at(m):
    """14:mm の先物。基準価格は 100"""
    return {'price': 200.0 + m, 'price_time': jst(m, 14), 'diff': 100.0 + m, 'diff_rate': 0.1, 'hv': 20.0,
            'contract_month': '2026-12-01'}


def opt(typ, strike, price, pt, volume, iv, base):
    return {'type': typ, 'target_price': strike, 'is_atm': False, 'price': price, 'price_time': pt, 'diff': price - base,
            'diff_rate': 0.5, 'iv': iv, 'bid': 1, 'bid_volume': 1, 'bid_iv': 19.0, 'ask': 2, 'ask_volume': 1,
            'ask_iv': 21.0, 'volume': volume, 'positions': 1000, 'quotation': 900, 'quotation_date': '2026-10-09',
            'delta': 0.5, 'gamma': 0.01, 'theta': -1.0, 'vega': 2.0, 'last_trading_day': MONTHS[0]}


def opt_a(m):
    """実例 1（call 68000）: 14:42 の約定のまま 14:57 まで、14:58 に約定、14:59 は出来高だけ増えた"""
    if m <= 57:
        return opt(1, 68000, 1960, jst(42, 14), 79, 26.34, 1500)
    return opt(1, 68000, 1995, jst(58, 14), 80 if m == 58 else 83, 26.9 if m == 58 else 26.95, 1500)


def opt_b(m):
    """実例 2（put 65000）: 14:57 までは 14:49 の約定、14:58・14:59 は 14:58 の約定"""
    if m <= 57:
        return opt(2, 65000, 1100, jst(49, 14), 616, 28.5, 1300)
    return opt(2, 65000, 1100, jst(58, 14), 622 if m == 58 else 629, 28.7 if m == 58 else 28.8, 1300)


def opt_c(m):
    """毎分約定する行（call 70000）"""
    return opt(1, 70000, 500 + m, jst(m, 14), 10 * m, 15 + m / 100, 500)


T = ts(59, 5)          # 比べる T（14:59 JST）
NEXT = ts(0, 6)        # その次のスナップショット（15:00 JST）


def t_values():
    """T と同じ値の spot・先物・option（次のスナップショットで新系の T の値を裏付ける）"""
    return spot_at(59), future_at(59), [opt_a(59), opt_b(59), opt_c(59)]


def older_fixture(next_new=True, next_legacy=False):
    """新系 14:44〜14:59（next_new なら 15:00 も。中身は 14:59 と同じ = 新系の T の値の裏付け）、旧系 14:59
    （next_legacy なら旧系の 15:00 も。中身は 14:59 の新系と同じ）"""
    d = empty()
    for m in range(44, 60):
        at = ts(m, 5)
        # spot は 14:57 のスナップショットが 14:56 の約定時刻（その分の更新を取れなかった）、14:58 で 14:58 に進む
        spot = spot_at(56, price=157.0) if m == 57 else spot_at(m)
        put(d, 'new', at, spot, future_at(m), [opt_a(m), opt_b(m), opt_c(m)])
        d['new_runs'].append({'snapshot_at': at, 'page2_updated_at': at})
    if next_new:
        spot, future, options = t_values()
        put(d, 'new', NEXT, spot, future, options)
        d['new_runs'].append({'snapshot_at': NEXT, 'page2_updated_at': NEXT})
    put(d, 'legacy', T,
        spot_at(57, price=157.5),          # 新系の 14:57（14:56）と 14:58（14:58）の間の約定時刻（途中の版）
        future_at(57),                     # 新系の 2 分前（14:57）の版
        [{**opt_a(57), 'bid': 5},          # 実例 1: 新系の 2 分前の版（気配は違う）
         opt(2, 65000, 1100, jst(57, 14), 621, 28.89, 1300),  # 実例 2: 新系の 14:57 と 14:58 の版の間
         opt_c(59)])
    if next_legacy:
        spot, future, options = t_values()
        put(d, 'legacy', NEXT, spot, future, options)
    return d


def counts(row, keys):
    return {k: int(row[k]) for k in keys}


OLDER_KEYS = ('spot_compared', 'spot_legacy_older', 'spot_unexplained', 'future_legacy_older', 'future_unexplained',
              'option_compared', 'option_legacy_older', 'option_unexplained', 'unexplained')


def test_compare_legacy_older():
    row = run_compare(older_fixture())
    assert counts(row, OLDER_KEYS) == {
        'spot_compared': 1, 'spot_legacy_older': 1, 'spot_unexplained': 0, 'future_legacy_older': 1,
        'future_unexplained': 0, 'option_compared': 3, 'option_legacy_older': 2, 'option_unexplained': 0,
        'unexplained': 0}
    assert counts(row, ('spot_version', 'future_version', 'option_version', 'option_quote_only')) == dict.fromkeys(
        ('spot_version', 'future_version', 'option_version', 'option_quote_only'), 0)


def _legacy(d, typ, strike, row):
    """旧系の option の行を差し替える"""
    d['legacy_option'] = [r if (r['type'], r['target_price']) != (typ, strike) else {**row, 'created_at': r['created_at']}
                          for r in d['legacy_option']]


def _new_t(d, typ, strike, **cols):
    """新系の T（05:59）の option の行の列を変える"""
    for r in d['new_option']:
        if r['created_at'] == ts(59, 5) and (r['type'], r['target_price']) == (typ, strike):
            r.update(cols)


def _a_new_price_only(d):
    # 新系の解析誤りを模す: 旧系は正しい T の版、新系の T は価格だけ違う（約定時刻・出来高は同じ）
    _legacy(d, 1, 68000, opt_a(59))
    _new_t(d, 1, 68000, price=1999)
    _next_a_moves_on(d)


def _a_new_volume_not_advanced(d):
    # 旧系は 2 分前の版だが、新系の T は出来高が増えていない（新系の約定の列だけ違う）
    _new_t(d, 1, 68000, volume=79)


def _b_same_volume_as_s2(d):
    # 出来高が S2（14:58）と同じなのに価格が違う（同じ出来高なら同じ版のはず）
    _legacy(d, 2, 65000, opt(2, 65000, 1101, jst(58, 14), 622, 28.7, 1299))


def _b_base_mismatch(d):
    # 出来高・約定時刻は間だが、基準価格（price − diff）が S1・S2 と違う（価格か前日比の誤り）
    _legacy(d, 2, 65000, opt(2, 65000, 1100, jst(57, 14), 621, 28.89, 1305))


def _b_positions_out_of_range(d):
    _legacy(d, 2, 65000, {**opt(2, 65000, 1100, jst(57, 14), 621, 28.89, 1300), 'positions': 1001})


def _c_older_than_10min(d):
    # 新系の T の 11 分前（14:48）の版は古すぎる
    _legacy(d, 1, 70000, opt_c(48))


def _new_row(d, table, at):
    [row] = [r for r in d[f'new_{table}'] if r['created_at'] == at]
    return row


# 新系の T の値を誤らせるケースでは、新系の直後（15:00）を次の版に進める（直後が旧系の T の版と同じだと、既存の version が
# 「旧系が直後の版を読んだ」として説明するため。新系の T だけが 1 回誤る形は version の限界で、ここでは扱わない）
def _next_spot_moves_on(d):
    _new_row(d, 'spot', NEXT).update(spot_at(0, pt=jst(0, 15), price=160.0))


def _next_a_moves_on(d):
    [r] = [r for r in d['new_option'] if r['created_at'] == NEXT and r['target_price'] == 68000]
    r.update(opt(1, 68000, 2000, jst(0, 15), 90, 27.0, 1500))


def _spot_new_price_only(d):
    # spot の新系の解析誤りを模す: 旧系は正しい T の版、新系の T は価格だけ違う
    d['legacy_spot'] = [{**spot_at(59), 'created_at': T}]
    _new_row(d, 'spot', T)['price'] = 999.0
    _next_spot_moves_on(d)


def _a_new_volume_up_price_wrong(d):
    # レビューの反例: 旧系は正しい T の版、新系の T は出来高を多く・価格を誤って読んだ（進んでいるように見える）
    _legacy(d, 1, 68000, opt_a(59))
    _new_t(d, 1, 68000, volume=830, price=1234, diff=1234 - 1500)
    _next_a_moves_on(d)


def _spot_new_pt_later_price_wrong(d):
    # レビューの反例: 旧系は正しい T の版、新系の T は約定時刻を後に・価格を誤って読んだ
    d['legacy_spot'] = [{**spot_at(59), 'created_at': T}]
    _new_row(d, 'spot', T).update({'price_time': jst(0, 15), 'price': 999.0})
    _next_spot_moves_on(d)


def _a_misread_persists(d):
    # 再レビューの反例 A: 新系が出来高・価格を誤って読み、その誤りが直後のスナップショットにも続く（直後は裏付けにならない）
    _legacy(d, 1, 68000, opt_a(59))
    for at in (T, NEXT):
        [r] = [r for r in d['new_option'] if r['created_at'] == at and r['target_price'] == 68000]
        r.update(volume=830, price=1234, diff=1234 - 1500)


def _spot_misread_persists(d):
    # 再レビューの反例 B: 新系が spot の約定時刻・価格を誤って読み、その誤りが直後にも続く
    d['legacy_spot'] = [{**spot_at(59), 'created_at': T}]
    for at in (T, NEXT):
        _new_row(d, 'spot', at).update({'price_time': jst(0, 15), 'price': 999.0})


def _a_new_row_missing(d):
    # レビューの反例: 新系の T の行が欠けた（旧系の行は新系の直前と全列一致）。行の欠けは version にしない
    d['new_option'] = [r for r in d['new_option'] if not (r['created_at'] == T and r['target_price'] == 68000)]
    _legacy(d, 1, 68000, opt_a(58))


def _future_cm_mismatch(d):
    # 古い版と値は同じだが限月が違う（+1 年以外）
    d['legacy_future'][0]['contract_month'] = '2027-06-01'


def _future_older_than_10min(d):
    d['legacy_future'] = [{**future_at(48), 'created_at': T}]


def _future_between(d, legacy_diff=157.5):
    """新系の 14:57 の先物が 14:56 の約定時刻のまま、旧系は 14:57 の約定時刻（14:57 と 14:58 のスナップショットの間）"""
    _new_row(d, 'future', ts(57, 5)).update(future_at(56))
    d['legacy_future'] = [{**future_at(57), 'price': 257.5, 'diff': legacy_diff, 'created_at': T}]


def _future_between_base_mismatch(d):
    _future_between(d, legacy_diff=150.0)


def _spot_between_base_mismatch(d):
    d['legacy_spot'][0]['diff'] = 50.0


@pytest.mark.parametrize('mutate, kind, older', [
    (_a_new_price_only, 'option', 1), (_a_new_volume_not_advanced, 'option', 1), (_b_same_volume_as_s2, 'option', 1),
    (_b_base_mismatch, 'option', 1), (_b_positions_out_of_range, 'option', 1), (_c_older_than_10min, 'option', 2),
    (_a_new_volume_up_price_wrong, 'option', 1), (_a_new_row_missing, 'option', 1), (_a_misread_persists, 'option', 1),
    (_spot_misread_persists, 'spot', 0),
    (_spot_new_price_only, 'spot', 0), (_spot_between_base_mismatch, 'spot', 0), (_spot_new_pt_later_price_wrong, 'spot', 0),
    (_future_cm_mismatch, 'future', 0), (_future_older_than_10min, 'future', 0), (_future_between_base_mismatch, 'future', 0),
])
def test_compare_legacy_older_keeps_new_errors_unexplained(mutate, kind, older):
    """新系の T の値が旧系のどの版とも合わない行・古すぎる版は説明しない（older は残りの説明済みの行数）"""
    d = older_fixture()
    mutate(d)
    row = run_compare(d)
    assert counts(row, (f'{kind}_unexplained', f'{kind}_legacy_older', 'unexplained')) == {
        f'{kind}_unexplained': 1, f'{kind}_legacy_older': older, 'unexplained': 1}


def test_compare_legacy_older_10min_boundary():
    """新系の T のちょうど 10 分前（14:49）の版は古い版として説明する"""
    d = older_fixture()
    _legacy(d, 1, 70000, opt_c(49))
    d['legacy_future'] = [{**future_at(49), 'created_at': T}]
    row = run_compare(d)
    assert counts(row, ('option_legacy_older', 'option_unexplained', 'future_legacy_older', 'future_unexplained')) == {
        'option_legacy_older': 3, 'option_unexplained': 0, 'future_legacy_older': 1, 'future_unexplained': 0}


def test_compare_future_between_is_legacy_older():
    d = older_fixture()
    _future_between(d)
    row = run_compare(d)
    assert counts(row, ('future_legacy_older', 'future_unexplained')) == {'future_legacy_older': 1, 'future_unexplained': 0}


@pytest.mark.parametrize('next_new, next_legacy, older', [(True, False, False), (False, True, True), (False, False, False)])
def test_compare_between_prev_and_t(next_new, next_legacy, older):
    """2026-10-13 13:07 JST の先物の形: 旧系の約定時刻が新系の直前（14:57 のまま）と T（14:59）の間。S2 = T は新系の T の値が
    旧系の次のスナップショットで裏付けられるときだけ（新系の直後だけでは説明しない。新系の誤りが直後まで続く形があるため）"""
    d = older_fixture(next_new=next_new, next_legacy=next_legacy)
    _new_row(d, 'future', ts(58, 5)).update(future_at(57))
    d['legacy_future'] = [r for r in d['legacy_future'] if r['created_at'] != T] + [{**future_at(58), 'created_at': T}]
    row = run_compare(d)
    assert counts(row, ('future_legacy_older', 'future_unexplained')) == {
        'future_legacy_older': int(older), 'future_unexplained': int(not older)}


@pytest.mark.parametrize('next_new, next_legacy, older', [
    (True, False, True),     # 新系の直後が T と同じ値
    (False, True, True),     # 旧系の次のスナップショットが新系の T と同じ値（旧系の 15:00 は新系に無いので legacy_only が 1）
    (False, False, False),   # 比べる相手が無い（T が最後のスナップショット）ときは説明しない
])
def test_compare_legacy_older_needs_new_t_corroborated(next_new, next_legacy, older):
    """legacy_older は新系の T の値が、新系の直後か旧系の次のスナップショットで裏付けられるときだけ"""
    row = run_compare(older_fixture(next_new=next_new, next_legacy=next_legacy))
    n = 1 if older else 0
    assert counts(row, ('spot_legacy_older', 'future_legacy_older', 'option_legacy_older')) == {
        'spot_legacy_older': n, 'future_legacy_older': n, 'option_legacy_older': 2 * n}
    assert counts(row, ('spot_unexplained', 'future_unexplained', 'option_unexplained', 'legacy_only_snapshots')) == {
        'spot_unexplained': 1 - n, 'future_unexplained': 1 - n, 'option_unexplained': 2 * (1 - n),
        'legacy_only_snapshots': int(next_legacy)}


def _half_updated(d, table, legacy_diff=None, seen_later=True):
    """新系の T（14:59）が約定時刻だけ進み価格の列が直前（14:58）のまま（書き換え途中の版）。旧系は 14:59 の価格まで書き換わった版。
    新系の直後（15:00）も T と同じ書き換え途中の版で、15:01 は seen_later なら旧系と同じ版（新系も後でその版を取った = 裏付け）、
    そうでなければ別の版（約定時刻 15:01）"""
    at = spot_at if table == 'spot' else future_at
    half = {**at(58), 'price_time': jst(59, 14)}
    _new_row(d, table, T).update(half)
    _new_row(d, table, NEXT).update(half)
    legacy = at(59)
    if legacy_diff is not None:
        legacy['diff'] = legacy_diff
    d[f'legacy_{table}'] = [{**legacy, 'created_at': T}]
    spot, future, options = t_values()
    later = dict(legacy) if seen_later else {**at(59), 'price': legacy['price'] + 2, 'price_time': jst(1, 15)}
    if table == 'spot':
        spot = later
    else:
        future = later
    put(d, 'new', ts(1, 6), spot, future, options)
    d['new_runs'].append({'snapshot_at': ts(1, 6), 'page2_updated_at': ts(1, 6)})


def carried_over_fixture(n):
    """先物の価格の持ち越し（レビューの反例 3）が 1 日に n 回: 新系の T が約定時刻だけ進み価格が直前のまま、旧系は正しい T の版、
    新系はその版を後で取らない（直後は次の版）。spot・option は新旧一致"""
    d = empty()
    halves = [2 + 3 * k for k in range(n)]
    for m in range(0, 3 * n + 3):
        at = ts(m, 5)
        future = {**future_at(m - 1), 'price_time': jst(m, 14)} if m in halves else future_at(m)
        put(d, 'new', at, spot_at(m), future, [opt_c(m)])
        d['new_runs'].append({'snapshot_at': at, 'page2_updated_at': at})
        if m in halves:
            put(d, 'legacy', at, spot_at(m), future_at(m), [opt_c(m)])
    return d


def test_compare_legacy_next_contradicts_new_t():
    """旧系の次のスナップショットが新系の T と矛盾する（出来高が新系の T より少ない）ときは、新系の直後で裏付けられても説明しない。
    説明できない行は T の実例 1 の行と、旧系の次（15:00）のその行（新系の 15:00 と違う）の 2 行"""
    d = older_fixture(next_legacy=True)
    [r] = [r for r in d['legacy_option'] if r['created_at'] == NEXT and r['target_price'] == 68000]
    r.update(volume=80)
    row = run_compare(d)
    assert counts(row, ('option_legacy_older', 'option_unexplained')) == {'option_legacy_older': 1, 'option_unexplained': 2}
    samples = sorted((s['created_at'][:16], s['detail']) for s in row['unexplained_samples'])
    assert samples == [('2026-10-13 05:59', '1 68000 2026-11-12'), ('2026-10-13 06:00', '1 68000 2026-11-12')]


def stale_same_fixture(n, table='future', catch_up=False):
    """新系の T が直前と同じ中身（版の遅れ）で旧系だけ新しい版の行（table の表）が 1 日に n 回。他の表は新旧一致。
    catch_up が偽なら新系の直後も T と同じ版（新系が止まり続ける。2026-10-14 18:52〜19:27 JST の凍結の形）、
    真なら直後は次の版（1 分で追いつく。新系の 1 分遅れ new_behind、#89。2026-10-15 の形）"""
    d = empty()
    stale = [2 + 3 * k for k in range(n)]
    at_fn = spot_at if table == 'spot' else future_at
    for m in range(0, 3 * n + 3):
        at = ts(m, 5)
        if m in stale:
            value = at_fn(m - 1)
        elif not catch_up and m - 1 in stale:
            value = at_fn(m - 2)       # 直後も T と同じ版のまま
        else:
            value = at_fn(m)
        spot = value if table == 'spot' else spot_at(m)
        future = value if table == 'future' else future_at(m)
        put(d, 'new', at, spot, future, [opt_c(m)])
        d['new_runs'].append({'snapshot_at': at, 'page2_updated_at': at})
        if m in stale:
            put(d, 'legacy', at, spot_at(m), future_at(m), [opt_c(m)])
    return d


@pytest.mark.parametrize('n, explained', [(3, True), (4, False)])
def test_compare_stale_same_limit(n, explained):
    """新系の版の遅れ（新系の T が直前と同じ中身）も 1 日に表ごと 3 行まで。超えたら全部 unexplained（系統的に古い版を返す不具合）"""
    row = run_compare(stale_same_fixture(n))
    assert counts(row, ('future_compared', 'future_new_stale', 'future_unexplained')) == {
        'future_compared': n, 'future_new_stale': n if explained else 0, 'future_unexplained': 0 if explained else n}


@pytest.mark.parametrize('table', ['spot', 'future'])
def test_compare_frozen_is_not_new_behind(table):
    """新系が止まり続ける形（直後も T と同じ版）は新系の 1 分遅れ（new_behind）にしない。new_stale の上限 3 を超えて全部 unexplained"""
    row = run_compare(stale_same_fixture(4, table))
    assert counts(row, (f'{table}_new_behind', f'{table}_new_stale', f'{table}_unexplained')) == {
        f'{table}_new_behind': 0, f'{table}_new_stale': 0, f'{table}_unexplained': 4}


@pytest.mark.parametrize('table', ['spot', 'future'])
@pytest.mark.parametrize('n, explained', [(10, True), (11, False)])
def test_compare_new_behind_limit(table, n, explained):
    """新系の 1 分遅れ（直後で追いつく）は 1 日に表ごと 10 行まで。超えたら全部 unexplained（系統的に遅れる不具合）"""
    row = run_compare(stale_same_fixture(n, table, catch_up=True))
    assert counts(row, (f'{table}_compared', f'{table}_new_behind', f'{table}_new_stale', f'{table}_unexplained')) == {
        f'{table}_compared': n, f'{table}_new_behind': n if explained else 0, f'{table}_new_stale': 0,
        f'{table}_unexplained': 0 if explained else n}


def behind_fixture(table, shape):
    """新系 14:00〜14:04（05:00〜05:04 UTC）、旧系は 14:02 だけ（table の表は 14:02 の約定。他の表は新旧一致）。
    shape で新系の 14:01〜14:03 の table の表の版を変える"""
    at_fn = spot_at if table == 'spot' else future_at
    new = {m: at_fn(m) for m in range(5)}
    legacy = at_fn(2)
    if shape in ('time_only', 'time_only_base', 'time_only_cm'):
        # 2026-10-15 12:16 JST の先物の形: 新系の T は約定時刻だけ直前から進んだ版で 1 分遅れ、直後で旧系より先へ進む
        new[1] = at_fn(0)
        new[2] = {**at_fn(0), 'price_time': jst(1, 14)}
        if shape == 'time_only_base':
            legacy = {**legacy, 'diff': legacy['diff'] - 5}
        if shape == 'time_only_cm':
            legacy = {**legacy, 'contract_month': '2027-06-01'}
    elif shape == 'same':
        new[2] = at_fn(1)                       # 新系の T は直前と同じ中身、直後で追いつく（2026-10-15 の形）
    elif shape == 'frozen':
        new[2] = new[3] = at_fn(1)              # 直後も T と同じ（止まり続ける）
    elif shape == 'behind_next':
        new[1] = new[2] = at_fn(0)              # 直後は進むが、まだ旧系より前（追いついていない）
        new[3] = at_fn(1)
    elif shape == 'freeze_end':
        # 長い凍結の最後の行（2026-10-14 19:28 JST の形）: 新系は止まった版の後に少しだけ新しい古い版、直後で旧系の先まで飛ぶ
        new = {0: at_fn(5), 1: at_fn(5), 2: at_fn(7), 3: at_fn(11), 4: at_fn(12)}
        legacy = at_fn(10)
    elif shape == 'rewind':
        # 新系の T だけ 3 分前の版に戻る（直後は戻る前の続き）
        new = {m: at_fn(m + 3) for m in range(5)}
        new[2] = at_fn(2)
        legacy = at_fn(5)
    d = empty()
    for m in range(5):
        at = ts(m, 5)
        put(d, 'new', at, new[m] if table == 'spot' else spot_at(m), new[m] if table == 'future' else future_at(m),
            [opt_c(m)])
        d['new_runs'].append({'snapshot_at': at, 'page2_updated_at': at})
    put(d, 'legacy', ts(2, 5), legacy if table == 'spot' else spot_at(2), legacy if table == 'future' else future_at(2),
        [opt_c(2)])
    return d


@pytest.mark.parametrize('table, shape, behind, stale', [
    ('spot', 'same', 1, 0), ('future', 'same', 1, 0),
    ('spot', 'time_only', 1, 0), ('future', 'time_only', 1, 0),
    ('spot', 'frozen', 0, 1), ('future', 'frozen', 0, 1),           # new_stale（上限 3 の中）
    ('spot', 'behind_next', 0, 1), ('future', 'behind_next', 0, 1),
    ('spot', 'time_only_base', 0, 0), ('future', 'time_only_base', 0, 0),  # 基準価格が違う → unexplained
    ('spot', 'freeze_end', 0, 0), ('future', 'freeze_end', 0, 0),          # 遅れが 1 分を超える → unexplained
    ('spot', 'rewind', 0, 0), ('future', 'rewind', 0, 0),
    ('future', 'time_only_cm', 0, 0),                                       # 限月が違う → unexplained
])
def test_compare_new_behind_shapes(table, shape, behind, stale):
    row = run_compare(behind_fixture(table, shape))
    assert counts(row, (f'{table}_new_behind', f'{table}_new_stale', f'{table}_unexplained', 'unexplained')) == {
        f'{table}_new_behind': behind, f'{table}_new_stale': stale, f'{table}_unexplained': 1 - behind - stale,
        'unexplained': 1 - behind - stale}


def long_freeze_fixture(table):
    """4 分の凍結: 新系の 14:02〜14:05 が 14:01 の版のまま、14:06 で追いつく。旧系は 14:01〜14:06 の毎分（table の表は毎分の版）"""
    at_fn = spot_at if table == 'spot' else future_at
    d = empty()
    for m in range(8):
        at = ts(m, 5)
        value = at_fn(1) if 2 <= m <= 5 else at_fn(m)
        put(d, 'new', at, value if table == 'spot' else spot_at(m), value if table == 'future' else future_at(m), [opt_c(m)])
        d['new_runs'].append({'snapshot_at': at, 'page2_updated_at': at})
        if 1 <= m <= 6:
            put(d, 'legacy', at, spot_at(m), future_at(m), [opt_c(m)])
    return d


@pytest.mark.parametrize('table', ['spot', 'future'])
def test_compare_long_freeze_last_row_is_not_new_behind(table):
    """凍結の最後の行（直後で追いつく）は遅れが 1 分を超えるので new_behind にしない。凍結の 4 行が new_stale の上限 3 を超えて全部 unexplained"""
    row = run_compare(long_freeze_fixture(table))
    assert counts(row, (f'{table}_new_behind', f'{table}_new_stale', f'{table}_unexplained', 'unexplained')) == {
        f'{table}_new_behind': 0, f'{table}_new_stale': 0, f'{table}_unexplained': 4, 'unexplained': 4}


# ---- option の新系の 1 分遅れ（new_behind、#131） ----
# 2026-10-19・10-20 の形。新系 14:00〜14:04（05:00〜05:04 UTC）、旧系は 14:02 だけ。spot・先物は新旧一致、option は 1 キー

OPT_T = ts(2, 5)       # 比べる T（14:02 JST）


def opt_old(strike=68000):
    """新系の直前・T の版: 13:30 の約定のまま"""
    return opt(1, strike, 1960, jst(30, 13), 79, 26.34, 1500)


def opt_legacy(strike=68000, pt=None):
    """旧系の T の版: 14:01（T の 1 分前）の約定で出来高が増えた"""
    return opt(1, strike, 1995, pt or jst(1, 14), 80, 26.9, 1500)


def opt_caught_up(strike=68000):
    """新系の直後の版: 旧系より後（14:02）の約定"""
    return opt(1, strike, 2000, jst(2, 14), 83, 26.95, 1500)


def no_trade(strike=68000):
    """約定の無い行（その日まだ約定が無い。建玉・清算値・清算日だけある）"""
    return {**opt(1, strike, 0, None, 0, 0.0, 1500), 'price': None, 'diff': None, 'diff_rate': None, 'iv': None,
            'volume': None, 'delta': None, 'gamma': None, 'theta': None, 'vega': None}


def opt_behind_fixture(shape, n=1):
    """shape で新系・旧系の option の版を変える。n は同じ T に並べる行使価格の数（上限の検査）"""
    strikes = [68000 + 125 * k for k in range(n)]
    new = {m: [opt_old(k) if m <= 2 else opt_caught_up(k) for k in strikes] for m in range(5)}
    legacy = [opt_legacy(k) for k in strikes]
    if shape == 'no_trade':
        # 2026-10-20 10:46 JST の形: 新系の直前・T は約定の列が空、旧系は T と同じ分のその日最初の約定、直後は旧系より先
        new[1] = new[2] = [no_trade(k) for k in strikes]
        legacy = [opt(1, k, 120, jst(2, 14), 5, 30.0, 100) for k in strikes]
        new[3] = new[4] = [opt(1, k, 125, jst(2, 14), 7, 30.1, 100) for k in strikes]
    elif shape == 'frozen':
        new[3] = new[2]                                        # 直後も T と同じ（止まり続ける）
    elif shape == 'delay_2min':
        legacy = [opt_legacy(k, pt=jst(0, 14)) for k in strikes]
    elif shape == 'next_behind':
        new[3] = [{**opt_caught_up(k), 'volume': 79} for k in strikes]  # 直後は進むが出来高が旧系より少ない
    elif shape == 'positions':
        legacy = [{**r, 'positions': 1001} for r in legacy]
    elif shape == 'quotation':
        legacy = [{**r, 'quotation': 901} for r in legacy]
    elif shape == 'new_missing':
        # 2026-10-21 19:48 JST の形: 新系の直前・T にその行が無く、直後から載る
        new[1] = new[2] = []
    elif shape == 'quotation_year':
        legacy = [{**r, 'quotation_date': '2027-10-09'} for r in legacy]
    elif shape == 'base':
        legacy = [{**r, 'diff': r['price'] - 1490} for r in legacy]
    d = empty()
    for m in range(5):
        at = ts(m, 5)
        put(d, 'new', at, spot_at(m), future_at(m), new[m])
        d['new_runs'].append({'snapshot_at': at, 'page2_updated_at': at})
    put(d, 'legacy', OPT_T, spot_at(2), future_at(2), legacy)
    return d


OPT_BEHIND_KEYS = ('option_compared', 'option_new_behind', 'option_unexplained', 'unexplained')


@pytest.mark.parametrize('shape, behind', [
    ('same', 1),               # 2026-10-19 の形: 新系の T = 直前（古い約定）、旧系は T の 1 分前の約定、直後で追いつく
    ('no_trade', 1),           # 2026-10-20 10:46 JST の形
    ('quotation_year', 1),     # 清算日だけ旧系が +1 年（旧系の年の補完の誤り）
    ('frozen', 0),             # 直後も T と同じ（凍結は説明しない）
    ('delay_2min', 0),         # 旧系の約定時刻が T の 2 分前（遅れが 1 分を超える）
    ('next_behind', 0),        # 直後の出来高が旧系より少ない（追いついていない）
    ('positions', 0),          # 建玉が新系の T と違う
    ('quotation', 0),          # 清算値が新系の T と違う
    ('new_missing', 0),        # 新系の T・直前に行が無い（新系の T の行の欠けは説明しない）
    ('base', 0),               # 基準価格（price − diff）が新系の直後と違う
])
def test_compare_option_new_behind_shapes(shape, behind):
    row = run_compare(opt_behind_fixture(shape))
    assert counts(row, OPT_BEHIND_KEYS) == {
        'option_compared': 1, 'option_new_behind': behind, 'option_unexplained': 1 - behind, 'unexplained': 1 - behind}
    assert counts(row, ('spot_unexplained', 'future_unexplained', 'option_legacy_older',
                        'option_legacy_older_unconfirmed')) == dict.fromkeys(
        ('spot_unexplained', 'future_unexplained', 'option_legacy_older', 'option_legacy_older_unconfirmed'), 0)


@pytest.mark.parametrize('n, explained', [(10, True), (11, False)])
def test_compare_option_new_behind_limit(n, explained):
    """option の新系の 1 分遅れも 1 日に 10 行まで。超えたら全部 unexplained（系統的に遅れる不具合）"""
    row = run_compare(opt_behind_fixture('same', n))
    assert counts(row, OPT_BEHIND_KEYS) == {
        'option_compared': n, 'option_new_behind': n if explained else 0, 'option_unexplained': 0 if explained else n,
        'unexplained': 0 if explained else n}
    assert len(row['unexplained_samples']) == (0 if explained else n)


def test_compare_option_long_freeze_last_minute_is_new_behind():
    """限界の明記（#131）: 2 分以上の凍結（新系の 14:00〜14:03 が同じ版、14:04 で追いつく。旧系は 14:01〜14:04 の毎分進む）は、
    option では新系の T の約定時刻から遅れを測れないので最後の分（14:03）が new_behind になる。凍結の途中の分（14:01・14:02）は
    直後も T と同じで unexplained に残る（凍結そのものは検出される）。14:04 は新旧一致"""
    d = empty()
    for m in range(6):
        at = ts(m, 5)
        put(d, 'new', at, spot_at(m), future_at(m), [opt_old() if m <= 3 else opt(1, 68000, 2010, jst(3, 14), 90, 27.0, 1500)])
        d['new_runs'].append({'snapshot_at': at, 'page2_updated_at': at})
    for m in range(1, 4):
        put(d, 'legacy', ts(m, 5), spot_at(m), future_at(m), [opt(1, 68000, 1990 + m, jst(m - 1, 14), 80 + m, 26.5, 1500)])
    put(d, 'legacy', ts(4, 5), spot_at(4), future_at(4), [opt(1, 68000, 2010, jst(3, 14), 90, 27.0, 1500)])
    row = run_compare(d)
    assert counts(row, OPT_BEHIND_KEYS) == {
        'option_compared': 4, 'option_new_behind': 1, 'option_unexplained': 2, 'unexplained': 2}
    samples = sorted(s['created_at'][:16] for s in row['unexplained_samples'])
    assert samples == ['2026-10-13 05:01', '2026-10-13 05:02']



# ---- option の新しく載った行使価格の 1 分遅れ（new_strike、#134） ----
# 2026-10-21 19:48 JST の形。新系 14:00〜14:04（05:00〜05:04 UTC）、旧系は 14:02 だけ。その限月には毎分約定する行（call 70000。
# 新旧一致）があり、新しく載る行使価格は新系の 14:03 以降と旧系の 14:02 にだけある（新系はページの書き換えの途中の版を読んだ）

def strike_next(typ, strike, traded=False):
    """新しく載った行の新系の直後の版。traded は T の分（14:02）に出来高 1 の約定がある行"""
    if traded:
        return opt(typ, strike, 120, jst(2, 14), 1, 30.0, 100)
    return {**no_trade(strike), 'type': typ}


def opt_new_strike_fixture(shape, n=4):
    """shape で新しく載る行の版を変える。'shape' は 10-21 の 4 行（put 2 行は直後と全列一致、call 2 行は ask だけ違い、
    call 100000 は約定あり）。'limit' は n 行（直後と全列一致）。ほかは call 100000 の 1 行で 1 つの条件を外す"""
    if shape == 'shape':
        nexts = [strike_next(2, 95000), strike_next(2, 100000), strike_next(1, 95000), strike_next(1, 100000, traded=True)]
        legacy = nexts[:2] + [{**r, 'ask': 7} for r in nexts[2:]]
    elif shape == 'limit':
        nexts = [strike_next(1, 95000 + 125 * k, traded=True) for k in range(n)]
        legacy = list(nexts)
    else:
        nexts = [strike_next(1, 100000, traded=True)]
        legacy = [{**nexts[0], 'ask': 7}]
    if shape == 'trade_mismatch':
        legacy = [{**legacy[0], 'volume': 2}]
    elif shape == 'quotation_year':
        legacy = [{**legacy[0], 'quotation_date': '2027-10-09'}]
    new = {m: [opt_c(m)] + (nexts if m >= 3 else []) for m in range(5)}
    legacy_moving = opt_c(2)
    if shape == 'in_prev':
        new[1] = new[1] + nexts                                # 直前にあり T だけ欠けた（本物の取りこぼし）
    elif shape == 'no_next':
        new[3], new[4] = [opt_c(3)], [opt_c(4)]                # 直後にも無い（2026-10-14 の凍結の形）
    elif shape == 'frozen_t':
        new[2] = [opt_c(1)]                                    # 新系の T のその限月が直前と同じ（凍結の最後の分）
        legacy_moving = opt_c(1)
    d = empty()
    for m in range(5):
        if shape == 'no_prev' and m <= 1:
            continue                                           # T が読んだ範囲の最初のスナップショット
        at = ts(m, 5)
        put(d, 'new', at, spot_at(m), future_at(m), new[m])
        d['new_runs'].append({'snapshot_at': at, 'page2_updated_at': at})
    put(d, 'legacy', OPT_T, spot_at(2), future_at(2), [legacy_moving, *legacy])
    return d



NEW_STRIKE_KEYS = ('option_compared', 'option_new_strike', 'option_unexplained', 'unexplained')
NEW_STRIKE_ZERO = ('option_version', 'option_quote_only', 'option_quotation_year', 'option_page2', 'option_new_behind',
                   'option_legacy_older', 'option_legacy_older_unconfirmed', 'spot_unexplained', 'future_unexplained')


def test_compare_option_new_strike_2026_10_21_shape():
    """2026-10-21 19:48 JST の形: 新系の T・直前に無く直後から載った 4 行は new_strike（気配と表示の列は直後と違ってよい）"""
    row = run_compare(opt_new_strike_fixture('shape'))
    assert counts(row, NEW_STRIKE_KEYS) == {
        'option_compared': 5, 'option_new_strike': 4, 'option_unexplained': 0, 'unexplained': 0}
    assert counts(row, NEW_STRIKE_ZERO) == dict.fromkeys(NEW_STRIKE_ZERO, 0)
    assert row['unexplained_samples'] == []


@pytest.mark.parametrize('shape, explained', [
    ('quotation_year', 1),     # 清算日だけ旧系が +1 年（旧系の年の補完の誤り）
    ('in_prev', 0),            # 新系の直前にその行がある（T だけの欠けは説明しない）
    ('no_next', 0),            # 新系の直後にも無い（2026-10-14 の凍結の間に旧系だけ多い形）
    ('trade_mismatch', 0),     # 旧系の出来高が新系の直後と違う
    ('frozen_t', 0),           # 新系の T のその限月が直前と同じ（凍結の最後の分で直後に行が載る形）
    # T が読んだ範囲の最初のスナップショット（has_prev が偽）。直前が無いと凍結かどうか（new_month_moved）も測れず偽になるので、
    # has_prev だけを切り分けるテストではない（どちらでも当たらないことの確認）
    ('no_prev', 0),
])
def test_compare_option_new_strike_shapes(shape, explained):
    row = run_compare(opt_new_strike_fixture(shape))
    assert counts(row, NEW_STRIKE_KEYS) == {
        'option_compared': 2, 'option_new_strike': explained, 'option_unexplained': 1 - explained,
        'unexplained': 1 - explained}
    assert counts(row, NEW_STRIKE_ZERO) == dict.fromkeys(NEW_STRIKE_ZERO, 0)


@pytest.mark.parametrize('n, explained', [(10, True), (11, False)])
def test_compare_option_new_strike_limit(n, explained):
    """新しく載った行使価格の 1 分遅れも 1 日に 10 行まで。超えたら全部 unexplained（新しく載った行を系統的に取りこぼす不具合）"""
    row = run_compare(opt_new_strike_fixture('limit', n))
    assert counts(row, NEW_STRIKE_KEYS) == {
        'option_compared': n + 1, 'option_new_strike': n if explained else 0, 'option_unexplained': 0 if explained else n,
        'unexplained': 0 if explained else n}
    assert len(row['unexplained_samples']) == (0 if explained else n)

def s1_minute_fixture(table, next_kind):
    """2026-10-14 14:58 JST の先物の形: 新系 14:55〜14:59（14:57 の版 → 14:58 の版）、旧系の 14:58 は新系の直前（14:57）と同じ分の
    別の価格（基準価格は同じ）。旧系の次（14:59）は新系の 14:59 と同じで、next_kind で変える:
    same_as_t = 新系の T と同じ（裏付けあり）、other_price = 新系の T と同じ分の別の価格、ahead = 新系の T より後の約定"""
    if table == 'spot':
        legacy_t = spot_at(57, price=157.5)
        nx = {'same_as_t': spot_at(58), 'other_price': spot_at(58, price=158.5), 'ahead': spot_at(59)}[next_kind]
    else:
        legacy_t = {**future_at(57), 'price': 257.5, 'diff': 157.5}
        nx = {'same_as_t': future_at(58), 'other_price': {**future_at(58), 'price': 258.5, 'diff': 158.5},
              'ahead': future_at(59)}[next_kind]
    d = empty()
    for m in range(55, 60):
        at = ts(m, 5)
        spot = nx if table == 'spot' and m == 59 else spot_at(m)
        future = nx if table == 'future' and m == 59 else future_at(m)
        put(d, 'new', at, spot, future, [opt_c(m)])
        d['new_runs'].append({'snapshot_at': at, 'page2_updated_at': at})
    put(d, 'legacy', ts(58, 5), legacy_t if table == 'spot' else spot_at(58),
        legacy_t if table == 'future' else future_at(58), [opt_c(58)])
    put(d, 'legacy', ts(59, 5), nx if table == 'spot' else spot_at(59), nx if table == 'future' else future_at(59),
        [opt_c(59)])
    return d


@pytest.mark.parametrize('table', ['spot', 'future'])
@pytest.mark.parametrize('next_kind, older, unconfirmed', [
    ('same_as_t', 1, 0),       # 旧系の次が新系の T を裏付ける → legacy_older（(b) の下の境界 S1 ≤ 旧、#89）
    ('other_price', 0, 0),     # 旧系の次が新系の T と違う（同じ分の別の価格）→ unexplained
    ('ahead', 0, 1),           # 裏付けは無いが旧系の次が新系の T より先 → legacy_older_unconfirmed
])
def test_compare_older_same_minute_as_s1(table, next_kind, older, unconfirmed):
    row = run_compare(s1_minute_fixture(table, next_kind))
    assert counts(row, (f'{table}_legacy_older', f'{table}_legacy_older_unconfirmed', f'{table}_unexplained', 'unexplained')) == {
        f'{table}_legacy_older': older, f'{table}_legacy_older_unconfirmed': unconfirmed,
        f'{table}_unexplained': 1 - older - unconfirmed, 'unexplained': 1 - older - unconfirmed}


def unconfirmed_fixture(n):
    """裏付けの無い古い版（legacy_older_unconfirmed、#89）が 1 日に表ごと n 回。新系は毎分 14:mm の版。旧系の T（14:02 + 3k）は
    spot・先物が新系の T と同じ約定時刻の別の価格（2026-10-14 10:15 JST の先物の形）、option は新系の 2 分前の版（2026-10-14
    15:35 JST の C 68125 の形）。旧系の次（T の 1 分後）は新系と同じ（新系の T より先へ進んでいて、新系の T を裏付けない）"""
    d = empty()
    legacy_t = [2 + 3 * k for k in range(n)]
    for m in range(0, 3 * n + 3):
        at = ts(m, 5)
        put(d, 'new', at, spot_at(m), future_at(m), [opt_c(m)])
        d['new_runs'].append({'snapshot_at': at, 'page2_updated_at': at})
        if m in legacy_t:
            put(d, 'legacy', at, spot_at(m, price=100.5 + m),
                {**future_at(m), 'price': 200.5 + m, 'diff': 100.5 + m}, [opt_c(m - 2)])
        if m - 1 in legacy_t:
            put(d, 'legacy', at, spot_at(m), future_at(m), [opt_c(m)])
    return d


@pytest.mark.parametrize('n, explained', [(1, True), (3, True), (4, False)])
def test_compare_legacy_older_unconfirmed_limit(n, explained):
    """裏付けの無い古い版は 1 日に表ごと 3 行まで。超えたらその表の分を全部 unexplained（新系の 1 回だけの読み誤りと区別できない）"""
    row = run_compare(unconfirmed_fixture(n))
    expected = {}
    for table in ('spot', 'future', 'option'):
        expected.update({f'{table}_legacy_older': 0, f'{table}_legacy_older_unconfirmed': n if explained else 0,
                         f'{table}_unexplained': 0 if explained else n})
    expected['unexplained'] = 0 if explained else 3 * n
    assert counts(row, tuple(expected)) == expected


@pytest.mark.parametrize('ahead', [True, False])
def test_compare_option_legacy_older_unconfirmed(ahead):
    """option の古い版（実例 1。新系の 2 分前の版）で、新系の直後は T と違う版（裏付けなし）。旧系の次の出来高が新系の T より多ければ
    legacy_older_unconfirmed。同じ出来高で別の値なら説明しない（旧系の次の行も新系と合わず unexplained で 2 行）"""
    d = older_fixture(next_legacy=True)
    _next_a_moves_on(d)
    [r] = [r for r in d['legacy_option'] if r['created_at'] == NEXT and r['target_price'] == 68000]
    if ahead:
        r.update(_new_row_key(d, NEXT, 68000))
    else:
        r.update(price=1999, diff=1999 - 1500)
    row = run_compare(d)
    assert counts(row, ('option_legacy_older', 'option_legacy_older_unconfirmed', 'option_unexplained', 'unexplained')) == {
        'option_legacy_older': 1, 'option_legacy_older_unconfirmed': int(ahead), 'option_unexplained': 0 if ahead else 2,
        'unexplained': 0 if ahead else 2}


def _new_row_key(d, at, strike):
    [r] = [r for r in d['new_option'] if r['created_at'] == at and r['target_price'] == strike]
    return {k: v for k, v in r.items() if k != 'created_at'}


def test_compare_page2_needs_new_row():
    """2 限月ページの版（page2）は新系の T に行があるときだけ。新系の T の行の欠けは説明しない"""
    d = empty()
    for n in range(3):
        put_new(d, ts(n), n, page2_at=ts(n, 1) if n == 1 else None)
        put(d, 'legacy', ts(n), spot_v(n), future_v(n), [option_v(n, 0), option_v(n, 1)])
    d['new_option'] = [r for r in d['new_option'] if not (r['created_at'] == ts(1) and r['last_trading_day'] == MONTHS[1])]
    d['legacy_option'] = [{**r, 'price': 999} if r['created_at'] == ts(1) and r['last_trading_day'] == MONTHS[1] else r
                          for r in d['legacy_option']]
    row = run_compare(d)
    assert counts(row, ('option_page2', 'option_unexplained')) == {'option_page2': 0, 'option_unexplained': 1}


@pytest.mark.parametrize('n, explained', [(3, True), (4, False)])
def test_compare_unconfirmed_half_updated_limit(n, explained):
    """裏付けの無い書き換え途中の版は 1 日に表ごと 3 行までなら new_stale、超えたら全部 unexplained（系統的な価格の持ち越しを捕まえる）"""
    row = run_compare(carried_over_fixture(n))
    assert counts(row, ('future_compared', 'future_new_stale', 'future_unexplained', 'unexplained')) == {
        'future_compared': n, 'future_new_stale': n if explained else 0, 'future_unexplained': 0 if explained else n,
        'unexplained': 0 if explained else n}


@pytest.mark.parametrize('table', ['spot', 'future'])
@pytest.mark.parametrize('legacy_diff, seen_later, stale', [
    (None, True, True),      # 書き換え途中の版で、新系も後で旧系の版を取った（裏付けあり）
    (None, False, True),     # 裏付けなし（2026-10-13 13:44 JST の先物の形）。1 日に 1 行だけなので説明済み（多数なら下のテスト）
    ('mismatch', True, False),  # 旧系の基準価格（price − diff）が新系と違う
])
def test_compare_new_half_updated(table, legacy_diff, seen_later, stale):
    d = older_fixture()
    diff = None if legacy_diff is None else (50.0 if table == 'spot' else 150.0)
    _half_updated(d, table, diff, seen_later)
    row = run_compare(d)
    assert counts(row, (f'{table}_new_stale', f'{table}_legacy_older', f'{table}_unexplained')) == {
        f'{table}_new_stale': int(stale), f'{table}_legacy_older': 0, f'{table}_unexplained': int(not stale)}


# ---- 説明できない差分の ERROR（fail_on_unexplained と日付の下限 fail_from、#61） ----

def fail_script(fail_on_unexplained, fail_from):
    """compare.sql の最後の IF（説明できない差分の ERROR）を、result を固定値の結果の副問い合わせにして動かす（表を書かない）"""
    text = read(COMPARE)
    block = re.search(r'^IF \(SELECT fail_on_unexplained AND unexplained > 0 FROM result\) THEN\n.*?^END IF;$', text, re.S | re.M)
    assert block
    d = compare_fixture()                  # 説明できない差分 3
    result = '(' + result_body(COMPARE, compare_sources(d), dataset='optionchan_tokyo',
                               copy_dataset='optionchan_legacy_copy') + ')'
    sql = re.sub(r'\bFROM result\b', lambda m: 'FROM ' + result, block.group(0))
    return declares(fail_on_unexplained, fail_from) + sql + "\nSELECT 'ok' AS status;\n"


@pytest.mark.parametrize('fail_on_unexplained, fail_from, expected', [
    (True, '2026-10-13', True),    # 窓の日付（2026-10-13）が下限以降
    (True, '2026-10-14', False),   # 下限より前の日（#38 の前を含む 2026-10-13 を再実行しても ERROR にしない）
    (False, '2026-10-13', False),  # 切り替えが false
])
def test_compare_fail_from(fail_on_unexplained, fail_from, expected):
    row = run_compare(compare_fixture(), fail_on_unexplained=fail_on_unexplained, fail_from=fail_from)
    assert int(row['unexplained']) == 3
    assert row['fail_on_unexplained'] == ('true' if expected else 'false')
    cmd = ['bq', f'--project_id={PROJECT}', f'--location={TOKYO}', 'query', '--use_legacy_sql=false', '--format=json',
           '--nouse_cache', '--maximum_bytes_billed=10485760']
    out = subprocess.run(cmd, input=fail_script(fail_on_unexplained, fail_from), capture_output=True, text=True, timeout=300)
    if expected:
        assert out.returncode != 0 and 'legacy_compare: unexplained 3 on 2026-10-13' in out.stdout + out.stderr
    else:
        assert out.returncode == 0 and '"ok"' in out.stdout


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
    (COMPARE, TOKYO, {'dataset': 'optionchan_tokyo', 'copy_dataset': 'optionchan_legacy_copy', 'fail_on_unexplained': 'true',
                      'fail_from': '2026-10-14'}),
    (STAGE, 'US', {'legacy_dataset': 'optionchan', 'stage_dataset': 'optionchan_legacy_stage'}),
])
def test_dry_run_whole_script(path, location, names):
    """スクリプト全体（DECLARE・IF・CREATE・MERGE・ERROR）の構文。スクリプトの dry run は構文だけを見る（表の有無は見ない）"""
    sql = render(read(path), project=PROJECT, **names)
    assert bq(sql, location=location, dry_run=True, params=['run_time:TIMESTAMP:2026-10-13T23:00:00Z']) == 0

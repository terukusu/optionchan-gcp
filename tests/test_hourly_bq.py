"""銘柄ごとの 1 時間足（#161）の SQL を BigQuery で実際に動かす検査（tests/test_cm_bq.py と同じ流儀）。

CI では動かさない（BigQuery の認証が要る）。手元で
`OPTIONCHAN_BQ_TEST=1 OPTIONCHAN_BQ_PROJECT=<PROJECT_ID> python -m pytest -q tests/test_hourly_bq.py`。
- 固定値の検査: sql/hourly/option_price_hourly.sql の `-- result:begin`〜`-- result:end` の source（src）を固定値の CTE に差し替えて
  実行する（テーブルを読まない）。期待値はテストに書いた値（docs/data.md の「1 時間足の計算（銘柄ごと）」の規則）
- 最新版の選び方: source の本文はそのままにし、option_price_latest の本文（sql/latest/option_price_latest.sql）を一時テーブルの
  option_price・ingest_runs に当てる
- 毎時の定期クエリの試験: option_price_hourly.sql 全体を、source を固定値・表と ingest_runs をスクリプトの一時テーブル（表は本番と同じ
  last_trading_day の月パーティションとクラスタ。一時テーブルには require_partition_filter を付けられない）にして @run_time ごとに
  1 ジョブで流す（本番の表には書かない）。窓の決め方・何もしない回・冪等・増分の積み重ねと 1 日の窓の一致・限月の範囲の ERROR
- 過去分の作り直し（hourly_backfill.sql）: 範囲の検査（JST の 07:00）と、1 日の窓の結果が定期クエリの作り直しの回と同じこと
- 実テーブル: スクリプトの変数での絞り込みが require_partition_filter を満たすこと（option_price を読むだけ）、元の行の dry run の
  見積もり（PR に記録する）、スクリプト全体の構文。表を読み書きする文（MAX・DELETE・INSERT）の dry run は、本番に表がある
  （deploy の apply の後）ときだけ
"""
import json
import os
import re
import subprocess
from datetime import datetime, timedelta

import pytest

from test_ohlc_bq import run_error, run_script, split_declares, tables_to_temp, temp_table
from test_sql_bq import GiB, MiB, PROJECT, TOKYO, between, bq, cte, literal, read, render

pytestmark = pytest.mark.skipif(
    os.environ.get('OPTIONCHAN_BQ_TEST') != '1' or not PROJECT,
    reason='OPTIONCHAN_BQ_TEST=1 と OPTIONCHAN_BQ_PROJECT のときだけ',
)

ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
DAILY = os.path.join(ROOT, 'sql', 'hourly', 'option_price_hourly.sql')
BACKFILL = os.path.join(ROOT, 'sql', 'hourly', 'hourly_backfill.sql')
LATEST = os.path.join(ROOT, 'sql', 'latest', 'option_price_latest.sql')
SCHEMA = os.path.join(ROOT, 'schema', 'tokyo', 'option_price_hourly.json')
TYPES = {'FLOAT': 'FLOAT64', 'INTEGER': 'INT64'}
with open(SCHEMA, encoding='utf-8') as f:
    HOURLY = [(c['name'], TYPES.get(c['type'], c['type'])) for c in json.load(f)]
COLS = [c for c, _ in HOURLY]
KEY = ('started_at', 'last_trading_day', 'target_price', 'type')

SRC = [('created_at', 'TIMESTAMP'), ('last_trading_day', 'DATE'), ('target_price', 'INT64'), ('type', 'INT64'), ('bid', 'INT64'),
       ('ask', 'INT64'), ('bid_iv', 'FLOAT64'), ('ask_iv', 'FLOAT64'), ('price', 'INT64'), ('price_time', 'DATETIME'),
       ('iv', 'FLOAT64'), ('volume', 'INT64'), ('positions', 'INT64')]
RUNS = [('snapshot_at', 'TIMESTAMP'), ('finished_at', 'TIMESTAMP')]
M1 = '2026-11-12'


def jst(at):
    """'YYYY-MM-DD HH:MM'（JST）→ TIMESTAMP の文字列"""
    return f'{at}:00+09:00'


def row(at, k=60000, typ=2, month=M1, bid=100, ask=110, bid_iv=20.0, ask_iv=21.0, price=None, pt=None, iv=None, volume=None,
        positions=None):
    """元の行 1 つ。at・pt は JST の 'YYYY-MM-DD HH:MM'"""
    return {'created_at': jst(at), 'last_trading_day': month, 'target_price': k, 'type': typ, 'bid': bid, 'ask': ask,
            'bid_iv': bid_iv, 'ask_iv': ask_iv, 'price': price, 'price_time': None if pt is None else f'{pt}:00', 'iv': iv,
            'volume': volume, 'positions': positions}


def num(v):
    return None if v is None else float(v)


def value(r):
    """bq の JSON（文字列）を比べられる値・一時テーブルに入れ直せる値にする"""
    out = {}
    for c, t in HOURLY:
        v = r.get(c)
        out[c] = None if v is None else int(v) if t == 'INT64' else float(v) if t == 'FLOAT64' else v[:19]
    return out


def key(r):
    return (r['started_at'][:16], r['last_trading_day'], int(r['target_price']), int(r['type']))


def fixed_sources(rows):
    """source（src）の代わり: 固定値を窓で絞る（option_price_latest(win_start, win_end) の代わり。type が NULL の行は落とす）"""
    return (cte('src_fixed', SRC, rows)
            + 'src AS (SELECT * FROM src_fixed WHERE created_at >= win_start AND created_at < win_end AND type IS NOT NULL),\n')


def replace_source(text, sources):
    m = between(text, '-- source:begin', '-- source:end')
    return text[:m.start()] + sources + text[m.end():]


def run_bars(rows, start, end):
    """結果の SELECT を固定値で流す。start・end は JST の 'YYYY-MM-DD HH:MM'。{キー: 行}"""
    body = between(read(DAILY), '-- result:begin', '-- result:end').group(1)
    sql = (f"DECLARE win_start TIMESTAMP DEFAULT TIMESTAMP '{jst(start)}';\n"
           f"DECLARE win_end TIMESTAMP DEFAULT TIMESTAMP '{jst(end)}';\n"
           + render(replace_source(body, fixed_sources(rows)), project=PROJECT, dataset='optionchan_tokyo'))
    out = {}
    for r in bq(sql):
        assert key(r) not in out, key(r)
        out[key(r)] = value(r)
    return out


def utc_hour(at):
    """JST の 'YYYY-MM-DD HH' の正時 → UTC の 'YYYY-MM-DD HH:00'（started_at の表示）"""
    return (datetime.strptime(at, '%Y-%m-%d %H') - timedelta(hours=9)).strftime('%Y-%m-%d %H:00')


QUOTE_NULL = ('mid_open', 'mid_high', 'mid_low', 'mid_close', 'bid_close', 'ask_close', 'bid_iv_close', 'ask_iv_close')
TRADE_NULL = ('trade_open', 'trade_high', 'trade_low', 'trade_close', 'iv_close')


# ---- 中値（AC 7） ----

# (JST の時刻, 窓の中か)。窓は [始まり, 終わり)。取引の日 sd（JST − 7 時間の日付）の時代で切り替える
WINDOW_CASES = [
    # sd < 2021-09-21: 日中 [9:00, 15:10)・夜間 [16:30, 翌 5:25)
    ('2021-09-16 08:59', False), ('2021-09-16 09:00', True), ('2021-09-16 15:09', True), ('2021-09-16 15:10', False),
    ('2021-09-16 15:14', False), ('2021-09-16 16:29', False), ('2021-09-16 16:30', True), ('2021-09-17 05:24', True),
    ('2021-09-17 05:25', False),
    # 時代の境目: 2021-09-21 の朝 5 時台は 09-20 の夜間（前の時代。5:25 で終わる）、09-21 の日中から 8:45 に始まる
    ('2021-09-21 05:30', False), ('2021-09-21 08:44', False), ('2021-09-21 08:45', True), ('2021-09-21 08:50', True),
    # 2021-09-21 ≤ sd < 2024-11-05: 日中 [8:45, 15:10)・夜間 [16:30, 翌 5:55)
    ('2021-09-21 15:09', True), ('2021-09-21 15:10', False), ('2021-09-21 15:14', False), ('2021-09-21 16:29', False),
    ('2021-09-21 16:30', True), ('2021-09-22 05:54', True), ('2021-09-22 05:55', False), ('2021-09-22 05:59', False),
    # 時代の境目: 11-04 は前の時代（15:10 まで・16:30 から）、11-05 の朝 5 時台は 11-04 の夜間（5:55 まで）
    ('2024-11-04 15:20', False), ('2024-11-04 16:45', True), ('2024-11-05 05:54', True), ('2024-11-05 05:55', False),
    # sd ≥ 2024-11-05: 日中 [8:45, 15:40)・夜間 [17:00, 翌 5:55)
    ('2024-11-05 08:44', False), ('2024-11-05 08:45', True), ('2024-11-05 15:20', True), ('2024-11-05 15:39', True),
    ('2024-11-05 15:40', False), ('2024-11-05 15:45', False), ('2024-11-05 16:45', False), ('2024-11-05 16:59', False),
    ('2024-11-05 17:00', True), ('2024-11-06 05:54', True), ('2024-11-06 05:55', False),
]
# (bid, ask, 使うか)。2024-11-05 10:00（窓の中）
QUOTE_CASES = [(None, 110, False), (100, None, False), (120, 110, False), (0, 10, False), (10, 0, False), (0, 0, False),
               (100, 100, True), (99, 100, True)]


def test_mid_conditions():
    """窓の中の bid > 0・ask > 0・bid ≤ ask の行だけが中値になる。該当の無い足も行はあり、中値・終値の気配の列が NULL"""
    rows = [row(at, k=50000 + 10 * i) for i, (at, _) in enumerate(WINDOW_CASES)]
    rows += [row('2024-11-05 10:00', k=40000 + 10 * i, bid=b, ask=a) for i, (b, a, _) in enumerate(QUOTE_CASES)]
    got = run_bars(rows, '2021-09-16 07:00', '2024-11-07 07:00')
    assert len(got) == len(rows)
    cases = [(r, ok, (r['bid'] + r['ask']) / 2 if ok else None)
             for r, ok in zip(rows, [ok for _, ok in WINDOW_CASES] + [ok for _, _, ok in QUOTE_CASES], strict=True)]
    for r, ok, mid in cases:
        [bar] = [b for b in got.values() if b['target_price'] == r['target_price']]
        assert bar['snapshots'] == 1 and bar['last_at'] is not None, r['created_at']
        if ok:
            assert [bar[c] for c in QUOTE_NULL] == [mid, mid, mid, mid, r['bid'], r['ask'], 20.0, 21.0], r['created_at']
        else:
            assert all(bar[c] is None for c in QUOTE_NULL), (r['created_at'], r['bid'], r['ask'])


def bar_rows(k):
    """1 つの足（2026-10-13 10 時台 JST）の気配。同じ created_at の行が 2 つある時刻（旧系の重複）を含む"""
    return [
        row('2026-10-13 10:00', k, bid=100, ask=110, volume=5, positions=7),
        row('2026-10-13 10:00', k, bid=90, ask=110, volume=4, positions=6),           # 同じ時刻: bid の小さい方が始値（中値 100）
        row('2026-10-13 10:20', k, bid=120, ask=130),                                   # 高値 125
        row('2026-10-13 10:30', k, bid=50, ask=60, volume=8),                           # 安値 55
        row('2026-10-13 10:40', k, bid=200, ask=190, volume=9, positions=8),          # bid > ask は使わない（使えば高値 195）
        row('2026-10-13 10:50', k, bid=80, ask=90, bid_iv=18.0, ask_iv=19.0),
        row('2026-10-13 10:50', k, bid=82, ask=86, bid_iv=18.5, ask_iv=19.5),         # 同じ時刻: bid の大きい方が終値（中値 84）
        row('2026-10-13 10:59', k, bid=None, ask=95, volume=None, positions=None),    # 片側の気配。足の最後の行（volume は NULL）
    ]


def test_mid_ohlc_volume_positions_snapshots():
    """中値の OHLC と終値の行の気配（AC 7）、volume・positions・snapshots・last_at（AC 10）。入力の順に左右されない"""
    rows = bar_rows(60000) + list(reversed(bar_rows(60500)))
    # volume: 最後の行の値（同じ時刻は大きい方）。positions: 最後の NULL でない値
    rows += [row('2026-10-13 10:00', 61000, volume=5, positions=7), row('2026-10-13 10:10', 61000, volume=8),
             row('2026-10-13 10:10', 61000, volume=6)]
    got = run_bars(rows, '2026-10-13 07:00', '2026-10-14 07:00')
    for k in (60000, 60500):
        bar = got[('2026-10-13 01:00', M1, k, 2)]
        assert [bar[c] for c in QUOTE_NULL] == [100.0, 125.0, 55.0, 84.0, 82, 86, 18.5, 19.5], k
        assert (bar['volume'], bar['positions'], bar['snapshots'], bar['last_at']) == (None, 8, 6, '2026-10-13 01:59:00'), k
        assert all(bar[c] is None for c in TRADE_NULL)
    bar = got[('2026-10-13 01:00', M1, 61000, 2)]
    assert (bar['volume'], bar['positions'], bar['snapshots'], bar['last_at']) == (8, 7, 2, '2026-10-13 01:10:00')


# ---- 約定（AC 8・9・13・14） ----

def trade_rows(k=62000):
    """(created_at, price, price_time, iv)。気配はどの行にもある"""
    return [row(at, k, price=p, pt=pt, iv=iv) for at, p, pt, iv in [
        ('2026-10-13 10:58', 100, '2026-10-13 10:58', 20.0),
        ('2026-10-13 10:59', 101, '2026-10-13 10:59', 20.1),   # 10:59 の分の途中のスナップショット
        ('2026-10-13 11:00', 102, '2026-10-13 10:59', 20.2),   # 10:59 の分の 2 回目（分が終わった後の最初）= 10 時台の終値
        ('2026-10-13 11:01', 102, '2026-10-13 10:59', 20.9),   # 前の足の持ち越し（10 時台の約定。11 時台には入れない）
        ('2026-10-13 11:05', 103, '2026-10-13 11:05', 21.0),
        ('2026-10-13 11:06', 103, '2026-10-13 11:05', 21.5),   # 11:05 の分が終わった後の最初 = 11 時台の終値（iv も）
        ('2026-10-13 15:46', 110, '2026-10-13 15:45', 22.0),   # 日中の終わりの 15:45 の約定（引け）は 15 時台
        ('2026-10-13 17:00', 999, '2026-10-13 15:45', 22.5),   # 夕方の日中の約定の持ち越し（別の立会）: どの足にも入れない
        ('2026-10-14 05:56', 120, '2026-10-14 05:55', 23.0),   # 夜間の終わりの約定は 05 時台
        ('2026-10-14 08:45', 998, '2026-10-14 05:55', 23.5),   # 朝の夜間の約定の持ち越し（別の立会）
        ('2026-10-17 05:56', 130, '2026-10-17 05:55', 24.0),   # 金曜の夜間の終わり（土曜の朝）
        ('2026-10-19 08:45', 997, '2026-10-17 05:55', 24.5),   # 週明けの朝の金曜夜間の約定の持ち越し
    ]]


def test_trade_bars_by_price_time():
    got = run_bars(trade_rows(), '2026-10-13 07:00', '2026-10-20 07:00')
    trades = {k[0]: tuple(b[c] for c in TRADE_NULL) for k, b in got.items()}
    assert trades == {
        utc_hour('2026-10-13 10'): (100, 102, 100, 102, 20.2),
        utc_hour('2026-10-13 11'): (103, 103, 103, 103, 21.5),
        utc_hour('2026-10-13 15'): (110, 110, 110, 110, 22.0),
        utc_hour('2026-10-13 17'): (None,) * 5,
        utc_hour('2026-10-14 05'): (120, 120, 120, 120, 23.0),
        utc_hour('2026-10-14 08'): (None,) * 5,
        utc_hour('2026-10-17 05'): (130, 130, 130, 130, 24.0),
        utc_hour('2026-10-19 08'): (None,) * 5,
    }
    # 気配・数の列は created_at の足（11 時台は 4 回、15 時台は窓の外の 15:46 の 1 回で中値は NULL）
    assert got[(utc_hour('2026-10-13 11'), M1, 62000, 2)]['snapshots'] == 4
    bar = got[(utc_hour('2026-10-13 15'), M1, 62000, 2)]
    assert bar['snapshots'] == 1 and bar['mid_close'] is None


def test_trade_bar_before_window_is_not_written():
    """窓の始まりより前の足の約定（窓の中の行が持ち越した分）は出さない（増分の窓の始まりで前の足を壊さない）"""
    got = run_bars(trade_rows(), '2026-10-13 11:00', '2026-10-13 12:00')
    assert sorted(got) == [(utc_hour('2026-10-13 11'), M1, 62000, 2)]
    assert tuple(got[(utc_hour('2026-10-13 11'), M1, 62000, 2)][c] for c in TRADE_NULL) == (103, 103, 103, 103, 21.5)


def night_rows():
    return [
        row('2026-10-13 23:30', 63000, 1, price=50, pt='2026-10-13 23:30', iv=30.0),
        row('2026-10-14 00:30', 63000, 1, price=51, pt='2026-10-14 00:30', iv=31.0),
        # UTC の日・月の境目（12-01 09:00 JST = 12-01 00:00 UTC）をまたぐ日中の足
        row('2026-12-01 08:50', 63000, 1, month='2026-12-10', price=60, pt='2026-12-01 08:50', iv=32.0),
        row('2026-12-01 09:10', 63000, 1, month='2026-12-10', price=60, pt='2026-12-01 08:50', iv=32.5),
        row('2026-12-01 09:20', 63000, 1, month='2026-12-10', bid=120, ask=130, price=61, pt='2026-12-01 09:20', iv=33.0),
    ]


def test_night_crosses_midnight():
    """夜間の 23 時台と 0 時台（JST）は別の行で、started_at は UTC の 14 時・15 時。UTC の日・月の境目をまたぐ窓でも同じ結果"""
    got = run_bars(night_rows(), '2026-10-13 07:00', '2026-12-02 07:00')
    assert sorted(k[0] for k in got) == ['2026-10-13 14:00', '2026-10-13 15:00', '2026-11-30 23:00', '2026-12-01 00:00']
    assert got[('2026-10-13 14:00', M1, 63000, 1)]['trade_close'] == 50
    assert got[('2026-10-13 15:00', M1, 63000, 1)]['trade_close'] == 51
    assert got[('2026-11-30 23:00', '2026-12-10', 63000, 1)]['trade_close'] == 60
    assert got[('2026-11-30 23:00', '2026-12-10', 63000, 1)]['iv_close'] == 32.5
    assert got[('2026-12-01 00:00', '2026-12-10', 63000, 1)]['trade_open'] == 61
    assert got[('2026-12-01 00:00', '2026-12-10', 63000, 1)]['mid_close'] == 125.0
    # 1 日の窓（07:00 JST〜。UTC の 11-30 と 12-01 にまたがる）だけで流しても同じ
    one_day = run_bars(night_rows(), '2026-12-01 07:00', '2026-12-02 07:00')
    assert one_day == {k: v for k, v in got.items() if k[1] == '2026-12-10'}


def test_fill114_rows():
    """#114 の行（気配・気配の IV が NULL、約定だけ）の足は中値・終値の気配が NULL で約定が入る"""
    rows = [row(at, 27500, 2, month='2023-03-09', bid=None, ask=None, bid_iv=None, ask_iv=None, price=p, pt=pt, iv=iv, volume=v)
            for at, p, pt, iv, v in [('2023-03-01 10:05', 300, '2023-03-01 10:04', 25.0, 10),
                                     ('2023-03-01 10:30', 310, '2023-03-01 10:29', 25.5, 20)]]
    [bar] = run_bars(rows, '2023-03-01 07:00', '2023-03-02 07:00').values()
    assert all(bar[c] is None for c in QUOTE_NULL)
    assert tuple(bar[c] for c in TRADE_NULL) == (300, 310, 300, 310, 25.5)
    assert (bar['volume'], bar['snapshots']) == (20, 2)


# ---- 最新版の選び方（AC 11） ----

OPTION = SRC + [('ingest_run_id', 'STRING')]
LATEST_RUNS = [('ingest_run_id', 'STRING'), ('snapshot_at', 'TIMESTAMP'), ('finished_at', 'TIMESTAMP')]


def test_latest_run_selection():
    """source の本文はそのまま、option_price_latest の本文を一時テーブルに当てる: 同じスナップショットの完了 2 回は finished_at の
    新しい方、ingest_runs に無い実行の行（書き込みの途中で失敗した残骸）と type が NULL の行は使わない"""
    s1, s2, s3 = '2026-10-13 10:00', '2026-10-13 10:10', '2026-10-13 10:20'
    option = [dict(row(s1, 64000, bid=100, ask=110), ingest_run_id='r1'),
              dict(row(s1, 64000, bid=200, ask=210), ingest_run_id='r2'),
              dict(row(s2, 64000, bid=300, ask=310), ingest_run_id='orphan'),
              dict(row(s3, 64500, typ=None, bid=400, ask=410), ingest_run_id='r3')]
    runs = [{'ingest_run_id': 'r1', 'snapshot_at': jst(s1), 'finished_at': jst('2026-10-13 10:01')},
            {'ingest_run_id': 'r2', 'snapshot_at': jst(s1), 'finished_at': jst('2026-10-13 10:05')},
            {'ingest_run_id': 'r3', 'snapshot_at': jst(s3), 'finished_at': jst('2026-10-13 10:21')}]
    latest = '\n'.join(line for line in read(LATEST).splitlines() if not line.startswith('--'))
    latest = re.sub(r'\bstart_at\b', 'win_start', re.sub(r'\bend_at\b', 'win_end', latest))
    text = tables_to_temp(between(read(DAILY), '-- result:begin', '-- result:end').group(1))
    text = text.replace('option_price_latest(win_start, win_end)', f'(\n{tables_to_temp(latest)}\n)')
    script = (f"DECLARE win_start TIMESTAMP DEFAULT TIMESTAMP '{jst('2026-10-13 07:00')}';\n"
              f"DECLARE win_end TIMESTAMP DEFAULT TIMESTAMP '{jst('2026-10-14 07:00')}';\n"
              + temp_table('option_price', OPTION, option) + temp_table('ingest_runs', LATEST_RUNS, runs) + text + ';\n')
    [rows] = run_script(script)
    [bar] = [value(r) for r in rows]
    assert key(bar) == ('2026-10-13 01:00', M1, 64000, 2)
    assert (bar['mid_close'], bar['snapshots'], bar['last_at']) == (205.0, 1, '2026-10-13 01:00:00')


# ---- 毎時の定期クエリ（AC 12・13。一時テーブルに書く） ----

RUN_TIME = '2026-10-13T05:45:00Z'   # 増分の回: 2026-10-13（火）14:45 JST


def utc(day, hour, minute=0):
    return f'2026-10-{day:02d} {hour:02d}:{minute:02d}:00+00'


def hourly_table(rows):
    """表の代わりの一時テーブル（本番と同じ last_trading_day の月パーティションとクラスタ。require_partition_filter は付けられない）"""
    struct = ', '.join(f'{c} {t}' for c, t in HOURLY)
    values = ', '.join('STRUCT(' + ', '.join(literal(r.get(c), t) for c, t in HOURLY) + ')' for r in rows)
    return ('CREATE TEMP TABLE option_price_hourly PARTITION BY DATE_TRUNC(last_trading_day, MONTH) '
            f'CLUSTER BY target_price, type, started_at AS SELECT * FROM UNNEST(ARRAY<STRUCT<{struct}>>[{values}]);\n')


def bar(started_at, k=60000, month=M1, mid=1.0, typ=2):
    return {'started_at': started_at, 'last_trading_day': month, 'target_price': k, 'type': typ, 'mid_close': mid, 'snapshots': 1}


AFTER = ('\nSELECT win_start, win_end, ARRAY(SELECT AS STRUCT * FROM option_price_hourly WHERE last_trading_day >= DATE "2000-01-01" '
         'ORDER BY started_at, last_trading_day, target_price, type) AS after;\n')


def hourly_script(existing, runs, rows=(), upto=False):
    """option_price_hourly.sql を、source を固定値・表と ingest_runs を一時テーブルにしたスクリプト。upto なら窓を決めたところで
    win_start・win_end・rebuild を返す。それ以外は最後に窓と表の中身を返す。どちらも何もしない回（RETURN）は出力が無い"""
    text = render(tables_to_temp(replace_source(read(DAILY), fixed_sources(list(rows)))), project=PROJECT, dataset='optionchan_tokyo')
    declares, rest = split_declares(text)
    setup = hourly_table(existing) + temp_table('ingest_runs', RUNS, runs)
    if upto:
        return declares + setup + rest[:rest.index('CREATE TEMP TABLE result')] + 'SELECT win_start, win_end, rebuild;\n'
    return declares + setup + rest + AFTER


def run_window(run_time, existing, runs):
    out = run_script(hourly_script(existing, runs, upto=True), params=[f'run_time:TIMESTAMP:{run_time}'])
    if not out:
        return None
    [[r]] = out
    return r['win_start'][:16], r['win_end'][:16], r['rebuild'] == 'true'


def run_hourly(run_time, existing, runs, rows):
    """1 回分を流す。(win_start, win_end, 表の行) か、何もしない回は None"""
    out = run_script(hourly_script(existing, runs, rows), params=[f'run_time:TIMESTAMP:{run_time}'])
    if not out:
        return None
    [[r]] = out
    return r['win_start'][:16], r['win_end'][:16], [value(x) for x in r['after']]


def fresh(*snapshots):
    """前の回の後に終わった取り込み（finished_at は RUN_TIME の 5 分前）"""
    return [{'snapshot_at': at, 'finished_at': utc(13, 5, 40)} for at in snapshots]


@pytest.mark.parametrize('existing,expected', [
    # 増分: 表の最後の足 − 2 時間から（失敗した回の分もここから埋まる）
    ([bar(utc(13, 1)), bar(utc(13, 2))], '2026-10-13 00:00'),
    # 3 日の上限: 最後の足 − 2 時間が 3 日前（の正時）より前なら 3 日前の正時から
    ([bar(utc(10, 6))], '2026-10-10 05:00'),
    ([], '2026-10-10 05:00'),
    ([bar(utc(9, 5))], '2026-10-10 05:00'),
    # 表に run_time より後の足（過去の run_time の手動実行）→ それを除いた最後の足から
    ([bar(utc(13, 3)), bar(utc(14, 1))], '2026-10-13 01:00'),
], ids=['incremental', 'limit_3_days', 'empty', 'only_old', 'rows_after_run_time'])
def test_hourly_window(existing, expected):
    assert run_window(RUN_TIME, existing, fresh(utc(13, 5, 30))) == (expected, '2026-10-13 05:45', False)


def test_rebuild_window_at_7_jst():
    # 10-14 07:45 JST の回: [10-13 07:00, 10-14 07:00) JST。新しい取り込みが無くても作り直す
    assert run_window('2026-10-13T22:45:00Z', [bar(utc(13, 21))], []) == ('2026-10-12 22:00', '2026-10-13 22:00', True)
    # 6 時台・8 時台は増分
    runs = [{'snapshot_at': utc(13, 21), 'finished_at': utc(13, 21, 1)}]
    assert run_window('2026-10-13T21:45:00Z', [bar(utc(13, 20))], runs) == ('2026-10-13 18:00', '2026-10-13 21:45', False)
    runs = [{'snapshot_at': utc(13, 23), 'finished_at': utc(13, 23, 1)}]
    assert run_window('2026-10-13T23:45:00Z', [bar(utc(13, 21))], runs) == ('2026-10-13 19:00', '2026-10-13 23:45', False)


@pytest.mark.parametrize('runs', [
    [{'snapshot_at': utc(13, 4, 30), 'finished_at': utc(13, 4, 39)}],
    [{'snapshot_at': utc(10, 4), 'finished_at': utc(13, 5, 40)}],
    [],
], ids=['no_new_run', 'only_old_snapshot', 'no_runs'])
def test_hourly_does_nothing_without_new_ingest(runs):
    existing = [bar(utc(13, 4))]
    assert run_window(RUN_TIME, existing, runs) is None
    assert run_hourly(RUN_TIME, existing, runs, [row('2026-10-13 14:00')]) is None


def test_rejects_future_run_time():
    declares, rest = split_declares(render(read(DAILY), project=PROJECT, dataset='optionchan_tokyo'))
    guard = rest[:rest.index('IF NOT rebuild')]
    assert 'is in the future' in run_error(declares + guard, params=['run_time:TIMESTAMP:2099-01-01T00:45:00Z'])


def snapshot_rows(at, i):
    """取り込み 1 回分（JST の at）: 2 限月 × 2 行使価格 × call / put。i で気配・約定・iv を変える。約定は 13:00・17:00 JST から
    25 分ごとに起き、その間のスナップショットは前の約定を持ち越す（iv はスナップショットごとに変わる）"""
    t = datetime.strptime(at, '%Y-%m-%d %H:%M')
    s = t - timedelta(hours=7)
    session_start = s.replace(hour=6, minute=0) if s.hour < 9 else s.replace(hour=10, minute=0)   # 13:00・17:00 JST から
    traded = session_start + timedelta(minutes=25 * ((s - session_start) // timedelta(minutes=25)))
    pt = (traded + timedelta(hours=7)).strftime('%Y-%m-%d %H:%M')
    n = int((traded - session_start) / timedelta(minutes=25))
    out = []
    for month in (M1, '2026-12-10'):
        for k in (60000, 60500):
            for typ in (1, 2):
                base = k // 100 + typ * 10 + (month == M1) * 5
                out.append(row(at, k, typ, month, bid=base + i, ask=base + i + 4 + i % 3, bid_iv=20 + i / 10, ask_iv=21 + i / 10,
                               price=base + n, pt=pt, iv=25 + i / 7, volume=10 * i, positions=None if i % 4 else 100 + i))
    return out


def day_inputs():
    """10-13 の日中の後半・夜間（10-13 07:00〜10-14 07:00 JST の窓）の 10 分おきのスナップショット。取り込みは 1 分後に終わる"""
    ats = []
    for start, end in (('2026-10-13 13:00', '2026-10-13 15:40'), ('2026-10-13 17:00', '2026-10-14 05:50')):
        t, e = datetime.strptime(start, '%Y-%m-%d %H:%M'), datetime.strptime(end, '%Y-%m-%d %H:%M')
        while t <= e:
            ats.append(t.strftime('%Y-%m-%d %H:%M'))
            t += timedelta(minutes=10)
    ats.append('2026-10-13 15:46')   # 日中の引けの後（15:45 の約定）
    rows = {at: snapshot_rows(at, i) for i, at in enumerate(sorted(ats))}
    rows['2026-10-13 15:46'] = [dict(r, price_time='2026-10-13 15:45:00') for r in rows['2026-10-13 15:46']]
    # 夕方の最初のスナップショットは日中の引けの約定を持ち越す（別の立会。数えない）
    rows['2026-10-13 17:00'] = [dict(r, price_time='2026-10-13 15:45:00', price=1) for r in rows['2026-10-13 17:00']]
    finished = {at: (datetime.strptime(at, '%Y-%m-%d %H:%M') + timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M') for at in ats}
    return ats, rows, finished


def inputs_at(run_time, ats, rows, finished):
    """run_time（JST）までに終わった取り込みだけ（option_price_latest は書き終わった実行の行だけを返す）"""
    done = [at for at in ats if finished[at] <= run_time]
    return ([{'snapshot_at': jst(at), 'finished_at': jst(finished[at])} for at in done], [r for at in done for r in rows[at]])


def same_rows(got, expected):
    assert [tuple(r[c] for c in KEY) for r in got] == [tuple(r[c] for c in KEY) for r in expected]
    for g, e in zip(got, expected, strict=True):
        for c, t in HOURLY:
            if t == 'FLOAT64' and e[c] is not None:
                assert g[c] == pytest.approx(e[c], abs=1e-9), (e['started_at'], c)
            else:
                assert g[c] == e[c], (e['started_at'], e['last_trading_day'], e['target_price'], e['type'], c)


def test_incremental_runs_match_daily_window():
    """増分の回を重ねて流した結果は、1 日の窓（7 時台の回）で 1 回流した結果と同じ（夜間の途中から始まる増分の窓・夕方の
    日中の約定の持ち越し・窓の始まりの前の足の約定の持ち越しを含む）"""
    ats, rows, finished = day_inputs()
    table = []
    for run_time in ('2026-10-13 15:45', '2026-10-13 17:45', '2026-10-13 20:45', '2026-10-13 23:45', '2026-10-14 02:45',
                     '2026-10-14 05:45', '2026-10-14 06:45'):
        runs, src = inputs_at(run_time, ats, rows, finished)
        out = run_hourly(jst(run_time), table, runs, src)
        assert out, run_time
        table = out[2]
    runs, src = inputs_at('2026-10-14 07:45', ats, rows, finished)
    start, end, daily = run_hourly(jst('2026-10-14 07:45'), [], runs, src)
    assert (start, end) == ('2026-10-12 22:00', '2026-10-13 22:00')
    # 日中 13〜15 時・夜間 17〜05 時（13 + 3 = 16 時間）× 8 銘柄。trade の持ち越しだけの足は無い
    assert len(daily) == 16 * 8
    assert all(r['snapshots'] > 0 for r in daily)
    assert any(r['trade_close'] is not None for r in daily) and any(r['mid_close'] is not None for r in daily)
    # 夕方の持ち越し（price = 1）はどの足にも入らない
    assert all(r['trade_low'] != 1 for r in daily)
    same_rows(table, daily)
    # 続けて 7 時台の回: 1 日の窓だけの結果と同じ。窓の後の足（07:00 JST の足）は残る
    after = value(bar('2026-10-13 22:00:00', mid=5.0))
    _, _, rebuilt = run_hourly(jst('2026-10-14 07:45'), table + [after], runs, src)
    same_rows(rebuilt, daily + [after])


def test_idempotent_and_replaces_window():
    # 表の最後の足 03:00 → 窓 [01:00, 05:45)。窓の中の古い足（計算に無い 02:00 を含む）は置き換え、窓の前の足は残す
    existing = [value(bar(utc(12, 20)[:19], mid=7.0)), value(bar(utc(13, 2)[:19], mid=9.0)),
                value(bar(utc(13, 3)[:19], mid=9.0))]
    ats = ['2026-10-13 09:40', '2026-10-13 10:20', '2026-10-13 12:00', '2026-10-13 13:20']
    # 約定はスナップショットの分（持ち越しなし。足は気配と同じ）
    src = [dict(r, price_time=f'{at}:00') for i, at in enumerate(ats) for r in snapshot_rows(at, i)
           if r['last_trading_day'] == M1 and r['target_price'] == 60000 and r['type'] == 2]
    runs = fresh(*[jst(at) for at in ats])
    start, end, first = run_hourly(RUN_TIME, existing, runs, src)
    assert (start, end) == ('2026-10-13 01:00', '2026-10-13 05:45')
    assert [r['started_at'][:16] for r in first] == ['2026-10-12 20:00', '2026-10-13 01:00', '2026-10-13 03:00', '2026-10-13 04:00']
    assert first[0]['mid_close'] == 7.0 and first[0]['snapshots'] == 1
    assert all(r['mid_close'] not in (9.0, None) for r in first[1:])
    # 同じ入力で 2 回目: 窓は 02:00 からになるが、同じ最新版からの計算なので行数・内容は同じ（キーの重複 0）
    start, end, second = run_hourly(RUN_TIME, first, runs, src)
    assert (start, end) == ('2026-10-13 02:00', '2026-10-13 05:45')
    same_rows(second, first)
    assert len({tuple(r[c] for c in KEY) for r in second}) == len(second)


def test_empty_result_keeps_table():
    """窓に結果の行が 1 件も無い回（休場日・データの無い過去の日の手動実行）は表を変えない"""
    existing = [value(bar('2026-10-13 01:00:00', mid=3.0))]
    start, end, after = run_hourly('2026-10-13T22:45:00Z', existing, [], [])
    assert (start, end) == ('2026-10-12 22:00', '2026-10-13 22:00')
    same_rows(after, existing)


def test_last_trading_day_ends_after_day_session():
    """限月の取引最終日（10-08）は日中の後の足が無い（元の行が無い）。翌の限月の足は夜間も続く。限月の範囲の検査は通る"""
    old, new = '2026-10-08', M1
    src = [row(at, 60000, 2, old) for at in ('2026-10-08 14:30', '2026-10-08 15:30')]
    src += [row('2026-10-08 15:46', 60000, 2, old, price=50, pt='2026-10-08 15:45', iv=40.0)]
    src += [row(at, 60000, 2, new) for at in ('2026-10-08 14:30', '2026-10-08 17:00', '2026-10-09 01:30')]
    _, _, after = run_hourly('2026-10-08T22:45:00Z', [], [], src)
    got = {(r['last_trading_day'], r['started_at'][:16]) for r in after}
    assert got == {(old, '2026-10-08 05:00'), (old, '2026-10-08 06:00'),
                   (new, '2026-10-08 05:00'), (new, '2026-10-08 08:00'), (new, '2026-10-08 16:00')}
    assert [r['trade_close'] for r in after if r['last_trading_day'] == old] == [None, 50]


def test_ltd_out_of_range_is_error():
    """結果に限月の範囲の外の行（最終取引日が足の JST の日付より前）があれば、書く前に ERROR（置き換えの DELETE から漏れて重複する）"""
    src = [row('2026-10-08 10:00', 60000, 2, '2026-10-07')]
    script = hourly_script([], [], src)
    msg = run_error(script, params=['run_time:TIMESTAMP:2026-10-08T22:45:00Z'])
    assert 'option_price_hourly: last_trading_day out of range' in msg


# ---- 過去分の作り直し（hourly_backfill.sql） ----

def backfill_script(existing, rows):
    text = render(tables_to_temp(replace_source(read(BACKFILL), fixed_sources(list(rows)))), project=PROJECT, dataset='optionchan_tokyo')
    declares, rest = split_declares(text)
    return declares + hourly_table(existing) + rest + AFTER


@pytest.mark.parametrize('start,end,message', [
    ('2026-10-01 00:00:00+00', '2026-10-02 00:00:00+00', 'must start and end at 07:00 JST'),   # UTC の日の境目（JST 9 時）
    ('2026-09-30 22:00:00+00', '2026-10-01 23:00:00+00', 'must start and end at 07:00 JST'),
    ('2026-09-30 22:00:00+00', '2099-01-01 22:00:00+00', 'must start and end at 07:00 JST'),   # 未来
    ('2026-10-01 22:00:00+00', '2026-09-30 22:00:00+00', 'bad range'),
    ('2026-10-01 22:00:00+00', '2026-10-01 22:00:00+00', 'bad range'),
], ids=['utc_midnight', 'not_0700_end', 'future', 'reversed', 'empty'])
def test_backfill_rejects_bad_range(start, end, message):
    declares, rest = split_declares(render(read(BACKFILL), project=PROJECT, dataset='optionchan_tokyo'))
    guard = rest[:rest.index('CREATE TEMP TABLE result')]
    assert message in run_error(declares + guard, params=[f'start_at:TIMESTAMP:{start}', f'end_at:TIMESTAMP:{end}'])


def test_backfill_matches_rebuild():
    """1 日の範囲の過去分の作り直しは、定期クエリの 7 時台の回と同じ結果。範囲の外の足は残し、流し直しても同じ（冪等）"""
    ats, rows, finished = day_inputs()
    runs, src = inputs_at('2026-10-14 07:45', ats, rows, finished)
    _, _, daily = run_hourly(jst('2026-10-14 07:45'), [], runs, src)
    before = value(bar('2026-10-12 21:00:00', mid=6.0))
    params = ['start_at:TIMESTAMP:2026-10-12 22:00:00+00', 'end_at:TIMESTAMP:2026-10-13 22:00:00+00']
    table = [before]
    for _ in range(2):
        [[summary], [out]] = run_script(backfill_script(table, src), params=params)[-2:]
        table = [value(x) for x in out['after']]
        assert (int(summary['bars']), int(summary['with_mid'])) == (len(daily), sum(r['mid_close'] is not None for r in daily))
        assert int(summary['with_trade']) == sum(r['trade_close'] is not None for r in daily)
        same_rows(table, [before] + daily)


# ---- 実テーブル（読むだけ・dry run） ----

def test_partition_filter_with_script_variables():
    """スクリプトの変数（DECLARE）での絞り込みは require_partition_filter を満たす（表の MAX・DELETE の ltd_lo・ltd_hi と同じ形）。
    require_partition_filter の付いた option_price を、変数の期間（1 時間）で数えるだけ"""
    sql = ("DECLARE lo TIMESTAMP DEFAULT TIMESTAMP '2026-10-29 01:00:00+00';\n"
           "DECLARE hi TIMESTAMP DEFAULT TIMESTAMP_ADD(lo, INTERVAL 1 HOUR);\n"
           f"SELECT COUNT(*) > 0 AS ok FROM `{PROJECT}.optionchan_tokyo.option_price` WHERE created_at >= lo AND created_at < hi;\n")
    [[r]] = run_script(sql, maximum_bytes=100 * MiB)
    assert r['ok'] == 'true'


def source_query(path):
    """結果の SELECT（元の行を読む部分）の窓をクエリパラメータにしたもの（スクリプトの dry run は見積もりを返さない）"""
    text = render(between(read(path), '-- result:begin', '-- result:end').group(1), project=PROJECT, dataset='optionchan_tokyo')
    return re.sub(r'\b(win_start|win_end)\b', r'@\1', text)


@pytest.mark.parametrize('path,start,end,limit', [
    # 作り直しの回（1 日の窓 = UTC の 2 パーティション）・増分の回（2 時間 45 分 = 1 パーティション）・過去分の 1 か月
    (DAILY, '2026-10-28 22:00:00+00', '2026-10-29 22:00:00+00', 300 * MiB),
    (DAILY, '2026-10-29 03:00:00+00', '2026-10-29 05:45:00+00', 150 * MiB),
    (BACKFILL, '2026-09-30 22:00:00+00', '2026-10-31 22:00:00+00', 3 * GiB),
], ids=['rebuild', 'incremental', 'backfill_month'])
def test_dry_run_source(path, start, end, limit):
    out = bq(source_query(path), dry_run=True, params=[f'win_start:TIMESTAMP:{start}', f'win_end:TIMESTAMP:{end}'])
    print(f'dry run {os.path.basename(path)} [{start}, {end}): {out / MiB:.1f} MiB')
    assert 0 < out < limit, out


@pytest.mark.parametrize('path,params', [
    (DAILY, ['run_time:TIMESTAMP:2026-10-29T22:45:00Z']),
    (BACKFILL, ['start_at:TIMESTAMP:2026-09-30 22:00:00+00', 'end_at:TIMESTAMP:2026-10-31 22:00:00+00']),
], ids=['option_price_hourly', 'hourly_backfill'])
def test_dry_run_whole_script(path, params):
    """スクリプト全体（DECLARE・IF・RETURN・CREATE TEMP TABLE・トランザクション・DELETE・INSERT）の構文。スクリプトの dry run は
    最初の文しか検証せず、何も実行しない（表がまだ無くても通る）"""
    assert bq(render(read(path), project=PROJECT, dataset='optionchan_tokyo'), dry_run=True, params=params) >= 0


def prod_has_table():
    out = subprocess.run(['bq', f'--project_id={PROJECT}', f'--location={TOKYO}', 'show', '--format=json',
                          f'{PROJECT}:optionchan_tokyo.option_price_hourly'], capture_output=True, text=True, timeout=60)
    return out.returncode == 0


def test_dry_run_table_statements():
    """表を読み書きする文（MAX・DELETE・INSERT）を 1 文ずつ本番の表に対して dry run する（require_partition_filter の表で限月の
    範囲の絞り込みが通ること。dry run は書かない）。表ができる（deploy の apply の）前は飛ばす"""
    if not prod_has_table():
        pytest.skip('option_price_hourly がまだ無い（Terraform の apply の後に流す）')
    text = render(read(DAILY), project=PROJECT, dataset='optionchan_tokyo')
    params = ['win_start:TIMESTAMP:2026-10-28 22:00:00+00', 'win_end:TIMESTAMP:2026-10-29 22:00:00+00',
              'ltd_lo:DATE:2026-10-29', 'ltd_hi:DATE:2027-10-30']
    table = f'`{PROJECT}.optionchan_tokyo.option_price_hourly`'
    select_max = re.search(r'\(\n\s+(SELECT MAX\(started_at\).*?)\n  \), INTERVAL 2 HOUR\)', text, re.S).group(1)
    delete = re.search(r'^  (DELETE FROM .*?;)$', text, re.S | re.M).group(1)
    insert = re.search(r'^  (INSERT INTO .*?)SELECT \* FROM result;$', text, re.S | re.M).group(1)
    insert += 'SELECT * FROM (\n' + between(text, '-- result:begin', '-- result:end').group(1) + ')'
    for name, sql in (('max', select_max), ('delete', delete), ('insert', insert)):
        assert table in sql, name
        sql = re.sub(r'\b(win_start|win_end|ltd_lo|ltd_hi)\b', r'@\1', sql)
        out = bq(sql, dry_run=True, params=params)
        print(f'dry run {name}: {out / MiB:.1f} MiB')
        assert out >= 0, name

"""過去データの欠けの一覧（#110）の SQL を BigQuery で実際に動かす検査（tests/test_tick_build_bq.py と同じ流儀）。

CI では動かさない（BigQuery の認証が要る）。手元で `OPTIONCHAN_BQ_TEST=1 OPTIONCHAN_BQ_PROJECT=<PROJECT_ID> python -m pytest -q tests/test_gaps_bq.py`。
入力はすべて合成した値（旧系・DataCube のデータは使わない。先物の値・前日比は架空の区切りの値）。
- 一覧の検査: gaps.sql を、本番の表の代わりにスクリプトの一時テーブル（trading_calendar_latest・ingest_runs・spot_price・future_price）に
  対して流す（本番の表は読まない・書かない）。区間のまとまり（グループ）ごとに範囲を替えて 1 つのスクリプトで続けて流し、グループの間に
  印の SELECT を挟んで結果を分ける。原因 4 種（前後の前日比・修正の時刻の前後・値の一致・行が無い）・状態 3 種（90% の境目）・
  立会の時間の変更日の前後（2021-09-21 の夜間の終わり、2024-11-05 の日中の終わり・夜間の始まり）・2021-09-17 の夜間・週末・休業日・
  祝日取引の日・立会中の分が 14 分と 15 分の区間を、手で数えた分の数と比べる
- ガード: 範囲の逆順・カレンダーの欠けで止まる
- 実テーブルの dry run: スクリプト全体の構文
"""
import os
import subprocess
from datetime import datetime, timedelta

import pytest

from test_price_time_fix_bq import schema_cols, temp_table
from test_sql_bq import PROJECT, TOKYO, bq, read, render
from test_tick_build_bq import run_script, to_temp

pytestmark = pytest.mark.skipif(
    os.environ.get('OPTIONCHAN_BQ_TEST') != '1' or not PROJECT,
    reason='OPTIONCHAN_BQ_TEST=1 と OPTIONCHAN_BQ_PROJECT のときだけ',
)

ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
GAPS = os.path.join(ROOT, 'sql', 'gaps', 'gaps.sql')
NAMES = {'dataset': 'optionchan_tokyo'}

# 取引カレンダーの行: 年始（missing_new_year の検査）、2021-09-20・2024-11-04 は休業日（平日）、2024-03-20 は祝日取引（先物は取引日）
CALENDAR = {'2020-01-02': False, '2021-01-01': False, '2022-01-03': False, '2023-01-02': False, '2024-01-02': False,
            '2021-09-20': False, '2024-11-04': False, '2024-03-20': True}


def utc(c):
    """JST 'YYYY-MM-DD HH:MM' → UTC の TIMESTAMP の文字列"""
    return (datetime.strptime(c, '%Y-%m-%d %H:%M') - timedelta(hours=9)).strftime('%Y-%m-%d %H:%M:00+00')


def run_id(prefix, c):
    return prefix + (datetime.strptime(c, '%Y-%m-%d %H:%M') - timedelta(hours=9)).strftime('%Y%m%dT%H%M%SZ')


def minutes(start, n):
    t0 = datetime.strptime(start, '%Y-%m-%d %H:%M')
    return [(t0 + timedelta(minutes=i)).strftime('%Y-%m-%d %H:%M') for i in range(n)]


def run_row(c, prefix, trigger):
    at = utc(c)
    return {'ingest_run_id': run_id(prefix, c), 'snapshot_at': at, 'source_object': 'legacy:optionchan', 'parser_version': 'legacy',
            'trigger': trigger, 'rows_spot': 1, 'rows_future': 1, 'rows_option': 1, 'started_at': at, 'finished_at': at}


class Data:
    def __init__(self):
        self.runs, self.future, self.spot = [], [], []

    def legacy(self, c, price=20000, diff=100, spot_diff=100, rows=True):
        """旧系のスナップショット（JST）と、その先物・現物の行（rows=False なら ingest_runs の行だけ）"""
        self.runs.append(run_row(c, 'legacy-', 'legacy_import'))
        if rows:
            self.future.append({'price': price, 'diff': diff, 'contract_month': '2030-12-01', 'created_at': utc(c),
                                'ingest_run_id': run_id('legacy-', c)})
            self.spot.append({'price': 20000, 'diff': spot_diff, 'created_at': utc(c), 'ingest_run_id': run_id('legacy-', c)})

    def fill(self, start, n):
        """#114 の作ったスナップショット（JST の分の終わり start から n 分）"""
        for c in minutes(start, n):
            r = run_row(c, 'legacy-fill114-', 'legacy_fill114')
            self.runs.append({**r, 'source_object': 'datacube:tick', 'parser_version': 'legacy-fill114-1'})

    def tables(self, calendar=CALENDAR):
        cal = [{'calendar_date': d, 'is_trading_day': t} for d, t in calendar.items()]
        return (
            temp_table('trading_calendar_latest', [('calendar_date', 'DATE'), ('is_trading_day', 'BOOL')], cal)
            + temp_table('ingest_runs', schema_cols('schema/tokyo/ingest_runs.json'), self.runs, 'snapshot_at')
            + temp_table('spot_price', schema_cols('schema/tokyo/spot_price.json'), self.spot, 'created_at')
            + temp_table('future_price', schema_cols('schema/tokyo/future_price.json'), self.future, 'created_at')
        )


D = Data()
# グループ（範囲）ごとの期待値: 区間の prev（JST）→ (gap_minutes, session_minutes, built_minutes, cause, status)
GROUPS = []

# A: 2020-06-10（水）の日中。原因・状態
D.legacy('2020-06-10 10:00', price=20000, diff=750)                  # 直前の先物の前日比 750 → bug_comma
D.legacy('2020-06-10 10:30', price=20100)
D.legacy('2020-06-10 10:40', price=20100)
D.legacy('2020-06-10 11:10', price=20200, spot_diff=750)             # 直後の現物の前日比 750 → bug_comma
D.fill('2020-06-10 10:41', 29)                                       # 立会中の 29 分すべて → filled
D.legacy('2020-06-10 11:20', price=20200)
D.legacy('2020-06-10 11:41', price=20300)
D.fill('2020-06-10 11:21', 18)                                       # 20 分のうち 18 分（ちょうど 90%）→ filled
D.legacy('2020-06-10 11:51', price=20300)
D.legacy('2020-06-10 12:12', price=20400)
D.fill('2020-06-10 11:52', 17)                                       # 20 分のうち 17 分 → partial
D.legacy('2020-06-10 12:22', rows=False)                             # 先物・現物の行が無い → unknown
D.legacy('2020-06-10 12:52', rows=False)
# 作った行・新系の行の前日比は見ない（旧系の行だけ）
D.future.append({'price': 20200, 'diff': 900, 'contract_month': '2030-12-01', 'created_at': utc('2020-06-10 11:20'),
                 'ingest_run_id': run_id('legacy-fill114-', '2020-06-10 11:20')})
D.spot.append({'price': 20000, 'diff': 900, 'created_at': utc('2020-06-10 12:22'), 'ingest_run_id': 'run-20200610'})
GROUPS.append(('2020-06-10 10:00', '2020-06-10 12:52', {
    '2020-06-10 10:00': (30, 29, 0, 'bug_comma', 'none'),
    '2020-06-10 10:40': (30, 29, 29, 'bug_comma', 'filled'),
    '2020-06-10 11:20': (21, 20, 18, 'unknown', 'filled'),
    '2020-06-10 11:51': (21, 20, 17, 'unknown', 'partial'),
    '2020-06-10 12:22': (30, 29, 0, 'unknown', 'none'),
}))

# B: 2020-06-10 の夜間。前後の先物の値が同じ → no_change。前日比が先なら bug_comma
D.legacy('2020-06-10 22:00', price=20500)
D.legacy('2020-06-10 22:30', price=20500)
D.legacy('2020-06-10 22:40', price=20500, diff=800)
D.legacy('2020-06-10 23:10', price=20500)
GROUPS.append(('2020-06-10 22:00', '2020-06-10 23:10', {
    '2020-06-10 22:00': (30, 29, 0, 'no_change', 'none'),
    '2020-06-10 22:40': (30, 29, 0, 'bug_comma', 'none'),
}))

# C: 土曜の朝（金曜の夜間）と週末。05:20 → 06:00 は立会中 10 分（出ない）、06:00 → 月曜 08:46 は 0 分（出ない）
D.legacy('2020-06-13 05:00', price=20000)
D.legacy('2020-06-13 05:20', price=20050)
D.legacy('2020-06-13 06:00', price=20100)
D.legacy('2020-06-15 08:46', price=20150)
GROUPS.append(('2020-06-13 05:00', '2020-06-15 08:46', {'2020-06-13 05:00': (20, 19, 0, 'unknown', 'none')}))

# D: 立会中の分が 15 分なら出る（08:45〜08:59）、14 分なら出ない（15:01〜15:14）
D.legacy('2020-06-16 08:30', price=20000)
D.legacy('2020-06-16 09:01', price=20050)
GROUPS.append(('2020-06-16 08:30', '2020-06-16 09:01', {'2020-06-16 08:30': (31, 15, 0, 'unknown', 'none')}))
D.legacy('2020-06-16 15:01', price=20000)
D.legacy('2020-06-16 15:31', price=20050)
GROUPS.append(('2020-06-16 15:01', '2020-06-16 15:31', {}))

# E: 2021-09-16 の夜間（翌 05:30 まで）+ 09-17 の日中 08:45〜。09-17 の夜間（市場の休み）・週末・09-20（休業日）を越えて 09-21 の朝
D.legacy('2021-09-17 05:00', price=20000)
D.legacy('2021-09-17 08:50', price=20050)
D.legacy('2021-09-21 09:00', price=20100)
GROUPS.append(('2021-09-17 05:00', '2021-09-21 09:00', {
    '2021-09-17 05:00': (230, 30 + 4, 0, 'unknown', 'none'),
    # 09-17 の日中 08:50〜15:14 の 385 分 + 09-21 の日中 08:45〜08:58 の 14 分
    '2021-09-17 08:50': (4 * 24 * 60 + 10, 385 + 14, 0, 'unknown', 'none'),
}))

# F: 2021-09-21 の夜間（翌 06:00 まで）
D.legacy('2021-09-22 05:00', price=20000)
D.legacy('2021-09-22 06:30', price=20050)
GROUPS.append(('2021-09-22 05:00', '2021-09-22 06:30', {'2021-09-22 05:00': (90, 60, 0, 'unknown', 'none')}))

# G: 2024-11-01（金。日中 15:15 まで・夜間 16:30 から・翌 06:00 まで）→ 週末・11-04（休業日）→ 11-05（日中 15:45 まで・夜間 17:00 から）
D.legacy('2024-11-01 15:00', price=20000)
D.legacy('2024-11-05 08:50', price=20050)
D.legacy('2024-11-05 17:15', price=20100)
GROUPS.append(('2024-11-01 15:00', '2024-11-05 17:15', {
    # 11-01 の 15:00〜15:14 の 15 分 + 16:30〜23:59 の 450 分 + 11-02 の 00:00〜05:59 の 360 分 + 11-05 の 08:45〜08:48 の 4 分
    '2024-11-01 15:00': (4 * 24 * 60 - 370, 15 + 450 + 360 + 4, 0, 'unknown', 'none'),
    # 11-05 の 08:50〜15:44 の 415 分 + 17:00〜17:13 の 14 分
    '2024-11-05 08:50': (505, 415 + 14, 0, 'unknown', 'none'),
}))

# H: 祝日取引の日（カレンダーが取引日）は数える
D.legacy('2024-03-20 10:00', price=20000)
D.legacy('2024-03-20 10:30', price=20050)
GROUPS.append(('2024-03-20 10:00', '2024-03-20 10:30', {'2024-03-20 10:00': (30, 29, 0, 'unknown', 'none')}))

# I: 欠け 1（前日比が大きくても bug_format が先）。日中は 15:15 まで・夜間は 16:30 から。立会の外の作ったスナップショットは数えない
D.legacy('2022-09-14 15:16', price=20000, diff=1200)
D.legacy('2022-09-14 17:00', price=20050)
D.fill('2022-09-14 16:31', 10)
D.fill('2022-09-14 15:30', 1)
GROUPS.append(('2022-09-14 15:16', '2022-09-14 17:00', {'2022-09-14 15:16': (104, 29, 10, 'bug_format', 'partial')}))

# J: 欠け 3 の修正の時刻（2024-08-06 12:25 JST）の前は bug_comma、後は前日比 800 でも unknown
D.legacy('2024-08-06 12:20', price=20000, diff=800)
D.legacy('2024-08-06 12:50', price=20050)
GROUPS.append(('2024-08-06 12:20', '2024-08-06 12:50', {'2024-08-06 12:20': (30, 29, 0, 'bug_comma', 'none')}))
D.legacy('2024-08-07 10:00', price=20000, diff=800)
D.legacy('2024-08-07 10:30', price=20050)
GROUPS.append(('2024-08-07 10:00', '2024-08-07 10:30', {'2024-08-07 10:00': (30, 29, 0, 'unknown', 'none')}))

MAX_DIFF = {'2020-06-10 10:00': 750, '2020-06-10 10:40': 750, '2020-06-10 11:20': 100, '2020-06-10 12:22': None,
            '2022-09-14 15:16': 1200, '2024-08-07 10:00': 800}


def ts(text):
    return f"TIMESTAMP '{text}'"


def run_groups():
    gaps, decls = to_temp(read(GAPS))
    declares = ''.join(f'DECLARE {n} {t};\n' for n, t in decls)
    script = declares + D.tables()
    for start, end, _ in GROUPS:
        # 範囲は最初のスナップショットから最後のスナップショットの 1 分後まで（グループの外のスナップショットは入らない）
        last = (datetime.strptime(end, '%Y-%m-%d %H:%M') + timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M')
        script += (gaps.replace('@start_at', ts(utc(start))).replace('@end_at', ts(utc(last)))
                   + "\nSELECT 'group_end' AS marker;\n")
    groups, rows = [], []
    for result in run_script(script, []):
        if 'marker' in result[0]:
            groups.append(rows)
            rows = []
        else:
            rows = result
    assert len(groups) == len(GROUPS)
    return groups


@pytest.fixture(scope='module')
def listed():
    return run_groups()


def jst(value):
    return value[:16].replace('T', ' ')


@pytest.mark.parametrize('i', range(len(GROUPS)))
def test_listed(listed, i):
    start, _, expected = GROUPS[i]
    got = {jst(r['prev_jst']): (int(r['gap_minutes']), int(r['session_minutes']), int(r['built_minutes']), r['cause'], r['status'])
           for r in listed[i]}
    assert got == expected, start


def test_columns(listed):
    rows = {jst(r['prev_jst']): r for g in listed for r in g}
    for c, d in MAX_DIFF.items():
        v = rows[c]['max_abs_diff']
        assert (None if v is None else float(v)) == d, c
    r = rows['2022-09-14 15:16']
    assert jst(r['next_jst']) == '2022-09-14 17:00'
    assert r['prev_at'].startswith('2022-09-14 06:16') or r['prev_at'].startswith('2022-09-14T06:16'), r['prev_at']


def run_guard(tables, start, end):
    gaps, decls = to_temp(read(GAPS))
    declares = ''.join(f'DECLARE {n} {t};\n' for n, t in decls)
    cmd = ['bq', f'--project_id={PROJECT}', f'--location={TOKYO}', 'query', '--use_legacy_sql=false', '--format=json',
           '--nouse_cache', '--maximum_bytes_billed=1073741824',
           f'--parameter=start_at:TIMESTAMP:{start}', f'--parameter=end_at:TIMESTAMP:{end}']
    out = subprocess.run(cmd, input=declares + tables + gaps, capture_output=True, text=True, timeout=600)
    return out.returncode, ' '.join((out.stdout + out.stderr).split())


@pytest.mark.parametrize('calendar, start, end, message', [
    (CALENDAR, '2020-06-11 00:00:00+00', '2020-06-10 00:00:00+00', 'gaps: invalid range'),
    ({'2021-01-01': False}, '2020-06-10 00:00:00+00', '2020-06-11 00:00:00+00', 'gaps: trading calendar starts at'),
    ({'2020-01-02': False, '2022-01-03': False}, '2020-06-10 00:00:00+00', '2022-06-11 00:00:00+00',
     'gaps: trading calendar has no new-year rows (1/1-1/3) for years [2021]'),
])
def test_guards(calendar, start, end, message):
    code, text = run_guard(D.tables(calendar), start, end)
    assert code != 0
    assert message in text


def test_dry_run_whole_script():
    """スクリプト全体の構文（スクリプトの dry run は構文だけを見る）"""
    p = ['start_at:TIMESTAMP:2018-10-01 00:00:00+00', 'end_at:TIMESTAMP:2026-10-17 00:00:00+00']
    assert bq(render(read(GAPS), project=PROJECT, **NAMES), dry_run=True, params=p) >= 0


def test_no_datacube_values_in_fixtures():
    """固定値は架空の値: 先物の値は 50 円の刻みの区切りの値、前日比は 50 の倍数"""
    assert all(f['price'] % 50 == 0 for f in D.future)
    assert all(r['diff'] % 50 == 0 for r in D.future + D.spot)

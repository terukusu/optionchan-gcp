"""sql/gaps/gaps.sql（過去データの欠けの一覧、#110）と docs/data-gaps.md の静的検査。

- 参照する表はすべて `${project}.${dataset}.<表>`（名前は runbook の render が入れる）で、プロジェクト・データセットを直書きしない
- 読み取りだけ（INSERT・UPDATE・DELETE・MERGE・CREATE・DROP・TRUNCATE が無い）
- 表を読む箇所はパーティション列で絞る（ingest_runs は snapshot_at、spot_price・future_price は created_at）。option_price は読まない
- 区間のあき（hole_gap_minutes）と欠け 3 の修正の時刻（comma_fix_at）が sql/tick_build/build.sql（#114）と同じ
- 原因 4 種・状態 3 種が SQL・先頭のコメント・docs にあり、しきい値が先頭のコメントにある
BigQuery での実行の確認は tests/test_gaps_bq.py（OPTIONCHAN_BQ_TEST=1）。
"""
import os
import re

import pytest

ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
GAPS = os.path.join(ROOT, 'sql', 'gaps', 'gaps.sql')
BUILD = os.path.join(ROOT, 'sql', 'tick_build', 'build.sql')
DOC = os.path.join(ROOT, 'docs', 'data-gaps.md')
CAUSES = ('bug_format', 'bug_comma', 'no_change', 'unknown')
STATUSES = ('none', 'filled', 'partial')
TABLES = {'ingest_runs', 'spot_price', 'future_price', 'trading_calendar_latest'}


def read(path):
    with open(path, encoding='utf-8') as f:
        return f.read()


def body(text):
    """コメントを除いた本文"""
    return '\n'.join(line for line in text.splitlines() if not line.lstrip().startswith('--'))


SQL = read(GAPS)
HEAD = SQL[:SQL.index('DECLARE')]


def test_files():
    assert sorted(os.listdir(os.path.dirname(GAPS))) == ['gaps.sql']


def test_names_are_placeholders():
    assert set(re.findall(r'\$\{(\w+)\}', SQL)) == {'project', 'dataset'}
    assert not re.search(r'optionchan_\w+', body(SQL))
    assert not re.search(r'optionchan-\d', SQL)
    refs = re.findall(r'`([^`]+)`', body(SQL))
    assert refs
    for ref in refs:
        assert re.match(r'\$\{project\}\.\$\{dataset\}\.\w+$', ref), ref
    assert {ref.split('.')[-1] for ref in refs} == TABLES


def test_read_only():
    assert not re.search(r'\b(INSERT|UPDATE|DELETE|MERGE|CREATE|DROP|TRUNCATE|ALTER)\b', body(SQL), re.I)


def test_tables_filtered_by_partition():
    """表を読む箇所はすべてパーティション列で絞る（全件スキャンの防止）"""
    text = body(SQL)
    refs = list(re.finditer(r'`\$\{project\}\.\$\{dataset\}\.(\w+)`', text))
    for m in refs:
        table = m.group(1)
        if table == 'trading_calendar_latest':
            continue  # 休業日の一覧（数百行のビュー）
        following = text[m.end():m.end() + 120]
        if table == 'ingest_runs':
            pattern = r'^\s+WHERE snapshot_at >= start_at AND snapshot_at < end_at'
        else:
            assert table in ('spot_price', 'future_price'), table
            pattern = r'^\s+WHERE created_at >= start_at AND created_at < end_at'
        assert re.match(pattern, following), (table, following)


def declared(text, name):
    m = re.search(rf'^DECLARE {name} (\w+) DEFAULT (.*?);$', text, re.M)
    assert m, name
    return m.groups()


def test_constants_match_build():
    build = read(BUILD)
    assert declared(SQL, 'hole_gap_minutes') == declared(build, 'hole_gap_minutes') == ('INT64', '15')
    # 欠け 3 の修正の時刻 = #114 の範囲の終わり
    assert declared(SQL, 'comma_fix_at')[1] == declared(build, 'range_end')[1]
    # 欠け 1 の期間（build.sql の先頭のコメントの JST の時刻）
    assert declared(SQL, 'format_from')[1] == "TIMESTAMP '2022-09-14 06:16:00+00'"
    assert declared(SQL, 'format_to')[1] == "TIMESTAMP '2023-04-25 04:35:00+00'"
    assert '2022-09-14 15:16 〜 2023-04-25 13:35 JST' in build[:build.index('DECLARE')]
    assert declared(SQL, 'comma_diff') == ('INT64', '700')
    assert declared(SQL, 'filled_ratio') == ('FLOAT64', '0.9')
    # 立会の時間の変更日と 2021-09-17 の夜間（市場の休み）
    assert declared(SQL, 'day_close_change')[1] == "DATE '2024-11-05'"
    assert declared(SQL, 'night_close_change')[1] == "DATE '2021-09-21'"
    assert "DECLARE suspended_nights ARRAY<DATE> DEFAULT [DATE '2021-09-17'];" in SQL


def test_calendar_same_as_build():
    build = read(BUILD)
    for name in ('calendar_from', 'closed_trading'):
        pattern = rf'^DECLARE {name} \w+(<\w+>)? DEFAULT \(\n.*?\n\);$'
        assert re.search(pattern, SQL, re.S | re.M).group(0) == re.search(pattern, build, re.S | re.M).group(0), name


def test_guards_before_select():
    text = body(SQL)
    pos = [text.index(s) for s in (
        'IF start_at IS NULL OR end_at IS NULL OR start_at >= end_at THEN',
        "IF calendar_from IS NULL OR calendar_from > DATE(start_at, 'Asia/Tokyo') THEN",
        'IF ARRAY_LENGTH(IFNULL(missing_new_year, [])) > 0 THEN',
        'WITH\nruns AS (',
    )]
    assert pos == sorted(pos)
    assert len(re.findall(r"SELECT ERROR\(FORMAT\('gaps: ", text)) == 3


def test_rules():
    text = body(SQL)
    # 区間: 旧系のスナップショットだけで決め、あきが hole_gap_minutes 以上、立会中の分も hole_gap_minutes 以上
    assert "FROM (SELECT DISTINCT snapshot_at FROM runs WHERE trigger = 'legacy_import')" in text
    assert 'WHERE TIMESTAMP_DIFF(next_at, prev_at, MINUTE) >= hole_gap_minutes' in text
    assert 'WHERE n.session_minutes >= hole_gap_minutes' in text
    # 分は分の終わり c（prev_at < c < next_at）で、分 [c − 1 分, c) の始まりの JST で数える
    assert ('GENERATE_TIMESTAMP_ARRAY(TIMESTAMP_ADD(h.prev_at, INTERVAL 1 MINUTE), TIMESTAMP_SUB(h.next_at, INTERVAL 1 MINUTE),'
            in text)
    assert "DATETIME(TIMESTAMP_SUB(c, INTERVAL 1 MINUTE), 'Asia/Tokyo') AS t" in text
    # 作った行は ingest_runs の trigger で数える（DataCube 由来の値は読まない）
    assert "trigger IN ('legacy_import', 'legacy_fill114')" in text
    assert "WHERE trigger = 'legacy_fill114') AS b ON b.snapshot_at = s.c" in text
    # 前後のスナップショットの前日比は旧系の行だけ
    assert text.count("AND STARTS_WITH(ingest_run_id, 'legacy-') AND NOT STARTS_WITH(ingest_run_id, 'legacy-fill114-')") == 2
    # 原因の順
    pos = [text.index(f"THEN '{c}'") for c in CAUSES[:3]] + [text.index("ELSE 'unknown'")]
    assert pos == sorted(pos)
    assert "WHEN prev_at < comma_fix_at AND IFNULL(max_abs_diff, 0) >= comma_diff THEN 'bug_comma'" in text
    assert "WHEN built_minutes >= filled_ratio * session_minutes THEN 'filled'" in text


@pytest.mark.parametrize('word', CAUSES + STATUSES)
def test_causes_and_statuses_documented(word):
    assert f"'{word}'" in body(SQL)
    assert re.search(rf'\b{word}\b', HEAD), word
    assert f'`{word}`' in read(DOC), word


def test_thresholds_in_head():
    for s in ('comma_diff（700）', 'filled_ratio（90%）', 'hole_gap_minutes（15 分', '2021-09-17', '2024-11-05', '2021-09-21'):
        assert s in HEAD, s


def test_doc_links():
    doc = read(DOC)
    assert 'sql/gaps/gaps.sql' in doc
    assert 'docs/data-gaps.md' in read(os.path.join(ROOT, 'README.md'))
    assert read(os.path.join(ROOT, 'docs', 'runbook.md')).count('docs/data-gaps.md') >= 2


def test_no_datacube_values_in_repo_files():
    """SQL に値を書かない: 4 桁以上の数字は日付・時刻だけ（日付・時刻の中の数字は前後が - か : なので当たらない）"""
    assert not re.findall(r'(?<![\w.:-])\d{4,}(?![\w:-])', body(SQL))

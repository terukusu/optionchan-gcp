"""SQL の静的検査（#52 AC 4）。

- SQL は sql/*.sql にだけ置く（Python で組み立てない）。名前は ${project}・${dataset}、値は @パラメータだけ
- 読むのは最新版のテーブル関数（*_latest・latest_runs）と 1 時間足 atm_iv だけ（生テーブルを直接読まない）。期間で絞る
- 書き込み・DDL をしない
"""

import re

import pytest
from conftest import PKG_DIR

from optionchan_viewer import bq

SQL_DIR = PKG_DIR / "sql"
SQL = {p.stem: p.read_text(encoding="utf-8") for p in SQL_DIR.glob("*.sql")}
ALLOWED_FUNCS = {"latest_runs", "option_price_latest"}
ALLOWED_TABLES = {"atm_iv"}


def body(text):
    return "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("--"))


def test_files_match_query_names():
    assert sorted(SQL) == sorted(bq.QUERY_NAMES)


def test_no_sql_in_python():
    # AC 4: grep -rnwE 'SELECT|FROM' apps/viewer/src --include=*.py が 0 件
    for path in PKG_DIR.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        assert not re.search(r"\b(SELECT|FROM)\b", text), path


@pytest.mark.parametrize("name", sorted(SQL))
def test_names_are_placeholders(name):
    text = body(SQL[name])
    assert set(re.findall(r"\$\{(\w+)\}", text)) == {"project", "dataset"}
    assert not re.search(r"optionchan-\d+", SQL[name])
    for ref in re.findall(r"`([^`]+)`", text):
        assert re.match(r"\$\{project\}\.\$\{dataset\}\.\w+$", ref), (name, ref)
    # 文字列リテラルに値を埋め込まない（タイムゾーン名だけ）
    assert set(re.findall(r"'([^']*)'", text)) <= {"Asia/Tokyo"}


@pytest.mark.parametrize("name", sorted(SQL))
def test_reads_only_latest_and_hourly(name):
    text = body(SQL[name])
    for m in re.finditer(r"`\$\{project\}\.\$\{dataset\}\.(\w+)`(\([^()]*\))?", text):
        table, args = m.groups()
        following = text[m.end():m.end() + 200]
        if table in ALLOWED_FUNCS:
            # テーブル関数の引数はパラメータ（定数。option_price のパーティションの除去が効く）
            assert re.fullmatch(r"\(@\w+, @\w+\)", args or ""), (name, table, args)
        else:
            assert table in ALLOWED_TABLES, (name, table)
            assert args is None
            assert re.match(r"\s*WHERE started_at >= @\w+ AND started_at < @\w+", following), (name, following)


@pytest.mark.parametrize("name", sorted(SQL))
def test_read_only(name):
    text = body(SQL[name]).upper()
    assert not re.search(r"\b(INSERT|UPDATE|DELETE|MERGE|CREATE|DROP|TRUNCATE|ALTER|DECLARE|EXECUTE)\b", text), name
    assert ";" not in body(SQL[name])


def test_atm_iv_uses_hourly_bars_and_put():
    text = SQL["atm_iv"]
    assert re.search(r"\n\s*WHERE is_atm AND type = 2\n", text)
    assert "SAFE_OFFSET(@n)" in text
    # 限月の候補は直近 10 日（現行の t1 と同じ開区間）
    assert "WHERE started_at > @months_since" in text
    # N3: 1 時間足・補った足のどちらも d 日の下端で絞る
    cond = r"WHERE last_trading_day = \(SELECT last_trading_day FROM month\) AND started_at >= @days_start\n"
    assert len(re.findall(cond, text)) == 2

"""SQL の静的検査（#52 AC 4）。

- SQL は sql/*.sql にだけ置く（Python で組み立てない）。名前は ${project}・${dataset}、値は @パラメータだけ
- 読むのは最新版のテーブル関数（*_latest・latest_runs）と 1 時間足 atm_iv・atm_mid_iv（#98）・取引カレンダーの最新版
  trading_calendar_latest（#68）だけ（生テーブルを直接読まない）。期間で絞る。欠けの一覧の表 data_gaps（#123。数百行）は
  data_gaps.sql からだけ全体を読んでよい
- 書き込み・DDL をしない
"""

import re

import pytest
from conftest import PKG_DIR, REPO_ROOT

from optionchan_viewer import bq

SQL_DIR = PKG_DIR / "sql"
SQL = {p.stem: p.read_text(encoding="utf-8") for p in SQL_DIR.glob("*.sql")}
ALLOWED_FUNCS = {"latest_runs", "option_price_latest"}
# 表 → 期間で絞る列
ALLOWED_TABLES = {"atm_iv": "started_at", "atm_mid_iv": "started_at", "trading_calendar_latest": "calendar_date",
                  "atm_cm_iv": "created_at"}
# 全体を読んでよい小さい表 → 読んでよい SQL（#123。表は runbook の手順で入れる数百行）
FULL_TABLES = {"data_gaps": "data_gaps"}
# 文字列リテラル: タイムゾーン名と、30 日固定（#100）の計算の出し方の名前（sql/cm/atm_cm.sql と同じ本文）
LITERALS = {"Asia/Tokyo"}
CM_METHODS = {"m1", "interp", "m2"}


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
    assert set(re.findall(r"'([^']*)'", text)) <= (LITERALS | CM_METHODS if name == "atm_cm" else LITERALS)


@pytest.mark.parametrize("name", sorted(SQL))
def test_reads_only_latest_and_hourly(name):
    text = body(SQL[name])
    for m in re.finditer(r"`\$\{project\}\.\$\{dataset\}\.(\w+)`(\([^()]*\))?", text):
        table, args = m.groups()
        following = text[m.end():m.end() + 200]
        if table in ALLOWED_FUNCS:
            # テーブル関数の引数はパラメータ（定数。option_price のパーティションの除去が効く）
            assert re.fullmatch(r"\(@\w+, @\w+\)", args or ""), (name, table, args)
        elif table in FULL_TABLES:
            assert FULL_TABLES[table] == name, (name, table)
            assert args is None
        else:
            assert table in ALLOWED_TABLES, (name, table)
            assert args is None
            col = ALLOWED_TABLES[table]
            assert re.match(rf"\s*WHERE {col} >= @\w+ AND {col} < @\w+", following), (name, following)


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
    # N3: 1 時間足・補った足のどちらも d 日の下端で絞る。#77: 上端（@range_end）でも
    cond = (r"WHERE last_trading_day = \(SELECT last_trading_day FROM month\) AND started_at >= @days_start "
            r"AND started_at < @range_end\n")
    assert len(re.findall(cond, text)) == 2


def test_atm_iv_minute_same_rules_as_hourly():
    # #68: 1 分足は ATM の put、限月は atm_iv.sql と同じ規則、OHLC は atm_hourly.sql の bars の iv の 4 式と同じ
    text = SQL["atm_iv_minute"]
    assert re.search(r"\n\s*WHERE is_atm AND type = 2\n", text)
    assert "SAFE_OFFSET(@n)" in text and "WHERE last_trading_day >= @today" in text
    assert "TIMESTAMP_TRUNC(created_at, MINUTE) AS started_at" in text
    hourly = (REPO_ROOT / "sql" / "ohlc" / "atm_hourly.sql").read_text(encoding="utf-8")
    for expr in ("ARRAY_AGG(iv IGNORE NULLS ORDER BY created_at, target_price, iv)[SAFE_OFFSET(0)] AS ",
                 "MAX(iv) AS ", "MIN(iv) AS ",
                 "ARRAY_AGG(iv IGNORE NULLS ORDER BY created_at DESC, target_price DESC, iv DESC)[SAFE_OFFSET(0)] AS "):
        assert expr in hourly and expr in text, expr
    # 値段・行使価格の列は読まない（スキャン量）
    assert not re.search(r"\bprice\b", body(text))
    assert "ORDER BY started_at" in text


def test_months_sql_same_rules_as_one_month():
    # #101: 期近つなぎの全限月のクエリは、限月で絞らない（限月の列を足して限月ごとに束ねる）だけで、
    # 読む表・列・OHLC の式は n=0/1 のクエリと同じ
    hourly = (REPO_ROOT / "sql" / "ohlc" / "atm_hourly.sql").read_text(encoding="utf-8")
    exprs = ("ARRAY_AGG(iv IGNORE NULLS ORDER BY created_at, target_price, iv)[SAFE_OFFSET(0)] AS open",
             "MAX(iv) AS high", "MIN(iv) AS low",
             "ARRAY_AGG(iv IGNORE NULLS ORDER BY created_at DESC, target_price DESC, iv DESC)[SAFE_OFFSET(0)] AS close")
    for name, base in (("atm_iv_minute_months", "atm_iv_minute"), ("atm_mid_minute_months", "atm_mid_minute")):
        text = SQL[name]
        assert re.search(r"\n\s*WHERE is_atm AND type = 2\n", text)
        for expr in exprs:
            assert expr in text and expr in SQL[base], (name, expr)
        assert "GROUP BY last_trading_day, started_at" in text
        assert "ORDER BY started_at, last_trading_day" in text
        assert "SAFE_OFFSET(@n)" not in text and "@today" not in text
        assert not re.search(r"\bprice\b", body(text))
        # 読む列（option_price_latest の SELECT）は n=0/1 のクエリと同じ
        cols = re.compile(r"SELECT ([^\n]+)\n\s*FROM `\$\{project\}\.\$\{dataset\}\.option_price_latest`")
        assert cols.search(text).group(1) == cols.search(SQL[base]).group(1), name
    assert "IF(bid_iv > 0 AND ask_iv > 0, (bid_iv + ask_iv) / 2, NULL) AS iv" in SQL["atm_mid_minute_months"]
    for expr in exprs:
        assert expr.rsplit(" AS ", 1)[0] in hourly
    # 1 時間足: bars の区間は sql/ohlc/atm_hourly.sql と一字一句同じ（tests/test_ohlc.py も検査）
    text = SQL["atm_iv_months"]
    block = text[text.index("\n-- bars:begin\n") + 1:text.index("\n-- bars:end\n") + 1]
    assert block == hourly[hourly.index("\n-- bars:begin\n") + 1:hourly.index("\n-- bars:end\n") + 1]
    assert "month AS" not in text and "@today" not in text and "@n" not in text
    assert "ORDER BY started_at, last_trading_day" in text
    assert "WHERE started_at >= @days_start AND started_at < @range_end\n" in text


def test_mid_hourly_same_as_trade_hourly():
    # #98: 1 時間足・日足の mid のクエリは、約定のクエリ（atm_iv.sql・atm_iv_months.sql）の
    # 1 時間足の表を atm_mid_iv に、補う分の計算（bars）を mid の計算（mid_bars。sql/ohlc/atm_hourly.sql と
    # 一字一句同じ）に、読む列を bid_iv・ask_iv に替えただけ
    hourly = (REPO_ROOT / "sql" / "ohlc" / "atm_hourly.sql").read_text(encoding="utf-8")

    def block(text, name):
        return text[text.index(f"\n-- {name}:begin\n") + 1:text.index(f"\n-- {name}:end\n") + 1]

    def after_bars(text, name):
        # 計算の区間の後（限月の選び方・期間・並べ方）
        return text[text.index(f"\n-- {name}:end\n"):]

    for mid, trade in (("atm_mid", "atm_iv"), ("atm_mid_months", "atm_iv_months")):
        text = SQL[mid]
        assert block(text, "mid_bars") == block(hourly, "mid_bars"), mid
        assert "`${project}.${dataset}.atm_mid_iv`" in text and "`${project}.${dataset}.atm_iv`" not in text
        assert "SELECT created_at, last_trading_day, target_price, bid_iv, ask_iv\n" in text
        assert not re.search(r"\b(iv|price)\b", body(text).replace("bid_iv", "").replace("ask_iv", "")), mid
        want = (after_bars(SQL[trade], "bars").replace("-- bars:end", "-- mid_bars:end")
                .replace("FROM bars\n", "FROM mid_bars\n")
                .replace("iv_open AS open, iv_high AS high, iv_low AS low, iv_close AS close",
                         "mid_open AS open, mid_high AS high, mid_low AS low, mid_close AS close"))
        assert after_bars(text, "mid_bars") == want, mid


def test_full_tables_only_from_own_sql():
    # #123: data_gaps を読むのは data_gaps.sql だけ（他の SQL が読めば test_reads_only_latest_and_hourly で落ちる）
    readers = {name for name, text in SQL.items() if "`${project}.${dataset}.data_gaps`" in body(text)}
    assert readers == {"data_gaps"}
    text = body(SQL["data_gaps"])
    # 表全体（パラメータなし）を UNIX 秒で prev_at 昇順に
    assert "@" not in text
    assert "UNIX_SECONDS(prev_at) AS prev_at" in text and "UNIX_SECONDS(next_at) AS next_at" in text
    assert "ORDER BY prev_at" in text


def test_smile_snapshot_returns_ingest_run_id():
    # #123: 実行 ID をそのまま返す（判定は smile.snapshot_source の 1 か所。SQL に接頭辞を書かない）
    text = body(SQL["smile_snapshot"])
    snap = r"SELECT [^\n]*\bingest_run_id\b[^\n]*\n\s*FROM `\$\{project\}\.\$\{dataset\}\.option_price_latest`"
    assert re.search(snap, text)
    assert "ingest_run_id\nFROM m CROSS JOIN" in text
    assert "fill114" not in SQL["smile_snapshot"]


def test_trading_holidays_reads_latest_calendar():
    text = body(SQL["trading_holidays"])
    assert "`${project}.${dataset}.trading_calendar_latest`" in text
    assert "AND NOT is_trading_day" in text

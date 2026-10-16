"""ビューアーの SQL を BigQuery で実際に動かす検査（#52 AC 5・6）。

ルートの tests/test_sql_bq.py と同じ流儀（bq CLI）。CI では動かさない（BigQuery の認証が要る）。手元で:
    OPTIONCHAN_BQ_TEST=1 OPTIONCHAN_BQ_PROJECT=<project-id> uv run --locked pytest -q tests/test_sql_bq.py
データセットは OPTIONCHAN_BQ_DATASET（既定 optionchan_tokyo）。gcloud / bq の構成は
CLOUDSDK_ACTIVE_CONFIG_NAME 等で渡す。

- ロジックの同値（AC 5）: 固定値の CTE（option_price・atm_iv・ingest_runs の代わり）に、現行の 2 関数の SQL
  （legacy_sql.py）と新しい SQL（テーブル関数の参照を、固定値の CTE を期間で絞ったものに置き換える）を当て、
  CSV（csvfmt.py）を比べる。新しい SQL は api.py の手順（2 段のクエリ・パラメータ）どおりに流す
  （runner を bq CLI にしたもの）。テーブルは読まない（課金 0）
- 違いが出る点は test_diff_* に、違いを示す入力で明記する
- 実テーブルの dry run（AC 6）: 見積もりバイト数が 1 GiB 未満（値は PR に記録する）
- /atm_iv_bars（#68 AC 5・6）: 1 分足 SQL を束ねた 5 分・15 分・1 時間足が、生の行から sql/ohlc/atm_hourly.sql の
  bars で作った足と一致する。限月が混ざらない。日足で金曜・休業日の前日の夜間が休み明けの日に入る。
  新しいクエリの dry run が 512 MiB 以下
- /smile_snapshot（#79 AC 6・7）: 再取り込み（同じスナップショットに 2 回の実行）で新しい実行の値だけ、週末の at で
  金曜の最後のスナップショット、限月の割り当てが smile.sql と同じ。dry run が 512 MiB 以下
- /smile_snapshot?dir=next（#80）: 週末・取引の切れ目の at で次の取引の最初のスナップショット、最新より後なら最新
- /atm_iv_bars?iv=mid（#93）: mid の 1 分足が、iv を mid に替えた atm_iv_minute と同じ（限月・空の分・OHLC の式）。
  bid・ask の片方・0 以下は使わない。3 日（4 パーティション）の dry run が 512 MiB 以下
- /atm_iv_bars?n=roll（#101）: 全限月のクエリ（1 時間足 + 補完、分足 trade・mid）の各限月の足が、同じ入力の n=0・n=1 の
  クエリの足と同じ（限月の列が足されただけ）。期近つなぎの API が乗り換えの無い期間で n=0 と同じ足。
  dry run が 512 MiB 以下
"""
import json
import os
import re
import subprocess
from datetime import UTC, date, datetime, timedelta

import legacy_sql
import pytest
from conftest import PKG_DIR, REPO_ROOT

from optionchan_viewer import api, bars, csvfmt

PROJECT = os.environ.get("OPTIONCHAN_BQ_PROJECT", "")
DATASET = os.environ.get("OPTIONCHAN_BQ_DATASET", "optionchan_tokyo")
LOCATION = os.environ.get("OPTIONCHAN_BQ_LOCATION", "asia-northeast1")

pytestmark = pytest.mark.skipif(
    os.environ.get("OPTIONCHAN_BQ_TEST") != "1" or not PROJECT,
    reason="OPTIONCHAN_BQ_TEST=1 と OPTIONCHAN_BQ_PROJECT のときだけ",
)

GiB = 1024**3
MiB = 1024**2
SQL_DIR = PKG_DIR / "sql"
# 2026-10-13 06:00 UTC（JST 15:00）
NOW = datetime(2026, 10, 13, 6, 0, tzinfo=UTC)
M0, M1, M2 = date(2026, 10, 9), date(2026, 11, 12), date(2026, 12, 10)  # M0 は今日（JST）より前に満了


# ---- bq CLI ----

def param(name, value):
    if isinstance(value, datetime):
        return f"{name}:TIMESTAMP:{value.astimezone(UTC).strftime('%Y-%m-%d %H:%M:%S.%f')}+00:00"
    if isinstance(value, date):
        return f"{name}:DATE:{value.isoformat()}"
    assert isinstance(value, int) and not isinstance(value, bool)
    return f"{name}:INT64:{value}"


def bq(sql, params=None, dry_run=False, maximum_bytes=10 * 1024**2):
    cmd = ["bq", f"--project_id={PROJECT}", f"--location={LOCATION}", "query", "--use_legacy_sql=false",
           "--format=json", "--nouse_cache", f"--maximum_bytes_billed={maximum_bytes}",
           *(f"--parameter={param(k, v)}" for k, v in (params or {}).items())]
    if dry_run:
        cmd.append("--dry_run")
    out = subprocess.run(cmd, input=sql, capture_output=True, text=True, timeout=300)
    if out.returncode != 0:
        # BigQuery のエラーの行だけを出す（ジョブの JSON にはアカウントのメールアドレスが入るので出さない）
        raise BqError("\n".join(line for line in (out.stdout + out.stderr).splitlines()
                                if "error" in line.lower() or "at [" in line))
    if dry_run:
        return int(json.loads(out.stdout)["statistics"]["totalBytesProcessed"])
    text = out.stdout.strip()
    return json.loads(text[text.index("["):]) if "[" in text else []


class BqError(AssertionError):
    pass


# bq の JSON は値がすべて文字列なので、列ごとに型を戻す（BigQuery クライアントが返す Python の型と同じにする）
def ts(v):
    return datetime.fromisoformat(v).replace(tzinfo=UTC)


TYPES = {
    "snapshot_at": ts, "last_started_at": ts, "started_at": ts,
    "o1_call_price_time": int, "o1_put_price_time": int, "o2_call_price_time": int,
    "o2_put_price_time": int,
    "o1_put_is_atm": lambda v: v == "true", "o2_put_is_atm": lambda v: v == "true",
    "open": float, "high": float, "low": float, "close": float,
    "o1_call_iv": float, "o1_put_iv": float, "o2_call_iv": float, "o2_put_iv": float,
    "calendar_date": date.fromisoformat,
}


def typed(rows, overrides=None):
    types = {**TYPES, **(overrides or {})}
    return [{k: (None if v is None else types[k](v)) for k, v in r.items()} for r in rows]


# ---- 固定値の CTE ----

def literal(value, typ):
    if value is None:
        return f"CAST(NULL AS {typ})"
    if typ in ("TIMESTAMP", "DATETIME", "DATE"):
        return f"{typ} '{value}'"
    if typ == "STRING":
        return f"'{value}'"
    if typ == "BOOL":
        return "TRUE" if value else "FALSE"
    return repr(value)


def cte(name, columns, rows):
    struct = ", ".join(f"{c} {t}" for c, t in columns)
    if not rows:
        return f"{name} AS (SELECT * FROM UNNEST(ARRAY<STRUCT<{struct}>>[]))"
    values = ", ".join("STRUCT(" + ", ".join(literal(r.get(c), t) for c, t in columns) + ")" for r in rows)
    return f"{name} AS (SELECT * FROM UNNEST(ARRAY<STRUCT<{struct}>>[{values}]))"


OPTION = [("created_at", "TIMESTAMP"), ("type", "INT64"), ("target_price", "INT64"), ("last_trading_day", "DATE"),
          ("iv", "FLOAT64"), ("price", "INT64"), ("price_time", "DATETIME"), ("is_atm", "BOOL"),
          ("bid_iv", "FLOAT64"), ("ask_iv", "FLOAT64"), ("bid", "INT64"), ("ask", "INT64"), ("ingest_run_id", "STRING")]
HOURLY = [("open", "FLOAT64"), ("high", "FLOAT64"), ("low", "FLOAT64"), ("close", "FLOAT64"),
          ("last_trading_day", "DATE"), ("time_frame", "INT64"), ("started_at", "TIMESTAMP")]
RUNS = [("snapshot_at", "TIMESTAMP"), ("ingest_run_id", "STRING"), ("finished_at", "TIMESTAMP")]
CALENDAR = [("calendar_date", "DATE"), ("is_trading_day", "BOOL")]


def opt(at, typ, strike, month, iv=None, price=None, price_time=None, is_atm=False, **quote):
    """quote: bid_iv・ask_iv・bid・ask・ingest_run_id（#79。省略は NULL）"""
    return {"created_at": at, "type": typ, "target_price": strike, "last_trading_day": month, "iv": iv,
            "price": price, "price_time": price_time, "is_atm": is_atm, **quote}


def bar(at, month, o, h, low, c):
    return {"open": o, "high": h, "low": low, "close": c, "last_trading_day": month, "time_frame": 3600,
            "started_at": at}


def fixtures(options=(), hourly=(), runs=(), calendar=(), mid_hourly=()):
    return ",\n".join([cte("option_price", OPTION, list(options)), cte("atm_iv", HOURLY, list(hourly)),
                       cte("ingest_runs", RUNS, list(runs)), cte("trading_calendar_latest", CALENDAR, list(calendar)),
                       cte("atm_mid_iv", HOURLY, list(mid_hourly))])


def with_fixtures(sql, fx):
    """WITH の先頭に固定値の CTE を足す（WITH が無ければ作る）"""
    m = re.match(r"(\s*(?:--[^\n]*\n\s*)*)WITH\b", sql)
    if m:
        return sql[:m.end()] + "\n" + fx + ",\n" + sql[m.end():]
    head = re.match(r"(\s*(?:--[^\n]*\n\s*)*)", sql).end()
    return sql[:head] + "WITH\n" + fx + "\n" + sql[head:]


def new_sql(name, fx):
    """新しい SQL の表の参照を固定値の CTE に置き換える（テーブル関数は CTE を期間で絞ったものにする）"""
    text = (SQL_DIR / f"{name}.sql").read_text(encoding="utf-8")
    text = re.sub(r"`\$\{project\}\.\$\{dataset\}\.option_price_latest`\((@\w+), (@\w+)\)",
                  r"(SELECT * FROM option_price WHERE created_at >= \1 AND created_at < \2)", text)
    text = re.sub(r"`\$\{project\}\.\$\{dataset\}\.latest_runs`\((@\w+), (@\w+)\)",
                  r"(SELECT * FROM ingest_runs WHERE snapshot_at >= \1 AND snapshot_at < \2)", text)
    text = text.replace("`${project}.${dataset}.atm_iv`", "atm_iv")
    text = text.replace("`${project}.${dataset}.atm_mid_iv`", "atm_mid_iv")
    text = text.replace("`${project}.${dataset}.trading_calendar_latest`", "trading_calendar_latest")
    assert "${" not in text, name
    return with_fixtures(text, fx)


def fixture_runner(fx, overrides=None):
    """api.py の runner と同じ形（名前とパラメータ → 型付きの行）で、固定値に新しい SQL を当てる"""
    def run(name, params):
        return typed(bq(new_sql(name, fx), params), (overrides or {}).get(name))

    return run


def jst(day, hour, minute=0):
    return f"2026-10-{day:02d} {hour:02d}:{minute:02d}:00"


def utc(day, hour, minute=0):
    return f"2026-10-{day:02d} {hour:02d}:{minute:02d}:00+00"


# ---- /smile_data ----

SNAP = utc(13, 5, 59)
SNAP_DT = datetime(2026, 10, 13, 5, 59, tzinfo=UTC)
SMILE_OPTIONS = [
    # 1 限月（M1）: 24750〜25250 の call / put。iv・price_time の NULL、ATM は put の 25000
    opt(SNAP, 1, 24750, M1, 21.5, 400, jst(13, 14, 58)),
    opt(SNAP, 2, 24750, M1, 22.0, 150, None),
    opt(SNAP, 1, 25000, M1, None, 300, jst(13, 14, 50)),
    opt(SNAP, 2, 25000, M1, 19.75, 260, jst(13, 14, 59), is_atm=True),
    opt(SNAP, 1, 25250, M1, 18.0, 180, jst(12, 23, 59)),  # 取引時刻が前日（夜間）
    opt(SNAP, 2, 25250, M1, 18.5, 420, jst(13, 9, 0)),
    # 2 限月（M2）: 行使価格が 1 限月より広い（25500 は M2 だけ）。put の ATM も立つ（メタ行には使わない）
    opt(SNAP, 1, 25000, M2, 17.0, 600, jst(13, 14, 30)),
    opt(SNAP, 2, 25000, M2, 18.25, 550, jst(13, 14, 40), is_atm=True),
    opt(SNAP, 2, 25500, M2, 16.5, 700, None),
    # 別のスナップショット（1 分前）は読まない
    opt(utc(13, 5, 58), 2, 25000, M1, 99.0, 1, jst(13, 14, 57), is_atm=True),
    opt(utc(13, 5, 58), 2, 24500, M1, 99.0, 1, jst(13, 14, 57)),
]


def legacy_smile_csv(options, snapshot):
    rows = typed(bq(with_fixtures(legacy_sql.smile_query(snapshot), fixtures(options))),
                 {"target_price": int, "o1_call_price_time": datetime.fromisoformat,
                  "o1_put_price_time": datetime.fromisoformat, "o2_call_price_time": datetime.fromisoformat,
                  "o2_put_price_time": datetime.fromisoformat})
    atm, rows = legacy_sql.legacy_smile_rows(rows)
    # 現行のメタ行は Datastore の最新先物の created_at（= このスナップショットの時刻）
    return csvfmt.smile_csv(snapshot, atm, rows)


SMILE_TYPES = {"smile": {"target_price": int}}


def test_smile_same_as_legacy():
    fx = fixtures(SMILE_OPTIONS, runs=[{"snapshot_at": utc(13, 5, 58)}, {"snapshot_at": SNAP}])
    new = api.smile_data(fixture_runner(fx, SMILE_TYPES), NOW)
    assert new == legacy_smile_csv(SMILE_OPTIONS, SNAP_DT)
    lines = new.split("\n")
    assert lines[0] == f"{int(SNAP_DT.timestamp())},25000"
    assert len(lines) == 1 + 4 + 1  # メタ行 + 行使価格 4 つ + 末尾の改行
    # 前日（夜間）の取引時刻: 2026-10-12 23:59 JST
    assert lines[3].split(",")[2] == str(int(datetime(2026, 10, 12, 14, 59, tzinfo=UTC).timestamp()))


def test_diff_smile_no_snapshot_is_503():
    # 違い: 現行は Datastore の時刻で引くので、書き込み前は 0 件 → IndexError（500、#5）。
    # 新は完了したスナップショットだけを読み、無ければ 503
    fx = fixtures(SMILE_OPTIONS, runs=[])
    with pytest.raises(api.NoData):
        api.smile_data(fixture_runner(fx, SMILE_TYPES), NOW)
    with pytest.raises(IndexError):
        legacy_sql.legacy_smile_rows([])


# ---- /atm_iv_data ----

def h(hour, day=13, minute=0):
    return utc(day, hour, minute)


# 1 時間足: 2026-10-12 23:00 UTC までがある（このとき新の補う分は 10-13 00:00 UTC から = 現行の「UTC の当日分」と同じ）
ATM_IV_HOURLY = [
    *(bar(h(hr, 12), M1, 18.0 + hr / 10, 19.0 + hr / 10, 17.5 + hr / 10, 18.5 + hr / 10) for hr in (0, 1, 2, 22, 23)),
    *(bar(h(hr, 8), M1, 20.0, 21.0, 19.0, 20.5) for hr in (6, 7)),
    # 10 日前の UTC の 0 時ちょうど（d=10 以上で表示。限月の候補には入らない）
    bar(h(0, 3), M1, 25.0, 25.0, 25.0, 25.0),
    bar(h(1, 12), M2, 16.0, 16.5, 15.5, 16.25),
    bar(h(2, 12), M2, None, None, None, None),  # 値が NULL の足（現行の insert_ohlc も作る）
    bar(h(3, 12), M0, 30.0, 30.0, 30.0, 30.0),  # 満了した限月
]
ATM_IV_OPTIONS = [
    # 10-13 00 時台: ATM が 25000 → 25125 に変わる（分けない）。iv の NULL は open・close で飛ばす
    opt(h(0, 13, 5), 2, 25000, M1, 18.0, 300, is_atm=True),
    opt(h(0, 13, 30), 2, 25000, M1, None, 290, is_atm=True),
    opt(h(0, 13, 55), 2, 25125, M1, 19.5, 250, is_atm=True),
    # 01 時台は行が無い。02 時台
    opt(h(2, 13, 0), 2, 25125, M1, 19.0, 255, is_atm=True),
    opt(h(2, 13, 59), 2, 25125, M1, 18.5, 260, is_atm=True),
    # call・ATM でない行は使わない
    opt(h(0, 13, 10), 1, 25000, M1, 99.0, 1, is_atm=True),
    opt(h(0, 13, 10), 2, 25500, M1, 99.0, 1, is_atm=False),
    # 2 限月
    opt(h(1, 13, 20), 2, 25000, M2, 17.0, 600, is_atm=True),
]


def legacy_atm_iv_csv(hourly, options, d, n, now):
    rows = typed(bq(with_fixtures(legacy_sql.atm_iv_query(d, n, now), fixtures(options, hourly))))
    return csvfmt.atm_iv_csv(rows)


@pytest.mark.parametrize("d,n", [(7, 0), (1, 0), (10, 0), (20, 0), (7, 1)])
def test_atm_iv_same_as_legacy(d, n):
    new = api.atm_iv_data(fixture_runner(fixtures(ATM_IV_OPTIONS, ATM_IV_HOURLY)), NOW, d, n)
    assert new == legacy_atm_iv_csv(ATM_IV_HOURLY, ATM_IV_OPTIONS, d, n, NOW)
    assert new  # 空でない


def test_atm_iv_values():
    lines = api.atm_iv_data(fixture_runner(fixtures(ATM_IV_OPTIONS, ATM_IV_HOURLY)), NOW, 1, 0).split("\n")
    # d=1: 10-12 00:00 UTC 以降の M1 の 1 時間足 5 本 + 補った 2 本（00 時台・02 時台）
    assert len(lines) == 5 + 2 + 1
    assert lines[-3] == "18.0,19.5,18.0,19.5,2026-10-13 00:00:00+00:00"
    assert lines[-2] == "19.0,19.0,18.5,18.5,2026-10-13 02:00:00+00:00"
    lines = api.atm_iv_data(fixture_runner(fixtures(ATM_IV_OPTIONS, ATM_IV_HOURLY)), NOW, 7, 1).split("\n")
    assert lines[:-1] == ["16.0,16.5,15.5,16.25,2026-10-12 01:00:00+00:00", ",,,,2026-10-12 02:00:00+00:00",
                          "17.0,17.0,17.0,17.0,2026-10-13 01:00:00+00:00"]


def test_diff_atm_iv_fill_starts_after_last_bar():
    # 違い: 新系の定期クエリは 07:40 JST に前日 07:00 JST（22:00 UTC）までを作る。最後の足が 10-12 21:00 UTC のとき、
    # 現行は UTC の当日分（10-13 00:00〜）しか補わず 22:00・23:00 UTC の足が欠ける。
    # 新は最後の足の次（22:00 UTC）から補う
    hourly = [b for b in ATM_IV_HOURLY if b["started_at"] not in (h(22, 12), h(23, 12))]
    hourly.append(bar(h(21, 12), M1, 18.0, 18.0, 18.0, 18.0))
    options = ATM_IV_OPTIONS + [opt(h(22, 12, 30), 2, 25000, M1, 21.0, 300, is_atm=True),
                                opt(h(23, 12, 30), 2, 25000, M1, 22.0, 300, is_atm=True)]
    new = api.atm_iv_data(fixture_runner(fixtures(options, hourly)), NOW, 7, 0).split("\n")
    old = legacy_atm_iv_csv(hourly, options, 7, 0, NOW).split("\n")
    added = ["21.0,21.0,21.0,21.0,2026-10-12 22:00:00+00:00", "22.0,22.0,22.0,22.0,2026-10-12 23:00:00+00:00"]
    assert [x for x in new if x not in added] == old
    assert all(x in new for x in added) and all(x not in old for x in added)


def test_atm_iv_all_null_hour_same_as_legacy():
    # 補う 1 時間の iv が全部 NULL: ARRAY_AGG(... IGNORE NULLS) が NULL になり、
    # 現行（OFFSET）も新（SAFE_OFFSET）も値の空の足
    options = ATM_IV_OPTIONS + [opt(h(3, 13, 0), 2, 25125, M1, None, 255, is_atm=True)]
    new = api.atm_iv_data(fixture_runner(fixtures(options, ATM_IV_HOURLY)), NOW, 7, 0)
    assert new == legacy_atm_iv_csv(ATM_IV_HOURLY, options, 7, 0, NOW)
    assert ",,,,2026-10-13 03:00:00+00:00\n" in new


def test_diff_atm_iv_missing_month():
    # 違い: n 番目の限月が無いとき、現行は OFFSET のエラー（500）、新は 0 行（200）
    hourly = [b for b in ATM_IV_HOURLY if b["last_trading_day"] != M2]
    options = [o for o in ATM_IV_OPTIONS if o["last_trading_day"] != M2]
    with pytest.raises(BqError):
        legacy_atm_iv_csv(hourly, options, 7, 1, NOW)
    assert api.atm_iv_data(fixture_runner(fixtures(options, hourly)), NOW, 7, 1) == ""


def test_atm_iv_fill_respects_days():
    # N3: 1 時間足が遅れて補う始まり（最大 3 日前）が d 日の下端より前になっても、d 日より前の足は返さない
    # 最後の足は 10-08（補う始まりは 10-10 06:00）
    hourly = [b for b in ATM_IV_HOURLY if b["started_at"] < h(0, 10)]
    # 10-10 07:00 の足は d=1 の下端（10-12 00:00）より前
    options = ATM_IV_OPTIONS + [opt(h(7, 10, 0), 2, 25000, M1, 23.0, 300, is_atm=True),
                                opt(h(1, 12, 0), 2, 25000, M1, 22.0, 300, is_atm=True)]
    lines = api.atm_iv_data(fixture_runner(fixtures(options, hourly)), NOW, 1, 0).split("\n")
    assert lines[:-1] == ["22.0,22.0,22.0,22.0,2026-10-12 01:00:00+00:00",
                          "18.0,19.5,18.0,19.5,2026-10-13 00:00:00+00:00",
                          "19.0,19.0,18.5,18.5,2026-10-13 02:00:00+00:00"]
    # d=7 なら補った 10-10 の足も入る
    assert "23.0,23.0,23.0,23.0,2026-10-10 07:00:00+00:00\n" in api.atm_iv_data(
        fixture_runner(fixtures(options, hourly)), NOW, 7, 0)


# ---- /atm_iv_bars（#68 AC 5） ----

def jdt(day, hour, minute=0):
    """JST の 2026-10-<day> <hour>:<minute>（UTC の aware な datetime）"""
    return datetime(2026, 10, day, hour, minute, tzinfo=UTC) - timedelta(hours=9)


def jt(day, hour, minute=0):
    """jdt を固定値の CTE の TIMESTAMP のリテラルにする"""
    return jdt(day, hour, minute).strftime("%Y-%m-%d %H:%M:%S+00")


def put(at, strike, month, iv):
    return opt(at, 2, strike, month, iv, 300, is_atm=True)


# 1 限月（M1）の iv は 10〜30、2 限月（M2）は 50 台、満了した M0 は 90 台（混ざったら値で分かる）
# 2026-10-09 は金曜、10-12（月、スポーツの日）は休業日（fixture のカレンダー）
BARS_OPTIONS = [
    # 金曜の日中の終わり: 全部 NULL の分（16:00）、分の途中・5 分足の途中での ATM の行使価格の変化（16:01・16:03）
    put(jt(9, 15, 58), 25000, M1, 18.0), put(jt(9, 15, 58), 25125, M1, 18.4),  # 同じ created_at の 2 行
    put(jt(9, 15, 59), 25000, M1, 18.2),
    put(jt(9, 16, 0), 25000, M1, None),
    put(jt(9, 16, 1), 25000, M1, 18.5), put(jt(9, 16, 1), 25125, M1, None),
    put(jt(9, 16, 3), 25125, M1, 19.5),
    put(jt(9, 16, 4), 25125, M1, None),
    put(jt(9, 16, 7), 25125, M1, 19.1),
    put(jt(9, 16, 14), 25125, M1, 19.3),  # 日中の最後
    # 金曜の夜間（17:00〜土曜 06:14）→ 休み明けの火曜の日足
    put(jt(9, 17, 0), 25125, M1, 20.0),
    put(jt(9, 17, 2), 25125, M1, 20.4),
    put(jt(9, 23, 59), 25250, M1, 21.0),
    put(jt(10, 0, 30), 25250, M1, None),  # 全部 NULL の時間（土曜 00 時台）
    put(jt(10, 6, 14), 25250, M1, 20.8),
    # 火曜の日中
    put(jt(13, 9, 0), 25250, M1, 21.5),
    put(jt(13, 9, 14), 25250, M1, 21.2),
    put(jt(13, 9, 15), 25250, M1, 21.9),
    put(jt(13, 14, 59), 25125, M1, 19.9),
    # 2 限月が混在する時間帯
    put(jt(9, 16, 1), 25000, M2, 55.0), put(jt(9, 17, 2), 25125, M2, 56.0), put(jt(13, 9, 14), 25250, M2, 57.0),
    # 満了した限月（今日より前）・call・ATM でない行は使わない
    put(jt(9, 16, 2), 25000, M0, 95.0),
    opt(jt(9, 16, 2), 1, 25000, M1, 99.0, 1, is_atm=True),
    opt(jt(9, 16, 2), 2, 25500, M1, 99.0, 1, is_atm=False),
]
BARS_CALENDAR = [
    {"calendar_date": "2026-10-12", "is_trading_day": False},
    {"calendar_date": "2026-11-03", "is_trading_day": True},  # 祝日取引の実施日（休業日ではない）
]
BARS_START = datetime(2026, 10, 9, tzinfo=UTC)


def minute_rows(n, options=BARS_OPTIONS, start=BARS_START):
    fx = fixtures(options, calendar=BARS_CALENDAR)
    params = {"start_at": start, "end_at": NOW, "today": api.jst_date(NOW), "n": n}
    return fixture_runner(fx)("atm_iv_minute", params)


def mid_options(options=BARS_OPTIONS):
    """iv を bid_iv・ask_iv（mid = 元の iv）に移し、iv は使われたら分かる値（77）にした行"""
    out = []
    for r in options:
        iv = r["iv"]
        out.append({**r, "iv": 77.0, "bid_iv": None if iv is None else iv - 0.25,
                    "ask_iv": None if iv is None else iv + 0.25})
    return out


def mid_minute_rows(n, options):
    fx = fixtures(options, calendar=BARS_CALENDAR)
    params = {"start_at": BARS_START, "end_at": NOW, "today": api.jst_date(NOW), "n": n}
    return fixture_runner(fx)("atm_mid_minute", params)


@pytest.mark.parametrize("n", [0, 1, 2])
def test_bars_mid_same_as_trade_logic(n):
    # #93 AC 1: mid の 1 分足は、iv を mid に替えた atm_iv_minute と同じ
    # （限月の選び方・空の分・OHLC・同じ created_at の順）
    got = mid_minute_rows(n, mid_options())
    want = minute_rows(n)
    assert [r["started_at"] for r in got] == [r["started_at"] for r in want]
    for g, w in zip(got, want, strict=True):
        for k in ("open", "high", "low", "close"):
            assert (g[k] is None) == (w[k] is None) and (g[k] is None or g[k] == pytest.approx(w[k])), (g, w)
    assert all(r["high"] != 77.0 for r in got)


def test_bars_mid_needs_both_positive_quotes():
    at = jt(13, 10, 0)
    options = [
        opt(at, 2, 25000, M1, 50.0, 1, is_atm=True, bid_iv=20.0, ask_iv=None),
        opt(at, 2, 25125, M1, 50.0, 1, is_atm=True, bid_iv=0.0, ask_iv=21.0),
        opt(jt(13, 10, 1), 2, 25000, M1, 50.0, 1, is_atm=True, bid_iv=20.0, ask_iv=21.0),
        opt(jt(13, 10, 1), 2, 25125, M1, 50.0, 1, is_atm=True, bid_iv=-1.0, ask_iv=22.0),
    ]
    rows = mid_minute_rows(0, options)
    assert [(r["started_at"], r["open"], r["high"], r["low"], r["close"]) for r in rows] == [
        (jdt(13, 10, 0), None, None, None, None),
        (jdt(13, 10, 1), 20.5, 20.5, 20.5, 20.5),
    ]


def hourly_bars_block():
    text = (REPO_ROOT / "sql" / "ohlc" / "atm_hourly.sql").read_text(encoding="utf-8")
    # 印は行頭の行（先頭のコメントの説明文中の「-- bars:begin 〜 -- bars:end」は飛ばす）
    return text[text.index("\n-- bars:begin\n") + 1:text.index("\n-- bars:end\n") + 1]


def reference_bars(seconds, month, options=BARS_OPTIONS):
    """生の行から atm_hourly.sql の bars（1 時間足の計算）で作った足（値のある足）。
    seconds が 3600 以外は、正時への切り捨てを seconds 秒への切り捨てに替える（式はほかは同じ）"""
    block = hourly_bars_block()
    if seconds != 3600:
        block = block.replace("TIMESTAMP_TRUNC(created_at, HOUR)",
                              f"TIMESTAMP_SECONDS(DIV(UNIX_SECONDS(created_at), {seconds}) * {seconds})")
    sql = with_fixtures(
        "WITH\natm AS (\n  SELECT created_at, last_trading_day, target_price, iv, price FROM option_price\n"
        "  WHERE is_atm AND type = 2 AND created_at >= @start_at AND created_at < @end_at\n),\n" + block
        + "SELECT iv_open AS open, iv_high AS high, iv_low AS low, iv_close AS close, started_at FROM bars\n"
        "WHERE last_trading_day = @month AND iv_high IS NOT NULL ORDER BY started_at\n",
        fixtures(options))
    return typed(bq(sql, {"start_at": BARS_START, "end_at": NOW, "month": month}))


def test_bars_minute_month_and_empty():
    rows = minute_rows(0)
    # 1 分足: 値の無い分（16:00・16:04・土曜 00:30）も行は出る（API が除く）。限月は M1 だけ
    times = [r["started_at"] for r in rows]
    assert times == sorted(set(times))
    assert len(rows) == 17
    assert [r for r in rows if r["high"] is None] == [
        {"open": None, "high": None, "low": None, "close": None, "started_at": t}
        for t in (jdt(9, 16, 0), jdt(9, 16, 4), jdt(10, 0, 30))]
    assert all(10 <= r["high"] <= 30 for r in bars.drop_empty(rows))
    # 同じ created_at の 2 行は行使価格の順（atm_hourly と同じ）: open = 25000 の 18.0、close = 25125 の 18.4
    first = rows[0]
    assert (first["open"], first["high"], first["low"], first["close"]) == (18.0, 18.4, 18.0, 18.4)
    # 2 限月（n=1）は M2 だけ、3 番目は無い
    m2 = minute_rows(1)
    assert [r["high"] for r in m2] == [55.0, 56.0, 57.0]
    assert minute_rows(2) == []


@pytest.mark.parametrize("seconds", [300, 900, 3600])
@pytest.mark.parametrize("n,month", [(0, M1), (1, M2)])
def test_bars_resample_same_as_hourly_logic(seconds, n, month):
    got = bars.resample(minute_rows(n), seconds)
    want = reference_bars(seconds, month)
    assert got == want
    assert got
    # 取引の無い時間（16:15〜16:59 JST、土曜 06:15〜火曜 08:59 JST）の足は無い
    for r in got:
        assert not jdt(9, 16, 15) <= r["started_at"] < jdt(9, 17, 0)
        assert not jdt(10, 6, 15) <= r["started_at"] < jdt(13, 9, 0)


def test_bars_hourly_same_as_atm_hourly_values():
    # 1 時間足の値の一部を手で確かめる（16 時台: open は 16:01 の 25000 の 18.5、close は 16:14 の 19.3、
    # 全部 NULL の 16:00・16:04 は飛ばす）
    got = {r["started_at"]: r for r in bars.resample(minute_rows(0), 3600)}
    h16 = got[jdt(9, 16)]
    assert (h16["open"], h16["high"], h16["low"], h16["close"]) == (18.5, 19.5, 18.5, 19.3)
    # 土曜 00 時台（全部 NULL）は足を作らない
    assert jdt(10, 0) not in got


def test_bars_api_minute_window():
    # API（#77）: 範囲 [start, end) の足。10-10 00:00 UTC（土曜 09:00 JST）〜now
    fx = fixtures(BARS_OPTIONS, runs=[{"snapshot_at": utc(13, 5, 59)}], calendar=BARS_CALENDAR)
    run = fixture_runner(fx)
    start = datetime(2026, 10, 10, tzinfo=UTC)
    lines = api.atm_iv_bars(run, NOW, "15m", 0, (start, NOW))[0].splitlines()
    want = [r for r in reference_bars(900, M1) if r["started_at"] >= start]
    assert lines == csvfmt.atm_iv_csv(want).splitlines()
    assert lines and all(not line.startswith(",") for line in lines)
    assert api.atm_iv_bars(run, NOW, "1m", 0, (start, NOW))[0] == csvfmt.atm_iv_csv(
        [r for r in bars.drop_empty(minute_rows(0)) if r["started_at"] >= start])
    # 上端: 10-09 16:10 JST より前だけ
    end = jdt(9, 16, 10)
    assert api.atm_iv_bars(run, NOW, "1m", 0, (jdt(9, 0), end))[0] == csvfmt.atm_iv_csv(
        [r for r in bars.drop_empty(minute_rows(0)) if r["started_at"] < end])
    # 既定（範囲なし）: 最新のスナップショットの 1 日前の UTC の 0 時（10-12）から
    text, s, e = api.atm_iv_bars(run, NOW, "1m", 0)
    assert (s, e) == (datetime(2026, 10, 12, tzinfo=UTC), NOW)
    assert text == csvfmt.atm_iv_csv([r for r in bars.drop_empty(minute_rows(0)) if r["started_at"] >= s])


# 日足用の 1 時間足（atm_iv）。最後の足は土曜 05:00 JST、その後（火曜の日中）は option_price から補う
BARS_HOURLY = [
    bar(jt(8, 15), M1, 30.0, 30.0, 30.0, 30.0),  # 木曜の日中
    bar(jt(8, 17), M1, 22.0, 23.0, 21.5, 22.5),  # 木曜の夜間 → 金曜
    bar(jt(9, 9), M1, 22.5, 24.0, 22.0, 23.0),  # 金曜の日中
    bar(jt(9, 16), M1, None, None, None, None),  # 全部 NULL の時間
    bar(jt(9, 17), M1, 23.0, 23.5, 20.0, 21.0),  # 金曜の夜間 → 休み明け
    bar(jt(10, 5), M1, 21.0, 22.0, 21.0, 21.5),
    bar(jt(9, 9), M2, 60.0, 60.0, 60.0, 60.0),
]


@pytest.mark.parametrize("calendar,want", [
    # 月曜（10-12）が休業日: 金曜の夜間は火曜の日足に入る
    (BARS_CALENDAR, ["30.0,30.0,30.0,30.0,2026-10-07 15:00:00+00:00",
                     "22.0,24.0,21.5,23.0,2026-10-08 15:00:00+00:00",
                     "23.0,23.5,19.9,19.9,2026-10-12 15:00:00+00:00"]),
    # 月曜が祝日取引の実施日（取引日）: 金曜の夜間は月曜の日足、火曜の日中は火曜
    ([{"calendar_date": "2026-10-12", "is_trading_day": True}],
     ["30.0,30.0,30.0,30.0,2026-10-07 15:00:00+00:00",
      "22.0,24.0,21.5,23.0,2026-10-08 15:00:00+00:00",
      "23.0,23.5,20.0,21.5,2026-10-11 15:00:00+00:00",
      "21.5,21.9,19.9,19.9,2026-10-12 15:00:00+00:00"]),
])
def test_bars_daily_friday_night_goes_to_next_trading_day(calendar, want):
    fx = fixtures(BARS_OPTIONS, BARS_HOURLY, calendar=calendar)
    run = fixture_runner(fx)
    rng = (datetime(2026, 10, 6, tzinfo=UTC), NOW)
    assert api.atm_iv_bars(run, NOW, "1d", 0, rng)[0].splitlines() == want
    # 2 限月は M2 の足だけ
    m2 = api.atm_iv_bars(run, NOW, "1d", 1, rng)[0].splitlines()
    assert m2[0] == "60.0,60.0,60.0,60.0,2026-10-08 15:00:00+00:00"
    assert all(float(line.split(",")[1]) >= 50 for line in m2)


def test_bars_daily_range_by_trading_day():
    # 範囲 [10-08 00:00 UTC, now): 00:00 JST（started_at）が範囲の前の木曜（10-07 15:00 UTC）の日足は出さない
    fx = fixtures(BARS_OPTIONS, BARS_HOURLY, calendar=BARS_CALENDAR)
    rng = (datetime(2026, 10, 8, tzinfo=UTC), NOW)
    lines = api.atm_iv_bars(fixture_runner(fx), NOW, "1d", 0, rng)[0].splitlines()
    assert [line.split(",")[4] for line in lines] == ["2026-10-08 15:00:00+00:00", "2026-10-12 15:00:00+00:00"]


def test_bars_hourly_same_as_atm_iv_data():
    # 1 時間足は /atm_iv_data と同じ足（値の無い足を除く）
    fx = fixtures(ATM_IV_OPTIONS, ATM_IV_HOURLY)
    for d, n in ((7, 0), (20, 0), (7, 1)):
        data = api.atm_iv_data(fixture_runner(fx), NOW, d, n).splitlines()
        rng = (api.utc_day(NOW - timedelta(days=d)), NOW)
        assert api.atm_iv_bars(fixture_runner(fx), NOW, "1h", n, rng)[0].splitlines() == [
            line for line in data if not line.startswith(",")]


def test_bars_hourly_past_range_without_fill():
    # #77: 過去だけの範囲（end が補う分の始まり以前）は生データを読まず、1 時間足だけから同じ足を返す
    fx = fixtures(ATM_IV_OPTIONS, ATM_IV_HOURLY)
    # 1 時間足の最後は 10-12 23:00 UTC（補う分は 10-13 00:00 から）。範囲の上端は 10-12 22:00
    end = datetime(2026, 10, 12, 22, tzinfo=UTC)
    start = api.utc_day(NOW - timedelta(days=20))
    params = api.hourly_params(NOW, datetime(2026, 10, 12, 23, tzinfo=UTC), start, end, 0)
    assert params["fill_end"] < params["fill_start"]
    data = api.atm_iv_data(fixture_runner(fx), NOW, 20, 0).splitlines()
    got = api.atm_iv_bars(fixture_runner(fx), NOW, "1h", 0, (start, end))[0]
    want = [line for line in data if not line.startswith(",") and datetime.fromisoformat(line.split(",")[4]) < end]
    assert got.splitlines() == want
    assert len(want) == 6  # 10-03・10-08 の 2 本・10-12 の 3 本（10-13 の補った足と 10-12 22:00 以降は入らない）


# ---- /atm_iv_bars?n=roll（#101 AC 6） ----

MONTHS_TYPES = {"last_trading_day": date.fromisoformat}


def months_rows(name, fx, params):
    return fixture_runner(fx, {name: MONTHS_TYPES})(name, params)


def of_month(rows, month):
    return [{k: v for k, v in r.items() if k != "last_trading_day"} for r in rows if r["last_trading_day"] == month]


def test_minute_months_same_as_one_month():
    # 全限月の 1 分足を限月で分けると、n=0（M1）・n=1（M2）の 1 分足と同じ（値の無い分・OHLC・時刻）。
    # 満了した M0 の行も出る（選ばないのは bars.roll）
    fx = fixtures(BARS_OPTIONS, calendar=BARS_CALENDAR)
    rows = months_rows("atm_iv_minute_months", fx, {"start_at": BARS_START, "end_at": NOW})
    assert of_month(rows, M1) == minute_rows(0)
    assert of_month(rows, M2) == minute_rows(1)
    assert of_month(rows, M0)
    keys = [(r["started_at"], r["last_trading_day"]) for r in rows]
    assert keys == sorted(set(keys))


def test_mid_minute_months_same_as_one_month():
    fx = fixtures(mid_options(), calendar=BARS_CALENDAR)
    rows = months_rows("atm_mid_minute_months", fx, {"start_at": BARS_START, "end_at": NOW})
    assert of_month(rows, M1) == mid_minute_rows(0, mid_options())
    assert of_month(rows, M2) == mid_minute_rows(1, mid_options())


@pytest.mark.parametrize("start,end", [
    (datetime(2026, 9, 23, tzinfo=UTC), NOW),  # 最新を含む（補う分を読む）
    (datetime(2026, 10, 12, tzinfo=UTC), NOW),
    (datetime(2026, 9, 23, tzinfo=UTC), datetime(2026, 10, 12, 22, tzinfo=UTC)),  # 過去だけ（生データを読まない）
])
def test_atm_iv_months_same_as_atm_iv(start, end):
    # 全限月の 1 時間足（+ 補った足）を限月で分けると、同じ範囲の atm_iv（n=0・n=1）の足と同じ
    fx = fixtures(ATM_IV_OPTIONS, ATM_IV_HOURLY)
    last = datetime(2026, 10, 12, 23, tzinfo=UTC)
    rows = months_rows("atm_iv_months", fx, api.roll_hourly_params(NOW, last, start, end))
    for n, month in ((0, M1), (1, M2)):
        want = fixture_runner(fx)("atm_iv", api.hourly_params(NOW, last, start, end, n))
        assert of_month(rows, month) == want, n
        assert want
    assert all(start <= r["started_at"] < end for r in rows)


def same_bars(got, want):
    assert [(r["started_at"], r.get("last_trading_day")) for r in got] == [
        (r["started_at"], r.get("last_trading_day")) for r in want]
    for g, w in zip(got, want, strict=True):
        for k in ("open", "high", "low", "close"):
            assert (g[k] is None) == (w[k] is None) and (g[k] is None or g[k] == pytest.approx(w[k])), (g, w)


@pytest.mark.parametrize("start,end", [
    (datetime(2026, 9, 23, tzinfo=UTC), NOW),  # 最新を含む（補う分を読む）
    (datetime(2026, 9, 23, tzinfo=UTC), datetime(2026, 10, 12, 22, tzinfo=UTC)),  # 過去だけ（生データを読まない）
])
def test_atm_mid_same_as_trade_logic(start, end):
    # #98: 1 時間足・日足の mid（atm_mid.sql・atm_mid_months.sql）は、1 時間足の表を atm_mid_iv に、
    # 補う分を mid に替えた atm_iv.sql・atm_iv_months.sql と同じ
    # （限月の選び方・期間・補う分・OHLC・同じ created_at の順）。
    # mid の固定値は iv を bid_iv・ask_iv に移し（mid = 元の iv）、iv は 77 にし、atm_iv は空にする（読んだら分かる）
    trade = fixtures(ATM_IV_OPTIONS, ATM_IV_HOURLY)
    mid = fixtures(mid_options(ATM_IV_OPTIONS), mid_hourly=ATM_IV_HOURLY)
    last = datetime(2026, 10, 12, 23, tzinfo=UTC)
    for n in (0, 1):
        params = api.hourly_params(NOW, last, start, end, n)
        want = fixture_runner(trade)("atm_iv", params)
        assert want
        same_bars(fixture_runner(mid)("atm_mid", params), want)
    params = api.roll_hourly_params(NOW, last, start, end)
    want = months_rows("atm_iv_months", trade, params)
    assert want
    same_bars(months_rows("atm_mid_months", mid, params), want)


def test_atm_mid_fill_needs_both_positive_quotes():
    # 補う分の mid は bid_iv・ask_iv が両方 0 より大きいときだけ（分足の atm_mid_minute.sql と同じ規則）
    at = datetime(2026, 10, 13, 2, 10, tzinfo=UTC)
    options = [
        opt(at, 2, 25000, M1, 50.0, 1, is_atm=True, bid_iv=20.0, ask_iv=None),
        opt(at + timedelta(minutes=1), 2, 25000, M1, 50.0, 1, is_atm=True, bid_iv=0.0, ask_iv=21.0),
        opt(at + timedelta(minutes=2), 2, 25000, M1, 50.0, 1, is_atm=True, bid_iv=20.0, ask_iv=21.0),
        opt(at + timedelta(minutes=3), 2, 25000, M1, 50.0, 1, is_atm=True, bid_iv=-1.0, ask_iv=22.0),
        opt(datetime(2026, 10, 13, 3, 0, tzinfo=UTC), 2, 25000, M1, 50.0, 1, is_atm=True),
    ]
    last = datetime(2026, 10, 12, 23, tzinfo=UTC)
    rows = fixture_runner(fixtures(options))("atm_mid", api.hourly_params(NOW, last, datetime(2026, 10, 13, tzinfo=UTC),
                                                                         NOW, 0))
    assert [(r["started_at"], r["open"], r["high"], r["low"], r["close"]) for r in rows] == [
        (datetime(2026, 10, 13, 2, tzinfo=UTC), 20.5, 20.5, 20.5, 20.5),
        (datetime(2026, 10, 13, 3, tzinfo=UTC), None, None, None, None),
    ]


@pytest.mark.parametrize("tf", ["1m", "15m", "1h", "1d"])
def test_roll_api_same_as_n0_without_switch(tf):
    # 期近（M1 = 11-12）の残存が長い期間: 期近つなぎは n=0 と同じ足（6 列目が M1）。
    # 満了した M0・2 限月の M2 は混ざらない
    fx = fixtures(BARS_OPTIONS, BARS_HOURLY, runs=[{"snapshot_at": utc(13, 5, 59)}], calendar=BARS_CALENDAR)
    run = fixture_runner(fx, {"atm_iv_months": MONTHS_TYPES, "atm_iv_minute_months": MONTHS_TYPES})
    rng = (BARS_START, NOW) if tf in ("1m", "15m") else (datetime(2026, 10, 6, tzinfo=UTC), NOW)
    roll = api.atm_iv_bars(run, NOW, tf, "roll", rng)[0].splitlines()
    zero = api.atm_iv_bars(run, NOW, tf, 0, rng)[0].splitlines()
    assert roll and [line.rsplit(",", 1)[0] for line in roll] == zero
    assert all(line.endswith(",2026-11-12") for line in roll)


# ---- /smile_snapshot（#79 AC 6） ----

def latest_body(func, start, end):
    """sql/latest/<func>.sql（テーブル関数の本文）を、引数を @パラメータに・表を固定値の CTE にしたサブクエリにする
    （new_sql の差し替えは created_at で絞るだけで最新版の選択を通らないので、再取り込みの検査にはこちらを使う）"""
    text = (REPO_ROOT / "sql" / "latest" / f"{func}.sql").read_text(encoding="utf-8")
    text = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("--"))
    text = re.sub(r"\bstart_at\b", start, text)
    text = re.sub(r"\bend_at\b", end, text)
    text = text.replace("`${project}.${dataset}.option_price`", "option_price")
    text = text.replace("`${project}.${dataset}.ingest_runs`", "ingest_runs")
    assert "${" not in text, func
    return f"({text})"


def latest_sql(name, fx):
    """新しい SQL のテーブル関数の参照を、本文（sql/latest/*.sql）を固定値に当てたものに置き換える"""
    text = (SQL_DIR / f"{name}.sql").read_text(encoding="utf-8")
    for func in ("option_price_latest", "latest_runs"):
        text = re.sub(rf"`\$\{{project\}}\.\$\{{dataset\}}\.{func}`\((@\w+), (@\w+)\)",
                      lambda m, f=func: latest_body(f, m.group(1), m.group(2)), text)
    assert "${" not in text, name
    return with_fixtures(text, fx)


SNAPSHOT_TYPES = {
    "snapshot_at": ts, "month": int, "type": int, "target_price": int, "last_trading_day": date.fromisoformat,
    "is_atm": lambda v: v == "true", "iv": float, "price": int, "price_time": int, "bid_iv": float, "ask_iv": float,
    "bid": int, "ask": int,
}


def snapshot_runner(fx):
    """api.smile_snapshot の runner（テーブル関数は本文どおりに最新版を選ぶ）"""
    def run(name, params):
        return typed(bq(latest_sql(name, fx), params), SNAPSHOT_TYPES)

    return run


def snapshot_csv(fx, at):
    return api.smile_snapshot(snapshot_runner(fx), at)


def quote(at, typ, strike, month, iv, bid_iv, ask_iv, run, **kw):
    return opt(at, typ, strike, month, iv, 300, jst(13, 14, 50), bid_iv=bid_iv, ask_iv=ask_iv, bid=290, ask=310,
               ingest_run_id=run, **kw)


def test_smile_snapshot_reingest_uses_newest_run():
    # 同じスナップショット（SNAP）に 2 回の実行: 古い実行 "a"（値 99、a にだけある行使価格 24500）と、
    # 後で終わった再取り込み "b"。新しい実行の値だけを返す
    options = [
        quote(SNAP, 2, 24500, M1, 99.0, 99.0, 99.0, "a"),
        quote(SNAP, 2, 25000, M1, 99.0, 99.0, 99.0, "a", is_atm=True),
        quote(SNAP, 1, 25000, M1, 99.0, 99.0, 99.0, "a"),
        quote(SNAP, 2, 25000, M1, 19.0, 18.5, 19.5, "b", is_atm=True),
        quote(SNAP, 1, 25000, M1, 18.0, 17.5, 18.5, "b"),
        # 前のスナップショット（実行 "c"）は読まない
        quote(utc(13, 5, 58), 2, 25000, M1, 88.0, 88.0, 88.0, "c", is_atm=True),
    ]
    runs = [{"snapshot_at": SNAP, "ingest_run_id": "a", "finished_at": utc(13, 5, 59)},
            {"snapshot_at": SNAP, "ingest_run_id": "b", "finished_at": utc(13, 7, 0)},
            {"snapshot_at": utc(13, 5, 58), "ingest_run_id": "c", "finished_at": utc(13, 5, 58)}]
    lines = snapshot_csv(fixtures(options, runs=runs), NOW).split("\n")
    assert lines[0] == f"{int(SNAP_DT.timestamp())},2026-11-12,25000,2026-11-12,25000"
    body = lines[1:-1]
    pt = int(datetime(2026, 10, 13, 5, 50, tzinfo=UTC).timestamp())  # 14:50 JST
    assert [line for line in body if line.startswith("1,")] == [
        f"1,1,25000,18.0,300,{pt},17.5,18.5,290,310,18.0,1",
        f"1,2,25000,19.0,300,{pt},18.5,19.5,290,310,19.0,0",
    ]
    assert "99" not in "\n".join(body) and "88" not in "\n".join(body)
    # 新しい実行 "b" が先に終わっていたら（再取り込みが古い）"a" を使う: 選び方は latest_runs と同じ finished_at
    runs[1]["finished_at"] = utc(13, 5, 59)
    runs[0]["finished_at"] = utc(13, 7, 0)
    assert "24500" in snapshot_csv(fixtures(options, runs=runs), NOW)


def test_smile_snapshot_weekend_at_returns_friday_last():
    # 2026-10-09（金）の夜間の最後 = 10-10（土）06:00 JST のスナップショット。その後は 10-13（火）08:45 JST まで無い
    fri_last = jdt(10, 6, 0)
    snaps = [(9, 15, 0), (10, 6, 0), (13, 8, 45)]  # JST の (日, 時, 分)
    options = [quote(jt(*s), 2, 25000, M1, 20.0 + i, 19.0 + i, 21.0 + i, f"r{i}", is_atm=True)
               for i, s in enumerate(snaps)]
    runs = [{"snapshot_at": jt(*s), "ingest_run_id": f"r{i}", "finished_at": jt(*s)} for i, s in enumerate(snaps)]
    fx = fixtures(options, runs=runs)
    # 日曜 12:00 JST・月曜（休業日）12:00 JST → 金曜の最後（土曜 06:00 JST）
    for at in (jdt(11, 12, 0), jdt(12, 12, 0), fri_last + timedelta(seconds=59)):
        head = snapshot_csv(fx, at).split("\n")[0]
        assert head.split(",")[0] == str(int(fri_last.timestamp())), at
    # at ちょうどのスナップショットを含む・1 秒前なら前のもの
    assert snapshot_csv(fx, fri_last).split(",")[0] == str(int(fri_last.timestamp()))
    assert snapshot_csv(fx, fri_last - timedelta(seconds=1)).split(",")[0] == str(int(jdt(9, 15, 0).timestamp()))
    # 14 日より前しか無ければデータなし（空）
    assert snapshot_csv(fx, jdt(13, 8, 45) + timedelta(days=14, seconds=1)) == ""


def test_smile_snapshot_next_crosses_gaps():
    # #80: dir=next は at 以後で最初のスナップショット（at ちょうどを含む）。週末・引け後の at から次の取引の最初へ。
    # 最新より後なら at 以前で最新
    snaps = [(9, 15, 44), (9, 17, 0), (10, 6, 0), (13, 8, 45)]  # JST の (日, 時, 分)
    options = [quote(jt(*s), 2, 25000, M1, 20.0 + i, 19.0 + i, 21.0 + i, f"r{i}", is_atm=True)
               for i, s in enumerate(snaps)]
    runs = [{"snapshot_at": jt(*s), "ingest_run_id": f"r{i}", "finished_at": jt(*s)} for i, s in enumerate(snaps)]
    fx = fixtures(options, runs=runs)

    def head(at):
        return api.smile_snapshot(snapshot_runner(fx), at, True).split(",")[0]

    for at, want in [
        (jdt(9, 15, 45), jdt(9, 17, 0)),  # 日中の引けの後 → 夜間の始まり
        (jdt(9, 17, 0), jdt(9, 17, 0)),  # ちょうど
        (jdt(10, 6, 1), jdt(13, 8, 45)),  # 土曜の朝 → 休み明けの火曜
        (jdt(11, 12, 0), jdt(13, 8, 45)),
        (jdt(13, 8, 46), jdt(13, 8, 45)),  # 最新より後 → 最新
    ]:
        assert head(at) == str(int(want.timestamp())), at


def test_smile_snapshot_months_same_as_smile_sql():
    # 限月の割り当て（1 限月 = last_trading_day の最小、2 限月 = 最大）と約定の IV・時刻が smile.sql と同じ
    fx = fixtures(SMILE_OPTIONS, runs=[{"snapshot_at": utc(13, 5, 58)}, {"snapshot_at": SNAP}])
    params = {"start_at": SNAP_DT, "end_at": SNAP_DT + timedelta(seconds=1)}
    smile_rows = fixture_runner(fx, SMILE_TYPES)("smile", params)
    rows = fixture_runner(fx, {"smile_snapshot": SNAPSHOT_TYPES})("smile_snapshot", params)
    got = {}
    for r in rows:
        side = "call" if r["type"] == 1 else "put"
        got[(r["target_price"], f"o{r['month']}_{side}_iv")] = r["iv"]
        got[(r["target_price"], f"o{r['month']}_{side}_price_time")] = r["price_time"]
    want = {}
    for r in smile_rows:
        for c in csvfmt.SMILE_COLUMNS[1:]:
            if r[c] is not None:
                want[(r["target_price"], c)] = r[c]
    assert {k: v for k, v in got.items() if v is not None} == want
    assert {r["month"] for r in rows} == {1, 2}
    assert {r["last_trading_day"] for r in rows if r["month"] == 1} == {M1}
    assert {r["last_trading_day"] for r in rows if r["month"] == 2} == {M2}
    # API の結果のメタ行: 限月ごとの ATM（put の is_atm）
    head = api.smile_snapshot(fixture_runner(fx, {"smile_snapshot": SNAPSHOT_TYPES}), NOW).split("\n")[0]
    assert head == f"{int(SNAP_DT.timestamp())},2026-11-12,25000,2026-12-10,25000"


# ---- 実テーブルの dry run（AC 6） ----

def real_sql(name):
    text = (SQL_DIR / f"{name}.sql").read_text(encoding="utf-8")
    return text.replace("${project}", PROJECT).replace("${dataset}", DATASET)


def dry_run_params(now):
    return {
        "latest_snapshot": {"start_at": now - timedelta(days=14), "end_at": now},
        "smile": {"start_at": now - timedelta(minutes=1), "end_at": now - timedelta(minutes=1) + timedelta(seconds=1)},
        "atm_iv_meta": {"start_at": now - timedelta(days=14), "end_at": now},
        "atm_iv": api.atm_iv_params(now, None, 100, 0),
    }


@pytest.mark.parametrize("name", ["latest_snapshot", "smile", "atm_iv_meta", "atm_iv"])
def test_dry_run_real_tables(name, record_property):
    now = datetime.now(UTC).replace(microsecond=0)
    nbytes = bq(real_sql(name), dry_run_params(now)[name], dry_run=True, maximum_bytes=GiB)
    record_property("bytes", nbytes)
    print(f"dry run {name}: {nbytes} bytes ({nbytes / 1024**2:.1f} MiB)")
    assert nbytes < GiB


def bars_dry_run_params(end):
    """/atm_iv_bars が許す 1 回の最大の期間（settings.BARS_RANGE）で、end（最新の完了した日の終わり = UTC の 0 時）を
    終わりにしたパラメータ（#77）。当日（書き込み中）のパーティションは dry run が 0 を返すので含めない。
    分足は日の境目に揃えない 3 日（4 日分のパーティション）が最大"""
    minute_max = api.st.BARS_RANGE["1m"][1]
    daily_max = api.st.BARS_RANGE["1d"][1]
    daily_start = end - timedelta(days=daily_max + api.st.DAILY_LOOKBEHIND_DAYS)
    return {
        "atm_iv_minute": ("atm_iv_minute", {"start_at": end - timedelta(days=minute_max + 1, hours=-1),
                                            "end_at": end - timedelta(days=1, hours=-1), "today": api.jst_date(end),
                                            "n": 0}),
        "atm_mid_minute": ("atm_mid_minute", {"start_at": end - timedelta(days=minute_max + 1, hours=-1),
                                              "end_at": end - timedelta(days=1, hours=-1), "today": api.jst_date(end),
                                              "n": 0}),
        "trading_holidays": ("trading_holidays", {
            "start_date": api.jst_date(daily_start) - timedelta(days=1),
            "end_date": api.jst_date(end) + timedelta(days=api.st.HOLIDAYS_AHEAD_DAYS)}),
        # 1h・1d の最新を含む範囲（補う分は最大 3 日。1 時間足が無いときが最大）
        "atm_iv": ("atm_iv", api.hourly_params(end, None, daily_start, end, 0)),
        # 過去だけの範囲（左への追加読み込み）: 生データを読まない
        "atm_iv_past": ("atm_iv", api.hourly_params(end, None, daily_start, end - timedelta(days=5), 0)),
        # 期近つなぎ（#101）: 同じ期間の全限月のクエリと、乗り換えを数える先までの休業日
        "atm_iv_minute_months": ("atm_iv_minute_months", {"start_at": end - timedelta(days=minute_max + 1, hours=-1),
                                                          "end_at": end - timedelta(days=1, hours=-1)}),
        "atm_mid_minute_months": ("atm_mid_minute_months", {"start_at": end - timedelta(days=minute_max + 1, hours=-1),
                                                            "end_at": end - timedelta(days=1, hours=-1)}),
        "atm_iv_months": ("atm_iv_months", api.roll_hourly_params(end, None, daily_start, end)),
        # mid の 1 時間足・日足（#98）: 約定と同じ期間（1 時間足は atm_mid_iv、補う分は bid_iv・ask_iv）
        "atm_mid": ("atm_mid", api.hourly_params(end, None, daily_start, end, 0)),
        "atm_mid_past": ("atm_mid", api.hourly_params(end, None, daily_start, end - timedelta(days=5), 0)),
        "atm_mid_months": ("atm_mid_months", api.roll_hourly_params(end, None, daily_start, end)),
        "atm_iv_months_past": ("atm_iv_months", api.roll_hourly_params(end, None, daily_start,
                                                                       end - timedelta(days=5))),
        "trading_holidays_roll": ("trading_holidays", {
            "start_date": api.jst_date(daily_start) - timedelta(days=1),
            "end_date": api.jst_date(end) + timedelta(days=api.st.ROLL_HOLIDAYS_AHEAD_DAYS)}),
    }


def completed_day_end(now):
    """最新の完了した日の終わり（UTC の 0 時）。UTC の 1 日（JST 09:00〜翌 08:59）は日中 + 夜間。夜間が終わった後
    （22:00 UTC = 07:00 JST 以降）なら当日の UTC の日も書き込みが終わっている"""
    return api.utc_day(now) + timedelta(days=1 if now.hour >= 22 else 0)


@pytest.mark.parametrize("name", ["smile_snapshot", "latest_snapshot", "next_snapshot"])
def test_dry_run_smile_snapshot_within_512mib(name, record_property):
    # #79 AC 7: 最新の完了した日の最後のスナップショット（23:59 UTC）を at にした /smile_snapshot の 2 クエリ
    at = completed_day_end(datetime.now(UTC)) - timedelta(minutes=1)
    params = {
        "latest_snapshot": {"start_at": at - timedelta(days=api.st.SEARCH_DAYS), "end_at": at + timedelta(seconds=1)},
        "smile_snapshot": {"start_at": at, "end_at": at + timedelta(seconds=1)},
        "next_snapshot": {"start_at": at - timedelta(days=api.st.SEARCH_DAYS), "end_at": at},
    }[name]
    nbytes = bq(real_sql(name), params, dry_run=True, maximum_bytes=GiB)
    record_property("bytes", nbytes)
    print(f"dry run {name} (at {at:%Y-%m-%d %H:%M} UTC): {nbytes} bytes ({nbytes / MiB:.1f} MiB)")
    assert nbytes <= 512 * MiB


@pytest.mark.parametrize("name", ["atm_iv_minute", "atm_mid_minute", "trading_holidays", "atm_iv", "atm_iv_past",
                                  "atm_iv_minute_months", "atm_mid_minute_months", "atm_iv_months",
                                  "atm_iv_months_past", "trading_holidays_roll", "atm_mid", "atm_mid_past",
                                  "atm_mid_months"])
def test_dry_run_bars_within_512mib(name, record_property):
    now = datetime.now(UTC)
    # UTC の 1 日（JST 09:00〜翌 08:59）は日中 + 夜間。夜間が終わった後（22:00 UTC = 07:00 JST 以降）なら
    # 当日の UTC の日も書き込みが終わっている
    end = api.utc_day(now) + timedelta(days=1 if now.hour >= 22 else 0)
    sql_name, params = bars_dry_run_params(end)[name]
    nbytes = bq(real_sql(sql_name), params, dry_run=True, maximum_bytes=GiB)
    record_property("bytes", nbytes)
    print(f"dry run {name} (end {end:%Y-%m-%d}): {nbytes} bytes ({nbytes / MiB:.1f} MiB)")
    assert nbytes <= 512 * MiB

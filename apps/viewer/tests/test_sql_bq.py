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
          ("iv", "FLOAT64"), ("price", "INT64"), ("price_time", "DATETIME"), ("is_atm", "BOOL")]
HOURLY = [("open", "FLOAT64"), ("high", "FLOAT64"), ("low", "FLOAT64"), ("close", "FLOAT64"),
          ("last_trading_day", "DATE"), ("time_frame", "INT64"), ("started_at", "TIMESTAMP")]
RUNS = [("snapshot_at", "TIMESTAMP")]
CALENDAR = [("calendar_date", "DATE"), ("is_trading_day", "BOOL")]


def opt(at, typ, strike, month, iv=None, price=None, price_time=None, is_atm=False):
    return {"created_at": at, "type": typ, "target_price": strike, "last_trading_day": month, "iv": iv,
            "price": price, "price_time": price_time, "is_atm": is_atm}


def bar(at, month, o, h, low, c):
    return {"open": o, "high": h, "low": low, "close": c, "last_trading_day": month, "time_frame": 3600,
            "started_at": at}


def fixtures(options=(), hourly=(), runs=(), calendar=()):
    return ",\n".join([cte("option_price", OPTION, list(options)), cte("atm_iv", HOURLY, list(hourly)),
                       cte("ingest_runs", RUNS, list(runs)), cte("trading_calendar_latest", CALENDAR, list(calendar))])


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
        "trading_holidays": ("trading_holidays", {
            "start_date": api.jst_date(daily_start) - timedelta(days=1),
            "end_date": api.jst_date(end) + timedelta(days=api.st.HOLIDAYS_AHEAD_DAYS)}),
        # 1h・1d の最新を含む範囲（補う分は最大 3 日。1 時間足が無いときが最大）
        "atm_iv": ("atm_iv", api.hourly_params(end, None, daily_start, end, 0)),
        # 過去だけの範囲（左への追加読み込み）: 生データを読まない
        "atm_iv_past": ("atm_iv", api.hourly_params(end, None, daily_start, end - timedelta(days=5), 0)),
    }


@pytest.mark.parametrize("name", ["atm_iv_minute", "trading_holidays", "atm_iv", "atm_iv_past"])
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

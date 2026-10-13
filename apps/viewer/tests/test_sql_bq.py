"""ビューアーの SQL を BigQuery で実際に動かす検査（#52 AC 5・6）。

ルートの tests/test_sql_bq.py と同じ流儀（bq CLI）。CI では動かさない（BigQuery の認証が要る）。手元で:
    OPTIONCHAN_BQ_TEST=1 OPTIONCHAN_BQ_PROJECT=<project-id> uv run --locked pytest -q tests/test_sql_bq.py
データセットは OPTIONCHAN_BQ_DATASET（既定 optionchan_tokyo）。gcloud / bq の構成は
CLOUDSDK_ACTIVE_CONFIG_NAME 等で渡す。

- ロジックの同値（AC 5）: 固定値の CTE（option_price・atm_iv・ingest_runs の代わり）に、現行の 3 関数の SQL
  （legacy_sql.py）と新しい SQL（テーブル関数の参照を、固定値の CTE を期間で絞ったものに置き換える）を当て、
  CSV（csvfmt.py）を比べる。新しい SQL は api.py の手順（2 段のクエリ・パラメータ）どおりに流す
  （runner を bq CLI にしたもの）。テーブルは読まない（課金 0）
- 違いが出る点は test_diff_* に、違いを示す入力で明記する
- 実テーブルの dry run（AC 6）: 見積もりバイト数が 1 GiB 未満（値は PR に記録する）
"""
import json
import os
import re
import subprocess
from datetime import UTC, date, datetime, timedelta

import legacy_sql
import pytest
from conftest import PKG_DIR

from optionchan_viewer import api, csvfmt

PROJECT = os.environ.get("OPTIONCHAN_BQ_PROJECT", "")
DATASET = os.environ.get("OPTIONCHAN_BQ_DATASET", "optionchan_tokyo")
LOCATION = os.environ.get("OPTIONCHAN_BQ_LOCATION", "asia-northeast1")

pytestmark = pytest.mark.skipif(
    os.environ.get("OPTIONCHAN_BQ_TEST") != "1" or not PROJECT,
    reason="OPTIONCHAN_BQ_TEST=1 と OPTIONCHAN_BQ_PROJECT のときだけ",
)

GiB = 1024**3
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
    "time": int, "o1_call_price_time": int, "o1_put_price_time": int, "o2_call_price_time": int,
    "o2_put_price_time": int,
    "o1_put_is_atm": lambda v: v == "true", "o2_put_is_atm": lambda v: v == "true",
    "open": float, "high": float, "low": float, "close": float, "iv": float, "price": float,
    "o1_call_iv": float, "o1_put_iv": float, "o2_call_iv": float, "o2_put_iv": float,
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


def opt(at, typ, strike, month, iv=None, price=None, price_time=None, is_atm=False):
    return {"created_at": at, "type": typ, "target_price": strike, "last_trading_day": month, "iv": iv,
            "price": price, "price_time": price_time, "is_atm": is_atm}


def bar(at, month, o, h, low, c):
    return {"open": o, "high": h, "low": low, "close": c, "last_trading_day": month, "time_frame": 3600,
            "started_at": at}


def fixtures(options=(), hourly=(), runs=()):
    return ",\n".join([cte("option_price", OPTION, list(options)), cte("atm_iv", HOURLY, list(hourly)),
                       cte("ingest_runs", RUNS, list(runs))])


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


# ---- /atm_data ----

def m(minute, hour=5, day=13):
    return utc(day, hour, minute)


ATM_OPTIONS = [
    # M1 の ATM（call と put の両方を平均する。現行どおり type を絞らない）
    opt(m(1), 1, 25000, M1, 18.0, 300, is_atm=True),
    opt(m(1), 2, 25000, M1, 19.0, 280, is_atm=True),
    # 同じ 5 分の中で ATM の行使価格が変わる（平均する）
    opt(m(4, 5), 2, 25125, M1, 19.5, 250, is_atm=True),
    # iv が NULL の行（AVG は NULL を除く）、5 分の中が全部 NULL の iv
    opt(m(7), 2, 25000, M1, None, 270, is_atm=True),
    opt(m(12), 2, 25000, M1, None, None, is_atm=True),
    # 取引の無い時間をはさんで前日・6 日前
    opt(m(30, 22, 12), 2, 24875, M1, 20.25, 310, is_atm=True),
    opt(m(10, 8, 7), 2, 24500, M1, 21.0, 500, is_atm=True),
    # ATM でない行・満了した限月（M0）・2 限月（M2）は使わない
    opt(m(1), 2, 25500, M1, 30.0, 999, is_atm=False),
    opt(m(1), 2, 25000, M0, 40.0, 999, is_atm=True),
    opt(m(1), 2, 25000, M2, 17.0, 600, is_atm=True),
    # 7 日より前は使わない
    opt(m(0, 5, 6), 2, 25000, M1, 50.0, 999, is_atm=True),
]
ATM_TYPES = {"atm": {"target_price": float}}


def legacy_atm_csv(options, now):
    rows = typed(bq(with_fixtures(legacy_sql.atm_query(now), fixtures(options))), {"target_price": float})
    if not rows:
        raise IndexError("現行は df['time'].iloc[-1] で IndexError（500）")
    return csvfmt.atm_csv(rows)


def test_atm_same_as_legacy():
    new = api.atm_data(fixture_runner(fixtures(ATM_OPTIONS), ATM_TYPES), NOW)
    assert new == legacy_atm_csv(ATM_OPTIONS, NOW)
    lines = new.split("\n")
    assert len(lines) == 1 + 5 + 1
    t = int(datetime(2026, 10, 13, 5, 0, tzinfo=UTC).timestamp())
    assert lines[0] == str(t + 600)
    # 05:00〜05:05: (25000 + 25000 + 25125) / 3、iv (18 + 19 + 19.5) / 3 = 18.83 → 18.8
    assert lines[-4] == f"{t},25041.7,18.8,276.7"
    # iv が NULL の行だけの 5 分、全部 NULL の 5 分は空のセル
    assert lines[-3] == f"{t + 300},25000.0,,270.0"
    assert lines[-2] == f"{t + 600},25000.0,,"
    assert lines[-1] == ""


def test_diff_atm_period_start_is_closed():
    # 違い: 現行は created_at > now − 7 日（開区間）、新は [now − 7 日, now)。ちょうど 7 日前の行だけが差になる
    edge = [opt(utc(6, 6, 0), 2, 25000, M1, 20.0, 300, is_atm=True), opt(m(1), 2, 25000, M1, 19.0, 280, is_atm=True)]
    new = api.atm_data(fixture_runner(fixtures(edge), ATM_TYPES), NOW).split("\n")
    old = legacy_atm_csv(edge, NOW).split("\n")
    assert new[0] == old[0] and len(new) == len(old) + 1
    assert new[1] == f"{int(datetime(2026, 10, 6, 6, 0, tzinfo=UTC).timestamp())},25000.0,20.0,300.0"
    assert new[2:] == old[1:]


def test_diff_atm_month_only_in_old_data():
    # 違い: 現行は限月を全期間の option_price から決める。期間内に行の無い限月（今日以降で、7 日より前にしか行が無い）が
    # 最小だと現行は 0 行（IndexError で 500）、新は期間内の最小の限月（M1）を表示する。
    # 実データでは起きない（今日以降の限月は毎分の行がある）
    old_only = [opt(utc(1, 5, 0), 2, 25000, date(2026, 10, 16), 20.0, 300, is_atm=True),
                opt(m(1), 2, 25000, M1, 19.0, 280, is_atm=True)]
    with pytest.raises(IndexError):
        legacy_atm_csv(old_only, NOW)
    assert api.atm_data(fixture_runner(fixtures(old_only), ATM_TYPES), NOW).split("\n")[1].endswith(",19.0,280.0")


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


# ---- 実テーブルの dry run（AC 6） ----

def real_sql(name):
    text = (SQL_DIR / f"{name}.sql").read_text(encoding="utf-8")
    return text.replace("${project}", PROJECT).replace("${dataset}", DATASET)


def dry_run_params(now):
    return {
        "latest_snapshot": {"start_at": now - timedelta(days=14), "end_at": now},
        "smile": {"start_at": now - timedelta(minutes=1), "end_at": now - timedelta(minutes=1) + timedelta(seconds=1)},
        "atm": {"start_at": now - timedelta(days=7), "end_at": now, "today": api.jst_date(now)},
        "atm_iv_meta": {"start_at": now - timedelta(days=14), "end_at": now},
        "atm_iv": api.atm_iv_params(now, None, 100, 0),
    }


@pytest.mark.parametrize("name", ["latest_snapshot", "smile", "atm", "atm_iv_meta", "atm_iv"])
def test_dry_run_real_tables(name, record_property):
    now = datetime.now(UTC).replace(microsecond=0)
    nbytes = bq(real_sql(name), dry_run_params(now)[name], dry_run=True, maximum_bytes=GiB)
    record_property("bytes", nbytes)
    print(f"dry run {name}: {nbytes} bytes ({nbytes / 1024**2:.1f} MiB)")
    assert nbytes < GiB

"""Flask のルート・パラメータの検証・期間・エラーの検査（#52 AC 2・3）。BigQuery は偽の runner に差し替える。"""

import json
import re
from datetime import UTC, date, datetime, timedelta

import pytest
from conftest import NOW, FakeRunner

from optionchan_viewer import api
from optionchan_viewer.app import create_app

SNAP = datetime(2026, 10, 13, 5, 59, tzinfo=UTC)


def smile_rows():
    base = {c: None for c in ("o1_call_iv", "o1_call_price_time", "o1_put_iv", "o1_put_price_time", "o2_call_iv",
                              "o2_call_price_time", "o2_put_iv", "o2_put_price_time")}
    return [
        {**base, "target_price": 24875, "o1_put_iv": 19.5, "o1_put_is_atm": False},
        {**base, "target_price": 25000, "o1_put_iv": 18.5, "o1_put_price_time": 1760335100, "o1_put_is_atm": True},
        {**base, "target_price": 25125, "o1_put_iv": 18.0, "o1_put_is_atm": None},
    ]


RUN_ID = "00000000-0000-0000-0000-000000000001"


def data_gaps_rows():
    """sql/data_gaps.sql の行（#123）。時刻は合成（UNIX 秒）。prev_at の順ではなく、(no_change, none) を含む"""
    def g(prev_at, next_at, cause, status):
        return {"prev_at": prev_at, "next_at": next_at, "cause": cause, "status": status}

    return [
        g(1700050000, 1700060000, "bug_comma", "partial"),
        g(1700000000, 1700003600, "no_change", "none"),
        g(1700040000, 1700045000, "no_change", "filled"),
        g(1700010000, 1700020000, "unknown", "none"),
        g(1700030000, 1700035000, "bug_format", "filled"),
    ]


def snapshot_rows():
    """sql/smile_snapshot.sql の行（#79）。1 限月の ATM は put の 25000、2 限月は ATM なし"""
    def r(month, typ, strike, **kw):
        base = {"month": month, "type": typ, "target_price": strike,
                "last_trading_day": date(2026, 11, 12) if month == 1 else date(2026, 12, 10), "is_atm": False,
                "iv": None, "price": None, "price_time": None, "bid_iv": None, "ask_iv": None, "bid": None, "ask": None,
                # 実行 ID（#123）。既定は新系の形の合成値
                "ingest_run_id": RUN_ID}
        return base | kw

    return [
        r(1, 2, 24875, bid_iv=19.0, ask_iv=20.0, bid=140, ask=150),
        r(1, 1, 25000, iv=18.25, price=300, price_time=1760335100, bid_iv=18.0, ask_iv=0.0, bid=295, ask=305),
        r(1, 2, 25000, iv=18.5, price=260, price_time=1760335140, bid_iv=18.25, ask_iv=18.75, bid=255, ask=265,
          is_atm=True),
        r(2, 1, 25000, iv=17.0, price=600, bid_iv=16.5, ask_iv=17.5),
    ]


def ok_results():
    return {
        "latest_snapshot": [{"snapshot_at": SNAP}],
        "smile": smile_rows(),
        "smile_snapshot": snapshot_rows(),
        "data_gaps": data_gaps_rows(),
        "atm_iv_meta": [{"last_started_at": datetime(2026, 10, 12, 21, tzinfo=UTC)}],
        "atm_iv": [{"open": 18.0, "high": 19.0, "low": 17.5, "close": 18.5,
                    "started_at": datetime(2026, 10, 12, 21, tzinfo=UTC)}],
    }


@pytest.fixture
def make():
    def _make(results=None):
        runner = FakeRunner(ok_results() if results is None else results)
        app = create_app(runner=runner, clock=lambda: NOW)
        return app.test_client(), runner

    return _make


@pytest.mark.parametrize("path", ["/", "/smile", "/atm_iv"])
def test_pages(make, path):
    client, runner = make()
    res = client.get(path)
    assert res.status_code == 200
    assert res.content_type.startswith("text/html")
    assert runner.calls == []


def test_smile_data(make):
    client, runner = make()
    res = client.get("/smile_data")
    assert res.status_code == 200
    assert res.headers["Content-Type"] == "text/csv; charset=utf-8"
    lines = res.get_data(as_text=True).split("\n")
    # メタ行: スナップショットの UNIX 秒, 1 限月 put の ATM の行使価格
    assert lines[0] == f"{int(SNAP.timestamp())},25000"
    assert lines[1:4] == ["24875,,,19.5,,,,,", "25000,,,18.5,1760335100,,,,", "25125,,,18.0,,,,,"]
    assert [name for name, _ in runner.calls] == ["latest_snapshot", "smile"]
    assert runner.calls[0][1] == {"start_at": NOW - timedelta(days=14), "end_at": NOW}
    assert runner.calls[1][1] == {"start_at": SNAP, "end_at": SNAP + timedelta(seconds=1)}


@pytest.mark.parametrize("path", ["/atm", "/atm_data", "/static/atm.js"])
def test_removed_atm_is_404(make, path):
    # 旧型の ATM 推移のページと API は削除した（#58）。BigQuery を呼ばない
    client, runner = make()
    assert client.get(path).status_code == 404
    assert runner.calls == []


def test_atm_iv_data_defaults(make):
    client, runner = make()
    res = client.get("/atm_iv_data")
    assert res.status_code == 200
    assert res.headers["Content-Type"] == "text/csv; charset=utf-8"
    assert res.get_data(as_text=True) == "18.0,19.0,17.5,18.5,2026-10-12 21:00:00+00:00\n"
    assert [name for name, _ in runner.calls] == ["atm_iv_meta", "atm_iv"]
    assert runner.calls[0][1] == {"start_at": NOW - timedelta(days=14), "end_at": NOW}
    params = runner.calls[1][1]
    # 既定 d = 7、n = 0（現行と同じ）
    assert params["days_start"] == datetime(2026, 10, 6, tzinfo=UTC)
    assert params["n"] == 0
    assert params["fill_start"] == datetime(2026, 10, 12, 22, tzinfo=UTC)


def test_atm_iv_data_params(make):
    client, runner = make()
    assert client.get("/atm_iv_data?d=100&n=1").status_code == 200
    params = runner.calls[1][1]
    assert params["days_start"] == datetime(2026, 7, 5, tzinfo=UTC)
    assert params["n"] == 1
    assert all(type(v) in (datetime, date, int) for v in params.values())


@pytest.mark.parametrize("query", [
    "d=0", "d=101", "d=abc", "d=1;DROP", "d=-1", "d=", "d=1.5", "d=%201", "d=0007", "d=1e2",
    "n=2", "n=-1", "n=", "n=01", "n=a", "d=7&n=1%20OR%201=1",
])
def test_atm_iv_data_bad_request(make, query, capsys):
    client, runner = make()
    res = client.get(f"/atm_iv_data?{query}")
    assert res.status_code == 400
    # BigQuery を呼ばない。入力値を応答に反射しない
    assert runner.calls == []
    assert res.get_data(as_text=True) == "bad request\n"
    record = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert record["severity"] == "INFO" and record["event"] == "bad_request"


@pytest.mark.parametrize("d", ["1", "7", "07", "100"])
def test_parse_d_accepts(d):
    assert api.parse_d(d) == int(d)


def test_no_snapshot_is_503(make, capsys):
    results = ok_results()
    results["latest_snapshot"] = [{"snapshot_at": None}]
    client, runner = make(results)
    res = client.get("/smile_data")
    assert res.status_code == 503
    assert [name for name, _ in runner.calls] == ["latest_snapshot"]
    record = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert record["severity"] == "WARNING" and record["event"] == "no_snapshot"


def test_no_atm_in_snapshot_is_503(make):
    results = ok_results()
    results["smile"] = [r | {"o1_put_is_atm": False} for r in smile_rows()]
    client, _ = make(results)
    assert client.get("/smile_data").status_code == 503


def test_atm_iv_no_rows_is_empty_200(make):
    # 限月が範囲外（n=1 で 2 限月目が無い等）は 0 行（現行は OFFSET のエラーで 500）
    results = ok_results()
    results["atm_iv"] = []
    client, _ = make(results)
    res = client.get("/atm_iv_data?n=1")
    assert res.status_code == 200 and res.get_data(as_text=True) == ""


@pytest.mark.parametrize("path,name", [("/smile_data", "latest_snapshot"), ("/atm_iv_data", "atm_iv_meta")])
def test_query_error_is_500_without_details(make, capsys, path, name):
    results = ok_results()
    results[name] = RuntimeError("secret detail: project 123456789012 user@example.com")
    client, _ = make(results)
    res = client.get(path)
    assert res.status_code == 500
    body = res.get_data(as_text=True)
    assert "secret" not in body and "123456789012" not in body
    out = capsys.readouterr().out
    record = json.loads(out.strip().splitlines()[-1])
    assert record["severity"] == "ERROR" and record["event"] == "query_failed"
    assert record["error_type"] == "RuntimeError"
    assert "secret" not in out


def test_atm_iv_params_fill_start():
    now = NOW
    # 最後の足の次の 1 時間から
    p = api.atm_iv_params(now, datetime(2026, 10, 12, 21, tzinfo=UTC), 7, 0)
    assert p["fill_start"] == datetime(2026, 10, 12, 22, tzinfo=UTC)
    # 足が無い・古いときは最大 3 日前の正時から
    floor = datetime(2026, 10, 10, 6, tzinfo=UTC)
    assert api.atm_iv_params(now, None, 7, 0)["fill_start"] == floor
    assert api.atm_iv_params(now, datetime(2026, 10, 1, tzinfo=UTC), 7, 0)["fill_start"] == floor
    # 限月を決める範囲は 10 日前の UTC の 0 時から（現行の t1 と同じ）。1 時間足を読む下端は d と 10 日の早い方
    assert p["months_since"] == datetime(2026, 10, 3, tzinfo=UTC)
    assert p["hourly_start"] == datetime(2026, 10, 3, tzinfo=UTC)
    assert api.atm_iv_params(now, None, 20, 0)["hourly_start"] == datetime(2026, 9, 23, tzinfo=UTC)
    assert p["fill_end"] == now and p["range_end"] == now and p["today"] == date(2026, 10, 13)


def test_today_is_jst():
    # UTC 2026-10-13 16:00 = JST 10-14 01:00
    assert api.jst_date(datetime(2026, 10, 13, 16, tzinfo=UTC)) == date(2026, 10, 14)


@pytest.mark.parametrize("path", ["/", "/smile", "/atm_iv", "/smile_data", "/atm_iv_data?d=0", "/static/smile.js",
                                  "/static/smile_age.js", "/static/smile_touch.js", "/static/viewer.css",
                                  "/nonexistent", "/smile_snapshot", "/smile_snapshot?at=abc", "/data_gaps",
                                  "/static/data_gaps.js", "/static/smile_csv.js"])
def test_security_headers(make, path):
    # N2: すべての応答（ページ・CSV・400・静的ファイル・404）に付ける
    client, _ = make()
    res = client.get(path)
    csp = res.headers["Content-Security-Policy"]
    directives = dict(d.strip().split(" ", 1) for d in csp.split(";"))
    assert directives["default-src"] == "'none'"
    for name in ("script-src", "style-src", "img-src", "connect-src"):
        # 同じオリジンの同梱ファイルだけ（inline・eval・外部は不可）
        assert directives[name] == "'self'", name
    assert directives["frame-ancestors"] == "'none'"
    assert res.headers["X-Content-Type-Options"] == "nosniff"
    assert res.headers["Referrer-Policy"] == "no-referrer"
    assert res.headers["X-Frame-Options"] == "DENY"


def test_d_with_trailing_newline_is_bad_request(make):
    # N7: 末尾の改行も受け付けない
    client, runner = make()
    assert client.get("/atm_iv_data?d=7%0A").status_code == 400
    assert runner.calls == []


# ---- /smile_snapshot（#79） ----

SEC = timedelta(seconds=1)


def test_smile_snapshot_latest(make):
    client, runner = make()
    res = client.get("/smile_snapshot")
    assert res.status_code == 200
    assert res.headers["Content-Type"] == "text/csv; charset=utf-8"
    assert res.headers["Cache-Control"] == "no-cache"
    lines = res.get_data(as_text=True).split("\n")
    # メタ行: スナップショットの UNIX 秒, 1 限月の最終取引日, ATM, 2 限月の最終取引日, ATM（2 限月は ATM なし）,
    # source（#123。新系の実行は空）
    assert lines[0] == f"{int(SNAP.timestamp())},2026-11-12,25000,2026-12-10,,"
    # month,type,target_price,iv,price,price_time,bid_iv,ask_iv,bid,ask,mid_iv,otm（ask_iv の 0 は空・mid なし）
    assert lines[1:-1] == [
        "1,2,24875,,,,19.0,20.0,140,150,19.5,1",
        "1,1,25000,18.25,300,1760335100,18.0,,295,305,,1",
        "1,2,25000,18.5,260,1760335140,18.25,18.75,255,265,18.5,0",
        "2,1,25000,17.0,600,,16.5,17.5,,,17.0,0",
    ]
    assert lines[-1] == ""
    # at なし = now。探す範囲は [now − 14 日, now]（latest_runs は [start, end) なので end は +1 秒）
    assert runner.calls == [
        ("latest_snapshot", {"start_at": NOW - timedelta(days=14), "end_at": NOW + SEC}),
        ("smile_snapshot", {"start_at": SNAP, "end_at": SNAP + SEC}),
    ]


def test_smile_snapshot_at(make):
    client, runner = make()
    at = datetime(2026, 10, 9, 21, 0, tzinfo=UTC)
    assert client.get(f"/smile_snapshot?at={int(at.timestamp())}").status_code == 200
    # at ちょうどのスナップショットを含む: [at − 14 日, at + 1 秒)
    assert runner.calls[0] == ("latest_snapshot", {"start_at": at - timedelta(days=14), "end_at": at + SEC})
    assert runner.calls[1] == ("smile_snapshot", {"start_at": SNAP, "end_at": SNAP + SEC})
    assert all(type(v) is datetime for _, p in runner.calls for v in p.values())


@pytest.mark.parametrize("ahead", [timedelta(seconds=1), timedelta(days=1), timedelta(days=3650)])
def test_smile_snapshot_future_at_is_now(make, ahead):
    # 未来の at は 400 にせず now に縮める（探す範囲の終わりが now）
    client, runner = make()
    res = client.get(f"/smile_snapshot?at={int((NOW + ahead).timestamp())}")
    assert res.status_code == 200
    assert runner.calls[0][1] == {"start_at": NOW - timedelta(days=14), "end_at": NOW + SEC}


@pytest.mark.parametrize("at", ["1262304000", "01262304000", "1760000000"])
def test_parse_at_accepts(at):
    assert api.parse_at(at, NOW) == datetime.fromtimestamp(int(at), UTC)


def test_parse_at_default_is_now():
    assert api.parse_at(None, NOW) == NOW


@pytest.mark.parametrize("query", [
    "at=-1", "at=1.5", "at=abc", "at=", "at=123456789012", "at=1760000000%0A", "at=%201760000000", "at=1e9",
    "at=1262303999", "at=0", "at=0x10", "at=1760000000;DROP",
])
def test_smile_snapshot_bad_request(make, query, capsys):
    client, runner = make()
    res = client.get(f"/smile_snapshot?{query}")
    assert res.status_code == 400
    # BigQuery を呼ばない。入力値を応答・ログに出さない
    assert runner.calls == []
    assert res.get_data(as_text=True) == "bad request\n"
    out = capsys.readouterr().out
    record = json.loads(out.strip().splitlines()[-1])
    assert record["event"] == "bad_request" and record["endpoint"] == "smile_snapshot" and record["param"] == "at"
    value = query.split("=", 1)[1]
    if value:
        assert value not in out


@pytest.mark.parametrize("name", ["latest_snapshot", "smile_snapshot"])
def test_smile_snapshot_query_error_is_500(make, capsys, name):
    results = ok_results()
    results[name] = RuntimeError("secret detail: project 123456789012 user@example.com")
    client, _ = make(results)
    res = client.get("/smile_snapshot")
    assert res.status_code == 500
    assert res.get_data(as_text=True) == "internal error\n"
    out = capsys.readouterr().out
    record = json.loads(out.strip().splitlines()[-1])
    assert record["event"] == "query_failed" and record["endpoint"] == "smile_snapshot"
    assert record["error_type"] == "RuntimeError"
    assert "secret" not in out and "123456789012" not in out


def test_smile_snapshot_no_snapshot_is_empty_200(make):
    # データなしは 5xx でない: 200 で本文が空（画面は「データがありません」）
    results = ok_results()
    results["latest_snapshot"] = [{"snapshot_at": None}]
    client, runner = make(results)
    res = client.get("/smile_snapshot?at=1760000000")
    assert res.status_code == 200 and res.get_data(as_text=True) == ""
    assert [name for name, _ in runner.calls] == ["latest_snapshot"]


def test_smile_snapshot_one_month_only(make):
    # 限月が 1 つだけのスナップショット: SQL が 1 限月・2 限月に同じ行を返す（smile.sql と同じ割り当て）
    results = ok_results()
    one = [r for r in snapshot_rows() if r["month"] == 1]
    results["smile_snapshot"] = one + [r | {"month": 2} for r in one]
    client, _ = make(results)
    lines = client.get("/smile_snapshot").get_data(as_text=True).split("\n")
    assert lines[0] == f"{int(SNAP.timestamp())},2026-11-12,25000,2026-11-12,25000,"
    body = lines[1:-1]
    assert [line[2:] for line in body if line.startswith("1,")] == [line[2:] for line in body if line.startswith("2,")]


@pytest.mark.parametrize("run_id,want", [
    ("legacy-fill114-20000101T000000Z", "fill114"),
    # 旧系の取り込みの実行・新系の実行は空
    ("legacy-20000101T000000Z", ""),
    (RUN_ID, ""),
    (None, ""),
])
def test_smile_snapshot_source(make, run_id, want):
    # #123: 1 行目の 6 列目 = source（#114 で作ったスナップショットなら fill114）。実行 ID は合成
    results = ok_results()
    results["smile_snapshot"] = [r | {"ingest_run_id": run_id} for r in snapshot_rows()]
    client, _ = make(results)
    lines = client.get("/smile_snapshot").get_data(as_text=True).split("\n")
    head = lines[0].split(",")
    assert len(head) == 6 and head[5] == want
    # 2 行目以降は実行 ID で変わらない
    base = make()[0].get("/smile_snapshot").get_data(as_text=True).split("\n")
    assert lines[1:] == base[1:]


# ---- /data_gaps（#123） ----

def test_data_gaps(make):
    client, runner = make()
    res = client.get("/data_gaps")
    assert res.status_code == 200
    assert res.headers["Content-Type"] == "text/csv; charset=utf-8"
    assert res.headers["Cache-Control"] == "no-cache"
    # (no_change, none) だけを除き、prev_at 昇順。ヘッダなし、prev_at,next_at,cause,status
    assert res.get_data(as_text=True) == (
        "1700010000,1700020000,unknown,none\n"
        "1700030000,1700035000,bug_format,filled\n"
        "1700040000,1700045000,no_change,filled\n"
        "1700050000,1700060000,bug_comma,partial\n"
    )
    # パラメータなしの 1 本だけ
    assert runner.calls == [("data_gaps", {})]


def test_data_gaps_ignores_query_string(make):
    # クエリ文字列は読まない（同じ応答・同じクエリ）
    client, runner = make()
    res = client.get("/data_gaps?start=1&x=%27")
    assert res.status_code == 200
    assert runner.calls == [("data_gaps", {})]


@pytest.mark.parametrize("rows", [[], [{"prev_at": 1700000000, "next_at": 1700003600, "cause": "no_change",
                                         "status": "none"}]])
def test_data_gaps_no_rows_is_empty_200(make, rows):
    # 表が空（作っていない利用者）・対象の区間が無い → 空の 200（画面は印を出さないだけ）
    results = ok_results()
    results["data_gaps"] = rows
    client, _ = make(results)
    res = client.get("/data_gaps")
    assert res.status_code == 200 and res.get_data(as_text=True) == ""


def test_data_gaps_query_error_is_500(make, capsys):
    results = ok_results()
    results["data_gaps"] = RuntimeError("secret detail: project 123456789012 user@example.com")
    client, _ = make(results)
    res = client.get("/data_gaps")
    assert res.status_code == 500
    assert res.get_data(as_text=True) == "internal error\n"
    out = capsys.readouterr().out
    record = json.loads(out.strip().splitlines()[-1])
    assert record["event"] == "query_failed" and record["endpoint"] == "data_gaps"
    assert record["error_type"] == "RuntimeError"
    assert "secret" not in out and "123456789012" not in out


def test_data_gaps_static_files_are_served(make):
    client, runner = make()
    for path in ("/static/data_gaps.js", "/static/smile_csv.js"):
        assert client.get(path).status_code == 200, path
    assert runner.calls == []


# ---- /atm_iv_bars（#68） ----

def minute(hour, m, o, h, low, c):
    return {"open": o, "high": h, "low": low, "close": c, "started_at": datetime(2026, 10, 13, hour, m, tzinfo=UTC)}


def hour(day, hr, o, h, low, c):
    return {"open": o, "high": h, "low": low, "close": c, "started_at": datetime(2026, 10, day, hr, tzinfo=UTC)}


def bars_results():
    return {
        **ok_results(),
        # 1 分足（値の無い分を含む）。06:14 UTC = 15:14 JST
        "atm_iv_minute": [
            minute(5, 50, 18.0, 18.5, 17.9, 18.2),
            minute(5, 51, None, None, None, None),
            minute(5, 54, 18.2, 18.9, 18.1, 18.8),
            minute(5, 55, 18.8, 18.8, 18.3, 18.4),
        ],
        "atm_iv": [
            # 金曜（10-09）の日中・夜間、火曜（10-13）の日中。10-12（月）は休業日
            hour(9, 0, 20.0, 21.0, 19.5, 20.5),
            hour(9, 8, 20.5, 22.0, 20.0, 21.0),
            hour(9, 9, None, None, None, None),
            hour(12, 0, 21.0, 21.5, 18.0, 18.5),
            hour(13, 0, 18.5, 19.0, 18.0, 18.8),
        ],
        "trading_holidays": [{"calendar_date": date(2026, 10, 12)}],
    }


def csv_times(text):
    return [line.split(",")[4] for line in text.splitlines()]


def ep(t):
    """aware な datetime → UNIX 秒の文字列（/atm_iv_bars の start・end）"""
    return str(int(t.timestamp()))


def utc(day, hr=0, month=10, year=2026):
    return datetime(year, month, day, hr, tzinfo=UTC)


@pytest.mark.parametrize("tf,names,count,start", [
    # 既定の範囲: 分足は最新のスナップショット（10-13 05:59）の 1 日前の UTC の 0 時、
    # 1h・1d は now の 10 日・100 日前の UTC の 0 時から
    ("1m", ["latest_snapshot", "atm_iv_minute"], 3, utc(12)),
    ("5m", ["latest_snapshot", "atm_iv_minute"], 2, utc(12)),
    ("15m", ["latest_snapshot", "atm_iv_minute"], 1, utc(12)),
    ("1h", ["atm_iv_meta", "atm_iv"], 4, utc(3)),
    ("1d", ["atm_iv_meta", "atm_iv", "trading_holidays"], 2, utc(5, month=7)),
])
def test_atm_iv_bars_each_tf(make, tf, names, count, start):
    client, runner = make(bars_results())
    res = client.get(f"/atm_iv_bars?tf={tf}")
    assert res.status_code == 200
    assert res.headers["Content-Type"] == "text/csv; charset=utf-8"
    assert res.headers["Cache-Control"] == "no-cache"
    # 読んだ範囲（#77。画面はこれで取得済みの範囲を覚える）
    assert (res.headers["X-Bars-Start"], res.headers["X-Bars-End"]) == (ep(start), ep(NOW))
    text = res.get_data(as_text=True)
    times = csv_times(text)
    assert len(times) == count
    # 昇順・重複なし、値の無い足は出さない
    assert times == sorted(set(times))
    assert all(line.split(",")[1] != "" for line in text.splitlines())
    assert [name for name, _ in runner.calls] == names


def test_atm_iv_bars_minute_range(make):
    client, runner = make(bars_results())
    res = client.get(f"/atm_iv_bars?tf=5m&n=1&start={ep(utc(10))}&end={ep(utc(13))}")
    assert res.get_data(as_text=True) == ("18.0,18.9,17.9,18.8,2026-10-13 05:50:00+00:00\n"
                                          "18.8,18.8,18.3,18.4,2026-10-13 05:55:00+00:00\n")
    # 範囲を指定したら最新のスナップショットは探さない。[start, end) をそのまま読む
    assert runner.calls == [("atm_iv_minute", {"start_at": utc(10), "end_at": utc(13), "today": date(2026, 10, 13),
                                               "n": 1})]
    assert (res.headers["X-Bars-Start"], res.headers["X-Bars-End"]) == (ep(utc(10)), ep(utc(13)))


def test_atm_iv_bars_end_is_clamped_to_now(make):
    # end が now より後なら（どれだけ先でも）now に縮め、ヘッダで返す（画面は最新まで読んだと分かる）
    client, runner = make(bars_results())
    res = client.get(f"/atm_iv_bars?tf=1m&start={ep(utc(12))}&end={ep(utc(14))}")
    assert res.status_code == 200
    assert runner.calls[0][1]["end_at"] == NOW
    assert res.headers["X-Bars-End"] == ep(NOW)


def test_atm_iv_bars_minute_defaults(make):
    client, runner = make(bars_results())
    assert client.get("/atm_iv_bars?tf=1m").get_data(as_text=True).splitlines()[0] == (
        "18.0,18.5,17.9,18.2,2026-10-13 05:50:00+00:00")
    assert runner.calls[1][1] == {"start_at": utc(12), "end_at": NOW, "today": date(2026, 10, 13), "n": 0}


def test_atm_iv_bars_hourly_default_same_params_as_atm_iv_data(make):
    # 1 時間足の既定（10 日〜now）は /atm_iv_data?d=10 と同じクエリ・パラメータ（#68 AC 10）
    client, runner = make(bars_results())
    assert client.get("/atm_iv_bars?n=1").status_code == 200
    bars_calls = list(runner.calls)
    runner.calls.clear()
    assert client.get("/atm_iv_data?d=10&n=1").status_code == 200
    assert bars_calls == runner.calls


def test_atm_iv_bars_hourly_recent_range_reads_fill(make):
    client, runner = make(bars_results())
    assert client.get(f"/atm_iv_bars?tf=1h&start={ep(utc(12))}&end={ep(NOW)}").status_code == 200
    params = runner.calls[1][1]
    # 最後の 1 時間足（10-12 21:00）の次からを生データで補う
    assert params["fill_start"] == utc(12, 22) and params["fill_end"] == NOW
    assert params["days_start"] == utc(12) and params["range_end"] == NOW
    assert params["hourly_start"] == utc(3)  # 限月を決める直近 10 日を含む


def test_atm_iv_bars_hourly_past_range_skips_fill(make):
    # 過去だけの範囲（end <= 補う分の始まり）は option_price を読まない:
    # fill_end を fill_start より前にする（逆向きの区間）
    client, runner = make(bars_results())
    res = client.get(f"/atm_iv_bars?tf=1h&start={ep(utc(1, month=9))}&end={ep(utc(1))}")
    assert res.status_code == 200
    params = runner.calls[1][1]
    assert params["fill_end"] < params["fill_start"] == utc(12, 22)
    assert params["days_start"] == utc(1, month=9) and params["range_end"] == utc(1)
    assert params["hourly_start"] == utc(1, month=9)
    assert params["months_since"] == utc(3)
    assert (res.headers["X-Bars-Start"], res.headers["X-Bars-End"]) == (ep(utc(1, month=9)), ep(utc(1)))


def test_atm_iv_bars_default_tf_is_1h(make):
    client, runner = make(bars_results())
    assert client.get("/atm_iv_bars").status_code == 200
    assert [name for name, _ in runner.calls] == ["atm_iv_meta", "atm_iv"]
    # 1h の既定は 10 日
    assert runner.calls[1][1]["days_start"] == utc(3)


def test_atm_iv_bars_daily(make):
    client, runner = make(bars_results())
    res = client.get(f"/atm_iv_bars?tf=1d&start={ep(utc(8))}&end={ep(NOW)}")
    # 10-09 00:00 UTC（金曜 09:00 JST）の日中 → 10-09、金曜の夜間・休業日（月）の足 → 休み明けの火曜
    assert res.get_data(as_text=True) == ("20.0,21.0,19.5,20.5,2026-10-08 15:00:00+00:00\n"
                                          "20.5,22.0,18.0,18.8,2026-10-12 15:00:00+00:00\n")
    # 1 時間足は start の 10 日前から読む（取引日の前の夜間・連休を含める）
    params = runner.calls[1][1]
    assert params["days_start"] == utc(28, month=9) and params["range_end"] == NOW
    assert runner.calls[2] == ("trading_holidays", {"start_date": date(2026, 9, 27), "end_date": date(2026, 10, 28)})


@pytest.mark.parametrize("start,end,want", [
    # 取引日の 00:00 JST（started_at）が [start, end) の日足だけ
    (utc(8, 15), NOW, ["2026-10-08 15:00:00+00:00", "2026-10-12 15:00:00+00:00"]),
    (utc(8, 16), NOW, ["2026-10-12 15:00:00+00:00"]),
    (utc(1), utc(12, 15), ["2026-10-08 15:00:00+00:00"]),
    (utc(1), utc(12, 16), ["2026-10-08 15:00:00+00:00", "2026-10-12 15:00:00+00:00"]),
])
def test_atm_iv_bars_daily_range(make, start, end, want):
    client, runner = make(bars_results())
    res = client.get(f"/atm_iv_bars?tf=1d&start={ep(start)}&end={ep(end)}")
    assert csv_times(res.get_data(as_text=True)) == want
    # 1 時間足は end の 1 日後（now まで）まで読む（取引日の日中は 00:00 JST の後）
    assert runner.calls[1][1]["range_end"] == min(end + timedelta(days=1), NOW)


def test_atm_iv_bars_daily_today_night_is_included(make):
    # end = now のときは上端なし: 今夜（10-13 17:00 JST 以降）の夜間は明日の取引日（00:00 JST が now より後）
    results = bars_results()
    results["atm_iv"] = [*results["atm_iv"], hour(13, 9, 30.0, 30.0, 30.0, 30.0)]  # 10-13 18:00 JST
    late = datetime(2026, 10, 13, 10, tzinfo=UTC)
    app = create_app(runner=FakeRunner(results), clock=lambda: late)
    times = csv_times(app.test_client().get(f"/atm_iv_bars?tf=1d&start={ep(utc(8))}&end={ep(late)}")
                      .get_data(as_text=True))
    assert times[-1] == "2026-10-13 15:00:00+00:00"


@pytest.mark.parametrize("query", [
    "tf=2m", "tf=", "tf=1H", "tf=1m%0A", "tf=%201m", "tf=1d;DROP",
    "tf=1m&n=2", "n=-1", "tf=1d&n=", "n=01",
    # start・end は両方か、両方なし
    f"start={ep(utc(12))}", f"end={ep(utc(12))}", f"start={ep(utc(12))}&end=",
    # 形（UNIX 秒の 10 進の整数だけ）
    "start=abc&end=1760000000", f"start=-1&end={ep(utc(12))}", f"start=1759000000.5&end={ep(utc(12))}",
    f"start=%201759000000&end={ep(utc(12))}", f"start={ep(utc(11))}%0A&end={ep(utc(12))}",
    f"start={ep(utc(11))}&end=123456789012", f"start=0x10&end={ep(utc(12))}",
    # start < end
    f"start={ep(utc(12))}&end={ep(utc(12))}", f"start={ep(utc(12))}&end={ep(utc(11))}",
    # 未来の start（end を now に縮めた後に start >= end）
    f"start={ep(NOW + timedelta(minutes=1))}&end={ep(NOW + timedelta(minutes=2))}",
    f"start={ep(NOW)}&end={ep(NOW + timedelta(days=3))}",
    # 1 回の最大: 分足 3 日、1h 100 日、1d 400 日
    f"tf=1m&start={ep(utc(9))}&end={ep(utc(12, 1))}", f"tf=15m&start={ep(utc(1))}&end={ep(utc(5))}",
    f"tf=1h&start={ep(utc(1, month=6))}&end={ep(utc(10, month=9))}",
    f"tf=1d&start={ep(utc(1, month=1, year=2025))}&end={ep(utc(10, month=2, year=2026))}",
    # 遡れる上限: 分足は now の 31 日前の UTC の 0 時（09-12）まで。全部は 2010-01-01 から
    f"tf=5m&start={ep(utc(11, 23, month=9))}&end={ep(utc(13, month=9))}",
    f"tf=1d&start={ep(utc(31, month=12, year=2009))}&end={ep(utc(10, month=1, year=2010))}",
])
def test_atm_iv_bars_bad_request(make, query, capsys):
    client, runner = make(bars_results())
    res = client.get(f"/atm_iv_bars?{query}")
    assert res.status_code == 400
    assert runner.calls == []
    assert res.get_data(as_text=True) == "bad request\n"
    record = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert record["event"] == "bad_request" and record["endpoint"] == "atm_iv_bars"


@pytest.mark.parametrize("tf,start,end", [
    ("1m", utc(9), utc(12)),  # ちょうど 3 日
    ("5m", utc(12, month=9), utc(14, month=9)),  # 遡れる上限ちょうど
    ("15m", utc(12, 5), NOW + timedelta(days=1)),  # 未来の end は now に縮める
    ("1m", utc(12, 5), NOW + timedelta(days=30)),  # 画面の時計が進んでいても 400 にしない
    ("1h", utc(1, month=6), utc(9, month=9)),  # 100 日
    ("1d", utc(1, month=1, year=2010), utc(5, month=2, year=2011)),  # 400 日・BARS_EPOCH ちょうど
])
def test_atm_iv_bars_range_accepts(make, tf, start, end):
    client, _ = make(bars_results())
    assert client.get(f"/atm_iv_bars?tf={tf}&start={ep(start)}&end={ep(end)}").status_code == 200


def test_parse_range_default_is_none():
    assert api.parse_range(None, None, "1m", NOW) is None


@pytest.mark.parametrize("tf,name", [("1m", "latest_snapshot"), ("15m", "atm_iv_minute"), ("1h", "atm_iv"),
                                     ("1d", "trading_holidays")])
def test_atm_iv_bars_query_error_is_500(make, capsys, tf, name):
    results = bars_results()
    results[name] = RuntimeError("secret detail: project 123456789012 user@example.com")
    client, _ = make(results)
    res = client.get(f"/atm_iv_bars?tf={tf}")
    assert res.status_code == 500
    assert res.get_data(as_text=True) == "internal error\n"
    out = capsys.readouterr().out
    record = json.loads(out.strip().splitlines()[-1])
    assert record["event"] == "query_failed" and record["endpoint"] == "atm_iv_bars"
    assert "secret" not in out


@pytest.mark.parametrize("tf,name", [("1m", "atm_iv_minute"), ("5m", "atm_iv_minute"), ("1h", "atm_iv"),
                                     ("1d", "atm_iv")])
def test_atm_iv_bars_no_rows_is_empty_200(make, tf, name):
    results = bars_results()
    results[name] = []
    client, _ = make(results)
    res = client.get(f"/atm_iv_bars?tf={tf}&n=1")
    assert res.status_code == 200 and res.get_data(as_text=True) == ""


def test_atm_iv_bars_all_null_is_empty_200(make):
    results = bars_results()
    results["atm_iv_minute"] = [minute(5, 51, None, None, None, None)]
    client, _ = make(results)
    for tf in ("1m", "5m", "15m"):
        assert client.get(f"/atm_iv_bars?tf={tf}").get_data(as_text=True) == ""


@pytest.mark.parametrize("tf", ["1m", "5m", "15m"])
def test_atm_iv_bars_no_snapshot_is_empty_200(make, tf):
    results = bars_results()
    results["latest_snapshot"] = [{"snapshot_at": None}]
    client, runner = make(results)
    res = client.get(f"/atm_iv_bars?tf={tf}")
    assert res.status_code == 200 and res.get_data(as_text=True) == ""
    assert [name for name, _ in runner.calls] == ["latest_snapshot"]


def test_atm_iv_bars_security_headers(make):
    client, _ = make(bars_results())
    for path in ("/atm_iv_bars?tf=1m", "/atm_iv_bars?tf=bad"):
        assert client.get(path).headers["Content-Security-Policy"].startswith("default-src 'none'")


def test_atm_iv_bars_params_types(make):
    client, runner = make(bars_results())
    for tf in ("1m", "1d"):
        client.get(f"/atm_iv_bars?tf={tf}")
    # 値は TIMESTAMP / DATE / INT64 のパラメータだけ（足の秒数は SQL に渡さない）
    for _, params in runner.calls:
        assert all(type(v) in (datetime, date, int) for v in params.values())


# ---- /smile_snapshot?dir=next（#80） ----

def test_smile_snapshot_next(make):
    results = ok_results()
    nxt = datetime(2026, 10, 13, 8, 0, tzinfo=UTC)
    results["next_snapshot"] = [{"snapshot_at": nxt}]
    client, runner = make(results)
    at = datetime(2026, 10, 12, 6, 45, tzinfo=UTC)
    res = client.get(f"/smile_snapshot?at={int(at.timestamp())}&dir=next")
    assert res.status_code == 200
    assert res.get_data(as_text=True).split("\n")[0].split(",")[0] == str(int(nxt.timestamp()))
    # at 以後 14 日で最初（at ちょうどを含む）。見つかれば at 以前は探さない
    assert runner.calls == [
        ("next_snapshot", {"start_at": at, "end_at": at + timedelta(days=14)}),
        ("smile_snapshot", {"start_at": nxt, "end_at": nxt + SEC}),
    ]


def test_smile_snapshot_next_none_falls_back_to_latest(make):
    # at より後に無ければ at 以前で最新（画面は時刻が at より前なのを見て「これより新しいデータは無い」と分かる）
    results = ok_results()
    results["next_snapshot"] = [{"snapshot_at": None}]
    client, runner = make(results)
    at = NOW - timedelta(minutes=1)
    res = client.get(f"/smile_snapshot?at={int(at.timestamp())}&dir=next")
    assert res.status_code == 200
    assert res.get_data(as_text=True).split("\n")[0].split(",")[0] == str(int(SNAP.timestamp()))
    assert [name for name, _ in runner.calls] == ["next_snapshot", "latest_snapshot", "smile_snapshot"]
    assert runner.calls[1][1] == {"start_at": at - timedelta(days=14), "end_at": at + SEC}


@pytest.mark.parametrize("query", ["dir=prev", "dir=", "dir=NEXT", "dir=next%0A", "at=1760000000&dir=1"])
def test_smile_snapshot_bad_dir(make, query, capsys):
    client, runner = make()
    res = client.get(f"/smile_snapshot?{query}")
    assert res.status_code == 400
    assert runner.calls == []
    record = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert record["param"] == "dir"


# ---- /atm_iv_bars?iv=mid（#93） ----

def mid_results():
    results = bars_results()
    results["atm_mid_minute"] = [
        minute(5, 50, 18.1, 18.6, 18.0, 18.3),
        minute(5, 52, None, None, None, None),
        minute(5, 53, 18.3, 18.4, 18.2, 18.4),
    ]
    # 1 時間足・日足の mid（#98）: 約定と同じ形の 1 時間足（値は約定と違う）
    results["atm_mid"] = [
        {**r, **({} if r["high"] is None else {k: r[k] + 0.5 for k in ("open", "high", "low", "close")})}
        for r in results["atm_iv"]]
    return results


@pytest.mark.parametrize("tf,want", [
    ("1m", ["18.1,18.6,18.0,18.3,2026-10-13 05:50:00+00:00", "18.3,18.4,18.2,18.4,2026-10-13 05:53:00+00:00"]),
    ("5m", ["18.1,18.6,18.0,18.4,2026-10-13 05:50:00+00:00"]),
])
def test_atm_iv_bars_mid(make, tf, want):
    client, runner = make(mid_results())
    res = client.get(f"/atm_iv_bars?tf={tf}&n=1&iv=mid&start={ep(utc(12))}&end={ep(utc(13))}")
    assert res.status_code == 200
    assert res.get_data(as_text=True).splitlines() == want
    assert runner.calls == [("atm_mid_minute", {"start_at": utc(12), "end_at": utc(13), "today": date(2026, 10, 13),
                                                "n": 1})]


def test_atm_iv_bars_trade_is_default(make):
    client, runner = make(mid_results())
    assert client.get("/atm_iv_bars?tf=1m&iv=trade").get_data(as_text=True) == client.get(
        "/atm_iv_bars?tf=1m").get_data(as_text=True)
    assert [name for name, _ in runner.calls].count("atm_mid_minute") == 0


@pytest.mark.parametrize("tf", ["1h", "1d"])
def test_atm_iv_bars_mid_hourly(make, tf):
    # #98: 1 時間足・日足の mid は sql/atm_mid.sql（1 時間足 atm_mid_iv）。
    # 読み方・パラメータ・CSV の形は約定と同じで、値だけ違う
    client, runner = make(mid_results())
    query = f"tf={tf}&n=1&start={ep(utc(8))}&end={ep(NOW)}"
    mid = client.get(f"/atm_iv_bars?{query}&iv=mid")
    mid_calls = list(runner.calls)
    runner.calls.clear()
    trade = client.get(f"/atm_iv_bars?{query}")
    assert mid.status_code == 200 and trade.status_code == 200
    assert [name for name, _ in mid_calls] == [("atm_mid" if name == "atm_iv" else name) for name, _ in runner.calls]
    assert [p for _, p in mid_calls] == [p for _, p in runner.calls]
    mid_lines, trade_lines = mid.get_data(as_text=True).splitlines(), trade.get_data(as_text=True).splitlines()
    assert mid_lines and len(mid_lines) == len(trade_lines)
    for m, t in zip(mid_lines, trade_lines, strict=True):
        assert m.split(",")[4] == t.split(",")[4]
        assert float(m.split(",")[3]) == pytest.approx(float(t.split(",")[3]) + 0.5)
    assert (mid.headers["X-Bars-Start"], mid.headers["X-Bars-End"]) == (trade.headers["X-Bars-Start"],
                                                                         trade.headers["X-Bars-End"])


@pytest.mark.parametrize("query", ["tf=1m&iv=", "tf=1m&iv=MID", "tf=1m&iv=bid", "tf=1m&iv=mid%0A", "tf=1h&iv=Mid",
                                   "tf=1d&iv=mid%20"])
def test_atm_iv_bars_bad_iv(make, query, capsys):
    client, runner = make(mid_results())
    res = client.get(f"/atm_iv_bars?{query}")
    assert res.status_code == 400
    assert runner.calls == []
    record = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert record["param"] == "iv"


# ---- favicon（#94） ----

def test_favicon(make):
    client, runner = make()
    assert client.get("/favicon.ico").status_code == 204
    res = client.get("/static/favicon.svg")
    assert res.status_code == 200
    assert res.content_type.startswith("image/svg+xml")
    for path in ("/", "/smile", "/atm_iv"):
        assert '<link rel="icon" href="/static/favicon.svg"' in client.get(path).get_data(as_text=True)
    assert runner.calls == []



# ---- 配色（既定はダーク）・/atm_iv の IV の既定（mid） ----

def test_theme_script_in_head(make):
    # 描く前に配色を決めるので、どのページも <head> で CSS より前に defer なしで読む
    client, runner = make()
    for path in ("/", "/smile", "/atm_iv"):
        html = client.get(path).get_data(as_text=True)
        head = html.split("</head>", 1)[0]
        tag = '<script src="/static/theme.js"></script>'
        assert tag in head, path
        assert head.index(tag) < head.index('<link rel="stylesheet"'), path
        assert 'id="theme-toggle"' in html, path
    res = client.get("/static/theme.js")
    assert res.status_code == 200
    assert runner.calls == []


def test_atm_iv_default_iv_is_mid(make):
    client, _ = make()
    html = client.get("/atm_iv").get_data(as_text=True)
    assert '<option value="mid" selected>' in html
    assert '<option value="trade">' in html

# ---- /atm_iv_bars?n=roll（期近つなぎ、#101） ----

OCT, NOV, DEC = date(2026, 10, 8), date(2026, 11, 12), date(2026, 12, 10)
YMD = r"\d{4}-\d{2}-\d{2}"


def with_month(rows, month, shift=0.0):
    """限月付きの行（sql/*_months.sql の形）。shift で限月ごとに値を変える（混ざったら値で分かる）"""
    return [{**r, **({} if r["high"] is None else {k: r[k] + shift for k in ("open", "high", "low", "close")}),
             "last_trading_day": month} for r in rows]


def roll_results():
    results = bars_results()
    results["atm_iv_minute_months"] = sorted(
        with_month(results["atm_iv_minute"], NOV) + with_month(results["atm_iv_minute"], DEC, 30.0),
        key=lambda r: (r["started_at"], r["last_trading_day"]))
    results["atm_mid_minute_months"] = with_month(mid_results()["atm_mid_minute"], NOV)
    results["atm_mid_months"] = sorted(
        with_month(mid_results()["atm_mid"], NOV) + with_month(mid_results()["atm_mid"], DEC, 30.0),
        key=lambda r: (r["started_at"], r["last_trading_day"]))
    results["atm_iv_months"] = sorted(
        with_month(results["atm_iv"], NOV) + with_month(results["atm_iv"], DEC, 30.0),
        key=lambda r: (r["started_at"], r["last_trading_day"]))
    return results


@pytest.mark.parametrize("tf,names,count", [
    ("1m", ["latest_snapshot", "atm_iv_minute_months", "trading_holidays"], 3),
    ("5m", ["latest_snapshot", "atm_iv_minute_months", "trading_holidays"], 2),
    ("15m", ["latest_snapshot", "atm_iv_minute_months", "trading_holidays"], 1),
    ("1h", ["atm_iv_meta", "atm_iv_months", "trading_holidays"], 4),
    ("1d", ["atm_iv_meta", "atm_iv_months", "trading_holidays"], 2),
])
def test_atm_iv_bars_roll_each_tf(make, tf, names, count):
    client, runner = make(roll_results())
    res = client.get(f"/atm_iv_bars?tf={tf}&n=roll")
    assert res.status_code == 200
    lines = res.get_data(as_text=True).splitlines()
    assert len(lines) == count
    assert all(len(line.split(",")) == 6 and re.fullmatch(YMD, line.split(",")[5]) for line in lines)
    times = [line.split(",")[4] for line in lines]
    assert times == sorted(set(times))
    # 期近（11 月限）の値だけ（12 月限は +30）
    assert all(line.split(",")[5] == "2026-11-12" and float(line.split(",")[1]) < 40 for line in lines)
    assert [name for name, _ in runner.calls] == names
    # 期間・既定の範囲のヘッダは n=0 と同じ
    runner.calls.clear()
    zero = client.get(f"/atm_iv_bars?tf={tf}&n=0")
    assert (res.headers["X-Bars-Start"], res.headers["X-Bars-End"]) == (zero.headers["X-Bars-Start"],
                                                                         zero.headers["X-Bars-End"])


@pytest.mark.parametrize("tf", ["1m", "5m", "15m", "1h", "1d"])
@pytest.mark.parametrize("n", ["0", "1"])
def test_atm_iv_bars_n01_stays_five_columns(make, tf, n):
    client, _ = make(roll_results())
    lines = client.get(f"/atm_iv_bars?tf={tf}&n={n}").get_data(as_text=True).splitlines()
    assert lines and all(len(line.split(",")) == 5 for line in lines)


def test_atm_iv_bars_roll_params(make):
    client, runner = make(roll_results())
    # 分足: 範囲は n=0 と同じ、全限月のクエリは start_at・end_at だけ
    client.get(f"/atm_iv_bars?tf=5m&n=roll&start={ep(utc(10))}&end={ep(utc(13))}")
    assert runner.calls[0] == ("atm_iv_minute_months", {"start_at": utc(10), "end_at": utc(13)})
    assert runner.calls[1] == ("trading_holidays", {"start_date": date(2026, 10, 9), "end_date": date(2026, 11, 22)})
    # 1h: hourly_params の補う分・期間の値だけ
    runner.calls.clear()
    client.get(f"/atm_iv_bars?tf=1h&n=roll&start={ep(utc(12))}&end={ep(NOW)}")
    assert runner.calls[1] == ("atm_iv_months", {"fill_start": utc(12, 22), "fill_end": NOW, "days_start": utc(12),
                                                 "range_end": NOW})
    # 1d: _daily_bars と同じ読み方（start の 10 日前〜min(end + 1 日, now)）
    runner.calls.clear()
    client.get(f"/atm_iv_bars?tf=1d&n=roll&start={ep(utc(8))}&end={ep(utc(10))}")
    assert runner.calls[1][1]["days_start"] == utc(28, month=9) and runner.calls[1][1]["range_end"] == utc(11)
    assert runner.calls[2] == ("trading_holidays", {"start_date": date(2026, 9, 27), "end_date": date(2026, 11, 20)})


@pytest.mark.parametrize("tf,names", [
    ("1m", ["latest_snapshot", "atm_mid_minute_months", "trading_holidays"]),
    ("5m", ["latest_snapshot", "atm_mid_minute_months", "trading_holidays"]),
    ("15m", ["latest_snapshot", "atm_mid_minute_months", "trading_holidays"]),
    # #98: 1 時間足・日足の mid の期近つなぎは sql/atm_mid_months.sql
    ("1h", ["atm_iv_meta", "atm_mid_months", "trading_holidays"]),
    ("1d", ["atm_iv_meta", "atm_mid_months", "trading_holidays"]),
])
def test_atm_iv_bars_roll_mid(make, tf, names):
    client, runner = make(roll_results())
    res = client.get(f"/atm_iv_bars?tf={tf}&n=roll&iv=mid")
    assert res.status_code == 200
    lines = res.get_data(as_text=True).splitlines()
    assert lines and all(len(line.split(",")) == 6 for line in lines)
    assert all(line.split(",")[5] == "2026-11-12" for line in lines)
    assert [name for name, _ in runner.calls] == names
    if tf in ("1h", "1d"):
        # パラメータは約定の期近つなぎと同じ
        mid_calls = list(runner.calls)
        runner.calls.clear()
        client.get(f"/atm_iv_bars?tf={tf}&n=roll")
        assert [p for _, p in mid_calls] == [p for _, p in runner.calls]


@pytest.mark.parametrize("path", [
    "/atm_iv_bars?tf=1h&n=roll&iv=MID", "/atm_iv_data?n=roll", "/atm_iv_bars?n=ROLL", "/atm_iv_bars?n=roll%0A",
    "/atm_iv_bars?n=Roll",
    "/atm_iv_bars?n=%20roll", "/atm_iv_bars?n=2", "/atm_iv_bars?n=", "/atm_iv_bars?tf=1m&n=rolls",
    # start・end の検証は n=0 と同じ（片方だけ・1 回の最大・分足の遡れる上限）
    f"/atm_iv_bars?n=roll&start={ep(utc(12))}",
    f"/atm_iv_bars?tf=1m&n=roll&start={ep(utc(9))}&end={ep(utc(12, 1))}",
    f"/atm_iv_bars?tf=1h&n=roll&start={ep(utc(1, month=6))}&end={ep(utc(10, month=9))}",
    f"/atm_iv_bars?tf=1d&n=roll&start={ep(utc(1, month=1, year=2025))}&end={ep(utc(10, month=2, year=2026))}",
    f"/atm_iv_bars?tf=5m&n=roll&start={ep(utc(11, 23, month=9))}&end={ep(utc(13, month=9))}",
    f"/atm_iv_bars?tf=1d&n=roll&start={ep(utc(31, month=12, year=2009))}&end={ep(utc(10, month=1, year=2010))}",
])
def test_atm_iv_bars_roll_bad_request(make, path, capsys):
    client, runner = make(roll_results())
    res = client.get(path)
    assert res.status_code == 400
    assert runner.calls == []
    record = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert record["event"] == "bad_request"


@pytest.mark.parametrize("tf,name", [("1m", "atm_iv_minute_months"), ("15m", "atm_iv_minute_months"),
                                     ("1h", "atm_iv_months"), ("1d", "atm_iv_months"), ("1d", "trading_holidays")])
def test_atm_iv_bars_roll_query_error_is_500(make, capsys, tf, name):
    results = roll_results()
    results[name] = RuntimeError("secret detail: project 123456789012 user@example.com")
    client, _ = make(results)
    res = client.get(f"/atm_iv_bars?tf={tf}&n=roll")
    assert res.status_code == 500
    assert res.get_data(as_text=True) == "internal error\n"
    out = capsys.readouterr().out
    record = json.loads(out.strip().splitlines()[-1])
    assert record["event"] == "query_failed" and record["endpoint"] == "atm_iv_bars"
    assert "secret" not in out


@pytest.mark.parametrize("tf,name", [("1m", "atm_iv_minute_months"), ("5m", "atm_iv_minute_months"),
                                     ("1h", "atm_iv_months"), ("1d", "atm_iv_months")])
def test_atm_iv_bars_roll_no_rows_is_empty_200(make, tf, name):
    results = roll_results()
    results[name] = []
    client, runner = make(results)
    res = client.get(f"/atm_iv_bars?tf={tf}&n=roll")
    assert res.status_code == 200 and res.get_data(as_text=True) == ""
    # 行が無ければ休業日を読まない（空の要求で課金を増やさない）
    assert "trading_holidays" not in [name for name, _ in runner.calls]


def test_atm_iv_bars_roll_no_snapshot_is_empty_200(make):
    results = roll_results()
    results["latest_snapshot"] = [{"snapshot_at": None}]
    client, runner = make(results)
    res = client.get("/atm_iv_bars?tf=1m&n=roll")
    assert res.status_code == 200 and res.get_data(as_text=True) == ""
    assert [name for name, _ in runner.calls] == ["latest_snapshot"]


def crossing_results():
    """10 月限（L = 10-08（木））→ 11 月限: 取引日 10-01（r = 5）から。09-30（水）15 時台 JST は 10 月限、
    17 時台（夜間の始まり）から 11 月限"""
    def hr(day, h, v, month):
        return {"open": v, "high": v + 1, "low": v - 1, "close": v, "last_trading_day": month,
                "started_at": datetime(2026, 9, day, h, tzinfo=UTC)}

    hourly = [hr(30, 6, 20.0, OCT), hr(30, 6, 18.0, NOV), hr(30, 8, 21.0, OCT), hr(30, 8, 19.0, NOV),
              hr(30, 9, 21.5, OCT), hr(30, 9, 19.5, NOV)]
    minutes = [{**r, "started_at": r["started_at"] + timedelta(minutes=m)} for r in hourly for m in (0, 1, 7)]
    minutes.sort(key=lambda r: (r["started_at"], r["last_trading_day"]))
    return {**roll_results(), "atm_iv_months": hourly, "atm_iv_minute_months": minutes, "trading_holidays": []}


@pytest.mark.parametrize("tf,want", [
    ("1h", [("2026-09-30 06:00:00+00:00", "2026-10-08"), ("2026-09-30 08:00:00+00:00", "2026-11-12"),
            ("2026-09-30 09:00:00+00:00", "2026-11-12")]),
    ("15m", [("2026-09-30 06:00:00+00:00", "2026-10-08"), ("2026-09-30 08:00:00+00:00", "2026-11-12"),
             ("2026-09-30 09:00:00+00:00", "2026-11-12")]),
    ("5m", [("2026-09-30 06:00:00+00:00", "2026-10-08"), ("2026-09-30 06:05:00+00:00", "2026-10-08"),
            ("2026-09-30 08:00:00+00:00", "2026-11-12"), ("2026-09-30 08:05:00+00:00", "2026-11-12"),
            ("2026-09-30 09:00:00+00:00", "2026-11-12"), ("2026-09-30 09:05:00+00:00", "2026-11-12")]),
    ("1d", [("2026-09-29 15:00:00+00:00", "2026-10-08"), ("2026-09-30 15:00:00+00:00", "2026-11-12")]),
])
def test_atm_iv_bars_roll_crosses_switch(make, tf, want):
    client, _ = make(crossing_results())
    res = client.get(f"/atm_iv_bars?tf={tf}&n=roll&start={ep(utc(29, month=9))}&end={ep(utc(1))}")
    lines = res.get_data(as_text=True).splitlines()
    assert [(line.split(",")[4], line.split(",")[5]) for line in lines] == want
    # 乗り換えの後は 11 月限の値（期近の値が混ざらない）
    assert all(float(line.split(",")[3]) < 20.0 for line in lines if line.endswith("2026-11-12"))


YEAR_END_HOLIDAYS = (date(2025, 12, 31), date(2026, 1, 1), date(2026, 1, 2))
JAN, FEB = date(2026, 1, 8), date(2026, 2, 12)


class HolidayRunner(FakeRunner):
    """trading_holidays だけ params の [start_date, end_date) に入る偽の休業日を返す runner"""

    def __call__(self, name, params):
        if name == "trading_holidays":
            self.calls.append((name, params))
            return [{"calendar_date": d} for d in YEAR_END_HOLIDAYS if params["start_date"] <= d < params["end_date"]]
        return super().__call__(name, params)


@pytest.mark.parametrize("tf,want", [
    ("1h", [("2025-12-26 06:00:00+00:00", "2026-01-08"), ("2025-12-26 08:00:00+00:00", "2026-02-12")]),
    ("1m", [("2025-12-26 06:00:00+00:00", "2026-01-08"), ("2025-12-26 08:00:00+00:00", "2026-02-12")]),
    ("1d", [("2025-12-25 15:00:00+00:00", "2026-01-08"), ("2025-12-28 15:00:00+00:00", "2026-02-12")]),
])
def test_atm_iv_bars_roll_year_end_holidays(tf, want):
    # 1 月限（L = 2026-01-08）→ 2 月限: 年末年始（12-31〜01-02）の休業をはさみ、取引日 12-29（月）から。
    # 読む窓の最後は 12-26（金）〜12-29。休業日を読む範囲が足りないと乗り換えが遅れる
    now = datetime(2025, 12, 29, 9, tzinfo=UTC)

    def r(h, v, month):
        return {"open": v, "high": v + 1, "low": v - 1, "close": v, "last_trading_day": month,
                "started_at": datetime(2025, 12, 26, h, tzinfo=UTC)}

    rows = [r(6, 20.0, JAN), r(6, 18.0, FEB), r(8, 21.0, JAN), r(8, 19.0, FEB)]
    runner = HolidayRunner({"atm_iv_meta": [{"last_started_at": datetime(2025, 12, 28, 21, tzinfo=UTC)}],
                            "atm_iv_months": rows, "atm_iv_minute_months": rows})
    client = create_app(runner=runner, clock=lambda: now).test_client()
    start, end = datetime(2025, 12, 25, tzinfo=UTC), now
    if tf == "1m":
        start, end = datetime(2025, 12, 26, tzinfo=UTC), datetime(2025, 12, 29, tzinfo=UTC)  # 分足は 1 回 3 日まで
    lines = client.get(f"/atm_iv_bars?tf={tf}&n=roll&start={ep(start)}&end={ep(end)}").get_data(as_text=True)
    assert [(line.split(",")[4], line.split(",")[5]) for line in lines.splitlines()] == want
    [(_, params)] = [c for c in runner.calls if c[0] == "trading_holidays"]
    assert params["end_date"] >= api.jst_date(end) + timedelta(days=26)


def test_roll_hourly_params():
    last = datetime(2026, 10, 12, 21, tzinfo=UTC)
    recent = api.roll_hourly_params(NOW, last, utc(12), NOW)
    assert set(recent) == {"fill_start", "fill_end", "days_start", "range_end"}
    assert recent["fill_end"] == NOW
    assert recent == {k: v for k, v in api.hourly_params(NOW, last, utc(12), NOW, 0).items() if k in recent}
    past = api.roll_hourly_params(NOW, last, utc(1, month=9), utc(1))
    assert past["fill_end"] < past["fill_start"]


@pytest.mark.parametrize("raw,want", [(None, 0), ("0", 0), ("1", 1), ("roll", "roll"), ("cm", "cm")])
def test_parse_bars_n(raw, want):
    assert api.parse_bars_n(raw) == want


# ---- /atm_iv_bars?n=cm（30 日固定の ATM IV、#100） ----

# atm_cm_iv の最後の行（10-13 05:55 JST。定期クエリの窓の終わりの前）
CM_LAST = datetime(2026, 10, 12, 20, 55, tzinfo=UTC)


def cm_results(tf):
    """sql/atm_cm.sql の行: 分足は 1 分の足（値の無い分を含む）、1h・1d は 1 時間の足
    （bars_results の atm_iv と同じ時刻）"""
    results = bars_results()
    results["atm_cm_meta"] = [{"last_created_at": CM_LAST}]
    results["atm_cm"] = results["atm_iv_minute"] if tf in ("1m", "5m", "15m") else results["atm_iv"]
    return results


@pytest.mark.parametrize("tf,names,count", [
    ("1m", ["latest_snapshot", "atm_cm_meta", "atm_cm"], 3),
    ("5m", ["latest_snapshot", "atm_cm_meta", "atm_cm"], 2),
    ("15m", ["latest_snapshot", "atm_cm_meta", "atm_cm"], 1),
    ("1h", ["atm_cm_meta", "atm_cm"], 4),
    ("1d", ["atm_cm_meta", "atm_cm", "trading_holidays"], 2),
])
def test_atm_iv_bars_cm_each_tf(make, tf, names, count):
    client, runner = make(cm_results(tf))
    res = client.get(f"/atm_iv_bars?tf={tf}&n=cm")
    assert res.status_code == 200
    lines = res.get_data(as_text=True).splitlines()
    assert len(lines) == count and all(len(line.split(",")) == 5 for line in lines)
    assert [name for name, _ in runner.calls] == names
    # 足・範囲のヘッダ・日足の束ね方は n=0 と同じ（同じ行を返すので同じ CSV）
    runner.calls.clear()
    zero = client.get(f"/atm_iv_bars?tf={tf}&n=0")
    assert res.get_data(as_text=True) == zero.get_data(as_text=True)
    assert (res.headers["X-Bars-Start"], res.headers["X-Bars-End"]) == (zero.headers["X-Bars-Start"],
                                                                         zero.headers["X-Bars-End"])


def test_atm_iv_bars_cm_ignores_iv(make):
    client, runner = make(cm_results("1h"))
    mid = client.get("/atm_iv_bars?tf=1h&n=cm&iv=mid").get_data(as_text=True)
    calls = list(runner.calls)
    runner.calls.clear()
    assert client.get("/atm_iv_bars?tf=1h&n=cm").get_data(as_text=True) == mid
    assert runner.calls == calls


def test_atm_iv_bars_cm_params(make):
    client, runner = make(cm_results("1m"))
    # 分足: 1 分（step 60）。補うのは atm_cm_iv の最後の行の 1 秒後から now まで
    client.get(f"/atm_iv_bars?tf=5m&n=cm&start={ep(utc(12))}&end={ep(utc(13, 6))}")
    assert runner.calls[0] == ("atm_cm_meta", {"start_at": NOW - timedelta(days=14), "end_at": NOW})
    assert runner.calls[1] == ("atm_cm", {
        "fill_start": CM_LAST + timedelta(seconds=1), "fill_end": NOW,
        "holiday_start": date(2026, 10, 13), "holiday_end": date(2026, 10, 13) + timedelta(days=120),
        "range_start": utc(12), "range_end": utc(13, 6), "step": 60})
    # 1h: step 3600
    runner.calls.clear()
    client.get(f"/atm_iv_bars?tf=1h&n=cm&start={ep(utc(3))}&end={ep(NOW)}")
    assert runner.calls[1][1]["step"] == 3600 and runner.calls[1][1]["range_start"] == utc(3)
    # 1d: 1 時間の足を start の 10 日前〜min(end + 1 日, now) で読む。
    # 表に入っている範囲だけなら生データを読まない（逆向き）
    runner.calls.clear()
    client.get(f"/atm_iv_bars?tf=1d&n=cm&start={ep(utc(8))}&end={ep(utc(10))}")
    params = runner.calls[1][1]
    assert (params["range_start"], params["range_end"], params["step"]) == (utc(28, month=9), utc(11), 3600)
    assert params["fill_end"] < params["fill_start"]


def test_cm_params_fill_floor():
    # 表が空・古いときに補うのは最大 3 日前の正時から（/atm_iv_data の 1 時間足と同じ）
    floor = utc(10, 6)
    assert api.cm_params(NOW, None, utc(1), NOW, 3600)["fill_start"] == floor
    assert api.cm_params(NOW, utc(1), utc(1), NOW, 3600)["fill_start"] == floor


@pytest.mark.parametrize("tf", ["1m", "1h", "1d"])
def test_atm_iv_bars_cm_no_rows_is_empty_200(make, tf):
    results = cm_results(tf)
    results["atm_cm"] = []
    client, runner = make(results)
    res = client.get(f"/atm_iv_bars?tf={tf}&n=cm")
    assert res.status_code == 200 and res.get_data(as_text=True) == ""
    # 行が無ければ休業日を読まない
    assert "trading_holidays" not in [name for name, _ in runner.calls]


@pytest.mark.parametrize("path", ["/atm_iv_bars?n=CM", "/atm_iv_bars?n=cm%0A", "/atm_iv_data?n=cm",
                                  "/atm_iv_bars?tf=1h&n=cm&iv=MID"])
def test_atm_iv_bars_cm_bad_request(make, path):
    client, runner = make(cm_results("1h"))
    assert client.get(path).status_code == 400
    assert runner.calls == []


@pytest.mark.parametrize("name", ["atm_cm_meta", "atm_cm"])
def test_atm_iv_bars_cm_query_error_is_500(make, capsys, name):
    results = cm_results("1h")
    results[name] = RuntimeError("secret detail: project 123456789012")
    client, _ = make(results)
    res = client.get("/atm_iv_bars?tf=1h&n=cm")
    assert res.status_code == 500
    assert "secret" not in capsys.readouterr().out

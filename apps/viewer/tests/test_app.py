"""Flask のルート・パラメータの検証・期間・エラーの検査（#52 AC 2・3）。BigQuery は偽の runner に差し替える。"""

import json
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


def ok_results():
    return {
        "latest_snapshot": [{"snapshot_at": SNAP}],
        "smile": smile_rows(),
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
    assert p["end_at"] == now and p["today"] == date(2026, 10, 13)


def test_today_is_jst():
    # UTC 2026-10-13 16:00 = JST 10-14 01:00
    assert api.jst_date(datetime(2026, 10, 13, 16, tzinfo=UTC)) == date(2026, 10, 14)


@pytest.mark.parametrize("path", ["/", "/smile", "/atm_iv", "/smile_data", "/atm_iv_data?d=0", "/static/smile.js",
                                  "/static/viewer.css", "/nonexistent"])
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


@pytest.mark.parametrize("tf,names,count", [
    ("1m", ["latest_snapshot", "atm_iv_minute"], 3),
    ("5m", ["latest_snapshot", "atm_iv_minute"], 2),
    ("15m", ["latest_snapshot", "atm_iv_minute"], 1),
    ("1h", ["atm_iv_meta", "atm_iv"], 4),
    ("1d", ["atm_iv_meta", "atm_iv", "trading_holidays"], 2),
])
def test_atm_iv_bars_each_tf(make, tf, names, count):
    client, runner = make(bars_results())
    res = client.get(f"/atm_iv_bars?tf={tf}")
    assert res.status_code == 200
    assert res.headers["Content-Type"] == "text/csv; charset=utf-8"
    assert res.headers["Cache-Control"] == "no-cache"
    text = res.get_data(as_text=True)
    times = csv_times(text)
    assert len(times) == count
    # 昇順・重複なし、値の無い足は出さない
    assert times == sorted(set(times))
    assert all(line.split(",")[1] != "" for line in text.splitlines())
    assert [name for name, _ in runner.calls] == names


def test_atm_iv_bars_minute_params(make):
    client, runner = make(bars_results())
    res = client.get("/atm_iv_bars?tf=5m&d=3&n=1")
    assert res.get_data(as_text=True) == ("18.0,18.9,17.9,18.8,2026-10-13 05:50:00+00:00\n"
                                          "18.8,18.8,18.3,18.4,2026-10-13 05:55:00+00:00\n")
    assert runner.calls[0][1] == {"start_at": NOW - timedelta(days=14), "end_at": NOW}
    # 窓の始まりは最新のスナップショットの d 日前の UTC の 0 時（取引時間外・週末に開いても直近の足が入る）
    assert runner.calls[1][1] == {"start_at": datetime(2026, 10, 10, tzinfo=UTC), "end_at": NOW,
                                  "today": date(2026, 10, 13), "n": 1}


def test_atm_iv_bars_minute_defaults(make):
    client, runner = make(bars_results())
    assert client.get("/atm_iv_bars?tf=1m").get_data(as_text=True).splitlines()[0] == (
        "18.0,18.5,17.9,18.2,2026-10-13 05:50:00+00:00")
    assert runner.calls[1][1]["start_at"] == datetime(2026, 10, 12, tzinfo=UTC)
    assert runner.calls[1][1]["n"] == 0


def test_atm_iv_bars_hourly_same_params_as_atm_iv_data(make):
    # 1 時間足は /atm_iv_data と同じクエリ・パラメータ（AC 10）
    client, runner = make(bars_results())
    assert client.get("/atm_iv_bars?d=20&n=1").status_code == 200
    bars_calls = list(runner.calls)
    runner.calls.clear()
    assert client.get("/atm_iv_data?d=20&n=1").status_code == 200
    assert bars_calls == runner.calls


def test_atm_iv_bars_default_tf_is_1h(make):
    client, runner = make(bars_results())
    assert client.get("/atm_iv_bars").status_code == 200
    assert [name for name, _ in runner.calls] == ["atm_iv_meta", "atm_iv"]
    # 1h の既定 d = 10
    assert runner.calls[1][1]["days_start"] == datetime(2026, 10, 3, tzinfo=UTC)


def test_atm_iv_bars_daily(make):
    client, runner = make(bars_results())
    res = client.get("/atm_iv_bars?tf=1d&d=4&n=0")
    # 10-09 00:00 UTC（金曜 09:00 JST）の日中 → 10-09、金曜の夜間・休業日（月）の足 → 休み明けの火曜
    assert res.get_data(as_text=True) == ("20.0,21.0,19.5,20.5,2026-10-08 15:00:00+00:00\n"
                                          "20.5,22.0,18.0,18.8,2026-10-12 15:00:00+00:00\n")
    # 日足は d + 1 日分の 1 時間足を読む
    params = runner.calls[1][1]
    assert params["days_start"] == datetime(2026, 10, 8, tzinfo=UTC)
    assert runner.calls[2] == ("trading_holidays", {"start_date": date(2026, 10, 7), "end_date": date(2026, 10, 28)})


def test_atm_iv_bars_daily_drops_partial_first_day(make):
    # 読んだ範囲の先頭（10-08 00:00 UTC = 09:00 JST）より前の夜間が欠けた日（10-08）は出さない
    results = bars_results()
    results["atm_iv"] = [hour(8, 0, 30.0, 30.0, 30.0, 30.0), *results["atm_iv"]]
    client, _ = make(results)
    times = csv_times(client.get("/atm_iv_bars?tf=1d&d=4").get_data(as_text=True))
    assert times == ["2026-10-08 15:00:00+00:00", "2026-10-12 15:00:00+00:00"]


@pytest.mark.parametrize("query", [
    "tf=2m", "tf=", "tf=1H", "tf=1m%0A", "tf=%201m", "tf=1d;DROP",
    "tf=1m&d=4", "tf=5m&d=4", "tf=15m&d=0", "tf=1h&d=101", "tf=1d&d=101", "d=0", "tf=1m&d=abc", "tf=1h&d=10%0A",
    "tf=1m&n=2", "n=-1", "tf=1d&n=", "n=01",
])
def test_atm_iv_bars_bad_request(make, query, capsys):
    client, runner = make(bars_results())
    res = client.get(f"/atm_iv_bars?{query}")
    assert res.status_code == 400
    assert runner.calls == []
    assert res.get_data(as_text=True) == "bad request\n"
    record = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert record["event"] == "bad_request" and record["endpoint"] == "atm_iv_bars"


@pytest.mark.parametrize("tf,d", [("1m", "1"), ("1m", "3"), ("5m", "2"), ("15m", "3"), ("1h", "1"), ("1h", "100"),
                                  ("1d", "1"), ("1d", "100")])
def test_atm_iv_bars_d_range_accepts(make, tf, d):
    client, _ = make(bars_results())
    assert client.get(f"/atm_iv_bars?tf={tf}&d={d}").status_code == 200


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

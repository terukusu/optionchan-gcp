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

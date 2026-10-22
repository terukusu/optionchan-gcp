import dataclasses
import re
from datetime import UTC, datetime, timedelta

import pytest
import requests
from tachibana_fakes import (
    AUTH_ID,
    BASE_URL,
    PASSWORD,
    VIRTUAL_URLS,
    FakeClock,
    credentials,
    fake_decrypt,
    fixture_json,
    json_body,
    make_client,
    market_price_handler,
    ok,
)

from optionchan_ingest import pipeline
from optionchan_ingest.parser import JST
from optionchan_ingest.sources.tachibana import client as tc
from optionchan_ingest.sources.tachibana.parser import MARKET_COLUMNS

AUTH_URL = BASE_URL + "auth/"
SD_DATE = re.compile(r"^\d{4}\.\d{2}\.\d{2}-\d{2}:\d{2}:\d{2}\.\d{3}$")
CODES_241 = [str(900100000 + i) for i in range(241)]


def logged_in(routes=None, **kwargs):
    client, http, clock = make_client(routes, **kwargs)
    client.login(timeout=10)
    return client, http, clock


# --- 要求の形（AC 7） ---


def test_p_no_p_sd_date_and_targets():
    client, http, clock = make_client()
    client.login(timeout=10)
    clock.t += 1.2345
    client.get_option_master(timeout=10)
    clock.t += 0.5
    client.get_market_price(["900000001"], ["pDPP"], timeout=10)

    payloads = http.payloads()
    assert [p["p_no"] for p in payloads] == ["1", "2", "3"]
    assert [p["sCLMID"] for p in payloads] == [tc.CLMID_LOGIN, tc.CLMID_OPTION_MASTER, tc.CLMID_MARKET_PRICE]
    for p, sent in zip(payloads, http.sent_at, strict=True):
        assert SD_DATE.match(p["p_sd_date"])
        expected = (clock.start + timedelta(seconds=sent)).astimezone(JST)
        assert p["p_sd_date"] == expected.strftime("%Y.%m.%d-%H:%M:%S.") + f"{expected.microsecond // 1000:03d}"
    assert payloads[0]["p_sd_date"] == "2031.03.03-08:00:00.000"  # UTC 23:00 → JST の翌日 08:00
    assert payloads[1]["p_sd_date"] == "2031.03.03-08:00:01.234"
    assert http.targets() == [AUTH_URL, VIRTUAL_URLS["master"], VIRTUAL_URLS["price"]]
    assert payloads[0]["sAuthId"] == AUTH_ID
    assert tc.LOGIN_PASSWORD_FIELD not in payloads[0]


def test_master_targets_and_logout_target():
    client, http, _ = logged_in()
    client.get_future_master(timeout=10)
    client.get_date_info(timeout=10)
    client.logout(timeout=10)
    assert http.targets()[1:] == [VIRTUAL_URLS["master"], VIRTUAL_URLS["master"], VIRTUAL_URLS["request"]]
    assert [p["sCLMID"] for p in http.payloads()[1:]] == [
        tc.CLMID_FUTURE_MASTER,
        tc.CLMID_DATE_INFO,
        tc.CLMID_LOGOUT,
    ]
    assert not client.logged_in


def test_login_again_resets_p_no():
    client, http, _ = logged_in()
    client.get_option_master(timeout=10)
    client.logout(timeout=10)
    client.login(timeout=10)
    client.get_option_master(timeout=10)
    assert [p["p_no"] for p in http.payloads()] == ["1", "2", "3", "1", "2"]


def test_url_is_encoded():
    client, http, _ = make_client(password=PASSWORD)
    client.login(timeout=10)
    client.get_market_price(["900000001", "900000002"], ["pDPP", "tDPP:T"], timeout=10)
    for url, _ in http.calls:
        query = url.partition("?")[2]
        for ch in '#+/:={}", ':
            assert ch not in query
    payloads = http.payloads()
    assert payloads[0][tc.LOGIN_PASSWORD_FIELD] == PASSWORD
    assert payloads[1]["sTargetIssueCode"] == "900000001,900000002"
    assert payloads[1]["sTargetColumn"] == "pDPP,tDPP:T"


def test_session_returned_by_login():
    client, _, _ = make_client()
    session = client.login(timeout=10)
    assert client.logged_in
    assert session.price_url == VIRTUAL_URLS["price"]
    assert session.master_url == VIRTUAL_URLS["master"]
    assert session.request_url == VIRTUAL_URLS["request"]
    assert session.event_url == VIRTUAL_URLS["event"]
    assert session.event_ws_url == VIRTUAL_URLS["event_ws"]


# --- 時価の分割と間隔（AC 8） ---


@pytest.mark.parametrize("cost", [0.0, 0.3])
def test_market_price_is_split_by_120_and_keeps_interval(cost):
    client, http, clock = logged_in(min_interval=0.5, cost=cost)
    prices = client.get_market_price(CODES_241, MARKET_COLUMNS, timeout=30)

    price_calls = http.calls_to(VIRTUAL_URLS["price"])
    assert len(price_calls) == 3
    sizes = [len(p["sTargetIssueCode"].split(",")) for p in http.payloads()[1:]]
    assert sizes == [120, 120, 1]
    assert [i["sIssueCode"] for i in prices.items] == CODES_241
    assert len(prices.raw) == 3
    gaps = [b - a for a, b in zip(http.sent_at, http.sent_at[1:], strict=False)]
    assert gaps and all(g >= 0.5 - 1e-9 for g in gaps)
    assert clock.sleeps  # 間隔は sleep で待つ


def test_market_price_rejects_bad_arguments():
    client, http, _ = logged_in()
    for codes, columns in (([], ["pDPP"]), (["1"], []), (["1,2"], ["pDPP"]), ([""], ["pDPP"])):
        with pytest.raises(ValueError):
            client.get_market_price(codes, columns, timeout=10)
    assert len(http.calls) == 1


# --- エラー（AC 9） ---


def body(obj) -> tc.HttpResponse:
    return tc.HttpResponse(200, json_body(obj))


ERROR_CASES = [
    ("p_errno", body({"p_errno": "2", "sResultCode": "0"}), tc.TachibanaApiError, "2"),
    ("sResultCode", body({"p_errno": "0", "sResultCode": "10031", "sResultText": "x"}), tc.TachibanaApiError, "10031"),
    ("http_503", tc.HttpResponse(503, b"busy"), tc.TachibanaHttpError, "503"),
    ("http_404", tc.HttpResponse(404, b"none"), tc.TachibanaHttpError, "404"),
    ("http_302", tc.HttpResponse(302, b""), tc.TachibanaHttpError, "302"),
    ("not_json", tc.HttpResponse(200, b"<html>"), tc.TachibanaResponseError, None),
    ("json_array", tc.HttpResponse(200, b"[1, 2]"), tc.TachibanaResponseError, None),
    ("no_array", body({"p_errno": "0", "sResultCode": "0"}), tc.TachibanaResponseError, None),
    ("bad_items", body({"aCLMMfdsMarketPrice": ["x"]}), tc.TachibanaResponseError, None),
    ("undecodable", tc.HttpResponse(200, b'{"a": "\x81"}'), tc.TachibanaResponseError, None),
    ("http_error", tc.TachibanaHttpError(f"down {VIRTUAL_URLS['price']}"), tc.TachibanaHttpError, None),
    ("timeout", tc.TachibanaTimeout(f"slow {VIRTUAL_URLS['price']}"), tc.TachibanaTimeout, None),
    ("unexpected", RuntimeError(f"boom {VIRTUAL_URLS['price']}"), tc.TachibanaHttpError, None),
]


@pytest.mark.parametrize(("name", "route", "expected", "code"), ERROR_CASES, ids=[c[0] for c in ERROR_CASES])
def test_errors_have_types_and_do_not_relogin(name, route, expected, code):
    client, http, _ = logged_in({tc.CLMID_MARKET_PRICE: route})
    with pytest.raises(expected) as e:
        client.get_market_price(["900000001"], ["pDPP"], timeout=10)
    assert type(e.value) is expected
    assert e.value.code == code
    assert e.value.clmid == tc.CLMID_MARKET_PRICE
    assert len(http.calls_to(AUTH_URL)) == 1  # 自動でログインし直さない
    assert client.logged_in  # セッション切れ以外ではセッションを残す
    # 次の呼び出しは同じセッションで送る
    http.routes[tc.CLMID_MARKET_PRICE] = market_price_handler
    client.get_market_price(["900000001"], ["pDPP"], timeout=10)
    assert len(http.calls_to(AUTH_URL)) == 1
    assert http.payloads()[-1]["p_no"] == "3"  # 失敗した要求の番号も使ったものとして進める


def test_api_error_keeps_kind_and_text():
    client, _, _ = logged_in({tc.CLMID_OPTION_MASTER: body({"sResultCode": "10031", "sResultText": "server text"})})
    with pytest.raises(tc.TachibanaApiError) as e:
        client.get_option_master(timeout=10)
    assert e.value.kind == "sResultCode"
    assert e.value.text == "server text"
    assert "server text" not in str(e.value)
    assert "server text" not in repr(e.value.args)


@pytest.mark.parametrize(
    ("code", "shown"),
    [("10031", "10031"), ("-1", "-1"), ("x" * 20, "?"), (VIRTUAL_URLS["price"], "?"), ("12345678901", "?")],
)
def test_api_error_message_shows_only_short_numeric_codes(code, shown):
    client, _, _ = logged_in({tc.CLMID_OPTION_MASTER: body({"p_errno": code})})
    with pytest.raises(tc.TachibanaApiError) as e:
        client.get_option_master(timeout=10)
    assert str(e.value) == f"{tc.CLMID_OPTION_MASTER} p_no=2: p_errno={shown}"
    assert e.value.code == code  # 属性には元の値を持つ


def test_lock_wait_is_bounded_by_budget():
    client, http, _ = logged_in()
    # 待つのをやめた呼び出しがロックを持ったまま居残っている状態
    assert client._lock.acquire()
    try:
        with pytest.raises(tc.TachibanaTimeout, match="lock"):
            client.get_option_master(timeout=0.05)
        with pytest.raises(tc.TachibanaTimeout, match="lock"):
            client.login(timeout=0.05)
    finally:
        client._lock.release()
    assert len(http.calls) == 1
    client.get_option_master(timeout=10)  # 居残りが終われば次の呼び出しは送れる
    assert http.payloads()[-1]["p_no"] == "2"


def test_login_error_is_api_error():
    client, http, _ = make_client({tc.CLMID_LOGIN: body({"p_errno": "0", "sResultCode": "10001"})})
    with pytest.raises(tc.TachibanaApiError):
        client.login(timeout=10)
    assert not client.logged_in
    assert len(http.calls) == 1


def test_session_expired_drops_session(monkeypatch):
    monkeypatch.setattr(tc, "SESSION_EXPIRED_CODES", frozenset({"99"}))
    client, http, _ = logged_in({tc.CLMID_MARKET_PRICE: body({"p_errno": "99"})})
    with pytest.raises(tc.TachibanaSessionExpired) as e:
        client.get_market_price(CODES_241, ["pDPP"], timeout=10)
    assert e.value.code == "99"
    assert not client.logged_in
    assert len(http.calls_to(VIRTUAL_URLS["price"])) == 1  # 残りの分割は送らない
    with pytest.raises(tc.TachibanaNotLoggedIn):
        client.get_market_price(["900000001"], ["pDPP"], timeout=10)
    assert len(http.calls_to(AUTH_URL)) == 1


def test_calls_before_login_are_rejected():
    client, http, _ = make_client()
    calls = [
        lambda: client.get_market_price(["900000001"], ["pDPP"], timeout=10),
        lambda: client.get_option_master(timeout=10),
        lambda: client.get_future_master(timeout=10),
        lambda: client.get_date_info(timeout=10),
        lambda: client.logout(timeout=10),
    ]
    for call in calls:
        with pytest.raises(tc.TachibanaNotLoggedIn):
            call()
    assert http.calls == []


def test_logout_drops_session_even_on_failure():
    client, _, _ = logged_in({tc.CLMID_LOGOUT: tc.HttpResponse(500, b"")})
    with pytest.raises(tc.TachibanaHttpError):
        client.logout(timeout=10)
    assert not client.logged_in


def test_timeout_budget_stops_before_sending():
    client, http, clock = logged_in(min_interval=0.2)
    clock.t += 10
    with pytest.raises(tc.TachibanaTimeout):
        client.get_market_price(CODES_241, ["pDPP"], timeout=0.3)
    # 1 回目は待たずに送る、2 回目は 0.2 秒待って送る、3 回目は 0.4 秒で予算 0.3 秒を超えるので送らない
    assert len(http.calls_to(VIRTUAL_URLS["price"])) == 2
    for _, timeout in http.calls[1:]:
        assert timeout <= 0.3 + 1e-9


def test_timeout_budget_includes_http_time():
    client, http, _ = logged_in(cost=2.0)
    with pytest.raises(tc.TachibanaTimeout):
        client.get_market_price(CODES_241, ["pDPP"], timeout=3)
    timeouts = [t for _, t in http.calls_to(VIRTUAL_URLS["price"])]
    assert timeouts == [3, 1]


def test_zero_timeout_sends_nothing():
    client, http, _ = make_client()
    with pytest.raises(tc.TachibanaTimeout):
        client.login(timeout=0)
    assert http.calls == []


def test_http_receives_remaining_budget():
    client, http, clock = logged_in()
    clock.t += 1  # 間隔の待ちが無いように
    client.get_option_master(timeout=4.5)
    assert http.calls[-1][1] == pytest.approx(4.5)
    # 間隔の待ちは予算から引く
    client.get_option_master(timeout=4.5)
    assert http.calls[-1][1] == pytest.approx(4.4)
    assert clock.sleeps == [pytest.approx(0.1)]


def test_can_be_called_from_pipeline_bounded():
    client, http, clock = logged_in()
    deadline = pipeline.Deadline(30, clock.monotonic)
    prices = pipeline.bounded(deadline, "tachibana_price", 5, client.get_market_price, CODES_241, ["pDPP"])
    assert len(prices.items) == 241
    assert all(t <= 5 for _, t in http.calls_to(VIRTUAL_URLS["price"]))


# --- ログイン ---


def test_login_without_virtual_url_is_auth_error():
    ack = fixture_json("login_ack.json")
    del ack["sUrlPrice"]
    client, _, _ = make_client({tc.CLMID_LOGIN: ok(ack)})
    with pytest.raises(tc.TachibanaAuthError) as e:
        client.login(timeout=10)
    assert "sUrlPrice" in str(e.value)
    assert not client.logged_in


def test_decrypt_failure_is_auth_error():
    client, _, _ = make_client({tc.CLMID_LOGIN: ok({**fixture_json("login_ack.json"), "sUrlMaster": "BAD:master"})})
    with pytest.raises(tc.TachibanaAuthError) as e:
        client.login(timeout=10)
    assert str(e.value) == "decrypt failed: sUrlMaster"
    assert e.value.__context__ is None and e.value.__cause__ is None
    assert not client.logged_in


@pytest.mark.parametrize(
    ("name", "url"),
    [("sUrlPrice", "http://example.invalid/plain/"), ("sUrlRequest", "wss://example.invalid/ws/"), ("sUrlMaster", "")],
)
def test_decrypted_url_must_be_https(name, url):
    client, _, _ = make_client()
    original = client._decrypt
    kind = fixture_json("login_ack.json")[name]
    client._decrypt = lambda enc: url if enc == kind else original(enc)
    with pytest.raises(tc.TachibanaAuthError):
        client.login(timeout=10)
    assert not client.logged_in


@pytest.mark.parametrize(
    "base_url", ["http://example.invalid/e_api_v4r10/", "https://example.invalid/e_api_v4r10", "example.invalid/"]
)
def test_base_url_must_be_https_with_slash(base_url):
    with pytest.raises(ValueError):
        tc.TachibanaClient(base_url, None, lambda s: s)


def test_unknown_encoding_is_rejected():
    with pytest.raises(LookupError):
        tc.TachibanaClient(BASE_URL, None, lambda s: s, encoding="no-such-encoding")


def test_naive_now_is_rejected():
    client, http, _ = make_client(now=lambda: datetime(2031, 3, 3, 8, 0))
    with pytest.raises(ValueError):
        client.login(timeout=10)
    assert http.calls == []


# --- 生の応答・文字コード ---


def test_raw_responses_are_returned_without_url():
    client, http, _ = logged_in()
    prices = client.get_market_price(CODES_241, ["pDPP"], timeout=30)
    assert [r.body for r in prices.raw] == http.returned[1:]
    assert [r.p_no for r in prices.raw] == [2, 3, 4]
    assert all(r.clmid == tc.CLMID_MARKET_PRICE and r.http_status == 200 for r in prices.raw)
    assert all(r.sent_at.tzinfo is not None for r in prices.raw)
    master = client.get_option_master(timeout=10)
    assert master.raw.body == http.returned[-1]
    names = {f.name for f in dataclasses.fields(tc.RawResponse)}
    assert names == {"clmid", "p_no", "sent_at", "http_status", "body"}


def test_reads_cp932_by_default():
    client, _, _ = logged_in()
    master = client.get_option_master(timeout=10)
    assert master.items[0]["sIssueName"].startswith("架空オプション")
    assert len(master.items) == len(fixture_json("master_op.json")["aCLMStkIssueMstOp"])


def test_reads_utf8_when_given():
    utf8 = tc.HttpResponse(200, json_body(fixture_json("master_sak.json"), "utf-8"))
    login = ok(fixture_json("login_ack.json"), "utf-8")
    client, _, _ = logged_in({tc.CLMID_FUTURE_MASTER: utf8, tc.CLMID_LOGIN: login}, encoding="utf-8")
    master = client.get_future_master(timeout=10)
    assert master.items[0]["sIssueName"].startswith("架空先物")


def test_date_info_items():
    client, _, _ = logged_in()
    info = client.get_date_info(timeout=10)
    assert info.items[0]["sTheDay"] == "20310303"


# --- 純粋な補助関数 ---


def test_format_sd_date():
    assert tc.format_sd_date(datetime(2031, 3, 2, 15, 4, 5, 678900, tzinfo=UTC)) == "2031.03.03-00:04:05.678"
    with pytest.raises(ValueError):
        tc.format_sd_date(datetime(2031, 3, 3))


def test_build_url_and_chunks():
    url = tc.build_url("https://example.invalid/x/", {"a": "1,2", "b": "日経"})
    assert url.startswith("https://example.invalid/x/?%7B%22a%22%3A%221%2C2%22")
    assert tc.chunks([str(i) for i in range(5)], 2) == [["0", "1"], ["2", "3"], ["4"]]
    assert tc.chunks([]) == []


def test_split_timeout():
    assert tc.split_timeout(10) == (3.0, 7.0)
    assert tc.split_timeout(2) == (1.0, 1.0)


# --- 既定の HTTP（requests を差し替える。通信なし） ---


class FakeRequestsResponse:
    status_code = 200
    content = b'{"p_errno":"0"}'


def test_requests_get_returns_response(monkeypatch):
    seen = {}

    def fake_get(self, url, timeout, allow_redirects=True):
        seen["url"], seen["timeout"], seen["allow_redirects"] = url, timeout, allow_redirects
        return FakeRequestsResponse()

    monkeypatch.setattr(requests.Session, "get", fake_get)
    response = tc.requests_get("https://example.invalid/x/?q", 5)
    assert response == tc.HttpResponse(200, b'{"p_errno":"0"}')
    assert seen["url"] == "https://example.invalid/x/?q"
    connect, read = seen["timeout"]
    assert connect + read <= 5
    assert seen["allow_redirects"] is False  # 検査していない宛先に送らない


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (requests.ConnectionError, tc.TachibanaHttpError),
        (requests.ReadTimeout, tc.TachibanaTimeout),
        (requests.ConnectTimeout, tc.TachibanaTimeout),
        (requests.exceptions.ChunkedEncodingError, tc.TachibanaHttpError),
    ],
)
def test_requests_get_wraps_errors(monkeypatch, error, expected):
    def fake_get(self, url, timeout, allow_redirects=True):
        raise error(f"failed for {url}")

    monkeypatch.setattr(requests.Session, "get", fake_get)
    with pytest.raises(expected) as e:
        tc.requests_get(VIRTUAL_URLS["price"] + "?q", 5)
    assert type(e.value) is expected
    assert e.value.reason == error.__name__
    assert "SECRET-" not in str(e.value)
    assert e.value.__context__ is None and e.value.__cause__ is None


def test_requests_error_reason_reaches_client(monkeypatch):
    def fake_get(self, url, timeout, allow_redirects=True):
        raise requests.ConnectionError(f"failed for {url}")

    monkeypatch.setattr(requests.Session, "get", fake_get)
    clock = FakeClock()
    client = tc.TachibanaClient(
        BASE_URL, credentials(), fake_decrypt, now=clock.now, monotonic=clock.monotonic, sleep=clock.sleep
    )
    with pytest.raises(tc.TachibanaHttpError) as e:
        client.login(timeout=10)
    assert str(e.value) == f"{tc.CLMID_LOGIN} p_no=1: ConnectionError"

import dataclasses
import json
import re
from datetime import UTC, datetime, timedelta

import pytest
import requests
from tachibana_fakes import (
    AUTH_ID,
    BASE_URL,
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
    # ログインの項目は共通項目と sAuthId だけ（パスワードは無い）
    assert set(payloads[0]) == {"p_no", "p_sd_date", "sCLMID", "sJsonOfmt", "sAuthId"}


def test_all_requests_carry_json_ofmt():
    client, http, _ = logged_in()
    client.get_option_master(timeout=10)
    client.get_market_price(["900000001"], ["pDPP"], timeout=10)
    client.logout(timeout=10)
    assert tc.JSON_OFMT == "4"
    for p in http.payloads():
        assert p["sJsonOfmt"] == "4"
        assert list(p)[:4] == ["p_no", "p_sd_date", "sCLMID", "sJsonOfmt"]


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


def test_request_is_json_body():
    client, http, _ = make_client()
    client.login(timeout=10)
    client.get_market_price(["900000001", "900000002"], ["pDPP", "tDPP:T"], timeout=10)
    # 要求の URL は宛先そのもの（クエリを付けない）。認証 ID は本文にあって URL に無い
    assert http.targets() == [AUTH_URL, VIRTUAL_URLS["price"]]
    for url, _ in http.calls:
        assert "?" not in url
        assert AUTH_ID not in url
    assert AUTH_ID in http.bodies[0].decode("cp932")
    payloads = http.payloads()
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
    ("p_errno", body({"p_errno": "-1", "p_err": "argument"}), tc.TachibanaApiError, "-1"),
    ("sResultCode", body({"p_errno": "0", "sResultCode": "10031", "sResultText": "x"}), tc.TachibanaApiError, "10031"),
    ("http_503", tc.HttpResponse(503, b"busy"), tc.TachibanaHttpError, "503"),
    ("http_404", tc.HttpResponse(404, b"none"), tc.TachibanaHttpError, "404"),
    ("http_302", tc.HttpResponse(302, b""), tc.TachibanaHttpError, "302"),
    ("not_json", tc.HttpResponse(200, b"<html>"), tc.TachibanaResponseError, None),
    ("json_array", tc.HttpResponse(200, b"[1, 2]"), tc.TachibanaResponseError, None),
    ("no_array", body({"p_errno": "0", "sResultCode": "0"}), tc.TachibanaResponseError, None),
    ("no_p_errno", body({"sResultCode": "0", "aCLMMfdsMarketPrice": []}), tc.TachibanaResponseError, None),
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


def test_missing_p_errno_is_response_error():
    client, _, _ = logged_in({tc.CLMID_OPTION_MASTER: body({"aCLMStkIssueMstOp": []})})
    with pytest.raises(tc.TachibanaResponseError, match="missing p_errno"):
        client.get_option_master(timeout=10)


# (kind, code) → 例外の型・セッションが残るか（資料の p_errno の一覧）
P_ERRNO_TYPES = [
    ("p_errno", "2", tc.TachibanaSessionExpired, False),
    ("p_errno", "9", tc.TachibanaServiceOffline, True),
    ("p_errno", "-12", tc.TachibanaServiceOffline, True),
    ("p_errno", "-2", tc.TachibanaBusy, True),
    ("p_errno", "-3", tc.TachibanaBusy, True),
    ("p_errno", "-62", tc.TachibanaOutsideHours, True),
    ("p_errno", "-50", tc.TachibanaLoginRejected, True),
    ("p_errno", "6", tc.TachibanaApiError, True),
    ("p_errno", "8", tc.TachibanaApiError, True),
    ("p_errno", "-1", tc.TachibanaApiError, True),
    ("sResultCode", "2", tc.TachibanaApiError, True),  # セッション切れは p_errno だけに当てる
    ("sResultCode", "10031", tc.TachibanaApiError, True),  # ログイン以外の要求では普通のエラー
]


@pytest.mark.parametrize(
    ("kind", "code", "expected", "stays"), P_ERRNO_TYPES, ids=[f"{k}={c}" for k, c, _, _ in P_ERRNO_TYPES]
)
def test_error_codes_map_to_types(kind, code, expected, stays):
    doc = {"p_errno": "0", "p_err": ""} | {kind: code}
    client, _, _ = logged_in({tc.CLMID_OPTION_MASTER: body(doc)})
    with pytest.raises(tc.TachibanaApiError) as e:
        client.get_option_master(timeout=10)
    assert type(e.value) is expected
    assert e.value.code == code
    assert e.value.kind == kind
    assert client.logged_in is stays


def test_error_type_hierarchy():
    for t in (tc.TachibanaServiceOffline, tc.TachibanaBusy, tc.TachibanaOutsideHours):
        assert issubclass(t, tc.TachibanaUnavailable)
    for t in (tc.TachibanaUnavailable, tc.TachibanaSessionExpired, tc.TachibanaLoginRejected):
        assert issubclass(t, tc.TachibanaApiError)
    assert not issubclass(tc.TachibanaSessionExpired, tc.TachibanaUnavailable)
    assert not issubclass(tc.TachibanaLoginRejected, tc.TachibanaUnavailable)
    assert not issubclass(tc.TachibanaAuthError, tc.TachibanaApiError)
    assert tc.SESSION_EXPIRED_CODES == frozenset({"2"})
    assert tc.SERVICE_OFFLINE_CODES == frozenset({"9", "-12"})
    assert tc.BUSY_CODES == frozenset({"-2", "-3"})
    assert tc.OUTSIDE_HOURS_CODES == frozenset({"-62"})
    assert tc.LOGIN_ERROR_CODES == frozenset({"-50"})


@pytest.mark.parametrize(
    ("doc", "kind", "text"),
    [
        ({"p_errno": "-1", "p_err": "server reason"}, "p_errno", "server reason"),
        (
            {"p_errno": "0", "p_err": "", "sResultCode": "10031", "sResultText": "server text"},
            "sResultCode",
            "server text",
        ),
        ({"p_errno": "-1"}, "p_errno", None),
    ],
)
def test_api_error_keeps_kind_and_text(doc, kind, text):
    client, _, _ = logged_in({tc.CLMID_OPTION_MASTER: body(doc)})
    with pytest.raises(tc.TachibanaApiError) as e:
        client.get_option_master(timeout=10)
    assert e.value.kind == kind
    assert e.value.text == text
    if text is not None:
        assert text not in str(e.value)
        assert text not in repr(e.value.args)


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


def test_login_error_is_login_rejected():
    client, http, _ = make_client({tc.CLMID_LOGIN: body({"p_errno": "0", "sResultCode": "10031", "sResultText": "x"})})
    with pytest.raises(tc.TachibanaApiError) as e:
        client.login(timeout=10)
    assert type(e.value) is tc.TachibanaLoginRejected
    assert e.value.kind == "sResultCode" and e.value.code == "10031"
    assert not client.logged_in
    assert len(http.calls) == 1  # 自動で再試行しない（間違いが続くとロックされる）


def test_session_expired_drops_session():
    client, http, _ = logged_in({tc.CLMID_MARKET_PRICE: body({"p_errno": "2", "p_err": "session"})})
    with pytest.raises(tc.TachibanaSessionExpired) as e:
        client.get_market_price(CODES_241, ["pDPP"], timeout=10)
    assert e.value.code == "2"
    assert e.value.text == "session"
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


def unread_ack(flag: str = "1", urls: str = ""):
    ack = fixture_json("login_ack.json")
    return {**ack, "sKinsyouhouMidokuFlg": flag, **dict.fromkeys(tc.VIRTUAL_URL_FIELDS, urls)}


def test_unread_documents_without_virtual_urls_is_documents_unread():
    # 交付書面が未読（sKinsyouhouMidokuFlg "1"）だと仮想 URL は "" になる。原因の分かる型にする
    client, http, _ = make_client({tc.CLMID_LOGIN: ok(unread_ack())})
    with pytest.raises(tc.TachibanaAuthError) as e:
        client.login(timeout=10)
    assert type(e.value) is tc.TachibanaDocumentsUnread
    assert str(e.value) == f"{tc.CLMID_LOGIN}: unread documents"
    assert e.value.clmid == tc.CLMID_LOGIN
    assert not client.logged_in
    assert len(http.calls) == 1


def test_unread_documents_with_virtual_urls_still_logs_in():
    ack = {**fixture_json("login_ack.json"), "sKinsyouhouMidokuFlg": "1"}
    client, _, _ = make_client({tc.CLMID_LOGIN: ok(ack)})
    session = client.login(timeout=10)
    assert client.logged_in and session.price_url == VIRTUAL_URLS["price"]


def test_missing_virtual_url_without_unread_flag_is_plain_auth_error():
    client, _, _ = make_client({tc.CLMID_LOGIN: ok(unread_ack(flag="0"))})
    with pytest.raises(tc.TachibanaAuthError) as e:
        client.login(timeout=10)
    assert type(e.value) is tc.TachibanaAuthError
    assert "missing sUrlRequest" in str(e.value)


def test_decrypt_failure_is_auth_error():
    client, _, _ = make_client({tc.CLMID_LOGIN: ok({**fixture_json("login_ack.json"), "sUrlMaster": "BAD:master"})})
    with pytest.raises(tc.TachibanaAuthError) as e:
        client.login(timeout=10)
    assert str(e.value) == "decrypt failed: sUrlMaster"
    assert e.value.__context__ is None and e.value.__cause__ is None
    assert not client.logged_in


def decrypting_to(name: str, url: str):
    """ログインの応答の name の仮想 URL だけを url に復号するクライアント"""
    client, _, _ = make_client()
    original = client._decrypt
    kind = fixture_json("login_ack.json")[name]
    client._decrypt = lambda enc: url if enc == kind else original(enc)
    return client


@pytest.mark.parametrize(
    ("name", "url"),
    [("sUrlPrice", "http://example.invalid/plain/"), ("sUrlRequest", "wss://example.invalid/ws/"), ("sUrlMaster", "")],
)
def test_decrypted_url_must_be_https(name, url):
    client = decrypting_to(name, url)
    with pytest.raises(tc.TachibanaAuthError) as e:
        client.login(timeout=10)
    assert "scheme" in str(e.value)
    assert not client.logged_in


@pytest.mark.parametrize(
    ("name", "url"),
    [
        ("sUrlPrice", "https://example.invalid/vurl/SECRET-price-5e6f"),
        ("sUrlMaster", "https://example.invalid/vurl/SECRET-master-5e6f/?x"),
        ("sUrlEventWebSocket", "wss://example.invalid/vurl/SECRET-event_ws-5e6f\r\n"),
    ],
)
def test_decrypted_url_must_end_with_slash(name, url):
    client = decrypting_to(name, url)
    with pytest.raises(tc.TachibanaAuthError) as e:
        client.login(timeout=10)
    assert str(e.value) == f"{name}: unexpected form"
    assert "SECRET-" not in str(e.value)
    assert not client.logged_in


def test_decrypted_url_trailing_newline_is_dropped():
    client = decrypting_to("sUrlPrice", VIRTUAL_URLS["price"] + "\r\n")
    session = client.login(timeout=10)
    assert session.price_url == VIRTUAL_URLS["price"]
    assert all("\r" not in u and "\n" not in u for u in (session.price_url, session.master_url, session.request_url))


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


def test_market_price_response_without_result_code_is_accepted():
    # 時価の応答には sResultCode が無い（資料の例）。共通項目（p_errno）だけで検査する
    client, http, _ = logged_in()
    prices = client.get_market_price(["900000001"], ["pDPP"], timeout=10)
    doc = json.loads(http.returned[-1].decode("cp932"))
    assert "sResultCode" not in doc and doc["p_errno"] == "0"
    assert len(prices.items) == 1


def test_date_info_items():
    client, _, _ = logged_in()
    info = client.get_date_info(timeout=10)
    assert info.items[0]["sTheDay"] == "20310303"


# --- 純粋な補助関数 ---


def test_format_sd_date():
    assert tc.format_sd_date(datetime(2031, 3, 2, 15, 4, 5, 678900, tzinfo=UTC)) == "2031.03.03-00:04:05.678"
    with pytest.raises(ValueError):
        tc.format_sd_date(datetime(2031, 3, 3))


def test_build_body_and_chunks():
    body = tc.build_body({"a": "1,2", "b": "日経"})
    assert isinstance(body, bytes)
    assert body == '{"a":"1,2","b":"日経"}'.encode("cp932")
    assert tc.build_body({"b": "日経"}, "utf-8") == '{"b":"日経"}'.encode()
    assert tc.chunks([str(i) for i in range(5)], 2) == [["0", "1"], ["2", "3"], ["4"]]
    assert tc.chunks([]) == []


def test_build_body_rejects_unencodable_without_value_in_message():
    with pytest.raises(ValueError) as e:
        tc.build_body({"a": "SECRET-\U0001f600"}, "cp932")
    assert "SECRET-" not in str(e.value)
    assert e.value.__context__ is None and e.value.__cause__ is None


def test_split_timeout():
    assert tc.split_timeout(10) == (3.0, 7.0)
    assert tc.split_timeout(2) == (1.0, 1.0)


# --- 既定の HTTP（requests を差し替える。通信なし） ---


class FakeRequestsResponse:
    status_code = 200
    content = b'{"p_errno":"0"}'


def test_requests_post_returns_response(monkeypatch):
    seen = {}

    def fake_post(self, url, data=None, headers=None, timeout=None, allow_redirects=True):
        seen.update(url=url, data=data, headers=headers, timeout=timeout, allow_redirects=allow_redirects)
        return FakeRequestsResponse()

    monkeypatch.setattr(requests.Session, "post", fake_post)
    response = tc.requests_post("https://example.invalid/x/", b'{"p_no":"1"}', 5)
    assert response == tc.HttpResponse(200, b'{"p_errno":"0"}')
    assert seen["url"] == "https://example.invalid/x/"
    assert seen["data"] == b'{"p_no":"1"}'
    assert seen["headers"] == {"Content-Type": "application/json"}
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
def test_requests_post_wraps_errors(monkeypatch, error, expected):
    def fake_post(self, url, data=None, headers=None, timeout=None, allow_redirects=True):
        raise error(f"failed for {url}")

    monkeypatch.setattr(requests.Session, "post", fake_post)
    with pytest.raises(expected) as e:
        tc.requests_post(VIRTUAL_URLS["price"], b"{}", 5)
    assert type(e.value) is expected
    assert e.value.reason == error.__name__
    assert "SECRET-" not in str(e.value)
    assert e.value.__context__ is None and e.value.__cause__ is None


def test_requests_error_reason_reaches_client(monkeypatch):
    def fake_post(self, url, data=None, headers=None, timeout=None, allow_redirects=True):
        raise requests.ConnectionError(f"failed for {url}")

    monkeypatch.setattr(requests.Session, "post", fake_post)
    clock = FakeClock()
    client = tc.TachibanaClient(
        BASE_URL, credentials(), fake_decrypt, now=clock.now, monotonic=clock.monotonic, sleep=clock.sleep
    )
    with pytest.raises(tc.TachibanaHttpError) as e:
        client.login(timeout=10)
    assert str(e.value) == f"{tc.CLMID_LOGIN} p_no=1: ConnectionError"

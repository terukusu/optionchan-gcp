"""秘密（認証 ID・秘密鍵・復号した仮想 URL）が repr・str・例外・ログ・標準出力に出ないこと（AC 6）。"""

import logging
import traceback

import pytest
import requests
from tachibana_fakes import (
    BASE_URL,
    SECRETS,
    VIRTUAL_URLS,
    all_text,
    credentials,
    fixture_json,
    json_body,
    make_client,
    market_price_handler,
    ok,
)
from test_tachibana_client import CODES_241, ERROR_CASES

from optionchan_ingest.sources.tachibana import client as tc


def assert_no_secret(text: str) -> None:
    for secret in SECRETS:
        assert secret not in text


def exception_chain(e: BaseException | None, seen: set[int] | None = None) -> list[BaseException]:
    """e と、__cause__ / __context__ を再帰でたどった例外のすべて"""
    seen = set() if seen is None else seen
    if e is None or id(e) in seen:
        return []
    seen.add(id(e))
    return [e, *exception_chain(e.__cause__, seen), *exception_chain(e.__context__, seen)]


def exception_text(e: BaseException) -> str:
    """例外の連鎖のすべての文・repr・args・属性と traceback の全文"""
    parts = ["".join(traceback.format_exception(e))]
    for x in exception_chain(e):
        parts += [str(x), repr(x), repr(x.args), repr(vars(x))]
    return "\n".join(parts)


def run_scenario(monkeypatch) -> list[BaseException]:
    """正常の呼び出しと、起こりうる失敗をひととおり起こし、投げられた例外を集める"""
    errors: list[BaseException] = []

    def catch(call):
        try:
            call()
        except tc.TachibanaError as e:
            errors.append(e)

    client, http, _ = make_client()
    session = client.login(timeout=10)
    # 検索が効くことの確かめ: 要求の本文には秘密（認証 ID）が入る（本文は外に出さないもの）。URL には入らない
    assert any(SECRETS[0] in body.decode("cp932") for body in http.bodies)
    assert all(SECRETS[0] not in url for url, _ in http.calls)
    prices = client.get_market_price(CODES_241, ["pDPP"], timeout=30)
    master = client.get_option_master(timeout=10)
    assert_no_secret(all_text(client, session, prices, master, *prices.raw, master.raw, credentials()))
    for raw in (*prices.raw, master.raw):
        assert_no_secret(raw.body.decode("cp932"))

    for _, route, _, _ in ERROR_CASES:
        http.routes[tc.CLMID_MARKET_PRICE] = route
        catch(lambda: client.get_market_price(["900000001"], ["pDPP"], timeout=10))
    # 停止中・混雑・時間外・ログインエラー（p_errno の表）
    for code in ("9", "-2", "-62", "-50"):
        http.routes[tc.CLMID_MARKET_PRICE] = tc.HttpResponse(200, json_body({"p_errno": code, "p_err": "reason"}))
        catch(lambda: client.get_market_price(["900000001"], ["pDPP"], timeout=10))
    http.routes[tc.CLMID_MARKET_PRICE] = market_price_handler

    # 予算切れ
    client2, _, _ = make_client(min_interval=0.2)
    client2.login(timeout=10)
    catch(lambda: client2.get_market_price(CODES_241, ["pDPP"], timeout=0.3))

    # セッション切れ（p_errno 2）→ 未ログイン
    http.routes[tc.CLMID_OPTION_MASTER] = tc.HttpResponse(200, b'{"p_errno": "2", "p_err": "expired"}')
    catch(lambda: client.get_option_master(timeout=10))
    catch(lambda: client.get_option_master(timeout=10))

    # ログインの失敗（拒否・復号の例外の文に秘密鍵を含む・平文の URL・/ で終わらない URL・仮想 URL の欠け・書面の未読）
    rejected, _, _ = make_client({tc.CLMID_LOGIN: ok({"sResultCode": "10031", "sResultText": "rejected"})})
    catch(lambda: rejected.login(timeout=10))
    ack = fixture_json("login_ack.json")
    unread = {**ack, "sKinsyouhouMidokuFlg": "1", **dict.fromkeys(tc.VIRTUAL_URL_FIELDS, "")}
    for bad in ({**ack, "sUrlPrice": "BAD:price"}, {k: v for k, v in ack.items() if k != "sUrlMaster"}, unread):
        failing, _, _ = make_client({tc.CLMID_LOGIN: ok(bad)})
        catch(lambda failing=failing: failing.login(timeout=10))
    for url in (VIRTUAL_URLS["price"].replace("https", "http"), VIRTUAL_URLS["price"].rstrip("/")):
        plain, _, _ = make_client()
        plain._decrypt = lambda enc, url=url: url
        catch(lambda plain=plain: plain.login(timeout=10))

    # 既定の HTTP（requests の例外の文は URL を含む）
    def fake_post(self, url, data=None, headers=None, timeout=None, allow_redirects=True):
        raise requests.ConnectionError(f"Max retries exceeded with url: {url}")

    monkeypatch.setattr(requests.Session, "post", fake_post)
    real = tc.TachibanaClient(BASE_URL, credentials(), str)
    catch(lambda: real.login(timeout=10))
    catch(lambda: tc.requests_post(VIRTUAL_URLS["price"], b"{}", 5))

    client2.logout(timeout=10)
    return errors


def test_secrets_do_not_leak(monkeypatch, caplog, capsys):
    caplog.set_level(logging.DEBUG)
    errors = run_scenario(monkeypatch)
    kinds = {type(e) for e in errors}
    assert {
        tc.TachibanaApiError,
        tc.TachibanaHttpError,
        tc.TachibanaTimeout,
        tc.TachibanaResponseError,
        tc.TachibanaSessionExpired,
        tc.TachibanaServiceOffline,
        tc.TachibanaBusy,
        tc.TachibanaOutsideHours,
        tc.TachibanaLoginRejected,
        tc.TachibanaAuthError,
        tc.TachibanaDocumentsUnread,
        tc.TachibanaNotLoggedIn,
    } <= kinds
    for e in errors:
        assert_no_secret(exception_text(e))
        assert exception_chain(e) == [e]  # 下の層の例外を連鎖に残さない
    assert_no_secret(caplog.text)
    out = capsys.readouterr()
    assert_no_secret(out.out + out.err)


def test_requests_error_message_with_url_is_suppressed(monkeypatch):
    def fake_post(self, url, data=None, headers=None, timeout=None, allow_redirects=True):
        raise requests.ConnectionError(f"Max retries exceeded with url: {url}")

    monkeypatch.setattr(requests.Session, "post", fake_post)
    with pytest.raises(tc.TachibanaHttpError) as e:
        tc.requests_post(VIRTUAL_URLS["price"], b"{}", 5)
    assert "Max retries" not in exception_text(e.value)
    assert e.value.__context__ is None and e.value.__cause__ is None


def test_chain_walk_finds_context_left_by_from_none():
    # 対照: except の中で from None で投げると表示は抑えられても __context__ に元の例外（URL）が残り、検索で見つかる
    try:
        try:
            raise ValueError(VIRTUAL_URLS["price"])
        except ValueError:
            raise tc.TachibanaHttpError("wrapped") from None
    except tc.TachibanaHttpError as e:
        caught = e
    assert "SECRET-" not in "".join(traceback.format_exception(caught))
    assert "SECRET-" in exception_text(caught)

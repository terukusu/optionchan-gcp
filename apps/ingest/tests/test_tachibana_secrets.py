"""秘密（認証 ID・パスワード・秘密鍵・復号した仮想 URL）が repr・str・例外・ログ・標準出力に出ないこと（AC 6）。"""

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

    client, http, _ = make_client(password="test-password-SECRET-7a8b")
    session = client.login(timeout=10)
    # 検索が効くことの確かめ: 要求の URL には秘密が入る（URL は外に出さないもの）
    assert any(SECRETS[0] in url for url, _ in http.calls)
    prices = client.get_market_price(CODES_241, ["pDPP"], timeout=30)
    master = client.get_option_master(timeout=10)
    assert_no_secret(all_text(client, session, prices, master, *prices.raw, master.raw, credentials()))
    for raw in (*prices.raw, master.raw):
        assert_no_secret(raw.body.decode("cp932"))

    for _, route, _, _ in ERROR_CASES:
        http.routes[tc.CLMID_MARKET_PRICE] = route
        catch(lambda: client.get_market_price(["900000001"], ["pDPP"], timeout=10))
    http.routes[tc.CLMID_MARKET_PRICE] = market_price_handler

    # 予算切れ
    client2, _, _ = make_client(min_interval=0.2)
    client2.login(timeout=10)
    catch(lambda: client2.get_market_price(CODES_241, ["pDPP"], timeout=0.3))

    # セッション切れ → 未ログイン
    monkeypatch.setattr(tc, "SESSION_EXPIRED_CODES", frozenset({"99"}))
    http.routes[tc.CLMID_OPTION_MASTER] = tc.HttpResponse(200, b'{"p_errno": "99"}')
    catch(lambda: client.get_option_master(timeout=10))
    catch(lambda: client.get_option_master(timeout=10))

    # ログインの失敗（復号の例外の文に秘密鍵を含む・平文の URL・仮想 URL の欠け）
    ack = fixture_json("login_ack.json")
    for bad in ({**ack, "sUrlPrice": "BAD:price"}, {k: v for k, v in ack.items() if k != "sUrlMaster"}):
        failing, _, _ = make_client({tc.CLMID_LOGIN: ok(bad)})
        catch(lambda failing=failing: failing.login(timeout=10))
    plain, _, _ = make_client()
    plain._decrypt = lambda enc: VIRTUAL_URLS["price"].replace("https", "http")
    catch(lambda: plain.login(timeout=10))

    # 既定の HTTP（requests の例外の文は URL を含む）
    def fake_get(self, url, timeout, allow_redirects=True):
        raise requests.ConnectionError(f"Max retries exceeded with url: {url}")

    monkeypatch.setattr(requests.Session, "get", fake_get)
    real = tc.TachibanaClient(BASE_URL, credentials(), str)
    catch(lambda: real.login(timeout=10))
    catch(lambda: tc.requests_get(VIRTUAL_URLS["price"] + "?x", 5))

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
        tc.TachibanaAuthError,
        tc.TachibanaNotLoggedIn,
    } <= kinds
    for e in errors:
        assert_no_secret(exception_text(e))
        assert exception_chain(e) == [e]  # 下の層の例外を連鎖に残さない
    assert_no_secret(caplog.text)
    out = capsys.readouterr()
    assert_no_secret(out.out + out.err)


def test_requests_error_message_with_url_is_suppressed(monkeypatch):
    def fake_get(self, url, timeout, allow_redirects=True):
        raise requests.ConnectionError(f"Max retries exceeded with url: {url}")

    monkeypatch.setattr(requests.Session, "get", fake_get)
    with pytest.raises(tc.TachibanaHttpError) as e:
        tc.requests_get(VIRTUAL_URLS["price"] + "?x", 5)
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

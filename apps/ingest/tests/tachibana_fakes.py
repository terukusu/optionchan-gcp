"""立花の部品のテスト用の偽の依存（HTTP・時計）と合成の値。

値はすべて架空（公開資料の項目名の形だけ。資料の例の値は写さない）。接続先は example.invalid で、実際の通信はしない。
秘密に見立てた文字列には SECRET- を入れ、外に出ていないことを文字列の検索で確かめる。
"""

import json
from datetime import UTC, datetime, timedelta

from conftest import FIXTURES

from optionchan_ingest.sources.tachibana import client as tc
from optionchan_ingest.sources.tachibana.credentials import TachibanaCredentials

BASE_URL = "https://example.invalid/e_api_v4r10/"
AUTH_ID = "test-auth-id-SECRET-1a2b"
PRIVATE_KEY = "-----BEGIN TEST KEY----- SECRET-3c4d"
URL_KINDS = {
    "sUrlRequest": "request",
    "sUrlMaster": "master",
    "sUrlPrice": "price",
    "sUrlEvent": "event",
    "sUrlEventWebSocket": "event_ws",
}
VIRTUAL_URLS = {
    kind: f"{'wss' if kind == 'event_ws' else 'https'}://example.invalid/vurl/SECRET-{kind}-5e6f/"
    for kind in URL_KINDS.values()
}
SECRETS = (AUTH_ID, PRIVATE_KEY, *VIRTUAL_URLS.values(), "SECRET-")

# 偽の時計の始まり（2031-03-03 08:00 JST）
START = datetime(2031, 3, 2, 23, 0, tzinfo=UTC)

FIXTURE_DIR = FIXTURES / "tachibana"


def fixture_json(name: str) -> dict:
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


def fake_decrypt(encrypted: str) -> str:
    """ "ENC:<種別>" → 復号した仮想 URL（合成）"""
    prefix, _, kind = encrypted.partition(":")
    if prefix != "ENC" or kind not in VIRTUAL_URLS:
        raise ValueError(f"cannot decrypt {encrypted} with {PRIVATE_KEY}")
    return VIRTUAL_URLS[kind]


def credentials() -> TachibanaCredentials:
    return TachibanaCredentials(auth_id=AUTH_ID, private_key=PRIVATE_KEY)


def json_body(obj, encoding: str = "cp932") -> bytes:
    return json.dumps(obj, ensure_ascii=False).encode(encoding)


def ok(obj: dict | None = None, encoding: str = "cp932") -> tc.HttpResponse:
    """ログイン・ログアウトの応答（共通項目と sResultCode を持つ）"""
    return tc.HttpResponse(200, json_body({"p_errno": "0", "p_err": "", "sResultCode": "0", **(obj or {})}, encoding))


DEFAULT_MARKET_VALUES = {
    "pDPP": "12",
    "tDPP:T": "07:59",
    "pDYWP": "+3",
    "pDYRP": "33.33",
    "pQBP": "11",
    "pBV": "5",
    "pQAP": "13",
    "pAV": "7",
    "pDV": "100",
}


def market_item(code: str, values: dict[str, str] | None = None) -> dict[str, str]:
    """aCLMMfdsMarketPrice の 1 要素（値は架空）"""
    return {"sIssueCode": code, **(DEFAULT_MARKET_VALUES if values is None else values)}


def payload_of(body: bytes, encoding: str = "cp932") -> dict:
    """要求の本文（JSON）→ 要求の dict"""
    return json.loads(body.decode(encoding))


def market_price_handler(payload: dict) -> tc.HttpResponse:
    """要求した銘柄を全部返す時価の偽の応答（共通項目を返し、sResultCode は無い。資料の例の形）"""
    codes = payload["sTargetIssueCode"].split(",")
    doc = {
        "p_no": payload["p_no"],
        "p_sd_date": payload["p_sd_date"],
        "p_rv_date": payload["p_sd_date"],
        "p_errno": "0",
        "p_err": "",
        "sCLMID": tc.CLMID_MARKET_PRICE,
        "aCLMMfdsMarketPrice": [market_item(c) for c in codes],
    }
    return tc.HttpResponse(200, json_body(doc))


class FakeClock:
    """now()・monotonic()・sleep() が同じ偽の時刻（START からの秒）を進める"""

    def __init__(self, start: datetime = START):
        self.start = start
        self.t = 0.0
        self.sleeps: list[float] = []

    def now(self) -> datetime:
        return self.start + timedelta(seconds=self.t)

    def monotonic(self) -> float:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.t += seconds


class FakeHttp:
    """要求の sCLMID ごとに用意した応答を返す偽の HttpPost。

    routes の値は HttpResponse、例外（投げる）、payload → HttpResponse の関数のどれか。呼ばれた (url, timeout)・
    本文と送った偽の時刻を記録する。cost は 1 要求で進める偽の時刻（秒）
    """

    def __init__(self, clock: FakeClock, routes: dict | None = None, cost: float = 0.0):
        self.clock = clock
        self.cost = cost
        self.routes = {
            tc.CLMID_LOGIN: ok(fixture_json("login_ack.json")),
            tc.CLMID_LOGOUT: ok({"sCLMID": "CLMAuthLogoutAck"}),
            tc.CLMID_OPTION_MASTER: tc.HttpResponse(200, json_body(fixture_json("master_op.json"))),
            tc.CLMID_FUTURE_MASTER: tc.HttpResponse(200, json_body(fixture_json("master_sak.json"))),
            tc.CLMID_DATE_INFO: tc.HttpResponse(200, json_body(fixture_json("date_info.json"))),
            tc.CLMID_MARKET_PRICE: market_price_handler,
            **(routes or {}),
        }
        self.calls: list[tuple[str, float]] = []
        self.bodies: list[bytes] = []
        self.sent_at: list[float] = []
        self.returned: list[bytes] = []

    def __call__(self, url: str, body: bytes, timeout: float) -> tc.HttpResponse:
        self.calls.append((url, timeout))
        self.bodies.append(body)
        self.sent_at.append(self.clock.t)
        self.clock.t += self.cost
        payload = payload_of(body)
        route = self.routes[payload["sCLMID"]]
        if isinstance(route, BaseException):
            raise route
        response = route(payload) if callable(route) else route
        self.returned.append(response.body)
        return response

    def targets(self) -> list[str]:
        return [url for url, _ in self.calls]

    def calls_to(self, target: str) -> list[tuple[str, float]]:
        return [c for c in self.calls if c[0] == target]

    def payloads(self) -> list[dict]:
        return [payload_of(body) for body in self.bodies]


def make_client(routes: dict | None = None, *, cost: float = 0.0, **kwargs):
    """偽の HTTP・時計のクライアント → (client, http, clock)"""
    clock = FakeClock()
    http = FakeHttp(clock, routes, cost)
    client = tc.TachibanaClient(
        BASE_URL,
        credentials(),
        fake_decrypt,
        **{"http": http, "now": clock.now, "monotonic": clock.monotonic, "sleep": clock.sleep, **kwargs},
    )
    return client, http, clock


def all_text(*values) -> str:
    """repr・str をつないだ文字列（秘密の検索用）"""
    return "\n".join(f"{v!r}\n{v!s}" for v in values)

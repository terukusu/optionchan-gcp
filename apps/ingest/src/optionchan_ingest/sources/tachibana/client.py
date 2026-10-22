"""立花証券 e 支店 API（e_api v4r10）のクライアント。

HTTP の送信・時計・待ちは差し込み（テストは偽物を渡す。既定の HTTP は requests）。要求は資料の例のとおり GET で、
URL は「宛先?<要求の JSON を URL エンコードしたもの>」。

- ログイン（<base>auth/）→ 応答の暗号化された仮想 URL 5 種を復号の関数（credentials.Decryptor）で復号 → Session
- 時価（CLMMfdsGetMarketPrice、sUrlPrice）は銘柄コードを 120 件ずつに分けて要求し、応答を 1 つにまとめる
- マスタ（CLMStkGetIssueMstOp・CLMStkGetIssueMstSak・CLMStkGetDateZyouhou、sUrlMaster）
- ログアウト（CLMAuthLogoutRequest、sUrlRequest）

要求通番 p_no はログインの要求が 1 で、1 回のログインの間は 1 ずつ増える。p_no の払い出し・間隔の待ち・送信は
1 つのロックの中で行う（pipeline.bounded が待つのをやめた呼び出しが後から送って、番号が逆転しないように）。
外に出る公開メソッドは timeout（秒、キーワード）を受け、メソッド全体の予算にする（分割した要求と間隔の待ちを含む）。

自動で再ログインしない（再認証すると前の仮想 URL が無効になる。いつログインし直すかは呼ぶ側が決める）。
セッション切れ（SESSION_EXPIRED_CODES）の応答ではセッションを捨て、以後の呼び出しは TachibanaNotLoggedIn にする。

秘密（認証 ID・仮想 URL）は repr・例外の文に出さない。例外の文に入れるのは機能名（sCLMID）・p_no・コード・
例外の型名だけ（URL・応答の本文・sResultText は入れない。p_errno / sResultCode は短い数字の形のときだけ）。
下の層の例外（requests の例外の文は URL を含む）は except の中では raise せず、型名だけを取って except を抜けてから
新しい例外を投げる。こうすると __context__ / __cause__ が None になり、例外の連鎖をたどっても元の文に届かない。
リダイレクトには従わない（検査していない宛先に送らない。3xx は TachibanaHttpError）。この部品はログを出さない。

制限: 復号した仮想 URL は先頭の scheme だけを確かめる。build_url は「宛先?…」とつなぐので、? や # を含む・
/ で終わらない URL だと壊れた要求になる。復号後の形は未確認なので、段 2 で確かめてから検査を足す。
"""

import codecs
import json
import re
import threading
import time
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from urllib.parse import quote
from zoneinfo import ZoneInfo

import requests

from .credentials import Decryptor, TachibanaCredentials

JST = ZoneInfo("Asia/Tokyo")

# 接続先（資料の記載）。テストは example.invalid を渡す
PROD_BASE_URL = "https://kabuka.e-shiten.jp/e_api_v4r10/"
DEMO_BASE_URL = "https://demo-kabuka.e-shiten.jp/e_api_v4r10/"
AUTH_PATH = "auth/"

CLMID_LOGIN = "CLMAuthLoginRequest"
CLMID_LOGOUT = "CLMAuthLogoutRequest"
CLMID_MARKET_PRICE = "CLMMfdsGetMarketPrice"
CLMID_OPTION_MASTER = "CLMStkGetIssueMstOp"
CLMID_FUTURE_MASTER = "CLMStkGetIssueMstSak"
CLMID_DATE_INFO = "CLMStkGetDateZyouhou"

# 要求の機能名 → 応答の配列のキー（資料の例。CLMStkGetDateZyouhou の応答の sCLMID は CLMDateZyouhou で名前が違う）
RESPONSE_ARRAYS = {
    CLMID_MARKET_PRICE: "aCLMMfdsMarketPrice",
    CLMID_OPTION_MASTER: "aCLMStkIssueMstOp",
    CLMID_FUTURE_MASTER: "aCLMStkIssueMstSak",
    CLMID_DATE_INFO: "aCLMDateZyouhou",
}

# ログインの応答の仮想 URL（暗号化された値）の項目
VIRTUAL_URL_FIELDS = ("sUrlRequest", "sUrlMaster", "sUrlPrice", "sUrlEvent", "sUrlEventWebSocket")
# 復号した仮想 URL に許す先頭。平文の宛先へ送らない。WebSocket の URL は wss:// と推定（未確認）
# 先頭だけを確かめる（? や # を含まない・/ で終わる、などの形は未確認なので見ない。段 2 で足す）
VIRTUAL_URL_SCHEMES = {name: ("https://",) for name in VIRTUAL_URL_FIELDS} | {
    "sUrlEventWebSocket": ("https://", "wss://")
}

# ログインのパスワードの項目名。未確認（v4r10 のリファレンスに見当たらない。旧版の名前。password を渡したときだけ送る）
LOGIN_PASSWORD_FIELD = "sPassword"

# 時価の 1 要求の銘柄数の上限（資料の記載）
MAX_ISSUES_PER_REQUEST = 120
# 要求と要求の間の下限（秒）。要求の受付は秒 10 件までと読める（文字化けのため数値は未確認）
MIN_INTERVAL_SECONDS = 0.1
# 応答の文字コード。未確認。資料のページが Shift_JIS 由来で、銘柄名に日本語があるため cp932 にした
# （cp932 は ASCII を含むので、JSON の構文は utf-8 と同じに読める）
DEFAULT_ENCODING = "cp932"
# セッション切れ・仮想 URL 無効を表す p_errno / sResultCode。未確認（資料で値を見つけられなかった）。
# 口座の応答で確かめて足す。空のあいだはどのコードも TachibanaApiError のまま
SESSION_EXPIRED_CODES: frozenset[str] = frozenset()
# 既定の HTTP の接続タイムアウトの上限（秒）。残りは読み取りに回す
CONNECT_TIMEOUT_CAP_SECONDS = 3.0

_REASON = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
# 例外の文に入れてよい p_errno / sResultCode の形（短い数字。それ以外は "?" にして、属性 code にだけ持つ）
_CODE = re.compile(r"^-?\d{1,10}$")


class TachibanaError(Exception):
    """立花の API の失敗の基底。clmid は機能名、code は HTTP の状態・p_errno・sResultCode（無ければ None）"""

    def __init__(self, message: str, *, clmid: str | None = None, code: str | None = None):
        super().__init__(message)
        self.clmid = clmid
        self.code = code


class TachibanaHttpError(TachibanaError):
    """HTTP の失敗（状態が 400 以上、接続の失敗）。code は状態の数字の文字列か None"""

    def __init__(self, message: str, *, clmid: str | None = None, code: str | None = None, reason: str | None = None):
        super().__init__(message, clmid=clmid, code=code)
        self.reason = reason  # 下の層の例外の型名（requests.ConnectionError なら "ConnectionError"）


class TachibanaTimeout(TachibanaHttpError):
    """接続・読み取りのタイムアウト、またはメソッドの予算（timeout）切れ"""


class TachibanaResponseError(TachibanaError):
    """応答の形の誤り（文字コードで読めない・JSON でない・object でない・配列が無い）"""


class TachibanaApiError(TachibanaError):
    """p_errno / sResultCode が 0 以外。kind はどちらの項目か、
    text は sResultText（サーバーの文。例外の文には入れない）"""

    def __init__(self, message: str, *, clmid: str, code: str, kind: str, text: str | None):
        super().__init__(message, clmid=clmid, code=code)
        self.kind = kind
        self.text = text


class TachibanaSessionExpired(TachibanaApiError):
    """セッション切れ・仮想 URL 無効（コードが SESSION_EXPIRED_CODES）。クライアントはセッションを捨てる"""


class TachibanaAuthError(TachibanaError):
    """ログインの応答に仮想 URL が無い、復号の失敗、復号した URL の scheme が VIRTUAL_URL_SCHEMES に無い"""


class TachibanaNotLoggedIn(TachibanaError):
    """セッションが無いのに呼んだ（ログインの前、ログアウト・セッション切れの後）"""


@dataclass(frozen=True)
class HttpResponse:
    status: int
    body: bytes


# (url, timeout 秒) → 応答。失敗は TachibanaHttpError / TachibanaTimeout（文に URL を入れない）
HttpGet = Callable[[str, float], HttpResponse]


def split_timeout(timeout: float, connect_cap: float = CONNECT_TIMEOUT_CAP_SECONDS) -> tuple[float, float]:
    """timeout を (接続, 読み取り) に分ける。合計は timeout"""
    connect = min(connect_cap, timeout / 2)
    return connect, timeout - connect


def requests_get(url: str, timeout: float) -> HttpResponse:
    """既定の HTTP。要求ごとに requests.Session を作って閉じる（io_jpx と同じ取り方）。リダイレクトには従わない。

    requests.Timeout → TachibanaTimeout、ほかの requests.RequestException → TachibanaHttpError。
    requests の例外の文は URL（認証 ID・仮想 URL を含む）を含むので、型名だけを取り、except を抜けてから投げる
    （__context__ に元の例外を残さない）
    """
    connect, read = split_timeout(timeout)
    error: type[TachibanaHttpError]
    try:
        with requests.Session() as session:
            response = session.get(url, timeout=(connect, read), allow_redirects=False)
            return HttpResponse(status=response.status_code, body=response.content)
    except requests.Timeout as e:
        error, reason = TachibanaTimeout, type(e).__name__
    except requests.RequestException as e:
        error, reason = TachibanaHttpError, type(e).__name__
    raise error(f"http: {reason}", reason=reason)


@dataclass(frozen=True)
class RawResponse:
    """生の応答（段 1a の生データの保存・再処理に使う）。要求の URL（秘密を含む）は持たない"""

    clmid: str
    p_no: int
    sent_at: datetime  # p_sd_date に入れた時刻（aware）
    http_status: int
    body: bytes


@dataclass(frozen=True)
class Session:
    """ログインで得た仮想 URL（復号したもの）。repr・str に URL を出さない"""

    logged_in_at: datetime
    request_url: str = field(repr=False)
    master_url: str = field(repr=False)
    price_url: str = field(repr=False)
    event_url: str = field(repr=False)
    event_ws_url: str = field(repr=False)

    def __repr__(self) -> str:
        return f"Session(logged_in_at={self.logged_in_at.isoformat()})"

    __str__ = __repr__


@dataclass(frozen=True)
class MarketPrices:
    items: list[dict[str, str]]  # aCLMMfdsMarketPrice の要素を要求の順につないだもの
    raw: tuple[RawResponse, ...]


@dataclass(frozen=True)
class MasterData:
    items: list[dict[str, str]]  # RESPONSE_ARRAYS の配列
    raw: RawResponse


def _require_aware(dt: datetime, name: str) -> None:
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")


def format_sd_date(dt: datetime) -> str:
    """p_sd_date（JST の yyyy.mm.dd-hh:mn:ss.ttt）"""
    _require_aware(dt, "p_sd_date")
    local = dt.astimezone(JST)
    return local.strftime("%Y.%m.%d-%H:%M:%S.") + f"{local.microsecond // 1000:03d}"


def build_url(target: str, payload: dict) -> str:
    """宛先 + "?" + 要求の JSON の URL エンコード。

    英数字と _.-~ 以外（資料が挙げる # + / : = を含む）をすべて %XX にする。サーバーがどこまで受け付けるかは未確認
    なので、エンコードの規則はこの 1 か所にまとめる
    """
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    return f"{target}?{quote(text, safe='')}"


def chunks(codes: Sequence[str], size: int = MAX_ISSUES_PER_REQUEST) -> list[list[str]]:
    """銘柄コードを size 件ずつに分ける（順序はそのまま）"""
    if size < 1:
        raise ValueError("size must be positive")
    items = list(codes)
    return [items[i : i + size] for i in range(0, len(items), size)]


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _safe_reason(reason: str | None) -> str | None:
    # 差し込みの HTTP が理由に識別子以外（URL など）を入れても文に出さない
    return reason if reason is not None and _REASON.match(reason) else None


class TachibanaClient:
    def __init__(
        self,
        base_url: str,
        credentials: TachibanaCredentials,
        decrypt: Decryptor,
        *,
        http: HttpGet = requests_get,
        now: Callable[[], datetime] = _utc_now,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        min_interval: float = MIN_INTERVAL_SECONDS,
        encoding: str = DEFAULT_ENCODING,
    ):
        if not base_url.startswith("https://") or not base_url.endswith("/"):
            raise ValueError("base_url must start with https:// and end with /")
        if min_interval < 0:
            raise ValueError("min_interval must not be negative")
        codecs.lookup(encoding)  # 未知の文字コードは LookupError（作るときに分かるように）
        self._base_url = base_url
        self._credentials = credentials
        self._decrypt = decrypt
        self._http = http
        self._now = now
        self._monotonic = monotonic
        self._sleep = sleep
        self._min_interval = min_interval
        self._encoding = encoding
        self._lock = threading.Lock()
        self._session: Session | None = None
        self._p_no = 0
        self._last_sent: float | None = None  # 前回の送信の monotonic

    def __repr__(self) -> str:
        return f"TachibanaClient(logged_in={self.logged_in})"

    __str__ = __repr__

    @property
    def logged_in(self) -> bool:
        return self._session is not None

    # --- 公開メソッド（どれも timeout 秒をメソッド全体の予算にする） ---

    def login(self, *, timeout: float) -> Session:
        """ログインして仮想 URL を復号し、セッションを持つ。p_no を 1 に戻す（前のセッションは捨てる）"""
        end = self._deadline(timeout)
        with self._locked(end, CLMID_LOGIN):
            self._session = None
            self._p_no = 0  # ログインをまたいで前回以下を拒むかは未確認（拒むなら段 2 でここを直す）
        fields = {"sAuthId": self._credentials.auth_id}
        if self._credentials.password is not None:
            fields[LOGIN_PASSWORD_FIELD] = self._credentials.password
        doc, _ = self._request(self._base_url + AUTH_PATH, CLMID_LOGIN, fields, end)
        urls = {name: self._virtual_url(doc, name) for name in VIRTUAL_URL_FIELDS}
        session = Session(
            logged_in_at=self._now(),
            request_url=urls["sUrlRequest"],
            master_url=urls["sUrlMaster"],
            price_url=urls["sUrlPrice"],
            event_url=urls["sUrlEvent"],
            event_ws_url=urls["sUrlEventWebSocket"],
        )
        self._session = session  # 属性の代入だけ（ロックは要求の番号と送信を守るためのもの）
        return session

    def logout(self, *, timeout: float) -> None:
        """ログアウトする。成否によらずセッションを捨てる"""
        end = self._deadline(timeout)
        session = self._require_session(CLMID_LOGOUT)
        try:
            self._request(session.request_url, CLMID_LOGOUT, {}, end)
        finally:
            if self._session is session:
                self._session = None

    def get_market_price(self, codes: Sequence[str], columns: Sequence[str], *, timeout: float) -> MarketPrices:
        """時価。codes を MAX_ISSUES_PER_REQUEST 件ずつに分けて要求し、要素を要求の順につなぐ。

        sTargetIssueCode・sTargetColumn はカンマ区切り。予算の中で送れなくなったら TachibanaTimeout
        （それまでの応答は返さない）
        """
        codes = list(codes)
        columns = list(columns)
        if not codes or not columns:
            raise ValueError("codes and columns must not be empty")
        if any(not c or "," in c for c in codes + columns):
            raise ValueError("codes and columns must be non-empty and must not contain ','")
        end = self._deadline(timeout)
        items: list[dict[str, str]] = []
        raws: list[RawResponse] = []
        for chunk in chunks(codes):
            session = self._require_session(CLMID_MARKET_PRICE)
            fields = {"sTargetIssueCode": ",".join(chunk), "sTargetColumn": ",".join(columns)}
            doc, raw = self._request(session.price_url, CLMID_MARKET_PRICE, fields, end)
            items.extend(_array(doc, raw))
            raws.append(raw)
        return MarketPrices(items=items, raw=tuple(raws))

    def get_option_master(self, *, timeout: float) -> MasterData:
        return self._master(CLMID_OPTION_MASTER, timeout)

    def get_future_master(self, *, timeout: float) -> MasterData:
        return self._master(CLMID_FUTURE_MASTER, timeout)

    def get_date_info(self, *, timeout: float) -> MasterData:
        return self._master(CLMID_DATE_INFO, timeout)

    # --- 内部 ---

    def _deadline(self, timeout: float) -> float:
        return self._monotonic() + timeout

    def _require_session(self, clmid: str) -> Session:
        session = self._session
        if session is None:
            raise TachibanaNotLoggedIn(f"{clmid}: not logged in", clmid=clmid)
        return session

    def _master(self, clmid: str, timeout: float) -> MasterData:
        end = self._deadline(timeout)
        session = self._require_session(clmid)
        doc, raw = self._request(session.master_url, clmid, {}, end)
        return MasterData(items=_array(doc, raw), raw=raw)

    def _virtual_url(self, doc: dict, name: str) -> str:
        encrypted = doc.get(name)
        if not isinstance(encrypted, str) or not encrypted:
            raise TachibanaAuthError(f"{CLMID_LOGIN}: missing {name}", clmid=CLMID_LOGIN)
        url = None
        try:
            url = self._decrypt(encrypted)
            failed = False
        except Exception:
            # 復号の関数の例外の文は鍵・値を含むかもしれないので、except を抜けてから投げる（__context__ に残さない）
            failed = True
        if failed:
            raise TachibanaAuthError(f"decrypt failed: {name}", clmid=CLMID_LOGIN)
        if not isinstance(url, str) or not url.startswith(VIRTUAL_URL_SCHEMES[name]):
            raise TachibanaAuthError(f"{name}: unexpected scheme", clmid=CLMID_LOGIN)
        return url

    @contextmanager
    def _locked(self, end: float, clmid: str) -> Iterator[None]:
        """ロックを予算の残りまで待って取る。取れなければ TachibanaTimeout。

        pipeline.bounded が待つのをやめた呼び出しが、少しずつ届く応答でロックを持ったまま居残っても、
        次の呼び出しが予算を超えて待ち続けないようにする（待ちは実時間。偽の monotonic でも実際に待つ）
        """
        if not self._lock.acquire(timeout=max(0.0, end - self._monotonic())):
            raise TachibanaTimeout(f"{clmid}: timeout budget exhausted waiting for the lock", clmid=clmid)
        try:
            yield
        finally:
            self._lock.release()

    def _next_p_no(self) -> int:
        # ロックの中で呼ぶ
        self._p_no += 1
        return self._p_no

    def _request(self, target: str, clmid: str, fields: dict[str, str], end: float) -> tuple[dict, RawResponse]:
        """1 回の要求。ロックの中で p_no の払い出し → 間隔の待ち → 予算の残りを timeout にして送信 → 応答の検査"""
        with self._locked(end, clmid):
            if self._last_sent is not None:
                wait = self._last_sent + self._min_interval - self._monotonic()
                if wait > 0:
                    if self._monotonic() + wait >= end:
                        raise TachibanaTimeout(f"{clmid}: timeout budget exhausted", clmid=clmid)
                    self._sleep(wait)
            remaining = end - self._monotonic()
            if remaining <= 0:
                raise TachibanaTimeout(f"{clmid}: timeout budget exhausted", clmid=clmid)
            sent_at = self._now()
            _require_aware(sent_at, "now()")
            p_no = self._next_p_no()
            payload = {"p_no": str(p_no), "p_sd_date": format_sd_date(sent_at), "sCLMID": clmid, **fields}
            url = build_url(target, payload)
            self._last_sent = self._monotonic()
            where = f"{clmid} p_no={p_no}"
            # 例外は except を抜けてから作り直して投げる（差し込みの HTTP の例外の文・連鎖に URL があっても残さない）
            failure: tuple[type[TachibanaHttpError], str, str | None] | None = None
            try:
                response = self._http(url, remaining)
            except TachibanaTimeout as e:
                reason = _safe_reason(e.reason)
                failure = (TachibanaTimeout, "timeout", reason)
            except TachibanaHttpError as e:
                reason = _safe_reason(e.reason)
                failure = (TachibanaHttpError, reason or "http error", reason)
            except Exception as e:
                reason = type(e).__name__
                failure = (TachibanaHttpError, reason, reason)
            if failure is not None:
                error, text, reason = failure
                raise error(f"{where}: {text}", clmid=clmid, reason=reason)
            if not 200 <= response.status < 300:
                status = str(response.status)
                raise TachibanaHttpError(f"{where}: HTTP {status}", clmid=clmid, code=status)
            raw = RawResponse(clmid=clmid, p_no=p_no, sent_at=sent_at, http_status=response.status, body=response.body)
            doc = self._decode(raw, where)
            self._check(doc, clmid, where)
            return doc, raw

    def _decode(self, raw: RawResponse, where: str) -> dict:
        problem = None
        try:
            doc = json.loads(raw.body.decode(self._encoding))
        except UnicodeDecodeError:
            problem = f"cannot decode as {self._encoding}"
        except json.JSONDecodeError:
            problem = "not JSON"
        if problem is not None:
            # except を抜けてから投げる（本文の断片を __context__ に残さない）
            raise TachibanaResponseError(f"{where}: {problem}", clmid=raw.clmid)
        if not isinstance(doc, dict):
            raise TachibanaResponseError(f"{where}: not a JSON object", clmid=raw.clmid)
        return doc

    def _check(self, doc: dict, clmid: str, where: str) -> None:
        """p_errno → sResultCode の順に 0 以外を例外にする。どちらも無い応答は通す
        （どの応答に何が入るかが未確認のため。口座の応答を fixture にしたら厳しくする）。ロックの中で呼ぶ"""
        text = doc.get("sResultText")
        text = text if isinstance(text, str) else None
        for kind in ("p_errno", "sResultCode"):
            if kind not in doc or doc[kind] is None:
                continue
            code = str(doc[kind])
            if code == "0":
                continue
            message = f"{where}: {kind}={code if _CODE.match(code) else '?'}"
            if code in SESSION_EXPIRED_CODES:
                self._session = None  # 自動で再ログインしない。以後は TachibanaNotLoggedIn
                raise TachibanaSessionExpired(message, clmid=clmid, code=code, kind=kind, text=text)
            raise TachibanaApiError(message, clmid=clmid, code=code, kind=kind, text=text)


def _array(doc: dict, raw: RawResponse) -> list[dict[str, str]]:
    """応答の配列（RESPONSE_ARRAYS）。無い・配列でない・要素が object でなければ TachibanaResponseError"""
    key = RESPONSE_ARRAYS[raw.clmid]
    items = doc.get(key)
    if not isinstance(items, list) or not all(isinstance(i, dict) for i in items):
        raise TachibanaResponseError(f"{raw.clmid} p_no={raw.p_no}: missing {key}", clmid=raw.clmid)
    return items

"""JPX のページの取得（requests）。

接続は要求ごとに張り直す（#88。インスタンスの中で keep-alive の接続を使い回さない。旧系の requests.get と同じ取り方）。
取り直しは呼び出し側（app._fetch_jpx）が回数の上限まで行う（#130）。ここは 1 回の要求の結果を返すだけ
"""

from datetime import UTC, datetime

import requests

from . import settings
from .parser import JpxTransient, JpxUnavailable
from .snapshot import FetchedPage

# 一時的な失敗（取り直しの対象）。ほかの RequestException（URL の誤りなど）は取り直さない
TRANSIENT_ERRORS = (requests.Timeout, requests.ConnectionError, requests.exceptions.ChunkedEncodingError)


def split_timeout(timeout: float, connect_cap: float) -> tuple[float, float]:
    """timeout を (接続, 読み取り) に分ける。合計は timeout"""
    connect = min(connect_cap, timeout / 2)
    return connect, timeout - connect


def fetch_page(
    page: int, timeout: float = settings.JPX_TIMEOUT_SECONDS, session: requests.Session | None = None
) -> FetchedPage:
    """1 限月（page=1）/ 2 限月（page=2）のページを取得する。

    session を渡さなければ、この要求だけのセッション（接続）を作って閉じる（#88）。
    失敗: HTTP 5xx・接続エラー・タイムアウトは JpxTransient（取り直しの対象）、ほかの 200 以外の応答・要求の誤りは
    JpxUnavailable。
    timeout（秒）を接続と読み取りに配分し、合計が timeout を超えないようにする:
    接続 = min(JPX_CONNECT_TIMEOUT_SECONDS, timeout / 2)、読み取り = timeout − 接続。
    読み取りタイムアウトは「バイトが届かない時間」の上限なので、全体の打ち切りは呼び出し側（pipeline.bounded）が行う
    """
    if session is None:
        with requests.Session() as own:
            return fetch_page(page, timeout, own)
    url = settings.JPX_URLS[page]
    connect, read = split_timeout(timeout, settings.JPX_CONNECT_TIMEOUT_SECONDS)
    try:
        response = session.get(url, headers=settings.JPX_HEADERS, timeout=(connect, read))
    except TRANSIENT_ERRORS as e:
        raise JpxTransient(f"page {page}: {type(e).__name__}") from e
    except requests.RequestException as e:
        raise JpxUnavailable(f"page {page}: {type(e).__name__}") from e
    if response.status_code >= 500:
        raise JpxTransient(f"page {page}: HTTP {response.status_code}")
    if response.status_code != 200:
        raise JpxUnavailable(f"page {page}: HTTP {response.status_code}")
    return FetchedPage(
        page=page,
        url=url,
        fetched_at=datetime.now(UTC),
        http_status=response.status_code,
        html=response.content,
    )

"""JPX のページの取得（requests）。リトライはしない（次の分に任せる）。"""

from datetime import UTC, datetime

import requests

from . import settings
from .parser import JpxUnavailable
from .snapshot import FetchedPage


def split_timeout(timeout: float, connect_cap: float) -> tuple[float, float]:
    """timeout を (接続, 読み取り) に分ける。合計は timeout"""
    connect = min(connect_cap, timeout / 2)
    return connect, timeout - connect


def fetch_page(
    page: int, timeout: float = settings.JPX_TIMEOUT_SECONDS, session: requests.Session | None = None
) -> FetchedPage:
    """1 限月（page=1）/ 2 限月（page=2）のページを取得する。接続断・タイムアウト・200 以外は JpxUnavailable

    timeout（秒）を接続と読み取りに配分し、合計が timeout を超えないようにする:
    接続 = min(JPX_CONNECT_TIMEOUT_SECONDS, timeout / 2)、読み取り = timeout − 接続。
    読み取りタイムアウトは「バイトが届かない時間」の上限なので、全体の打ち切りは呼び出し側（app._bounded）が行う
    """
    url = settings.JPX_URLS[page]
    getter = session or requests
    connect, read = split_timeout(timeout, settings.JPX_CONNECT_TIMEOUT_SECONDS)
    try:
        response = getter.get(url, headers=settings.JPX_HEADERS, timeout=(connect, read))
    except requests.RequestException as e:
        raise JpxUnavailable(f"page {page}: {type(e).__name__}") from e
    if response.status_code != 200:
        raise JpxUnavailable(f"page {page}: HTTP {response.status_code}")
    return FetchedPage(
        page=page,
        url=url,
        fetched_at=datetime.now(UTC),
        http_status=response.status_code,
        html=response.content,
    )

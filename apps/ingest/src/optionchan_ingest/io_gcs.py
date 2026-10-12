"""生 HTML の GCS への保存（存在確認と、上書き禁止 ifGenerationMatch=0 での作成）。"""

import google.auth.exceptions
import requests
from google.api_core import exceptions
from google.cloud import storage
from google.cloud.storage.retry import DEFAULT_RETRY

from . import settings
from .io_jpx import split_timeout

# StorageFailed に変換する例外。retry=None の呼び出しでは、通信のタイムアウト・接続断（requests）や
# 認証トークンの取得失敗（google.auth）が GoogleAPIError に包まれずにそのまま出てくる
_STORAGE_ERRORS = (exceptions.GoogleAPIError, requests.RequestException, google.auth.exceptions.TransportError)


class AlreadyExists(Exception):
    """同じ名前のオブジェクトが既にある（412。他の実行が処理済み）"""


class StorageFailed(Exception):
    """GCS の呼び出しに失敗した（タイムアウト・接続断・5xx 等）"""


def _timeouts(timeout: float) -> tuple[float, float]:
    return split_timeout(timeout, settings.GCS_CONNECT_TIMEOUT_SECONDS)


class RawStore:
    def __init__(self, bucket: str, project: str | None = None, client: storage.Client | None = None):
        self._client = client or storage.Client(project=project)
        self._bucket = self._client.bucket(bucket)

    def exists(self, name: str, timeout: float = settings.GCS_TIMEOUT_SECONDS) -> bool:
        """存在確認。1 回の試行は min(GCS_EXISTS_ATTEMPT_TIMEOUT_SECONDS, timeout) 秒。

        再試行は「最後の試行も timeout 内に終わる」範囲（timeout − 1 回分）でだけ始める
        （api_core の Retry の timeout は新しい試行を始めるかの判断にだけ使われ、始めた試行は最後まで走るため）
        """
        attempt = min(settings.GCS_EXISTS_ATTEMPT_TIMEOUT_SECONDS, timeout)
        window = timeout - attempt
        retry = DEFAULT_RETRY.with_timeout(window) if window > 0 else None
        try:
            return self._bucket.blob(name).exists(timeout=_timeouts(attempt), retry=retry)
        except _STORAGE_ERRORS as e:
            raise StorageFailed(f"exists {name}: {type(e).__name__}") from e

    def create(self, name: str, data: bytes, timeout: float = settings.GCS_TIMEOUT_SECONDS) -> int:
        """作成して generation を返す。既にあれば AlreadyExists。リトライしない

        （自動リトライで 1 回目の成功の応答だけが失われると、再送が自分の作成に 412 を受けて
        skipped_locked になり BigQuery に書かれない。リトライしなければ、失敗はそのまま StorageFailed になり、
        オブジェクトが無ければ次の分に作り直される。作成済みで応答だけ失われた場合は 11c の回収が拾う）
        """
        blob = self._bucket.blob(name)
        try:
            blob.upload_from_string(
                data,
                content_type=settings.RAW_CONTENT_TYPE,
                if_generation_match=0,
                timeout=_timeouts(timeout),
                retry=None,
            )
        except exceptions.PreconditionFailed as e:
            raise AlreadyExists(name) from e
        except _STORAGE_ERRORS as e:
            raise StorageFailed(f"create {name}: {type(e).__name__}") from e
        return int(blob.generation)

"""I/O 層（JPX・GCS・BigQuery）を偽物のクライアントで検査する。実際の通信はしない"""

from datetime import UTC, datetime

import google.auth.exceptions
import pytest
import requests
from google.api_core import exceptions

from optionchan_ingest import io_bq, settings
from optionchan_ingest.io_bq import BigQueryWriter, WriteFailed
from optionchan_ingest.io_gcs import AlreadyExists, RawStore, StorageFailed
from optionchan_ingest.io_jpx import fetch_page
from optionchan_ingest.parser import JpxUnavailable

# --- JPX ---


class FakeResponse:
    def __init__(self, status, content=b"<html></html>"):
        self.status_code = status
        self.content = content


class FakeSession:
    def __init__(self, result):
        self.result = result
        self.kwargs = None

    def get(self, url, **kwargs):
        self.url, self.kwargs = url, kwargs
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


def test_fetch_page_sends_headers_and_timeout():
    session = FakeSession(FakeResponse(200, b"abc"))
    page = fetch_page(2, 15, session)
    assert session.url == settings.JPX_URLS[2]
    assert session.kwargs["headers"]["Referer"] == "https://svc.qri.jp/jpx/nkopm/2"
    assert session.kwargs["timeout"] == (5, 10)  # (接続, 読み取り)。合計 15
    assert (page.page, page.http_status, page.html) == (2, 200, b"abc")
    assert page.fetched_at.tzinfo is not None


@pytest.mark.parametrize("timeout, expected", [(10, (5, 5)), (3, (1.5, 1.5)), (2.5, (1.25, 1.25))])
def test_fetch_page_timeout_total_within_budget(timeout, expected):
    session = FakeSession(FakeResponse(200))
    fetch_page(1, timeout, session)
    assert session.kwargs["timeout"] == expected
    assert sum(session.kwargs["timeout"]) == timeout


@pytest.mark.parametrize(
    "result", [FakeResponse(400), FakeResponse(503), requests.Timeout(), requests.ConnectionError()]
)
def test_fetch_page_failures_are_unavailable(result):
    with pytest.raises(JpxUnavailable):
        fetch_page(1, 10, FakeSession(result))


# --- GCS ---


class FakeBlob:
    def __init__(self, bucket, name):
        self.bucket, self.name, self.generation = bucket, name, None

    def exists(self, timeout, retry):
        self.bucket.last = ("exists", timeout, retry)
        if self.bucket.raise_error:
            raise self.bucket.raise_error
        if self.bucket.fail:
            raise exceptions.ServiceUnavailable("down")
        return self.name in self.bucket.objects

    def upload_from_string(self, data, content_type, if_generation_match, timeout, retry):
        assert if_generation_match == 0
        assert content_type == "application/json"
        self.bucket.last = ("create", timeout, retry)
        if self.bucket.raise_error:
            raise self.bucket.raise_error
        if self.bucket.fail:
            raise exceptions.ServiceUnavailable("down")
        if self.name in self.bucket.objects:
            raise exceptions.PreconditionFailed("exists")
        self.bucket.objects[self.name] = data
        self.generation = 42


class FakeBucket:
    def __init__(self):
        self.objects = {}
        self.last = None
        self.fail = False
        self.raise_error = None

    def blob(self, name):
        return FakeBlob(self, name)


class FakeStorageClient:
    def __init__(self):
        self.the_bucket = FakeBucket()

    def bucket(self, name):
        return self.the_bucket


def test_raw_store_create_once():
    client = FakeStorageClient()
    store = RawStore("b", client=client)
    assert not store.exists("raw/x.json.gz", timeout=10)
    kind, timeout, retry = client.the_bucket.last
    assert (kind, timeout) == ("exists", (2, 2))  # 1 回の試行 4 秒を (接続, 読み取り) に配分
    assert retry is not None and retry.timeout == 6  # 最後の試行も 10 秒内に終わる範囲でだけ再試行を始める
    store.exists("raw/x.json.gz", timeout=3)
    assert client.the_bucket.last == ("exists", (1.5, 1.5), None)  # 1 回分しか入らないので再試行しない
    assert store.create("raw/x.json.gz", b"data", timeout=9) == 42
    assert client.the_bucket.last == ("create", (4.5, 4.5), None)  # 作成はリトライしない
    assert store.exists("raw/x.json.gz")
    with pytest.raises(AlreadyExists):
        store.create("raw/x.json.gz", b"other")


@pytest.mark.parametrize(
    "error",
    [
        requests.exceptions.ReadTimeout("slow"),
        requests.exceptions.ConnectionError("reset"),
        google.auth.exceptions.TransportError("token"),
        exceptions.ServiceUnavailable("down"),
    ],
)
def test_raw_store_transport_errors_are_storage_failed(error):
    """retry=None の作成では通信の例外がそのまま出てくるので、StorageFailed に変換する"""
    client = FakeStorageClient()
    client.the_bucket.raise_error = error
    store = RawStore("b", client=client)
    with pytest.raises(StorageFailed):
        store.create("raw/x.json.gz", b"data")
    with pytest.raises(StorageFailed):
        store.exists("raw/x.json.gz", timeout=3)


def test_raw_store_with_real_client_read_timeout():
    """実物の storage.Client を、必ず ReadTimeout を返す HTTP セッションにつないで確かめる（通信はしない）"""
    from google.auth.credentials import AnonymousCredentials
    from google.cloud import storage

    class TimeoutSession:
        is_mtls = False

        def __init__(self):
            self.calls = 0

        def request(self, method, url, *args, **kwargs):
            # バケットのメタデータの GET（ライブラリが裏のスレッドで 1 回行う）は数えない。オブジェクトへの呼び出しだけ
            if "/o" in url.split("?")[0].split("/b/b", 1)[-1]:
                self.calls += 1
            raise requests.exceptions.ReadTimeout("slow")

    http = TimeoutSession()
    client = storage.Client(project="test", credentials=AnonymousCredentials(), _http=http)
    store = RawStore("b", client=client)
    with pytest.raises(StorageFailed):
        store.create("raw/x.json.gz", b"data", timeout=2)
    assert http.calls == 1  # 作成は再試行しない
    with pytest.raises(StorageFailed):
        store.exists("raw/x.json.gz", timeout=2)  # 1 回分しか入らないので再試行しない
    assert http.calls == 2


def test_raw_store_errors_are_storage_failed():
    client = FakeStorageClient()
    client.the_bucket.fail = True
    store = RawStore("b", client=client)
    with pytest.raises(StorageFailed):
        store.exists("raw/x.json.gz")
    with pytest.raises(StorageFailed):
        store.create("raw/x.json.gz", b"data")


# --- BigQuery ---


class FakeResponses:
    def __init__(self, items):
        self.items = items
        self.cancelled = False

    def __iter__(self):
        return iter(self.items)

    def cancel(self):
        self.cancelled = True


class FakeWriteClient:
    def __init__(self, result=None):
        self.result = result
        self.calls = []

    def table_path(self, project, dataset, table):
        return f"projects/{project}/datasets/{dataset}/tables/{table}"

    def append_rows(self, requests, retry, timeout, metadata):
        self.calls.append({"requests": list(requests), "retry": retry, "timeout": timeout, "metadata": metadata})
        if isinstance(self.result, Exception):
            raise self.result
        self.responses = FakeResponses(self.result if self.result is not None else [io_bq.types.AppendRowsResponse()])
        return self.responses


def writer(result=None):
    client = FakeWriteClient(result)
    return BigQueryWriter("p", "optionchan_tokyo", settings.DEFAULT_SCHEMA_DIR, client=client), client


SPOT = {"price": 1.0, "created_at": datetime(2026, 10, 12, 1, 46, tzinfo=UTC)}
STREAM = "projects/p/datasets/optionchan_tokyo/tables/spot_price/streams/_default"


def test_append_one_request_with_deadline():
    w, client = writer()
    w.append("spot_price", [SPOT], timeout=12)
    call = client.calls[0]
    assert len(call["requests"]) == 1
    request = call["requests"][0]
    assert request.write_stream == STREAM
    assert request.proto_rows.writer_schema.proto_descriptor.name == "spot_price"
    assert len(request.proto_rows.rows.serialized_rows) == 1
    assert (call["retry"], call["timeout"]) == (None, 12)  # リトライなし・gRPC の deadline
    assert ("x-goog-request-params", f"write_stream={STREAM}") in call["metadata"]
    assert client.responses.cancelled


def test_append_row_errors_raise():
    w, _ = writer([io_bq.types.AppendRowsResponse(row_errors=[io_bq.types.RowError(index=0, message="bad")])])
    with pytest.raises(WriteFailed):
        w.append("spot_price", [SPOT])


def test_append_no_response_raises():
    w, _ = writer([])
    with pytest.raises(WriteFailed):
        w.append("spot_price", [SPOT])


@pytest.mark.parametrize("error", [exceptions.ServiceUnavailable("down"), exceptions.DeadlineExceeded("slow")])
def test_append_exception_is_write_failed(error):
    w, _ = writer(error)
    with pytest.raises(WriteFailed):
        w.append("spot_price", [SPOT])


def test_append_required_missing_raises_before_send():
    w, client = writer()
    with pytest.raises(ValueError):
        w.append("spot_price", [{"price": 1.0}])
    assert client.calls == []

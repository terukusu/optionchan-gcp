"""I/O 層（JPX・GCS・BigQuery）を偽物のクライアントで検査する。実際の通信はしない"""

import http.server
import io
import threading
from datetime import UTC, date, datetime

import google.auth.exceptions
import pytest
import requests
from google.api_core import exceptions
from google.cloud.storage.exceptions import DataCorruption, InvalidResponse

from optionchan_ingest import io_bq, settings
from optionchan_ingest.io_bq import BigQueryLoader, BigQueryQuery, BigQueryWriter, LoadFailed, QueryFailed, WriteFailed
from optionchan_ingest.io_gcs import AlreadyExists, RawStore, StorageFailed
from optionchan_ingest.io_jpx import fetch_page
from optionchan_ingest.parser import JpxTransient, JpxUnavailable

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


@pytest.mark.parametrize(
    "result, detail",
    [
        (FakeResponse(500), "page 1: HTTP 500"),
        (FakeResponse(502), "page 1: HTTP 502"),
        (FakeResponse(503), "page 1: HTTP 503"),
        (requests.ConnectionError(), "page 1: ConnectionError"),
        (requests.ConnectTimeout(), "page 1: ConnectTimeout"),
        (requests.ReadTimeout(), "page 1: ReadTimeout"),
        (requests.exceptions.ChunkedEncodingError(), "page 1: ChunkedEncodingError"),
    ],
)
def test_fetch_page_transient_failures(result, detail):
    """5xx・接続エラー・タイムアウトは取り直しの対象（#88）"""
    with pytest.raises(JpxTransient, match=f"^{detail}$"):
        fetch_page(1, 10, FakeSession(result))


@pytest.mark.parametrize(
    "result", [FakeResponse(400), FakeResponse(403), FakeResponse(404), FakeResponse(304), requests.TooManyRedirects()]
)
def test_fetch_page_other_failures_are_not_transient(result):
    """4xx などは取り直さない"""
    with pytest.raises(JpxUnavailable) as e:
        fetch_page(1, 10, FakeSession(result))
    assert not isinstance(e.value, JpxTransient)


class _KeepAliveHandler(http.server.BaseHTTPRequestHandler):
    """HTTP/1.1 の keep-alive に応じる JPX の代わり。接続ごとのクライアントのポートを記録する"""

    protocol_version = "HTTP/1.1"
    ports: list

    def do_GET(self):
        self.ports.append(self.client_address[1])
        body = b"<html></html>"
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


@pytest.fixture
def local_jpx(monkeypatch):
    """127.0.0.1 の一時ポートの HTTP サーバーに JPX_URLS を向ける。戻り値は要求ごとのクライアントのポート"""
    ports = []
    handler = type("Handler", (_KeepAliveHandler,), {"ports": ports})
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    monkeypatch.setattr(settings, "JPX_URLS", {1: f"{base}/", 2: f"{base}/1"})
    try:
        yield ports
    finally:
        server.shutdown()
        server.server_close()


def test_fetch_page_new_connection_per_request(local_jpx):
    """セッションを渡さない取得（本番の経路）は、要求ごとに新しい接続になる（#88。keep-alive の接続を使い回さない）"""
    for page in (1, 2, 1):
        assert fetch_page(page, 5).http_status == 200
    assert len(local_jpx) == 3
    assert len(set(local_jpx)) == 3  # クライアントのポートがすべて違う = 接続が 3 本


def test_shared_session_would_reuse_connection(local_jpx):
    """検査自体の確認: 同じセッションを使い回すと、このサーバーでは 1 本の接続に乗る（#88 の前の取り方）"""
    with requests.Session() as session:
        for page in (1, 2, 1):
            fetch_page(page, 5, session)
    assert len(local_jpx) == 3
    assert len(set(local_jpx)) == 1


def test_real_deps_do_not_share_jpx_session(monkeypatch):
    """本番の依存の JPX の取得は io_jpx.fetch_page そのもの（セッションを閉じ込めた関数ではない）"""
    from optionchan_ingest import app as app_module
    from optionchan_ingest import io_jpx

    monkeypatch.setenv("RAW_BUCKET", "b")
    monkeypatch.delenv("INGEST_DRY_RUN", raising=False)
    monkeypatch.setattr(app_module, "_cfg", None)
    monkeypatch.setattr(app_module, "_deps", None)
    monkeypatch.setattr(app_module, "_holder", None)
    monkeypatch.setattr(app_module, "_clients", lambda cfg: (_FakeStore(), _FakeWriter()))
    monkeypatch.setattr(app_module, "_load_latest_calendar", lambda timeout: None)
    assert app_module._real_deps().fetch_page is io_jpx.fetch_page


class _FakeStore:
    exists = create = list = None


class _FakeWriter:
    append = None


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


# --- GCS: 回収・再処理用の一覧と読み出し（#34） ---


class ListingBlob:
    def __init__(self, name, generation, time_created):
        self.name, self.generation, self.time_created = name, generation, time_created


class ListingClient:
    def __init__(self, blobs=None, error=None):
        self.blobs = blobs or []
        self.error = error
        self.kwargs = None
        self.read = None

    def bucket(self, name):
        client = self

        class Bucket:
            def blob(self, blob_name, generation=None):
                class Blob:
                    def download_as_bytes(self, timeout, retry):
                        client.read = (blob_name, generation, timeout, retry)
                        if client.error:
                            raise client.error
                        return b"data"

                return Blob()

        return Bucket()

    def list_blobs(self, bucket_name, **kwargs):
        self.kwargs = {"bucket": bucket_name, **kwargs}
        if self.error:
            raise self.error
        return iter(self.blobs)


def test_raw_store_list_offsets_and_fields():
    created = datetime(2026, 10, 12, 1, 46, 9, tzinfo=UTC)
    client = ListingClient([ListingBlob("raw/2026/10/12/20261012T1046.json.gz", "17", created)])
    store = RawStore("b", client=client)
    objects = store.list("raw/2026/10/12/20261012T0500.json.gz", "raw/2026/10/12/20261012T1101.json.gz", timeout=10)
    assert [(o.name, o.generation, o.time_created) for o in objects] == [
        ("raw/2026/10/12/20261012T1046.json.gz", 17, created)
    ]
    kwargs = client.kwargs
    assert kwargs["bucket"] == "b" and kwargs["prefix"] == "raw/"
    assert (kwargs["start_offset"], kwargs["end_offset"]) == (
        "raw/2026/10/12/20261012T0500.json.gz",
        "raw/2026/10/12/20261012T1101.json.gz",
    )
    assert kwargs["fields"] == "items(name,generation,timeCreated),nextPageToken"
    assert kwargs["timeout"] == (2.5, 2.5)  # 1 回の試行 5 秒
    assert kwargs["retry"].timeout == 5  # 最後の試行も 10 秒内に終わる範囲でだけ再試行する


def test_raw_store_read_with_generation():
    client = ListingClient()
    store = RawStore("b", client=client)
    assert store.read("raw/x.json.gz", 123, timeout=3) == b"data"
    name, generation, timeout, retry = client.read
    assert (name, generation, timeout, retry) == ("raw/x.json.gz", 123, (1.5, 1.5), None)


@pytest.mark.parametrize(
    "error", [exceptions.ServiceUnavailable("down"), requests.exceptions.ReadTimeout("slow"), exceptions.NotFound("x")]
)
def test_raw_store_list_and_read_errors_are_storage_failed(error):
    store = RawStore("b", client=ListingClient(error=error))
    with pytest.raises(StorageFailed):
        store.list("raw/a", "raw/b")
    with pytest.raises(StorageFailed):
        store.read("raw/a", 1)


def test_raw_store_list_with_real_client_pages():
    """実物の storage.Client で、一覧の全ページを読み、offset・prefix・fields がクエリに入ること（通信はしない）"""
    import json as jsonlib
    from urllib.parse import parse_qs, urlparse

    from google.auth.credentials import AnonymousCredentials
    from google.cloud import storage

    class Response:
        def __init__(self, body):
            self.status_code = 200
            self.headers = {"content-type": "application/json"}
            self.content = jsonlib.dumps(body).encode()
            self.text = self.content.decode()

        def json(self):
            return jsonlib.loads(self.content)

    class Session:
        is_mtls = False

        def __init__(self):
            self.queries = []

        def request(self, method, url, *args, **kwargs):
            parsed = urlparse(url)
            if not parsed.path.endswith("/o"):
                raise AssertionError(f"想定外の呼び出し {method} {parsed.path}")
            query = parse_qs(parsed.query)
            self.queries.append(query)
            item = {
                "name": "raw/2026/10/12/20261012T1046.json.gz",
                "generation": "5",
                "timeCreated": "2026-10-12T01:46:09.123Z",
            }
            if "pageToken" not in query:
                return Response({"items": [item], "nextPageToken": "t2"})
            return Response({"items": [{**item, "name": "raw/2026/10/12/20261012T1047.json.gz", "generation": "6"}]})

    http = Session()
    client = storage.Client(project="test", credentials=AnonymousCredentials(), _http=http)
    objects = RawStore("b", client=client).list(
        "raw/2026/10/12/20261012T0500.json.gz", "raw/2026/10/12/20261012T1101.json.gz"
    )
    assert [(o.name[-13:], o.generation) for o in objects] == [("T1046.json.gz", 5), ("T1047.json.gz", 6)]
    assert objects[0].time_created == datetime(2026, 10, 12, 1, 46, 9, 123000, tzinfo=UTC)
    first = http.queries[0]
    assert first["prefix"] == ["raw/"]
    assert first["startOffset"] == ["raw/2026/10/12/20261012T0500.json.gz"]
    assert first["endOffset"] == ["raw/2026/10/12/20261012T1101.json.gz"]
    assert first["fields"] == ["items(name,generation,timeCreated),nextPageToken"]
    assert http.queries[1]["pageToken"] == ["t2"]


# --- BigQuery: 完了記録のクエリ（jobs.query の REST、#34） ---


class QueryResponse:
    def __init__(self, status, body):
        self.status_code = status
        self.body = body

    def json(self):
        if isinstance(self.body, Exception):
            raise self.body
        return self.body


class QuerySession:
    def __init__(self, result):
        self.result = result
        self.calls = []

    def post(self, url, json, timeout):
        self.calls.append({"url": url, "json": json, "timeout": timeout})
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


START = datetime(2026, 10, 11, 19, 59, 20, tzinfo=UTC)
END = datetime(2026, 10, 12, 2, 1, 20, tzinfo=UTC)


def query_client(result):
    session = QuerySession(result)
    return BigQueryQuery("p", "optionchan_tokyo", "asia-northeast1", session=session), session


OK_BODY = {
    "jobComplete": True,
    "totalRows": "2",
    "rows": [{"f": [{"v": "gs://b/raw/2026/10/12/20261012T1046.json.gz#5"}]}, {"f": [{"v": "gs://b/raw/x#6"}]}],
}


def test_completed_sources_request_uses_parameters():
    from optionchan_ingest.parser import JST

    client, session = query_client(QueryResponse(200, OK_BODY))
    start_jst = START.astimezone(JST)
    assert client.completed_sources(start_jst, END, timeout=15) == [
        "gs://b/raw/2026/10/12/20261012T1046.json.gz#5",
        "gs://b/raw/x#6",
    ]
    (call,) = session.calls
    assert call["url"] == "https://bigquery.googleapis.com/bigquery/v2/projects/p/queries"
    body = call["json"]
    assert body["useLegacySql"] is False
    assert body["location"] == "asia-northeast1"
    assert body["parameterMode"] == "NAMED"
    assert body["queryParameters"] == [
        {
            "name": "start_at",
            "parameterType": {"type": "TIMESTAMP"},
            "parameterValue": {"value": "2026-10-11 19:59:20.000000+00:00"},  # JST で渡しても UTC にする
        },
        {
            "name": "end_at",
            "parameterType": {"type": "TIMESTAMP"},
            "parameterValue": {"value": "2026-10-12 02:01:20.000000+00:00"},
        },
    ]
    sql = body["query"]
    assert "`p.optionchan_tokyo.ingest_runs`" in sql
    assert "@start_at" in sql and "@end_at" in sql
    assert "2026" not in sql  # 時刻をクエリ文字列に埋め込まない
    assert call["timeout"] == (5, 10)
    assert body["timeoutMs"] == 9000  # 読み取りタイムアウトより 1 秒短く


@pytest.mark.parametrize(
    "result",
    [
        QueryResponse(200, {"jobComplete": False}),
        QueryResponse(200, {**OK_BODY, "pageToken": "next"}),
        QueryResponse(200, {**OK_BODY, "totalRows": "3"}),
        QueryResponse(200, {"jobComplete": True, "errors": [{"reason": "x"}]}),
        QueryResponse(200, ValueError("not json")),
        QueryResponse(500, {"error": {"message": "backend error"}}),
        QueryResponse(403, {"error": {"message": "Access Denied"}}),
        requests.exceptions.ReadTimeout("slow"),
        requests.exceptions.ConnectionError("reset"),
        google.auth.exceptions.RefreshError("token"),
    ],
)
def test_query_failures_are_query_failed(result):
    client, _ = query_client(result)
    with pytest.raises(QueryFailed):
        client.completed_sources(START, END)


def test_query_empty_result():
    client, _ = query_client(QueryResponse(200, {"jobComplete": True, "totalRows": "0"}))
    assert client.completed_sources(START, END) == []


def test_query_rejects_naive_time():
    client, session = query_client(QueryResponse(200, OK_BODY))
    with pytest.raises(ValueError):
        client.completed_sources(datetime(2026, 10, 12), END)
    assert session.calls == []


@pytest.mark.parametrize("error", [DataCorruption(None, "md5 mismatch"), InvalidResponse(None, "bad")])
def test_raw_store_read_corruption_is_storage_failed(error):
    """チェックサムの不一致・想定外の応答（どちらも Exception の直下）も StorageFailed にする"""
    store = RawStore("b", client=ListingClient(error=error))
    with pytest.raises(StorageFailed):
        store.read("raw/a", 1)


# --- クエリのパラメータ（DATE・STRING。#164） ---


def test_query_date_and_string_parameters():
    client, session = query_client(QueryResponse(200, {"jobComplete": True, "totalRows": "0"}))
    client.query("SELECT 1 WHERE @d >= @d AND @v = @v", {"d": date(2026, 11, 16), "v": "1600", "t": START})
    params = session.calls[0]["json"]["queryParameters"]
    assert params == [
        {"name": "d", "parameterType": {"type": "DATE"}, "parameterValue": {"value": "2026-11-16"}},
        {"name": "v", "parameterType": {"type": "STRING"}, "parameterValue": {"value": "1600"}},
        {
            "name": "t",
            "parameterType": {"type": "TIMESTAMP"},
            "parameterValue": {"value": "2026-10-11 19:59:20.000000+00:00"},
        },
    ]


def test_query_rejects_other_parameter_types():
    client, session = query_client(QueryResponse(200, OK_BODY))
    with pytest.raises(TypeError):
        client.query("SELECT @x", {"x": 1})
    assert session.calls == []


# --- ロードジョブ（#164） ---


class LoadResponse(QueryResponse):
    def __init__(self, status, body=None, headers=None):
        super().__init__(status, body if body is not None else {})
        self.headers = headers or {}


class LoadSession:
    def __init__(self, start, upload, polls=()):
        self.start, self.upload, self.polls = start, upload, list(polls)
        self.calls = []

    def _result(self, result):
        if isinstance(result, Exception):
            raise result
        return result

    def post(self, url, json, headers, timeout):
        self.calls.append(("post", url, json, headers, timeout))
        return self._result(self.start)

    def put(self, url, data, headers, timeout):
        self.calls.append(("put", url, data, headers, timeout))
        return self._result(self.upload)

    def get(self, url, params, timeout):
        self.calls.append(("get", url, params, timeout))
        return self._result(self.polls.pop(0))


STARTED = LoadResponse(200, headers={"Location": "https://upload.test/session-1"})
DONE = {"status": {"state": "DONE"}, "statistics": {"load": {"outputRows": "4"}}}
RUNNING = {"status": {"state": "RUNNING"}}


def loader(session, clock=None):
    sleeps = []
    t = iter(clock or [0.0] * 100)
    client = BigQueryLoader(
        "p", "optionchan_tokyo", "asia-northeast1", session=session, sleep=sleeps.append, monotonic=lambda: next(t)
    )
    return client, sleeps


def test_load_replaces_partition_and_waits():
    session = LoadSession(
        STARTED, LoadResponse(200, RUNNING), polls=[LoadResponse(200, RUNNING), LoadResponse(200, DONE)]
    )
    client, sleeps = loader(session)
    stats = client.load_ndjson(
        "jscc_var_scenario", date(2026, 11, 20), io.BytesIO(b"gz"), "job-1", gzip=True, timeout=600
    )
    assert stats == {"outputRows": "4"}
    post, put, get1, get2 = session.calls
    assert post[1] == "https://bigquery.googleapis.com/upload/bigquery/v2/projects/p/jobs?uploadType=resumable"
    job = post[2]
    assert job["jobReference"] == {"projectId": "p", "jobId": "job-1", "location": "asia-northeast1"}
    load = job["configuration"]["load"]
    assert load["destinationTable"] == {
        "projectId": "p",
        "datasetId": "optionchan_tokyo",
        "tableId": "jscc_var_scenario$20261120",
    }
    assert load["writeDisposition"] == "WRITE_TRUNCATE"
    assert load["createDisposition"] == "CREATE_NEVER"
    assert load["sourceFormat"] == "NEWLINE_DELIMITED_JSON"
    assert load["maxBadRecords"] == 0 and load["ignoreUnknownValues"] is False
    assert post[3] == {"X-Upload-Content-Type": "application/gzip"}
    assert put[1] == "https://upload.test/session-1" and put[2] == b"gz"
    assert get1[1] == "https://bigquery.googleapis.com/bigquery/v2/projects/p/jobs/job-1"
    assert get1[2] == {"location": "asia-northeast1"}
    assert sleeps == [settings.JSCC_LOAD_POLL_SECONDS] * 2


def test_load_uncompressed_content_type():
    session = LoadSession(STARTED, LoadResponse(200, DONE))
    client, _ = loader(session)
    client.load_ndjson("t", date(2026, 11, 20), io.BytesIO(b"{}"), "job-1", gzip=False)
    assert session.calls[0][3] == {"X-Upload-Content-Type": "application/octet-stream"}
    assert len(session.calls) == 2  # 完了済みなら jobs.get しない


@pytest.mark.parametrize(
    "start, upload, polls",
    [
        (LoadResponse(403, {"error": {"message": "denied"}}), None, []),
        (LoadResponse(200), None, []),  # Location が無い
        (requests.exceptions.ConnectionError("reset"), None, []),
        (google.auth.exceptions.RefreshError("token"), None, []),
        (STARTED, LoadResponse(400, {"error": {"message": "bad"}}), []),
        (STARTED, requests.exceptions.ReadTimeout("slow"), []),
        (STARTED, LoadResponse(200, ValueError("not json")), []),
        (STARTED, LoadResponse(200, RUNNING), [LoadResponse(500, {})]),
        (STARTED, LoadResponse(200, RUNNING), [requests.exceptions.ConnectionError("x")]),
        (
            STARTED,
            LoadResponse(
                200,
                {
                    "status": {
                        "state": "DONE",
                        "errorResult": {"reason": "invalid", "message": "JSON parsing error"},
                        "errors": [{"reason": "invalid", "location": "row 1"}],
                    }
                },
            ),
            [],
        ),
    ],
)
def test_load_failures(start, upload, polls):
    client, _ = loader(LoadSession(start, upload, polls))
    with pytest.raises(LoadFailed):
        client.load_ndjson("t", date(2026, 11, 20), io.BytesIO(b"x"), "job-1", gzip=True)


def test_load_failure_message_has_no_row_values():
    done = {
        "status": {
            "state": "DONE",
            "errorResult": {"reason": "invalid", "message": 'JSON parsing error in row: {"pnl":[123456789]}'},
            "errors": [{"reason": "invalid", "location": "row 1"}],
        }
    }
    client, _ = loader(LoadSession(STARTED, LoadResponse(200, done), []))
    with pytest.raises(LoadFailed) as e:
        client.load_ndjson("t", date(2026, 11, 20), io.BytesIO(b"x"), "job-1", gzip=True)
    assert "123456789" not in str(e.value)
    assert "job-1" in str(e.value) and "row 1" in str(e.value)


def test_load_timeout_while_running():
    polls = [LoadResponse(200, RUNNING)] * 10
    clock = [0.0, 0.0] + [float(i * 10) for i in range(1, 20)]
    client, sleeps = loader(LoadSession(STARTED, LoadResponse(200, RUNNING), polls), clock=clock)
    with pytest.raises(LoadFailed, match="期限"):
        client.load_ndjson("t", date(2026, 11, 20), io.BytesIO(b"x"), "job-1", gzip=True, timeout=30)
    assert len(sleeps) < 5


def test_load_rejects_datetime_partition():
    session = LoadSession(STARTED, LoadResponse(200, DONE))
    client, _ = loader(session)
    with pytest.raises(TypeError):
        client.load_ndjson("t", datetime(2026, 11, 20, tzinfo=UTC), io.BytesIO(b"x"), "job-1", gzip=True)
    assert session.calls == []

"""BigQuery Storage Write API（_default ストリーム）への追記と、回収・再処理用の完了記録のクエリ（jobs.query の REST）。

`writer.AppendRowsStream` はストリームを開くときの待ちが既定 600 秒で、呼び出し側から期限を渡せない。
ここでは gapic の双方向ストリーミング `append_rows` を 1 リクエストだけ送って閉じる形で使い、
gRPC の deadline（timeout）で開始から応答までの全体を打ち切る。
"""

from datetime import UTC, datetime

import google.auth
import google.auth.exceptions
import requests
from google.cloud import bigquery_storage_v1
from google.cloud.bigquery_storage_v1 import types

from . import bq_proto, settings
from .io_jpx import split_timeout


class WriteFailed(Exception):
    """BigQuery への書き込みに失敗した"""


class BigQueryWriter:
    def __init__(self, project: str, dataset: str, schema_dir, client=None):
        if not project:
            raise RuntimeError("GCP_PROJECT が未設定")
        self._client = client or bigquery_storage_v1.BigQueryWriteClient()
        self._project = project
        self._dataset = dataset
        self._schema_dir = schema_dir
        # テーブル → (スキーマ, 記述子)。メッセージクラスは bq_proto がキャッシュする
        self._schemas: dict[str, tuple[list[dict], object]] = {}

    def _schema(self, table: str):
        if table not in self._schemas:
            schema = bq_proto.load_schema(self._schema_dir, table)
            self._schemas[table] = (schema, bq_proto.schema_to_descriptor(schema, name=table))
        return self._schemas[table]

    def append(self, table: str, rows: list[dict], timeout: float = settings.BQ_APPEND_TIMEOUT_SECONDS) -> None:
        """1 回の AppendRows で追記する。開始から応答までを timeout 秒で打ち切り、リトライしない

        失敗は WriteFailed
        """
        if not rows:
            return
        schema, descriptor = self._schema(table)
        proto_rows = bq_proto.rows_to_proto(descriptor, schema, rows)

        stream_name = f"{self._client.table_path(self._project, self._dataset, table)}/streams/_default"
        proto_data = types.AppendRowsRequest.ProtoData()
        proto_data.writer_schema = types.ProtoSchema(proto_descriptor=descriptor)
        proto_data.rows = proto_rows
        request = types.AppendRowsRequest(write_stream=stream_name, proto_rows=proto_data)

        responses = None
        try:
            responses = self._client.append_rows(
                requests=iter([request]),
                retry=None,
                timeout=timeout,
                # 書き込み先のリージョンへのルーティングに必要（AppendRowsStream と同じヘッダ）
                metadata=(("x-goog-request-params", f"write_stream={stream_name}"),),
            )
            response = next(iter(responses), None)
        except Exception as e:
            raise WriteFailed(f"{table}: {type(e).__name__}: {e}") from e
        finally:
            cancel = getattr(responses, "cancel", None)
            if cancel is not None:
                cancel()

        if response is None:
            raise WriteFailed(f"{table}: 応答なし")
        if response.row_errors:
            raise WriteFailed(f"{table}: row_errors {len(response.row_errors)} 件（先頭 {response.row_errors[0]}）")
        if response.error and response.error.code:
            raise WriteFailed(f"{table}: {response.error.code} {response.error.message}")


class QueryFailed(Exception):
    """BigQuery のクエリに失敗した（通信・HTTP エラー・期限内に終わらない・結果が 1 ページに収まらない）"""


BQ_API = "https://bigquery.googleapis.com/bigquery/v2"
# 結果の行数の上限。完了記録は 6 時間で最大 約 360 件、1 日で 約 1,200 件。超えたら黙って一部だけ使わずに失敗にする
QUERY_MAX_RESULTS = 10000


def _timestamp_param(name: str, value: datetime) -> dict:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    text = value.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.%f+00:00")
    return {"name": name, "parameterType": {"type": "TIMESTAMP"}, "parameterValue": {"value": text}}


class BigQueryQuery:
    """jobs.query（REST）で 1 ページに収まる SELECT を実行する。リトライ・ページ送りはしない

    google-cloud-bigquery を依存に足さないため、google-auth の AuthorizedSession で直接呼ぶ（Phase 2 の決定）。
    値はクエリ文字列に埋め込まず、名前付きパラメータで渡す
    """

    def __init__(self, project: str, dataset: str, location: str, session=None):
        if not project:
            raise RuntimeError("GCP_PROJECT が未設定")
        if session is None:
            from google.auth.transport.requests import AuthorizedSession

            credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/bigquery"])
            session = AuthorizedSession(credentials)
        self._session = session
        self._project = project
        self._dataset = dataset
        self._location = location

    def query(self, sql: str, params: dict[str, datetime], timeout: float = settings.BQ_QUERY_TIMEOUT_SECONDS):
        """行（列の値の list）の list を返す。値は REST の文字列のまま。失敗は QueryFailed"""
        connect, read = split_timeout(timeout, settings.BQ_CONNECT_TIMEOUT_SECONDS)
        body = {
            "query": sql,
            "useLegacySql": False,
            "location": self._location,
            "parameterMode": "NAMED",
            "queryParameters": [_timestamp_param(k, v) for k, v in params.items()],
            # サーバの待ちは読み取りタイムアウトより 1 秒短く（jobComplete=false で返してもらう）
            "timeoutMs": max(1000, int((read - 1) * 1000)),
            "maxResults": QUERY_MAX_RESULTS,
        }
        url = f"{BQ_API}/projects/{self._project}/queries"
        try:
            response = self._session.post(url, json=body, timeout=(connect, read))
        except (requests.RequestException, google.auth.exceptions.GoogleAuthError) as e:
            raise QueryFailed(f"{type(e).__name__}") from e
        if response.status_code != 200:
            raise QueryFailed(f"HTTP {response.status_code}: {_error_message(response)}")
        try:
            doc = response.json()
        except ValueError as e:
            raise QueryFailed("応答が JSON でない") from e
        if doc.get("errors"):
            raise QueryFailed(f"errors: {str(doc['errors'])[:300]}")
        if not doc.get("jobComplete"):
            raise QueryFailed("期限内に終わらなかった（jobComplete=false）")
        rows = doc.get("rows") or []
        if doc.get("pageToken") or int(doc.get("totalRows", len(rows))) != len(rows):
            raise QueryFailed(f"結果が 1 ページに収まらない（totalRows={doc.get('totalRows')}）")
        return [[cell.get("v") for cell in row.get("f", [])] for row in rows]

    def completed_sources(
        self, start_at: datetime, end_at: datetime, timeout: float = settings.BQ_QUERY_TIMEOUT_SECONDS
    ) -> list[str]:
        """snapshot_at が [start_at, end_at) の ingest_runs（完了記録）の source_object（gs://<bucket>/<name>#<generation>）"""
        table = f"`{self._project}.{self._dataset}.{settings.RUNS_TABLE}`"
        sql = (
            f"SELECT DISTINCT source_object FROM {table} "
            "WHERE snapshot_at >= @start_at AND snapshot_at < @end_at AND source_object IS NOT NULL"
        )
        return [row[0] for row in self.query(sql, {"start_at": start_at, "end_at": end_at}, timeout=timeout)]


def _error_message(response) -> str:
    try:
        return str(response.json()["error"]["message"])[:300]
    except Exception:
        return ""

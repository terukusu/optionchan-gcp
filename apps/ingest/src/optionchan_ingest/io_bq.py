"""BigQuery Storage Write API（_default ストリーム）への追記。

`writer.AppendRowsStream` はストリームを開くときの待ちが既定 600 秒で、呼び出し側から期限を渡せない。
ここでは gapic の双方向ストリーミング `append_rows` を 1 リクエストだけ送って閉じる形で使い、
gRPC の deadline（timeout）で開始から応答までの全体を打ち切る。
"""

from google.cloud import bigquery_storage_v1
from google.cloud.bigquery_storage_v1 import types

from . import bq_proto, settings


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

"""回収・照合・再処理のテスト用の偽の依存（GCS の一覧・読み出し、完了記録のクエリ、BigQuery の追記）。

生 HTML は htmlgen のページを encode_raw で封筒にしたもの（値は架空。JPX から取らない）。
"""

import itertools
import json
from datetime import timedelta

from htmlgen import page_html

from optionchan_ingest.io_bq import QueryFailed, WriteFailed
from optionchan_ingest.io_gcs import RawObject, StorageFailed
from optionchan_ingest.parser import JST
from optionchan_ingest.pipeline import Deadline
from optionchan_ingest.recovery import RecoveryDeps
from optionchan_ingest.snapshot import FetchedPage, encode_raw, raw_object_name

BUCKET = "test-bucket"
STRIKES = tuple(range(38000, 43000, 500))
GENERATION = 1700000000000001


def raw_bytes(updated_at, page1=None, page2=None) -> bytes:
    """updated_at（JST aware）の 1 限月ページと、その 1 分前の 2 限月ページの封筒"""
    local = updated_at.astimezone(JST)
    p2_time = local - timedelta(minutes=1)
    page1 = page1 or page_html(updated_at=local.strftime("%Y/%m/%d %H:%M"), strikes=STRIKES, atm=40000)
    page2 = page2 or page_html(
        updated_at=p2_time.strftime("%Y/%m/%d %H:%M"), last_trading_day="2026/12/10", strikes=STRIKES, atm=40000
    )
    fetched = local + timedelta(seconds=8)
    return encode_raw(
        [
            FetchedPage(1, "https://example.invalid/1", fetched, 200, page1.encode("utf-8"), local),
            FetchedPage(2, "https://example.invalid/2", fetched, 200, page2.encode("utf-8"), p2_time),
        ]
    )


class FakeRecovery:
    def __init__(self, wall):
        self.wall = wall  # 実時刻（aware）
        self.objects = {}  # name → (RawObject, data)
        self.completed = set()  # 完了記録のある名前
        self.calls = []
        self.appended = []  # (table, rows)
        self.clock_value = 0.0  # 偽の monotonic（秒）
        self.cost = {}  # 呼び出しの種類 → 進める秒数
        self.list_error = None
        self.query_error = None
        self.read_error_on = set()
        self.read_unexpected_on = set()  # 想定外の例外（RuntimeError）を出す名前
        self.append_error_on = None  # (name, table)
        self._ids = itertools.count(1)
        self._current = None  # 今読んでいる名前（追記の失敗の対象を決める）

    def add(self, updated_at, age_seconds=600, data=None, completed=False, generation=GENERATION):
        name = raw_object_name(updated_at)
        created = self.wall - timedelta(seconds=age_seconds)
        self.objects[name] = (RawObject(name, generation, created), data if data is not None else raw_bytes(updated_at))
        if completed:
            self.completed.add(name)
        return name

    def _tick(self, kind):
        self.clock_value += self.cost.get(kind, 0.0)

    def monotonic(self):
        return self.clock_value

    def clock(self):
        return self.wall + timedelta(seconds=self.clock_value)

    def list_raw(self, start_offset, end_offset, timeout):
        self.calls.append(("list", start_offset, end_offset))
        self._tick("list")
        if self.list_error:
            raise self.list_error
        return [obj for name, (obj, _) in sorted(self.objects.items()) if start_offset <= name < end_offset]

    def read_raw(self, name, generation, timeout):
        self.calls.append(("read", name, generation))
        self._tick("read")
        if name in self.read_error_on:
            raise StorageFailed(f"read {name}")
        if name in self.read_unexpected_on:
            raise RuntimeError(f"unexpected {name}")
        self._current = name
        obj, data = self.objects[name]
        assert obj.generation == generation
        return data

    def completed_sources(self, start_at, end_at, timeout):
        self.calls.append(("query", start_at, end_at))
        self._tick("query")
        if self.query_error:
            raise self.query_error
        # 他のバケット・形式違いの値が混じっても数えない
        return [f"gs://{BUCKET}/{n}#{GENERATION}" for n in sorted(self.completed)] + ["broken", None]

    def append_rows(self, table, rows, timeout):
        self.calls.append(("append", table))
        self._tick(f"append_{table}")
        if self.append_error_on == (self._current, table):
            raise WriteFailed(table)
        self.appended.append((table, rows))

    def deps(self):
        return RecoveryDeps(
            BUCKET,
            self.list_raw,
            self.read_raw,
            self.completed_sources,
            self.append_rows,
            clock=self.clock,
            monotonic=self.monotonic,
            new_run_id=lambda: f"run-{next(self._ids)}",
        )

    def deadline(self, seconds=45):
        return Deadline(seconds, self.monotonic)

    def kinds(self):
        return [c[0] for c in self.calls]

    def runs(self):
        return [row for table, rows in self.appended if table == "ingest_runs" for row in rows]


def corrupt_body(data: bytes) -> bytes:
    """gzip のヘッダは正しく本体だけ壊れたもの（decode_raw が zlib.error を出す）"""
    return data[:10] + bytes(x ^ 0xFF for x in data[10:40]) + data[40:]


def logs(capsys):
    return [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.strip()]


__all__ = ["BUCKET", "FakeRecovery", "QueryFailed", "StorageFailed", "corrupt_body", "logs", "raw_bytes"]

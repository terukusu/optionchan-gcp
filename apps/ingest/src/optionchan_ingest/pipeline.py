"""取り込み・回収・再処理で共有する部品: 期限（Deadline）、期限つきの I/O 呼び出し、
3 テーブル → ingest_runs の書き込み。

app.handle（毎分）、recovery（毎時の回収・日次の照合）、reprocess（手動のジョブ）が同じ書き込みの順序と
タイムアウトの付け方を使う（target.md 3.1）。
"""

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeout
from datetime import datetime

from . import settings


class DeadlineExceeded(Exception):
    """全体の期限を超えた。stage は次に入ろうとした処理"""

    def __init__(self, stage: str):
        super().__init__(stage)
        self.stage = stage


class Deadline:
    def __init__(self, seconds: float, monotonic: Callable[[], float]):
        self._monotonic = monotonic
        self._start = monotonic()
        self._end = self._start + seconds

    def elapsed_ms(self) -> int:
        return int((self._monotonic() - self._start) * 1000)

    def remaining(self) -> float:
        return self._end - self._monotonic()

    def timeout(self, cap: float, stage: str) -> float:
        """次の外部呼び出しのタイムアウト = min(cap, 残り時間)。残りが少なければ DeadlineExceeded"""
        remaining = self.remaining()
        if remaining < settings.MIN_REMAINING_SECONDS:
            raise DeadlineExceeded(stage)
        return min(cap, remaining)


# JPX・GCS・BigQuery のクエリの呼び出しを出すスレッド。
# 待つのをやめた呼び出しが残っても、各呼び出しのタイムアウトで終わる
_IO_EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix="ingest-io")


def bounded(deadline: Deadline, stage: str, cap: float, fn: Callable, *args):
    """fn(*args, timeout=t) を別スレッドで実行し、t + CALL_GRACE_SECONDS で待つのをやめて DeadlineExceeded にする。

    t = min(cap, 残り時間)。fn の中のタイムアウトが効かない場合（少しずつ届く応答、再試行の最後の 1 回など）でも、
    リクエスト全体が期限 + 小さな余裕で終わるようにする
    """
    timeout = deadline.timeout(cap, stage)
    future = _IO_EXECUTOR.submit(fn, *args, timeout=timeout)
    try:
        return future.result(timeout=timeout + settings.CALL_GRACE_SECONDS)
    except FutureTimeout:
        raise DeadlineExceeded(stage) from None


def run_row(
    *,
    ingest_run_id: str,
    snapshot_at: datetime,
    page2_updated_at: datetime | None,
    source_object: str,
    source_generation: int,
    parser_version: str,
    trigger: str,
    counts: dict[str, int],
    started_at: datetime,
) -> dict:
    """ingest_runs の 1 行（finished_at は write_snapshot が 3 テーブルの後に入れる）"""
    return {
        "ingest_run_id": ingest_run_id,
        "snapshot_at": snapshot_at,
        "page2_updated_at": page2_updated_at,
        "source_object": source_object,
        "source_generation": source_generation,
        "parser_version": parser_version,
        "trigger": trigger,
        "rows_spot": counts["spot_price"],
        "rows_future": counts["future_price"],
        "rows_option": counts["option_price"],
        "started_at": started_at,
    }


def write_snapshot(
    rows: dict[str, list[dict]],
    run: dict,
    append_rows: Callable[..., None],
    deadline: Deadline,
    clock: Callable[[], datetime],
) -> None:
    """spot_price → future_price → option_price → ingest_runs の順に追記する。

    各テーブルの追記のタイムアウトは min(BQ_APPEND_TIMEOUT_SECONDS, 残り時間)。期限を超えたら次のテーブルに入らない
    （DeadlineExceeded）。失敗は WriteFailed（append_rows が出す）。
    ingest_runs は 3 テーブルがすべて成功したときだけ書く
    """
    for table in settings.DATA_TABLES:
        append_rows(table, rows[table], timeout=deadline.timeout(settings.BQ_APPEND_TIMEOUT_SECONDS, f"append_{table}"))
    append_rows(
        settings.RUNS_TABLE,
        [{**run, "finished_at": clock()}],
        timeout=deadline.timeout(settings.BQ_APPEND_TIMEOUT_SECONDS, "append_ingest_runs"),
    )

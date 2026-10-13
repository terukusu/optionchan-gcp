"""取りこぼしの回収（毎時 0 分）・日次の照合（07:00 JST）・再処理の 1 件の処理（target.md 3.1、#34）。

完了記録（ingest_runs）が無いのに生 HTML があるスナップショットを、保存済みの生 HTML から読み直して
新しい ingest_run_id で追記する（D10 の追記のみ。同じスナップショットの複数の版は、
最新版の選択（sql/latest/）で 1 つにする）。
JPX にはアクセスしない。行の数え方・選び方は常に「ingest_runs にある ingest_run_id の行だけ」。
"""

import re
import time
import uuid
import zlib
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from . import PARSER_VERSION, settings
from .io_bq import QueryFailed, WriteFailed
from .io_gcs import RawObject, StorageFailed
from .log import log
from .parser import JST, JpxUnavailable, ParsedPage, ParseError, parse_page
from .pipeline import Deadline, DeadlineExceeded, bounded, run_row, write_snapshot
from .snapshot import RunMeta, build_rows, decode_raw, raw_object_name, source_object, validate_rows

# reingest_one の結果
RECOVERED = "recovered"
PARSE_FAILED = "parse_failed"
WRITE_FAILED = "write_failed"
STORAGE_FAILED = "storage_failed"
UNEXPECTED = "unexpected"  # 1 件の処理の想定外の例外（ERROR <trigger>_item_failed を出して次へ進む）

# 1 件ごとの成功のログのイベント名（trigger → イベント）
_OK_EVENT = {"recovery": "recovered", "reprocess": "reprocessed"}

_SOURCE_RE = re.compile(r"^gs://[^/]+/(raw/[^#]+)#\d+$")


@dataclass
class RecoveryDeps:
    """回収・照合・再処理が使う I/O。テストでは偽物を渡す。I/O の関数はすべて timeout（秒）をキーワード引数で受け取る"""

    bucket: str
    list_raw: Callable[..., list[RawObject]]  # (start_offset, end_offset, timeout=) 失敗は StorageFailed
    read_raw: Callable[..., bytes]  # (name, generation, timeout=) 失敗は StorageFailed
    completed_sources: Callable[..., list[str]]  # (start_at, end_at, timeout=) 失敗は QueryFailed
    append_rows: Callable[..., None]  # (table, rows, timeout=) 失敗は WriteFailed
    clock: Callable[[], datetime] = field(default=lambda: datetime.now(UTC))
    monotonic: Callable[[], float] = time.monotonic
    new_run_id: Callable[[], str] = field(default=lambda: str(uuid.uuid4()))


class RawParseError(Exception):
    """保存済みの生 HTML から行を作れない（封筒の破損・解析失敗・構造の検査の違反・名前と更新時刻の不一致）"""


# --- 純粋関数 ---


def _require_aware(dt: datetime, name: str) -> None:
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")


def trigger_time(schedule_time: str | None, now: datetime) -> datetime:
    """毎時・日次の判定に使う時刻: Scheduler の予定時刻（RFC3339）。無い・読めない・タイムゾーンが無ければ実時刻"""
    if schedule_time:
        try:
            t = datetime.fromisoformat(schedule_time)
        except ValueError:
            return now
        if t.tzinfo is not None and t.utcoffset() is not None:
            return t
    return now


def is_recovery_minute(t: datetime) -> bool:
    """JST の毎時 0 分か"""
    _require_aware(t, "t")
    return t.astimezone(JST).minute == 0


def is_reconcile_time(t: datetime) -> bool:
    _require_aware(t, "t")
    local = t.astimezone(JST)
    return (local.hour, local.minute) == settings.RECONCILE_AT


def name_time(name: str) -> datetime | None:
    """生 HTML の名前の時刻（JST aware）。形式違いは None"""
    # 書式に %Y が 2 回あり strptime に渡せないので、最後の要素から読み、名前を作り直して一致を確かめる
    try:
        t = datetime.strptime(name.rsplit("/", 1)[-1], settings.RAW_OBJECT_FORMAT.rsplit("/", 1)[-1])
    except ValueError:
        return None
    t = t.replace(tzinfo=JST)
    return t if raw_object_name(t) == name else None


@dataclass(frozen=True)
class Window:
    """名前の時刻の範囲 [start, end)"""

    start: datetime
    end: datetime

    def __post_init__(self):
        _require_aware(self.start, "start")
        _require_aware(self.end, "end")

    def offsets(self) -> tuple[str, str]:
        """GCS の一覧の start_offset（含む）と end_offset（含まない）。

        end が分の途中でも end の分の生 HTML を含めて一覧し、contains で絞る
        """
        return raw_object_name(self.start), raw_object_name(self.end + timedelta(minutes=1))

    def query_range(self) -> tuple[datetime, datetime]:
        """ingest_runs を引く snapshot_at の範囲（UTC aware）。名前の時刻 = snapshot_at の分なので前後 1 分の余裕"""
        return (self.start - timedelta(minutes=1)).astimezone(UTC), (self.end + timedelta(minutes=1)).astimezone(UTC)

    def contains(self, name: str) -> bool:
        t = name_time(name)
        return t is not None and self.start <= t < self.end


def recovery_window(now: datetime) -> Window:
    """名前の時刻が直近 RECOVERY_LOOKBACK_HOURS 時間"""
    _require_aware(now, "now")
    return Window(now - timedelta(hours=settings.RECOVERY_LOOKBACK_HOURS), now)


def reconcile_window(t: datetime) -> Window:
    """JST の [前日 RECONCILE_AT, 当日 RECONCILE_AT)。t が当日の RECONCILE_AT より前なら 1 日前の窓"""
    _require_aware(t, "t")
    local = t.astimezone(JST)
    hour, minute = settings.RECONCILE_AT
    end = local.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if local < end:
        end -= timedelta(days=1)
    return Window(end - timedelta(days=1), end)


def object_name_from_source(source: str | None) -> str | None:
    """'gs://<bucket>/raw/...json.gz#<generation>' → 'raw/...json.gz'。形式違いは None（数えない）"""
    if not source:
        return None
    m = _SOURCE_RE.match(source)
    return m.group(1) if m else None


def completed_names(sources: list[str]) -> set[str]:
    return {name for name in map(object_name_from_source, sources) if name}


def old_enough(objects: list[RawObject], now: datetime, min_age: float) -> list[RawObject]:
    """作成から min_age 秒以上たったもの（入力の順のまま）"""
    _require_aware(now, "now")
    limit = now - timedelta(seconds=min_age)
    out = []
    for o in objects:
        _require_aware(o.time_created, "time_created")
        if o.time_created <= limit:
            out.append(o)
    return out


def select_pending(
    objects: list[RawObject], completed: set[str], now: datetime, min_age: float, limit: int
) -> tuple[list[RawObject], int]:
    """完了記録が無く作成から min_age 秒以上のものを名前（= 時刻）の昇順に先頭 limit 件と、条件を満たす総数。

    limit <= 0 は空（総数は数える）。now / time_created は aware 必須
    """
    candidates = sorted((o for o in old_enough(objects, now, min_age) if o.name not in completed), key=lambda o: o.name)
    return (candidates[:limit] if limit > 0 else []), len(candidates)


# --- 1 件の再取り込み ---


def parse_raw(name: str, data: bytes, run: RunMeta) -> tuple[dict[str, list[dict]], ParsedPage, ParsedPage]:
    """生 HTML（封筒）から 3 テーブルの行を作る。作れなければ RawParseError"""
    try:
        pages = {p.page: p for p in decode_raw(data)}
    # 壊れた gzip（ヘッダ不正は OSError、途中で切れたものは EOFError、本体の破損は zlib.error）・JSON・封筒の形式違い
    except (OSError, EOFError, zlib.error, ValueError, KeyError, TypeError) as e:
        raise RawParseError(f"decode: {type(e).__name__}: {e}") from e
    if set(pages) != {1, 2}:
        raise RawParseError(f"封筒のページが {sorted(pages)}（1 と 2 のはず）")
    parsed, errors = {}, []
    for number in (1, 2):
        try:
            parsed[number] = parse_page(pages[number].html)
        except (ParseError, JpxUnavailable, ValueError) as e:
            errors.append(f"page {number}: {type(e).__name__}: {e}")
    if errors:
        raise RawParseError("; ".join(errors))
    p1, p2 = parsed[1], parsed[2]
    if raw_object_name(p1.updated_at) != name:
        raise RawParseError(f"名前と 1 限月の更新時刻が違う（{p1.updated_at.isoformat()}）")
    rows = build_rows(p1, p2, run)
    violations = validate_rows(rows)
    if violations:
        raise RawParseError("; ".join(violations))
    return rows, p1, p2


def reingest_one(obj: RawObject, deps: RecoveryDeps, deadline: Deadline, trigger: str, common: dict) -> str:
    """保存済みの生 HTML 1 件を読み直して追記する（3 テーブル → ingest_runs。ingest_runs は 3 テーブルの成功後だけ）。

    戻り値は RECOVERED / PARSE_FAILED / WRITE_FAILED / STORAGE_FAILED。DeadlineExceeded は呼び出し側へ
    （回収・再処理はそこで止めて残りに数える）。解析の失敗は WARNING（元の実行が ERROR を出し済み。集計は呼び出し側）、
    書き込み・読み出しの失敗は ERROR
    """
    started_at = deps.clock()
    fields: dict[str, Any] = {"object": obj.name, "trigger": trigger, **common}
    try:
        data = bounded(
            deadline, f"{trigger}_read", settings.GCS_READ_TIMEOUT_SECONDS, deps.read_raw, obj.name, obj.generation
        )
    except StorageFailed as e:
        log("ERROR", "storage_failed", failure="storage_failed", stage=f"{trigger}_read", detail=str(e), **fields)
        return STORAGE_FAILED

    run_id = deps.new_run_id()
    src = source_object(deps.bucket, obj.name, obj.generation)
    fields["ingest_run_id"] = run_id
    try:
        rows, p1, p2 = parse_raw(obj.name, data, RunMeta(run_id, PARSER_VERSION, src))
    except RawParseError as e:
        log("WARNING", f"{trigger}_parse_failed", detail=str(e)[:1000], **fields)
        return PARSE_FAILED

    counts = {table: len(table_rows) for table, table_rows in rows.items()}
    run = run_row(
        ingest_run_id=run_id,
        snapshot_at=p1.updated_at,
        page2_updated_at=p2.updated_at,
        source_object=src,
        source_generation=obj.generation,
        parser_version=PARSER_VERSION,
        trigger=trigger,
        counts=counts,
        started_at=started_at,
    )
    try:
        write_snapshot(rows, run, deps.append_rows, deadline, deps.clock)
    except WriteFailed as e:
        log("ERROR", "write_failed", failure="write_failed", detail=str(e), **fields)
        return WRITE_FAILED
    log("INFO", _OK_EVENT.get(trigger, f"{trigger}_ok"), rows=counts, **fields)
    return RECOVERED


def try_one(obj: RawObject, deps: RecoveryDeps, deadline: Deadline, trigger: str, common: dict) -> str:
    """reingest_one を 1 件単位で囲む。

    想定外の例外は ERROR <trigger>_item_failed（種別と名前）を出して UNEXPECTED を返す。

    1 件の想定外の失敗で回収・再処理の全体を止めない（古い順なので、止めると同じ 1 件が毎回先頭で全体を塞ぐ）。
    全体の期限（DeadlineExceeded）は従来どおり呼び出し側へ出して止める
    """
    try:
        return reingest_one(obj, deps, deadline, trigger, common)
    except DeadlineExceeded:
        raise
    except Exception as e:
        log(
            "ERROR",
            f"{trigger}_item_failed",
            failure=f"{trigger}_item_failed",
            object=obj.name,
            trigger=trigger,
            error_type=type(e).__name__,
            detail=str(e)[:500],
            **common,
        )
        return UNEXPECTED


def list_window(deps: RecoveryDeps, window: Window, deadline: Deadline, stage: str) -> list[RawObject]:
    """窓の生 HTML の一覧（名前の時刻で [start, end) に絞る）。失敗は StorageFailed / DeadlineExceeded"""
    start_offset, end_offset = window.offsets()
    objects = bounded(deadline, stage, settings.GCS_LIST_TIMEOUT_SECONDS, deps.list_raw, start_offset, end_offset)
    return [o for o in objects if window.contains(o.name)]


def query_completed(deps: RecoveryDeps, window: Window, deadline: Deadline, stage: str) -> set[str]:
    """窓の完了記録の名前の集合。失敗は QueryFailed / DeadlineExceeded（空集合にしない: 全件を再取り込みしない）"""
    start_at, end_at = window.query_range()
    sources = bounded(deadline, stage, settings.BQ_QUERY_TIMEOUT_SECONDS, deps.completed_sources, start_at, end_at)
    return completed_names(sources)


# --- 毎時の回収・日次の照合 ---


def recover(now: datetime, deps: RecoveryDeps, deadline: Deadline, common: dict, reserve: float = 0.0) -> dict:
    """毎時の回収。期限の残りが RECOVERY_MIN_REMAINING_SECONDS + reserve 以上のときだけ 1 件を始める。まとめを返す"""
    window = recovery_window(now)
    min_remaining = settings.RECOVERY_MIN_REMAINING_SECONDS + reserve
    summary: dict[str, Any] = {"window_start": window.start, "window_end": window.end}

    if deadline.remaining() < min_remaining:
        log("WARNING", "recovery_skipped", reason="no_time", elapsed_ms=deadline.elapsed_ms(), **summary, **common)
        return summary

    stage = "recovery_list"
    try:
        objects = list_window(deps, window, deadline, stage)
        summary["listed"] = len(objects)
        aged = old_enough(objects, now, settings.RECOVERY_MIN_AGE_SECONDS)
        summary["old_enough"] = len(aged)
        if not aged:  # 時間外・休場日はほぼ常にここで終わる（BigQuery にクエリしない）
            log(
                "INFO",
                "recovery",
                queried=False,
                pending=0,
                remaining=0,
                elapsed_ms=deadline.elapsed_ms(),
                **summary,
                **common,
            )
            return summary
        stage = "recovery_query"
        completed = query_completed(deps, window, deadline, stage)
    except DeadlineExceeded:
        log(
            "WARNING",
            "recovery_skipped",
            reason="deadline",
            stage=stage,
            elapsed_ms=deadline.elapsed_ms(),
            **summary,
            **common,
        )
        return summary
    except (StorageFailed, QueryFailed) as e:
        log(
            "ERROR",
            "recovery_failed",
            failure="recovery_failed",
            stage=stage,
            detail=str(e),
            elapsed_ms=deadline.elapsed_ms(),
            **summary,
            **common,
        )
        return summary

    pending, total = select_pending(
        objects, completed, now, settings.RECOVERY_MIN_AGE_SECONDS, settings.RECOVERY_MAX_ITEMS
    )
    summary["completed"] = len(aged) - total
    summary["pending"] = total
    outcomes: Counter = Counter()
    for obj in pending:
        if deadline.remaining() < min_remaining:
            break
        try:
            outcomes[try_one(obj, deps, deadline, "recovery", common)] += 1
        except DeadlineExceeded:
            break
    attempted = sum(outcomes.values())
    summary.update(
        recovered=outcomes[RECOVERED],
        parse_failed=outcomes[PARSE_FAILED],
        write_failed=outcomes[WRITE_FAILED],
        storage_failed=outcomes[STORAGE_FAILED],
        unexpected=outcomes[UNEXPECTED],
        remaining=total - attempted,
    )
    severity = "WARNING" if summary["remaining"] > 0 else "INFO"
    log(severity, "recovery", queried=True, elapsed_ms=deadline.elapsed_ms(), **summary, **common)
    return summary


def reconcile(t: datetime, deps: RecoveryDeps, deadline: Deadline, common: dict) -> dict | None:
    """日次の照合: 窓の生 HTML の数・完了記録のある数・未回収の数と名前（先頭 RECONCILE_LOG_MAX 件）。

    未回収が 1 件以上なら ERROR raw_unrecovered。失敗（一覧・クエリ・期限）は ERROR reconcile_failed
    """
    window = reconcile_window(t)
    base = {"window_start": window.start, "window_end": window.end}
    stage = "reconcile_list"
    try:
        objects = list_window(deps, window, deadline, stage)
        names = {o.name for o in objects}
        if names:
            stage = "reconcile_query"
            completed = query_completed(deps, window, deadline, stage)
        else:  # 休場日・週末: クエリしない
            completed = set()
    except (StorageFailed, QueryFailed, DeadlineExceeded) as e:
        detail = f"deadline at {e.stage}" if isinstance(e, DeadlineExceeded) else str(e)
        log("ERROR", "reconcile_failed", failure="reconcile_failed", stage=stage, detail=detail, **base, **common)
        return None
    unresolved = sorted(names - completed)
    result = {
        **base,
        "raw": len(names),
        "completed": len(names) - len(unresolved),
        "unresolved": len(unresolved),
        "unresolved_objects": unresolved[: settings.RECONCILE_LOG_MAX],
    }
    log("INFO", "raw_reconciled", **result, **common)
    if unresolved:
        log("ERROR", "raw_unrecovered", failure="raw_unrecovered", **result, **common)
    return result


def run_hourly(now: datetime, at: datetime, deps: RecoveryDeps, deadline: Deadline, common: dict) -> None:
    """0 分のリクエストの後処理: 回収、07:00 ならその後に照合（回収で直った分を未回収に数えない）。例外を外に出さない"""
    reconcile_now = is_reconcile_time(at)
    reserve = settings.RECONCILE_RESERVE_SECONDS if reconcile_now else 0.0
    try:
        recover(now, deps, deadline, common, reserve=reserve)
    except Exception as e:  # 想定外。通常の取り込みの応答は変えない
        log(
            "ERROR",
            "recovery_failed",
            failure="recovery_failed",
            stage="unexpected",
            detail=f"{type(e).__name__}: {e}",
            **common,
        )
    if reconcile_now:
        try:
            reconcile(at, deps, deadline, common)
        except Exception as e:
            log(
                "ERROR",
                "reconcile_failed",
                failure="reconcile_failed",
                stage="unexpected",
                detail=f"{type(e).__name__}: {e}",
                **common,
            )


# --- 本番の依存 ---


def make_real_deps(cfg: settings.Settings) -> RecoveryDeps:
    """GCS・BigQuery（Storage Write API と jobs.query）の実物。クライアントはここで作る"""
    from .io_bq import BigQueryQuery, BigQueryWriter
    from .io_gcs import RawStore

    store = RawStore(cfg.raw_bucket, cfg.project)
    writer = BigQueryWriter(cfg.project, cfg.dataset, cfg.schema_dir)
    query = BigQueryQuery(cfg.project, cfg.dataset, cfg.bq_location)
    return RecoveryDeps(cfg.raw_bucket, store.list, store.read, query.completed_sources, writer.append)


def make_dry_run_deps(cfg: settings.Settings) -> RecoveryDeps:
    """ローカルの動作確認（INGEST_DRY_RUN）用: 一覧は常に空（GCS・BigQuery に触らない）"""

    def forbidden(*args, **kwargs):
        raise AssertionError("dry run の回収で I/O を呼んだ")

    return RecoveryDeps(cfg.raw_bucket, lambda start, end, timeout=None: [], forbidden, forbidden, forbidden)

"""Cloud Run サービスのエントリポイント（Cloud Scheduler から毎分 POST /）。

処理（docs/architecture/target.md 3.1）:
1. 取引カレンダーで時間外なら 204（JPX・GCS・BigQuery に触らない。クライアントも作らない。待たない）
2. 分の頭から settings.FETCH_AT_SECOND 秒まで待ってから（#38）、1 限月ページを取得して更新時刻を得る。
   鮮度切れなら ERROR stale。生 HTML が既にあれば skipped_no_update
3. 2 限月ページを取得し、2 ページを封筒にして ifGenerationMatch=0 で作成（412 なら skipped_locked）
4. 行を組み立てて構造を検査（違反は parse_failed。生 HTML は残し ingest_runs は書かない）
5. spot_price → future_price → option_price → ingest_runs の順に Storage Write API で追記（失敗は write_failed）
6. 予定時刻（無ければ実時刻）が JST の毎時 0 分なら、応答の前に同じ期限の残りで取りこぼしを回収し、
   07:00 は日次の照合もする
   （recovery.py、#34。時間外の 0 分は回収用のクライアントだけを作る。回収の結果・失敗は応答を変えない）
全体の期限は settings.REQUEST_DEADLINE_SECONDS。各呼び出しのタイムアウトは残り時間で頭打ちにし、
超えたら deadline_exceeded
"""

import json
import os
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from flask import Flask, Response, request

from . import PARSER_VERSION, recovery, settings
from .calendar import Calendar, load_calendar, session_at
from .io_bq import WriteFailed
from .io_gcs import AlreadyExists, StorageFailed
from .log import log
from .parser import JST, JpxUnavailable, ParseError, parse_page, parse_updated_at
from .pipeline import Deadline, DeadlineExceeded, bounded, run_row, write_snapshot
from .snapshot import (
    FetchedPage,
    RunMeta,
    build_rows,
    encode_raw,
    is_stale,
    raw_object_name,
    source_object,
    validate_rows,
    with_updated_at,
)


@dataclass
class Deps:
    """handle が使う I/O。テストでは偽物を渡す。I/O の関数はすべて timeout（秒）をキーワード引数で受け取る"""

    calendar: Calendar
    bucket: str
    fetch_page: Callable[..., FetchedPage]  # (page, timeout=) 失敗は JpxUnavailable
    raw_exists: Callable[..., bool]  # (name, timeout=) 失敗は StorageFailed
    raw_create: Callable[..., int]  # (name, data, timeout=) generation を返す。既にあれば AlreadyExists
    append_rows: Callable[..., None]  # (table, rows, timeout=) 失敗は WriteFailed
    clock: Callable[[], datetime] = field(default=lambda: datetime.now(UTC))
    monotonic: Callable[[], float] = time.monotonic
    new_run_id: Callable[[], str] = field(default=lambda: str(uuid.uuid4()))
    trigger: str = "scheduler"
    fetch_at_second: float = settings.FETCH_AT_SECOND  # 分の頭からこの秒まで待って JPX を取得する（0 で待たない）
    sleep: Callable[[float], None] = time.sleep


@dataclass
class Result:
    status: int
    outcome: str
    body: dict[str, Any] = field(default_factory=dict)


def second_of_minute(t: datetime) -> float:
    """分内の秒（0 以上 60 未満。JST と UTC で同じ）"""
    return t.second + t.microsecond / 1_000_000


def fetch_wait_seconds(now: datetime, at_second: float) -> float:
    """分の頭から at_second 秒になるまでの待ち（秒）。既に過ぎていれば 0"""
    return max(0.0, at_second - second_of_minute(now))


def _calendar_notice(now: datetime, calendar: Calendar, common: dict) -> None:
    """取引カレンダーの期限の通知。毎分ではなく 1 日 1 回（settings.CALENDAR_NOTICE_AT の分のリクエスト）だけ出す"""
    local = now.astimezone(JST)
    if (local.hour, local.minute) != settings.CALENDAR_NOTICE_AT:
        return
    days_left = calendar.days_left(now)
    if days_left < 0:
        log(
            "ERROR",
            "calendar_outdated",
            failure="calendar_outdated",
            covered_until=calendar.covered_until,
            days_left=days_left,
            **common,
        )
    elif days_left <= settings.CALENDAR_WARN_DAYS:
        log("WARNING", "calendar_expiring", covered_until=calendar.covered_until, days_left=days_left, **common)


def handle(now: datetime, deps: Deps, schedule_time: str | None = None, deadline: Deadline | None = None) -> Result:
    """deadline を渡すと、その残りで動く（ルートが毎時の回収と同じ期限を共有するため）。省略時はここで作る"""
    if deadline is None:
        deadline = Deadline(settings.REQUEST_DEADLINE_SECONDS, deps.monotonic)
    common: dict[str, Any] = {"schedule_time": schedule_time}
    try:
        return _handle(now, deps, deadline, common)
    except DeadlineExceeded as e:
        log(
            "ERROR",
            "deadline_exceeded",
            failure="deadline_exceeded",
            stage=e.stage,
            elapsed_ms=deadline.elapsed_ms(),
            **common,
        )
        return Result(500, "deadline_exceeded")
    except StorageFailed as e:
        log(
            "ERROR",
            "storage_failed",
            failure="storage_failed",
            detail=str(e),
            elapsed_ms=deadline.elapsed_ms(),
            **common,
        )
        return Result(500, "storage_failed")


def _handle(now: datetime, deps: Deps, deadline: Deadline, common: dict) -> Result:
    def done(severity, outcome, status, **fields):
        log(severity, outcome, elapsed_ms=deadline.elapsed_ms(), **fields, **common)
        return Result(status, outcome)

    _calendar_notice(now, deps.calendar, common)

    session = session_at(now, deps.calendar)
    if session is None:
        return done("INFO", "out_of_hours", 204)
    common["session"] = session.kind

    # 分の頭の数秒は JPX のページが書き換え途中なので、FETCH_AT_SECOND まで待つ（#38）。
    # now（受付時刻）ではなく今の時刻で計算する（コールドスタートで依存の作成にかかった分は待たない）。
    # 待ちは全体の期限に含まれる（Deadline は handle の開始から）
    wait = fetch_wait_seconds(deps.clock(), deps.fetch_at_second)
    if wait > 0:
        deps.sleep(wait)
    common["wait_ms"] = int(wait * 1000)

    # 1 限月ページ（JPX はリトライしない）
    t_jpx = deps.monotonic()
    try:
        page1 = bounded(deadline, "jpx_page1", settings.JPX_TIMEOUT_SECONDS, deps.fetch_page, 1)
        updated_at = parse_updated_at(page1.html)
    except JpxUnavailable as e:
        return done("WARNING", "jpx_unavailable", 200, failure="jpx_unavailable", detail=str(e))
    except ParseError as e:
        return done("ERROR", "parse_failed", 500, failure="parse_failed", detail=str(e))
    page1 = with_updated_at(page1, updated_at)
    common["snapshot_at"] = updated_at
    common["page1_fetch_second"] = round(second_of_minute(page1.fetched_at), 3)

    # 鮮度。カレンダーの期限切れ後は休業日を判定できないので見ない（calendar_outdated で通知する）
    if not deps.calendar.is_outdated(now) and is_stale(now, updated_at, session):
        log("ERROR", "stale", failure="stale", lag_seconds=int((now - updated_at).total_seconds()), **common)

    name = raw_object_name(updated_at)
    if bounded(deadline, "raw_exists", settings.GCS_TIMEOUT_SECONDS, deps.raw_exists, name):
        return done("INFO", "skipped_no_update", 200, object=name, jpx_fetch_ms=int((deps.monotonic() - t_jpx) * 1000))

    # 2 限月ページ
    try:
        page2 = bounded(deadline, "jpx_page2", settings.JPX_TIMEOUT_SECONDS, deps.fetch_page, 2)
    except JpxUnavailable as e:
        return done("WARNING", "jpx_unavailable", 200, failure="jpx_unavailable", detail=str(e))
    jpx_fetch_ms = int((deps.monotonic() - t_jpx) * 1000)

    # 解析（空ページなら保存せず次の分に任せる。構造違いは生 HTML を残してから parse_failed）
    parsed, parse_errors = {}, []
    for number, page in ((1, page1), (2, page2)):
        try:
            parsed[number] = parse_page(page.html)
        except JpxUnavailable as e:
            return done("WARNING", "jpx_unavailable", 200, failure="jpx_unavailable", detail=f"page {number}: {e}")
        except ParseError as e:
            parse_errors.append(f"page {number}: {e}")
    p1, p2 = parsed.get(1), parsed.get(2)
    try:
        page2_updated_at = p2.updated_at if p2 else parse_updated_at(page2.html)
    except (JpxUnavailable, ParseError):
        page2_updated_at = None
    page2 = with_updated_at(page2, page2_updated_at)

    try:
        generation = bounded(
            deadline, "raw_create", settings.GCS_TIMEOUT_SECONDS, deps.raw_create, name, encode_raw([page1, page2])
        )
    except AlreadyExists:
        return done("INFO", "skipped_locked", 200, object=name)
    src = source_object(deps.bucket, name, generation)
    common["source_object"] = src

    if parse_errors:
        return done("ERROR", "parse_failed", 500, failure="parse_failed", detail=parse_errors)

    run_id = deps.new_run_id()
    common["ingest_run_id"] = run_id
    rows = build_rows(p1, p2, RunMeta(run_id, PARSER_VERSION, src))
    violations = validate_rows(rows)
    if violations:
        return done("ERROR", "parse_failed", 500, failure="parse_failed", detail=violations)

    # 書き込み（3 テーブル → ingest_runs）。期限を超えたら次のテーブルに入らない
    # （ingest_runs が無いので毎時の回収が拾う）
    counts = {table: len(table_rows) for table, table_rows in rows.items()}
    common["rows"] = counts
    run = run_row(
        ingest_run_id=run_id,
        snapshot_at=p1.updated_at,
        page2_updated_at=p2.updated_at,
        source_object=src,
        source_generation=generation,
        parser_version=PARSER_VERSION,
        trigger=deps.trigger,
        counts=counts,
        started_at=now,
    )
    try:
        write_snapshot(rows, run, deps.append_rows, deadline, deps.clock)
    except WriteFailed as e:
        return done("ERROR", "write_failed", 500, failure="write_failed", detail=str(e))

    log("INFO", "ingested", jpx_fetch_ms=jpx_fetch_ms, elapsed_ms=deadline.elapsed_ms(), **common)
    return Result(200, "ingested", {"rows": counts})


# --- 本番の依存（取引時間中の初回のリクエストで作り、以降は使い回す） ---

_cfg: settings.Settings | None = None
_calendar: Calendar | None = None
_deps: Deps | None = None


def _config() -> tuple[settings.Settings, Calendar]:
    global _cfg, _calendar
    if _cfg is None:
        _cfg = settings.Settings.from_env()
        _calendar = load_calendar(_cfg.calendar_path)
    return _cfg, _calendar


def _real_deps() -> Deps:
    global _deps
    if _deps is not None:
        return _deps
    cfg, calendar = _config()

    from . import io_jpx

    if cfg.dry_run:
        from . import bq_proto

        def append_rows(table, rows, timeout=None):
            schema = bq_proto.load_schema(cfg.schema_dir, table)
            proto = bq_proto.rows_to_proto(bq_proto.schema_to_descriptor(schema, table), schema, rows)
            size = sum(len(r) for r in proto.serialized_rows)
            log("INFO", "dry_run_append", table=table, rows=len(rows), proto_bytes=size)

        def raw_create(name, data, timeout=None):
            log("INFO", "dry_run_raw_create", object=name, bytes=len(data))
            return 0

        _deps = Deps(
            calendar,
            cfg.raw_bucket,
            io_jpx.fetch_page,
            lambda name, timeout=None: False,
            raw_create,
            append_rows,
            fetch_at_second=cfg.fetch_at_second,
        )
        return _deps

    import requests

    store, writer = _clients(cfg)
    http = requests.Session()
    _deps = Deps(
        calendar,
        cfg.raw_bucket,
        lambda page, timeout: io_jpx.fetch_page(page, timeout, http),
        store.exists,
        store.create,
        writer.append,
        fetch_at_second=cfg.fetch_at_second,
    )
    return _deps


_store = None
_writer = None
_recovery_deps: recovery.RecoveryDeps | None = None


def _clients(cfg: settings.Settings):
    """GCS と BigQuery（Storage Write API）のクライアント。毎分の取り込みと回収で共有する"""
    global _store, _writer
    if _store is None:
        from .io_bq import BigQueryWriter
        from .io_gcs import RawStore

        _store = RawStore(cfg.raw_bucket, cfg.project)
        _writer = BigQueryWriter(cfg.project, cfg.dataset, cfg.schema_dir)
    return _store, _writer


def _real_recovery_deps() -> recovery.RecoveryDeps:
    """毎時の回収の依存（0 分のリクエストで初めて作り、以降は使い回す）"""
    global _recovery_deps
    if _recovery_deps is not None:
        return _recovery_deps
    cfg, _ = _config()
    if cfg.dry_run:
        _recovery_deps = recovery.make_dry_run_deps(cfg)
        return _recovery_deps

    from .io_bq import BigQueryQuery

    store, writer = _clients(cfg)
    query = BigQueryQuery(cfg.project, cfg.dataset, cfg.bq_location)
    _recovery_deps = recovery.RecoveryDeps(
        cfg.raw_bucket, store.list, store.read, query.completed_sources, writer.append
    )
    return _recovery_deps


def _now() -> datetime:
    # ローカルの動作確認用（INGEST_DRY_RUN のときだけ有効）: INGEST_FAKE_NOW=2026-10-13T10:00:00+09:00
    fake = os.environ.get("INGEST_FAKE_NOW")
    if fake and os.environ.get("INGEST_DRY_RUN") in ("1", "true"):
        return datetime.fromisoformat(fake)
    return datetime.now(UTC)


app = Flask(__name__)


@app.post("/")
def ingest() -> Response:
    now = _now()
    schedule_time = request.headers.get("X-CloudScheduler-ScheduleTime")
    try:
        _, calendar = _config()
        if session_at(now, calendar) is None:
            # 時間外の高速経路: クライアントを作らない
            deps = Deps(calendar, "", _forbidden, _forbidden, _forbidden, _forbidden)
        else:
            deps = _real_deps()
        deadline = Deadline(settings.REQUEST_DEADLINE_SECONDS, deps.monotonic)
        result = handle(now, deps, schedule_time, deadline=deadline)
    except Exception as e:  # 想定外。ERROR を出して 500
        log("ERROR", "unexpected", failure="unexpected", detail=f"{type(e).__name__}: {e}", schedule_time=schedule_time)
        return Response('{"outcome":"unexpected"}', status=500, mimetype="application/json")

    # 毎時 0 分（JST）だけ、通常の処理の後に同じ期限の残りで回収（07:00 は照合も）。応答は handle の結果のまま
    at = recovery.trigger_time(schedule_time, now)
    if recovery.is_recovery_minute(at):
        _hourly(now, at, deadline, {"schedule_time": schedule_time})

    if result.status == 204:
        return Response(status=204)
    return Response(
        json.dumps({"outcome": result.outcome, **result.body}), status=result.status, mimetype="application/json"
    )


def _hourly(now: datetime, at: datetime, deadline: Deadline, common: dict) -> None:
    try:
        recovery.run_hourly(now, at, _real_recovery_deps(), deadline, common)
    except Exception as e:  # 依存の作成の失敗など。通常の取り込みの応答は変えない
        log(
            "ERROR",
            "recovery_failed",
            failure="recovery_failed",
            stage="setup",
            detail=f"{type(e).__name__}: {e}",
            **common,
        )


def _forbidden(*args, **kwargs):
    raise AssertionError("時間外の経路で I/O を呼んだ")

"""取引カレンダーの自動取り込み（#63）: JPX「祝日取引」のページを読み、検証した版を BigQuery に追記する。
休業日の正はこの版だけ（#75）。ingest は最新版（ビュー trading_calendar_latest）を読み、読めなければ直前に読めた値、
一度も読めていなければ休業日なし（土日以外はすべて取引日）で動いて ERROR calendar_unavailable を出す。

- 毎日 settings.CALENDAR_SYNC_AT（06:40 JST）のリクエストで、通常の処理の後に同じ期限の残りで run_sync（app.py）
- 手動: Cloud Run ジョブ calendar-sync
      python -m optionchan_ingest.calendar_sync [--dry-run] [--accept 理由] [--backfill-from YYYY-01-01]
  --dry-run: 取得・検証して結果をログに出すだけ（書かない）
  --accept: 前の版との差分の検証（上書き可の問題）を越えて受け入れる（ページの形・祝日・年末年始の問題は越えない）
  --backfill-from: 過去年の埋め戻し（#67）。指定の 1/1 から版の掲載範囲の開始の前日までの行を、祝日 CSV と
      JPX「祝日取引実施日（確定）一覧」（xlsx。ページのリンクから取る）から作って足した版を書く（足すのは掲載範囲より
      前の行だけ。既存の行は変えない）。既に埋まっていれば何も足さない
  終了コード: 0 = 更新・変化なし・dry-run、1 = 失敗・拒否、2 = 引数の誤り

処理（run_sync）:
1. 最新版を読む（無ければ初回。前の版との差分の検証はせず、ページの形・祝日・年末年始の検証だけで受け入れる）
2. ページを取得・解析し、版を組み立てる（前の版の過去年の行を引き継ぐ）。取引時間が settings の定数と違えば ERROR
   --backfill-from なら祝日 CSV と xlsx を取得して過去年の行を足す
3. 内容が最新版と同じなら INFO calendar_checked で終わる（祝日 CSV は取らない）
4. 祝日 CSV を取得して検証（jpx_calendar.validate）。問題があれば書かずに ERROR calendar_sync_rejected
5. 1 回の追記（Storage Write API。1 版 = 1 リクエストで原子的）→ NOTICE calendar_updated
失敗は ERROR calendar_sync_failed（stage 付き）。どれも新系の M2 で通知する
"""

import argparse
import os
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta

import requests

from . import jpx_calendar as jc
from . import settings
from .calendar import build_calendar, sessions_of
from .io_bq import QueryFailed, WriteFailed
from .log import log
from .parser import JST
from .pipeline import Deadline, DeadlineExceeded, bounded

SOURCE_BIGQUERY = "bigquery"
SOURCE_NONE = "none"  # 版を一度も読めていない（休業日なし = 土日以外はすべて取引日。ERROR calendar_unavailable）
EXIT_OK, EXIT_FAILED, EXIT_USAGE = 0, 1, 2


class CalendarFetchFailed(Exception):
    """JPX のページ・祝日 CSV が取れない（接続断・タイムアウト・200 以外）"""


def fetch_url(url: str, timeout: float, session: requests.Session | None = None) -> bytes:
    getter = session or requests
    try:
        response = getter.get(url, timeout=(min(5.0, timeout / 2), timeout - min(5.0, timeout / 2)))
    except requests.RequestException as e:
        raise CalendarFetchFailed(f"{url}: {type(e).__name__}") from e
    if response.status_code != 200:
        raise CalendarFetchFailed(f"{url}: HTTP {response.status_code}")
    return response.content


# ---- 最新版の読み込み（BigQuery のビュー） ----


def latest_sql(project: str, dataset: str) -> str:
    view = f"`{project}.{dataset}.{settings.CALENDAR_VIEW}`"
    return (
        "SELECT FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E6S+00:00', version_at, 'UTC'), CAST(calendar_date AS STRING), "
        "is_trading_day, status, day_type, name, note, source, CAST(covered_until AS STRING), "
        f"CAST(listed_until AS STRING), accepted, content_hash FROM {view} ORDER BY calendar_date"
    )


def rows_from_rest(values: list[list]) -> list[dict]:
    """jobs.query の値（文字列）→ jpx_calendar.from_rows の行"""
    rows = []
    for v in values:
        if len(v) != 12:
            raise jc.CalendarDataError(f"列の数が {len(v)}")
        try:
            rows.append(
                {
                    "version_at": datetime.fromisoformat(v[0]),
                    "calendar_date": date.fromisoformat(v[1]),
                    "is_trading_day": {"true": True, "false": False}[v[2]],
                    "status": v[3],
                    "day_type": v[4],
                    "name": v[5],
                    "note": v[6],
                    "source": v[7],
                    "covered_until": date.fromisoformat(v[8]),
                    "listed_until": date.fromisoformat(v[9]),
                    "accepted": v[10],
                    "content_hash": v[11],
                }
            )
        except (TypeError, ValueError, KeyError) as e:
            raise jc.CalendarDataError(f"値が読めない: {type(e).__name__}: {e}") from None
    return rows


def load_latest(query, project: str, dataset: str, timeout: float) -> jc.LoadedVersion | None:
    """最新版。無ければ None。失敗は QueryFailed / CalendarDataError"""
    return jc.from_rows(rows_from_rest(query.query(latest_sql(project, dataset), {}, timeout=timeout)))


def previous_sql(project: str, dataset: str) -> str:
    """version_at が @version_at より前の版のうち最新の 1 つ（= 読んだ版の 1 つ前の版）の行。テーブルを直接読む"""
    table = f"`{project}.{dataset}.{settings.CALENDAR_TABLE}`"
    return (
        f"SELECT CAST(calendar_date AS STRING), is_trading_day FROM {table} "
        f"WHERE version_at = (SELECT MAX(version_at) FROM {table} WHERE version_at < @version_at)"
    )


def load_previous_closed(query, project: str, dataset: str, version_at: datetime, timeout: float):
    """version_at の版の 1 つ前の版の休業日（frozenset）。1 つ前の版が無ければ None。失敗は QueryFailed /
    CalendarDataError（ingest 側の検査で 14 日以内の休業日の増加を比べる相手。起動直後に使う）"""
    values = query.query(previous_sql(project, dataset), {"version_at": version_at}, timeout=timeout)
    if not values:
        return None
    closed = set()
    for v in values:
        if len(v) != 2 or v[1] not in ("true", "false"):
            raise jc.CalendarDataError(f"1 つ前の版の値が読めない: {v!r}"[:200])
        try:
            d = date.fromisoformat(v[0])
        except (TypeError, ValueError):
            raise jc.CalendarDataError(f"1 つ前の版の日付が読めない: {v[0]!r}") from None
        if v[1] == "false":
            closed.add(d)
    return frozenset(closed)


# ---- ingest が使うカレンダー ----


class CalendarHolder:
    """ingest のインスタンスが使うカレンダー。起動時と毎時 0 分に最新版を読み直す。

    - 起動直後（版を一度も読めていない）は休業日なし（土日以外はすべて取引日。calendar.build_calendar() の既定）。
      読めない（失敗・期限・版が無い）たびに ERROR calendar_unavailable を出し、settings.CALENDAR_RETRY_SECONDS 後の
      次のリクエストで読み直す（毎時 0 分を待たない）。版を使わなかった（下の拒否）ときも、版がまだ無い間は毎回 ERROR
      calendar_unavailable を出す（calendar_load_rejected は同じ理由では 1 回だけ。黙って休業日なしで動き続けない）
    - 一度読めた後に読めなければ、直前に読めた版を使い続ける（WARNING calendar_load_failed。毎時 0 分に読み直す）
    - 版が不正（content_hash の不一致・重複など）か、ingest 側の検査（jpx_calendar.ingest_sanity）に当たれば
      使わずに今の値のまま（ERROR calendar_load_rejected。同じ理由では 1 回だけ）。
      14 日以内の休業日の増加は直前に読めた版と比べる。プロセス内に直前の版が無い（起動直後）ときは、BigQuery の
      1 つ前の版（テーブルの version_at が読んだ版の 1 つ前）と比べる（新しいインスタンスでも同じ判定になる）。
      1 つ前の版が無い（版が 1 つだけ）なら比べない。1 つ前の版を読めない（失敗・期限）ときは比べずに
      WARNING calendar_previous_unavailable（「休業日なし」と比べると近い休業日を持つ正しい版をすべて拒んでしまうため）
    """

    def __init__(
        self,
        load: Callable[..., jc.LoadedVersion | None] | None,
        monotonic: Callable[[], float] = time.monotonic,
        load_previous: Callable[..., frozenset | None] | None = None,  # (version_at, timeout=) load_previous_closed
    ):
        self.current = build_calendar()
        self.source = SOURCE_NONE
        self.version_at: datetime | None = None
        self.attempted = False
        self._load = load
        self._load_previous = load_previous
        self._monotonic = monotonic
        self._retry_at: float | None = None
        self._rejected: str | None = None

    @property
    def remote(self) -> bool:
        """BigQuery から読む（dry-run では読まない）"""
        return self._load is not None

    def due(self) -> bool:
        """リクエストの最初に読むか: インスタンスの最初、または一度も読めていなくて再試行の時刻を過ぎた"""
        if not self.attempted:
            return True
        return self._retry_at is not None and self._monotonic() >= self._retry_at

    def refresh(self, deadline: Deadline, common: dict, stage: str, now: datetime) -> None:
        """例外を外に出さない"""
        self.attempted = True
        if self._load is None:
            return
        try:
            loaded = bounded(deadline, "calendar_load", settings.CALENDAR_LOAD_TIMEOUT_SECONDS, self._load)
        except jc.CalendarDataError as e:
            return self._reject(f"data: {e}", stage, common)
        except Exception as e:  # QueryFailed・期限・認証など（一時的なもの）
            detail = "deadline_exceeded" if isinstance(e, DeadlineExceeded) else f"{type(e).__name__}: {e}"
            return self._load_failed(stage, detail, common)
        if loaded is None:
            return self._load_failed(stage, "版が無い", common)
        self.apply(loaded, common, stage, now, deadline)

    def _load_failed(self, stage: str, detail: str, common: dict) -> None:
        if self.source == SOURCE_NONE:
            # 休業日なしで判定している（休業日にも取り込みに行き、鮮度・期限の判定もしない）
            log("ERROR", "calendar_unavailable", failure="calendar_unavailable", stage=stage, detail=detail, **common)
        else:
            log(
                "WARNING",
                "calendar_load_failed",
                stage=stage,
                detail=detail,
                version_at=self.version_at,
                **common,
            )
        self._schedule_retry()

    def _schedule_retry(self) -> None:
        # 一度でも版を読めていれば毎時の読み直しに任せる
        if self.source == SOURCE_NONE:
            self._retry_at = self._monotonic() + settings.CALENDAR_RETRY_SECONDS

    def _reject(self, detail: str, stage: str, common: dict, **fields) -> None:
        """版を使わない。calendar_load_rejected は同じ理由では 1 回。版がまだ無い間は毎回 calendar_unavailable も"""
        if self._rejected != detail:
            log(
                "ERROR",
                "calendar_load_rejected",
                failure="calendar_load_rejected",
                stage=stage,
                detail=detail,
                source=self.source,
                **fields,
                **common,
            )
        self._rejected = detail
        if self.source == SOURCE_NONE:
            log(
                "ERROR",
                "calendar_unavailable",
                failure="calendar_unavailable",
                stage=stage,
                detail=f"rejected: {detail}",
                **common,
            )
        self._schedule_retry()

    def _previous_closed(self, loaded: jc.LoadedVersion, deadline: Deadline | None, stage: str, common: dict):
        """14 日以内の休業日の増加を比べる相手。プロセス内の直前の版、無ければ BigQuery の 1 つ前の版。
        比べないときは None（1 つ前の版が無い・読めない・override の版）"""
        if self.source == SOURCE_BIGQUERY:
            return self.current.closed_dates
        if self._load_previous is None or loaded.accepted == jc.ACCEPTED_OVERRIDE:
            return None
        if deadline is None:
            deadline = Deadline(
                settings.CALENDAR_PREVIOUS_TIMEOUT_SECONDS + settings.MIN_REMAINING_SECONDS, self._monotonic
            )
        try:
            return bounded(
                deadline,
                "calendar_load_previous",
                settings.CALENDAR_PREVIOUS_TIMEOUT_SECONDS,
                self._load_previous,
                loaded.version_at,
            )
        except Exception as e:  # QueryFailed・CalendarDataError・期限など。比べずに進む
            detail = "deadline_exceeded" if isinstance(e, DeadlineExceeded) else f"{type(e).__name__}: {e}"
            log("WARNING", "calendar_previous_unavailable", stage=stage, detail=detail, **common)
            return None

    def apply(
        self, loaded: jc.LoadedVersion, common: dict, stage: str, now: datetime, deadline: Deadline | None = None
    ) -> None:
        """deadline は 1 つ前の版を読むとき（起動直後）の期限。省略時は CALENDAR_PREVIOUS_TIMEOUT_SECONDS で作る"""
        version = loaded.version
        previous = self._previous_closed(loaded, deadline, stage, common)
        problems = jc.ingest_sanity(loaded, previous, now.astimezone(JST).date())
        if problems:
            return self._reject("; ".join(problems), stage, common, version_at=loaded.version_at)
        self._rejected = None
        self._retry_at = None
        changed = self.source != SOURCE_BIGQUERY or self.version_at != loaded.version_at
        self.current = build_calendar(version.closed_dates, version.covered_until)
        self.source, self.version_at = SOURCE_BIGQUERY, loaded.version_at
        if changed:
            log(
                "INFO",
                "calendar_loaded",
                stage=stage,
                version_at=loaded.version_at,
                covered_until=version.covered_until,
                listed_until=version.listed_until,
                closed_dates=len(version.closed_dates),
                **common,
            )


def is_sync_time(t: datetime) -> bool:
    if t.tzinfo is None or t.utcoffset() is None:
        raise ValueError("t must be timezone-aware")
    local = t.astimezone(JST)
    return (local.hour, local.minute) == settings.CALENDAR_SYNC_AT


# ---- 取り込み ----


@dataclass
class SyncDeps:
    load_latest: Callable[..., jc.LoadedVersion | None]  # (timeout=)
    fetch_page: Callable[..., bytes]  # (timeout=) JPX「祝日取引」のページ
    fetch_holidays: Callable[..., bytes]  # (timeout=) 内閣府の祝日 CSV
    append_rows: Callable[..., None]  # (table, rows, timeout=)
    clock: Callable[[], datetime] = field(default=lambda: datetime.now(UTC))
    fetch_url: Callable[..., bytes] | None = None  # (url, timeout=) 過去年の埋め戻しの xlsx（--backfill-from だけ）


@dataclass
class SyncResult:
    outcome: str  # updated / unchanged / dry_run / rejected / failed
    loaded: jc.LoadedVersion | None = None  # 取り込みの後の最新版（失敗・拒否なら前の版）


def run_sync(
    now: datetime,
    deps: SyncDeps,
    deadline: Deadline,
    common: dict,
    *,
    accept_reason: str | None = None,
    dry_run: bool = False,
    backfill_from: date | None = None,
) -> SyncResult:
    """想定した失敗（取得・解析・読み書き・期限）はログに出して SyncResult で返す。想定外の例外は呼び出し側で捕まえる"""
    today = now.astimezone(JST).date()
    loaded = None

    def failed(stage: str, e: Exception | str) -> SyncResult:
        detail = e if isinstance(e, str) else f"{type(e).__name__}: {e}"
        log("ERROR", "calendar_sync_failed", failure="calendar_sync_failed", stage=stage, detail=detail, **common)
        return SyncResult("failed", loaded)

    try:
        try:
            loaded = bounded(deadline, "calendar_load", settings.CALENDAR_LOAD_TIMEOUT_SECONDS, deps.load_latest)
        except QueryFailed as e:
            return failed("load", e)
        except jc.CalendarDataError as e:
            # 最新版が壊れている（同じ version_at の重複など）。--accept なら版が無いときと同じく、前の版との
            # 差分の検証をせずに新しい版を書いて復旧する
            if not accept_reason:
                return failed("load", e)
            log("WARNING", "calendar_previous_ignored", detail=f"{type(e).__name__}: {e}", **common)
        try:
            html = bounded(deadline, "calendar_fetch_page", settings.CALENDAR_HTTP_TIMEOUT_SECONDS, deps.fetch_page)
        except CalendarFetchFailed as e:
            return failed("fetch_page", e)
        try:
            page = jc.parse_jpx_page(html)
            version = jc.build_version(page, loaded.version if loaded else None)
        except jc.CalendarParseError as e:
            return failed("parse", e)

        holidays = None
        history = None
        if backfill_from is not None and version.listed_from is not None and backfill_from < version.listed_from:
            try:
                csv = bounded(
                    deadline, "calendar_fetch_holidays", settings.CALENDAR_HTTP_TIMEOUT_SECONDS, deps.fetch_holidays
                )
            except CalendarFetchFailed as e:
                return failed("fetch_holidays", e)
            try:
                holidays = jc.parse_holidays_csv(csv)
                url = jc.xlsx_url(html, settings.HOLIDAY_TRADING_URL)
            except jc.CalendarParseError as e:
                return failed("parse_holidays" if holidays is None else "parse", e)
            if deps.fetch_url is None:
                return failed("fetch_history", "fetch_url が無い")
            try:
                xlsx = bounded(
                    deadline, "calendar_fetch_history", settings.CALENDAR_HTTP_TIMEOUT_SECONDS, deps.fetch_url, url
                )
            except CalendarFetchFailed as e:
                return failed("fetch_history", e)
            try:
                held_days = jc.parse_holiday_trading_xlsx(xlsx)
                rows = jc.history_rows(holidays, held_days, backfill_from, version.listed_from)
            except jc.CalendarParseError as e:
                return failed("parse_history", e)
            mismatch_held = jc.held_days_mismatch(version, held_days)
            if mismatch_held:
                return failed("parse_history", f"xlsx と版の実施有無が違う: {mismatch_held[:5]}")
            history = {
                "history_from": backfill_from,
                "history_until": version.listed_from - timedelta(days=1),
                "history_rows": len(rows),
                "history_closed": sum(1 for r in rows if not r.is_trading_day),
                "history_xlsx": url,
            }
            version = jc.with_history(version, rows)
        elif backfill_from is not None:
            log(
                "INFO",
                "calendar_backfill_skipped",
                backfill_from=backfill_from,
                listed_from=version.listed_from,
                **common,
            )

        mismatch = jc.sessions_mismatch(page.sessions, sessions_of(build_calendar()))
        if mismatch:
            log("ERROR", "calendar_sessions_changed", failure="calendar_sessions_changed", detail=mismatch, **common)

        # 版が無い（初回・--accept で壊れた版を無視した）ときは差分の基準が無いので、差分の検証をしない
        ref = jc.reference_from_version(loaded.version) if loaded is not None else None
        info = {
            "covered_until": version.covered_until,
            "previous_covered_until": ref.covered_until if ref else None,
            "listed_until": version.listed_until,
            "previous_version_at": loaded.version_at if loaded else None,
            **jc.closed_diff(version, ref, today),
            **(history or {}),
        }
        if loaded is not None and version.content_hash == loaded.version.content_hash:
            log("INFO", "calendar_checked", version_at=loaded.version_at, covered_until=version.covered_until, **common)
            return SyncResult("unchanged", loaded)

        if holidays is None:
            try:
                csv = bounded(
                    deadline, "calendar_fetch_holidays", settings.CALENDAR_HTTP_TIMEOUT_SECONDS, deps.fetch_holidays
                )
            except CalendarFetchFailed as e:
                return failed("fetch_holidays", e)
            try:
                holidays = jc.parse_holidays_csv(csv)
            except jc.CalendarParseError as e:
                return failed("parse_holidays", e)

        problems = jc.validate(version, page, holidays, ref, today)
        blocking = [p for p in problems if not (p.overridable and accept_reason)]
        if blocking:
            log(
                "ERROR",
                "calendar_sync_rejected",
                failure="calendar_sync_rejected",
                problems=[f"{p.rule}: {p.detail}" for p in blocking],
                overridable=all(p.overridable for p in blocking),
                dry_run=dry_run or None,
                **info,
                **common,
            )
            return SyncResult("rejected", loaded)
        unverified = sorted(set(page.years) - {d.year for d in holidays})
        if unverified:
            log("WARNING", "calendar_unverified_year", years=unverified, **common)
        accepted = jc.ACCEPTED_OVERRIDE if problems else jc.ACCEPTED_AUTO
        overridden = [f"{p.rule}: {p.detail}" for p in problems] or None
        if dry_run:
            days = None
            if history:
                # 確かめる用に過去年の行の一覧（休業 / 祝日取引）を出す（dry-run だけ）
                days = [
                    f"{r.calendar_date} {'祝日取引' if r.is_trading_day else '休業'} {r.name}"
                    for r in version.rows
                    if r.source == jc.SOURCE_HISTORY and r.calendar_date >= backfill_from
                ]
            log(
                "INFO",
                "calendar_sync_dry_run",
                accepted=accepted,
                overridden=overridden,
                history_days=days,
                **info,
                **common,
            )
            return SyncResult("dry_run", loaded)

        version_at = deps.clock()
        rows = jc.to_rows(version, version_at, accepted)
        try:
            bounded(
                deadline,
                "calendar_write",
                settings.CALENDAR_WRITE_TIMEOUT_SECONDS,
                deps.append_rows,
                settings.CALENDAR_TABLE,
                rows,
            )
        except WriteFailed as e:
            return failed("write", e)
        log(
            "NOTICE",
            "calendar_updated",
            version_at=version_at,
            rows=len(rows),
            accepted=accepted,
            accept_reason=accept_reason if problems else None,
            overridden=overridden,
            **info,
            **common,
        )
        return SyncResult("updated", jc.LoadedVersion(version_at, version, accepted))
    except DeadlineExceeded as e:
        return failed("deadline", f"deadline_exceeded at {e.stage}")


# ---- 本番の依存 ----


def make_real_deps(cfg: settings.Settings, writer=None) -> SyncDeps:
    from .io_bq import BigQueryQuery, BigQueryWriter

    query = BigQueryQuery(cfg.project, cfg.dataset, cfg.bq_location)
    writer = writer or BigQueryWriter(cfg.project, cfg.dataset, cfg.schema_dir)
    http = requests.Session()
    return SyncDeps(
        lambda timeout: load_latest(query, cfg.project, cfg.dataset, timeout),
        lambda timeout: fetch_url(settings.HOLIDAY_TRADING_URL, timeout, http),
        lambda timeout: fetch_url(settings.HOLIDAYS_CSV_URL, timeout, http),
        writer.append,
        fetch_url=lambda url, timeout: fetch_url(url, timeout, http),
    )


def _reason(text: str) -> str:
    text = text.strip()
    if not text or len(text) > 200:
        raise argparse.ArgumentTypeError("理由は 1〜200 文字")
    return text


def _backfill_from(text: str) -> date:
    try:
        d = date.fromisoformat(text)
    except ValueError:
        raise argparse.ArgumentTypeError("YYYY-01-01 の形で指定する") from None
    if (d.month, d.day) != (1, 1):
        raise argparse.ArgumentTypeError("年の初め（YYYY-01-01）を指定する（掲載範囲は年単位）")
    return d


def main(argv: list[str] | None = None, deps: SyncDeps | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m optionchan_ingest.calendar_sync")
    parser.add_argument("--dry-run", action="store_true", help="取得・検証だけ（書かない）")
    parser.add_argument("--accept", type=_reason, metavar="理由", help="前の版との差分の検証を越えて受け入れる")
    parser.add_argument(
        "--backfill-from",
        type=_backfill_from,
        metavar="YYYY-01-01",
        help="この日から版の掲載範囲の前までの過去年の行を足す（#67）",
    )
    try:
        args = parser.parse_args(argv)
    except SystemExit as e:
        return EXIT_USAGE if e.code else EXIT_OK

    # 生 HTML のバケットは使わない（Settings の必須を満たすだけ）
    env = dict(os.environ)
    env["RAW_BUCKET"] = env.get("RAW_BUCKET") or "unused"
    env.pop("INGEST_DRY_RUN", None)
    deps = deps or make_real_deps(settings.Settings.from_env(env))
    deadline = Deadline(settings.CALENDAR_JOB_DEADLINE_SECONDS, time.monotonic)
    now = deps.clock()

    common = {"trigger": "job"}
    try:
        result = run_sync(
            now,
            deps,
            deadline,
            common,
            accept_reason=args.accept,
            dry_run=args.dry_run,
            backfill_from=args.backfill_from,
        )
    except Exception as e:  # 想定外
        log(
            "ERROR",
            "calendar_sync_failed",
            failure="calendar_sync_failed",
            stage="unexpected",
            detail=f"{type(e).__name__}: {e}",
            **common,
        )
        return EXIT_FAILED
    return EXIT_OK if result.outcome in ("updated", "unchanged", "dry_run") else EXIT_FAILED


if __name__ == "__main__":
    sys.exit(main())

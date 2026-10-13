"""取引カレンダーの自動取り込み（#63）: JPX「祝日取引」のページを読み、検証した版を BigQuery に追記する。
ingest は最新版（ビュー trading_calendar_latest）を読み、読めなければ前の値 → イメージの TOML を使う。

- 毎日 settings.CALENDAR_SYNC_AT（06:40 JST）のリクエストで、通常の処理の後に同じ期限の残りで run_sync（app.py）
- 手動: Cloud Run ジョブ calendar-sync
      python -m optionchan_ingest.calendar_sync [--dry-run] [--accept 理由] [--print-toml]
  --dry-run: 取得・検証して結果をログに出すだけ（書かない）
  --accept: 前の版との差分の検証（上書き可の問題）を越えて受け入れる（ページの形・祝日・年末年始の問題は越えない）
  --print-toml: 最新版から config/trading_calendar.toml を作って標準出力に出す（JPX にはアクセスしない）
  終了コード: 0 = 更新・変化なし・dry-run、1 = 失敗・拒否、2 = 引数の誤り

処理（run_sync）:
1. 最新版を読む（無ければ初回。差分の検証の基準はイメージの TOML）
2. ページを取得・解析し、版を組み立てる（前の版の過去年の行を引き継ぐ）。取引時間が TOML と違えば ERROR
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
from datetime import UTC, date, datetime
from pathlib import Path

import requests

from . import jpx_calendar as jc
from . import settings
from .calendar import Calendar, load_calendar, sessions_of, with_closed_dates
from .io_bq import QueryFailed, WriteFailed
from .log import log
from .parser import JST
from .pipeline import Deadline, DeadlineExceeded, bounded

SOURCE_BIGQUERY = "bigquery"
SOURCE_IMAGE = "image"  # BigQuery の版を読めない・使えないので TOML（09:00 に ERROR calendar_fallback）
SOURCE_IMAGE_NEWER = "image_newer"  # TOML の covered_until の方が新しいので意図して TOML（09:00 に WARNING）
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


# ---- ingest が使うカレンダー ----


class CalendarHolder:
    """ingest のインスタンスが使うカレンダー。起動時と毎時 0 分に最新版を読み直す。

    - 読めない（失敗・期限・版が無い）ときは今の値（前に読めた版、無ければイメージの TOML）を使い続ける（WARNING）。
      一度も読めていなければ settings.CALENDAR_RETRY_SECONDS 後の次のリクエストで読み直す（毎時 0 分を待たない）
    - 版が不正（content_hash の不一致・重複など）か、ingest 側の検査（jpx_calendar.ingest_sanity）に当たれば
      使わずに今の値のまま（ERROR calendar_load_rejected。同じ版・同じ理由では 1 回だけ）
    - TOML の covered_until の方が新しければ（手で延ばした）TOML を使う（WARNING calendar_source_conflict）
    """

    def __init__(
        self,
        base: Calendar,
        load: Callable[..., jc.LoadedVersion | None] | None,
        monotonic: Callable[[], float] = time.monotonic,
    ):
        self.base = base
        self.current = base
        self.source = SOURCE_IMAGE
        self.version_at: datetime | None = None
        self.attempted = False
        self._load = load
        self._monotonic = monotonic
        self._retry_at: float | None = None
        self._conflict = False
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
            self._reject(f"data: {e}", stage, common)
            return self._schedule_retry()
        except Exception as e:  # QueryFailed・期限・認証など（一時的なもの）
            detail = "deadline_exceeded" if isinstance(e, DeadlineExceeded) else f"{type(e).__name__}: {e}"
            log("WARNING", "calendar_load_failed", stage=stage, detail=detail, source=self.source, **common)
            return self._schedule_retry()
        if loaded is None:
            log("WARNING", "calendar_load_failed", stage=stage, detail="版が無い", source=self.source, **common)
            return self._schedule_retry()
        self.apply(loaded, common, stage, now)

    def _schedule_retry(self) -> None:
        # 一度でも使える値を得ていれば（BigQuery の版か、意図して TOML を選んだ）毎時の読み直しに任せる
        if self.source == SOURCE_IMAGE:
            self._retry_at = self._monotonic() + settings.CALENDAR_RETRY_SECONDS

    def _reject(self, detail: str, stage: str, common: dict, **fields) -> None:
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

    def apply(self, loaded: jc.LoadedVersion, common: dict, stage: str, now: datetime) -> None:
        version = loaded.version
        if version.covered_until < self.base.covered_until:
            if not self._conflict:
                log(
                    "WARNING",
                    "calendar_source_conflict",
                    stage=stage,
                    version_at=loaded.version_at,
                    covered_until=version.covered_until,
                    image_covered_until=self.base.covered_until,
                    **common,
                )
            self._conflict = True
            self._retry_at = None
            self.current, self.source, self.version_at = self.base, SOURCE_IMAGE_NEWER, None
            return
        problems = jc.ingest_sanity(loaded, self.current.closed_dates, now.astimezone(JST).date())
        if problems:
            self._reject("; ".join(problems), stage, common, version_at=loaded.version_at)
            return self._schedule_retry()
        self._conflict = False
        self._rejected = None
        self._retry_at = None
        changed = self.source != SOURCE_BIGQUERY or self.version_at != loaded.version_at
        self.current = with_closed_dates(self.base, version.closed_dates, version.covered_until)
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
    base: Calendar  # イメージの TOML（取引時間の照合と、版が無いときの差分の基準）
    load_latest: Callable[..., jc.LoadedVersion | None]  # (timeout=)
    fetch_page: Callable[..., bytes]  # (timeout=) JPX「祝日取引」のページ
    fetch_holidays: Callable[..., bytes]  # (timeout=) 内閣府の祝日 CSV
    append_rows: Callable[..., None]  # (table, rows, timeout=)
    clock: Callable[[], datetime] = field(default=lambda: datetime.now(UTC))


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
            # 最新版が壊れている（同じ version_at の重複など）。--accept なら TOML を基準に新しい版を書いて復旧する
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

        mismatch = jc.sessions_mismatch(page.sessions, sessions_of(deps.base))
        if mismatch:
            log("ERROR", "calendar_sessions_changed", failure="calendar_sessions_changed", detail=mismatch, **common)

        if loaded is not None:
            ref = jc.reference_from_version(loaded.version)
        else:
            ref = jc.reference_from_calendar(deps.base.closed_dates, deps.base.covered_until)
        info = {
            "covered_until": version.covered_until,
            "previous_covered_until": ref.covered_until,
            "listed_until": version.listed_until,
            "previous_version_at": loaded.version_at if loaded else None,
            **jc.closed_diff(version, ref, today),
        }
        if loaded is not None and version.content_hash == loaded.version.content_hash:
            log("INFO", "calendar_checked", version_at=loaded.version_at, covered_until=version.covered_until, **common)
            return SyncResult("unchanged", loaded)

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
            log("INFO", "calendar_sync_dry_run", accepted=accepted, overridden=overridden, **info, **common)
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


def make_real_deps(cfg: settings.Settings, base: Calendar, writer=None) -> SyncDeps:
    from .io_bq import BigQueryQuery, BigQueryWriter

    query = BigQueryQuery(cfg.project, cfg.dataset, cfg.bq_location)
    writer = writer or BigQueryWriter(cfg.project, cfg.dataset, cfg.schema_dir)
    http = requests.Session()
    return SyncDeps(
        base,
        lambda timeout: load_latest(query, cfg.project, cfg.dataset, timeout),
        lambda timeout: fetch_url(settings.HOLIDAY_TRADING_URL, timeout, http),
        lambda timeout: fetch_url(settings.HOLIDAYS_CSV_URL, timeout, http),
        writer.append,
    )


def _reason(text: str) -> str:
    text = text.strip()
    if not text or len(text) > 200:
        raise argparse.ArgumentTypeError("理由は 1〜200 文字")
    return text


def main(argv: list[str] | None = None, deps: SyncDeps | None = None, base_path: Path | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m optionchan_ingest.calendar_sync")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="取得・検証だけ（書かない）")
    mode.add_argument("--print-toml", action="store_true", help="最新版から trading_calendar.toml を作って出す")
    parser.add_argument("--accept", type=_reason, metavar="理由", help="前の版との差分の検証を越えて受け入れる")
    try:
        args = parser.parse_args(argv)
    except SystemExit as e:
        return EXIT_USAGE if e.code else EXIT_OK
    if args.accept and args.print_toml:
        print("--accept と --print-toml は一緒に使えない", file=sys.stderr)
        return EXIT_USAGE

    # 生 HTML のバケットは使わない（Settings の必須を満たすだけ）
    env = dict(os.environ)
    env["RAW_BUCKET"] = env.get("RAW_BUCKET") or "unused"
    env.pop("INGEST_DRY_RUN", None)
    cfg = settings.Settings.from_env(env)
    path = base_path or cfg.calendar_path
    base = load_calendar(path)
    deps = deps or make_real_deps(cfg, base)
    deadline = Deadline(settings.CALENDAR_JOB_DEADLINE_SECONDS, time.monotonic)
    now = deps.clock()

    if args.print_toml:
        try:
            loaded = bounded(deadline, "calendar_load", settings.CALENDAR_LOAD_TIMEOUT_SECONDS, deps.load_latest)
        except (QueryFailed, jc.CalendarDataError, DeadlineExceeded) as e:
            print(f"最新版を読めない: {type(e).__name__}: {e}", file=sys.stderr)
            return EXIT_FAILED
        if loaded is None:
            print("最新版が無い", file=sys.stderr)
            return EXIT_FAILED
        text = Path(path).read_text(encoding="utf-8")
        sys.stdout.write(jc.render_toml(text, loaded.version, now.astimezone(JST).date()))
        return EXIT_OK

    common = {"trigger": "job"}
    try:
        result = run_sync(now, deps, deadline, common, accept_reason=args.accept, dry_run=args.dry_run)
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

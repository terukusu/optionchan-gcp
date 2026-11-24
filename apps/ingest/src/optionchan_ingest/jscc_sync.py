"""JSCC の VaR 証拠金計算用パラメータファイル（日経225 分）の毎日の取り込み（#164）。

JSCC の一覧のページから 1600 版の BPF_IDX（zip）・VaRParameter・All-Instruments-VaR を取り、Product Group NK225 の
銘柄のシナリオの値を BigQuery（optionchan_tokyo の jscc_*）に営業日のパーティションごと置き換えで書く。
1 枚の売り・買いの証拠金を計算して All-Instruments-VaR と照合し、件数を jscc_ingest_runs に記録する。

- 定期: Cloud Scheduler jscc-sync（平日 18:30 JST）が Cloud Run ジョブ jscc-sync を起動（引数なし）
- 手動: python -m optionchan_ingest.jscc_sync [--dry-run] [--date YYYY-MM-DD ...]
  引数なし: settings.JSCC_START_DATE 以降で jscc_ingest_runs に succeeded の行が無い日（3 つのファイルが揃った日）を
      古い順に最大 settings.JSCC_MAX_DATES_PER_RUN 日（trigger = auto）。対象が無ければ何もしない（祝日など）
  --date: 指定の日を取り直す（成功済みでも。パーティションの置き換えなので重複しない。trigger = manual）。
      JSCC_START_DATE より前の日（一覧の archive にある日）も入れられる。自動の対象にはならない（#165）
  --dry-run: 取得・解析・照合・当てはめまでしてログに出すだけ（BigQuery に書かない。取引カレンダーは読む）
  終了コード: 0 = すべて成功・対象なし・dry-run、1 = 1 日でも失敗（一覧・クエリを含む）、2 = 引数の誤り・一覧に無い日

日の処理の前に取引カレンダー（trading_calendar_latest）を 1 回読む（SQ・h の営業日。失敗は stage=calendar で止める）。
1 日の処理: VaRParameter・All-Instruments-VaR を取得・解析 → BPF の zip を一時ファイルに落とし、展開せずに行を順に読んで
NK225 の銘柄を NDJSON に書きながら照合し、当てはめに要る銘柄を残す → 限月ごとのシナリオの r・g を当てはめる（#168。
jscc_factor.py。ロードの前なので、当てはめの失敗ではどの表も書かない）→ ロード（jscc_var_parameter →
jscc_var_scenario_set → jscc_var_scenario → jscc_var_scenario_factor、どれも WRITE_TRUNCATE で <表>$YYYYMMDD）→
jscc_ingest_runs に succeeded を追記 → NOTICE jscc_ingested。
照合の不一致は ERROR jscc_margin_mismatch（書き込みは行う）。失敗は ERROR jscc_sync_failed（stage 付き）と
jscc_ingest_runs の failed の行で、次の日へ進む。4 表は別々のロードなので、読み手は succeeded の日だけを使う。
ダウンロードしたファイルは信頼しない: 大きさの上限、zip は期待する 1 メンバーだけを読む、一覧の href は同じ
ディレクトリのファイル名だけ。ログには件数・銘柄コードまでを出し、価格・損益の値を出さない
"""

import argparse
import gzip
import io
import json
import os
import resource
import sys
import tempfile
import time
import uuid
import zipfile
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta

import requests

from . import calendar_sync, jscc, jscc_factor, settings
from .io_bq import QueryFailed
from .jpx_calendar import CalendarDataError
from .log import log
from .parser import JST
from .pipeline import Deadline, DeadlineExceeded, bounded

EXIT_OK, EXIT_FAILED, EXIT_USAGE = 0, 1, 2
TRIGGER_AUTO, TRIGGER_MANUAL = "auto", "manual"
KINDS = frozenset({"bpf", "parameter", "margin"})
DETAIL_MAX = 1000


class JsccFetchFailed(Exception):
    """JSCC のページ・ファイルが取れない（接続断・タイムアウト・200 以外・大きさの上限超え）"""


# ---- HTTP ----


def _get(url: str, timeout: float, session, sink, max_bytes: int) -> int:
    """url の本体を sink に書き、バイト数を返す。失敗は JsccFetchFailed"""
    connect = min(settings.JSCC_CONNECT_TIMEOUT_SECONDS, timeout / 2)
    read = min(settings.JSCC_READ_TIMEOUT_SECONDS, timeout - connect)
    name = url.rsplit("/", 1)[-1]
    try:
        # リダイレクトはたどらない（一覧で確かめた URL のホスト・ディレクトリの外に出ない。3xx は失敗）
        with session.get(url, timeout=(connect, read), stream=True, allow_redirects=False) as response:
            if response.status_code != 200:
                raise JsccFetchFailed(f"{name}: HTTP {response.status_code}")
            length = response.headers.get("Content-Length")
            encoded = bool(response.headers.get("Content-Encoding"))
            if length is not None and length.isdigit() and int(length) > max_bytes:
                raise JsccFetchFailed(f"{name}: 大きさ {length} が上限 {max_bytes} を超える")
            size = 0
            for chunk in response.iter_content(chunk_size=1 << 20):
                size += len(chunk)
                if size > max_bytes:
                    raise JsccFetchFailed(f"{name}: 大きさが上限 {max_bytes} を超える")
                sink.write(chunk)
            if length is not None and length.isdigit() and not encoded and int(length) != size:
                raise JsccFetchFailed(f"{name}: 途中で切れた（{size} / {length}）")
    except requests.RequestException as e:
        raise JsccFetchFailed(f"{name}: {type(e).__name__}") from e
    return size


def fetch_bytes(url: str, timeout: float, session) -> bytes:
    """一覧のページ・CSV（小さいもの）"""
    buf = io.BytesIO()
    _get(url, timeout, session, buf, settings.JSCC_MAX_TEXT_BYTES)
    return buf.getvalue()


def download_file(url: str, timeout: float, session):
    """BPF の zip を一時ファイル（Cloud Run ではメモリ）に落とし、先頭に戻して返す（閉じるのは呼び出し側）"""
    f = tempfile.TemporaryFile()
    try:
        _get(url, timeout, session, f, settings.JSCC_MAX_ZIP_BYTES)
    except BaseException:
        f.close()
        raise
    f.seek(0)
    return f


# ---- 成功済みの日（BigQuery） ----


def succeeded_sql(project: str, dataset: str) -> str:
    table = f"`{project}.{dataset}.{settings.JSCC_RUNS_TABLE}`"
    return (
        f"SELECT DISTINCT CAST(business_date AS STRING) FROM {table} "
        "WHERE business_date >= @start AND status = 'succeeded' AND file_version = @version"
    )


def succeeded_dates(query, project: str, dataset: str, start: date, timeout: float) -> set[date]:
    values = query.query(
        succeeded_sql(project, dataset), {"start": start, "version": settings.JSCC_FILE_VERSION}, timeout=timeout
    )
    try:
        return {date.fromisoformat(v[0]) for v in values}
    except (TypeError, ValueError, IndexError) as e:
        raise QueryFailed(f"成功済みの日が読めない: {type(e).__name__}") from None


# ---- 取引カレンダー（SQ・h の営業日。#168） ----

CALENDAR_LOOKAHEAD_DAYS = 7  # 営業日 + 7 日が covered_until を超えたら WARNING jscc_calendar_outdated


def load_calendar(query, project: str, dataset: str, timeout: float) -> tuple[frozenset[date], date]:
    """(営業日でない平日 = 最新版の全行の日付, covered_until)。祝日取引の日も行なので営業日でない（#149 の現物の
    営業日と同じ）。版が無ければ CalendarDataError、読めなければ QueryFailed"""
    loaded = calendar_sync.load_latest(query, project, dataset, timeout)
    if loaded is None:
        raise CalendarDataError("取引カレンダーの版が無い")
    return frozenset(r.calendar_date for r in loaded.version.rows), loaded.version.covered_until


# ---- 取り込み ----


@dataclass
class SyncDeps:
    fetch: Callable[..., bytes]  # (url, timeout=) 一覧のページ・CSV
    download: Callable[..., object]  # (url, timeout=) BPF の zip（先頭に戻したファイル）
    query_succeeded: Callable[..., set]  # (start, timeout=) succeeded の行がある営業日
    load: Callable[..., object]  # (table, partition, fileobj, job_id, timeout=) パーティションの置き換え
    append_run: Callable[..., None]  # (table, rows, timeout=) jscc_ingest_runs への追記
    load_holidays: Callable[..., tuple]  # (timeout=) → (営業日でない平日, covered_until)。取引カレンダーの最新版
    clock: Callable[[], datetime] = field(default=lambda: datetime.now(UTC))
    new_id: Callable[[], str] = field(default=lambda: str(uuid.uuid4()))


@dataclass
class DayResult:
    business_date: date
    status: str  # succeeded / failed / dry_run
    stage: str | None = None
    stats: dict = field(default_factory=dict)


@dataclass
class SyncResult:
    outcome: str  # ok / nothing / failed / not_listed
    days: list[DayResult] = field(default_factory=list)


class _Ndjson:
    """ロード用の NDJSON（settings.JSCC_LOAD_GZIP なら gzip）を一時ファイルに書く"""

    def __init__(self):
        self.file = tempfile.TemporaryFile()
        self.gzip = settings.JSCC_LOAD_GZIP
        self._out = gzip.GzipFile(fileobj=self.file, mode="wb", compresslevel=6, mtime=0) if self.gzip else self.file
        self.rows = 0

    def write(self, row: dict) -> None:
        # NaN・inf は送らない（ValueError で段の失敗にする）
        self._out.write(json.dumps(row, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8"))
        self._out.write(b"\n")
        self.rows += 1

    def finish(self):
        if self.gzip and not self._out.closed:
            self._out.close()  # fileobj は閉じない
        self.file.seek(0)
        return self.file

    def close(self) -> None:
        if self.gzip and not self._out.closed:
            self._out.close()
        self.file.close()


def _text(data: bytes, what: str) -> str:
    try:
        return data.decode("utf-8-sig")
    except UnicodeDecodeError:
        raise jscc.JsccFormatError(f"{what} が UTF-8 でない") from None


def run_row(
    *,
    run_id: str,
    business_date: date,
    trigger: str,
    execution: str | None,
    status: str,
    stage: str | None,
    detail: str | None,
    started_at: datetime,
    finished_at: datetime,
    stats: dict,
) -> dict:
    """jscc_ingest_runs の 1 行（schema/tokyo/jscc_ingest_runs.json の列と 1 対 1）"""
    return {
        "jscc_run_id": run_id,
        "business_date": business_date,
        "file_version": settings.JSCC_FILE_VERSION,
        "trigger": trigger,
        "execution": execution,
        "status": status,
        "stage": stage,
        "detail": detail[:DETAIL_MAX] if detail else None,
        "started_at": started_at,
        "finished_at": finished_at,
        **{k: stats.get(k) for k in RUN_STATS},
    }


RUN_STATS = (
    "rows_scenario",
    "rows_parameter",
    "scenarios",
    "scenarios_h",
    "scenarios_s",
    "margin_matched",
    "margin_mismatched",
    "margin_ref_only",
    "margin_bpf_only",
    "bpf_bytes",
    "rows_factor",
    "rows_factor_fitted",
)


def read_bpf(
    zfile,
    business_date: date,
    records,
    reference: dict,
    run_id: str,
    out: _Ndjson,
    collector: jscc_factor.FactorCollector | None = None,
) -> tuple:
    """zip の中の CSV を読み、NK225 の銘柄の行を out に書く。(BpfSummary, 計算した 1 枚の証拠金) を返す。
    collector があれば各銘柄を渡す（当てはめに要るものだけを残す）"""
    expected = f"BPF_IDX_{business_date:%Y%m%d}_{settings.JSCC_FILE_VERSION}.csv"
    with zipfile.ZipFile(zfile) as z:
        infos = z.infolist()
        if len(infos) != 1 or infos[0].filename != expected:
            raise jscc.JsccFormatError(f"zip の中身が想定と違う（メンバー {len(infos)} 個）")
        if infos[0].file_size > settings.JSCC_MAX_MEMBER_BYTES:
            raise jscc.JsccFormatError(f"zip の中の CSV が上限 {settings.JSCC_MAX_MEMBER_BYTES} を超える")
        computed: dict[str, tuple[int, int]] = {}
        stress: dict[str, tuple[float, int]] = {}
        short_floor = business_date >= settings.JSCC_OPTION_SHORT_FLOOR_FROM
        with z.open(infos[0]) as raw:
            lines = io.TextIOWrapper(raw, encoding="utf-8", newline="")
            reader = jscc.BpfReader(lines, product_group=settings.JSCC_PRODUCT_GROUP, business_date=business_date)
            is_hist = None
            for inst in reader:
                if is_hist is None:
                    is_hist = [t == "H" for _, t in reader.scenarios]
                group = inst.aggregation_group
                if group not in stress:
                    stress[group] = jscc.stress_params(records, group)
                confidence, n_stress = stress[group]
                computed[inst.instrument_code] = jscc.instrument_margins(
                    inst, is_hist, confidence, n_stress, short_floor=short_floor
                )
                out.write(
                    jscc.scenario_row(
                        inst,
                        business_date=business_date,
                        file_version=settings.JSCC_FILE_VERSION,
                        run_id=run_id,
                        reference=reference.get(inst.instrument_code),
                    )
                )
                if collector is not None:
                    collector.add(inst)
    return reader.summary, computed


def process_day(
    business_date: date,
    urls: dict[str, str],
    deps: SyncDeps,
    deadline: Deadline,
    common: dict,
    *,
    trigger: str,
    execution: str | None,
    dry_run: bool,
    holidays: frozenset[date] = frozenset(),
) -> DayResult:
    """1 営業日を取り込む。想定した失敗も想定外の例外もログ（と jscc_ingest_runs の failed の行）にして返す。
    holidays は取引カレンダーの営業日でない平日（当てはめの SQ・h）"""
    run_id = deps.new_id()
    started_at = deps.clock()
    ctx = {"business_date": business_date, "jscc_run_id": run_id, "dry_run": dry_run or None, **common}
    stats: dict = {}
    extra: dict = {}  # NOTICE にだけ出す（jscc_ingest_runs には書かない）
    stage = "fetch_parameter"
    files = []
    version, group = settings.JSCC_FILE_VERSION, settings.JSCC_PRODUCT_GROUP
    try:
        data = bounded(
            deadline, "jscc_fetch_parameter", settings.JSCC_FETCH_TIMEOUT_SECONDS, deps.fetch, urls["parameter"]
        )
        stage = "parse_parameter"
        records = jscc.parse_var_parameter(_text(data, "VaRParameter"))
        stage = "fetch_margin"
        data = bounded(deadline, "jscc_fetch_margin", settings.JSCC_FETCH_TIMEOUT_SECONDS, deps.fetch, urls["margin"])
        stage = "parse_margin"
        reference = jscc.parse_all_instruments_var(_text(data, "All-Instruments-VaR"), group)
        del data
        stage = "fetch_bpf"
        zfile = bounded(deadline, "jscc_fetch_bpf", settings.JSCC_DOWNLOAD_TIMEOUT_SECONDS, deps.download, urls["bpf"])
        files.append(zfile)
        zfile.seek(0, io.SEEK_END)
        stats["bpf_bytes"] = zfile.tell()
        zfile.seek(0)

        stage = "parse_bpf"
        scenario_out = _Ndjson()
        files.append(scenario_out)
        collector = jscc_factor.FactorCollector()
        summary, computed = read_bpf(zfile, business_date, records, reference, run_id, scenario_out, collector)
        files.remove(zfile)
        zfile.close()  # メモリ（Cloud Run の /tmp）を先に空ける
        check = jscc.compare_margins(computed, reference)
        set_out = _Ndjson()
        files.append(set_out)
        set_out.write(jscc.scenario_set_row(summary, business_date=business_date, file_version=version, run_id=run_id))
        param_out = _Ndjson()
        files.append(param_out)
        for row in jscc.parameter_rows(records, business_date=business_date, file_version=version, run_id=run_id):
            param_out.write(row)
        stats.update(
            rows_scenario=scenario_out.rows,
            rows_parameter=param_out.rows,
            scenarios=len(summary.scenarios),
            scenarios_h=sum(1 for _, t in summary.scenarios if t == "H"),
            scenarios_s=sum(1 for _, t in summary.scenarios if t == "S"),
            margin_matched=check.matched,
            margin_mismatched=check.mismatched,
            margin_ref_only=check.ref_only,
            margin_bpf_only=check.bpf_only,
        )

        stage = "factor"
        factor_out, factor, seconds = _factor(
            collector, summary, records, reference, business_date, holidays, run_id, deadline
        )
        files.append(factor_out)
        del collector  # 残した銘柄（pnl の参照）を空ける
        stats.update(rows_factor=factor_out.rows, rows_factor_fitted=factor.fitted)
        extra.update(_factor_log(factor, seconds, ctx))

        if not dry_run:
            stage = "load"
            ymd = f"{business_date:%Y%m%d}"
            for table, out in (
                (settings.JSCC_PARAMETER_TABLE, param_out),
                (settings.JSCC_SCENARIO_SET_TABLE, set_out),
                (settings.JSCC_SCENARIO_TABLE, scenario_out),
                (settings.JSCC_FACTOR_TABLE, factor_out),
            ):
                job_id = f"jscc_{table}_{ymd}_{run_id.replace('-', '')}"
                bounded(
                    deadline,
                    f"jscc_load_{table}",
                    settings.JSCC_LOAD_TIMEOUT_SECONDS,
                    deps.load,
                    table,
                    business_date,
                    out.finish(),
                    job_id,
                )
            stage = "record"
            row = run_row(
                run_id=run_id,
                business_date=business_date,
                trigger=trigger,
                execution=execution,
                status="succeeded",
                stage=None,
                detail=None,
                started_at=started_at,
                finished_at=deps.clock(),
                stats=stats,
            )
            bounded(
                deadline,
                "jscc_record",
                settings.JSCC_RECORD_TIMEOUT_SECONDS,
                deps.append_run,
                settings.JSCC_RUNS_TABLE,
                [row],
            )
    except Exception as e:
        if isinstance(e, DeadlineExceeded):
            stage, detail = "deadline", f"deadline_exceeded at {e.stage}"
        else:
            detail = f"{type(e).__name__}: {e}"
        log("ERROR", "jscc_sync_failed", failure="jscc_sync_failed", stage=stage, detail=detail[:DETAIL_MAX], **ctx)
        if not dry_run:
            _record_failure(
                deps, deadline, ctx, run_id, business_date, trigger, execution, stage, detail, started_at, stats
            )
        return DayResult(business_date, "failed", stage, stats)
    finally:
        for f in files:
            f.close()

    log(
        "NOTICE",
        "jscc_ingested",
        file_version=version,
        product_group=group,
        elapsed_seconds=round((deps.clock() - started_at).total_seconds(), 1),
        **stats,
        **extra,
        **ctx,
    )
    if check.mismatched:
        log(
            "ERROR",
            "jscc_margin_mismatch",
            failure="jscc_margin_mismatch",
            mismatched=check.mismatched,
            matched=check.matched,
            codes=list(check.mismatched_codes),
            **ctx,
        )
    return DayResult(business_date, "dry_run" if dry_run else "succeeded", None, stats)


def _factor(collector, summary, records, reference, business_date, holidays, run_id, deadline):
    """限月ごとの r・g を当てはめて NDJSON に書く。(NDJSON, FactorResult, 秒)。期限は限月ごとに見る（後のロードと
    記録の分 settings.JSCC_FACTOR_MIN_REMAINING_SECONDS を残す）"""
    started = time.monotonic()

    def check() -> None:
        if deadline.remaining() < settings.JSCC_FACTOR_MIN_REMAINING_SECONDS:
            raise DeadlineExceeded("jscc_factor")

    stress = {group: jscc.stress_params(records, group) for group in sorted(collector.groups)}
    result = jscc_factor.factor_rows(
        collector,
        scenarios=summary.scenarios,
        stress=stress,
        reference=reference,
        business_date=business_date,
        holidays=holidays,
        file_version=settings.JSCC_FILE_VERSION,
        run_id=run_id,
        rate=settings.JSCC_FACTOR_RATE,
        check=check,
    )
    out = _Ndjson()
    try:
        for row in result.rows:
            out.write(row)
    except BaseException:
        out.close()
        raise
    return out, result, round(time.monotonic() - started, 1)


def _factor_log(result, seconds: float, ctx: dict) -> dict:
    """当てはめの件数（NOTICE に足す分）。月次・先物の限月で行が無い・T₁ ≤ 0 以外で g が空なら WARNING
    jscc_factor_incomplete（限月コードと理由ごとの件数だけ）"""
    weekly = 0
    incomplete: dict[str, list[str]] = {}
    for reason, keys in result.skipped.items():
        for key in keys:
            product, period = key.split(":", 1)
            if product in settings.JSCC_FACTOR_PRODUCTS and product != settings.JSCC_FACTOR_MONTHLY_PRODUCT:
                weekly += 1
            elif reason != jscc_factor.T1_EXPIRED:
                incomplete.setdefault(reason, []).append(key)
    if incomplete:
        log(
            "WARNING",
            "jscc_factor_incomplete",
            periods=sorted({k for keys in incomplete.values() for k in keys}),
            reasons={reason: len(keys) for reason, keys in sorted(incomplete.items())},
            **ctx,
        )
    return {
        "factor_seconds": seconds,
        "factor_skipped_weekly": weekly,
        "factor_empty_scenarios": result.empty_scenarios,
        "max_rss_mb": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024,
    }


def _record_failure(deps, deadline, ctx, run_id, business_date, trigger, execution, stage, detail, started_at, stats):
    """失敗の行を追記する。追記にも失敗したら WARNING だけ（例外を外に出さない。ERROR は既に出している）"""
    row = run_row(
        run_id=run_id,
        business_date=business_date,
        trigger=trigger,
        execution=execution,
        status="failed",
        stage=stage,
        detail=detail,
        started_at=started_at,
        finished_at=deps.clock(),
        stats=stats,
    )
    try:
        bounded(
            deadline,
            "jscc_record",
            settings.JSCC_RECORD_TIMEOUT_SECONDS,
            deps.append_run,
            settings.JSCC_RUNS_TABLE,
            [row],
        )
    except Exception as e:
        log("WARNING", "jscc_run_record_failed", detail=f"{type(e).__name__}: {e}"[:DETAIL_MAX], **ctx)


def collect_listing(deps: SyncDeps, deadline: Deadline, common: dict) -> tuple[dict, bool]:
    """4 つの一覧のページを日ごとにまとめる（同じ日・同じ種類は前のページを使う）。(一覧, 取れないページがあった)"""
    listing: dict[date, dict[str, str]] = {}
    failed = False
    for url in settings.JSCC_LIST_URLS:
        try:
            html = bounded(deadline, "jscc_list", settings.JSCC_FETCH_TIMEOUT_SECONDS, deps.fetch, url)
        except (JsccFetchFailed, DeadlineExceeded) as e:
            detail = "deadline_exceeded" if isinstance(e, DeadlineExceeded) else str(e)
            log("ERROR", "jscc_sync_failed", failure="jscc_sync_failed", stage="list", detail=detail, url=url, **common)
            failed = True
            continue
        found = jscc.parse_listing(html.decode("utf-8", errors="replace"), url, settings.JSCC_FILE_VERSION)
        for day, kinds in found.items():
            slot = listing.setdefault(day, {})
            for kind, file_url in kinds.items():
                slot.setdefault(kind, file_url)
    return listing, failed


def run_sync(
    now: datetime,
    deps: SyncDeps,
    deadline: Deadline,
    common: dict,
    *,
    dates: list[date] | None = None,
    dry_run: bool = False,
) -> SyncResult:
    """dates が無ければ未取り込みの日（自動）、あれば指定の日を取り込む。失敗はログに出して SyncResult で返す。
    common の execution（Cloud Run ジョブの実行名）は jscc_ingest_runs にも書く"""
    execution = common.get("execution")
    trigger = TRIGGER_MANUAL if dates else TRIGGER_AUTO
    common = {**common, "trigger": trigger}
    today = now.astimezone(JST).date()

    listing, failed = collect_listing(deps, deadline, common)
    if not listing:
        log(
            "ERROR",
            "jscc_sync_failed",
            failure="jscc_sync_failed",
            stage="list",
            detail="一覧にファイルが無い",
            **common,
        )
        return SyncResult("failed")

    remaining: list[date] = []
    if dates:
        missing = sorted(d for d in set(dates) if set(listing.get(d, {})) != KINDS)
        if missing:
            log(
                "WARNING",
                "jscc_date_not_listed",
                dates=missing,
                found={str(d): sorted(listing.get(d, {})) for d in missing},
                **common,
            )
            return SyncResult("not_listed")
        targets = sorted(set(dates))
    else:
        try:
            done = bounded(
                deadline,
                "jscc_query",
                settings.JSCC_QUERY_TIMEOUT_SECONDS,
                deps.query_succeeded,
                settings.JSCC_START_DATE,
            )
        except (QueryFailed, DeadlineExceeded) as e:
            detail = "deadline_exceeded" if isinstance(e, DeadlineExceeded) else f"{type(e).__name__}: {e}"
            log("ERROR", "jscc_sync_failed", failure="jscc_sync_failed", stage="query", detail=detail, **common)
            return SyncResult("failed")
        pending = [d for d in sorted(listing) if settings.JSCC_START_DATE <= d <= today and d not in done]
        incomplete = [d for d in pending if set(listing[d]) != KINDS]
        if incomplete:
            # 翌日以降の実行で再び対象になる（掲載の途中・片方のページだけ取れなかった）
            log(
                "WARNING",
                "jscc_files_incomplete",
                dates=incomplete,
                found={str(d): sorted(listing[d]) for d in incomplete},
                **common,
            )
        ready = [d for d in pending if d not in incomplete]
        targets, remaining = ready[: settings.JSCC_MAX_DATES_PER_RUN], ready[settings.JSCC_MAX_DATES_PER_RUN :]
        if not targets:
            log("INFO", "jscc_nothing_to_do", incomplete=len(incomplete), **common)
            return SyncResult("failed" if failed else "nothing")

    try:
        holidays, covered_until = bounded(
            deadline, "jscc_calendar", settings.JSCC_QUERY_TIMEOUT_SECONDS, deps.load_holidays
        )
    except (QueryFailed, CalendarDataError, DeadlineExceeded) as e:
        detail = "deadline_exceeded" if isinstance(e, DeadlineExceeded) else f"{type(e).__name__}: {e}"
        log(
            "ERROR",
            "jscc_sync_failed",
            failure="jscc_sync_failed",
            stage="calendar",
            detail=detail[:DETAIL_MAX],
            **common,
        )
        return SyncResult("failed")
    if max(targets) + timedelta(days=CALENDAR_LOOKAHEAD_DAYS) > covered_until:
        log("WARNING", "jscc_calendar_outdated", covered_until=covered_until, dates=targets, **common)

    results = []
    for i, day in enumerate(targets):
        if deadline.remaining() < settings.JSCC_MIN_REMAINING_SECONDS:
            skipped = targets[i:]
            log("WARNING", "jscc_deadline_skip", dates=skipped, **common)
            remaining = skipped + remaining
            break
        results.append(
            process_day(
                day,
                listing[day],
                deps,
                deadline,
                common,
                trigger=trigger,
                execution=execution,
                dry_run=dry_run,
                holidays=holidays,
            )
        )
    failed = failed or any(r.status == "failed" for r in results)
    log(
        "INFO",
        "jscc_sync_finished",
        processed=[str(r.business_date) for r in results],
        failed_dates=[str(r.business_date) for r in results if r.status == "failed"] or None,
        remaining=[str(d) for d in remaining] or None,
        dry_run=dry_run or None,
        **common,
    )
    return SyncResult("failed" if failed else "ok", results)


# ---- 本番の依存 ----


def make_real_deps(cfg: settings.Settings) -> SyncDeps:
    from .io_bq import BigQueryLoader, BigQueryQuery, BigQueryWriter

    query = BigQueryQuery(cfg.project, cfg.dataset, cfg.bq_location)
    loader = BigQueryLoader(cfg.project, cfg.dataset, cfg.bq_location)
    writer = BigQueryWriter(cfg.project, cfg.dataset, cfg.schema_dir)
    http = requests.Session()
    http.headers.update(settings.JSCC_HEADERS)
    return SyncDeps(
        fetch=lambda url, timeout: fetch_bytes(url, timeout, http),
        download=lambda url, timeout: download_file(url, timeout, http),
        query_succeeded=lambda start, timeout: succeeded_dates(query, cfg.project, cfg.dataset, start, timeout),
        load=lambda table, partition, fileobj, job_id, timeout: loader.load_ndjson(
            table, partition, fileobj, job_id, gzip=settings.JSCC_LOAD_GZIP, timeout=timeout
        ),
        append_run=writer.append,
        load_holidays=lambda timeout: load_calendar(query, cfg.project, cfg.dataset, timeout),
    )


def _date_arg(text: str) -> date:
    try:
        return date.fromisoformat(text)
    except ValueError:
        raise argparse.ArgumentTypeError("YYYY-MM-DD の形で指定する") from None


def main(argv: list[str] | None = None, deps: SyncDeps | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m optionchan_ingest.jscc_sync")
    parser.add_argument("--dry-run", action="store_true", help="取得・解析・照合・当てはめだけ（BigQuery に書かない）")
    parser.add_argument(
        "--date",
        dest="dates",
        type=_date_arg,
        action="append",
        metavar="YYYY-MM-DD",
        help="この営業日を取り直す（繰り返し可。成功済みの日も置き換える）",
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
    deadline = Deadline(settings.JSCC_JOB_DEADLINE_SECONDS, time.monotonic)
    execution = os.environ.get("CLOUD_RUN_EXECUTION") or None
    common = {"execution": execution}
    try:
        result = run_sync(deps.clock(), deps, deadline, common, dates=args.dates, dry_run=args.dry_run)
    except Exception as e:  # 想定外
        log(
            "ERROR",
            "jscc_sync_failed",
            failure="jscc_sync_failed",
            stage="unexpected",
            detail=f"{type(e).__name__}: {e}"[:DETAIL_MAX],
            **common,
        )
        return EXIT_FAILED
    return {"ok": EXIT_OK, "nothing": EXIT_OK, "not_listed": EXIT_USAGE}.get(result.outcome, EXIT_FAILED)


if __name__ == "__main__":
    sys.exit(main())

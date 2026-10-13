"""再処理ジョブ（Cloud Run ジョブ reprocess、手動実行。target.md 3.1、#34）。

    python -m optionchan_ingest.reprocess --start <ISO8601> --end <ISO8601> [--only-missing] [--dry-run] [--limit N]

名前の時刻が [start, end) の生 HTML を読み直し、今のイメージの PARSER_VERSION で解析して追記する（trigger = reprocess、
新しい ingest_run_id。置換は追記のみ = target.md 3.1 の (b)。同じスナップショットの版は最新版の選択で 1 つにする）。
JPX にはアクセスしない。定期起動しない。

- --start / --end はタイムゾーン付き必須（例 2026-10-13T08:45:00+09:00 / 2026-10-12T23:45:00Z）。naive は終了コード 2
- --only-missing: ingest_runs に完了記録のあるものを飛ばす（回収と同じ判定。作成からの経過時間では絞らない）
- --dry-run: 対象の件数と先頭の名前をログに出すだけ（GCS の一覧と BigQuery のクエリだけ。読み出し・書き込み 0）
- --limit N: 処理する件数の上限（古い順）

終了コード: 0 = すべて成功、1 = 解析・書き込み・読み出しの失敗が 1 件以上（または一覧・クエリの失敗）、
2 = 引数の誤り、3 = 期限（settings.REPROCESS_DEADLINE_SECONDS）で途中終了（残りは --only-missing で再実行）
"""

import argparse
import os
import sys
from collections import Counter
from collections.abc import Callable
from datetime import datetime

from . import PARSER_VERSION, settings
from .io_bq import QueryFailed
from .io_gcs import StorageFailed
from .log import log
from .pipeline import Deadline, DeadlineExceeded
from .recovery import (
    PARSE_FAILED,
    RECOVERED,
    STORAGE_FAILED,
    UNEXPECTED,
    WRITE_FAILED,
    RecoveryDeps,
    Window,
    list_window,
    make_real_deps,
    query_completed,
    try_one,
)

TRIGGER = "reprocess"
EXIT_OK, EXIT_FAILED, EXIT_USAGE, EXIT_DEADLINE = 0, 1, 2, 3


def _aware(text: str) -> datetime:
    try:
        value = datetime.fromisoformat(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"ISO8601 でない: {text!r}") from None
    if value.tzinfo is None or value.utcoffset() is None:
        raise argparse.ArgumentTypeError(f"タイムゾーンが無い（+09:00 や Z を付ける）: {text!r}")
    return value


def _positive(text: str) -> int:
    try:
        value = int(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"整数でない: {text!r}") from None
    if value < 1:
        raise argparse.ArgumentTypeError(f"1 以上: {text!r}")
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m optionchan_ingest.reprocess",
        description="保存済みの生 HTML から期間を指定して作り直す（追記のみ。JPX にはアクセスしない）",
    )
    parser.add_argument(
        "--start", required=True, type=_aware, help="名前の時刻の始め（含む。タイムゾーン付き ISO8601）"
    )
    parser.add_argument(
        "--end", required=True, type=_aware, help="名前の時刻の終わり（含まない。タイムゾーン付き ISO8601）"
    )
    parser.add_argument("--only-missing", action="store_true", help="完了記録（ingest_runs）のあるものを飛ばす")
    parser.add_argument("--dry-run", action="store_true", help="対象の件数と名前を出すだけ（読み出し・書き込みなし）")
    parser.add_argument("--limit", type=_positive, default=None, help="処理する件数の上限（古い順）")
    return parser


def parse_args(argv: list[str]) -> argparse.Namespace | int:
    """引数を解釈する。誤り・--help は終了コード（int）を返す"""
    try:
        args = build_parser().parse_args(argv)
    except SystemExit as e:  # argparse の誤り（2）と --help（0）
        return e.code if isinstance(e.code, int) else EXIT_USAGE
    if args.start >= args.end:
        log("ERROR", "reprocess_failed", failure="reprocess_failed", reason="usage", detail="--start が --end 以降")
        return EXIT_USAGE
    return args


def run(args: argparse.Namespace, deps: RecoveryDeps) -> int:
    deadline = Deadline(settings.REPROCESS_DEADLINE_SECONDS, deps.monotonic)
    window = Window(args.start, args.end)
    common = {"execution": os.environ.get("CLOUD_RUN_EXECUTION")}
    summary: dict = {
        "window_start": window.start,
        "window_end": window.end,
        "only_missing": args.only_missing,
        "dry_run": args.dry_run,
        "limit": args.limit,
        "parser_version": PARSER_VERSION,
        **common,
    }

    def fail(stage: str, detail: str) -> int:
        log("ERROR", "reprocess_failed", failure="reprocess_failed", stage=stage, detail=detail, **summary)
        return EXIT_FAILED

    stage = "reprocess_list"
    try:
        objects = sorted(list_window(deps, window, deadline, stage), key=lambda o: o.name)
        summary["listed"] = len(objects)
        skipped = 0
        if args.only_missing and objects:
            stage = "reprocess_query"
            completed = query_completed(deps, window, deadline, stage)
            targets = [o for o in objects if o.name not in completed]
            skipped = len(objects) - len(targets)
        else:
            targets = objects
    except (StorageFailed, QueryFailed) as e:
        return fail(stage, str(e))
    except DeadlineExceeded as e:
        return fail(stage, f"deadline at {e.stage}")
    except Exception as e:  # 想定外でも traceback で落とさず、まとめ（reprocess_failed）を出して 1 で終える
        return fail(stage, f"{type(e).__name__}: {e}"[:500])
    summary["skipped_completed"] = skipped
    summary["targets"] = len(targets)
    if args.limit is not None:
        targets = targets[: args.limit]
    summary["over_limit"] = summary["targets"] - len(targets)

    if args.dry_run:
        names = [o.name for o in targets]
        log(
            "INFO",
            "reprocess_done",
            processed=0,
            remaining=len(targets),
            target_objects=names[: settings.RECONCILE_LOG_MAX],
            **summary,
        )
        return EXIT_OK

    outcomes: Counter = Counter()
    stopped = False
    for obj in targets:
        # 1 件の途中で期限に当たると完了記録の無い行（残骸）が増えるので、残りが少なければ始めない
        if deadline.remaining() < settings.REPROCESS_MIN_REMAINING_SECONDS:
            stopped = True
            break
        try:
            outcomes[try_one(obj, deps, deadline, TRIGGER, common)] += 1
        except DeadlineExceeded:
            stopped = True
            break
    processed = sum(outcomes.values())
    failed = sum(outcomes[k] for k in (PARSE_FAILED, WRITE_FAILED, STORAGE_FAILED, UNEXPECTED))
    result = {
        **summary,
        "processed": processed,
        "ok": outcomes[RECOVERED],
        "parse_failed": outcomes[PARSE_FAILED],
        "write_failed": outcomes[WRITE_FAILED],
        "storage_failed": outcomes[STORAGE_FAILED],
        "unexpected": outcomes[UNEXPECTED],
        "remaining": len(targets) - processed,
    }
    log("WARNING" if stopped or failed else "INFO", "reprocess_done", **result)
    if stopped:
        log("ERROR", "reprocess_failed", failure="reprocess_failed", reason="deadline", **result)
        return EXIT_DEADLINE
    if failed:
        log("ERROR", "reprocess_failed", failure="reprocess_failed", reason="items_failed", **result)
        return EXIT_FAILED
    return EXIT_OK


def main(argv: list[str], make_deps: Callable[[], RecoveryDeps] | None = None) -> int:
    args = parse_args(argv)
    if isinstance(args, int):
        return args
    try:
        deps = make_real_deps(settings.Settings.from_env()) if make_deps is None else make_deps()
    except Exception as e:  # 設定・資格情報の誤りなど
        log(
            "ERROR",
            "reprocess_failed",
            failure="reprocess_failed",
            stage="setup",
            detail=f"{type(e).__name__}: {e}"[:500],
        )
        return EXIT_FAILED
    return run(args, deps)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

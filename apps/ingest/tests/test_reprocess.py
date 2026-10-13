"""再処理ジョブ（reprocess.py）の検査。I/O は偽物（#34 AC 1）"""

import pytest
from conftest import read_fixture
from recovery_fakes import FakeRecovery, corrupt_body, logs, raw_bytes

from optionchan_ingest import PARSER_VERSION, settings
from optionchan_ingest.io_bq import QueryFailed
from optionchan_ingest.parser import jst
from optionchan_ingest.reprocess import EXIT_DEADLINE, EXIT_FAILED, EXIT_OK, EXIT_USAGE, main

WALL = jst(2026, 10, 13, 12, 0)
START_JST = "2026-10-13T08:45:00+09:00"
END_JST = "2026-10-13T09:00:00+09:00"


def make_fake():
    fake = FakeRecovery(WALL)
    fake.add(jst(2026, 10, 13, 8, 44))  # 範囲の前
    fake.add(jst(2026, 10, 13, 8, 45), completed=True)
    fake.add(jst(2026, 10, 13, 8, 46))
    fake.add(jst(2026, 10, 13, 8, 59), age_seconds=10)  # 作成直後でも対象（経過時間では絞らない）
    fake.add(jst(2026, 10, 13, 9, 0))  # 範囲の終わり（含まない）
    return fake


def run(argv, fake):
    return main(argv, make_deps=fake.deps)


def reads(fake):
    return [c[1][-13:-8] for c in fake.calls if c[0] == "read"]


def test_reprocesses_range_with_reprocess_trigger(capsys):
    fake = make_fake()
    assert run(["--start", START_JST, "--end", END_JST], fake) == EXIT_OK
    assert reads(fake) == ["T0845", "T0846", "T0859"]  # 完了記録があっても作り直す（--only-missing なし）
    assert "query" not in fake.kinds()
    runs = fake.runs()
    assert [r["trigger"] for r in runs] == ["reprocess"] * 3
    assert {r["parser_version"] for r in runs} == {PARSER_VERSION}
    assert [r["snapshot_at"] for r in runs] == [
        jst(2026, 10, 13, 8, 45),
        jst(2026, 10, 13, 8, 46),
        jst(2026, 10, 13, 8, 59),
    ]
    option_rows = [row for table, rows in fake.appended if table == "option_price" for row in rows]
    assert {r["parser_version"] for r in option_rows} == {PARSER_VERSION}
    records = logs(capsys)
    assert [r["event"] for r in records] == ["reprocessed"] * 3 + ["reprocess_done"]
    done = records[-1]
    assert (done["severity"], done["listed"], done["processed"], done["ok"], done["remaining"]) == ("INFO", 3, 3, 3, 0)


def test_jst_and_utc_inputs_are_the_same_range():
    fake_jst, fake_utc = make_fake(), make_fake()
    run(["--start", START_JST, "--end", END_JST, "--dry-run"], fake_jst)
    run(["--start", "2026-10-12T23:45:00Z", "--end", "2026-10-13T00:00:00+00:00", "--dry-run"], fake_utc)
    assert fake_jst.calls == fake_utc.calls
    assert fake_jst.calls[0] == ("list", "raw/2026/10/13/20261013T0845.json.gz", "raw/2026/10/13/20261013T0901.json.gz")


def test_only_missing_skips_completed(capsys):
    fake = make_fake()
    assert run(["--start", START_JST, "--end", END_JST, "--only-missing"], fake) == EXIT_OK
    assert reads(fake) == ["T0846", "T0859"]
    query = next(c for c in fake.calls if c[0] == "query")
    assert query[1:] == (jst(2026, 10, 13, 8, 44), jst(2026, 10, 13, 9, 1))
    done = logs(capsys)[-1]
    assert (done["listed"], done["skipped_completed"], done["processed"]) == (3, 1, 2)


def test_dry_run_reads_and_writes_nothing(capsys):
    fake = make_fake()
    assert run(["--start", START_JST, "--end", END_JST, "--only-missing", "--dry-run"], fake) == EXIT_OK
    assert fake.kinds() == ["list", "query"]  # 読み出し・書き込み 0
    assert fake.appended == []
    (done,) = logs(capsys)
    assert (done["event"], done["dry_run"], done["targets"], done["processed"]) == ("reprocess_done", True, 2, 0)
    assert done["target_objects"] == ["raw/2026/10/13/20261013T0846.json.gz", "raw/2026/10/13/20261013T0859.json.gz"]


def test_limit(capsys):
    fake = make_fake()
    assert run(["--start", START_JST, "--end", END_JST, "--limit", "2"], fake) == EXIT_OK
    assert reads(fake) == ["T0845", "T0846"]
    done = logs(capsys)[-1]
    assert (done["targets"], done["over_limit"], done["processed"], done["remaining"]) == (3, 1, 2, 0)


def test_failure_exit_1(capsys):
    fake = make_fake()
    fake.objects["raw/2026/10/13/20261013T0846.json.gz"] = (
        fake.objects["raw/2026/10/13/20261013T0846.json.gz"][0],
        raw_bytes(jst(2026, 10, 13, 8, 46), page1=read_fixture("mismatch.html")),
    )
    assert run(["--start", START_JST, "--end", END_JST], fake) == EXIT_FAILED
    assert len(fake.runs()) == 2  # 失敗しても次へ進む
    records = logs(capsys)
    assert ("WARNING", "reprocess_parse_failed") in [(r["severity"], r["event"]) for r in records]
    assert (records[-1]["severity"], records[-1]["event"], records[-1]["reason"]) == (
        "ERROR",
        "reprocess_failed",
        "items_failed",
    )
    assert records[-2]["parse_failed"] == 1


def test_write_failure_exit_1(capsys):
    fake = make_fake()
    fake.append_error_on = ("raw/2026/10/13/20261013T0845.json.gz", "spot_price")
    assert run(["--start", START_JST, "--end", END_JST], fake) == EXIT_FAILED
    assert logs(capsys)[-2]["write_failed"] == 1


def test_query_failure_exit_1(capsys):
    fake = make_fake()
    fake.query_error = QueryFailed("HTTP 403")
    assert run(["--start", START_JST, "--end", END_JST, "--only-missing"], fake) == EXIT_FAILED
    assert "read" not in fake.kinds()
    assert logs(capsys)[-1]["stage"] == "reprocess_query"


def test_deadline_exit_3(capsys, monkeypatch):
    """残りが REPROCESS_MIN_REMAINING_SECONDS 未満なら次の 1 件を始めず、終了コード 3"""
    monkeypatch.setattr(settings, "REPROCESS_DEADLINE_SECONDS", 20)
    monkeypatch.setattr(settings, "REPROCESS_MIN_REMAINING_SECONDS", 5)
    fake = make_fake()
    fake.cost = {"read": 8.0}
    assert run(["--start", START_JST, "--end", END_JST], fake) == EXIT_DEADLINE
    assert len(fake.runs()) == 2
    records = logs(capsys)
    assert (records[-1]["event"], records[-1]["reason"], records[-1]["remaining"]) == (
        "reprocess_failed",
        "deadline",
        1,
    )


@pytest.mark.parametrize(
    "argv",
    [
        [],
        ["--start", "2026-10-13T08:45:00", "--end", END_JST],  # タイムゾーンなし
        ["--start", START_JST, "--end", "2026-10-13 09:00"],
        ["--start", "yesterday", "--end", END_JST],
        ["--start", END_JST, "--end", START_JST],  # 逆順
        ["--start", START_JST, "--end", START_JST],
        ["--start", START_JST, "--end", END_JST, "--limit", "0"],
        ["--start", START_JST, "--end", END_JST, "--unknown"],
    ],
)
def test_usage_errors_exit_2(argv, capsys):
    def boom():
        raise AssertionError("引数の誤りで依存を作った")

    assert main(argv, make_deps=boom) == EXIT_USAGE


def test_help_exit_0(capsys):
    assert main(["--help"], make_deps=None) == 0
    assert "--only-missing" in capsys.readouterr().out


def test_window_end_mid_minute(capsys):
    """終わりが分の途中なら、その分の生 HTML（時刻 < 終わり）を含む"""
    fake = make_fake()
    run(["--start", START_JST, "--end", "2026-10-13T08:59:30+09:00", "--dry-run"], fake)
    assert fake.calls[0][2] == "raw/2026/10/13/20261013T0900.json.gz"
    assert logs(capsys)[-1]["target_objects"][-1] == "raw/2026/10/13/20261013T0859.json.gz"


def test_start_mid_minute_excludes_that_minute(capsys):
    fake = make_fake()
    run(["--start", "2026-10-13T08:45:30+09:00", "--end", END_JST, "--dry-run"], fake)
    assert fake.calls[0][1] == "raw/2026/10/13/20261013T0845.json.gz"  # 一覧には入る
    assert logs(capsys)[-1]["target_objects"] == [  # 08:45:00 < 08:45:30 で落とす
        "raw/2026/10/13/20261013T0846.json.gz",
        "raw/2026/10/13/20261013T0859.json.gz",
    ]


def test_unexpected_item_error_exit_1_with_summary(capsys):
    """1 件の想定外の例外・本体の壊れた gzip でも次へ進み、まとめを出してから終了コード 1"""
    fake = make_fake()
    fake.read_unexpected_on = {"raw/2026/10/13/20261013T0845.json.gz"}
    obj, data = fake.objects["raw/2026/10/13/20261013T0846.json.gz"]
    fake.objects["raw/2026/10/13/20261013T0846.json.gz"] = (obj, corrupt_body(data))
    assert run(["--start", START_JST, "--end", END_JST], fake) == EXIT_FAILED
    assert len(fake.runs()) == 1  # 08:59 だけ
    records = logs(capsys)
    events = [(r["severity"], r["event"]) for r in records]
    assert events[:2] == [("ERROR", "reprocess_item_failed"), ("WARNING", "reprocess_parse_failed")]
    done = records[-2]
    assert (done["event"], done["unexpected"], done["parse_failed"], done["ok"], done["remaining"]) == (
        "reprocess_done",
        1,
        1,
        1,
        0,
    )
    assert (records[-1]["event"], records[-1]["reason"]) == ("reprocess_failed", "items_failed")


def test_unexpected_list_error_exit_1(capsys):
    fake = make_fake()
    fake.list_error = RuntimeError("boom")
    assert run(["--start", START_JST, "--end", END_JST], fake) == EXIT_FAILED
    (record,) = logs(capsys)
    assert (record["event"], record["stage"]) == ("reprocess_failed", "reprocess_list")


def test_setup_error_exit_1(capsys):
    def broken():
        raise RuntimeError("RAW_BUCKET が未設定")

    assert main(["--start", START_JST, "--end", END_JST], make_deps=broken) == EXIT_FAILED
    (record,) = logs(capsys)
    assert (record["event"], record["stage"]) == ("reprocess_failed", "setup")

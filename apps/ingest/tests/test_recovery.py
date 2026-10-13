"""毎時の回収・日次の照合（recovery.py）の検査。I/O は偽物（#34 AC 1）"""

from datetime import UTC, datetime, timedelta

import pytest
from conftest import read_fixture
from htmlgen import page_html
from recovery_fakes import BUCKET, GENERATION, FakeRecovery, corrupt_body, logs, raw_bytes

from optionchan_ingest import PARSER_VERSION, settings
from optionchan_ingest.io_bq import QueryFailed
from optionchan_ingest.io_gcs import RawObject, StorageFailed
from optionchan_ingest.parser import jst
from optionchan_ingest.recovery import (
    Window,
    is_reconcile_time,
    is_recovery_minute,
    name_time,
    object_name_from_source,
    reconcile,
    reconcile_window,
    recover,
    recovery_window,
    run_hourly,
    select_pending,
    trigger_time,
)

NOW = jst(2026, 10, 12, 11, 0) + timedelta(seconds=20)  # 取引時間中の 0 分の回（待ち + 通常の処理の後）
COMMON = {"schedule_time": "2026-10-12T02:00:00Z"}


def events(records):
    return [(r["severity"], r["event"]) for r in records]


def by_event(records, event):
    return [r for r in records if r["event"] == event]


# --- 純粋関数 ---


@pytest.mark.parametrize(
    "header, expected",
    [
        ("2026-10-12T02:00:00Z", datetime(2026, 10, 12, 2, 0, tzinfo=UTC)),
        ("2026-10-12T02:00:00.123+00:00", datetime(2026, 10, 12, 2, 0, 0, 123000, tzinfo=UTC)),
        ("not-a-time", NOW),  # 読めなければ実時刻
        ("2026-10-12T02:00:00", NOW),  # タイムゾーンが無ければ実時刻
        (None, NOW),
        ("", NOW),
    ],
)
def test_trigger_time(header, expected):
    assert trigger_time(header, NOW) == expected


@pytest.mark.parametrize(
    "t, expected",
    [
        (jst(2026, 10, 12, 11, 0), True),
        (datetime(2026, 10, 12, 2, 0, 59, tzinfo=UTC), True),  # JST 11:00:59
        (jst(2026, 10, 12, 11, 1), False),
        (jst(2026, 10, 12, 10, 59), False),
        (jst(2026, 10, 13, 0, 0), True),
    ],
)
def test_is_recovery_minute(t, expected):
    assert is_recovery_minute(t) is expected


def test_is_reconcile_time():
    assert is_reconcile_time(datetime(2026, 10, 12, 22, 0, tzinfo=UTC))  # JST 07:00
    assert not is_reconcile_time(jst(2026, 10, 13, 8, 0))
    assert not is_reconcile_time(jst(2026, 10, 13, 7, 1))
    with pytest.raises(ValueError):
        is_recovery_minute(datetime(2026, 10, 12, 11, 0))


def test_name_time():
    assert name_time("raw/2026/10/12/20261012T1046.json.gz") == jst(2026, 10, 12, 10, 46)
    assert name_time("raw/2026/10/12/other.json.gz") is None


def test_recovery_window_crosses_date():
    window = recovery_window(jst(2026, 10, 13, 3, 0) + timedelta(seconds=5))
    assert window.offsets() == ("raw/2026/10/12/20261012T2100.json.gz", "raw/2026/10/13/20261013T0301.json.gz")
    assert not window.contains("raw/2026/10/12/20261012T2100.json.gz")  # 21:00:00 < 21:00:05
    assert window.contains("raw/2026/10/12/20261012T2101.json.gz")
    assert window.contains("raw/2026/10/13/20261013T0300.json.gz")  # 03:00:00 < 03:00:05
    assert not window.contains("raw/2026/10/13/20261013T0301.json.gz")
    start_at, end_at = window.query_range()
    assert (start_at, end_at) == (
        datetime(2026, 10, 12, 11, 59, 5, tzinfo=UTC),
        datetime(2026, 10, 12, 18, 1, 5, tzinfo=UTC),
    )


@pytest.mark.parametrize(
    "t, start",
    [
        (jst(2026, 10, 13, 7, 0), jst(2026, 10, 12, 7, 0)),
        (jst(2026, 10, 13, 7, 0) + timedelta(seconds=30), jst(2026, 10, 12, 7, 0)),
        (jst(2026, 10, 13, 6, 59), jst(2026, 10, 11, 7, 0)),  # 07:00 より前なら前日の 07:00 が終わり
        (datetime(2026, 10, 12, 22, 0, tzinfo=UTC), jst(2026, 10, 12, 7, 0)),  # UTC で渡しても JST で切る
    ],
)
def test_reconcile_window(t, start):
    window = reconcile_window(t)
    assert (window.start, window.end) == (start, start + timedelta(days=1))


@pytest.mark.parametrize(
    "source, expected",
    [
        ("gs://b/raw/2026/10/13/20261013T0839.json.gz#123", "raw/2026/10/13/20261013T0839.json.gz"),
        ("gs://b/raw/2026/10/13/20261013T0839.json.gz", None),  # generation が無い
        ("gs://b/other/x.json.gz#1", None),
        ("raw/2026/10/13/20261013T0839.json.gz#1", None),
        ("", None),
        (None, None),
    ],
)
def test_object_name_from_source(source, expected):
    assert object_name_from_source(source) == expected


def _obj(hhmm, age):
    name = f"raw/2026/10/12/20261012T{hhmm}.json.gz"
    return RawObject(name, 1, NOW - timedelta(seconds=age))


def test_select_pending():
    objects = [_obj("1040", 1200), _obj("1030", 1800), _obj("1050", 600), _obj("1058", 179), _obj("1020", 2400)]
    completed = {"raw/2026/10/12/20261012T1030.json.gz"}
    pending, total = select_pending(objects, completed, NOW, 180, 2)
    assert [o.name for o in pending] == ["raw/2026/10/12/20261012T1020.json.gz", "raw/2026/10/12/20261012T1040.json.gz"]
    assert total == 3  # 1020・1040・1050（1030 は完了済み、1058 は 3 分未満）
    assert select_pending(objects, completed, NOW, 180, 0) == ([], 3)
    # ちょうど 180 秒は対象
    assert select_pending([_obj("1057", 180)], set(), NOW, 180, 10)[1] == 1
    with pytest.raises(ValueError):
        select_pending(objects, completed, datetime(2026, 10, 12, 11, 0), 180, 2)
    with pytest.raises(ValueError):
        select_pending([RawObject("raw/x", 1, datetime(2026, 10, 12))], set(), NOW, 180, 2)


# --- 回収 ---


def test_no_candidates_no_query(capsys):
    fake = FakeRecovery(NOW)
    fake.add(jst(2026, 10, 12, 10, 59), age_seconds=50)  # 3 分未満だけ
    fake.add(jst(2026, 10, 12, 4, 30))  # 窓（6 時間）の外
    recover(NOW, fake.deps(), fake.deadline(), COMMON)
    assert fake.kinds() == ["list"]  # クエリしない
    assert fake.calls[0][1:] == ("raw/2026/10/12/20261012T0500.json.gz", "raw/2026/10/12/20261012T1101.json.gz")
    (record,) = logs(capsys)
    assert (record["severity"], record["event"]) == ("INFO", "recovery")
    assert (record["listed"], record["old_enough"], record["queried"]) == (1, 0, False)
    assert record["schedule_time"] == COMMON["schedule_time"]


def test_recovers_only_missing_and_old_enough(capsys):
    fake = FakeRecovery(NOW)
    done = fake.add(jst(2026, 10, 12, 10, 30), completed=True)
    missing = fake.add(jst(2026, 10, 12, 10, 40))
    fake.add(jst(2026, 10, 12, 10, 58), age_seconds=100)  # 作成から 3 分未満
    fake.add(jst(2026, 10, 12, 4, 59))  # 窓の外
    summary = recover(NOW, fake.deps(), fake.deadline(), COMMON)

    assert [c[1] for c in fake.calls if c[0] == "read"] == [missing]
    assert done not in [c[1] for c in fake.calls if c[0] == "read"]
    # 書き込みの順序（3 テーブル → ingest_runs）
    assert [c[1] for c in fake.calls if c[0] == "append"] == [
        "spot_price",
        "future_price",
        "option_price",
        "ingest_runs",
    ]
    (run,) = fake.runs()
    assert run["trigger"] == "recovery"
    assert run["ingest_run_id"] == "run-1"
    assert run["snapshot_at"] == jst(2026, 10, 12, 10, 40)
    assert run["page2_updated_at"] == jst(2026, 10, 12, 10, 39)
    assert run["source_object"] == f"gs://{BUCKET}/{missing}#{GENERATION}"
    assert run["source_generation"] == GENERATION
    assert run["parser_version"] == PARSER_VERSION
    assert (run["rows_spot"], run["rows_future"], run["rows_option"]) == (1, 1, 40)
    assert run["started_at"] <= run["finished_at"]
    option_rows = next(rows for table, rows in fake.appended if table == "option_price")
    assert {r["ingest_run_id"] for r in option_rows} == {"run-1"}
    assert {r["source_object"] for r in option_rows} == {run["source_object"]}

    # クエリの範囲は窓の ±1 分（UTC）
    query = next(c for c in fake.calls if c[0] == "query")
    assert query[1:] == recovery_window(NOW).query_range()

    records = logs(capsys)
    assert events(records) == [("INFO", "recovered"), ("INFO", "recovery")]
    last = records[-1]
    assert (last["listed"], last["old_enough"], last["completed"], last["pending"]) == (3, 2, 1, 1)
    assert (last["recovered"], last["remaining"], last["queried"]) == (1, 0, True)
    assert summary["recovered"] == 1
    assert records[0]["object"] == missing and records[0]["rows"]["option_price"] == 40


def test_query_failure_stops_recovery(capsys):
    fake = FakeRecovery(NOW)
    fake.add(jst(2026, 10, 12, 10, 40))
    fake.query_error = QueryFailed("HTTP 403")
    recover(NOW, fake.deps(), fake.deadline(), COMMON)
    assert fake.kinds() == ["list", "query"]  # 空集合とみなして全件を再取り込みしない
    (record,) = logs(capsys)
    assert (record["severity"], record["event"], record["stage"]) == ("ERROR", "recovery_failed", "recovery_query")


def test_list_failure_is_error(capsys):
    fake = FakeRecovery(NOW)
    fake.list_error = StorageFailed("list: ReadTimeout")
    recover(NOW, fake.deps(), fake.deadline(), COMMON)
    (record,) = logs(capsys)
    assert (record["severity"], record["event"], record["stage"]) == ("ERROR", "recovery_failed", "recovery_list")


def test_limit_and_remaining(capsys):
    fake = FakeRecovery(NOW)
    for minute in range(12):
        fake.add(jst(2026, 10, 12, 10, minute))
    recover(NOW, fake.deps(), fake.deadline(), COMMON)
    reads = [c[1] for c in fake.calls if c[0] == "read"]
    assert len(reads) == settings.RECOVERY_MAX_ITEMS == 10
    assert reads == sorted(reads)  # 古い順
    last = logs(capsys)[-1]
    assert (last["severity"], last["event"]) == ("WARNING", "recovery")
    assert (last["pending"], last["recovered"], last["remaining"]) == (12, 10, 2)


def test_stops_when_remaining_time_is_short(capsys):
    """1 件 5 秒かかると、残り 15 秒未満で次の 1 件を始めない（期限 45 秒を通常の処理が 13 秒使った後）"""
    fake = FakeRecovery(NOW)
    for minute in range(5):
        fake.add(jst(2026, 10, 12, 10, minute))
    fake.cost = {"read": 5.0}
    deadline = fake.deadline()
    fake.clock_value = 13.0  # 待ち 8 秒 + 通常の処理 5 秒
    recover(NOW, fake.deps(), deadline, COMMON)
    # 残り 32 → 27 → 22 → 17 → 12（ここで止まる）
    assert len([c for c in fake.calls if c[0] == "read"]) == 4
    last = logs(capsys)[-1]
    assert (last["severity"], last["recovered"], last["remaining"]) == ("WARNING", 4, 1)
    assert fake.clock_value < settings.REQUEST_DEADLINE_SECONDS


def test_deadline_during_writes_skips_ingest_runs(capsys):
    """書き込みの途中で期限を超えたら ingest_runs を書かずに止め、その件を残りに数える"""
    fake = FakeRecovery(NOW)
    fake.add(jst(2026, 10, 12, 10, 40))
    fake.cost = {"append_option_price": 31.0}
    deadline = fake.deadline()
    fake.clock_value = 13.0
    recover(NOW, fake.deps(), deadline, COMMON)
    assert fake.runs() == []
    last = logs(capsys)[-1]
    assert (last["severity"], last["event"], last["recovered"], last["remaining"]) == ("WARNING", "recovery", 0, 1)


def test_skipped_when_no_time_left(capsys):
    fake = FakeRecovery(NOW)
    fake.add(jst(2026, 10, 12, 10, 40))
    deadline = fake.deadline()
    fake.clock_value = 35.0  # 通常の処理が長引いた
    recover(NOW, fake.deps(), deadline, COMMON)
    assert fake.calls == []
    (record,) = logs(capsys)
    assert (record["severity"], record["event"], record["reason"]) == ("WARNING", "recovery_skipped", "no_time")


@pytest.mark.parametrize(
    "data",
    [
        b"not gzip",
        corrupt_body(raw_bytes(jst(2026, 10, 12, 10, 40))),  # ヘッダは正しく本体が壊れた gzip（zlib.error）
        raw_bytes(jst(2026, 10, 12, 10, 40), page1=read_fixture("mismatch.html")),
        # 1 限月ページの更新時刻が名前と違う
        raw_bytes(jst(2026, 10, 12, 10, 40), page1=page_html(updated_at="2026/10/12 10:41", strikes=(39500, 40000))),
    ],
    ids=["broken_gzip", "corrupt_gzip_body", "parse_error", "name_mismatch"],
)
def test_parse_failure_is_warning_and_continues(capsys, data):
    fake = FakeRecovery(NOW)
    bad = fake.add(jst(2026, 10, 12, 10, 40), data=data)
    good = fake.add(jst(2026, 10, 12, 10, 41))
    recover(NOW, fake.deps(), fake.deadline(), COMMON)
    assert [c[1] for c in fake.calls if c[0] == "read"] == [bad, good]
    assert [r["source_object"] for r in fake.runs()] == [f"gs://{BUCKET}/{good}#{GENERATION}"]
    records = logs(capsys)
    assert events(records) == [("WARNING", "recovery_parse_failed"), ("INFO", "recovered"), ("INFO", "recovery")]
    assert records[0]["object"] == bad
    assert (records[-1]["parse_failed"], records[-1]["recovered"], records[-1]["remaining"]) == (1, 1, 0)


def test_validation_failure_is_parse_failed(capsys):
    """構造の検査の違反（2 限月ページが 1 限月と同じ = 限月が 1 つ）も WARNING"""
    t = jst(2026, 10, 12, 10, 40)
    page1 = page_html(updated_at="2026/10/12 10:40", strikes=tuple(range(38000, 43000, 500)), atm=40000)
    fake = FakeRecovery(NOW)
    fake.add(t, data=raw_bytes(t, page1=page1, page2=page1))
    recover(NOW, fake.deps(), fake.deadline(), COMMON)
    assert fake.runs() == []
    assert ("WARNING", "recovery_parse_failed") in events(logs(capsys))


def test_write_failure_is_error_and_continues(capsys):
    fake = FakeRecovery(NOW)
    first = fake.add(jst(2026, 10, 12, 10, 40))
    fake.add(jst(2026, 10, 12, 10, 41))
    fake.append_error_on = (first, "option_price")
    recover(NOW, fake.deps(), fake.deadline(), COMMON)
    assert len(fake.runs()) == 1  # 失敗した件の ingest_runs は書かない
    records = logs(capsys)
    assert events(records) == [("ERROR", "write_failed"), ("INFO", "recovered"), ("INFO", "recovery")]
    assert records[0]["trigger"] == "recovery" and records[0]["object"] == first
    assert (records[-1]["write_failed"], records[-1]["recovered"]) == (1, 1)


def test_read_failure_is_error(capsys):
    fake = FakeRecovery(NOW)
    name = fake.add(jst(2026, 10, 12, 10, 40))
    fake.read_error_on = {name}
    recover(NOW, fake.deps(), fake.deadline(), COMMON)
    records = logs(capsys)
    assert (records[0]["severity"], records[0]["event"], records[0]["stage"]) == (
        "ERROR",
        "storage_failed",
        "recovery_read",
    )
    assert records[-1]["storage_failed"] == 1


# --- 日次の照合 ---

RECONCILE_NOW = jst(2026, 10, 13, 7, 0) + timedelta(seconds=1)


def test_reconcile_counts_and_unrecovered_error(capsys):
    fake = FakeRecovery(RECONCILE_NOW)
    fake.add(jst(2026, 10, 12, 7, 0), completed=True)  # 窓の始め（含む）
    fake.add(jst(2026, 10, 12, 15, 45), completed=True)
    missing = fake.add(jst(2026, 10, 13, 5, 59))
    fake.add(jst(2026, 10, 13, 7, 0))  # 窓の終わり（含まない）
    fake.add(jst(2026, 10, 12, 6, 59))  # 前の窓
    result = reconcile(RECONCILE_NOW, fake.deps(), fake.deadline(), COMMON)
    assert fake.calls[0] == ("list", "raw/2026/10/12/20261012T0700.json.gz", "raw/2026/10/13/20261013T0701.json.gz")
    assert fake.calls[1] == ("query", *Window(jst(2026, 10, 12, 7, 0), jst(2026, 10, 13, 7, 0)).query_range())
    assert (result["raw"], result["completed"], result["unresolved"]) == (3, 2, 1)
    records = logs(capsys)
    assert events(records) == [("INFO", "raw_reconciled"), ("ERROR", "raw_unrecovered")]
    assert records[1]["unresolved_objects"] == [missing]
    assert records[0]["window_start"] == "2026-10-12T07:00:00+09:00"


def test_reconcile_all_completed_is_info_only(capsys):
    fake = FakeRecovery(RECONCILE_NOW)
    fake.add(jst(2026, 10, 12, 9, 0), completed=True)
    reconcile(RECONCILE_NOW, fake.deps(), fake.deadline(), COMMON)
    records = logs(capsys)
    assert events(records) == [("INFO", "raw_reconciled")]
    assert (records[0]["raw"], records[0]["unresolved"], records[0]["unresolved_objects"]) == (1, 0, [])


def test_reconcile_names_are_capped(capsys):
    fake = FakeRecovery(RECONCILE_NOW)
    for minute in range(25):
        fake.add(jst(2026, 10, 12, 9, minute))
    reconcile(RECONCILE_NOW, fake.deps(), fake.deadline(), COMMON)
    error = logs(capsys)[-1]
    assert (error["event"], error["unresolved"]) == ("raw_unrecovered", 25)
    assert len(error["unresolved_objects"]) == settings.RECONCILE_LOG_MAX == 20


def test_reconcile_no_raw_no_query(capsys):
    fake = FakeRecovery(RECONCILE_NOW)
    reconcile(RECONCILE_NOW, fake.deps(), fake.deadline(), COMMON)
    assert fake.kinds() == ["list"]
    records = logs(capsys)
    assert events(records) == [("INFO", "raw_reconciled")]
    assert records[0]["raw"] == 0


def test_reconcile_query_failure_is_error(capsys):
    fake = FakeRecovery(RECONCILE_NOW)
    fake.add(jst(2026, 10, 12, 9, 0))
    fake.query_error = QueryFailed("HTTP 500")
    assert reconcile(RECONCILE_NOW, fake.deps(), fake.deadline(), COMMON) is None
    records = logs(capsys)
    assert events(records) == [("ERROR", "reconcile_failed")]
    assert records[0]["stage"] == "reconcile_query"


def test_run_hourly_at_0700_recovers_then_reconciles(capsys):
    """07:00 は回収の後に照合する（回収で直った分は未回収に数えない）"""
    fake = FakeRecovery(RECONCILE_NOW)
    name = fake.add(jst(2026, 10, 13, 5, 30))

    def completed_after_recovery(start_at, end_at, timeout):
        fake.calls.append(("query", start_at, end_at))
        return [r["source_object"] for r in fake.runs()]

    deps = fake.deps()
    deps.completed_sources = completed_after_recovery
    run_hourly(RECONCILE_NOW, datetime(2026, 10, 12, 22, 0, tzinfo=UTC), deps, fake.deadline(), COMMON)
    records = logs(capsys)
    assert events(records) == [("INFO", "recovered"), ("INFO", "recovery"), ("INFO", "raw_reconciled")]
    assert records[0]["object"] == name
    assert (records[-1]["raw"], records[-1]["unresolved"]) == (1, 0)


def test_run_hourly_reserves_time_for_reconcile(capsys):
    """07:00 の回は、照合の分（RECONCILE_RESERVE_SECONDS）を残して回収を止める"""
    fake = FakeRecovery(RECONCILE_NOW)
    for minute in range(10):
        fake.add(jst(2026, 10, 13, 5, minute))
    fake.cost = {"read": 4.0}
    run_hourly(RECONCILE_NOW, jst(2026, 10, 13, 7, 0), fake.deps(), fake.deadline(), COMMON)
    records = logs(capsys)
    recovery_log = by_event(records, "recovery")[0]
    # 残り 45 → 41 → … 1 件を始める条件は残り 30 秒以上: 45, 41, 37, 33 の 4 件
    assert (recovery_log["recovered"], recovery_log["remaining"]) == (4, 6)
    reconciled = by_event(records, "raw_reconciled")[0]
    assert (reconciled["raw"], reconciled["completed"], reconciled["unresolved"]) == (10, 0, 10)


def test_run_hourly_not_0700_skips_reconcile(capsys):
    fake = FakeRecovery(NOW)
    run_hourly(NOW, jst(2026, 10, 12, 11, 0), fake.deps(), fake.deadline(), COMMON)
    assert events(logs(capsys)) == [("INFO", "recovery")]


def test_run_hourly_swallows_unexpected(capsys):
    fake = FakeRecovery(RECONCILE_NOW)
    fake.list_error = RuntimeError("boom")
    run_hourly(RECONCILE_NOW, jst(2026, 10, 13, 7, 0), fake.deps(), fake.deadline(), COMMON)
    records = logs(capsys)
    assert events(records) == [("ERROR", "recovery_failed"), ("ERROR", "reconcile_failed")]
    assert records[0]["stage"] == "unexpected"


def test_unexpected_item_error_is_logged_and_continues(capsys):
    """1 件の想定外の例外で回収全体を止めない: ERROR recovery_item_failed（種別と名前）を出して次へ進む"""
    fake = FakeRecovery(NOW)
    bad = fake.add(jst(2026, 10, 12, 10, 40))
    good = fake.add(jst(2026, 10, 12, 10, 41))
    fake.read_unexpected_on = {bad}
    recover(NOW, fake.deps(), fake.deadline(), COMMON)
    assert [r["source_object"] for r in fake.runs()] == [f"gs://{BUCKET}/{good}#{GENERATION}"]
    records = logs(capsys)
    assert events(records) == [("ERROR", "recovery_item_failed"), ("INFO", "recovered"), ("INFO", "recovery")]
    assert (records[0]["object"], records[0]["error_type"]) == (bad, "RuntimeError")
    assert (records[-1]["unexpected"], records[-1]["recovered"], records[-1]["remaining"]) == (1, 1, 0)


def test_unexpected_error_in_build_is_isolated(capsys, monkeypatch):
    """解析の後（行の組み立て）の想定外の例外も 1 件単位で扱う"""
    from optionchan_ingest import recovery as recovery_module

    calls = []
    original = recovery_module.build_rows

    def flaky(p1, p2, run):
        calls.append(run.ingest_run_id)
        if len(calls) == 1:
            raise KeyError("boom")
        return original(p1, p2, run)

    monkeypatch.setattr(recovery_module, "build_rows", flaky)
    fake = FakeRecovery(NOW)
    fake.add(jst(2026, 10, 12, 10, 40))
    fake.add(jst(2026, 10, 12, 10, 41))
    recover(NOW, fake.deps(), fake.deadline(), COMMON)
    assert len(fake.runs()) == 1
    assert events(logs(capsys))[0] == ("ERROR", "recovery_item_failed")

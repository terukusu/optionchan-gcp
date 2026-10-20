"""app.handle の経路ごとの検査（I/O は偽物）（AC 11）"""

import gzip
import json
import time
from datetime import UTC, datetime, timedelta

import pytest
from calgen import CAL
from conftest import read_fixture
from htmlgen import default_side, empty_page_html, page_html

from optionchan_ingest import app as app_module
from optionchan_ingest import settings
from optionchan_ingest.app import Deps, fetch_wait_seconds, handle
from optionchan_ingest.calendar import build_calendar
from optionchan_ingest.io_bq import WriteFailed
from optionchan_ingest.io_gcs import AlreadyExists, RawObject, StorageFailed
from optionchan_ingest.parser import JST, JpxTransient, JpxUnavailable, jst
from optionchan_ingest.snapshot import BodyWatch, FetchedPage, decode_raw

STRIKES = tuple(range(38000, 43000, 500))
PAGE1 = page_html(strikes=STRIKES, atm=40000)
PAGE2 = page_html(updated_at="2026/10/12 10:45", last_trading_day="2026/12/10", strikes=STRIKES, atm=40000)
IN_HOURS = jst(2026, 10, 12, 11, 0)  # 祝日取引（スポーツの日）の日中。ページ 10:46 から 14 分
OUT_OF_HOURS = jst(2026, 10, 12, 7, 0)
NAME = "raw/2026/10/12/20261012T1046.json.gz"


# 偽の実時刻: 分の頭（IN_HOURS の 11:00:00 JST）+ 到着の秒 + 偽の monotonic の経過
MINUTE_HEAD = datetime(2026, 10, 12, 2, 0, tzinfo=UTC)


class Fake:
    def __init__(
        self,
        pages=None,
        exists=False,
        create_error=None,
        append_error_on=None,
        exists_error=None,
        arrival=5.0,
        fetch_at_second=0.0,
        raw_names=None,
        list_error=None,
    ):
        self.calls = []
        self.timeouts = []
        self.pages = pages or {1: PAGE1, 2: PAGE2}
        self.exists = exists
        self.exists_error = exists_error
        self.create_error = create_error
        self.append_error_on = append_error_on
        self.created = {}
        self.appended = {}
        self.clock_value = 0.0  # 偽の monotonic（秒）
        self.cost = {}  # 呼び出しの種類 → その呼び出しで進める秒数
        self.arrival = arrival  # 分の頭から何秒目に届いたか
        self.fetch_at_second = fetch_at_second  # 既定 0（待たない）。待ちの検査では明示する
        self.sleeps = []
        self.raw_names = raw_names  # 生 HTML の名前（None なら list_raw を渡さない = 継続の判定をしない）
        self.list_error = list_error

    def _tick(self, kind, timeout):
        self.timeouts.append((kind, timeout))
        self.clock_value += self.cost.get(kind, 0.0)

    def monotonic(self):
        return self.clock_value

    def wall(self):
        return MINUTE_HEAD + timedelta(seconds=self.arrival + self.clock_value)

    def sleep(self, seconds):
        self.calls.append(("sleep", seconds))
        self.sleeps.append(seconds)
        self.clock_value += seconds

    def fetch_page(self, page, timeout):
        self.calls.append(("fetch", page))
        self._tick(f"fetch{page}", timeout)
        html = self.pages[page]
        if isinstance(html, list):  # 呼ぶたびに先頭から返す（最後の 1 つは返し続ける）
            html = html.pop(0) if len(html) > 1 else html[0]
        if isinstance(html, Exception):
            raise html
        return FetchedPage(page, f"https://example.invalid/{page}", self.wall(), 200, html.encode("utf-8"))

    def raw_exists(self, name, timeout):
        self.calls.append(("exists", name))
        self._tick("exists", timeout)
        if self.exists_error:
            raise self.exists_error
        return self.exists

    def raw_create(self, name, data, timeout):
        self.calls.append(("create", name))
        self._tick("create", timeout)
        if self.create_error:
            raise self.create_error
        self.created[name] = data
        return 1234567

    def list_raw(self, start_offset, end_offset, timeout):
        self.calls.append(("list", start_offset, end_offset))
        self._tick("list", timeout)
        if self.list_error:
            raise self.list_error
        # GCS の一覧と同じく、名前が [start_offset, end_offset) のものだけ返す
        return [RawObject(n, 1, MINUTE_HEAD) for n in self.raw_names if start_offset <= n < end_offset]

    def append_rows(self, table, rows, timeout):
        self.calls.append(("append", table))
        self._tick(f"append_{table}", timeout)
        if table == self.append_error_on:
            raise WriteFailed(table)
        self.appended[table] = rows

    def deps(self):
        return Deps(
            CAL,
            "test-bucket",
            self.fetch_page,
            self.raw_exists,
            self.raw_create,
            self.append_rows,
            clock=self.wall,
            new_run_id=lambda: "run-1",
            monotonic=self.monotonic,
            fetch_at_second=self.fetch_at_second,
            sleep=self.sleep,
            list_raw=None if self.raw_names is None else self.list_raw,
        )


def logs(capsys):
    return [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.strip()]


def events(capsys):
    return [(r["severity"], r["event"]) for r in logs(capsys)]


def test_out_of_hours_calls_nothing(capsys):
    fake = Fake(arrival=1.0, fetch_at_second=8.0)
    result = handle(OUT_OF_HOURS, fake.deps())
    assert result.status == 204
    assert fake.calls == []  # 待たない（sleep も呼ばない）
    assert events(capsys) == [("INFO", "out_of_hours")]


def test_ingested_write_order(capsys):
    fake = Fake()
    result = handle(IN_HOURS, fake.deps(), schedule_time="2026-10-12T02:00:00Z")
    assert (result.status, result.outcome) == (200, "ingested")
    assert [c for c in fake.calls if c[0] == "append"] == [
        ("append", "spot_price"),
        ("append", "future_price"),
        ("append", "option_price"),
        ("append", "ingest_runs"),
    ]
    # GCS の作成は書き込みより前、名前は 1 限月の更新時刻
    assert fake.calls.index(("create", NAME)) < fake.calls.index(("append", "spot_price"))
    pages = decode_raw(fake.created[NAME])
    assert [p.page for p in pages] == [1, 2]
    assert pages[0].updated_at == jst(2026, 10, 12, 10, 46)
    assert pages[1].updated_at == jst(2026, 10, 12, 10, 45)
    assert pages[0].html == PAGE1.encode("utf-8")

    run = fake.appended["ingest_runs"][0]
    assert run["ingest_run_id"] == "run-1"
    assert run["snapshot_at"] == jst(2026, 10, 12, 10, 46)
    assert run["source_object"] == f"gs://test-bucket/{NAME}#1234567"
    assert run["source_generation"] == 1234567
    assert (run["rows_spot"], run["rows_future"], run["rows_option"]) == (1, 1, 40)
    assert run["trigger"] == "scheduler"
    assert fake.appended["option_price"][0]["source_object"] == run["source_object"]

    records = logs(capsys)
    assert records[-1]["event"] == "ingested"
    assert records[-1]["ingest_run_id"] == "run-1"
    assert records[-1]["schedule_time"] == "2026-10-12T02:00:00Z"
    assert records[-1]["rows"] == {"spot_price": 1, "future_price": 1, "option_price": 40}
    assert "elapsed_ms" in records[-1] and "jpx_fetch_ms" in records[-1]
    assert all(r["severity"] != "ERROR" for r in records)


def test_skipped_no_update(capsys):
    fake = Fake(exists=True)
    result = handle(IN_HOURS, fake.deps())
    assert (result.status, result.outcome) == (200, "skipped_no_update")
    assert fake.calls == [("fetch", 1), ("exists", NAME)]


def test_locked_412_does_not_write_bigquery(capsys):
    fake = Fake(create_error=AlreadyExists(NAME))
    result = handle(IN_HOURS, fake.deps())
    assert (result.status, result.outcome) == (200, "skipped_locked")
    assert not [c for c in fake.calls if c[0] == "append"]


def test_parse_failed_keeps_raw_and_skips_runs(capsys):
    fake = Fake(pages={1: read_fixture("mismatch.html"), 2: PAGE2})
    result = handle(jst(2026, 10, 12, 11, 0), fake.deps())
    assert (result.status, result.outcome) == (500, "parse_failed")
    assert len(fake.created) == 1  # 生 HTML は残す
    assert not [c for c in fake.calls if c[0] == "append"]
    assert ("ERROR", "parse_failed") in events(capsys)


def test_validation_failure_is_parse_failed(capsys):
    # 2 限月ページが 1 限月と同じ（限月が 1 つ）
    fake = Fake(pages={1: PAGE1, 2: PAGE1})
    result = handle(IN_HOURS, fake.deps())
    assert (result.status, result.outcome) == (500, "parse_failed")
    assert len(fake.created) == 1
    assert ("append", "ingest_runs") not in fake.calls
    assert not [c for c in fake.calls if c[0] == "append"]


@pytest.mark.parametrize("table", ["spot_price", "option_price", "ingest_runs"])
def test_write_failed(capsys, table):
    fake = Fake(append_error_on=table)
    result = handle(IN_HOURS, fake.deps())
    assert (result.status, result.outcome) == (500, "write_failed")
    if table != "ingest_runs":
        assert ("append", "ingest_runs") not in fake.calls
    assert ("ERROR", "write_failed") in events(capsys)


@pytest.mark.parametrize(
    "pages",
    [
        {1: JpxUnavailable("timeout"), 2: PAGE2},
        {1: empty_page_html(), 2: PAGE2},
        {1: PAGE1, 2: JpxUnavailable("HTTP 503")},
        {1: PAGE1, 2: empty_page_html()},
    ],
    ids=["page1_error", "page1_empty", "page2_error", "page2_empty"],
)
def test_jpx_unavailable_is_warning(capsys, pages):
    fake = Fake(pages=pages)
    result = handle(IN_HOURS, fake.deps())
    assert (result.status, result.outcome) == (200, "jpx_unavailable")
    assert fake.created == {}
    assert not [c for c in fake.calls if c[0] == "append"]
    assert [s for s, _ in events(capsys)] == ["WARNING"]


# --- jpx_unavailable が続いたら ERROR（#42） ---

UNAVAILABLE_PAGES = [
    {1: JpxUnavailable("timeout"), 2: PAGE2},
    {1: empty_page_html(), 2: PAGE2},
    {1: PAGE1, 2: JpxUnavailable("HTTP 503")},
    {1: PAGE1, 2: empty_page_html()},
]
UNAVAILABLE_IDS = ["page1_error", "page1_empty", "page2_error", "page2_empty"]
DOWN = {1: JpxUnavailable("timeout"), 2: PAGE2}


def raw(t):
    return f"raw/{t:%Y/%m/%d}/{t:%Y%m%dT%H%M}.json.gz"


@pytest.mark.parametrize("pages", UNAVAILABLE_PAGES, ids=UNAVAILABLE_IDS)
def test_jpx_unavailable_continued_is_error(capsys, pages):
    # 11:00 の日中。直近 30 分（10:30〜）に保存が無い → ERROR。応答は 200 jpx_unavailable のまま
    fake = Fake(pages=pages, raw_names=[])
    result = handle(IN_HOURS, fake.deps())
    assert (result.status, result.outcome) == (200, "jpx_unavailable")
    assert fake.created == {}
    assert [c for c in fake.calls if c[0] == "list"] == [
        ("list", "raw/2026/10/12/20261012T1030.json.gz", "raw/2026/10/12/20261012T1101.json.gz")
    ]
    records = logs(capsys)
    assert [(r["severity"], r["event"]) for r in records] == [
        ("ERROR", "jpx_unavailable_stale"),
        ("WARNING", "jpx_unavailable"),
    ]
    error = records[0]
    assert error["failure"] == "jpx_unavailable_stale"
    assert "last_snapshot_at" not in error  # 窓の中に保存が無い
    assert error["lag_seconds"] == (2 * 60 + 15) * 60  # セッション開始 08:45 から
    assert error["detail"] == records[1]["detail"]
    assert error["session"] == "day"


@pytest.mark.parametrize(
    "names, stale",
    [
        ([], True),
        ([raw(jst(2026, 10, 12, 10, 29))], True),  # 窓の外（31 分前）
        ([raw(jst(2026, 10, 12, 10, 30))], False),  # ちょうど 30 分前は stale ではない（is_stale と同じ）
        ([raw(jst(2026, 10, 12, 10, 59))], False),  # 04 時台の単発の空ページと同じく、直前まで保存できていた
        ([raw(jst(2026, 10, 12, 9, 0)), raw(jst(2026, 10, 12, 10, 45))], False),
    ],
)
def test_jpx_unavailable_recent_snapshot(capsys, names, stale):
    handle(IN_HOURS, Fake(pages=DOWN, raw_names=names).deps())
    assert (("ERROR", "jpx_unavailable_stale") in events(capsys)) is stale


def test_jpx_unavailable_stale_reports_last_snapshot(capsys):
    # 11:00:30 に最後の保存が 10:30 → 30 分 30 秒で stale。最後の保存の時刻と遅れを出す
    handle(
        jst(2026, 10, 12, 11, 0) + timedelta(seconds=30),
        Fake(pages=DOWN, raw_names=[raw(jst(2026, 10, 12, 10, 30))]).deps(),
    )
    error = next(r for r in logs(capsys) if r["event"] == "jpx_unavailable_stale")
    assert error["last_snapshot_at"] == "2026-10-12T10:30:00+09:00"
    assert error["lag_seconds"] == 30 * 60 + 30


@pytest.mark.parametrize(
    "now, names, stale, listed",
    [
        (jst(2026, 10, 12, 9, 15), [], False, False),  # 日中の開始 08:45 から 30 分以内は一覧しない
        (jst(2026, 10, 12, 9, 16), [], True, True),
        (jst(2026, 10, 12, 17, 40), [], False, False),  # 夜間の開始 17:00 から 40 分以内
        (jst(2026, 10, 13, 0, 20), [], True, True),  # 夜間の日付またぎ
        (jst(2026, 10, 13, 0, 20), [raw(jst(2026, 10, 12, 23, 50))], False, True),
        (jst(2026, 10, 12, 16, 10), [raw(jst(2026, 10, 12, 15, 20))], False, True),  # 余裕の時間帯は終了 15:45 基準
        (jst(2026, 10, 12, 16, 10), [raw(jst(2026, 10, 12, 15, 10))], True, True),  # 終了前から止まっていた
    ],
)
def test_jpx_unavailable_session_edges(capsys, now, names, stale, listed):
    fake = Fake(pages=DOWN, raw_names=names)
    result = handle(now, fake.deps())
    assert result.outcome == "jpx_unavailable"
    assert (("ERROR", "jpx_unavailable_stale") in events(capsys)) is stale
    assert any(c[0] == "list" for c in fake.calls) is listed


def test_jpx_unavailable_night_lists_across_midnight(capsys):
    fake = Fake(pages=DOWN, raw_names=[])
    handle(jst(2026, 10, 13, 0, 20), fake.deps())
    assert [c for c in fake.calls if c[0] == "list"] == [
        ("list", "raw/2026/10/12/20261012T2340.json.gz", "raw/2026/10/13/20261013T0021.json.gz")
    ]


def test_jpx_unavailable_no_check_after_calendar_outdated(capsys):
    after = CAL.covered_until + timedelta(days=3)
    while after.weekday() >= 5:
        after += timedelta(days=1)
    fake = Fake(pages=DOWN, raw_names=[])
    result = handle(datetime(after.year, after.month, after.day, 11, 0, tzinfo=JST), fake.deps())
    assert result.outcome == "jpx_unavailable"
    assert not [c for c in fake.calls if c[0] == "list"]
    assert ("ERROR", "jpx_unavailable_stale") not in events(capsys)


def test_jpx_unavailable_list_failure_stays_warning(capsys):
    fake = Fake(pages=DOWN, raw_names=[], list_error=StorageFailed("list: ReadTimeout"))
    result = handle(IN_HOURS, fake.deps())
    assert (result.status, result.outcome) == (200, "jpx_unavailable")
    records = logs(capsys)
    assert [(r["severity"], r["event"]) for r in records] == [("WARNING", "jpx_unavailable")]
    assert records[0]["stale_check"] == "storage_failed"


def test_jpx_unavailable_deadline_stays_warning(capsys):
    # 1 限月の取得で残りが 2 秒未満になった → 一覧せず WARNING のまま（deadline_exceeded にしない）
    fake = Fake(pages=DOWN, raw_names=[])
    fake.cost = {"fetch1": settings.REQUEST_DEADLINE_SECONDS - 1}
    result = handle(IN_HOURS, fake.deps())
    assert (result.status, result.outcome) == (200, "jpx_unavailable")
    assert not [c for c in fake.calls if c[0] == "list"]
    records = logs(capsys)
    assert [(r["severity"], r["event"]) for r in records] == [("WARNING", "jpx_unavailable")]
    assert records[0]["stale_check"] == "deadline_exceeded"


def test_out_of_hours_does_not_list(capsys):
    fake = Fake(raw_names=[])
    result = handle(OUT_OF_HOURS, fake.deps())
    assert result.status == 204
    assert fake.calls == []


def test_ingested_does_not_list(capsys):
    # 通常の分は一覧しない（追加の I/O は jpx_unavailable の分だけ）
    fake = Fake(raw_names=[])
    assert handle(IN_HOURS, fake.deps()).outcome == "ingested"
    assert not [c for c in fake.calls if c[0] == "list"]


def test_stale_logs_error_even_without_update(capsys):
    fake = Fake(exists=True)
    result = handle(jst(2026, 10, 12, 11, 17), fake.deps())  # ページ 10:46 から 31 分
    assert result.outcome == "skipped_no_update"
    assert ("ERROR", "stale") in events(capsys)


def test_not_stale_at_29_minutes(capsys):
    handle(jst(2026, 10, 12, 11, 15), Fake(exists=True).deps())
    assert ("ERROR", "stale") not in events(capsys)


def test_calendar_notice_once_a_day(capsys):
    after = CAL.covered_until + timedelta(days=3)
    at_notice = datetime(after.year, after.month, after.day, 9, 0, tzinfo=JST)
    # 期限切れ: 09:00 の 1 回だけ ERROR。他の分（取引時間中・時間外とも）は出さない
    handle(at_notice, Fake(exists=True).deps())
    assert ("ERROR", "calendar_outdated") in events(capsys)
    for minute in (
        datetime(after.year, after.month, after.day, 9, 1, tzinfo=JST),
        datetime(after.year, after.month, after.day, 7, 0, tzinfo=JST),
        datetime(after.year, after.month, after.day, 12, 0, tzinfo=JST),
    ):
        handle(minute, Fake(exists=True).deps())
        assert all(e[1] != "calendar_outdated" for e in events(capsys)), minute


def test_calendar_expiring_warning_before_deadline(capsys):
    before = CAL.covered_until - timedelta(days=10)
    handle(datetime(before.year, before.month, before.day, 9, 0, tzinfo=JST), Fake(exists=True).deps())
    records = events(capsys)
    assert ("WARNING", "calendar_expiring") in records
    assert all(e[1] != "calendar_outdated" for e in records)
    # 期限まで十分あるときは何も出さない
    handle(jst(2026, 10, 14, 9, 0), Fake(exists=True).deps())
    assert all(e[1] not in ("calendar_expiring", "calendar_outdated") for e in events(capsys))


def test_no_stale_after_calendar_outdated(capsys):
    # 期限切れ後は休業日を判定できないので鮮度を見ない（未知の休業日に毎分 stale を出さない）
    after = CAL.covered_until + timedelta(days=3)
    while after.weekday() >= 5:
        after += timedelta(days=1)
    handle(datetime(after.year, after.month, after.day, 11, 0, tzinfo=JST), Fake(exists=True).deps())
    assert ("ERROR", "stale") not in events(capsys)


def test_no_version_no_stale_and_no_expiry_notice(capsys):
    """#75: 版を読めていない（休業日なし）間は鮮度・期限の通知を出さない（calendar_unavailable は CalendarHolder）"""
    deps = Fake(exists=True).deps()
    deps.calendar = build_calendar()
    handle(jst(2026, 11, 23, 9, 0), deps)  # 版では休業日。休業日なしなので取り込みに行く
    records = events(capsys)
    assert ("INFO", "skipped_no_update") in records
    assert ("ERROR", "stale") not in records
    assert all(e[1] not in ("calendar_expiring", "calendar_outdated") for e in records)


# --- 取引終了後の鮮度（毎日の誤報を出さない） ---


@pytest.mark.parametrize(
    "now, page",
    [
        (jst(2026, 10, 14, 16, 15), "2026/10/14 15:40"),  # 日中の余裕の終わり。最後の更新が 15:40 でも stale にしない
        (jst(2026, 10, 15, 6, 30), "2026/10/15 05:50"),  # 夜間の余裕の終わり
    ],
)
def test_no_stale_in_grace_after_close(capsys, now, page):
    html = page_html(updated_at=page, strikes=STRIKES, atm=40000)
    result = handle(now, Fake(pages={1: html, 2: PAGE2}, exists=True).deps())
    assert result.outcome == "skipped_no_update"
    assert ("ERROR", "stale") not in events(capsys)


def test_stale_before_close_still_detected(capsys):
    html = page_html(updated_at="2026/10/14 15:00", strikes=STRIKES, atm=40000)
    handle(jst(2026, 10, 14, 15, 31), Fake(pages={1: html, 2: PAGE2}, exists=True).deps())
    assert ("ERROR", "stale") in events(capsys)


# --- 全体の期限 ---


def test_timeouts_are_passed():
    fake = Fake()
    handle(IN_HOURS, fake.deps())
    kinds = dict(fake.timeouts)
    assert kinds["fetch1"] == settings.JPX_TIMEOUT_SECONDS
    assert kinds["exists"] == settings.GCS_TIMEOUT_SECONDS
    assert kinds["create"] == settings.GCS_TIMEOUT_SECONDS
    assert kinds["append_option_price"] == settings.BQ_APPEND_TIMEOUT_SECONDS
    assert kinds["append_ingest_runs"] == settings.BQ_APPEND_TIMEOUT_SECONDS


def test_timeout_capped_by_remaining():
    fake = Fake()
    fake.cost = {"fetch1": 30.0, "append_spot_price": 7.0}
    handle(IN_HOURS, fake.deps())
    kinds = dict(fake.timeouts)
    # 期限 45 秒: fetch1 で 30 秒使った後の BQ は残り 15 秒、spot で 7 秒使った後の future は残り 8 秒
    assert kinds["append_spot_price"] == 15.0
    assert kinds["append_future_price"] == 8.0


def test_deadline_exceeded_before_raw_create(capsys):
    fake = Fake()
    fake.cost = {"fetch1": 20.0, "fetch2": 24.0}
    result = handle(IN_HOURS, fake.deps())
    assert (result.status, result.outcome) == (500, "deadline_exceeded")
    # 2 限月の取得後に残り 1 秒 → 生 HTML の作成に入らない
    assert ("create", NAME) not in fake.calls
    last = logs(capsys)[-1]
    assert (last["severity"], last["failure"], last["stage"]) == ("ERROR", "deadline_exceeded", "raw_create")


def test_deadline_exceeded_between_tables_skips_runs(capsys):
    fake = Fake()
    fake.cost = {"append_spot_price": 44.0}
    result = handle(IN_HOURS, fake.deps())
    assert result.outcome == "deadline_exceeded"
    assert ("append", "future_price") not in fake.calls
    assert ("append", "ingest_runs") not in fake.calls
    assert logs(capsys)[-1]["stage"] == "append_future_price"


def test_deadline_exceeded_before_ingest_runs(capsys):
    """3 テーブルは書いた後、完了記録の前に期限を超えた: ingest_runs は書かない（11c の回収対象）"""
    fake = Fake()
    fake.cost = {"append_option_price": 44.0}
    result = handle(IN_HOURS, fake.deps())
    assert (result.status, result.outcome) == (500, "deadline_exceeded")
    assert set(fake.appended) == {"spot_price", "future_price", "option_price"}
    assert ("append", "ingest_runs") not in fake.calls
    last = logs(capsys)[-1]
    assert (last["severity"], last["stage"], last["ingest_run_id"]) == ("ERROR", "append_ingest_runs", "run-1")
    assert last["rows"] == {"spot_price": 1, "future_price": 1, "option_price": 40}


@pytest.mark.parametrize(
    "cost, stage",
    [
        ({"fetch1": 44.0}, "raw_exists"),
        ({"exists": 44.0}, "jpx_page2"),
    ],
)
def test_deadline_exceeded_stages(capsys, cost, stage):
    fake = Fake()
    fake.cost = cost
    assert handle(IN_HOURS, fake.deps()).outcome == "deadline_exceeded"
    assert logs(capsys)[-1]["stage"] == stage


@pytest.mark.parametrize("hang", ["fetch_page", "raw_exists", "raw_create"])
def test_hanging_io_is_cut_off_by_deadline(capsys, monkeypatch, hang):
    """JPX・GCS の呼び出しがタイムアウトを無視して返らなくても、timeout + 余裕で deadline_exceeded にして終える"""
    monkeypatch.setattr(settings, "JPX_TIMEOUT_SECONDS", 0.3)
    monkeypatch.setattr(settings, "GCS_TIMEOUT_SECONDS", 0.3)
    monkeypatch.setattr(settings, "CALL_GRACE_SECONDS", 0.1)
    fake = Fake()
    original = getattr(fake, hang)

    def slow(*args, **kwargs):
        time.sleep(2)
        return original(*args, **kwargs)

    setattr(fake, hang, slow)
    started = time.monotonic()
    result = handle(IN_HOURS, fake.deps())
    assert time.monotonic() - started < 1.5
    assert (result.status, result.outcome) == (500, "deadline_exceeded")
    stage = {"fetch_page": "jpx_page1", "raw_exists": "raw_exists", "raw_create": "raw_create"}[hang]
    assert logs(capsys)[-1]["stage"] == stage


def test_storage_failed_is_error(capsys):
    fake = Fake(exists_error=StorageFailed("timeout"))
    result = handle(IN_HOURS, fake.deps())
    assert (result.status, result.outcome) == (500, "storage_failed")
    assert ("ERROR", "storage_failed") in events(capsys)


# --- Flask のエントリポイント ---


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setenv("INGEST_DRY_RUN", "1")
    monkeypatch.setattr(app_module, "_cfg", None)
    monkeypatch.setattr(app_module, "_deps", None)
    return app_module.app.test_client()


def test_http_out_of_hours_204_without_clients(client, monkeypatch, capsys):
    monkeypatch.setenv("INGEST_FAKE_NOW", "2026-10-12T07:00:00+09:00")

    def boom():
        raise AssertionError("時間外に依存を作った")

    monkeypatch.setattr(app_module, "_real_deps", boom)
    response = client.post("/", headers={"X-CloudScheduler-ScheduleTime": "2026-10-11T22:00:00Z"})
    assert response.status_code == 204
    assert logs(capsys)[-1]["schedule_time"] == "2026-10-11T22:00:00Z"


def test_http_get_is_405(client):
    assert client.get("/").status_code == 405


def test_http_unexpected_error_is_500(client, monkeypatch, capsys):
    monkeypatch.setenv("INGEST_FAKE_NOW", "2026-10-12T11:00:00+09:00")

    def boom():
        raise RuntimeError("x")

    monkeypatch.setattr(app_module, "_real_deps", boom)
    assert client.post("/").status_code == 500
    assert events(capsys)[-1] == ("ERROR", "unexpected")


def test_fake_now_ignored_without_dry_run(monkeypatch):
    monkeypatch.delenv("INGEST_DRY_RUN", raising=False)
    monkeypatch.setenv("INGEST_FAKE_NOW", "2000-01-01T00:00:00+09:00")
    assert app_module._now().year >= 2026


def test_page1_broken_page2_empty_is_unavailable(capsys):
    """1 限月が構造違いでも、2 限月が空ページなら保存せず次の分に任せる"""
    fake = Fake(pages={1: read_fixture("mismatch.html"), 2: empty_page_html()})
    result = handle(IN_HOURS, fake.deps())
    assert (result.status, result.outcome) == (200, "jpx_unavailable")
    assert fake.created == {}


def test_storage_failed_on_create(capsys):
    fake = Fake(create_error=StorageFailed("create raw/x: ReadTimeout"))
    result = handle(IN_HOURS, fake.deps())
    assert (result.status, result.outcome) == (500, "storage_failed")
    assert not [c for c in fake.calls if c[0] == "append"]


# --- 取得を分の頭から遅らせる（#38） ---


@pytest.mark.parametrize(
    "second, at, wait",
    [
        (1.2, 8.0, 6.8),
        (0.0, 8.0, 8.0),
        (7.999, 8.0, 0.001),
        (8.0, 8.0, 0.0),
        (30.0, 8.0, 0.0),
        (59.5, 8.0, 0.0),
        (1.2, 0.0, 0.0),
    ],
)
def test_fetch_wait_seconds(second, at, wait):
    now = MINUTE_HEAD + timedelta(seconds=second)
    assert fetch_wait_seconds(now, at) == pytest.approx(wait)
    # JST でも同じ（分内の秒はタイムゾーンによらない）
    assert fetch_wait_seconds(now.astimezone(JST), at) == pytest.approx(wait)


def test_waits_once_before_page1(capsys):
    fake = Fake(arrival=1.2, fetch_at_second=8.0)
    result = handle(IN_HOURS, fake.deps())
    assert result.outcome == "ingested"
    assert fake.sleeps == [pytest.approx(6.8)]
    assert fake.calls[0][0] == "sleep" and fake.calls[1] == ("fetch", 1)
    # 待った後も各呼び出しのタイムアウトは上限のまま（期限 45 秒に十分収まる）
    kinds = dict(fake.timeouts)
    assert kinds["fetch1"] == settings.JPX_TIMEOUT_SECONDS
    assert kinds["create"] == settings.GCS_TIMEOUT_SECONDS
    assert kinds["append_ingest_runs"] == settings.BQ_APPEND_TIMEOUT_SECONDS
    last = logs(capsys)[-1]
    assert last["wait_ms"] == 6800
    assert last["page1_fetch_second"] == pytest.approx(8.0)


def test_no_wait_when_arrived_late(capsys):
    fake = Fake(arrival=9.5, fetch_at_second=8.0)
    assert handle(IN_HOURS, fake.deps()).outcome == "ingested"
    assert fake.sleeps == []
    last = logs(capsys)[-1]
    assert last["wait_ms"] == 0
    assert last["page1_fetch_second"] == pytest.approx(9.5)


def test_default_fetch_at_second_is_used():
    deps = Deps(CAL, "b", None, None, None, None)
    assert deps.fetch_at_second == settings.FETCH_AT_SECOND == 8.0
    assert deps.sleep is time.sleep


def test_skipped_no_update_also_waits_and_logs_second(capsys):
    fake = Fake(exists=True, arrival=1.0, fetch_at_second=8.0)
    assert handle(IN_HOURS, fake.deps()).outcome == "skipped_no_update"
    assert fake.sleeps == [pytest.approx(7.0)]
    last = logs(capsys)[-1]
    assert (last["event"], last["wait_ms"], last["page1_fetch_second"]) == ("skipped_no_update", 7000, 8.0)


def test_wait_counts_toward_deadline(capsys):
    """待ちも全体の期限に数える: 上限の待ち（20 秒）の後に JPX が 24 秒かかれば、残り 1 秒で次に入らない"""
    fake = Fake(arrival=0.0, fetch_at_second=settings.MAX_FETCH_AT_SECOND)
    fake.cost = {"fetch1": 24.0}
    result = handle(IN_HOURS, fake.deps())
    assert (result.status, result.outcome) == (500, "deadline_exceeded")
    assert dict(fake.timeouts)["fetch1"] == settings.JPX_TIMEOUT_SECONDS  # 待ち 20 秒の後も 15 秒取れる
    last = logs(capsys)[-1]
    assert (last["stage"], last["wait_ms"]) == ("raw_exists", 20000)


def test_max_wait_plus_measured_processing_fits_deadline():
    """既定の秒で最大の待ち（0 秒目に到着）+ 実測（p50 3.8 秒・最大 5.7 秒）より重い処理 12 秒
    （JPX 3 秒×2、GCS 1 秒×2、BQ 1 秒×4）でも期限内"""
    fake = Fake(arrival=0.0, fetch_at_second=settings.FETCH_AT_SECOND)
    fake.cost = {"fetch1": 3.0, "fetch2": 3.0, "exists": 1.0, "create": 1.0}
    fake.cost.update({f"append_{t}": 1.0 for t in (*settings.DATA_TABLES, settings.RUNS_TABLE)})
    assert handle(IN_HOURS, fake.deps()).outcome == "ingested"
    assert fake.clock_value == pytest.approx(8.0 + 12.0)
    assert fake.clock_value < settings.REQUEST_DEADLINE_SECONDS - settings.MIN_REMAINING_SECONDS


def test_raw_envelope_format_unchanged():
    """生 HTML の封筒は format 1 のまま（キーを変えていない）で、decode_raw で読める"""
    fake = Fake(arrival=1.0, fetch_at_second=8.0)
    handle(IN_HOURS, fake.deps())
    data = fake.created[NAME]
    doc = json.loads(gzip.decompress(data))
    assert doc["format"] == 1
    assert set(doc) == {"format", "pages"}
    assert [set(p) for p in doc["pages"]] == [{"page", "url", "fetched_at", "http_status", "updated_at", "html"}] * 2
    pages = decode_raw(data)
    assert pages[0].fetched_at == MINUTE_HEAD + timedelta(seconds=8)


@pytest.mark.parametrize(
    "value, expected",
    [(None, 8.0), ("", 8.0), ("0", 0.0), ("10", 10.0), ("7.5", 7.5), ("20", 20.0)],
)
def test_settings_fetch_at_second(value, expected):
    env = {"RAW_BUCKET": "b"}
    if value is not None:
        env["INGEST_FETCH_AT_SECOND"] = value
    assert settings.Settings.from_env(env).fetch_at_second == expected


@pytest.mark.parametrize("value", ["-1", "20.5", "abc", "nan", "inf"])
def test_settings_fetch_at_second_invalid(value):
    with pytest.raises(RuntimeError, match="INGEST_FETCH_AT_SECOND"):
        settings.Settings.from_env({"RAW_BUCKET": "b", "INGEST_FETCH_AT_SECOND": value})


def test_real_deps_use_configured_second(monkeypatch):
    monkeypatch.setenv("INGEST_DRY_RUN", "1")
    monkeypatch.setenv("INGEST_FETCH_AT_SECOND", "9")
    monkeypatch.setattr(app_module, "_cfg", None)
    monkeypatch.setattr(app_module, "_deps", None)
    assert app_module._real_deps().fetch_at_second == 9.0


# --- 毎時 0 分の回収（ルート、#34） ---


class FakeRecoveryDeps:
    """ルートの検査用: 回収の依存の作成と一覧の呼び出しだけを記録する"""

    def __init__(self, list_error=None):
        self.calls = []
        self.list_error = list_error

    def build(self):
        from optionchan_ingest.recovery import RecoveryDeps

        self.calls.append("build")

        def list_raw(start_offset, end_offset, timeout):
            self.calls.append(("list", start_offset, end_offset))
            if self.list_error:
                raise self.list_error
            return []

        return RecoveryDeps("b", list_raw, _no_io, _no_io, _no_io)


def _no_io(*args, **kwargs):
    raise AssertionError("一覧が空なのに I/O を呼んだ")


def _boom_recovery():
    raise AssertionError("0 分以外で回収の依存を作った")


@pytest.fixture
def in_hours_client(client, monkeypatch):
    """取引時間中のルート: 毎分の取り込みの依存は偽物（待たない）"""
    fake = Fake()
    monkeypatch.setattr(app_module, "_real_deps", fake.deps)
    monkeypatch.setattr(app_module, "_recovery_deps", None)
    return client, fake


@pytest.mark.parametrize(
    "fake_now, header",
    [
        ("2026-10-12T11:01:00+09:00", "2026-10-12T02:01:00Z"),
        ("2026-10-12T11:59:00+09:00", None),
        # 予定時刻のヘッダを優先する（実時刻が 0 分でも、予定時刻が 59 分なら回収しない）
        ("2026-10-12T11:00:01+09:00", "2026-10-12T01:59:00Z"),
    ],
)
def test_http_not_recovery_minute_skips_recovery(in_hours_client, monkeypatch, capsys, fake_now, header):
    client, fake = in_hours_client
    monkeypatch.setenv("INGEST_FAKE_NOW", fake_now)
    monkeypatch.setattr(app_module, "_real_recovery_deps", _boom_recovery)
    headers = {"X-CloudScheduler-ScheduleTime": header} if header else {}
    response = client.post("/", headers=headers)
    assert (response.status_code, response.get_json()["outcome"]) == (200, "ingested")
    assert "recovery" not in [r["event"] for r in logs(capsys)]


def test_http_recovery_minute_runs_after_ingest(in_hours_client, monkeypatch, capsys):
    client, fake = in_hours_client
    monkeypatch.setenv("INGEST_FAKE_NOW", "2026-10-12T11:00:01+09:00")
    recovery_fake = FakeRecoveryDeps()
    monkeypatch.setattr(app_module, "_real_recovery_deps", recovery_fake.build)
    response = client.post("/", headers={"X-CloudScheduler-ScheduleTime": "2026-10-12T02:00:00Z"})
    assert (response.status_code, response.get_json()["outcome"]) == (200, "ingested")
    assert recovery_fake.calls == [
        "build",
        ("list", "raw/2026/10/12/20261012T0500.json.gz", "raw/2026/10/12/20261012T1101.json.gz"),
    ]
    records = logs(capsys)
    assert [r["event"] for r in records][-2:] == ["ingested", "recovery"]
    assert records[-1]["schedule_time"] == "2026-10-12T02:00:00Z"


def test_http_recovery_uses_header_when_clock_is_late(in_hours_client, monkeypatch, capsys):
    """実時刻が 1 分を過ぎていても（起動の遅れ）、予定時刻が 0 分なら回収する"""
    client, fake = in_hours_client
    monkeypatch.setenv("INGEST_FAKE_NOW", "2026-10-12T11:01:02+09:00")
    recovery_fake = FakeRecoveryDeps()
    monkeypatch.setattr(app_module, "_real_recovery_deps", recovery_fake.build)
    client.post("/", headers={"X-CloudScheduler-ScheduleTime": "2026-10-12T02:00:00Z"})
    assert "build" in recovery_fake.calls


@pytest.mark.parametrize("where", ["build", "list"])
def test_http_recovery_exception_does_not_change_response(in_hours_client, monkeypatch, capsys, where):
    client, fake = in_hours_client
    monkeypatch.setenv("INGEST_FAKE_NOW", "2026-10-12T11:00:01+09:00")
    if where == "build":

        def broken():
            raise RuntimeError("no credentials")

        monkeypatch.setattr(app_module, "_real_recovery_deps", broken)
    else:
        monkeypatch.setattr(app_module, "_real_recovery_deps", FakeRecoveryDeps(RuntimeError("x")).build)
    response = client.post("/")
    assert (response.status_code, response.get_json()) == (
        200,
        {"outcome": "ingested", "rows": {"spot_price": 1, "future_price": 1, "option_price": 40}},
    )
    records = logs(capsys)
    assert ("ERROR", "recovery_failed") in [(r["severity"], r["event"]) for r in records]
    assert [r["stage"] for r in records if r["event"] == "recovery_failed"] == [
        "setup" if where == "build" else "unexpected"
    ]


def test_http_recovery_shares_deadline(in_hours_client, monkeypatch, capsys):
    """通常の処理が期限の大半を使ったら、回収は始めない（同じ 45 秒の期限を共有する）"""
    client, fake = in_hours_client
    fake.cost = {"fetch1": 31.0}  # 残り 14 秒 < RECOVERY_MIN_REMAINING_SECONDS
    monkeypatch.setenv("INGEST_FAKE_NOW", "2026-10-12T11:00:01+09:00")
    recovery_fake = FakeRecoveryDeps()
    monkeypatch.setattr(app_module, "_real_recovery_deps", recovery_fake.build)
    assert client.post("/").status_code == 200
    assert recovery_fake.calls == ["build"]  # 一覧を呼ばない
    last = logs(capsys)[-1]
    assert (last["severity"], last["event"], last["reason"]) == ("WARNING", "recovery_skipped", "no_time")


def test_http_out_of_hours_recovery_minute(client, monkeypatch, capsys):
    """時間外の 0 分: 毎分の取り込みの依存は作らず 204、回収（07:00 は照合も）を行う"""
    monkeypatch.setenv("INGEST_FAKE_NOW", "2026-10-12T07:00:01+09:00")

    def boom():
        raise AssertionError("時間外に取り込みの依存を作った")

    monkeypatch.setattr(app_module, "_real_deps", boom)
    recovery_fake = FakeRecoveryDeps()
    monkeypatch.setattr(app_module, "_real_recovery_deps", recovery_fake.build)
    response = client.post("/", headers={"X-CloudScheduler-ScheduleTime": "2026-10-11T22:00:00Z"})
    assert response.status_code == 204
    assert [c[0] if isinstance(c, tuple) else c for c in recovery_fake.calls] == ["build", "list", "list"]
    assert [r["event"] for r in logs(capsys)] == ["out_of_hours", "recovery", "raw_reconciled"]


def test_http_out_of_hours_not_minute_0_creates_nothing(client, monkeypatch, capsys):
    monkeypatch.setenv("INGEST_FAKE_NOW", "2026-10-12T07:01:00+09:00")

    def boom():
        raise AssertionError("時間外に取り込みの依存を作った")

    monkeypatch.setattr(app_module, "_real_deps", boom)
    monkeypatch.setattr(app_module, "_real_recovery_deps", _boom_recovery)
    assert client.post("/", headers={"X-CloudScheduler-ScheduleTime": "2026-10-11T22:01:00Z"}).status_code == 204
    assert [r["event"] for r in logs(capsys)] == ["out_of_hours"]


def test_dry_run_recovery_deps_touch_nothing(client, monkeypatch, capsys):
    """INGEST_DRY_RUN の回収は一覧が空（GCS・BigQuery に触らない）"""
    monkeypatch.setattr(app_module, "_recovery_deps", None)
    deps = app_module._real_recovery_deps()
    assert deps.list_raw("a", "b", timeout=1) == []
    with pytest.raises(AssertionError):
        deps.completed_sources(None, None, timeout=1)


def test_settings_bq_location_from_tables_json():
    """クエリの場所はデータセットの場所（schema/tokyo/tables.json）。環境変数 BQ_LOCATION で上書きできる"""
    assert settings.Settings.from_env({"RAW_BUCKET": "b"}).bq_location == "asia-northeast1"
    assert settings.Settings.from_env({"RAW_BUCKET": "b", "BQ_LOCATION": "US"}).bq_location == "US"


# --- JPX の取り直し（#88・#130） ---

HTTP_502 = JpxTransient("page 1: HTTP 502")
WAITS = settings.JPX_RETRY_WAITS_SECONDS
N = len(WAITS)


def fetches(fake):
    return [c for c in fake.calls if c[0] == "fetch"]


def retry_logs(records):
    return [r for r in records if r["event"] == "jpx_retry"]


def transients(page, n):
    """互いに区別できる一時的な失敗を n 個（detail が直前の失敗かを見分けるため）"""
    return [JpxTransient(f"page {page}: HTTP {502 + i}") for i in range(n)]


def test_retry_waits_fit_deadline():
    """待ちは正の数で回ごとに伸び、分の頭の待ち + 待ちの合計 + 最小のタイムアウト + 予約が期限に収まる
    （即時の 502 が続けば上限の回数まで取り直せる）"""
    assert N >= 1
    assert all(w > 0 for w in WAITS)
    assert list(WAITS) == sorted(set(WAITS))
    total = (
        settings.FETCH_AT_SECOND
        + sum(WAITS)
        + settings.JPX_RETRY_MIN_TIMEOUT_SECONDS
        + settings.JPX_RETRY_RESERVE_SECONDS
    )
    assert total <= settings.REQUEST_DEADLINE_SECONDS


@pytest.mark.parametrize("k", range(1, N + 1))
def test_retry_after_502_ingests(capsys, k):
    """1 限月が k 回続けて 502 の後に 200 なら取り込む。取り直しの前ごとに待ちを伸ばし、ログに回数と結果が出る"""
    errors = transients(1, k)
    fake = Fake(pages={1: [*errors, PAGE1], 2: PAGE2})
    result = handle(IN_HOURS, fake.deps())
    assert (result.status, result.outcome) == (200, "ingested")
    assert fetches(fake) == [("fetch", 1)] * (k + 1) + [("fetch", 2)]
    assert fake.sleeps == list(WAITS[:k])
    # 取得と待ちが交互（1 回目 → 待ち → 取り直し → …）
    jpx_calls = [c[0] for c in fake.calls if c[0] in ("fetch", "sleep")]
    assert jpx_calls[: 2 * k + 1] == ["fetch", "sleep"] * k + ["fetch"]
    assert set(fake.appended) == {"spot_price", "future_price", "option_price", "ingest_runs"}
    records = logs(capsys)
    assert [(r["severity"], r["event"]) for r in records] == [("INFO", "jpx_retry")] * k + [("INFO", "ingested")]
    retries = retry_logs(records)
    assert [r["page"] for r in retries] == [1] * k
    assert [r["attempt"] for r in retries] == list(range(1, k + 1))
    assert [r["detail"] for r in retries] == [str(e) for e in errors]  # 直前の失敗
    assert [r["retry_wait_ms"] for r in retries] == [int(w * 1000) for w in WAITS[:k]]
    assert records[-1]["jpx_retry"] == {"page": 1, "first": str(errors[0]), "retries": k, "result": "ok"}


@pytest.mark.parametrize(
    "k, detail",
    [(k, "HTTP 503") for k in range(1, N + 1)] + [(1, "ConnectionError"), (1, "ReadTimeout")],
)
def test_retry_page2(capsys, k, detail):
    error = JpxTransient(f"page 2: {detail}")
    fake = Fake(pages={1: PAGE1, 2: [error] * k + [PAGE2]})
    assert handle(IN_HOURS, fake.deps()).outcome == "ingested"
    assert fetches(fake) == [("fetch", 1)] + [("fetch", 2)] * (k + 1)  # 1 限月は 1 回だけ
    assert fake.sleeps == list(WAITS[:k])
    records = logs(capsys)
    assert [r["attempt"] for r in retry_logs(records)] == list(range(1, k + 1))
    assert records[-1]["jpx_retry"] == {"page": 2, "first": str(error), "retries": k, "result": "ok"}


def test_retry_exhausted_is_jpx_unavailable(capsys):
    """N + 1 回続けて一時的な失敗なら jpx_unavailable（detail は最後の失敗）"""
    errors = transients(1, N + 1)
    fake = Fake(pages={1: [*errors, PAGE1], 2: PAGE2})
    result = handle(IN_HOURS, fake.deps())
    assert (result.status, result.outcome) == (200, "jpx_unavailable")
    assert fetches(fake) == [("fetch", 1)] * (N + 1)  # 取り直しは N 回まで
    assert fake.sleeps == list(WAITS)
    assert fake.created == {}
    records = logs(capsys)
    assert [(r["severity"], r["event"]) for r in records] == [("INFO", "jpx_retry")] * N + [
        ("WARNING", "jpx_unavailable")
    ]
    assert records[-1]["failure"] == "jpx_unavailable"
    assert records[-1]["detail"] == str(errors[-1])
    assert records[-1]["jpx_retry"] == {"page": 1, "first": str(errors[0]), "retries": N, "result": "failed"}


@pytest.mark.parametrize("j", range(1, N + 1))
def test_retry_budget_shared_across_pages(capsys, j):
    """1 限月で j 回取り直した後の 2 限月の取り直しは残りの N − j 回まで（1 リクエストの合計が N を超えない）"""
    page2_error = JpxTransient("page 2: HTTP 502")
    fake = Fake(pages={1: [HTTP_502] * j + [PAGE1], 2: [page2_error]})
    assert handle(IN_HOURS, fake.deps()).outcome == "jpx_unavailable"
    assert fetches(fake) == [("fetch", 1)] * (j + 1) + [("fetch", 2)] * (1 + N - j)
    assert fake.sleeps == list(WAITS)  # 待ちはリクエスト全体の通し番号で伸びる
    records = logs(capsys)
    assert [r["attempt"] for r in retry_logs(records)] == list(range(1, N + 1))
    assert records[-1]["event"] == "jpx_unavailable"
    assert records[-1]["detail"] == str(page2_error)
    assert records[-1]["jpx_retry"] == {"page": 2, "first": str(page2_error), "retries": N, "result": "failed"}


@pytest.mark.parametrize("error", [JpxUnavailable(f"page 1: HTTP {status}") for status in (400, 403, 404)])
def test_no_retry_on_4xx(capsys, error):
    fake = Fake(pages={1: [error, PAGE1], 2: PAGE2})
    assert handle(IN_HOURS, fake.deps()).outcome == "jpx_unavailable"
    assert fetches(fake) == [("fetch", 1)]
    assert fake.sleeps == []
    records = logs(capsys)
    assert [r["event"] for r in records] == ["jpx_unavailable"]
    assert "jpx_retry" not in records[0]


def test_no_retry_on_empty_page(capsys):
    fake = Fake(pages={1: [empty_page_html(), PAGE1], 2: PAGE2})
    assert handle(IN_HOURS, fake.deps()).outcome == "jpx_unavailable"
    assert fetches(fake) == [("fetch", 1)]
    assert fake.sleeps == []


def test_no_retry_after_retry_returns_4xx(capsys):
    """取り直しの結果が 4xx なら、それ以降は取り直さない"""
    fake = Fake(pages={1: [HTTP_502, JpxUnavailable("page 1: HTTP 404"), PAGE1], 2: PAGE2})
    assert handle(IN_HOURS, fake.deps()).outcome == "jpx_unavailable"
    assert fetches(fake) == [("fetch", 1), ("fetch", 1)]
    assert fake.sleeps == [WAITS[0]]
    record = logs(capsys)[-1]
    assert record["detail"] == "page 1: HTTP 404"
    assert record["jpx_retry"] == {"page": 1, "first": str(HTTP_502), "retries": 1, "result": "failed"}


def test_no_retry_beyond_deadline(capsys):
    """読み取りタイムアウト（15 秒）の後は、待ち + 取り直し + 残りの処理が期限に収まらないので取り直さない"""
    fake = Fake(pages={1: [JpxTransient("page 1: ReadTimeout"), PAGE1], 2: PAGE2}, arrival=0.0, fetch_at_second=8.0)
    fake.cost = {"fetch1": 15.0}
    result = handle(IN_HOURS, fake.deps())
    assert (result.status, result.outcome) == (200, "jpx_unavailable")
    assert fetches(fake) == [("fetch", 1)]
    assert fake.sleeps == [8.0]  # 分の頭の待ちだけ
    record = logs(capsys)[-1]
    assert record["jpx_retry"] == {
        "page": 1,
        "first": "page 1: ReadTimeout",
        "retries": 0,
        "result": "skipped_deadline",
    }
    assert record["detail"] == "page 1: ReadTimeout"


@pytest.mark.parametrize(
    "first_cost, retried",
    [
        # 残り − 待ち 3（WAITS[0]）− 予約 15 が 5 以上なら取り直す: 期限 45 − 待ち 8 − 1 回目 = 残り
        (14.0, True),  # 残り 23 → 予算 5
        (14.5, False),  # 残り 22.5 → 予算 4.5
    ],
)
def test_retry_deadline_boundary(capsys, first_cost, retried):
    assert WAITS[0] == 3  # 上のコメントの数値の前提
    fake = Fake(pages={1: [HTTP_502, PAGE1], 2: PAGE2}, arrival=0.0, fetch_at_second=8.0)
    fake.cost = {"fetch1": first_cost}
    handle(IN_HOURS, fake.deps())
    assert len(fetches(fake)) == (3 if retried else 1)
    if retried:
        # 取り直しのタイムアウトは予算（5 秒）で頭打ち
        assert [t for k, t in fake.timeouts if k == "fetch1"] == [settings.JPX_TIMEOUT_SECONDS, pytest.approx(5.0)]


def with_fetch_costs(fake, kind, costs):
    """kind の取得を呼ぶたびに costs の秒数だけ偽の時間を進める"""
    costs = iter(costs)
    original = fake._tick

    def tick(k, timeout):
        if k == kind:
            fake.cost[kind] = next(costs)
        original(k, timeout)

    fake._tick = tick


def test_retry_leaves_time_for_the_rest(capsys):
    """8 秒目の 502 → 3 秒待って取り直し、取り直しが上限の 15 秒かかっても、残りの処理に 15 秒が残る"""
    fake = Fake(pages={1: [HTTP_502, PAGE1], 2: PAGE2}, arrival=0.0, fetch_at_second=8.0)
    with_fetch_costs(fake, "fetch1", [0.0, 15.0])
    assert handle(IN_HOURS, fake.deps()).outcome == "ingested"
    assert fake.clock_value == pytest.approx(8.0 + WAITS[0] + 15.0)
    kinds = dict(fake.timeouts)
    assert kinds["exists"] == settings.GCS_TIMEOUT_SECONDS
    assert kinds["append_spot_price"] == settings.BQ_APPEND_TIMEOUT_SECONDS


def test_retry_stops_when_budget_runs_out(capsys):
    """取り直しが 15 秒のタイムアウトで返ったら、次の待ちの分の予算が無いので待たずに止める"""
    fake = Fake(
        pages={1: [HTTP_502, JpxTransient("page 1: ReadTimeout"), PAGE1], 2: PAGE2}, arrival=0.0, fetch_at_second=8.0
    )
    with_fetch_costs(fake, "fetch1", [0.0, 15.0])
    result = handle(IN_HOURS, fake.deps())
    assert (result.status, result.outcome) == (200, "jpx_unavailable")
    assert fetches(fake) == [("fetch", 1), ("fetch", 1)]
    assert fake.sleeps == [8.0, WAITS[0]]  # 分の頭の待ちと 1 回目の取り直しの前の待ちだけ
    records = logs(capsys)
    assert len(retry_logs(records)) == 1
    assert records[-1]["detail"] == "page 1: ReadTimeout"
    assert records[-1]["jpx_retry"] == {
        "page": 1,
        "first": str(HTTP_502),
        "retries": 1,
        "result": "skipped_deadline",
    }


def test_retry_spans_15_seconds(capsys):
    """1 回目が elapsed 6.5 秒の即時の 502 で、以降も即時の 502 なら、最後の取り直しは 15 秒以上後に始まり、
    jpx_unavailable は予約を残した時点で出る"""
    fake = Fake(pages={1: [HTTP_502], 2: PAGE2}, arrival=0.0, fetch_at_second=6.5)
    original = fake.fetch_page
    starts = []

    def fetch(page, timeout):
        starts.append(fake.monotonic())
        return original(page, timeout)

    fake.fetch_page = fetch
    result = handle(IN_HOURS, fake.deps())
    assert (result.status, result.outcome) == (200, "jpx_unavailable")
    assert len(starts) == N + 1
    assert starts[0] == pytest.approx(6.5)
    assert starts[-1] >= 6.5 + 15
    record = logs(capsys)[-1]
    assert record["event"] == "jpx_unavailable"
    assert record["elapsed_ms"] <= (settings.REQUEST_DEADLINE_SECONDS - settings.JPX_RETRY_RESERVE_SECONDS) * 1000
    assert record["jpx_retry"]["retries"] == N


def test_deadline_exceeded_during_retry_is_error(capsys):
    """2 回目の取り直しが返らなければ従来どおり deadline_exceeded（stage は取り直し、回数は加算済み）"""
    errors = transients(1, 2)
    fake = Fake(pages={1: [*errors, PAGE1], 2: PAGE2})
    original = fake.fetch_page
    calls = []

    def fetch(page, timeout):
        calls.append(page)
        if len(calls) == 3:
            raise app_module.DeadlineExceeded("jpx_page1_retry")
        return original(page, timeout)

    fake.fetch_page = fetch
    result = handle(IN_HOURS, fake.deps())
    assert (result.status, result.outcome) == (500, "deadline_exceeded")
    last = logs(capsys)[-1]
    assert (last["severity"], last["event"], last["stage"]) == ("ERROR", "deadline_exceeded", "jpx_page1_retry")
    assert (last["jpx_retry"]["result"], last["jpx_retry"]["retries"]) == ("failed", 2)


# --- 本文の凍結（#88） ---


def minute_page(minute, volume="30", iv="24.50%", hour=10):
    """10:mm（JST）の 1 限月ページ。volume で本文（出来高）、iv で本文に入れない列を変える"""
    side = default_side(40000, "call")
    side[5], side[2] = volume, iv
    rows = {40000: (side, default_side(40000, "put"))}
    return page_html(updated_at=f"2026/10/12 {hour:02d}:{minute:02d}", strikes=STRIKES, atm=40000, rows=rows)


def ingest_minutes(capsys, pages, now=IN_HOURS):
    """同じ BodyWatch（インスタンス）で毎分取り込み、分ごとの body_frozen の有無を返す"""
    watch = BodyWatch()
    frozen = []
    for page in pages:
        fake = Fake(pages={1: page, 2: PAGE2})
        deps = fake.deps()
        deps.body_watch = watch
        assert handle(now, deps).outcome == "ingested"
        frozen.append([r for r in logs(capsys) if r["event"] == "body_frozen"])
    return frozen


def test_body_frozen_after_n_repeats(capsys):
    """取引時間中に本文が N 回（3 回）直前と同じなら WARNING。N − 1 回までは出さない。データは書く"""
    n = settings.BODY_FROZEN_REPEATS
    pages = [minute_page(40 + i) for i in range(n + 2)]
    frozen = ingest_minutes(capsys, pages)
    assert [bool(f) for f in frozen] == [False] * n + [True, True]
    record = frozen[n][0]
    assert record["severity"] == "WARNING"
    assert record["body_repeats"] == n
    assert record["body_since"] == jst(2026, 10, 12, 10, 40).isoformat()
    assert record["snapshot_at"] == jst(2026, 10, 12, 10, 40 + n).isoformat()


def test_body_change_resets_count(capsys):
    n = settings.BODY_FROZEN_REPEATS
    pages = (
        [minute_page(40 + i) for i in range(n)]
        + [minute_page(43, volume="31")]
        + [minute_page(44 + i, volume="31") for i in range(n - 1)]
    )
    assert not any(ingest_minutes(capsys, pages))


def test_body_ignores_iv_changes(capsys):
    """iv・ギリシャ指標は本文に入れない（凍結中も JPX が毎分計算し直して変わる）"""
    n = settings.BODY_FROZEN_REPEATS
    pages = [minute_page(40 + i, iv=f"2{i}.00%") for i in range(n + 1)]
    assert [bool(f) for f in ingest_minutes(capsys, pages)] == [False] * n + [True]


def test_body_not_frozen_after_close(capsys):
    """引け後（ページ 15:45 以降）は本文が止まっても出さない（セッションの余裕の時間帯の取り込み）"""
    pages = [minute_page(45 + i, hour=15) for i in range(6)]
    assert not any(ingest_minutes(capsys, pages, now=jst(2026, 10, 12, 16, 10)))


def test_body_watch_failure_does_not_stop_ingest(capsys, monkeypatch):
    monkeypatch.setattr(app_module, "option_body_hash", lambda options: 1 / 0)
    fake = Fake()
    assert handle(IN_HOURS, fake.deps()).outcome == "ingested"
    assert "ingest_runs" in fake.appended
    assert [r["event"] for r in logs(capsys)] == ["body_watch_failed", "ingested"]

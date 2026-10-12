"""app.handle の経路ごとの検査（I/O は偽物）（AC 11）"""

import json
import time
from datetime import UTC, datetime, timedelta

import pytest
from conftest import REPO_ROOT, read_fixture
from htmlgen import empty_page_html, page_html

from optionchan_ingest import app as app_module
from optionchan_ingest import settings
from optionchan_ingest.app import Deps, handle
from optionchan_ingest.calendar import load_calendar
from optionchan_ingest.io_bq import WriteFailed
from optionchan_ingest.io_gcs import AlreadyExists, StorageFailed
from optionchan_ingest.parser import JST, JpxUnavailable, jst
from optionchan_ingest.snapshot import FetchedPage, decode_raw

CAL = load_calendar(REPO_ROOT / "config" / "trading_calendar.toml")
STRIKES = tuple(range(38000, 43000, 500))
PAGE1 = page_html(strikes=STRIKES, atm=40000)
PAGE2 = page_html(updated_at="2026/10/12 10:45", last_trading_day="2026/12/10", strikes=STRIKES, atm=40000)
IN_HOURS = jst(2026, 10, 12, 11, 0)  # 祝日取引（スポーツの日）の日中。ページ 10:46 から 14 分
OUT_OF_HOURS = jst(2026, 10, 12, 7, 0)
NAME = "raw/2026/10/12/20261012T1046.json.gz"


class Fake:
    def __init__(self, pages=None, exists=False, create_error=None, append_error_on=None, exists_error=None):
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

    def _tick(self, kind, timeout):
        self.timeouts.append((kind, timeout))
        self.clock_value += self.cost.get(kind, 0.0)

    def monotonic(self):
        return self.clock_value

    def fetch_page(self, page, timeout):
        self.calls.append(("fetch", page))
        self._tick(f"fetch{page}", timeout)
        html = self.pages[page]
        if isinstance(html, Exception):
            raise html
        return FetchedPage(
            page, f"https://example.invalid/{page}", datetime(2026, 10, 12, 2, 0, tzinfo=UTC), 200, html.encode("utf-8")
        )

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
            clock=lambda: datetime(2026, 10, 12, 2, 0, 5, tzinfo=UTC),
            new_run_id=lambda: "run-1",
            monotonic=self.monotonic,
        )


def logs(capsys):
    return [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.strip()]


def events(capsys):
    return [(r["severity"], r["event"]) for r in logs(capsys)]


def test_out_of_hours_calls_nothing(capsys):
    fake = Fake()
    result = handle(OUT_OF_HOURS, fake.deps())
    assert result.status == 204
    assert fake.calls == []
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

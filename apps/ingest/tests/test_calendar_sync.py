"""取引カレンダーの自動取り込み（#63）: run_sync・CalendarHolder・ルートへの組み込み・手動ジョブ（I/O は偽物）。

最優先は「誤った休業日で取引日を取りこぼさない」こと:
読めない・不自然なら前のカレンダー（無ければイメージの TOML）を保つ。
"""

import json
import time
from dataclasses import replace
from datetime import UTC, date, datetime

import pytest
from calgen import page_html, standard_rows
from conftest import FIXTURES, REPO_ROOT
from test_app import Fake

from optionchan_ingest import app as app_module
from optionchan_ingest import calendar_sync, settings
from optionchan_ingest import jpx_calendar as jc
from optionchan_ingest.app import Deps, handle
from optionchan_ingest.calendar import load_calendar, session_at
from optionchan_ingest.calendar_sync import CalendarFetchFailed, CalendarHolder, SyncDeps, run_sync
from optionchan_ingest.io_bq import QueryFailed, WriteFailed
from optionchan_ingest.parser import jst
from optionchan_ingest.pipeline import Deadline

CAL = load_calendar(REPO_ROOT / "config" / "trading_calendar.toml")
FIXTURE = (FIXTURES / "holiday_trading.html").read_bytes()
CSV = (FIXTURES / "syukujitsu_2026_2027.csv").read_bytes()
HOLIDAYS = jc.parse_holidays_csv(CSV)
BCP = (date(2026, 11, 23), date(2027, 9, 20))
SYNC_AT = jst(2026, 10, 15, 6, 40)
VERSION_AT = datetime(2026, 10, 14, 21, 40, tzinfo=UTC)


def fixture_version():
    return jc.build_version(jc.parse_jpx_page(FIXTURE))


def loaded(version=None, version_at=VERSION_AT):
    return jc.LoadedVersion(version_at, version or fixture_version(), jc.ACCEPTED_AUTO)


def std_html(confirmed_until=date(2027, 5, 31), closed=BCP, **kwargs):
    return page_html(standard_rows(HOLIDAYS, [2026, 2027], confirmed_until, closed), **kwargs)


def logs(capsys):
    return [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.strip()]


def events(records):
    return [(r["severity"], r["event"]) for r in records]


class FakeSync:
    def __init__(
        self, latest=None, page=FIXTURE, csv=CSV, load_error=None, page_error=None, csv_error=None, write_error=None
    ):
        self.latest = latest
        self.page = page
        self.csv = csv
        self.load_error = load_error
        self.page_error = page_error
        self.csv_error = csv_error
        self.write_error = write_error
        self.calls = []
        self.appended = []

    def load_latest(self, timeout):
        self.calls.append("load")
        if self.load_error:
            raise self.load_error
        return self.latest

    def fetch_page(self, timeout):
        self.calls.append("page")
        if self.page_error:
            raise self.page_error
        return self.page

    def fetch_holidays(self, timeout):
        self.calls.append("csv")
        if self.csv_error:
            raise self.csv_error
        return self.csv

    def append_rows(self, table, rows, timeout):
        self.calls.append(("append", table))
        if self.write_error:
            raise self.write_error
        self.appended.append((table, rows))

    def deps(self, base=CAL):
        return SyncDeps(
            base, self.load_latest, self.fetch_page, self.fetch_holidays, self.append_rows, clock=lambda: VERSION_AT
        )


def sync(fake, now=SYNC_AT, **kwargs):
    return run_sync(now, fake.deps(), Deadline(45, time.monotonic), {}, **kwargs)


# ---- run_sync ----


def test_first_sync_writes_version(capsys):
    """BigQuery に版が無い（初回）: TOML を基準に検証し、1 回の追記で版を書く"""
    fake = FakeSync()
    result = sync(fake)
    assert result.outcome == "updated"
    assert fake.calls == ["load", "page", "csv", ("append", "trading_calendar")]
    [(table, rows)] = fake.appended
    assert len(rows) == 36
    assert {r["version_at"] for r in rows} == {VERSION_AT}
    assert {r["accepted"] for r in rows} == {"auto"}
    assert result.loaded.version == fixture_version()
    [record] = [r for r in logs(capsys) if r["event"] == "calendar_updated"]
    assert record["severity"] == "NOTICE"
    assert record["added_closed"] == [] and record["removed_closed"] == []
    assert record["covered_until"] == "2027-06-30"


def test_unchanged_does_not_fetch_csv_or_write(capsys):
    fake = FakeSync(latest=loaded())
    result = sync(fake)
    assert result.outcome == "unchanged"
    assert fake.calls == ["load", "page"]
    assert events(logs(capsys)) == [("INFO", "calendar_checked")]


@pytest.mark.parametrize(
    "kwargs, stage, calls",
    [
        ({"load_error": QueryFailed("HTTP 403")}, "load", ["load"]),
        ({"load_error": jc.CalendarDataError("bad")}, "load", ["load"]),
        ({"page_error": CalendarFetchFailed("HTTP 503")}, "fetch_page", ["load", "page"]),
        ({"page": b"<html><body>maintenance</body></html>"}, "parse", ["load", "page"]),
        ({"csv_error": CalendarFetchFailed("timeout")}, "fetch_holidays", ["load", "page", "csv"]),
        ({"csv": b"<html>not csv</html>"}, "parse_holidays", ["load", "page", "csv"]),
        ({"write_error": WriteFailed("x")}, "write", ["load", "page", "csv", ("append", "trading_calendar")]),
    ],
)
def test_failures_keep_previous(capsys, kwargs, stage, calls):
    fake = FakeSync(latest=loaded(), **{"page": std_html(confirmed_until=date(2027, 12, 31)), **kwargs})
    result = sync(fake)
    assert result.outcome == "failed"
    assert fake.calls == calls
    record = [r for r in logs(capsys) if r["event"] == "calendar_sync_failed"]
    assert [(r["severity"], r["stage"]) for r in record] == [("ERROR", stage)]
    if stage != "load":
        assert result.loaded == loaded()


def test_deadline(capsys):
    fake = FakeSync()
    result = run_sync(SYNC_AT, fake.deps(), Deadline(1, time.monotonic), {})
    assert result.outcome == "failed"
    assert fake.calls == []
    assert [r["stage"] for r in logs(capsys) if r["event"] == "calendar_sync_failed"] == ["deadline"]


def test_rejected_is_error_and_not_written(capsys):
    """年末年始の欠落（形の問題）は書かずに ERROR。--accept でも越えない"""
    rows = [r for r in standard_rows(HOLIDAYS, [2026, 2027], date(2027, 5, 31), BCP) if r[0] != date(2026, 12, 31)]
    for accept in (None, "確認済み"):
        fake = FakeSync(latest=loaded(), page=page_html(rows))
        assert sync(fake, accept_reason=accept).outcome == "rejected"
        assert fake.appended == []
        [record] = [r for r in logs(capsys) if r["event"] == "calendar_sync_rejected"]
        assert record["severity"] == "ERROR"
        assert record["problems"][0].startswith("year_end_missing")
        assert record["overridable"] is False


def test_closes_soon_rejected_then_accepted(capsys):
    """7 日以内の新しい休業日は自動では受け入れない（誤りなら取りこぼす）。確認後に --accept で受け入れる"""
    closed = BCP + (date(2027, 7, 19),)
    html = std_html(closed=closed)
    now = jst(2027, 7, 14, 6, 40)
    fake = FakeSync(latest=loaded(), page=html)
    assert sync(fake, now=now).outcome == "rejected"
    record = [r for r in logs(capsys) if r["event"] == "calendar_sync_rejected"][0]
    assert record["overridable"] is True
    assert record["added_closed"] == ["2027-07-19"]

    fake = FakeSync(latest=loaded(), page=html)
    result = sync(fake, now=now, accept_reason="JPX の公表を確認")
    assert result.outcome == "updated"
    assert {r["accepted"] for r in fake.appended[0][1]} == {"override"}
    record = [r for r in logs(capsys) if r["event"] == "calendar_updated"][0]
    assert record["accept_reason"] == "JPX の公表を確認"
    assert record["overridden"][0].startswith("closes_soon")


def test_december_publication_is_accepted_automatically(capsys):
    """12 月の確定の公表（covered_until を 2027-12-31 に延ばす）は自動で受け入れる"""
    fake = FakeSync(latest=loaded(), page=std_html(confirmed_until=date(2027, 12, 31)))
    result = sync(fake, now=jst(2026, 12, 20, 6, 40))
    assert result.outcome == "updated"
    assert result.loaded.version.covered_until == date(2027, 12, 31)
    # 引き継ぎ・内容のハッシュ: 次の日は変化なし
    fake2 = FakeSync(latest=result.loaded, page=std_html(confirmed_until=date(2027, 12, 31)))
    assert sync(fake2, now=jst(2026, 12, 21, 6, 40)).outcome == "unchanged"


def test_dry_run_does_not_write(capsys):
    fake = FakeSync(latest=loaded(), page=std_html(confirmed_until=date(2027, 12, 31)))
    assert sync(fake, dry_run=True).outcome == "dry_run"
    assert fake.appended == []
    assert ("INFO", "calendar_sync_dry_run") in events(logs(capsys))


def test_sessions_changed_is_error_but_dates_update(capsys):
    fake = FakeSync(page=std_html(sessions=("8:45&sim;15:30", "17:00&sim;翌6:00")))
    assert sync(fake).outcome == "updated"
    records = logs(capsys)
    [record] = [r for r in records if r["event"] == "calendar_sessions_changed"]
    assert record["severity"] == "ERROR"
    assert "day_close" in record["detail"]


def test_unverified_year_warning(capsys):
    holidays_2028 = {date(2028, 1, 10): "成人の日", date(2028, 2, 11): "建国記念の日", date(2028, 3, 20): "春分の日"}
    rows = standard_rows(HOLIDAYS, [2027], date(2027, 5, 31), (date(2027, 9, 20),))
    rows += standard_rows({**holidays_2028, **HOLIDAYS_EXTRA_2028}, [2028], date(2027, 12, 31))
    fake = FakeSync(latest=loaded(), page=page_html(rows))
    result = sync(fake, now=jst(2027, 2, 10, 6, 40))
    assert result.outcome == "updated", logs(capsys)
    assert ("WARNING", "calendar_unverified_year") in events(logs(capsys))


HOLIDAYS_EXTRA_2028 = {
    date(2028, 2, 23): "天皇誕生日",
    date(2028, 5, 3): "憲法記念日",
    date(2028, 5, 4): "みどりの日",
    date(2028, 5, 5): "こどもの日",
    date(2028, 7, 17): "海の日",
    date(2028, 8, 11): "山の日",
    date(2028, 9, 18): "敬老の日",
    date(2028, 9, 22): "秋分の日",
    date(2028, 10, 9): "スポーツの日",
    date(2028, 11, 3): "文化の日",
    date(2028, 11, 23): "勤労感謝の日",
}


# ---- CalendarHolder ----


class Loader:
    def __init__(self, *results):
        self.results = list(results)
        self.calls = 0

    def __call__(self, timeout):
        self.calls += 1
        result = self.results.pop(0) if len(self.results) > 1 else self.results[0]
        if isinstance(result, Exception):
            raise result
        return result


def december():
    return jc.build_version(jc.parse_jpx_page(std_html(confirmed_until=date(2027, 12, 31))))


TODAY_NOW = jst(2026, 10, 14, 10, 0)


def refresh(holder, stage="startup", now=TODAY_NOW):
    holder.refresh(Deadline(10, time.monotonic), {}, stage, now)


def test_holder_uses_bigquery_version(capsys):
    holder = CalendarHolder(CAL, Loader(loaded(december())))
    refresh(holder)
    assert holder.source == calendar_sync.SOURCE_BIGQUERY
    assert holder.current.covered_until == date(2027, 12, 31)
    assert holder.current.closed_dates == december().closed_dates
    assert holder.current.day_open == CAL.day_open  # 取引時間は TOML
    assert events(logs(capsys)) == [("INFO", "calendar_loaded")]
    refresh(holder, "hourly")  # 同じ版なら何も出さない
    assert logs(capsys) == []


@pytest.mark.parametrize("result", [QueryFailed("HTTP 500"), None, RuntimeError("auth")])
def test_holder_falls_back_to_toml(capsys, result):
    holder = CalendarHolder(CAL, Loader(result))
    refresh(holder)
    assert holder.current is CAL
    assert holder.source == calendar_sync.SOURCE_IMAGE
    assert events(logs(capsys)) == [("WARNING", "calendar_load_failed")]


def test_holder_keeps_previous_value_on_failure(capsys):
    holder = CalendarHolder(CAL, Loader(loaded(december()), QueryFailed("HTTP 500")))
    refresh(holder)
    refresh(holder, "hourly")
    assert holder.source == calendar_sync.SOURCE_BIGQUERY
    assert holder.current.covered_until == date(2027, 12, 31)
    assert events(logs(capsys))[-1] == ("WARNING", "calendar_load_failed")


def test_holder_prefers_newer_toml(capsys):
    old = replace(fixture_version(), covered_until=date(2026, 12, 31))
    holder = CalendarHolder(CAL, Loader(loaded(old)))
    refresh(holder)
    refresh(holder, "hourly")
    assert holder.current is CAL
    assert holder.source == calendar_sync.SOURCE_IMAGE_NEWER
    assert events(logs(capsys)) == [("WARNING", "calendar_source_conflict")]  # 状態が変わったときだけ


def test_holder_deadline_keeps_value(capsys):
    holder = CalendarHolder(CAL, Loader(loaded()))
    holder.refresh(Deadline(1, time.monotonic), {}, "hourly", TODAY_NOW)
    assert holder.current is CAL
    assert logs(capsys)[0]["detail"] == "deadline_exceeded"


def test_holder_without_loader_is_local():
    holder = CalendarHolder(CAL, None)
    refresh(holder)
    assert (holder.remote, holder.attempted, holder.current) == (False, True, CAL)


def test_holiday_trading_day_from_bigquery_is_ingested():
    """BigQuery の版で「実施する」になった祝日（2026-10-12 スポーツの日）は取引日"""
    holder = CalendarHolder(CAL, Loader(loaded()))
    refresh(holder)
    assert session_at(jst(2026, 10, 12, 11, 0), holder.current) is not None
    assert session_at(jst(2026, 11, 23, 11, 0), holder.current) is None


# ---- ingest のルート ----


@pytest.fixture
def remote(monkeypatch):
    """BigQuery から読む設定（dry-run でない）。毎分の取り込みの依存は偽物"""
    monkeypatch.setenv("INGEST_DRY_RUN", "1")  # INGEST_FAKE_NOW を効かせるため（設定は下で差し替える）
    cfg = settings.Settings.from_env({"RAW_BUCKET": "b"})
    state = {"loader": Loader(loaded()), "fake": Fake(), "sync": FakeSync(latest=loaded())}

    def set_loader(loader):
        state["loader"] = loader
        monkeypatch.setattr(app_module, "_holder", CalendarHolder(CAL, loader))

    monkeypatch.setattr(app_module, "_cfg", cfg)
    set_loader(state["loader"])
    monkeypatch.setattr(app_module, "_deps", None)
    monkeypatch.setattr(app_module, "_sync_deps", None)
    monkeypatch.setattr(app_module, "_real_deps", lambda: state["fake"].deps())
    monkeypatch.setattr(app_module, "_real_recovery_deps", _no_recovery)
    monkeypatch.setattr(app_module, "_real_sync_deps", lambda: state["sync"].deps())
    state["set_loader"] = set_loader
    state["client"] = app_module.app.test_client()
    return state


def _no_recovery():
    from optionchan_ingest.recovery import RecoveryDeps

    return RecoveryDeps("b", lambda start, end, timeout: [], None, None, None)


def post(remote, monkeypatch, fake_now, schedule_time=None):
    monkeypatch.setenv("INGEST_FAKE_NOW", fake_now)
    headers = {"X-CloudScheduler-ScheduleTime": schedule_time} if schedule_time else {}
    return remote["client"].post("/", headers=headers)


def test_route_loads_calendar_once_and_ingests(remote, monkeypatch, capsys):
    response = post(remote, monkeypatch, "2026-10-12T11:01:00+09:00")
    assert response.status_code == 200
    post(remote, monkeypatch, "2026-10-12T11:02:00+09:00")
    assert remote["loader"].calls == 1
    assert app_module._holder.source == calendar_sync.SOURCE_BIGQUERY
    assert [e for e in events(logs(capsys)) if e[1].startswith("calendar")] == [("INFO", "calendar_loaded")]


def test_route_load_failure_still_ingests_with_toml(remote, monkeypatch, capsys):
    """起動時に BigQuery が読めなくても TOML で取り込む（取引日を取りこぼさない）"""
    remote["set_loader"](Loader(QueryFailed("HTTP 503")))
    response = post(remote, monkeypatch, "2026-10-12T11:01:00+09:00")
    assert (response.status_code, response.get_json()["outcome"]) == (200, "ingested")
    assert ("WARNING", "calendar_load_failed") in events(logs(capsys))


def test_route_load_time_shortens_deadline(remote, monkeypatch, capsys):
    """起動時の読み込みにかかった秒数だけ、そのリクエストの期限を短くする（Cloud Run の 50 秒を超えない）"""
    monkeypatch.setattr(app_module, "_ensure_calendar", lambda holder, common, now: 40.0)
    remote["fake"].cost = {"fetch1": 4.0}
    response = post(remote, monkeypatch, "2026-10-12T11:01:00+09:00")
    assert response.status_code == 500
    record = [r for r in logs(capsys) if r["event"] == "deadline_exceeded"][0]
    assert record["stage"] == "raw_exists"


def test_route_hourly_refresh(remote, monkeypatch, capsys):
    remote["set_loader"](
        Loader(loaded(), QueryFailed("HTTP 500"), loaded(december(), datetime(2026, 12, 20, tzinfo=UTC)))
    )
    post(remote, monkeypatch, "2026-10-12T11:01:00+09:00")
    post(remote, monkeypatch, "2026-10-12T12:00:01+09:00", "2026-10-12T03:00:00Z")  # 失敗: 前の値のまま
    assert app_module._holder.current.covered_until == date(2027, 6, 30)
    assert app_module._holder.source == calendar_sync.SOURCE_BIGQUERY
    post(remote, monkeypatch, "2026-10-12T12:30:00+09:00")  # 0 分以外は読まない
    post(remote, monkeypatch, "2026-10-12T13:00:01+09:00", "2026-10-12T04:00:00Z")
    assert remote["loader"].calls == 3
    assert app_module._holder.current.covered_until == date(2027, 12, 31)
    # 取り込みの依存は同じものを使い回すので、リクエストごとにカレンダーを差し替える
    remote["fake"] = Fake()
    post(remote, monkeypatch, "2026-10-12T13:01:00+09:00")
    assert remote["fake"].calls  # 取引時間中


def test_route_sync_at_0640(remote, monkeypatch, capsys):
    remote["sync"] = FakeSync(latest=loaded(), page=std_html(confirmed_until=date(2027, 12, 31)))
    response = post(remote, monkeypatch, "2026-10-15T06:40:02+09:00", "2026-10-14T21:40:00Z")
    assert response.status_code == 204
    assert remote["sync"].calls == ["load", "page", "csv", ("append", "trading_calendar")]
    assert ("NOTICE", "calendar_updated") in events(logs(capsys))
    assert app_module._holder.current.covered_until == date(2027, 12, 31)  # 書いた版をすぐ使う


@pytest.mark.parametrize(
    "fake_now, header", [("2026-10-15T06:41:00+09:00", None), ("2026-10-15T06:40:30+09:00", "2026-10-14T21:39:00Z")]
)
def test_route_sync_only_at_0640(remote, monkeypatch, capsys, fake_now, header):
    post(remote, monkeypatch, fake_now, header)
    assert remote["sync"].calls == []


def test_route_sync_exception_does_not_change_response(remote, monkeypatch, capsys):
    def broken():
        raise RuntimeError("no credentials")

    monkeypatch.setattr(app_module, "_real_sync_deps", broken)
    response = post(remote, monkeypatch, "2026-10-15T06:40:02+09:00", "2026-10-14T21:40:00Z")
    assert response.status_code == 204
    [record] = [r for r in logs(capsys) if r["event"] == "calendar_sync_failed"]
    assert (record["severity"], record["stage"]) == ("ERROR", "unexpected")


def test_route_sync_rejected_keeps_calendar(remote, monkeypatch, capsys):
    """不自然なページ（年末の欠落）: 書かずに ERROR、ingest のカレンダーは前のまま"""
    rows = [r for r in standard_rows(HOLIDAYS, [2026, 2027], date(2027, 5, 31), BCP) if r[0] != date(2026, 12, 31)]
    remote["sync"] = FakeSync(latest=loaded(), page=page_html(rows))
    post(remote, monkeypatch, "2026-10-15T06:40:02+09:00", "2026-10-14T21:40:00Z")
    assert remote["sync"].appended == []
    assert date(2026, 12, 31) in app_module._holder.current.closed_dates
    assert ("ERROR", "calendar_sync_rejected") in events(logs(capsys))


def test_route_sync_skipped_in_dry_run(client_dry, monkeypatch, capsys):
    monkeypatch.setenv("INGEST_FAKE_NOW", "2026-10-15T06:40:02+09:00")
    assert client_dry.post("/").status_code == 204
    assert ("INFO", "calendar_sync_skipped") in events(logs(capsys))


@pytest.fixture
def client_dry(monkeypatch):
    monkeypatch.setenv("INGEST_DRY_RUN", "1")
    monkeypatch.setattr(app_module, "_cfg", None)
    monkeypatch.setattr(app_module, "_deps", None)
    return app_module.app.test_client()


@pytest.mark.parametrize(
    "source, expected",
    [(calendar_sync.SOURCE_IMAGE, True), (calendar_sync.SOURCE_BIGQUERY, False), (None, False)],
)
def test_calendar_fallback_notice_at_0900(capsys, source, expected):
    """BigQuery の版を読めずに TOML で判定していれば、09:00 に 1 回 ERROR calendar_fallback"""
    deps = Deps(CAL, "", None, None, None, None, calendar_source=source)
    handle(jst(2026, 10, 17, 9, 0), deps)  # 土曜（時間外）
    assert (("ERROR", "calendar_fallback") in events(logs(capsys))) is expected
    handle(jst(2026, 10, 17, 9, 1), deps)
    assert ("ERROR", "calendar_fallback") not in events(logs(capsys))


def test_is_sync_time():
    assert calendar_sync.is_sync_time(datetime(2026, 10, 14, 21, 40, tzinfo=UTC))
    assert not calendar_sync.is_sync_time(datetime(2026, 10, 14, 21, 41, tzinfo=UTC))
    with pytest.raises(ValueError):
        calendar_sync.is_sync_time(datetime(2026, 10, 15, 6, 40))


# ---- BigQuery の読み込み（REST の値） ----


class FakeQuery:
    def __init__(self, values):
        self.values = values
        self.sql = None

    def query(self, sql, params, timeout):
        self.sql = sql
        return self.values


def rest_values(version, version_at="2026-10-14T21:40:00.000000+00:00"):
    return [
        [
            version_at,
            r.calendar_date.isoformat(),
            "true" if r.is_trading_day else "false",
            r.status,
            r.day_type,
            r.name,
            r.note,
            r.source,
            version.covered_until.isoformat(),
            version.listed_until.isoformat(),
            "auto",
            version.content_hash,
        ]
        for r in version.rows
    ]


def test_load_latest_from_rest():
    query = FakeQuery(rest_values(fixture_version()))
    result = calendar_sync.load_latest(query, "proj", "ds", timeout=5)
    assert result == loaded()
    assert "`proj.ds.trading_calendar_latest`" in query.sql


def test_load_latest_empty_and_bad():
    assert calendar_sync.load_latest(FakeQuery([]), "p", "d", timeout=5) is None
    values = rest_values(fixture_version())
    values[0][2] = "maybe"
    with pytest.raises(jc.CalendarDataError):
        calendar_sync.load_latest(FakeQuery(values), "p", "d", timeout=5)


# ---- 手動のジョブ ----


def run_main(argv, fake, capsys=None):
    return calendar_sync.main(argv, deps=fake.deps(), base_path=REPO_ROOT / "config" / "trading_calendar.toml")


def test_main_first_run_and_unchanged(capsys):
    fake = FakeSync()
    assert run_main([], fake) == 0
    assert len(fake.appended) == 1
    assert run_main([], FakeSync(latest=loaded())) == 0


def test_main_dry_run(capsys):
    fake = FakeSync(latest=loaded(), page=std_html(confirmed_until=date(2027, 12, 31)))
    assert run_main(["--dry-run"], fake) == 0
    assert fake.appended == []


def test_main_rejected_and_failed_exit_1(capsys):
    rows = [r for r in standard_rows(HOLIDAYS, [2026, 2027], date(2027, 5, 31), BCP) if r[0] != date(2026, 12, 31)]
    assert run_main([], FakeSync(latest=loaded(), page=page_html(rows))) == 1
    assert run_main([], FakeSync(page_error=CalendarFetchFailed("x"))) == 1


def test_main_print_toml(capsys):
    assert run_main(["--print-toml"], FakeSync(latest=loaded())) == 0
    out = capsys.readouterr().out
    assert "covered_until = 2027-06-30" in out
    assert "[sessions]" in out
    assert run_main(["--print-toml"], FakeSync()) == 1


def run_update_toml(tmp_path, fake, text=None):
    path = tmp_path / "trading_calendar.toml"
    path.write_text(text or (REPO_ROOT / "config" / "trading_calendar.toml").read_text(encoding="utf-8"), "utf-8")
    code = calendar_sync.main(["--update-toml"], deps=fake.deps(), base_path=path)
    return code, path


def test_main_update_toml_unchanged(tmp_path, capsys):
    before = (REPO_ROOT / "config" / "trading_calendar.toml").read_text(encoding="utf-8")
    code, path = run_update_toml(tmp_path, FakeSync(latest=loaded()))
    assert code == 0
    assert json.loads(capsys.readouterr().out) == {"status": "unchanged", "title": None, "body": None}
    assert path.read_text(encoding="utf-8") == before


def test_main_update_toml_changed(tmp_path, capsys):
    version = jc.build_version(jc.parse_jpx_page(std_html(confirmed_until=date(2027, 12, 31))))
    code, path = run_update_toml(tmp_path, FakeSync(latest=loaded(version)))
    assert code == 0
    out = json.loads(capsys.readouterr().out)
    assert out["status"] == "changed"
    assert "covered_until 2027-06-30 → 2027-12-31" in out["title"]
    assert "version_at 2026-10-14T21:40:00+00:00" in out["body"]
    assert load_calendar(path).covered_until == date(2027, 12, 31)


def test_main_update_toml_toml_newer(tmp_path, capsys):
    text = (REPO_ROOT / "config" / "trading_calendar.toml").read_text(encoding="utf-8")
    newer = text.replace("covered_until = 2027-06-30", "covered_until = 2027-12-31")
    code, path = run_update_toml(tmp_path, FakeSync(latest=loaded()), newer)
    assert code == 0
    assert json.loads(capsys.readouterr().out)["status"] == "toml_newer"
    assert path.read_text(encoding="utf-8") == newer


def test_main_update_toml_load_failure(tmp_path, capsys):
    assert run_update_toml(tmp_path, FakeSync())[0] == 1
    assert run_update_toml(tmp_path, FakeSync(load_error=QueryFailed("HTTP 403")))[0] == 1
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize(
    "argv",
    [
        ["--accept", ""],
        ["--unknown"],
        ["--dry-run", "--print-toml"],
        ["--print-toml", "--accept", "x"],
        ["--update-toml", "--accept", "x"],
        ["--update-toml", "--print-toml"],
    ],
)
def test_main_usage(capsys, argv):
    assert run_main(argv, FakeSync()) == 2


# ---- ingest 側の多層防御（#63 のレビュー） ----


class Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def test_holder_rejects_broken_version_with_error(capsys):
    """content_hash が合わない（重複・欠け・書き換え）版は使わず ERROR。同じ理由では 1 回だけ"""
    holder = CalendarHolder(CAL, Loader(jc.CalendarDataError("content_hash が合わない")))
    refresh(holder)
    refresh(holder, "hourly")
    assert holder.current is CAL
    assert holder.source == calendar_sync.SOURCE_IMAGE
    assert events(logs(capsys)) == [("ERROR", "calendar_load_rejected")]


def test_holder_rejects_new_closed_day_within_14_days(capsys):
    """前の値（TOML）に無い休業日が 14 日以内に増える版は使わない（誤りなら取引日を取りこぼす）"""
    bad = jc.build_version(jc.parse_jpx_page(std_html(closed=BCP + (date(2026, 11, 3),))))
    holder = CalendarHolder(CAL, Loader(loaded(bad)))
    refresh(holder, now=jst(2026, 10, 20, 10, 0))
    assert holder.current is CAL
    [record] = logs(capsys)
    assert (record["severity"], record["event"]) == ("ERROR", "calendar_load_rejected")
    assert record["detail"].startswith("closes_soon")
    assert session_at(jst(2026, 11, 3, 10, 0), holder.current) is not None  # 取引日のまま
    # 手動で受け入れた版（override）は使う
    holder = CalendarHolder(CAL, Loader(jc.LoadedVersion(VERSION_AT, bad, jc.ACCEPTED_OVERRIDE)))
    refresh(holder, now=jst(2026, 10, 20, 10, 0))
    assert date(2026, 11, 3) in holder.current.closed_dates


def test_holder_rejected_keeps_previous_bigquery_value(capsys):
    bad = jc.build_version(jc.parse_jpx_page(std_html(closed=BCP + (date(2026, 11, 3),))))
    later = datetime(2026, 10, 19, 21, 40, tzinfo=UTC)
    holder = CalendarHolder(CAL, Loader(loaded(december()), loaded(bad, later)))
    refresh(holder, now=jst(2026, 10, 20, 10, 0))
    refresh(holder, "hourly", now=jst(2026, 10, 20, 11, 0))
    assert holder.source == calendar_sync.SOURCE_BIGQUERY
    assert holder.current.covered_until == date(2027, 12, 31)
    assert date(2026, 11, 3) not in holder.current.closed_dates
    assert ("ERROR", "calendar_load_rejected") in events(logs(capsys))


def test_holder_retries_startup_load_after_backoff(capsys):
    """起動時の読み込みが失敗したら、毎時 0 分を待たず CALENDAR_RETRY_SECONDS 後のリクエストで読み直す"""
    clock = Clock()
    loader = Loader(QueryFailed("HTTP 503"), loaded())
    holder = CalendarHolder(CAL, loader, monotonic=clock)
    assert holder.due()
    refresh(holder)
    assert not holder.due()
    clock.t += settings.CALENDAR_RETRY_SECONDS - 1
    assert not holder.due()
    clock.t += 1
    assert holder.due()
    refresh(holder)
    assert holder.source == calendar_sync.SOURCE_BIGQUERY
    assert not holder.due()  # 読めたら毎時 0 分の読み直しに任せる
    clock.t += 10_000
    assert not holder.due()


def test_holder_no_retry_after_previous_value(capsys):
    """前に読めた版があれば、失敗しても再試行を早めない（毎時 0 分に任せる）"""
    clock = Clock()
    holder = CalendarHolder(CAL, Loader(loaded(), QueryFailed("HTTP 503")), monotonic=clock)
    refresh(holder)
    refresh(holder, "hourly")
    clock.t += 10_000
    assert not holder.due()


def test_route_retries_startup_load(remote, monkeypatch, capsys):
    clock = Clock()
    loader = Loader(QueryFailed("HTTP 503"), loaded())
    holder = CalendarHolder(CAL, loader, monotonic=clock)
    monkeypatch.setattr(app_module, "_holder", holder)
    post(remote, monkeypatch, "2026-10-12T11:01:00+09:00")
    post(remote, monkeypatch, "2026-10-12T11:02:00+09:00")
    assert loader.calls == 1
    clock.t += settings.CALENDAR_RETRY_SECONDS
    post(remote, monkeypatch, "2026-10-12T11:06:00+09:00")
    assert loader.calls == 2
    assert holder.source == calendar_sync.SOURCE_BIGQUERY


def test_calendar_fallback_image_newer_is_warning(capsys):
    """TOML の方が新しく意図して TOML を使うときは ERROR にしない（WARNING）"""
    deps = Deps(CAL, "", None, None, None, None, calendar_source=calendar_sync.SOURCE_IMAGE_NEWER)
    handle(jst(2026, 10, 17, 9, 0), deps)
    assert ("WARNING", "calendar_fallback") in events(logs(capsys))


def test_sync_recovers_broken_previous_with_accept(capsys):
    """最新版が壊れている（同じ version_at の重複など）: 通常は失敗、--accept なら TOML を基準に新しい版を書く"""
    fake = FakeSync(load_error=jc.CalendarDataError("content_hash が合わない"))
    assert sync(fake).outcome == "failed"
    fake = FakeSync(load_error=jc.CalendarDataError("content_hash が合わない"))
    assert sync(fake, accept_reason="壊れた版の復旧").outcome == "updated"
    assert len(fake.appended) == 1
    assert ("WARNING", "calendar_previous_ignored") in events(logs(capsys))


def test_load_latest_detects_hash_mismatch():
    values = rest_values(fixture_version())
    values[0][5] = "書き換え"
    with pytest.raises(jc.CalendarDataError):
        calendar_sync.load_latest(FakeQuery(values), "p", "d", timeout=5)
    values = rest_values(fixture_version())
    values.append(list(values[0]))  # 同じ version_at の重複行
    values[-1][1] = "2026-11-24"
    with pytest.raises(jc.CalendarDataError):
        calendar_sync.load_latest(FakeQuery(values), "p", "d", timeout=5)

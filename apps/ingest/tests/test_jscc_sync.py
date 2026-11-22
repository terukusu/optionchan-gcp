"""JSCC の VaR パラメータの取り込み（#164）: run_sync・main の流れ（I/O は偽物。データは jsccgen の合成）"""

import gzip
import io
import json
import time
from datetime import UTC, date, datetime
from urllib.parse import urljoin

import pytest
import requests
from conftest import REPO_ROOT
from jsccgen import Bpf, Inst, all_instruments_var, default_instruments, listing_html, var_parameter

from optionchan_ingest import bq_proto, jscc, jscc_sync, settings
from optionchan_ingest.io_bq import LoadFailed, QueryFailed, WriteFailed
from optionchan_ingest.jscc_sync import JsccFetchFailed, SyncDeps, run_sync
from optionchan_ingest.parser import jst
from optionchan_ingest.pipeline import Deadline

D1, D2, D3 = date(2026, 11, 18), date(2026, 11, 19), date(2026, 11, 20)
NOW = jst(2026, 11, 20, 18, 30)
WEEKDAY, IM = settings.JSCC_LIST_URLS[0], settings.JSCC_LIST_URLS[1]
NK225 = ["100000001", "100000002", "100000003", "100000004"]
SCHEMA = REPO_ROOT / "schema" / "tokyo"


def schema(table):
    with open(SCHEMA / f"{table}.json", encoding="utf-8") as f:
        return json.load(f)


def expected_margins(bpf: Bpf):
    """合成の BPF から計算した 1 枚の証拠金（照合の基準を合成する。計算の正しさは test_jscc の手計算で検査）"""
    reader = jscc.BpfReader(bpf.lines(), product_group="NK225", business_date=bpf.business_date)
    records = jscc.parse_var_parameter(var_parameter())
    out = {}
    for inst in reader:
        hist = [t == "H" for _, t in reader.scenarios]
        conf, n = jscc.stress_params(records, inst.aggregation_group)
        out[inst.instrument_code] = jscc.instrument_margins(inst, hist, conf, n)
    return out


def names(day):
    ymd = day.strftime("%Y%m%d")
    return f"BPF_IDX_{ymd}_1600.zip", f"VaRParameter_{ymd}_1600.csv", f"All-Instruments-VaR_{ymd}_1600.csv"


class FakeJscc:
    def __init__(self, days=(D2, D3), succeeded=(), bpfs=None, margins=None):
        self.files: dict[str, bytes] = {}
        self.errors: dict[str, Exception] = {}
        self.succeeded = set(succeeded)
        self.calls = []
        self.loads = []
        self.appended = []
        self.load_error = None
        self.append_errors: list[Exception] = []
        self.query_error = None
        weekday, im = [], []
        for day in days:
            bpf = (bpfs or {}).get(day) or Bpf(business_date=day)
            bpf_name, param_name, aiv_name = names(day)
            weekday += [bpf_name, param_name]
            im.append(aiv_name)
            self.files[urljoin(WEEKDAY, bpf_name)] = bpf.zip_bytes()
            self.files[urljoin(WEEKDAY, param_name)] = var_parameter().encode()
            ref = (margins or {}).get(day) or expected_margins(bpf)
            self.files[urljoin(IM, aiv_name)] = all_instruments_var(ref).encode()
        self.files[WEEKDAY] = listing_html(weekday).encode()
        self.files[IM] = listing_html(im).encode()
        self.files[settings.JSCC_LIST_URLS[2]] = listing_html([]).encode()
        self.files[settings.JSCC_LIST_URLS[3]] = listing_html([]).encode()
        self._ids = iter(f"00000000-0000-4000-8000-{i:012d}" for i in range(1, 100))

    def fetch(self, url, timeout):
        self.calls.append(("fetch", url.rsplit("/", 1)[-1]))
        if url in self.errors:
            raise self.errors[url]
        if url not in self.files:
            raise JsccFetchFailed(f"{url}: HTTP 404")
        return self.files[url]

    def download(self, url, timeout):
        self.calls.append(("download", url.rsplit("/", 1)[-1]))
        if url in self.errors:
            raise self.errors[url]
        return io.BytesIO(self.files[url])

    def query_succeeded(self, start, timeout):
        self.calls.append(("query", start))
        if self.query_error:
            raise self.query_error
        return set(self.succeeded)

    def load(self, table, partition, fileobj, job_id, timeout):
        self.calls.append(("load", table, partition))
        if self.load_error and table == self.load_error[0]:
            error, self.load_error = self.load_error[1], None  # 1 回だけ
            raise error
        data = fileobj.read()
        if settings.JSCC_LOAD_GZIP:
            data = gzip.decompress(data)
        rows = [json.loads(line) for line in data.decode().splitlines()]
        self.loads.append({"table": table, "partition": partition, "rows": rows, "job_id": job_id})

    def append_run(self, table, rows, timeout):
        self.calls.append(("append", table))
        if self.append_errors:
            raise self.append_errors.pop(0)
        self.appended.append((table, rows))

    def deps(self):
        times = iter(datetime(2026, 11, 20, 9, 30, s % 60, tzinfo=UTC) for s in range(1000))
        return SyncDeps(
            self.fetch,
            self.download,
            self.query_succeeded,
            self.load,
            self.append_run,
            clock=lambda: next(times),
            new_id=lambda: next(self._ids),
        )

    def runs(self):
        return [row for _, rows in self.appended for row in rows]


def logs(capsys):
    return [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.strip()]


def events(records):
    return [(r["severity"], r["event"]) for r in records]


def sync(fake, now=NOW, deadline=None, **kwargs):
    return run_sync(now, fake.deps(), deadline or Deadline(3300, time.monotonic), {"execution": "exec-1"}, **kwargs)


# ---- 自動（引数なし） ----


def test_auto_ingests_pending_days_in_order(capsys):
    fake = FakeJscc()
    result = sync(fake)
    assert result.outcome == "ok"
    assert [(d.business_date, d.status) for d in result.days] == [(D2, "succeeded"), (D3, "succeeded")]
    loads = [(c[1], c[2]) for c in fake.calls if c[0] == "load"]
    order = [settings.JSCC_PARAMETER_TABLE, settings.JSCC_SCENARIO_SET_TABLE, settings.JSCC_SCENARIO_TABLE]
    assert loads == [(t, D2) for t in order] + [(t, D3) for t in order]
    # 各日の最後に取り込みの記録
    assert [c[0] for c in fake.calls if c[0] in ("load", "append")] == (["load"] * 3 + ["append"]) * 2
    runs = fake.runs()
    assert [(r["business_date"], r["status"], r["trigger"]) for r in runs] == [
        (D2, "succeeded", "auto"),
        (D3, "succeeded", "auto"),
    ]
    assert runs[0]["rows_scenario"] == 4 and runs[0]["scenarios"] == 7
    assert (runs[0]["scenarios_h"], runs[0]["scenarios_s"]) == (4, 3)
    assert (runs[0]["margin_matched"], runs[0]["margin_mismatched"], runs[0]["margin_ref_only"]) == (8, 0, 0)
    assert runs[0]["rows_parameter"] == 6
    assert runs[0]["execution"] == "exec-1"
    assert runs[0]["bpf_bytes"] > 0
    records = logs(capsys)
    assert ("NOTICE", "jscc_ingested") in events(records)
    assert not [r for r in records if r["severity"] in ("ERROR", "WARNING")]


def test_scenario_rows_only_nk225_and_match_schema():
    fake = FakeJscc(days=(D3,))
    sync(fake)
    by_table = {x["table"]: x for x in fake.loads}
    scen = by_table[settings.JSCC_SCENARIO_TABLE]
    assert scen["partition"] == D3
    assert [r["instrument_code"] for r in scen["rows"]] == NK225  # NK225 以外の行は 0 件
    assert {r["product_group_code"] for r in scen["rows"]} == {"NK225"}
    for table, x in by_table.items():
        cols = [c["name"] for c in schema(table)]
        for row in x["rows"]:
            assert list(row) == cols, table
    assert all(len(r["pnl"]) == 7 for r in scen["rows"])
    margins = expected_margins(Bpf(business_date=D3))
    assert [(r["margin_short_jscc"], r["margin_long_jscc"]) for r in scen["rows"]] == [margins[c] for c in NK225]
    assert scen["job_id"] == "jscc_jscc_var_scenario_20261120_00000000000040008000000000000001"
    assert by_table[settings.JSCC_SCENARIO_SET_TABLE]["rows"][0]["scenarios_h"] == 4


def test_run_row_encodes_for_storage_write():
    """取り込みの記録の行は schema の列と 1 対 1 で、Storage Write API の proto にできる"""
    fake = FakeJscc(days=(D3,))
    sync(fake)
    table_schema = schema(settings.JSCC_RUNS_TABLE)
    [row] = fake.runs()
    assert list(row) == [c["name"] for c in table_schema]
    descriptor = bq_proto.schema_to_descriptor(table_schema, name="jscc_ingest_runs")
    bq_proto.rows_to_proto(descriptor, table_schema, [row])


def test_succeeded_days_are_not_reingested(capsys):
    fake = FakeJscc(succeeded={D2})
    result = sync(fake)
    assert [d.business_date for d in result.days] == [D3]
    assert ("fetch", "VaRParameter_20261119_1600.csv") not in fake.calls
    assert ("query", settings.JSCC_START_DATE) in fake.calls


def test_nothing_to_do(capsys):
    fake = FakeJscc(succeeded={D2, D3})
    result = sync(fake)
    assert result.outcome == "nothing"
    assert not [c for c in fake.calls if c[0] in ("load", "append", "download")]
    assert events(logs(capsys)) == [("INFO", "jscc_nothing_to_do")]


def test_days_before_start_and_future_are_ignored(capsys):
    fake = FakeJscc(days=(date(2026, 11, 13), D3, date(2026, 11, 24)))
    result = sync(fake)
    assert [d.business_date for d in result.days] == [D3]


def test_max_dates_per_run(monkeypatch, capsys):
    monkeypatch.setattr(settings, "JSCC_MAX_DATES_PER_RUN", 1)
    fake = FakeJscc(days=(D1, D2, D3))
    result = sync(fake)
    assert [d.business_date for d in result.days] == [D1]
    [done] = [r for r in logs(capsys) if r["event"] == "jscc_sync_finished"]
    assert done["remaining"] == ["2026-11-19", "2026-11-20"]


def test_incomplete_day_is_skipped(capsys):
    fake = FakeJscc()
    fake.files[IM] = listing_html([names(D3)[2]]).encode()  # D2 の All-Instruments-VaR がまだ無い
    result = sync(fake)
    assert [d.business_date for d in result.days] == [D3]
    assert result.outcome == "ok"
    [warn] = [r for r in logs(capsys) if r["event"] == "jscc_files_incomplete"]
    assert warn["severity"] == "WARNING"
    assert warn["dates"] == ["2026-11-19"]
    assert warn["found"] == {"2026-11-19": ["bpf", "parameter"]}


def test_index_page_wins_over_archive():
    fake = FakeJscc(days=(D3,))
    archive = settings.JSCC_LIST_URLS[2]
    fake.files[archive] = listing_html(list(names(D3))).encode()
    sync(fake)
    assert ("download", names(D3)[0]) in fake.calls
    # archive から作った URL（同じファイル名）は index のものと同じディレクトリでなくても index を使う
    assert urljoin(archive, names(D3)[0]) == urljoin(WEEKDAY, names(D3)[0])


# ---- dry-run・手動 ----


def test_dry_run_does_not_write(capsys):
    fake = FakeJscc()
    result = sync(fake, dry_run=True)
    assert result.outcome == "ok"
    assert [d.status for d in result.days] == ["dry_run", "dry_run"]
    assert not [c for c in fake.calls if c[0] in ("load", "append")]
    ingested = [r for r in logs(capsys) if r["event"] == "jscc_ingested"]
    assert len(ingested) == 2 and all(r["dry_run"] is True for r in ingested)
    assert ingested[0]["rows_scenario"] == 4


def test_dry_run_failure_does_not_write(capsys):
    fake = FakeJscc(days=(D3,))
    fake.errors[urljoin(WEEKDAY, names(D3)[0])] = JsccFetchFailed("HTTP 503")
    result = sync(fake, dry_run=True)
    assert result.outcome == "failed"
    assert not [c for c in fake.calls if c[0] in ("load", "append")]


def test_manual_date_reingests_succeeded_day():
    fake = FakeJscc(succeeded={D2, D3})
    result = sync(fake, dates=[D2])
    assert [d.business_date for d in result.days] == [D2]
    assert [r["trigger"] for r in fake.runs()] == ["manual"]
    assert not [c for c in fake.calls if c[0] == "query"]  # 成功済みかは見ない


def test_manual_date_not_listed(capsys):
    fake = FakeJscc()
    result = sync(fake, dates=[D1])
    assert result.outcome == "not_listed"
    assert not [c for c in fake.calls if c[0] in ("download", "load", "append")]
    [warn] = [r for r in logs(capsys) if r["event"] == "jscc_date_not_listed"]
    assert warn["dates"] == ["2026-11-18"]


# ---- 失敗 ----


def failures(records):
    return [r for r in records if r["event"] == "jscc_sync_failed"]


@pytest.mark.parametrize(
    "setup, stage",
    [
        (
            lambda f: f.errors.__setitem__(urljoin(WEEKDAY, names(D2)[1]), JsccFetchFailed("HTTP 503")),
            "fetch_parameter",
        ),
        (lambda f: f.files.__setitem__(urljoin(WEEKDAY, names(D2)[1]), b"5,X,L01,IDX,1,2\r\n"), "parse_parameter"),
        (lambda f: f.errors.__setitem__(urljoin(IM, names(D2)[2]), JsccFetchFailed("timeout")), "fetch_margin"),
        (lambda f: f.files.__setitem__(urljoin(IM, names(D2)[2]), b"\xff\xfe"), "parse_margin"),
        (lambda f: f.errors.__setitem__(urljoin(WEEKDAY, names(D2)[0]), JsccFetchFailed("HTTP 404")), "fetch_bpf"),
        (lambda f: f.files.__setitem__(urljoin(WEEKDAY, names(D2)[0]), b"not a zip"), "parse_bpf"),
        (
            lambda f: f.files.__setitem__(
                urljoin(WEEKDAY, names(D2)[0]), Bpf(business_date=D2, date_text="20261118").zip_bytes()
            ),
            "parse_bpf",
        ),
        (
            lambda f: f.files.__setitem__(
                urljoin(WEEKDAY, names(D2)[0]), Bpf(business_date=D2).zip_bytes(extra_members=["other.csv"])
            ),
            "parse_bpf",
        ),
        (
            lambda f: f.files.__setitem__(
                urljoin(WEEKDAY, names(D2)[0]), Bpf(business_date=D2).zip_bytes(member="../BPF_IDX_20261119_1600.csv")
            ),
            "parse_bpf",
        ),
        (lambda f: setattr(f, "load_error", (settings.JSCC_SCENARIO_TABLE, LoadFailed("invalid"))), "load"),
        (lambda f: f.append_errors.append(WriteFailed("unavailable")), "record"),
    ],
)
def test_day_failure_is_recorded_and_next_day_continues(setup, stage, capsys):
    fake = FakeJscc()
    setup(fake)
    result = sync(fake)
    assert result.outcome == "failed"
    assert [(d.business_date, d.status) for d in result.days] == [(D2, "failed"), (D3, "succeeded")]
    [err] = failures(logs(capsys))
    assert err["severity"] == "ERROR"
    assert err["stage"] == stage
    assert err["business_date"] == "2026-11-19"
    runs = fake.runs()
    assert [(r["business_date"], r["status"], r["stage"]) for r in runs] == [
        (D2, "failed", stage),
        (D3, "succeeded", None),
    ]
    assert runs[0]["detail"]
    if stage == "load":
        # パラメータ・シナリオの並びは置き換え済み。succeeded の行が無いので読み手は使わず、次の実行が取り直す
        assert [x["table"] for x in fake.loads if x["partition"] == D2] == [
            settings.JSCC_PARAMETER_TABLE,
            settings.JSCC_SCENARIO_SET_TABLE,
        ]


def test_failure_record_failure_is_warning(capsys):
    fake = FakeJscc(days=(D3,))
    fake.errors[urljoin(WEEKDAY, names(D3)[0])] = JsccFetchFailed("HTTP 503")
    fake.append_errors.append(WriteFailed("unavailable"))
    result = sync(fake)
    assert result.outcome == "failed"
    assert ("WARNING", "jscc_run_record_failed") in events(logs(capsys))


def test_unexpected_exception_is_recorded(capsys):
    fake = FakeJscc(days=(D3,))

    def boom(*args, **kwargs):
        raise RuntimeError("bug")

    fake.download = boom
    result = sync(fake)
    assert result.outcome == "failed"
    [err] = failures(logs(capsys))
    assert err["stage"] == "fetch_bpf" and "RuntimeError" in err["detail"]


def test_margin_mismatch_is_error_but_written(capsys):
    bpf = Bpf(business_date=D3)
    margins = expected_margins(bpf)
    margins["100000002"] = (margins["100000002"][0] + 1, margins["100000002"][1])
    margins["900000009"] = (1, 1)  # All-Instruments-VaR にだけある（翌日の新しい銘柄）
    fake = FakeJscc(days=(D3,), margins={D3: margins})
    result = sync(fake)
    assert result.outcome == "ok"
    [run] = fake.runs()
    assert run["status"] == "succeeded"
    assert (run["margin_matched"], run["margin_mismatched"], run["margin_ref_only"], run["margin_bpf_only"]) == (
        7,
        1,
        1,
        0,
    )
    [err] = [r for r in logs(capsys) if r["event"] == "jscc_margin_mismatch"]
    assert err["severity"] == "ERROR"
    assert err["codes"] == ["100000002"]
    assert err["mismatched"] == 1


def test_mismatch_log_has_no_values(capsys):
    """ログに価格・損益の値を出さない（件数と銘柄コードだけ）"""
    instruments = default_instruments()
    instruments[0] = Inst("100000001", price="987654.25", pnl=[-111111, 222222, 333333, 444444, 555555, 666666, 777777])
    bpf = Bpf(business_date=D3, instruments=instruments)
    margins = {code: (1, 1) for code in NK225}
    fake = FakeJscc(days=(D3,), bpfs={D3: bpf}, margins={D3: margins})
    sync(fake)
    out = capsys.readouterr().out
    for value in ("987654", "111111", "222222", "777777"):
        assert value not in out


def test_query_failure_stops(capsys):
    fake = FakeJscc()
    fake.query_error = QueryFailed("HTTP 403")
    result = sync(fake)
    assert result.outcome == "failed"
    assert not [c for c in fake.calls if c[0] in ("download", "load", "append")]
    [err] = failures(logs(capsys))
    assert err["stage"] == "query"


def test_list_page_failure_continues_with_others(capsys):
    fake = FakeJscc(days=(D3,))
    fake.errors[settings.JSCC_LIST_URLS[3]] = JsccFetchFailed("HTTP 503")
    result = sync(fake)
    assert result.outcome == "failed"  # 取れないページがあれば終了コード 1（気付けるように）
    assert [d.status for d in result.days] == ["succeeded"]
    [err] = failures(logs(capsys))
    assert err["stage"] == "list"


def test_no_listing_is_failure(capsys):
    fake = FakeJscc()
    for url in settings.JSCC_LIST_URLS:
        fake.files[url] = b"<html>maintenance</html>"
    result = sync(fake)
    assert result.outcome == "failed"
    [err] = failures(logs(capsys))
    assert err["stage"] == "list"


def test_deadline_skip(capsys):
    fake = FakeJscc()
    clock = iter([0.0] + [3300.0 - settings.JSCC_MIN_REMAINING_SECONDS + 1] * 100)
    deadline = Deadline(3300, lambda: next(clock))
    result = sync(fake, deadline=deadline)
    assert result.days == []
    assert result.outcome == "ok"  # 次の実行に任せる
    [warn] = [r for r in logs(capsys) if r["event"] == "jscc_deadline_skip"]
    assert warn["dates"] == ["2026-11-19", "2026-11-20"]


def test_uncompressed_load(monkeypatch):
    monkeypatch.setattr(settings, "JSCC_LOAD_GZIP", False)
    fake = FakeJscc(days=(D3,))
    sync(fake)
    assert len([x for x in fake.loads if x["table"] == settings.JSCC_SCENARIO_TABLE][0]["rows"]) == 4


# ---- main ----


def test_main_exit_codes(capsys):
    assert jscc_sync.main([], deps=FakeJscc().deps()) == 0
    assert jscc_sync.main(["--dry-run"], deps=FakeJscc().deps()) == 0
    assert jscc_sync.main([], deps=FakeJscc(succeeded={D2, D3}).deps()) == 0
    assert jscc_sync.main(["--date", "2026-11-18"], deps=FakeJscc().deps()) == 2
    assert jscc_sync.main(["--date", "2026/11/18"], deps=FakeJscc().deps()) == 2
    assert jscc_sync.main(["--unknown"], deps=FakeJscc().deps()) == 2
    fake = FakeJscc()
    fake.query_error = QueryFailed("x")
    assert jscc_sync.main([], deps=fake.deps()) == 1


def test_main_dates_and_trigger(monkeypatch, capsys):
    monkeypatch.setenv("CLOUD_RUN_EXECUTION", "jscc-sync-abc")
    fake = FakeJscc(succeeded={D2, D3})
    assert jscc_sync.main(["--date", "2026-11-19", "--date=2026-11-20"], deps=fake.deps()) == 0
    assert [(r["business_date"], r["trigger"], r["execution"]) for r in fake.runs()] == [
        (D2, "manual", "jscc-sync-abc"),
        (D3, "manual", "jscc-sync-abc"),
    ]


def test_main_unexpected(capsys):
    fake = FakeJscc()

    def boom(url, timeout):
        raise RuntimeError("bug")

    fake.fetch = boom
    assert jscc_sync.main([], deps=fake.deps()) == 1
    [err] = failures(logs(capsys))
    assert err["stage"] == "unexpected"


# ---- HTTP・クエリ ----


class FakeResponse:
    def __init__(self, status=200, body=b"", headers=None, error=None):
        self.status_code = status
        self.body = body
        self.headers = headers if headers is not None else {"Content-Length": str(len(body))}
        self.error = error

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def iter_content(self, chunk_size):
        for i in range(0, len(self.body), 3):
            if self.error and i >= 3:
                raise self.error
            yield self.body[i : i + 3]


class FakeHttp:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


def test_fetch_bytes_ok():
    http = FakeHttp(FakeResponse(body=b"abcdefgh"))
    assert jscc_sync.fetch_bytes("https://x.test/a.csv", 120, http) == b"abcdefgh"
    url, kwargs = http.calls[0]
    assert kwargs["stream"] is True
    assert kwargs["allow_redirects"] is False
    assert kwargs["timeout"] == (settings.JSCC_CONNECT_TIMEOUT_SECONDS, settings.JSCC_READ_TIMEOUT_SECONDS)


def test_fetch_redirect_is_failure():
    response = FakeResponse(status=302, body=b"", headers={"Location": "https://elsewhere.test/a.csv"})
    with pytest.raises(jscc_sync.JsccFetchFailed, match="HTTP 302"):
        jscc_sync.fetch_bytes("https://x.test/a.csv", 120, FakeHttp(response))


def test_load_gzip_env_switch():
    assert settings.env_off("JSCC_LOAD_GZIP", {"JSCC_LOAD_GZIP": "0"}) is True
    assert settings.env_off("JSCC_LOAD_GZIP", {"JSCC_LOAD_GZIP": "1"}) is False
    assert settings.env_off("JSCC_LOAD_GZIP", {}) is False


def test_download_file_returns_rewound_file():
    f = jscc_sync.download_file("https://x.test/a.zip", 900, FakeHttp(FakeResponse(body=b"0123456789")))
    assert f.read() == b"0123456789"
    f.close()


@pytest.mark.parametrize(
    "response",
    [
        FakeResponse(status=404),
        FakeResponse(status=503),
        FakeResponse(body=b"abc", headers={"Content-Length": "10"}),  # 途中で切れた
        FakeResponse(body=b"x" * 10, headers={"Content-Length": str(10**12)}),  # 上限超え（宣言）
        FakeResponse(body=b"abcdefgh", error=requests.exceptions.ChunkedEncodingError("reset")),
        requests.exceptions.ConnectTimeout("slow"),
        requests.exceptions.ConnectionError("reset"),
    ],
)
def test_fetch_failures(response):
    with pytest.raises(JsccFetchFailed):
        jscc_sync.fetch_bytes("https://x.test/a.csv", 120, FakeHttp(response))


def test_fetch_size_limit_without_length(monkeypatch):
    monkeypatch.setattr(settings, "JSCC_MAX_TEXT_BYTES", 5)
    with pytest.raises(JsccFetchFailed):
        jscc_sync.fetch_bytes("https://x.test/a.csv", 120, FakeHttp(FakeResponse(body=b"x" * 9, headers={})))


def test_content_encoding_skips_length_check():
    response = FakeResponse(body=b"abcdef", headers={"Content-Length": "3", "Content-Encoding": "gzip"})
    assert jscc_sync.fetch_bytes("https://x.test/a.csv", 120, FakeHttp(response)) == b"abcdef"


class FakeQuery:
    def __init__(self, values):
        self.values = values
        self.calls = []

    def query(self, sql, params, timeout):
        self.calls.append((sql, params, timeout))
        return self.values


def test_succeeded_dates_query():
    q = FakeQuery([["2026-11-16"], ["2026-11-17"]])
    assert jscc_sync.succeeded_dates(q, "p", "optionchan_tokyo", date(2026, 11, 16), 30) == {
        date(2026, 11, 16),
        date(2026, 11, 17),
    }
    sql, params, _ = q.calls[0]
    assert "`p.optionchan_tokyo.jscc_ingest_runs`" in sql
    assert "business_date >= @start" in sql and "status = 'succeeded'" in sql and "@version" in sql
    assert "2026" not in sql
    assert params == {"start": date(2026, 11, 16), "version": "1600"}


def test_succeeded_dates_bad_value():
    with pytest.raises(QueryFailed):
        jscc_sync.succeeded_dates(FakeQuery([["x"]]), "p", "d", date(2026, 11, 16), 30)

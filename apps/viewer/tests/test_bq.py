"""BigQuery の実行の検査（#52 AC 4）。

全クエリに maximum_bytes_billed・location・パラメータを付ける。クライアントは偽物。
"""

import re
from datetime import UTC, date, datetime

import pytest
from conftest import NOW

from optionchan_viewer import bq, settings
from optionchan_viewer.api import atm_iv_params, cm_params, roll_hourly_params

CFG = settings.Settings(project="example-project")


class FakeJob:
    def __init__(self, rows):
        self.rows = rows
        self.timeout = None
        self.total_bytes_billed = 10 * 1024**2

    def result(self, timeout=None):
        self.timeout = timeout
        return self.rows


class FakeRow(dict):
    pass


class FakeClient:
    def __init__(self):
        self.queries = []

    def query(self, sql, job_config=None, location=None):
        self.queries.append((sql, job_config, location))
        return FakeJob([FakeRow(a=1)])


# 各クエリが受け取るパラメータ（api.py が渡すものと同じ名前・型）
PARAMS = {
    "latest_snapshot": {"start_at": NOW, "end_at": NOW},
    "next_snapshot": {"start_at": NOW, "end_at": NOW},
    "smile": {"start_at": NOW, "end_at": NOW},
    "smile_snapshot": {"start_at": NOW, "end_at": NOW},
    "atm_iv_meta": {"start_at": NOW, "end_at": NOW},
    "atm_iv": atm_iv_params(NOW, None, 7, 0),
    "atm_mid": atm_iv_params(NOW, None, 7, 0),
    "atm_iv_minute": {"start_at": NOW, "end_at": NOW, "today": date(2026, 10, 13), "n": 0},
    "atm_mid_minute": {"start_at": NOW, "end_at": NOW, "today": date(2026, 10, 13), "n": 0},
    "trading_holidays": {"start_date": date(2026, 10, 1), "end_date": date(2026, 10, 28)},
    "atm_iv_months": roll_hourly_params(NOW, None, NOW, NOW),
    "atm_mid_months": roll_hourly_params(NOW, None, NOW, NOW),
    "atm_iv_minute_months": {"start_at": NOW, "end_at": NOW},
    "atm_mid_minute_months": {"start_at": NOW, "end_at": NOW},
    "atm_cm_meta": {"start_at": NOW, "end_at": NOW},
    "atm_cm": cm_params(NOW, None, NOW, NOW, 3600),
}


@pytest.mark.parametrize("name", bq.QUERY_NAMES)
def test_job_config(name):
    client = FakeClient()
    runner = bq.BigQueryRunner(CFG, client=client)
    assert runner(name, PARAMS[name]) == [{"a": 1}]
    [(sql, config, location)] = client.queries
    assert location == "asia-northeast1"
    assert config.maximum_bytes_billed == 1024**3
    assert config.use_legacy_sql is False
    got = {p.name: (p.type_, p.value) for p in config.query_parameters}
    assert got == {k: (bq.param_type(v), v) for k, v in PARAMS[name].items()}
    # SQL 中（コメントを除く）の @name とパラメータが一致する（片方だけ直すと落ちる）
    code = "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))
    assert set(re.findall(r"@(\w+)", code)) == set(PARAMS[name])
    assert "${" not in sql
    assert "`example-project.optionchan_tokyo." in sql


def test_param_types():
    assert bq.param_type(datetime(2026, 1, 1, tzinfo=UTC)) == "TIMESTAMP"
    assert bq.param_type(date(2026, 1, 1)) == "DATE"
    assert bq.param_type(7) == "INT64"
    for bad in (True, "7", 1.5, datetime(2026, 1, 1)):
        with pytest.raises(TypeError):
            bq.param_type(bad)


def test_names_come_from_settings():
    runner = bq.BigQueryRunner(settings.Settings(project="other-project", dataset="ds_x"), client=FakeClient())
    for sql in runner.sql.values():
        assert "`other-project.ds_x." in sql


def test_billed_bytes_go_to_callback():
    # 1 日の上限（#95）: 終わったジョブの total_bytes_billed を渡す
    billed = []
    runner = bq.BigQueryRunner(CFG, client=FakeClient(), on_billed=billed.append)
    runner("latest_snapshot", PARAMS["latest_snapshot"])
    runner("latest_snapshot", PARAMS["latest_snapshot"])
    assert billed == [10 * 1024**2, 10 * 1024**2]

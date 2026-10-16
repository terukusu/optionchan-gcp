"""1 日のクエリ量の上限（#95。budget.py と app.py の 429）。"""

import json
from datetime import UTC, datetime, timedelta

import pytest
from conftest import NOW, FakeRunner
from test_app import bars_results

from optionchan_viewer import budget
from optionchan_viewer.app import create_app

GiB = 1024**3


class Clock:
    def __init__(self, t):
        self.t = t

    def __call__(self):
        return self.t


def test_counts_until_limit():
    daily = budget.DailyBudget(2 * GiB, Clock(NOW))
    daily.check()
    daily.add(GiB)
    daily.check()
    daily.add(GiB)
    with pytest.raises(budget.OverBudget):
        daily.check()
    assert daily.used == 2 * GiB


def test_none_and_zero_are_ignored():
    daily = budget.DailyBudget(GiB, Clock(NOW))
    daily.add(None)
    daily.add(0)
    assert daily.used == 0


def test_resets_at_jst_midnight():
    # NOW = 10-13 15:00 JST。JST の 10-14 0:00 = 10-13 15:00 UTC で数え直す
    clock = Clock(NOW)
    daily = budget.DailyBudget(GiB, clock)
    daily.add(GiB)
    clock.t = datetime(2026, 10, 13, 14, 59, tzinfo=UTC)
    with pytest.raises(budget.OverBudget):
        daily.check()
    clock.t = datetime(2026, 10, 13, 15, 0, tzinfo=UTC)
    daily.check()
    assert daily.used == 0


class BillingRunner(FakeRunner):
    """呼ばれるたびに nbytes を課金したことにする偽の runner"""

    def __init__(self, results, daily, nbytes):
        super().__init__(results)
        self.daily = daily
        self.nbytes = nbytes

    def __call__(self, name, params):
        rows = super().__call__(name, params)
        self.daily.add(self.nbytes)
        return rows


@pytest.mark.parametrize("path", ["/smile_snapshot", "/atm_iv_bars?tf=1m", "/atm_iv_data", "/smile_data"])
def test_over_limit_is_429_without_query(path, capsys):
    clock = Clock(NOW)
    daily = budget.DailyBudget(GiB, clock)
    runner = BillingRunner(bars_results(), daily, 600 * 1024**2)
    client = create_app(runner=runner, clock=clock, daily=daily).test_client()
    # 1 回目は 2 本のクエリで 1.2 GiB（クエリの前に見るので、超えた後の 1 本までは通る）
    assert client.get("/atm_iv_bars?tf=1m").status_code == 200
    calls = len(runner.calls)
    res = client.get(path)
    assert res.status_code == 429
    assert res.get_data(as_text=True) == "daily limit\n"
    assert len(runner.calls) == calls
    record = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert record["event"] == "daily_limit" and record["severity"] == "WARNING"
    # 翌日（JST）は通る
    clock.t = NOW + timedelta(days=1)
    assert client.get(path).status_code == 200


def test_pages_do_not_count():
    daily = budget.DailyBudget(1, Clock(NOW))
    daily.add(1)
    client = create_app(runner=FakeRunner({}), clock=Clock(NOW), daily=daily).test_client()
    for path in ("/", "/smile", "/atm_iv", "/favicon.ico", "/static/favicon.svg"):
        assert client.get(path).status_code in (200, 204), path

"""1 日のクエリ量の上限（#95）。

このプロセスが JST の 1 日に BigQuery で課金されたバイト数（ジョブの total_bytes_billed）を数え、上限を超えたら
次のクエリを出さない（app.py が 429 を返す）。数えるのはプロセス（Cloud Run のインスタンス）ごとで、
ほかのインスタンスの分・止まる前の分は数えない（最大 2 インスタンスなら 1 日 最大 上限 × 2。
合計を INFORMATION_SCHEMA・jobs.list で取るには SA に bigquery.jobs.list が要り、jobUser には無い）。
繰り返しの読み込みが続く間はインスタンスが動き続けるので、暴走は止まる。
上限はクエリを出す前に見るので、超えた後の 1 本（最大 maximum_bytes_billed）までは通る。
"""

import threading
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

JST = ZoneInfo("Asia/Tokyo")


class OverBudget(Exception):
    """今日の上限に達した（429。BigQuery は呼ばない）"""


class DailyBudget:
    def __init__(self, limit: int, clock=None):
        self.limit = limit
        self._clock = clock or (lambda: datetime.now(UTC))
        self._lock = threading.Lock()
        self._day = None
        self._used = 0

    def _roll(self):
        day = self._clock().astimezone(JST).date()
        if day != self._day:
            self._day = day
            self._used = 0

    def check(self):
        with self._lock:
            self._roll()
            if self._used >= self.limit:
                raise OverBudget()

    def add(self, nbytes):
        if not nbytes:
            return
        with self._lock:
            self._roll()
            self._used += int(nbytes)

    @property
    def used(self) -> int:
        with self._lock:
            self._roll()
            return self._used

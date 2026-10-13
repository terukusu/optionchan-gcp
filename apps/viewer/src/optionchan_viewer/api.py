"""API の手順: パラメータの検証、クエリの期間の決定、実行（runner 経由）、CSV 化。

runner(name, params) は sql/<name>.sql をパラメータ付きで実行して行（dict）のリストを返す
（bq.BigQueryRunner。テストでは偽物）。期間はすべて now（サーバーの現在時刻、UTC）から固定の上限で決める
（リクエストの値で広がるのは /atm_iv_data・/atm_iv_bars の d だけで、上限は足ごとに settings.BARS_D）。
"""

import re
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

from . import bars, csvfmt
from . import settings as st

JST = ZoneInfo("Asia/Tokyo")
_D_RE = re.compile(r"[0-9]{1,3}")


class BadRequest(Exception):
    """クエリ文字列が不正（400。BigQuery は呼ばない）"""


class NoData(Exception):
    """探す範囲にデータが無い（503）"""


def parse_d(raw: str | None) -> int:
    if raw is None:
        return st.D_DEFAULT
    # fullmatch: 末尾の改行（d=7%0A）も受け付けない
    if not _D_RE.fullmatch(raw):
        raise BadRequest("d")
    d = int(raw)
    if not st.D_MIN <= d <= st.D_MAX:
        raise BadRequest("d")
    return d


def parse_tf(raw: str | None) -> str:
    if raw is None:
        return st.TF_DEFAULT
    # 固定の集合との完全一致（大文字・前後の空白・改行も受け付けない）
    if raw not in st.TIMEFRAMES:
        raise BadRequest("tf")
    return raw


def parse_bars_d(raw: str | None, tf: str) -> int:
    default, low, high = st.BARS_D[tf]
    if raw is None:
        return default
    if not _D_RE.fullmatch(raw):
        raise BadRequest("d")
    d = int(raw)
    if not low <= d <= high:
        raise BadRequest("d")
    return d


def parse_n(raw: str | None) -> int:
    if raw is None:
        return st.N_DEFAULT
    if raw not in tuple(str(v) for v in st.N_VALUES):
        raise BadRequest("n")
    return int(raw)


def _utc(now: datetime) -> datetime:
    if now.tzinfo is None:
        raise ValueError("now は aware な datetime にする")
    return now.astimezone(UTC)


def jst_date(now: datetime) -> date:
    return now.astimezone(JST).date()


def utc_day(t: datetime) -> datetime:
    """UTC の 0 時に切り捨て（BigQuery の TIMESTAMP_TRUNC(t, DAY) と同じ）"""
    return t.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0)


def utc_hour(t: datetime) -> datetime:
    return t.astimezone(UTC).replace(minute=0, second=0, microsecond=0)


def smile_data(runner, now: datetime) -> str:
    now = _utc(now)
    [row] = runner("latest_snapshot", {"start_at": now - timedelta(days=st.SEARCH_DAYS), "end_at": now})
    snapshot_at = row["snapshot_at"]
    if snapshot_at is None:
        raise NoData("no_snapshot")
    rows = runner("smile", {"start_at": snapshot_at, "end_at": snapshot_at + timedelta(seconds=1)})
    # 現行と同じ: 1 限月 put の is_atm の行のうち最初（行使価格の最小）
    atm = next((r["target_price"] for r in rows if r["o1_put_is_atm"]), None)
    if atm is None:
        raise NoData("no_atm")
    return csvfmt.smile_csv(snapshot_at, atm, rows)


def atm_iv_params(now: datetime, last_started_at: datetime | None, d: int, n: int) -> dict:
    """/atm_iv_data のクエリ 2 のパラメータ（純粋関数。テストで期間を検査する）"""
    now = _utc(now)
    floor = utc_hour(now - timedelta(days=st.ATM_IV_FILL_MAX_DAYS))
    fill_start = floor if last_started_at is None else max(last_started_at + timedelta(hours=1), floor)
    days_start = utc_day(now - timedelta(days=d))
    months_since = utc_day(now - timedelta(days=st.ATM_IV_MONTH_DAYS))
    return {
        "fill_start": fill_start,
        "end_at": now,
        "hourly_start": min(days_start, months_since),
        "months_since": months_since,
        "days_start": days_start,
        "today": jst_date(now),
        "n": n,
    }


def atm_iv_data(runner, now: datetime, d: int, n: int) -> str:
    now = _utc(now)
    [meta] = runner("atm_iv_meta", {"start_at": now - timedelta(days=st.SEARCH_DAYS), "end_at": now})
    rows = runner("atm_iv", atm_iv_params(now, meta["last_started_at"], d, n))
    return csvfmt.atm_iv_csv(rows)


def atm_iv_bars(runner, now: datetime, tf: str, d: int, n: int) -> str:
    """/atm_iv_bars（#68）: 足 tf の ATM IV の OHLC（/atm_iv_data と同じ CSV。値の無い足は除く）"""
    now = _utc(now)
    if st.TIMEFRAMES[tf] < 3600:
        rows = _minute_bars(runner, now, d, n)
        if tf != "1m":
            rows = bars.resample(rows, st.TIMEFRAMES[tf])
    elif tf == "1h":
        rows = bars.drop_empty(_hourly(runner, now, d, n)[0])
    else:
        rows = _daily_bars(runner, now, d, n)
    return csvfmt.atm_iv_csv(rows)


def _minute_bars(runner, now: datetime, d: int, n: int) -> list[dict]:
    # 窓は最新のスナップショットの d 日前の UTC の 0 時から（now 基準だと週末・朝に開いたとき行が無い）
    [row] = runner("latest_snapshot", {"start_at": now - timedelta(days=st.SEARCH_DAYS), "end_at": now})
    snapshot_at = row["snapshot_at"]
    if snapshot_at is None:
        return []
    params = {"start_at": utc_day(snapshot_at - timedelta(days=d)), "end_at": now, "today": jst_date(now), "n": n}
    return bars.drop_empty(runner("atm_iv_minute", params))


def _hourly(runner, now: datetime, d: int, n: int) -> tuple[list[dict], dict]:
    """/atm_iv_data と同じクエリ・パラメータの 1 時間足（値の無い足を含む）と、クエリ 2 のパラメータ"""
    [meta] = runner("atm_iv_meta", {"start_at": now - timedelta(days=st.SEARCH_DAYS), "end_at": now})
    params = atm_iv_params(now, meta["last_started_at"], d, n)
    return runner("atm_iv", params), params


def _daily_bars(runner, now: datetime, d: int, n: int) -> list[dict]:
    # d + 1 日分の 1 時間足を読み、先頭（days_start より前の夜間）が欠けた取引日を捨てる
    # （trading_day は時刻について単調なので、それより後の取引日は全部読んだ範囲に入る）
    rows, params = _hourly(runner, now, d + 1, n)
    days_start = params["days_start"]
    holidays = runner("trading_holidays", {
        "start_date": jst_date(days_start) - timedelta(days=1),
        "end_date": jst_date(now) + timedelta(days=st.HOLIDAYS_AHEAD_DAYS),
    })
    holidays = frozenset(r["calendar_date"] for r in holidays)
    after = bars.trading_day(days_start - timedelta(microseconds=1), holidays)
    return bars.daily(rows, holidays, after)

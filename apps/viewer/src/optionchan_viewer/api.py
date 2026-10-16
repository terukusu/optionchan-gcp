"""API の手順: パラメータの検証、クエリの期間の決定、実行（runner 経由）、CSV 化。

runner(name, params) は sql/<name>.sql をパラメータ付きで実行して行（dict）のリストを返す
（bq.BigQueryRunner。テストでは偽物）。期間はすべて now（サーバーの現在時刻、UTC）から固定の上限で決める
（リクエストの値で広がるのは /atm_iv_data の d と /atm_iv_bars の start・end だけで、上限は settings.D_MAX・
settings.BARS_RANGE。/smile_snapshot の at は探す時点を動かすだけで、読む幅は固定）。
"""

import re
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

from . import bars, csvfmt, smile
from . import settings as st

JST = ZoneInfo("Asia/Tokyo")
_D_RE = re.compile(r"[0-9]{1,3}")
_EPOCH_RE = re.compile(r"[0-9]{1,11}")
DAY = timedelta(days=1)


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


def parse_iv(raw: str | None) -> str:
    """/atm_iv_bars の iv（#93）。省略は trade。mid はすべての足（1 時間足・日足は 1 時間足 atm_mid_iv から。#98）"""
    if raw is None:
        return st.IV_DEFAULT
    if raw not in st.IV_KINDS:
        raise BadRequest("iv")
    return raw


def parse_dir(raw: str | None) -> bool:
    """/smile_snapshot の dir（#80）。省略は at 以前で最新（False）、next は at 以後で最初（True）"""
    if raw is None:
        return False
    if raw != "next":
        raise BadRequest("dir")
    return True


def parse_range(raw_start: str | None, raw_end: str | None, tf: str, now: datetime):
    """/atm_iv_bars の start・end（UTC の UNIX 秒、#77）→ (start, end)（aware な UTC）。両方省くと None（既定の範囲）。
    end は now を超えたら now に縮める。片方だけ・形が不正・start >= end・1 回の最大を超える・遡れる上限より前は
    BadRequest"""
    if raw_start is None and raw_end is None:
        return None
    for name, raw in (("start", raw_start), ("end", raw_end)):
        # fullmatch: 符号・小数・空白・末尾の改行を受け付けない
        if raw is None or not _EPOCH_RE.fullmatch(raw):
            raise BadRequest(name)
    now = _utc(now)
    start = datetime.fromtimestamp(int(raw_start), UTC)
    end = datetime.fromtimestamp(int(raw_end), UTC)
    # 未来の end は不正にせず now に縮める（画面の時計がサーバーより進んでいても 400 にしない）
    end = min(end, now)
    _, max_days, lookback_days = st.BARS_RANGE[tf]
    floor = st.BARS_EPOCH if lookback_days is None else max(st.BARS_EPOCH, utc_day(now - timedelta(days=lookback_days)))
    if start < floor or start >= end:
        raise BadRequest("start")
    if end - start > timedelta(days=max_days):
        raise BadRequest("range")
    return start, end


def parse_at(raw: str | None, now: datetime) -> datetime:
    """/smile_snapshot の at（UTC の UNIX 秒、#79）→ aware な UTC。省略は now。形は parse_range と同じ
    （符号・小数・空白・末尾の改行・空は BadRequest）。2010-01-01 UTC より前は BadRequest、未来は now に縮める"""
    now = _utc(now)
    if raw is None:
        return now
    if not _EPOCH_RE.fullmatch(raw):
        raise BadRequest("at")
    at = datetime.fromtimestamp(int(raw), UTC)
    if at < st.BARS_EPOCH:
        raise BadRequest("at")
    # 未来の at は不正にせず now に縮める（画面の時計がサーバーより進んでいても 400 にしない）
    return min(at, now)


def parse_n(raw: str | None) -> int:
    if raw is None:
        return st.N_DEFAULT
    if raw not in tuple(str(v) for v in st.N_VALUES):
        raise BadRequest("n")
    return int(raw)


def parse_bars_n(raw: str | None) -> int | str:
    """/atm_iv_bars の n: parse_n の値に加え、roll（期近つなぎ、#101。完全一致）。/atm_iv_data は parse_n のまま"""
    if raw == st.N_ROLL:
        return st.N_ROLL
    return parse_n(raw)


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


def smile_snapshot(runner, at: datetime, nxt: bool = False) -> str:
    """/smile_snapshot（#79）: at 以前 SEARCH_DAYS 日（[at − 14 日, at]。at ちょうどを含む）で最新の完了した
    スナップショットの 1 時点を、限月 × 行使価格 × call / put の縦持ちの CSV にする（csvfmt.snapshot_csv）。
    nxt（dir=next、#80）なら at 以後 SEARCH_DAYS 日（[at, at + 14 日)）で最初のもの。無ければ（at が最新より後）
    at 以前で最新のもの（画面は時刻が at より前なのを見て「これより新しいデータは無い」と分かる）。
    取引の切れ目を「+」の 1 回で越える。
    スナップショットが無ければ空（/atm_iv_bars の「行が無ければ空の 200」と同じ。5xx と区別できる）"""
    at = _utc(at)
    snapshot_at = None
    if nxt:
        [row] = runner("next_snapshot", {"start_at": at, "end_at": at + timedelta(days=st.SEARCH_DAYS)})
        snapshot_at = row["snapshot_at"]
    if snapshot_at is None:
        # latest_runs は [start, end) なので、at ちょうどのスナップショットを含めるために 1 秒足す
        # （snapshot_at は分の頭）
        [row] = runner("latest_snapshot", {"start_at": at - timedelta(days=st.SEARCH_DAYS),
                                           "end_at": at + timedelta(seconds=1)})
        snapshot_at = row["snapshot_at"]
    if snapshot_at is None:
        return ""
    rows = runner("smile_snapshot", {"start_at": snapshot_at, "end_at": snapshot_at + timedelta(seconds=1)})
    months, out = smile.snapshot_rows(rows)
    return csvfmt.snapshot_csv(snapshot_at, months, out)


def atm_iv_params(now: datetime, last_started_at: datetime | None, d: int, n: int) -> dict:
    """/atm_iv_data のクエリ 2 のパラメータ（純粋関数。テストで期間を検査する）"""
    now = _utc(now)
    return hourly_params(now, last_started_at, utc_day(now - timedelta(days=d)), now, n)


def hourly_params(now: datetime, last_started_at: datetime | None, start: datetime, end: datetime, n: int) -> dict:
    """sql/atm_iv.sql のパラメータ: 足の started_at が [start, end) の 1 時間足（end <= now）。
    end が補う分の始まり（fill_start）以前なら生データを読まない（fill_end を fill_start の 1 日前にして
    逆向きの区間にする。
    #77 の左への追加読み込みで option_price のパーティションを読まないため）"""
    now = _utc(now)
    floor = utc_hour(now - timedelta(days=st.ATM_IV_FILL_MAX_DAYS))
    fill_start = floor if last_started_at is None else max(last_started_at + timedelta(hours=1), floor)
    months_since = utc_day(now - timedelta(days=st.ATM_IV_MONTH_DAYS))
    return {
        "fill_start": fill_start,
        "fill_end": now if end > fill_start else fill_start - DAY,
        "hourly_start": min(start, months_since),
        "months_since": months_since,
        "days_start": start,
        "range_end": end,
        "today": jst_date(now),
        "n": n,
    }


def atm_iv_data(runner, now: datetime, d: int, n: int) -> str:
    now = _utc(now)
    [meta] = runner("atm_iv_meta", {"start_at": now - timedelta(days=st.SEARCH_DAYS), "end_at": now})
    rows = runner("atm_iv", atm_iv_params(now, meta["last_started_at"], d, n))
    return csvfmt.atm_iv_csv(rows)


def atm_iv_bars(runner, now: datetime, tf: str, n: int | str, rng=None,
                iv: str = st.IV_DEFAULT) -> tuple[str, datetime, datetime]:
    """/atm_iv_bars（#68・#77）: 足 tf の ATM IV の OHLC（/atm_iv_data と同じ CSV。値の無い足は除く）と、
    読んだ範囲 (start, end)。rng = parse_range の (start, end)（足の started_at が入る範囲。日足は取引日の 00:00 JST）。
    None なら既定（_bars_range）。iv = parse_iv の値（mid は分足が sql/atm_mid_minute.sql（#93）、1 時間足・日足が
    sql/atm_mid.sql（#98））。n = parse_bars_n の値。roll（期近つなぎ、#101）は _roll_bars（CSV は 6 列目に限月）"""
    now = _utc(now)
    if n == st.N_ROLL:
        return _roll_bars(runner, now, tf, rng, iv)
    rng = _bars_range(runner, now, tf, rng)
    if rng[2]:
        return "", rng[0], rng[1]
    start, end = rng[:2]
    if st.TIMEFRAMES[tf] < 3600:
        params = {"start_at": start, "end_at": end, "today": jst_date(now), "n": n}
        rows = bars.drop_empty(runner("atm_mid_minute" if iv == "mid" else "atm_iv_minute", params))
        if tf != "1m":
            rows = bars.resample(rows, st.TIMEFRAMES[tf])
    elif tf == "1h":
        rows = bars.drop_empty(_hourly(runner, now, start, end, n, iv))
    else:
        read_start, read_end = _daily_read(start, end, now)
        hourly = _hourly(runner, now, read_start, read_end, n, iv)
        holidays = _holidays(runner, read_start, end, st.HOLIDAYS_AHEAD_DAYS)
        rows = _daily(hourly, holidays, read_start, start, end, now)
    return csvfmt.atm_iv_csv(rows), start, end


def _bars_range(runner, now: datetime, tf: str, rng) -> tuple[datetime, datetime, bool]:
    """/atm_iv_bars の範囲 (start, end, empty)（n=0/1 と期近つなぎで共通）。rng（parse_range の値）があればそのまま。
    無ければ足ごとの既定: 分足は最新のスナップショットの BARS_RANGE の既定の日数前の UTC の 0 時〜now
    （now 基準だと週末・朝に開いたとき行が無い。スナップショットが無ければ empty = True で、クエリを読まずに
    空を返す）、1 時間足・日足は now の既定の日数前の UTC の 0 時〜now"""
    default_days = st.BARS_RANGE[tf][0]
    if rng is not None:
        return rng[0], rng[1], False
    if st.TIMEFRAMES[tf] >= 3600:
        return utc_day(now - timedelta(days=default_days)), now, False
    [row] = runner("latest_snapshot", {"start_at": now - timedelta(days=st.SEARCH_DAYS), "end_at": now})
    snapshot_at = row["snapshot_at"]
    if snapshot_at is None:
        return utc_day(now - timedelta(days=default_days)), now, True
    return utc_day(snapshot_at - timedelta(days=default_days)), now, False


def _hourly(runner, now: datetime, start: datetime, end: datetime, n: int, iv: str = st.IV_DEFAULT) -> list[dict]:
    """/atm_iv_data と同じクエリの 1 時間足（値の無い足を含む）。started_at が [start, end)。
    iv = mid なら同じ形の sql/atm_mid.sql（1 時間足 atm_mid_iv、#98）"""
    [meta] = runner("atm_iv_meta", {"start_at": now - timedelta(days=st.SEARCH_DAYS), "end_at": now})
    return runner("atm_mid" if iv == "mid" else "atm_iv", hourly_params(now, meta["last_started_at"], start, end, n))


def _daily_read(start: datetime, end: datetime, now: datetime) -> tuple[datetime, datetime]:
    """日足の 1 時間足を読む範囲: start の DAILY_LOOKBEHIND_DAYS 日前〜end の 1 日後（now まで）"""
    return start - timedelta(days=st.DAILY_LOOKBEHIND_DAYS), min(end + DAY, now)


def _holidays(runner, read_start: datetime, read_end: datetime, ahead_days: int) -> frozenset:
    """読んだ最初の取引日の前日から、read_end の ahead_days 日後までの休業日"""
    rows = runner("trading_holidays", {
        "start_date": jst_date(read_start) - timedelta(days=1),
        "end_date": jst_date(read_end) + timedelta(days=ahead_days),
    })
    return frozenset(r["calendar_date"] for r in rows)


def _daily(rows: list[dict], holidays: frozenset, read_start: datetime, start: datetime, end: datetime,
           now: datetime) -> list[dict]:
    """1 時間足を取引日の日足に束ね、started_at（その取引日の 00:00 JST）が [start, end) のものを返す
    （end = now なら上端なし: 今夜の夜間は明日の取引日で、00:00 JST が now より後になる）。
    読んだ範囲の先頭の取引日（前の夜間が欠けうる）は捨てる"""
    after = bars.trading_day(read_start - timedelta(microseconds=1), holidays)
    return [r for r in bars.daily(rows, holidays, after)
            if r["started_at"] >= start and (end >= now or r["started_at"] < end)]


def roll_hourly_params(now: datetime, last_started_at: datetime | None, start: datetime, end: datetime) -> dict:
    """sql/atm_iv_months.sql（期近つなぎの 1 時間足、#101）のパラメータ: hourly_params の補う分・期間の値だけ
    （限月を選ばないので months_since・today・n・hourly_start は要らない）"""
    params = hourly_params(now, last_started_at, start, end, 0)
    return {k: params[k] for k in ("fill_start", "fill_end", "days_start", "range_end")}


def _roll_bars(runner, now: datetime, tf: str, rng, iv: str) -> tuple[str, datetime, datetime]:
    """/atm_iv_bars?n=roll（期近つなぎ、#101）: 全限月の足を読み、取引日ごとに期近の最終取引日の ROLL_BUSINESS_DAYS
    営業日前で 2 限月へ乗り換えた足（bars.roll）。期間・既定の範囲・読むクエリの量は n=0 と同じ
    （+ 休業日。範囲の決め方は _bars_range・_daily_read を共有）。
    iv = mid なら mid IV のクエリ（分足 #93・1 時間足 / 日足 #98）。
    行が無ければ休業日を読まない（日足を左へ読み続けたときに空の要求で課金を増やさない）"""
    rng = _bars_range(runner, now, tf, rng)
    if rng[2]:
        return "", rng[0], rng[1]
    start, end = rng[:2]
    mid = iv == "mid"
    if st.TIMEFRAMES[tf] < 3600:
        read_start, read_end = start, end
        rows = runner("atm_mid_minute_months" if mid else "atm_iv_minute_months", {"start_at": start, "end_at": end})
    else:
        read_start, read_end = (start, end) if tf == "1h" else _daily_read(start, end, now)
        [meta] = runner("atm_iv_meta", {"start_at": now - timedelta(days=st.SEARCH_DAYS), "end_at": now})
        rows = runner("atm_mid_months" if mid else "atm_iv_months",
                      roll_hourly_params(now, meta["last_started_at"], read_start, read_end))
    if not rows:
        return "", start, end
    # 期近つなぎの休業日は最後の取引日の後の ROLL_BUSINESS_DAYS + 1 営業日目まで
    holidays = _holidays(runner, read_start, read_end, st.ROLL_HOLIDAYS_AHEAD_DAYS)
    rows = bars.roll(rows, holidays, st.ROLL_BUSINESS_DAYS)
    if tf == "1d":
        rows = _daily(rows, holidays, read_start, start, end, now)
    elif tf in ("5m", "15m"):
        # 束ねるのは乗り換えた後（違う限月の足は 1 本にしない）
        rows = bars.resample(rows, st.TIMEFRAMES[tf])
    return csvfmt.atm_iv_months_csv(rows), start, end

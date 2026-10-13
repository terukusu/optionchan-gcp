"""取引カレンダーの自動取り込み（#63）の純粋関数（now()・I/O を使わない）。

- JPX「祝日取引」のページ（祝日取引実施日の表・注記・取引時間の表）と内閣府の祝日 CSV の解析
- 版（Version）の組み立て: ページの行 + 規則（平日の 1/1 は常に休業。表に載らない）+ 前の版の過去年の行の引き継ぎ
- 検証（Problem の一覧）: 形・祝日との照合・年末年始（上書き不可）と、前の版との差分（--accept で上書き可）
- BigQuery の行（schema/tokyo/trading_calendar.json）との変換、TOML（config/trading_calendar.toml）の生成

版の 1 行は「業務規程上の休業日に当たる平日 1 日」（国民の祝日・休日と年末年始 12/31・1/1・1/2・1/3）。
is_trading_day は祝日取引を実施するか。取引日 = 土日と is_trading_day = false の日を除いた日。
"""

import hashlib
import json
import re
import tomllib
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta

from lxml import html as lxml_html

CONFIRMED = "confirmed"
PLANNED = "planned"
STATUSES = (CONFIRMED, PLANNED)
NATIONAL_HOLIDAY = "national_holiday"
YEAR_END = "year_end"
DAY_TYPES = (NATIONAL_HOLIDAY, YEAR_END)
SOURCE_JPX = "jpx_table"
SOURCE_RULE = "rule"
SOURCES = (SOURCE_JPX, SOURCE_RULE)
ACCEPTED_AUTO = "auto"
ACCEPTED_OVERRIDE = "override"
ACCEPTED = (ACCEPTED_AUTO, ACCEPTED_OVERRIDE)

# 年末年始の休業日のうち 1/1 以外（1/1 は国民の祝日。表に載らないので規則で足す）
YEAR_END_DAYS = {(12, 31), (1, 2), (1, 3)}
# 「当分の間、祝日取引を実施しない」日（注4）。平日なら表に「実施しない」で載っていること
YEAR_END_CLOSED_DAYS = {(12, 31), (1, 2)}
WEEKDAYS = "月火水木金土日"
STATUS_TEXT = {"確定": CONFIRMED, "予定": PLANNED}
SESSION_KEYS = ("day_open", "day_close", "night_open", "night_close")

# 検証のしきい値（docs/runbook.md「取引カレンダー（#63）」）
MAX_FUTURE_CHANGES = 3  # 前の版の掲載範囲の中で、今日以降の休業日の追加・削除の上限（1 版あたり）
CLOSE_GUARD_DAYS = 14  # 今日からこの日数以内の日を新しく休業日にしない（ingest 側の検査と同じ日数）
ROWS_PER_YEAR = (10, 25)  # 表の 1 年あたりの行数（2026 年・2027 年は 18 行）
MAX_CLOSED_PER_YEAR = 25  # ingest 側の検査: 1 年あたりの休業平日の上限（実際は 2〜4 日）


class CalendarParseError(Exception):
    """ページ・CSV の形が想定と違う（形式変更・取得の誤り）"""


class CalendarDataError(Exception):
    """BigQuery から読んだ版が不正"""


@dataclass(frozen=True)
class JpxRow:
    day: date
    name: str
    held: bool  # 祝日取引を実施する
    status: str  # CONFIRMED / PLANNED
    note: str | None  # 実施有無の欄が参照する注記の本文


@dataclass(frozen=True)
class JpxPage:
    years: tuple[int, ...]  # 表の年（行の順）
    intro_years: tuple[int, ...]  # 表の前の文（「2026年及び2027年における…」）の年
    rows: tuple[JpxRow, ...]
    sessions: dict[str, time] | None  # 取引時間の表（読めなければ None）


@dataclass(frozen=True)
class DayRow:
    calendar_date: date
    is_trading_day: bool
    status: str
    day_type: str
    name: str
    note: str | None
    source: str


@dataclass(frozen=True)
class Version:
    rows: tuple[DayRow, ...]  # 日付の順
    covered_until: date  # 確定の最終日（確定の最後の行を含む半期の末日）
    listed_until: date  # 表の最終年の 12/31（これより後の平日は未知）

    @property
    def closed_dates(self) -> frozenset[date]:
        return frozenset(r.calendar_date for r in self.rows if not r.is_trading_day)

    @property
    def by_date(self) -> dict[date, DayRow]:
        return {r.calendar_date: r for r in self.rows}

    @property
    def content_hash(self) -> str:
        """内容（版の作成時刻・受け入れ方を除く）の SHA-256。変化の検知に使う"""
        doc = {
            "covered_until": self.covered_until.isoformat(),
            "listed_until": self.listed_until.isoformat(),
            "rows": [
                [r.calendar_date.isoformat(), r.is_trading_day, r.status, r.day_type, r.name, r.note, r.source]
                for r in self.rows
            ],
        }
        text = json.dumps(doc, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        return hashlib.sha256(text.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class LoadedVersion:
    """BigQuery の最新版（trading_calendar_latest）"""

    version_at: datetime
    version: Version
    accepted: str


@dataclass(frozen=True)
class Reference:
    """差分の検証の基準（前の版、または版が無いときのイメージの TOML）"""

    closed_dates: frozenset[date]
    covered_until: date
    listed_until: date
    rows: dict[date, DayRow] | None  # 確定/予定を比べられる行（TOML には無い）
    check_past: bool  # 過去日の変化を見る（TOML は過去日を書かないので見ない）


@dataclass(frozen=True)
class Problem:
    rule: str
    detail: str
    overridable: bool  # 前の版との差分の検証（--accept で上書きできる）


# ---- 解析 ----


def _text(el) -> str:
    return re.sub(r"\s+", " ", el.text_content().replace("\xa0", " ")).strip()


def _one(nodes, what: str):
    if len(nodes) != 1:
        raise CalendarParseError(f"{what} が {len(nodes)} 個（1 個のはず）")
    return nodes[0]


def parse_jpx_page(data: bytes) -> JpxPage:
    """JPX「祝日取引」のページを解析する。形が想定と違えば CalendarParseError"""
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as e:
        raise CalendarParseError(f"UTF-8 でない: {e}") from None
    try:
        doc = lxml_html.fromstring(text)
    except Exception as e:  # lxml の解析エラー（空文書など）
        raise CalendarParseError(f"HTML を解析できない: {type(e).__name__}") from None

    h2 = _one(doc.xpath('//h2[normalize-space(.)="祝日取引実施日"]'), "見出し「祝日取引実施日」")
    table = _one(h2.xpath("following::table[1]"), "見出しの後の表")
    intro = h2.xpath("following::p[1]")
    intro_years = tuple(int(y) for y in re.findall(r"(\d{4})年", _text(intro[0]))) if intro else ()

    headers = [_text(th) for th in table.xpath(".//th")]
    if not (
        any(h.startswith("祝日取引の対象日") for h in headers) and "実施有無" in headers and "確定/予定" in headers
    ):
        raise CalendarParseError(f"表の見出しが想定と違う: {headers}")

    notes = _parse_notes(table)
    rows: list[JpxRow] = []
    years: list[int] = []
    year = None
    # 年の欄（rowspan）の後の行はその年。rowspan の値は実際の行数と合わないことがある
    # （2026-10 の 2027 年は 18 で 16 行）ので見ない
    for tr in table.xpath(".//tr"):
        tds = tr.xpath("./td")
        if not tds:
            continue
        cells = [_text(td) for td in tds]
        if len(cells) == 6:
            m = re.fullmatch(r"(\d{4})年", cells[0])
            if not m:
                raise CalendarParseError(f"年の欄が読めない: {cells[0]!r}")
            year = int(m.group(1))
            if year in years:
                raise CalendarParseError(f"{year} 年が 2 回ある")
            years.append(year)
            cells = cells[1:]
        elif len(cells) != 5:
            raise CalendarParseError(f"列の数が {len(cells)}（5 か 6 のはず）: {cells}")
        if year is None:
            raise CalendarParseError(f"年の決まらない行: {cells}")
        rows.append(_parse_row(year, cells, notes))
    if not rows:
        raise CalendarParseError("表に行が無い")
    return JpxPage(tuple(years), intro_years, tuple(rows), _parse_sessions(doc))


def _parse_notes(table) -> dict[int, str]:
    notes = {}
    blocks = table.xpath(
        'following::div[contains(concat(" ", normalize-space(@class), " "), " component-annotation-num ")]'
    )
    if blocks:
        for li in blocks[0].xpath(".//li"):
            m = re.search(r"annotation(\d+)", li.get("class", ""))
            if m:
                notes[int(m.group(1))] = _text(li)
    return notes


def _parse_row(year: int, cells: list[str], notes: dict[int, str]) -> JpxRow:
    day_text, weekday, name, held_text, status_text = cells
    m = re.fullmatch(r"(\d{1,2})月(\d{1,2})日", day_text)
    if not m:
        raise CalendarParseError(f"日付が読めない: {day_text!r}")
    try:
        day = date(year, int(m.group(1)), int(m.group(2)))
    except ValueError:
        raise CalendarParseError(f"日付が不正: {year}年{day_text}") from None
    if weekday != WEEKDAYS[day.weekday()]:
        raise CalendarParseError(f"{day} の曜日が {weekday!r}（{WEEKDAYS[day.weekday()]} のはず）")
    m = re.fullmatch(r"実施(する|しない)\s*(?:（注([\d、,]+)）)?", held_text)
    if not m:
        raise CalendarParseError(f"{day} の実施有無が読めない: {held_text!r}")
    held = m.group(1) == "する"
    refs = [int(n) for n in re.split(r"[、,]", m.group(2))] if m.group(2) else []
    texts = [notes[n] for n in refs if n in notes]
    if status_text not in STATUS_TEXT:
        raise CalendarParseError(f"{day} の確定/予定が読めない: {status_text!r}")
    name = re.sub(r"\s*（注[\d、,]+）$", "", name).strip()
    if not name:
        raise CalendarParseError(f"{day} の名称が空")
    return JpxRow(day, name, held, STATUS_TEXT[status_text], " ".join(texts) or None)


def _parse_sessions(doc) -> dict[str, time] | None:
    """取引時間の表の「立会時間」の行（例 8:45∼15:45 / 17:00∼翌6:00）。読めなければ None"""
    h3 = doc.xpath('//h3[normalize-space(.)="取引時間/取引日"]')
    if len(h3) != 1:
        return None
    table = h3[0].xpath("following::table[1]")
    if not table:
        return None
    for tr in table[0].xpath(".//tr"):
        cells = [_text(td) for td in tr.xpath("./td|./th")]
        if len(cells) == 3 and cells[0] == "立会時間":
            day = re.fullmatch(r"(\d{1,2}):(\d{2})\D+(\d{1,2}):(\d{2})", cells[1])
            night = re.fullmatch(r"(\d{1,2}):(\d{2})\D+翌(\d{1,2}):(\d{2})", cells[2])
            if not (day and night):
                return None
            d, n = [int(x) for x in day.groups()], [int(x) for x in night.groups()]
            try:
                return {
                    "day_open": time(d[0], d[1]),
                    "day_close": time(d[2], d[3]),
                    "night_open": time(n[0], n[1]),
                    "night_close": time(n[2], n[3]),
                }
            except ValueError:
                return None
    return None


def parse_holidays_csv(data: bytes) -> dict[date, str]:
    """内閣府「国民の祝日」CSV（Shift_JIS。UTF-8 にも対応）→ 日付 → 名称"""
    for encoding in ("utf-8-sig", "cp932"):
        try:
            text = data.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    else:
        raise CalendarParseError("祝日 CSV の文字コードが読めない")
    lines = [line for line in text.splitlines() if line.strip()]
    if not lines or not lines[0].startswith("国民の祝日"):
        raise CalendarParseError(f"祝日 CSV の見出しが想定と違う: {lines[0][:40] if lines else ''!r}")
    holidays = {}
    for line in lines[1:]:
        parts = line.split(",", 1)
        m = re.fullmatch(r"(\d{4})/(\d{1,2})/(\d{1,2})", parts[0].strip())
        if not m or len(parts) != 2:
            raise CalendarParseError(f"祝日 CSV の行が読めない: {line[:40]!r}")
        try:
            day = date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            raise CalendarParseError(f"祝日 CSV の日付が不正: {parts[0]!r}") from None
        holidays[day] = parts[1].strip()
    if len(holidays) < 10:
        raise CalendarParseError(f"祝日 CSV の行が少ない（{len(holidays)} 行）")
    return holidays


# ---- 版の組み立て ----


def half_year_end(d: date) -> date:
    return date(d.year, 6, 30) if d.month <= 6 else date(d.year, 12, 31)


def build_version(page: JpxPage, previous: Version | None = None) -> Version:
    """ページの行 + 1/1 の規則 + 前の版の過去年（表の最初の年より前）の行。

    - day_type: 名称に「休業日」を含む年末年始の日は year_end、それ以外は national_holiday（祝日 CSV で検証する）
    - covered_until: 確定の最後の行を含む半期の末日（JPX は 6 月・12 月に半期ごとの確定を公表する。注2）
    """
    rows: dict[date, DayRow] = {}
    for r in page.rows:
        year_end = (r.day.month, r.day.day) in YEAR_END_DAYS and "休業日" in r.name
        rows[r.day] = DayRow(
            r.day, r.held, r.status, YEAR_END if year_end else NATIONAL_HOLIDAY, r.name, r.note, SOURCE_JPX
        )
    for year in page.years:
        jan1 = date(year, 1, 1)
        if jan1.weekday() < 5 and jan1 not in rows:
            rows[jan1] = DayRow(jan1, False, CONFIRMED, NATIONAL_HOLIDAY, "元日", None, SOURCE_RULE)
    if previous is not None:
        first = min(page.years)
        for r in previous.rows:
            if r.calendar_date.year < first and r.calendar_date not in rows:
                rows[r.calendar_date] = r
    confirmed = [r.day for r in page.rows if r.status == CONFIRMED]
    if not confirmed:
        raise CalendarParseError("確定の行が無い")
    return Version(
        rows=tuple(rows[d] for d in sorted(rows)),
        covered_until=half_year_end(max(confirmed)),
        listed_until=date(max(page.years), 12, 31),
    )


def reference_from_version(version: Version) -> Reference:
    return Reference(version.closed_dates, version.covered_until, version.listed_until, version.by_date, True)


def reference_from_calendar(closed_dates, covered_until: date) -> Reference:
    """BigQuery に版が無いとき（初回）の基準: イメージの TOML。掲載範囲は書いてある最後の休業日の年末まで"""
    closed = frozenset(closed_dates)
    listed = date(max(closed).year, 12, 31) if closed else covered_until
    return Reference(closed, covered_until, max(listed, covered_until), None, False)


# ---- 検証 ----


def validate(
    version: Version,
    page: JpxPage,
    holidays: dict[date, str],
    reference: Reference | None,
    today: date,
) -> list[Problem]:
    """問題の一覧（空なら受け入れてよい）。overridable=False は --accept でも受け入れない"""
    problems: list[Problem] = []

    def fixed(rule, detail):
        problems.append(Problem(rule, detail, False))

    def diff(rule, detail):
        problems.append(Problem(rule, detail, True))

    years = set(page.years)
    if set(page.intro_years) != years:
        fixed("years_mismatch", f"表の前の文の年 {sorted(set(page.intro_years))} と表の年 {sorted(years)} が違う")
    if today.year not in years:
        # 12 月は翌年・翌々年の表に切り替わっていてもよい（今年の残りの行は前の版から引き継ぐ）
        if not (today.month == 12 and today.year + 1 in years):
            fixed("current_year_missing", f"表に今年（{today.year}）が無い（表の年 {sorted(years)}）")
    counts = Counter(r.day.year for r in page.rows)
    lo, hi = ROWS_PER_YEAR
    for year in sorted(years):
        if not lo <= counts[year] <= hi:
            fixed("row_count", f"{year} 年の行数 {counts[year]}（{lo}〜{hi} のはず）")
    dup = [d for d, n in Counter(r.day for r in page.rows).items() if n > 1]
    if dup:
        fixed("duplicate", f"同じ日付の行: {sorted(dup)}")
    weekend = [r.calendar_date for r in version.rows if r.calendar_date.weekday() >= 5]
    if weekend:
        fixed("weekend", f"土日の行: {weekend}")
    seen_planned = None
    for r in sorted(page.rows, key=lambda r: r.day):
        if r.status == PLANNED and seen_planned is None:
            seen_planned = r.day
        elif r.status == CONFIRMED and seen_planned is not None:
            fixed("status_order", f"予定（{seen_planned}）より後に確定（{r.day}）がある")
            break

    by_date = version.by_date
    for year in sorted(years):
        for month, day in sorted(YEAR_END_DAYS):
            d = date(year, month, day)
            if d.weekday() >= 5:
                continue
            row = by_date.get(d)
            if row is None:
                fixed("year_end_missing", f"年末年始の休業日 {d} が表に無い")
            elif (month, day) in YEAR_END_CLOSED_DAYS and row.is_trading_day:
                fixed("year_end_held", f"{d} が「実施する」（12/31・1/2 は実施しないはず）")
        jan1 = by_date.get(date(year, 1, 1))
        if jan1 is not None and jan1.is_trading_day:
            fixed("jan1_held", f"{jan1.calendar_date} が「実施する」")

    holiday_years = {d.year for d in holidays}
    for r in version.rows:
        if r.source != SOURCE_JPX:
            continue
        if r.day_type == YEAR_END:
            if (r.calendar_date.month, r.calendar_date.day) not in YEAR_END_DAYS:
                fixed("not_year_end", f"{r.calendar_date}（{r.name}）は年末年始の休業日ではない")
        elif r.calendar_date.year in holiday_years and r.calendar_date not in holidays:
            fixed("not_holiday", f"{r.calendar_date}（{r.name}）が祝日 CSV に無い")
    for d in sorted(holidays):
        if d.year in years and d.weekday() < 5 and (d.month, d.day) != (1, 1) and d not in by_date:
            fixed("holiday_missing", f"平日の祝日 {d}（{holidays[d]}）が表に無い")
    for year in sorted(years - holiday_years):
        confirmed = [r.day for r in page.rows if r.day.year == year and r.status == CONFIRMED]
        if confirmed:
            diff("unverified_confirmed", f"祝日 CSV に無い {year} 年に確定の行がある: {confirmed[:3]}")

    if reference is not None:
        problems.extend(_diff_problems(version, reference, today))
    return problems


def _diff_problems(version: Version, ref: Reference, today: date) -> list[Problem]:
    problems = []
    new_closed = version.closed_dates
    old_closed = ref.closed_dates
    changed = new_closed ^ old_closed
    if ref.check_past:
        past = sorted(d for d in changed if d < today)
        if past:
            problems.append(Problem("past_changed", f"過去日の休業/取引日が変わる: {past[:5]}", True))
    if version.covered_until < ref.covered_until:
        problems.append(
            Problem(
                "covered_until_regressed",
                f"covered_until が後退する: {ref.covered_until} → {version.covered_until}",
                True,
            )
        )
    if version.listed_until < ref.listed_until:
        problems.append(
            Problem("listed_until_regressed", f"掲載範囲が縮む: {ref.listed_until} → {version.listed_until}", True)
        )
    if ref.rows is not None:
        regressed = []
        for d, old in sorted(ref.rows.items()):
            new = version.by_date.get(d)
            if d < today or old.status != CONFIRMED:
                continue
            if new is None or new.status != CONFIRMED or new.is_trading_day != old.is_trading_day:
                regressed.append(d)
        if regressed:
            problems.append(
                Problem("confirmed_changed", f"確定の日が消える・予定に戻る・実施有無が変わる: {regressed[:5]}", True)
            )
    future = sorted(d for d in changed if today <= d <= ref.listed_until)
    if len(future) > MAX_FUTURE_CHANGES:
        problems.append(
            Problem(
                "too_many_changes",
                f"今日以降の休業日の変更が {len(future)} 件（上限 {MAX_FUTURE_CHANGES}）: {future[:5]}",
                True,
            )
        )
    soon = sorted(d for d in new_closed - old_closed if today <= d <= today + timedelta(days=CLOSE_GUARD_DAYS))
    if soon:
        problems.append(Problem("closes_soon", f"{CLOSE_GUARD_DAYS} 日以内に新しい休業日: {soon}", True))
    return problems


def closed_diff(version: Version, ref: Reference | None, today: date) -> dict:
    """ログ用: 今日以降の休業日の追加・削除"""
    old = ref.closed_dates if ref is not None else frozenset()
    return {
        "added_closed": sorted(d for d in version.closed_dates - old if d >= today),
        "removed_closed": sorted(d for d in old - version.closed_dates if d >= today),
    }


def sessions_mismatch(page_sessions: dict[str, time] | None, configured: dict[str, time]) -> str | None:
    """ページの取引時間と設定（TOML の [sessions]）の違い。同じなら None"""
    if page_sessions is None:
        return "ページの取引時間の表が読めない"
    diffs = [
        f"{k}: page {page_sessions[k]:%H:%M} / config {configured[k]:%H:%M}"
        for k in SESSION_KEYS
        if page_sessions[k] != configured[k]
    ]
    return "; ".join(diffs) or None


# ---- BigQuery の行との変換 ----


def to_rows(version: Version, version_at: datetime, accepted: str) -> list[dict]:
    if accepted not in ACCEPTED:
        raise ValueError(f"accepted: {accepted!r}")
    content_hash = version.content_hash
    return [
        {
            "version_at": version_at,
            "calendar_date": r.calendar_date,
            "is_trading_day": r.is_trading_day,
            "status": r.status,
            "day_type": r.day_type,
            "name": r.name,
            "note": r.note,
            "source": r.source,
            "covered_until": version.covered_until,
            "listed_until": version.listed_until,
            "content_hash": content_hash,
            "accepted": accepted,
        }
        for r in version.rows
    ]


def from_rows(rows: list[dict]) -> LoadedVersion | None:
    """最新版のビューの行（列名 → Python の値）→ LoadedVersion。行が無ければ None。不正なら CalendarDataError"""
    if not rows:
        return None
    keys = ("version_at", "covered_until", "listed_until", "accepted", "content_hash")
    one = {k: {r[k] for r in rows} for k in keys}
    for key, values in one.items():
        if len(values) != 1:
            raise CalendarDataError(f"{key} が行ごとに違う: {sorted(map(str, values))[:3]}")
    day_rows = []
    for r in rows:
        d = r["calendar_date"]
        if not isinstance(d, date) or d.weekday() >= 5:
            raise CalendarDataError(f"平日でない行: {d}")
        if r["status"] not in STATUSES or r["day_type"] not in DAY_TYPES or r["source"] not in SOURCES:
            raise CalendarDataError(f"{d} の値が不正: {r['status']}/{r['day_type']}/{r['source']}")
        if not isinstance(r["is_trading_day"], bool) or not r["name"]:
            raise CalendarDataError(f"{d} の is_trading_day・name が不正")
        day_rows.append(DayRow(d, r["is_trading_day"], r["status"], r["day_type"], r["name"], r["note"], r["source"]))
    dates = [r.calendar_date for r in day_rows]
    if len(set(dates)) != len(dates):
        raise CalendarDataError("同じ日付の行がある")
    accepted = one["accepted"].pop()
    if accepted not in ACCEPTED:
        raise CalendarDataError(f"accepted が不正: {accepted!r}")
    version = Version(
        tuple(sorted(day_rows, key=lambda r: r.calendar_date)), one["covered_until"].pop(), one["listed_until"].pop()
    )
    if version.covered_until > version.listed_until:
        raise CalendarDataError(f"covered_until {version.covered_until} > listed_until {version.listed_until}")
    # 書いたときの内容のハッシュと照合する（同じ version_at の重複・行の欠け・手での書き換えを検知）
    stored = one["content_hash"].pop()
    if stored != version.content_hash:
        raise CalendarDataError(
            f"content_hash が合わない（保存 {str(stored)[:12]}…、再計算 {version.content_hash[:12]}…）"
        )
    return LoadedVersion(one["version_at"].pop(), version, accepted)


def ingest_sanity(loaded: LoadedVersion, current_closed, today: date) -> list[str]:
    """ingest が版を使う前の最低限の検査（取り込み側の検証をすり抜けた誤りで取引日を取りこぼさないため）。

    - 今日から CLOSE_GUARD_DAYS 日以内に、今使っている値（前の版か TOML）より休業日が増えていない
      （手動で受け入れた版（override）は確認済みなので見ない）
    - 1 年あたりの休業平日が MAX_CLOSED_PER_YEAR 以下
    """
    problems = []
    closed = loaded.version.closed_dates
    if loaded.accepted != ACCEPTED_OVERRIDE:
        horizon = today + timedelta(days=CLOSE_GUARD_DAYS)
        soon = sorted(d for d in closed - frozenset(current_closed) if today <= d <= horizon)
        if soon:
            problems.append(f"closes_soon: {CLOSE_GUARD_DAYS} 日以内に休業日が増える {soon}")
    per_year = Counter(d.year for d in closed)
    over = {y: n for y, n in sorted(per_year.items()) if n > MAX_CLOSED_PER_YEAR}
    if over:
        problems.append(f"too_many_closed: 1 年の休業平日が {MAX_CLOSED_PER_YEAR} を超える {over}")
    return problems


# ---- TOML の生成（config/trading_calendar.toml を最新版に合わせる。PR は workflow calendar-toml が出す（#66）） ----


def render_toml(base_text: str, version: Version, today: date) -> str:
    """base_text（今の TOML）の covered_until と closed_dates だけを版の値に置き換える。コメント・他のキーはそのまま。

    closed_dates は今日以降の休業日（予定を含む）。過去日は書かない（今の運用と同じ）
    """
    lines = []
    for r in version.rows:
        if r.is_trading_day or r.calendar_date < today:
            continue
        status = "確定" if r.status == CONFIRMED else "予定"
        note = "。祝日取引を実施しない" if r.day_type == NATIONAL_HOLIDAY and r.source == SOURCE_JPX else ""
        lines.append(f"  {r.calendar_date.isoformat()}, # {r.name}（{status}{note}）")
    block = "closed_dates = [\n" + "\n".join(lines) + ("\n" if lines else "") + "]"
    text, n1 = re.subn(r"(?m)^covered_until = .*$", f"covered_until = {version.covered_until.isoformat()}", base_text)
    text, n2 = re.subn(r"(?ms)^closed_dates = \[.*?^\]", lambda _: block, text)
    if n1 != 1 or n2 != 1:
        raise ValueError("TOML の covered_until / closed_dates が見つからない")
    data = tomllib.loads(text)  # 生成した TOML が読めること
    if set(data["closed_dates"]) != {d for d in version.closed_dates if d >= today}:
        raise ValueError("生成した TOML の closed_dates が版と合わない")
    return text


@dataclass(frozen=True)
class TomlChange:
    """今の TOML と最新版の違い（今日以降の休業日と covered_until。過ぎた休業日・コメントだけの違いは数えない）"""

    text: str  # 最新版に合わせた TOML（render_toml）
    added: tuple[tuple[date, str], ...]  # (日付, コメント) 今日以降で版にだけある休業日
    removed: tuple[tuple[date, str], ...]  # (日付, コメント) 今日以降で TOML にだけある休業日
    covered_until: tuple[date, date]  # (TOML, 版)

    @property
    def toml_newer(self) -> bool:
        """TOML の covered_until の方が新しい（手で延ばした。ingest は TOML を使う）ので版に合わせない"""
        return self.covered_until[0] > self.covered_until[1]

    @property
    def changed(self) -> bool:
        return bool(self.added or self.removed) or self.covered_until[0] != self.covered_until[1]


_TOML_DATE_LINE = re.compile(r"^\s*(\d{4}-\d{2}-\d{2})\s*,?\s*(?:#\s*(.*?))?\s*$")


def _toml_comments(text: str) -> dict[date, str]:
    """closed_dates の各行のコメント（無ければ空文字）"""
    m = re.search(r"(?ms)^closed_dates = \[(.*?)^\]", text)
    comments = {}
    for line in (m.group(1) if m else "").splitlines():
        hit = _TOML_DATE_LINE.match(line)
        if hit:
            comments[date.fromisoformat(hit.group(1))] = hit.group(2) or ""
    return comments


def toml_change(base_text: str, version: Version, today: date) -> TomlChange:
    """今の TOML（base_text）と版の違い。text は render_toml の結果（changed でなければ使わない）"""
    data = tomllib.loads(base_text)
    old_closed = {d for d in data["closed_dates"] if d >= today}
    new_closed = {d for d in version.closed_dates if d >= today}
    text = render_toml(base_text, version, today)
    old_comments, new_comments = _toml_comments(base_text), _toml_comments(text)
    return TomlChange(
        text=text,
        added=tuple((d, new_comments.get(d, "")) for d in sorted(new_closed - old_closed)),
        removed=tuple((d, old_comments.get(d, "")) for d in sorted(old_closed - new_closed)),
        covered_until=(data["covered_until"], version.covered_until),
    )


def toml_pr_text(change: TomlChange, version_at: datetime, today: date) -> tuple[str, str]:
    """PR のタイトルと本文（Markdown）"""
    old, new = change.covered_until
    parts = []
    if change.added:
        parts.append(f"休業日 +{len(change.added)}")
    if change.removed:
        parts.append(f"休業日 -{len(change.removed)}")
    if old != new:
        parts.append(f"covered_until {old.isoformat()} → {new.isoformat()}")
    title = f"取引カレンダーの TOML を BigQuery の最新版に合わせる（{'、'.join(parts)}）"

    def items(days):
        return [f"  - {d.isoformat()}" + (f"（{c}）" if c else "") for d, c in days] or ["  - なし"]

    body = "\n".join(
        [
            "`config/trading_calendar.toml` の `closed_dates` / `covered_until` を BigQuery の取引カレンダーの最新版"
            f"（`trading_calendar_latest`、version_at {version_at.astimezone(UTC).isoformat()}）に合わせる。",
            "workflow `calendar-toml`（#66）が `python -m optionchan_ingest.calendar_sync --update-toml` で生成した。",
            "",
            f"- covered_until: {old.isoformat()} → {new.isoformat()}" + ("" if old != new else "（変わらない）"),
            "- 追加した休業日:",
            *items(change.added),
            "- 削除した休業日:",
            *items(change.removed),
            "",
            f"{today.isoformat()} より前の休業日は TOML に書かないので、過ぎた日の削除は差分に数えない。"
            "コメント（確定/予定・名称）は版の値で書き直す。",
            "",
            "確認: JPX「祝日取引」のページ（https://www.jpx.co.jp/derivatives/rules/holidaytrading/index.html ）と"
            "休業日が合うこと。マージすると ingest のイメージが作り直され、deploy の承認が要る"
            "（docs/runbook.md「取引カレンダー（#63）」の「TOML」）。",
        ]
    )
    return title, body + "\n"

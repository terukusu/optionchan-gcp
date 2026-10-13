"""テスト用の JPX「祝日取引」ページ（祝日取引実施日の表・注記・取引時間の表）の組み立て。

形は tests/fixtures/holiday_trading.html（2026-10-14 の実ページの抜粋）と同じ。
行は (日付, 名称, 実施する, 確定, 注記番号)
"""

from datetime import date

from optionchan_ingest.jpx_calendar import WEEKDAYS

NOTES = {
    4: "1月2日と12月31日は当分の間、祝日取引を実施しません。",
    6: "BCPテストのため、祝日取引を実施しません。",
}


def row(day: date, name: str, held: bool = True, confirmed: bool = True, note: int | None = None):
    return (day, name, held, confirmed, note)


def page_html(
    rows: list,
    intro_years: list[int] | None = None,
    sessions: tuple[str, str] = ("8:45&sim;15:45", "17:00&sim;翌6:00"),
    headers: tuple[str, str, str] = ("祝日取引の対象日（注1、2、3）", "実施有無", "確定/予定"),
    weekday_override: dict[date, str] | None = None,
) -> bytes:
    years = sorted({r[0].year for r in rows})
    intro = "及び".join(f"{y}年" for y in (intro_years if intro_years is not None else years))
    trs = []
    for year in years:
        year_rows = sorted((r for r in rows if r[0].year == year), key=lambda r: r[0])
        for i, (day, name, held, confirmed, note) in enumerate(year_rows):
            weekday = (weekday_override or {}).get(day, WEEKDAYS[day.weekday()])
            held_text = "実施する" if held else "実施しない"
            if note:
                held_text += f"（注{note}）"
            cells = [
                f"{day.month}月{day.day}日",
                weekday,
                name,
                held_text,
                "確定" if confirmed else "予定",
            ]
            tds = "".join(f'\n    <td class="a-center a-middle">{c}</td>' for c in cells)
            if i == 0:
                tds = f'\n    <td class="a-center a-middle" rowspan="{len(year_rows)}">{year}年<br> </td>' + tds
            trs.append(f"  <tr>{tds}\n  </tr>")
    notes = "\n".join(f'      <li class="annotation{n:02d}">{text}</li>' for n, text in sorted(NOTES.items()))
    day_s, night_s = sessions
    html = f"""<!DOCTYPE html>
<html lang="ja"><head><meta charset="UTF-8"><title>祝日取引</title></head><body>
<div><h2 class="heading-title" id="heading_2"><span>祝日取引実施日</span></h2></div>
<div><p class="component-text">{intro}における祝日取引実施日は以下のとおりです。</p></div>
<div><p class="component-text"><div class="component-normal-table">
<table class="overtable">
  <tr>
    <th colspan="4">{headers[0]}</th>
    <th>{headers[1]}</th>
    <th>{headers[2]}</th>
  </tr>
{chr(10).join(trs)}
</table>
</div></p></div>
<div><div class="component-annotation-num"><ul>
{notes}
</ul></div></div>
<div><h3 class="subhead-title" id="heading_15"><span>取引時間/取引日</span></h3></div>
<div class="component-normal-table"><table><caption></caption>
<tr class="first"><td>&nbsp;</td><td>デイ・セッション</td><td>ナイト・セッション</td></tr>
<tr class="end"><td>立会時間</td><td>{day_s}</td><td>{night_s}</td></tr>
</table></div>
</body></html>
"""
    return html.encode("utf-8")


def standard_rows(holidays: dict[date, str], years: list[int], confirmed_until: date, closed=()) -> list:
    """holidays（祝日 CSV）から作る標準の表: 平日の祝日（1/1 を除く）は実施する、12/31・1/2 は実施しない（注4）、
    closed の日は実施しない（注6）。confirmed_until までは確定"""
    rows = []
    for year in years:
        days = {}
        for d, name in holidays.items():
            if d.year == year and d.weekday() < 5 and (d.month, d.day) != (1, 1):
                days[d] = name
        for month, day, name in ((1, 2, "年始休業日"), (1, 3, "年始休業日"), (12, 31, "年末休業日")):
            d = date(year, month, day)
            if d.weekday() < 5 and d not in days:
                days[d] = name
        for d, name in days.items():
            note = 4 if (d.month, d.day) in ((1, 2), (12, 31)) else (6 if d in closed else None)
            rows.append(row(d, name, held=note is None and d not in closed, confirmed=d <= confirmed_until, note=note))
    return rows

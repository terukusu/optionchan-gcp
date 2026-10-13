"""テスト用の「祝日取引実施日（確定）一覧」（xlsx）の組み立て。

形は tests/fixtures/holiday_trading_days.xlsx（2026-10-14 取得の実物から、シートと共有文字列だけを残して 2026 年末までに
絞ったもの）と同じ: 1 枚のシート、1 行目が見出し、A 列は日付（Excel のシリアル値）、B・C 列は共有文字列。
行は (日付, 名称, 実施する)
"""

import io
import zipfile
from datetime import date
from xml.sax.saxutils import escape

M = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
R = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
P = "http://schemas.openxmlformats.org/package/2006/relationships"
HEADERS = ("祝日取引の対象日", "名称", "実施有無")


def read_rows(data: bytes) -> list[tuple[date, str, bool]]:
    """fixture の行（parse_holiday_trading_xlsx を通す。テストで変えて作り直すため）"""
    from optionchan_ingest.jpx_calendar import parse_holiday_trading_xlsx

    return [(h.day, h.name, h.held) for h in parse_holiday_trading_xlsx(data)]


def xlsx_bytes(rows, headers=HEADERS, held_text=None, sheets=1) -> bytes:
    strings: list[str] = []

    def s(text: str) -> int:
        if text not in strings:
            strings.append(text)
        return strings.index(text)

    def cell(ref: str, text: str) -> str:
        return f'<c r="{ref}" t="s"><v>{s(text)}</v></c>'

    xml_rows = [f'<row r="1">{"".join(cell(f"{c}1", h) for c, h in zip("ABC", headers, strict=True))}</row>']
    for i, (day, name, held) in enumerate(rows, start=2):
        serial = (day - date(1899, 12, 30)).days
        text = (held_text or {}).get(day) or ("実施する" if held else "実施しない")
        xml_rows.append(
            f'<row r="{i}"><c r="A{i}" s="2"><v>{serial}</v></c>{cell(f"B{i}", name)}{cell(f"C{i}", text)}</row>'
        )
    sheet = f'<worksheet xmlns="{M}"><sheetData>{"".join(xml_rows)}</sheetData></worksheet>'
    # ふりがな（rPh）は値に含めない
    sst = "".join(f'<si><t>{escape(t)}</t><rPh sb="0" eb="1"><t>フリガナ</t></rPh></si>' for t in strings)
    sheet_entries = "".join(f'<sheet name="s{n}" sheetId="{n}" r:id="rId{n}"/>' for n in range(1, sheets + 1))
    parts = {
        "_rels/.rels": f'<Relationships xmlns="{P}"><Relationship Id="rId1" Type="{R}/officeDocument" '
        'Target="xl/workbook.xml"/></Relationships>',
        "xl/workbook.xml": f'<workbook xmlns="{M}" xmlns:r="{R}"><sheets>{sheet_entries}</sheets></workbook>',
        "xl/_rels/workbook.xml.rels": f'<Relationships xmlns="{P}">'
        + "".join(
            f'<Relationship Id="rId{n}" Type="{R}/worksheet" Target="worksheets/sheet1.xml"/>'
            for n in range(1, sheets + 1)
        )
        + f'<Relationship Id="rId99" Type="{R}/sharedStrings" Target="sharedStrings.xml"/></Relationships>',
        "xl/worksheets/sheet1.xml": sheet,
        "xl/sharedStrings.xml": f'<sst xmlns="{M}">{sst}</sst>',
    }
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for name, text in parts.items():
            z.writestr(name, text.encode("utf-8"))
    return buf.getvalue()

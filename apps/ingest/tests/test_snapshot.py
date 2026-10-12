import gzip
from datetime import UTC, date, datetime, timedelta

import pytest
from conftest import read_fixture
from htmlgen import page_html

from optionchan_ingest import settings
from optionchan_ingest.bq_proto import load_schema
from optionchan_ingest.calendar import DAY, NIGHT, Session
from optionchan_ingest.parser import jst, parse_page
from optionchan_ingest.snapshot import (
    FetchedPage,
    RunMeta,
    build_rows,
    decode_raw,
    encode_raw,
    is_stale,
    raw_object_name,
    validate_rows,
)

RUN = RunMeta("00000000-0000-4000-8000-000000000000", "ingest-1", "gs://b/raw/x.json.gz#1")
STRIKES = tuple(range(38000, 43000, 500))  # 10 個


def pages(p1_kwargs=None, p2_kwargs=None):
    p1 = parse_page(page_html(**{"strikes": STRIKES, "atm": 40000, **(p1_kwargs or {})}))
    defaults = {"updated_at": "2026/10/12 10:45", "last_trading_day": "2026/12/10", "strikes": STRIKES, "atm": 40000}
    p2 = parse_page(page_html(**{**defaults, **(p2_kwargs or {})}))
    return p1, p2


# --- 生 HTML の名前と封筒（AC 9） ---


def test_raw_object_name_is_jst():
    # UTC 2026-10-12 23:46 = JST 2026-10-13 08:46
    assert raw_object_name(datetime(2026, 10, 12, 23, 46, tzinfo=UTC)) == "raw/2026/10/13/20261013T0846.json.gz"
    assert raw_object_name(jst(2027, 1, 4, 8, 46)) == "raw/2027/01/04/20270104T0846.json.gz"


def test_raw_object_name_rejects_naive():
    with pytest.raises(ValueError):
        raw_object_name(datetime(2026, 10, 12, 10, 46))


def fetched():
    return [
        FetchedPage(
            1,
            settings.JPX_URLS[1],
            datetime(2026, 10, 12, 1, 47, 3, 123456, tzinfo=UTC),
            200,
            read_fixture("page1.html").encode("utf-8"),
            jst(2026, 10, 12, 10, 46),
        ),
        FetchedPage(
            2, settings.JPX_URLS[2], datetime(2026, 10, 12, 1, 47, 4, tzinfo=UTC), 200, b"\xff\xfe not utf-8 \x80", None
        ),
    ]


def test_encode_decode_roundtrip():
    original = fetched()
    data = encode_raw(original)
    decoded = decode_raw(data)
    assert decoded == original
    assert decoded[0].html == original[0].html  # バイト列として一致
    assert decoded[1].html == original[1].html  # UTF-8 でない本文は html_b64 で往復


def test_encode_is_deterministic_gzip():
    assert encode_raw(fetched()) == encode_raw(fetched())
    data = encode_raw(fetched())
    assert data[:2] == b"\x1f\x8b"
    assert data[4:8] == b"\x00\x00\x00\x00"  # mtime 0


def test_decode_rejects_unknown_format():
    with pytest.raises(ValueError):
        decode_raw(gzip.compress(b'{"format": 2, "pages": []}'))


# --- 行の組み立て ---


def test_build_rows():
    p1, p2 = pages()
    rows = build_rows(p1, p2, RUN)
    assert set(rows) == {"spot_price", "future_price", "option_price"}
    assert len(rows["spot_price"]) == 1
    assert len(rows["future_price"]) == 1
    assert len(rows["option_price"]) == 4 * len(STRIKES)
    # 1 限月 → 2 限月の順。created_at は全行 1 限月の更新時刻、page_updated_at は各ページ
    opts = rows["option_price"]
    assert [r["last_trading_day"] for r in opts] == [date(2026, 11, 12)] * 20 + [date(2026, 12, 10)] * 20
    assert {r["created_at"] for t in rows.values() for r in t} == {jst(2026, 10, 12, 10, 46)}
    assert {r["page_updated_at"] for r in opts[20:]} == {jst(2026, 10, 12, 10, 45)}
    assert {r["type"] for r in opts} == {1, 2}
    for table in rows.values():
        for r in table:
            assert (r["ingest_run_id"], r["parser_version"], r["source_object"]) == (
                RUN.ingest_run_id,
                RUN.parser_version,
                RUN.source_object,
            )


@pytest.mark.parametrize("table", settings.DATA_TABLES)
def test_build_rows_columns_match_schema(table):
    p1, p2 = pages()
    schema = load_schema(settings.DEFAULT_SCHEMA_DIR, table)
    names = {c["name"] for c in schema}
    for row in build_rows(p1, p2, RUN)[table]:
        assert set(row) == names


# --- 構造の検査（AC 6） ---


def test_validate_ok():
    p1, p2 = pages()
    assert validate_rows(build_rows(p1, p2, RUN)) == []


def test_validate_ok_fixtures_with_low_min():
    p1 = parse_page(read_fixture("page1.html"))
    p2 = parse_page(read_fixture("page2.html"))
    assert validate_rows(build_rows(p1, p2, RUN), min_strikes=2) == []


def test_validate_one_month():
    p1, _ = pages()
    errors = validate_rows(build_rows(p1, p1, RUN))
    assert any("限月" in e and "1 個" in e for e in errors)
    assert any("同じキー" in e for e in errors)


def test_validate_call_put_mismatch():
    p1, p2 = pages()
    rows = build_rows(p1, p2, RUN)
    rows["option_price"] = [r for r in rows["option_price"] if not (r["type"] == 2 and r["target_price"] == 38000)]
    assert any("call" in e and "put" in e and "違う" in e for e in validate_rows(rows))


@pytest.mark.parametrize("atm", [None, "two"])
def test_validate_atm(atm):
    if atm is None:
        p1, p2 = pages({"atm": 1})  # どの行使価格も ATM でない
    else:
        p1, p2 = pages()
    rows = build_rows(p1, p2, RUN)
    if atm == "two":
        for r in rows["option_price"]:
            if r["target_price"] == 38000:
                r["is_atm"] = True
    errors = validate_rows(rows)
    expected = "0 行" if atm is None else "2 行"
    assert any("ATM" in e and expected in e for e in errors)


def test_validate_min_strikes():
    p1, p2 = pages({"strikes": STRIKES[:9], "atm": 40000})
    errors = validate_rows(build_rows(p1, p2, RUN))
    assert any("行使価格が 9 個" in e for e in errors)


def test_validate_created_at():
    p1, p2 = pages()
    rows = build_rows(p1, p2, RUN)
    rows["spot_price"][0]["created_at"] = jst(2026, 10, 12, 10, 47)
    assert any("created_at" in e for e in validate_rows(rows))


def test_validate_duplicate_key():
    p1, p2 = pages()
    rows = build_rows(p1, p2, RUN)
    rows["option_price"].append(dict(rows["option_price"][0], is_atm=False))
    assert any("同じキー" in e for e in validate_rows(rows))


# --- 鮮度（AC 8） ---

DAY_SESSION = Session(DAY, date(2026, 10, 14), jst(2026, 10, 14, 8, 45), jst(2026, 10, 14, 15, 45))
NIGHT_SESSION = Session(NIGHT, date(2026, 10, 15), jst(2026, 10, 14, 17, 0), jst(2026, 10, 15, 6, 0))


def test_stale_day():
    page = jst(2026, 10, 14, 11, 0)
    assert is_stale(page + timedelta(minutes=31), page, DAY_SESSION)
    assert not is_stale(page + timedelta(minutes=29), page, DAY_SESSION)
    assert not is_stale(page + timedelta(minutes=30), page, DAY_SESSION)


def test_stale_night():
    page = jst(2026, 10, 14, 23, 0)
    assert not is_stale(page + timedelta(minutes=39), page, NIGHT_SESSION)
    assert is_stale(page + timedelta(minutes=41), page, NIGHT_SESSION)


def test_stale_counts_from_session_start():
    # 開始直後はページが前のセッションの時刻のまま。開始時刻起点で判定する
    previous = jst(2026, 10, 14, 6, 0)
    assert not is_stale(jst(2026, 10, 14, 9, 10), previous, DAY_SESSION)
    assert is_stale(jst(2026, 10, 14, 9, 16), previous, DAY_SESSION)


def test_stale_uses_session_end_after_close():
    # 立会終了後（余裕の時間帯）は終了時刻を基準にする: 最後の更新 15:40、16:15 でも stale にしない
    assert not is_stale(jst(2026, 10, 14, 16, 15), jst(2026, 10, 14, 15, 40), DAY_SESSION)
    assert not is_stale(jst(2026, 10, 15, 6, 30), jst(2026, 10, 15, 5, 50), NIGHT_SESSION)
    # 終了前から止まっていたら（15:10 で止まる）終了後も stale
    assert is_stale(jst(2026, 10, 14, 16, 0), jst(2026, 10, 14, 15, 10), DAY_SESSION)

"""スナップショットの組み立て（純粋関数）: 生 HTML の名前と封筒、行の組み立てと構造の検査、鮮度判定。"""

import base64
import gzip
import json
from collections import Counter
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta

from . import settings
from .calendar import DAY, Session
from .parser import JST, ParsedPage


@dataclass(frozen=True)
class FetchedPage:
    page: int  # 1 = 1 限月、2 = 2 限月
    url: str
    fetched_at: datetime  # 取得した実時刻（aware）
    http_status: int
    html: bytes
    updated_at: datetime | None = None  # ページの最終更新時刻（JST aware）


@dataclass(frozen=True)
class RunMeta:
    ingest_run_id: str
    parser_version: str
    source_object: str  # gs://<bucket>/<name>#<generation>


def _require_aware(dt: datetime, name: str) -> None:
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")


def raw_object_name(updated_at: datetime) -> str:
    """生 HTML のオブジェクト名（1 限月ページの更新時刻を JST にした raw/YYYY/MM/DD/YYYYMMDDTHHMM.json.gz）"""
    _require_aware(updated_at, "updated_at")
    return updated_at.astimezone(JST).strftime(settings.RAW_OBJECT_FORMAT)


def source_object(bucket: str, name: str, generation: int) -> str:
    return f"gs://{bucket}/{name}#{generation}"


def _iso(dt: datetime | None) -> str | None:
    return None if dt is None else dt.isoformat()


def encode_raw(pages: list[FetchedPage]) -> bytes:
    """2 ページとメタデータを gzip した JSON にする（mtime 0 固定で、同じ入力は同じバイト列）"""
    out = []
    for p in pages:
        _require_aware(p.fetched_at, "fetched_at")
        item = {
            "page": p.page,
            "url": p.url,
            "fetched_at": _iso(p.fetched_at),
            "http_status": p.http_status,
            "updated_at": _iso(p.updated_at),
        }
        try:
            item["html"] = p.html.decode("utf-8")
        except UnicodeDecodeError:
            item["html_b64"] = base64.b64encode(p.html).decode("ascii")
        out.append(item)
    body = json.dumps({"format": settings.RAW_ENVELOPE_FORMAT, "pages": out}, ensure_ascii=False)
    return gzip.compress(body.encode("utf-8"), mtime=0)


def decode_raw(data: bytes) -> list[FetchedPage]:
    doc = json.loads(gzip.decompress(data).decode("utf-8"))
    if doc.get("format") != settings.RAW_ENVELOPE_FORMAT:
        raise ValueError(f"unsupported raw format: {doc.get('format')!r}")
    pages = []
    for item in doc["pages"]:
        html = item["html"].encode("utf-8") if "html" in item else base64.b64decode(item["html_b64"])
        updated_at = item.get("updated_at")
        pages.append(
            FetchedPage(
                page=item["page"],
                url=item["url"],
                fetched_at=datetime.fromisoformat(item["fetched_at"]),
                http_status=item["http_status"],
                html=html,
                updated_at=None if updated_at is None else datetime.fromisoformat(updated_at),
            )
        )
    return pages


def build_rows(p1: ParsedPage, p2: ParsedPage, run: RunMeta) -> dict[str, list[dict]]:
    """2 ページの解析結果から 3 テーブルの行（列名 → Python 値）を作る。created_at は全行 1 限月ページの更新時刻"""
    created_at = p1.updated_at
    meta = {
        "ingest_run_id": run.ingest_run_id,
        "parser_version": run.parser_version,
        "source_object": run.source_object,
    }

    def row(obj, page: ParsedPage) -> dict:
        return {**asdict(obj), "created_at": created_at, "page_updated_at": page.updated_at, **meta}

    return {
        "spot_price": [row(p1.spot, p1)],
        "future_price": [row(p1.future, p1)],
        "option_price": [row(o, p) for p in (p1, p2) for o in p.options],
    }


def validate_rows(rows: dict[str, list[dict]], min_strikes: int = settings.MIN_STRIKES) -> list[str]:
    """行の構造を検査し、違反の説明を返す（空なら OK）。行数の固定下限は使わない（実測で 84〜1,080 行と変動する）"""
    errors = []
    options = rows.get("option_price", [])
    all_rows = [r for table in rows.values() for r in table]

    created = {r["created_at"] for r in all_rows}
    if len(created) != 1:
        errors.append(f"created_at が全行で同じでない（{len(created)} 種類）")

    months = sorted({r["last_trading_day"] for r in options})
    if len(months) != 2:
        errors.append(f"限月（last_trading_day）が {len(months)} 個（2 個のはず）")

    for m in months:
        in_month = [r for r in options if r["last_trading_day"] == m]
        calls = [r for r in in_month if r["type"] == 1]
        puts = [r for r in in_month if r["type"] == 2]
        if len(calls) != len(puts):
            errors.append(f"{m}: call {len(calls)} 行と put {len(puts)} 行が違う")
        for kind, group in (("call", calls), ("put", puts)):
            atm = sum(1 for r in group if r["is_atm"])
            if atm != 1:
                errors.append(f"{m} {kind}: ATM が {atm} 行（1 行のはず）")
        strikes = {r["target_price"] for r in in_month}
        if len(strikes) < min_strikes:
            errors.append(f"{m}: 行使価格が {len(strikes)} 個（下限 {min_strikes}）")

    keys = Counter((r["type"], r["target_price"], r["last_trading_day"]) for r in options)
    dup = sorted(k for k, n in keys.items() if n > 1)
    if dup:
        errors.append(f"同じキー (type, target_price, last_trading_day) が 2 行以上: {dup[:5]}")
    return errors


def is_stale(
    now: datetime,
    page_updated_at: datetime,
    session: Session,
    day_limit: int = settings.STALE_DAY_MINUTES,
    night_limit: int = settings.STALE_NIGHT_MINUTES,
) -> bool:
    """取引時間中に「基準時刻 − max(ページ更新時刻, セッション開始)」がしきい値（分）を超えたら True。

    基準時刻は min(実時刻, セッション終了)。立会の終了後（余裕の時間帯）はページの更新が止まるので、
    実時刻で比べると毎日終了の約 30 分後に誤って stale になる。終了時刻で止めれば、終了前から
    更新が止まっていた場合だけ（終了前の判定で既に）stale になる
    """
    _require_aware(now, "now")
    _require_aware(page_updated_at, "page_updated_at")
    limit = stale_limit(session, day_limit, night_limit)
    return min(now, session.end) - max(page_updated_at, session.start) > limit


def stale_limit(
    session: Session, day_limit: int = settings.STALE_DAY_MINUTES, night_limit: int = settings.STALE_NIGHT_MINUTES
) -> timedelta:
    """鮮度のしきい値（日中 / 夜間）"""
    return timedelta(minutes=day_limit if session.kind == DAY else night_limit)


def unavailable_window(
    now: datetime,
    session: Session,
    day_limit: int = settings.STALE_DAY_MINUTES,
    night_limit: int = settings.STALE_NIGHT_MINUTES,
) -> tuple[datetime, datetime] | None:
    """jpx_unavailable が続いているかの判定（#42）で一覧する生 HTML の名前の時刻の範囲 (start, end)。

    この範囲に保存済みのスナップショットが無ければ、最後の保存は start 以前なので is_stale と同じ規則で stale。
    start = min(now, セッション終了) − しきい値、end = now。セッションの開始からしきい値以内
    （start <= セッション開始）は、最後の保存が無くても stale にならないので None（一覧しない）
    """
    _require_aware(now, "now")
    start = min(now, session.end) - stale_limit(session, day_limit, night_limit)
    if start <= session.start:
        return None
    return start, now


def with_updated_at(page: FetchedPage, updated_at: datetime | None) -> FetchedPage:
    return replace(page, updated_at=updated_at)

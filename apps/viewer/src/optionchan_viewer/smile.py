"""スマイルカーブの曲線の規則（/smile_snapshot、#79）。純粋関数。

行は sql/smile_snapshot.sql の行（month, type, target_price, last_trading_day, is_atm, iv, price, price_time, bid_iv,
ask_iv, bid, ask。type は 1 = call / 2 = put）。
- ATM（限月ごと）: その限月の put で is_atm の行の行使価格の最小（/smile_data の「1 限月 put の is_atm の最初」と
  同じ規則を限月ごとに）。無ければ None
- IV（iv・bid_iv・ask_iv）は 0 以下を欠損（None）にする（気配の IV が 0 の行がある）
- mid_iv = (bid_iv + ask_iv) / 2。正規化した後に両方あるときだけ（片方だけ・0 は None）
- otm = OTM 合成に使う行: put で K < ATM、call で K >= ATM（ATM ちょうどは call）。ATM が無い限月はすべて False
画面（static/smile.js）はこの列で絞って描くだけで、規則を持たない。
"""

CALL, PUT = 1, 2
MONTHS = (1, 2)


def positive_iv(v):
    """IV の 0 以下・NULL は欠損（None）"""
    return v if v is not None and v > 0 else None


def mid_iv(bid_iv, ask_iv):
    bid_iv, ask_iv = positive_iv(bid_iv), positive_iv(ask_iv)
    if bid_iv is None or ask_iv is None:
        return None
    return (bid_iv + ask_iv) / 2


def is_otm(typ: int, strike: int, atm) -> bool:
    if atm is None:
        return False
    return strike < atm if typ == PUT else strike >= atm


def atm_by_month(rows: list[dict]) -> dict:
    """限月（1・2）→ ATM の行使価格（put の is_atm の最小）。無ければ None"""
    out = dict.fromkeys(MONTHS)
    for r in rows:
        if r["type"] == PUT and r["is_atm"] and r["month"] in out:
            m = r["month"]
            out[m] = r["target_price"] if out[m] is None else min(out[m], r["target_price"])
    return out


def snapshot_rows(rows: list[dict]) -> tuple[dict, list[dict]]:
    """(限月 → {"last_trading_day", "atm"}, 出力の行（month, target_price, type の昇順）)"""
    atm = atm_by_month(rows)
    months = {m: {"last_trading_day": None, "atm": atm[m]} for m in MONTHS}
    out = []
    for r in rows:
        m = r["month"]
        if m not in months:
            continue
        months[m]["last_trading_day"] = r["last_trading_day"]
        out.append({
            "month": m,
            "type": r["type"],
            "target_price": r["target_price"],
            "iv": positive_iv(r["iv"]),
            "price": r["price"],
            "price_time": r["price_time"],
            "bid_iv": positive_iv(r["bid_iv"]),
            "ask_iv": positive_iv(r["ask_iv"]),
            "bid": r["bid"],
            "ask": r["ask"],
            "mid_iv": mid_iv(r["bid_iv"], r["ask_iv"]),
            "otm": 1 if is_otm(r["type"], r["target_price"], atm[m]) else 0,
        })
    out.sort(key=lambda r: (r["month"], r["target_price"], r["type"]))
    return months, out

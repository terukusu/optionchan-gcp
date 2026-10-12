"""テスト用の JPX ページの HTML 断片を組み立てる（値は架空）。

JPX のページ構造（class 名・tr/td の並び）のうちパーサーが参照する部分だけを再現する。
ルートの tests/fixtures/jpx/nkopm_minimal.html と同じ形。JPX のページそのものは保存しない。
"""

# 値が無い行（2 段のセルは 2 段とも "-"）
DASH_ROW = ["- <br/>-", "-<br/>-", "-", "- (-)<br/>- (-)", "-<br/>-", "-", "-", "-"]

HEAD = """<!DOCTYPE html>
<!-- テスト用に手書きした最小の HTML 断片（{note}）。値は架空 -->
<html>
<body>
<div class="update-time">
    <dl><dt>最終更新時刻</dt><dd>{updated_at}</dd></dl>
</div>
<div id="priceInfo">
    <table>
        <tr><th>&nbsp;</th><th>現在値</th><th>前日比</th><th>騰落率</th><th>HV</th></tr>
        <tr>
            <td>日経平均株価
(日経225)</td>
            <td class="a-right price-now">{spot_price}</td>
            <td class="a-right">{spot_diff}</td>
            <td class="a-right">{spot_diff_rate}</td>
            <td class="a-right">{spot_hv}</td>
        </tr>
        <tr>
            <td>日経225先物
({future_label})</td>
            <td class="a-right price-now">{future_price}</td>
            <td class="a-right">{future_diff}</td>
            <td class="a-right">{future_diff_rate}</td>
            <td class="a-right">{future_hv}</td>
        </tr>
    </table>
</div>
<div class="date-table last-tradingday">
    <dl><dt>取引最終日</dt><dd>{last_trading_day}</dd></dl>
</div>
<table>
    <thead class="price-info-header">
        <tr><th colspan="8">CALL</th><th rowspan="2">権利行使<br/>価格</th><th colspan="8">PUT</th></tr>
        <tr><th>清算値<br/>{quotation}</th><th>建玉残</th></tr>
    </thead>
    <tbody class="price-info-scroll">
"""

TAIL = """    </tbody>
</table>
</body>
</html>
"""

ROW = """        <tr class="row-num">
{cells}
        </tr>
"""

GREEK = """        <tr class="greek">
            <td colspan="8"><table>
                <tr><th>デルタ</th><th>ガンマ</th><th>セータ</th><th>ベガ</th></tr>
                <tr>{call}</tr>
            </table></td>
            <td>&nbsp;</td>
            <td colspan="8"><table>
                <tr><th>デルタ</th><th>ガンマ</th><th>セータ</th><th>ベガ</th></tr>
                <tr>{put}</tr>
            </table></td>
        </tr>
"""


def option_cells(price, diff, iv, order, order_iv, volume, positions, quotation):
    """call / put 片側 8 列（put の並び。call はこれを逆順に並べる）"""
    return [price, diff, iv, order, order_iv, volume, positions, quotation]


def default_side(strike, kind, price_time="10/12 10:40"):
    base = 1000 if kind == "call" else 1100
    return option_cells(
        f"{base:,} <br/>{price_time}",
        "-10<br/>-0.98%",
        "24.50%",
        f"{base + 20:,} (5)<br/>{base:,} (3)",
        "25.00%<br/>24.00%",
        "30",
        "1,200",
        f"{base + 10:,}",
    )


def strike_cell(strike, atm):
    # 実ページの ATM 表示は「A T M」の間が NBSP（旧パーサーも NBSP で探している）
    atm_text = '<span class="atm">A&nbsp;T&nbsp;M</span>' if atm else ""
    return f"{strike:,}{atm_text} <button>リスク指標</button>"


def page_html(
    updated_at="2026/10/12 10:46",
    last_trading_day="2026/11/12",
    future_label="26年12月限",
    quotation="10/09",
    strikes=(39500, 40000, 40500),
    atm=40000,
    rows=None,
    greeks=None,
    extra_greeks=0,
    spot=("39,876.54<span>(10/09 15:45)</span>", "-1,234.56", "-3.00%", "20.00%"),
    future=("39,870<span>(10/12 10:45)</span>", "-1,230", "-2.99%", "19.50%"),
    note="",
):
    """rows: {strike: (call 8 列, put 8 列)} で既定値を上書きする。greeks: {strike: (call 4 個, put 4 個)}"""
    rows = rows or {}
    greeks = greeks or {}
    out = [
        HEAD.format(
            note=note,
            updated_at=updated_at,
            spot_price=spot[0],
            spot_diff=spot[1],
            spot_diff_rate=spot[2],
            spot_hv=spot[3],
            future_label=future_label,
            future_price=future[0],
            future_diff=future[1],
            future_diff_rate=future[2],
            future_hv=future[3],
            last_trading_day=last_trading_day,
            quotation=quotation,
        )
    ]
    strikes = list(strikes)
    for strike in strikes:
        call, put = rows.get(strike, (default_side(strike, "call"), default_side(strike, "put")))
        cells = list(reversed(call)) + [strike_cell(strike, strike == atm)] + list(put)
        out.append(ROW.format(cells="\n".join(f"            <td>{c}</td>" for c in cells)))
        g_call, g_put = greeks.get(
            strike, (("0.5000", "0.0001", "-10.0000", "40.0000"), ("-0.5000", "0.0001", "-11.0000", "41.0000"))
        )
        out.append(
            GREEK.format(call="".join(f"<td>{v}</td>" for v in g_call), put="".join(f"<td>{v}</td>" for v in g_put))
        )
    for _ in range(extra_greeks):
        out.append(GREEK.format(call="<td>-</td>" * 4, put="<td>-</td>" * 4))
    out.append(TAIL)
    return "".join(out)


def empty_page_html():
    """4 時台の空ページ: 更新時刻が空でオプションの行が無い"""
    return page_html(updated_at="", strikes=(), note="空ページ")

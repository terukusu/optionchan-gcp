"""tests/fixtures/*.html を htmlgen から作り直す（値は架空。JPX のページは保存しない）。

    cd apps/ingest && uv run python tests/fixtures/generate.py

作り直したら legacy/make_golden.py で旧パーサーの golden も作り直す。
"""

import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

from htmlgen import DASH_ROW, empty_page_html, page_html  # noqa: E402

DASH_GREEKS = (("-",) * 4, ("-",) * 4)

FIXTURES = {
    # 1 限月: ATM は 40,000。40,500 は値が全部 '-'
    "page1.html": page_html(
        note="1 限月",
        rows={40500: (DASH_ROW, DASH_ROW)},
        greeks={40500: DASH_GREEKS},
    ),
    # 2 限月: 取引最終日・更新時刻が 1 限月と違う。現物の値が無い
    "page2.html": page_html(
        note="2 限月",
        updated_at="2026/10/12 10:45",
        last_trading_day="2026/12/10",
        strikes=(39000, 40000),
        atm=40000,
        spot=("-", "-", "-", "-"),
    ),
    # 年末年始: 更新時刻 2027-01-04、清算日・約定 12/30
    "yearend.html": page_html(
        note="年末年始",
        updated_at="2027/01/04 08:46",
        last_trading_day="2027/01/08",
        future_label="27年03月限",
        quotation="12/30",
        strikes=(40000, 40500),
        atm=40000,
        spot=("39,000.00<span>(12/30 15:45)</span>", "+100.00", "+0.26%", "18.00%"),
        future=("39,010<span>(12/30 15:45)</span>", "+110", "+0.28%", "17.50%"),
        rows={
            40000: (
                [
                    "900 <br/>12/30 15:45",
                    "-5<br/>-0.55%",
                    "20.00%",
                    "910 (1)<br/>890 (2)",
                    "-<br/>19.00%",
                    "10",
                    "500",
                    "905",
                ],
                [
                    "1,000 <br/>12/30 15:44",
                    "+5<br/>+0.50%",
                    "21.00%",
                    "1,010 (1)<br/>990 (2)",
                    "22.00%<br/>-",
                    "11",
                    "600",
                    "1,005",
                ],
            )
        },
    ),
    # SQ のある月の 1 日〜限月交代: 先物は当月限（旧パーサーは翌年にしてしまう）
    "sq_month.html": page_html(
        note="SQ 月の限月交代前",
        updated_at="2026/12/04 10:00",
        last_trading_day="2026/12/10",
        quotation="12/03",
        future=("39,500<span>(12/04 09:59)</span>", "+10", "+0.03%", "18.00%"),
    ),
    # 4 時台の空ページ
    "empty.html": empty_page_html(),
    # 更新時刻はあるがオプションの行が無い
    "empty_rows.html": page_html(note="オプションの行が無い", strikes=()),
    # 構造違い: row-num 2 行に対しギリシャ指標が 3 行
    "mismatch.html": page_html(note="行数不一致", strikes=(40000, 40500), extra_greeks=1),
}

if __name__ == "__main__":
    for name, html in FIXTURES.items():
        (HERE / name).write_text(html, encoding="utf-8")
        print(name)

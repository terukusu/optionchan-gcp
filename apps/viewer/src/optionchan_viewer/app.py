"""ビューアー（Flask）。ページ 3 つ（/ /smile /atm_iv）と CSV の API 5 つ（/smile_data /smile_snapshot /atm_iv_data
/atm_iv_bars /data_gaps）。

/smile_data・/atm_iv_data の URL・CSV の形式は現行（appengine/ + HTTP 関数）と同じ。/atm_iv_bars は /atm_iv の足の
切り替え・表示範囲に応じた読み込み用（#68・#77。CSV は /atm_iv_data と同じ列。期間は start・end。n=roll（期近つなぎ、
#101）は 6 列目に限月。n=cm（30 日固定の ATM IV、#100）は同じ 5 列）。
/smile_snapshot は /smile の時点の移動用（#79。at 以前で最新のスナップショットの 1 時点。dir=next は at 以後で最初
（#80）。曲線の列は smile.py。1 行目の 6 列目は #114 で作ったスナップショットの印（#123））。
/data_gaps は過去データの欠けの区間（#123。表 data_gaps の全体。/atm_iv がページを開いたときに 1 回読む）。
1 日のクエリ量はプロセスごとに数え、上限を超えたら 429（#95。budget.py）。
認証はアプリでは行わない（Cloud Run の IAP。#13 の 13b）。
"""

from datetime import UTC, datetime

from flask import Flask, Response, render_template, request

from . import api, bq, budget
from . import settings as st
from .log import log

CSV_TYPE = "text/csv; charset=utf-8"
# すべての応答に付けるセキュリティヘッダ。スクリプト・スタイル・画像・XHR は同じオリジンの同梱ファイルだけ
# （CDN をやめ、テンプレートに inline の script / style を置かない）。他のサイトの frame に入れない
SECURITY_HEADERS = {
    "Content-Security-Policy": (
        "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self'; connect-src 'self'; "
        "base-uri 'none'; form-action 'none'; frame-ancestors 'none'"
    ),
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "no-referrer",
    "X-Frame-Options": "DENY",
}


def _now() -> datetime:
    return datetime.now(UTC)


def _guarded(runner, daily):
    """クエリの前に 1 日の上限を見る runner（超えていたら budget.OverBudget。BigQuery を呼ばない）"""

    def run(name, params):
        daily.check()
        return runner(name, params)

    return run


def create_app(runner=None, clock=None, daily=None) -> Flask:
    """runner: (name, params) -> rows。省略時は環境変数の設定で BigQuery を読み、課金されたバイト数を 1 日の上限
    （budget.DailyBudget）に数える。clock: 現在時刻（テスト用）。daily: 1 日の上限（テスト用。runner を渡したときは
    これを渡さなければ上限なし）"""
    app = Flask(__name__)
    clock = clock or _now
    if runner is None:
        cfg = st.from_env()
        daily = daily or budget.DailyBudget(cfg.daily_bytes_billed, clock)
        runner = bq.BigQueryRunner(cfg, on_billed=daily.add)
    if daily is not None:
        runner = _guarded(runner, daily)

    @app.after_request
    def security_headers(res):
        for k, v in SECURITY_HEADERS.items():
            res.headers.setdefault(k, v)
        return res

    def csv_response(endpoint, produce):
        try:
            body = produce()
        except api.BadRequest as e:
            # 入力値は応答・ログに出さない（反射しない）。どのパラメータかだけ
            log("INFO", "bad_request", endpoint=endpoint, param=str(e))
            return Response("bad request\n", status=400, content_type="text/plain; charset=utf-8")
        except budget.OverBudget:
            log("WARNING", "daily_limit", endpoint=endpoint)
            return Response("daily limit\n", status=429, content_type="text/plain; charset=utf-8")
        except api.NoData as e:
            log("WARNING", str(e), endpoint=endpoint)
            return Response("no data\n", status=503, content_type="text/plain; charset=utf-8")
        except Exception as e:
            # BigQuery の例外の本文はプロジェクト番号・アカウントを含みうるので、応答には出さず、ログは型だけ
            log("ERROR", "query_failed", endpoint=endpoint, error_type=type(e).__name__)
            return Response("internal error\n", status=500, content_type="text/plain; charset=utf-8")
        headers = {}
        if isinstance(body, tuple):
            body, headers = body
        res = Response(body, status=200, content_type=CSV_TYPE)
        res.headers["Cache-Control"] = "no-cache"
        res.headers.update(headers)
        return res

    @app.get("/")
    def index():
        return render_template("index.html")

    @app.get("/favicon.ico")
    def favicon_ico():
        # ページは <link rel="icon"> で /static/favicon.svg を指す。<link> を読む前・読まないブラウザの要求に
        # 404 を出さない（#94）
        return Response(status=204)

    @app.get("/smile")
    def smile():
        return render_template("smile.html")

    @app.get("/atm_iv")
    def atm_iv():
        return render_template("atm_iv.html")

    @app.get("/smile_data")
    def smile_data():
        return csv_response("smile_data", lambda: api.smile_data(runner, clock()))

    @app.get("/smile_snapshot")
    def smile_snapshot():
        def produce():
            # 検証を先に（不正なら BigQuery を呼ばない）
            at = api.parse_at(request.args.get("at"), clock())
            nxt = api.parse_dir(request.args.get("dir"))
            return api.smile_snapshot(runner, at, nxt)

        return csv_response("smile_snapshot", produce)

    @app.get("/atm_iv_data")
    def atm_iv_data():
        def produce():
            # 検証を先に（不正なら BigQuery を呼ばない）
            d = api.parse_d(request.args.get("d"))
            n = api.parse_n(request.args.get("n"))
            return api.atm_iv_data(runner, clock(), d, n)

        return csv_response("atm_iv_data", produce)

    @app.get("/atm_iv_bars")
    def atm_iv_bars():
        # 読んだ範囲（UTC の UNIX 秒）を応答のヘッダで返す。画面はこれで取得済みの範囲を覚え、
        # 表示範囲の外だけを取りに来る（#77）
        def produce():
            # 検証を先に（tf → n → iv → start・end。不正なら BigQuery を呼ばない）。n は roll（期近つなぎ、#101）・
            # cm（30 日固定、#100。iv は検証だけして使わない）も
            tf = api.parse_tf(request.args.get("tf"))
            n = api.parse_bars_n(request.args.get("n"))
            iv = api.parse_iv(request.args.get("iv"))
            now = clock()
            rng = api.parse_range(request.args.get("start"), request.args.get("end"), tf, now)
            body, start, end = api.atm_iv_bars(runner, now, tf, n, rng, iv)
            return body, {"X-Bars-Start": str(int(start.timestamp())), "X-Bars-End": str(int(end.timestamp()))}

        return csv_response("atm_iv_bars", produce)

    @app.get("/data_gaps")
    def data_gaps():
        # パラメータを読まない（表全体。数百行）
        return csv_response("data_gaps", lambda: api.data_gaps(runner))

    return app


def __getattr__(name):
    # gunicorn の optionchan_viewer.app:app。import しただけでは設定・BigQuery に触れない（テストのため）
    if name == "app":
        global app
        app = create_app()
        return app
    raise AttributeError(name)

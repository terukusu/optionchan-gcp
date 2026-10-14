"""ビューアー（Flask）。ページ 3 つ（/ /smile /atm_iv）と CSV の API 4 つ（/smile_data /smile_snapshot /atm_iv_data
/atm_iv_bars）。

/smile_data・/atm_iv_data の URL・CSV の形式は現行（appengine/ + HTTP 関数）と同じ。/atm_iv_bars は /atm_iv の足の
切り替え・表示範囲に応じた読み込み用（#68・#77。CSV は /atm_iv_data と同じ列。期間は start・end）。
/smile_snapshot は /smile の時点の移動用（#79。at 以前で最新のスナップショットの 1 時点。曲線の列は smile.py）。
認証はアプリでは行わない（Cloud Run の IAP。#13 の 13b）。
"""

from datetime import UTC, datetime

from flask import Flask, Response, render_template, request

from . import api, bq
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


def create_app(runner=None, clock=None) -> Flask:
    """runner: (name, params) -> rows。省略時は環境変数の設定で BigQuery を読む。clock: 現在時刻（テスト用）"""
    app = Flask(__name__)
    if runner is None:
        runner = bq.BigQueryRunner(st.from_env())
    clock = clock or _now

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
            return api.smile_snapshot(runner, at)

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
            # 検証を先に（tf → n → start・end。不正なら BigQuery を呼ばない）
            tf = api.parse_tf(request.args.get("tf"))
            n = api.parse_n(request.args.get("n"))
            now = clock()
            rng = api.parse_range(request.args.get("start"), request.args.get("end"), tf, now)
            body, start, end = api.atm_iv_bars(runner, now, tf, n, rng)
            return body, {"X-Bars-Start": str(int(start.timestamp())), "X-Bars-End": str(int(end.timestamp()))}

        return csv_response("atm_iv_bars", produce)

    return app


def __getattr__(name):
    # gunicorn の optionchan_viewer.app:app。import しただけでは設定・BigQuery に触れない（テストのため）
    if name == "app":
        global app
        app = create_app()
        return app
    raise AttributeError(name)

"""ビューアー（Flask）。ページ 4 つ（/ /smile /atm /atm_iv）と CSV の API 3 つ（/smile_data /atm_data /atm_iv_data）。

URL・CSV の形式は現行（appengine/ + HTTP 関数）と同じ。認証はアプリでは行わない（Cloud Run の IAP。#13 の 13b）。
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
        res = Response(body, status=200, content_type=CSV_TYPE)
        res.headers["Cache-Control"] = "no-cache"
        return res

    @app.get("/")
    def index():
        return render_template("index.html")

    @app.get("/smile")
    def smile():
        return render_template("smile.html")

    @app.get("/atm")
    def atm():
        return render_template("atm.html")

    @app.get("/atm_iv")
    def atm_iv():
        return render_template("atm_iv.html")

    @app.get("/smile_data")
    def smile_data():
        return csv_response("smile_data", lambda: api.smile_data(runner, clock()))

    @app.get("/atm_data")
    def atm_data():
        return csv_response("atm_data", lambda: api.atm_data(runner, clock()))

    @app.get("/atm_iv_data")
    def atm_iv_data():
        def produce():
            # 検証を先に（不正なら BigQuery を呼ばない）
            d = api.parse_d(request.args.get("d"))
            n = api.parse_n(request.args.get("n"))
            return api.atm_iv_data(runner, clock(), d, n)

        return csv_response("atm_iv_data", produce)

    return app


def __getattr__(name):
    # gunicorn の optionchan_viewer.app:app。import しただけでは設定・BigQuery に触れない（テストのため）
    if name == "app":
        global app
        app = create_app()
        return app
    raise AttributeError(name)

"""旧パーサー（functions/jpx_loader.parse_jpx_html）の出力を golden として保存する。

ルートの Python 3.11 の venv（docs/development.md の「テスト」）で、リポジトリ直下から実行する:

    .venv/bin/python apps/ingest/tests/fixtures/legacy/make_golden.py

出力は旧系が GCS に書く JSON Lines と同じ値（to_json）。
1 行目が spot、2 行目が future、残りが option（全 call → 全 put）。
旧パーサーは清算日の年を datetime.now().year で決めるので、生成時の「今」を固定する。
yearend_now2029.jsonl は「今」を 2029 年にした出力で、旧パーサーが年をまたぐ再処理で清算日を誤る例として残す。
"""

import json
import os
import sys
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.normpath(os.path.join(HERE, "..", "..", "..", "..", ".."))
sys.path.insert(0, os.path.join(REPO, "functions"))

import jpx_loader  # noqa: E402

FIXTURES = ["page1.html", "page2.html", "yearend.html", "sq_month.html"]


def fixed_now(year, month, day):
    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(year, month, day)

    return FixedDatetime


def dump(fixture, out_name, now):
    jpx_loader.datetime = fixed_now(*now)
    with open(os.path.join(HERE, "..", fixture), encoding="utf-8") as f:
        info = jpx_loader.parse_jpx_html(f.read())
    lines = [info.spot_price.to_json(), info.future_price.to_json()]
    lines += [o.to_json() for o in info.call_option_list + info.put_option_list]
    with open(os.path.join(HERE, out_name), "w", encoding="utf-8") as f:
        for line in lines:
            f.write(json.dumps(json.loads(line), ensure_ascii=False, sort_keys=True) + "\n")
    print(out_name, len(lines))


if __name__ == "__main__":
    for fixture in FIXTURES:
        dump(fixture, fixture.replace(".html", ".jsonl"), (2026, 10, 12))
    # 年末年始の断片の更新時刻（2027-01-04）の年に生成した場合
    dump("yearend.html", "yearend.jsonl", (2027, 1, 4))
    dump("yearend.html", "yearend_now2029.jsonl", (2029, 6, 1))

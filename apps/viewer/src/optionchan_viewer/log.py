"""JSON の構造化ログ（stdout 1 行 1 件）。

Cloud Logging は severity と message を解釈する（apps/ingest の log.py と同じ形）。
"""

import json
import sys
from datetime import date, datetime


def _default(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


def log(severity: str, event: str, stream=None, **fields) -> None:
    record = {"severity": severity, "message": event, "event": event}
    record.update({k: v for k, v in fields.items() if v is not None})
    print(json.dumps(record, ensure_ascii=False, default=_default), file=stream or sys.stdout, flush=True)

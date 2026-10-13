import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

TESTS_DIR = Path(__file__).resolve().parent
APP_ROOT = TESTS_DIR.parent
PKG_DIR = APP_ROOT / "src" / "optionchan_viewer"
REPO_ROOT = TESTS_DIR.parents[2]

# legacy_sql（現行の SQL の写し）を import できるようにする
sys.path.insert(0, str(TESTS_DIR))

# 2026-10-13 06:00 UTC（JST 15:00）
NOW = datetime(2026, 10, 13, 6, 0, tzinfo=UTC)


class FakeRunner:
    """runner の偽物。名前ごとの結果（行のリスト、または例外）を返し、呼び出しを記録する"""

    def __init__(self, results=None):
        self.results = results or {}
        self.calls = []

    def __call__(self, name, params):
        self.calls.append((name, params))
        result = self.results[name]
        if isinstance(result, Exception):
            raise result
        return result


@pytest.fixture
def fake_runner():
    return FakeRunner

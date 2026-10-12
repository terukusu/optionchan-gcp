import sys
from pathlib import Path

import pytest

TESTS_DIR = Path(__file__).resolve().parent
FIXTURES = TESTS_DIR / "fixtures"
REPO_ROOT = TESTS_DIR.parents[2]

# htmlgen（テスト用 HTML の組み立て）を import できるようにする
sys.path.insert(0, str(TESTS_DIR))


def read_fixture(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


@pytest.fixture
def fixture_html():
    return read_fixture

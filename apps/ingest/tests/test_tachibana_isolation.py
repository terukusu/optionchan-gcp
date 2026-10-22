"""立花の部品が本番の経路から読み込まれないこと・秘密を読まないこと（AC 4・5 を CI でも確かめる）。"""

import ast
import subprocess
import sys
from pathlib import Path

from conftest import TESTS_DIR

import optionchan_ingest

PACKAGE_DIR = Path(optionchan_ingest.__file__).resolve().parent
SOURCES_DIR = PACKAGE_DIR / "sources"


def test_app_import_does_not_load_sources():
    code = (
        "import sys, optionchan_ingest.app; "
        "print('\\n'.join(sorted(m for m in sys.modules if m.startswith('optionchan_ingest.sources'))))"
    )
    result = subprocess.run([sys.executable, "-c", code], check=True, capture_output=True, text=True)
    assert result.stdout.strip() == ""


def test_parser_import_does_not_load_client():
    # requests は optionchan_ingest.parser の pyquery が読み込むので見ない。client を読み込まないことだけ確かめる
    code = (
        "import sys, optionchan_ingest.sources.tachibana.parser, optionchan_ingest.sources.tachibana.master; "
        "print(sorted(m for m in ('optionchan_ingest.sources.tachibana.client',) if m in sys.modules))"
    )
    result = subprocess.run([sys.executable, "-c", code], check=True, capture_output=True, text=True)
    assert result.stdout.strip() == "[]"


def _imports(path: Path) -> list[str]:
    names = []
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            names += [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom):
            names.append("." * node.level + (node.module or ""))
            names += [a.name for a in node.names]
    return names


def test_production_modules_do_not_import_sources():
    files = [p for p in PACKAGE_DIR.rglob("*.py") if SOURCES_DIR not in p.parents]
    assert files
    for path in files:
        for name in _imports(path):
            assert "sources" not in name.split(".")
            assert "tachibana" not in name


def test_sources_do_not_read_environment_or_secret_manager():
    files = list(SOURCES_DIR.rglob("*"))
    assert any(p.name == "client.py" for p in files)
    for path in files:
        if path.is_file() and "__pycache__" not in path.parts:
            text = path.read_text(encoding="utf-8")
            for word in ("os.environ", "getenv", "secret" + "manager"):
                assert word not in text, f"{path.name}: {word}"


def test_sources_do_not_log_or_print():
    for path in SOURCES_DIR.rglob("*.py"):
        names = _imports(path)
        assert "logging" not in names and "log" not in names, path.name
        tree = ast.parse(path.read_text(encoding="utf-8"))
        calls = [n.func.id for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)]
        assert "print" not in calls, path.name


def test_tests_do_not_name_the_real_host():
    host = "e-shiten" + ".jp"
    for path in TESTS_DIR.rglob("*"):
        if path.is_file() and "__pycache__" not in path.parts:
            assert host not in path.read_text(encoding="utf-8", errors="replace"), path.name

"""イメージの中身の約束の静的検査（#52 AC 7。docker build は CI の viewer-image job）。

- テンプレートは外部の script / stylesheet を読まない（CDN をやめ、static/vendor/ に同梱したものだけ）
- package.json の依存は版を固定し、npm run vendor のコピー対象とテンプレートの参照が一致する
- .dockerignore は許可リスト方式（auth.txt 等が context に入らない）
"""

import json
import re

from conftest import APP_ROOT, PKG_DIR

TEMPLATES = sorted((PKG_DIR / "templates").glob("*.html"))
PACKAGE = json.loads((APP_ROOT / "package.json").read_text(encoding="utf-8"))
LOCK = json.loads((APP_ROOT / "package-lock.json").read_text(encoding="utf-8"))


def vendor_files():
    cmd = PACKAGE["scripts"]["vendor"]
    return sorted(p.rsplit("/", 1)[1] for p in re.findall(r"node_modules/\S+", cmd))


def test_templates_have_no_external_resources():
    assert len(TEMPLATES) == 4
    for path in TEMPLATES:
        text = path.read_text(encoding="utf-8")
        refs = re.findall(r"<script[^>]*\bsrc=\"([^\"]+)\"", text) + re.findall(r"<link[^>]*\bhref=\"([^\"]+)\"", text)
        for src in refs:
            assert src.startswith("/static/") and not src.startswith("//"), (path.name, src)
        assert "http://" not in text and "https://" not in text and "//cdn" not in text, path.name


def test_templates_have_no_inline_script_or_style():
    # N2: CSP（script-src / style-src 'self'）で動くように、inline の script・style・イベント属性を置かない
    for path in TEMPLATES:
        text = path.read_text(encoding="utf-8")
        assert not re.search(r"<script(?![^>]*\bsrc=)[^>]*>", text), path.name
        assert "<style" not in text and not re.search(r"\sstyle=", text), path.name
        assert not re.search(r"\son[a-z]+=", text), path.name
        assert "javascript:" not in text, path.name


def test_templates_reference_existing_static():
    vendor = set(vendor_files())
    for path in TEMPLATES:
        text = path.read_text(encoding="utf-8")
        for src in re.findall(r"(?:src|href)=\"/static/([^\"]+)\"", text):
            if src.startswith("vendor/"):
                assert src.removeprefix("vendor/") in vendor, src
            else:
                assert (PKG_DIR / "static" / src).is_file(), src


def test_vendor_files_used():
    used = set()
    for path in TEMPLATES:
        used |= set(re.findall(r"/static/vendor/([^\"]+)", path.read_text(encoding="utf-8")))
    assert sorted(used) == vendor_files()


def test_npm_versions_pinned_and_locked():
    deps = PACKAGE["dependencies"]
    assert set(deps) == {"chart.js", "chartjs-chart-financial", "luxon", "chartjs-adapter-luxon"}
    assert "devDependencies" not in PACKAGE
    for name, version in deps.items():
        assert re.fullmatch(r"\d+\.\d+\.\d+", version), (name, version)
        locked = LOCK["packages"][f"node_modules/{name}"]
        assert locked["version"] == version
    assert deps["chart.js"].startswith("4.")
    for key, pkg in LOCK["packages"].items():
        if key:
            assert pkg["integrity"].startswith("sha512-"), key
            assert pkg["resolved"].startswith("https://registry.npmjs.org/"), key


def test_dockerignore_is_allowlist():
    lines = [line.strip() for line in (APP_ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()]
    lines = [line for line in lines if line and not line.startswith("#")]
    assert lines[0] == "*"
    allowed = {line[1:] for line in lines if line.startswith("!")}
    assert allowed == {"pyproject.toml", "uv.lock", "package.json", "package-lock.json", "src/"}


def test_dockerfile():
    text = (APP_ROOT / "Dockerfile").read_text(encoding="utf-8")
    # ベースイメージは digest で固定、npm は lock どおり・スクリプトを実行しない、実行は非 root
    for image in re.findall(r"^ARG \w+_IMAGE=(\S+)$", text, re.M):
        assert re.search(r"@sha256:[0-9a-f]{64}$", image), image
    assert "npm ci --ignore-scripts" in text
    assert "uv sync --locked --no-dev" in text
    assert "USER 10001" in text
    assert "auth.txt" not in text.replace("auth.txt・", "")

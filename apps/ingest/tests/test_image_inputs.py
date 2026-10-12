"""イメージのタグの入力（.github/scripts/ingest-image.sh）の検査（#33 AC 3）。

タグは入力パスの git のハッシュから決まる。イメージに入るものが入力から漏れると、中身が変わったのに
タグが同じ（= Artifact Registry の古いイメージのまま）になるので、.dockerignore・Dockerfile と突き合わせる。
"""

import re
import shutil
import subprocess

import pytest
from conftest import REPO_ROOT

SCRIPT = REPO_ROOT / ".github" / "scripts" / "ingest-image.sh"
DOCKERFILE = REPO_ROOT / "apps" / "ingest" / "Dockerfile"
DOCKERIGNORE = REPO_ROOT / ".dockerignore"
# イメージの中身には入らないがビルドを決めるもの（スクリプト側だけにある）
BUILD_ONLY = {"apps/ingest/Dockerfile", ".dockerignore"}


def script_paths():
    out = subprocess.run(["bash", str(SCRIPT), "paths"], check=True, capture_output=True, text=True).stdout
    return [line for line in out.splitlines() if line]


def dockerignore_allowed():
    allowed = set()
    for line in DOCKERIGNORE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line.startswith("!"):
            allowed.add(line[1:].rstrip("/"))
    return allowed


def dockerfile_copy_sources():
    sources = []
    for line in DOCKERFILE.read_text(encoding="utf-8").splitlines():
        parts = line.split()
        if not parts or parts[0].upper() != "COPY":
            continue
        if any(p.startswith("--from") for p in parts[1:]):
            continue
        args = [p for p in parts[1:] if not p.startswith("--")]
        sources.extend(a.rstrip("/") for a in args[:-1])
    return sources


def under(path, roots):
    return any(path == r or path.startswith(r + "/") for r in roots)


def test_paths_are_unique_and_exist():
    paths = script_paths()
    assert len(paths) == len(set(paths))
    for p in paths:
        assert (REPO_ROOT / p).exists(), p


def test_paths_match_dockerignore_allowlist():
    # 片方だけ直すと落ちる
    assert set(script_paths()) - BUILD_ONLY == dockerignore_allowed()
    assert BUILD_ONLY <= set(script_paths())


def test_dockerfile_copies_only_inputs():
    roots = set(script_paths())
    sources = dockerfile_copy_sources()
    assert sources  # 構文の変化で検査が空にならないように
    for src in sources:
        assert under(src, roots), src


@pytest.mark.skipif(shutil.which("git") is None, reason="git が無い")
def test_tag_is_deterministic():
    def tag():
        cmd = ["bash", str(SCRIPT), "tag"]
        return subprocess.run(cmd, check=True, capture_output=True, text=True, cwd=REPO_ROOT).stdout.strip()

    first = tag()
    assert re.fullmatch(r"src-[0-9a-f]{20}", first)
    assert tag() == first

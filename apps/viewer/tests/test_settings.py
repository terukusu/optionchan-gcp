"""設定（環境変数）の検査。プロジェクト ID は必須で既定値を持たない（リポジトリに個人の設定を入れない）。"""

import pytest

from optionchan_viewer import settings as st


def test_project_is_required():
    with pytest.raises(RuntimeError):
        st.from_env({})


def test_defaults():
    cfg = st.from_env({"GCP_PROJECT": "example-project"})
    assert (cfg.dataset, cfg.location, cfg.maximum_bytes_billed) == ("optionchan_tokyo", "asia-northeast1", 1024**3)
    assert cfg.daily_bytes_billed == 4 * 1024**3


def test_daily_bytes_billed_from_env():
    cfg = st.from_env({"GCP_PROJECT": "example-project", "DAILY_BYTES_BILLED": str(8 * 1024**3)})
    assert cfg.daily_bytes_billed == 8 * 1024**3


@pytest.mark.parametrize("project", ["example-project", "example.com:my-project", "a-b.example.co.jp:proj-1"])
def test_project_forms(project):
    # N6: ドメイン付きのプロジェクト ID も受け付ける
    assert st.from_env({"GCP_PROJECT": project}).project == project


@pytest.mark.parametrize("env", [
    {"GCP_PROJECT": "x`; DROP"},
    {"GCP_PROJECT": "example.com:proj`x"},
    {"GCP_PROJECT": "example..com:my-project"},
    {"GCP_PROJECT": "example.com:my project"},
    {"GCP_PROJECT": ":my-project"},
    {"GCP_PROJECT": "example-project", "BQ_DATASET": "a.b"},
    {"GCP_PROJECT": "example-project", "BQ_DATASET": "a`b"},
    {"GCP_PROJECT": "example-project", "MAX_BYTES_BILLED": "0"},
    {"GCP_PROJECT": "example-project", "MAX_BYTES_BILLED": str(11 * 1024**3)},
    {"GCP_PROJECT": "example-project", "DAILY_BYTES_BILLED": "-1"},
    {"GCP_PROJECT": "example-project", "DAILY_BYTES_BILLED": str(101 * 1024**3)},
])
def test_rejects_bad_values(env):
    with pytest.raises(ValueError):
        st.from_env(env)


def test_app_import_does_not_need_settings(monkeypatch):
    # gunicorn の optionchan_viewer.app:app を読むまでは設定・BigQuery に触れない
    monkeypatch.delenv("GCP_PROJECT", raising=False)
    import optionchan_viewer.app as app_module

    assert callable(app_module.create_app)

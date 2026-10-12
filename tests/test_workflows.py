"""GitHub Actions の workflow の静的検査 (#10)。

公開リポジトリで本番の権限を扱うので、壊すと危ない約束をテストで固定する。
"""
import glob
import os
import re

import pytest
import yaml

WORKFLOWS_DIR = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.github', 'workflows'))
WORKFLOW_FILES = sorted(glob.glob(os.path.join(WORKFLOWS_DIR, '*.yml')) + glob.glob(os.path.join(WORKFLOWS_DIR, '*.yaml')))
SHA_PINNED = re.compile(r'^[^@\s]+@[0-9a-f]{40}$')
# id-token: write（GCP の SA を借用できる）を持ってよい job
ID_TOKEN_JOBS = {'terraform-plan', 'plan', 'apply'}


def load(path):
    with open(path, encoding='utf-8') as f:
        return yaml.safe_load(f)


def triggers(workflow):
    # YAML 1.1 では on が True として読まれる
    on = workflow.get('on', workflow.get(True))
    if isinstance(on, str):
        return {on: None}
    if isinstance(on, list):
        return {name: None for name in on}
    return on or {}


def iter_uses(workflow):
    for job in workflow.get('jobs', {}).values():
        if 'uses' in job:
            yield job['uses']
        for step in job.get('steps', []):
            if 'uses' in step:
                yield step['uses']


@pytest.fixture(scope='module', params=WORKFLOW_FILES, ids=os.path.basename)
def workflow(request):
    return load(request.param)


def test_workflows_exist():
    names = {os.path.basename(p) for p in WORKFLOW_FILES}
    assert {'ci.yml', 'deploy.yml'} <= names


def test_actions_are_pinned_by_sha(workflow):
    for uses in iter_uses(workflow):
        if uses.startswith('./'):
            continue
        assert SHA_PINNED.match(uses), uses


def test_top_level_permissions_are_read_only(workflow):
    permissions = workflow.get('permissions')
    # 省略するとリポジトリ既定の権限になるので必ず書く
    assert isinstance(permissions, dict), permissions
    assert 'id-token' not in permissions
    assert 'write' not in permissions.values()


def test_no_dangerous_triggers(workflow):
    on = triggers(workflow)
    assert 'pull_request_target' not in on
    assert 'workflow_run' not in on


def test_required_triggers_are_not_path_filtered(workflow):
    # 必須 status check の workflow をパスで絞ると check が来ずに PR が詰まる
    on = triggers(workflow)
    for event in ('pull_request', 'push'):
        config = on.get(event) or {}
        assert 'paths' not in config, event
        assert 'paths-ignore' not in config, event


def test_id_token_only_in_terraform_jobs(workflow):
    for name, job in workflow.get('jobs', {}).items():
        permissions = job.get('permissions') or {}
        if isinstance(permissions, dict) and permissions.get('id-token') == 'write':
            assert name in ID_TOKEN_JOBS, name


def test_ci_jobs_match_required_checks():
    ci = load(os.path.join(WORKFLOWS_DIR, 'ci.yml'))
    assert 'pull_request' in triggers(ci)
    # ブランチ保護の必須 status check（runbook「GitHub 設定」）と同じ名前
    assert {'pytest', 'terraform-plan'} <= set(ci['jobs'])
    for name in ('pytest', 'terraform-plan'):
        assert 'name' not in ci['jobs'][name], name


def test_deploy_apply_requires_prod_environment():
    deploy = load(os.path.join(WORKFLOWS_DIR, 'deploy.yml'))
    on = triggers(deploy)
    assert on['push']['branches'] == ['master']
    apply = deploy['jobs']['apply']
    environment = apply.get('environment')
    if isinstance(environment, dict):
        environment = environment.get('name')
    assert environment == 'prod'
    # 差分があるときだけ承認待ちを作る
    assert 'changes' in apply.get('if', '')
    # plan job は Environment を使わない（承認なしで apply 用の SA を借用できないように）
    assert 'environment' not in deploy['jobs']['plan']
    assert deploy['concurrency']['cancel-in-progress'] is False


def test_apply_sa_only_in_apply_job():
    for path in WORKFLOW_FILES:
        for name, job in load(path).get('jobs', {}).items():
            text = yaml.safe_dump(job, allow_unicode=True)
            if 'GCP_SA_TF_APPLY' in text:
                assert (os.path.basename(path), name) == ('deploy.yml', 'apply')


def test_no_plan_artifacts():
    # plan ファイル・ログは state 由来の値を含むので artifact にしない
    for path in WORKFLOW_FILES:
        for uses in iter_uses(load(path)):
            assert not uses.startswith('actions/upload-artifact@'), uses

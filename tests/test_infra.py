"""infra/ の Terraform の静的検査 (#33)。

target.md 3.5: Cloud Run は全リソースで service_account を明示する（省略すると Compute Engine の既定 SA で動く）。
HCL のパーサーは入れず、resource ブロックを波かっこの対応で切り出して文字列で確かめる。
"""
import glob
import os
import re

import pytest

INFRA_DIR = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'infra'))
RUN_RESOURCE = re.compile(r'^resource\s+"(google_cloud_run_v2_(?:service|job))"\s+"([^"]+)"\s*\{', re.M)


def resource_blocks(pattern):
    for path in sorted(glob.glob(os.path.join(INFRA_DIR, '**', '*.tf'), recursive=True)):
        with open(path, encoding='utf-8') as f:
            text = f.read()
        for m in pattern.finditer(text):
            depth, i = 0, m.end() - 1
            while True:
                if text[i] == '{':
                    depth += 1
                elif text[i] == '}':
                    depth -= 1
                    if depth == 0:
                        break
                i += 1
            yield f'{os.path.relpath(path, INFRA_DIR)}:{m.group(1)}.{m.group(2)}', text[m.end():i]


RUN_BLOCKS = list(resource_blocks(RUN_RESOURCE))


def test_cloud_run_exists():
    # 検査が空振りしないように（サービス ingest は #33、ジョブ reprocess は #34 で作る）
    names = [name for name, _ in RUN_BLOCKS]
    assert any('google_cloud_run_v2_service.ingest' in name for name in names)
    assert any('google_cloud_run_v2_job.reprocess' in name for name in names)
    assert any('google_cloud_run_v2_job.calendar_sync' in name for name in names)  # #63
    assert any('google_cloud_run_v2_service.viewer' in name for name in names)


@pytest.mark.parametrize('name,body', RUN_BLOCKS, ids=[n for n, _ in RUN_BLOCKS])
def test_cloud_run_has_service_account(name, body):
    assert re.search(r'^\s*service_account\s*=', body, re.M), name


# ---- 定期クエリ（BigQuery Data Transfer Service、#40） ----

DTS_RESOURCE = re.compile(r'^resource\s+"(google_bigquery_data_transfer_config)"\s+"([^"]+)"\s*\{', re.M)
DTS_BLOCKS = dict(resource_blocks(DTS_RESOURCE))
# 名前 → (data_source_id, スケジュール（UTC）)。stage → copy → quality → compare の順で、どれも 07 時台（JST）に終える
# （失敗の M2 は 1 時間に 1 回までなので、その抑制が 08:45 の寄り付きの前に終わるように）
DTS_EXPECTED = {
    'prod/sq.tf:google_bigquery_data_transfer_config.quality_daily': ('scheduled_query', 'every day 22:15'),
    'prod/sq.tf:google_bigquery_data_transfer_config.legacy_stage': ('scheduled_query', 'every day 22:02'),
    'prod/sq.tf:google_bigquery_data_transfer_config.legacy_copy': ('cross_region_copy', 'every day 22:10'),
    'prod/sq.tf:google_bigquery_data_transfer_config.legacy_compare': ('scheduled_query', 'every day 22:30'),
    # 1 時間足（#49）。上の 4 本（07:02〜07:30 JST）の後の 07:40。失敗の M2 の抑制は 08:40 に終わる
    'prod/sq.tf:google_bigquery_data_transfer_config.atm_hourly': ('scheduled_query', 'every day 22:40'),
}


def all_tf():
    for path in sorted(glob.glob(os.path.join(INFRA_DIR, '**', '*.tf'), recursive=True)):
        with open(path, encoding='utf-8') as f:
            yield os.path.relpath(path, INFRA_DIR), f.read()


def test_dts_configs():
    assert set(DTS_BLOCKS) == set(DTS_EXPECTED)
    for name, (source, schedule) in DTS_EXPECTED.items():
        body = DTS_BLOCKS[name]
        # 実行 SA は sa-sq-dts（省略すると apply した sa-tf-apply の権限で動く）
        assert re.search(r'^\s*service_account_name\s*=\s*local\.sa_sq$', body, re.M), name
        assert re.search(rf'^\s*data_source_id\s*=\s*"{source}"$', body, re.M), name
        assert re.search(rf'^\s*schedule\s*=\s*"{schedule}"$', body, re.M), name
        # 失敗の通知は M2（ログ一致）。メールは実行 SA 宛てで届かない
        assert 'email_preferences' not in body, name
    tf = dict(all_tf())['prod/sq.tf']
    assert 'sa_sq = "sa-sq-dts@${var.project_id}.iam.gserviceaccount.com"' in tf


def test_no_bigquery_admin():
    for path, text in all_tf():
        assert 'roles/bigquery.admin' not in text, path


def test_legacy_dataset_access_is_table_level_read_only():
    """旧データセット optionchan への変更は sa-sq-dts のテーブル単位の読み取り 3 件だけ（データセットの ACL・他のロールは付けない）"""
    found = []
    for path, text in all_tf():
        for m in re.finditer(r'^resource\s+"([^"]+)"\s+"([^"]+)"\s*\{(.*?)^\}', text, re.M | re.S):
            kind, name, body = m.groups()
            if re.search(r'dataset_id\s*=\s*"optionchan"', body):
                found.append((path, kind, name))
                assert kind == 'google_bigquery_table_iam_member', (path, name)
                assert re.search(r'role\s*=\s*"roles/bigquery.dataViewer"', body)
                assert 'google_service_account.sq.member' in body
                assert 'toset(["spot_price", "future_price", "option_price"])' in body
    assert found == [('bootstrap/sq.tf', 'google_bigquery_table_iam_member', 'sq_legacy_reader')]


# ---- ビューアー（Cloud Run viewer + IAP、#53） ----

def test_viewer_is_behind_iap():
    """アプリに認証が無いので、IAP を外したり IAM の検査を無効にしたりしない"""
    body = dict(RUN_BLOCKS)['prod/viewer.tf:google_cloud_run_v2_service.viewer']
    assert re.search(r'^\s*iap_enabled\s*=\s*true$', body, re.M)
    assert re.search(r'^\s*invoker_iam_disabled\s*=\s*false$', body, re.M)
    assert re.search(r'^\s*service_account\s*=\s*local\.sa_viewer$', body, re.M)
    assert re.search(r'^\s*image\s*=\s*var\.viewer_image$', body, re.M)
    tf = dict(all_tf())['prod/viewer.tf']
    assert 'sa_viewer = "sa-viewer@${var.project_id}.iam.gserviceaccount.com"' in tf


def test_viewer_invoker_is_iap_agent_only():
    tf = dict(all_tf())['prod/viewer.tf']
    members = re.findall(r'^\s*member\s*=\s*(.+)$', tf, re.M)
    assert members == ['"serviceAccount:service-${data.google_project.this.number}@gcp-sa-iap.iam.gserviceaccount.com"']


def code_only(text):
    return '\n'.join(line for line in text.splitlines() if not line.lstrip().startswith('#'))


def test_no_public_members():
    """allUsers / allAuthenticatedUsers をどの付与にも使わない（コメント・validation の文言は除く）"""
    for path, text in all_tf():
        code = code_only(text)
        assert not re.search(r'^\s*members?\s*=.*all(Authenticated)?Users', code, re.M), path
        assert not re.search(r'"all(Authenticated)?Users"', code), path


def test_iap_access_only_in_bootstrap():
    """IAP のアクセス許可は roles/iap.admin が要るので所有者の bootstrap だけ（sa-tf-apply に iap.admin を付けない）。
    OAuth のシークレットを state に入れない（google_iap_settings・client_secret を使わない）"""
    found = []
    for path, text in all_tf():
        code = code_only(text)
        assert 'roles/iap.admin' not in code, path
        assert 'google_iap_settings' not in text and 'client_secret' not in text, path
        for m in re.finditer(r'^resource\s+"(google_iap_[^"]+)"\s+"([^"]+)"', text, re.M):
            found.append((path, m.group(1), m.group(2)))
    assert found == [('bootstrap/viewer.tf', 'google_iap_web_cloud_run_service_iam_member', 'viewer_access')]
    tf = dict(all_tf())['bootstrap/viewer.tf']
    assert 'for_each = toset(var.viewer_iap_members)' in tf
    assert 'role                   = "roles/iap.httpsResourceAccessor"' in tf


def test_viewer_iap_members_has_no_default():
    """既定値があると、tfvars を置き忘れた apply で付与が消える"""
    tf = dict(all_tf())['bootstrap/variables.tf']
    m = re.search(r'^variable\s+"viewer_iap_members"\s*\{(.*?)^\}', tf, re.M | re.S)
    assert m
    assert 'default' not in m.group(1)
    assert 'validation' in m.group(1)


# ---- 旧系の停止（#14、target.md 6 章 5a） ----

LEGACY_DTS = {'legacy_stage', 'legacy_copy', 'legacy_compare'}


def test_legacy_stopped_defaults_to_false():
    """既定のままなら旧系に触れない（plan 差分 0）。止める・戻すは既定値を変える PR で行う"""
    tf = dict(all_tf())['prod/variables.tf']
    m = re.search(r'^variable\s+"legacy_stopped"\s*\{(.*?)^\}', tf, re.M | re.S)
    assert m
    assert re.search(r'^\s*type\s*=\s*bool$', m.group(1), re.M)
    assert re.search(r'^\s*default\s*=\s*false$', m.group(1), re.M)


def test_legacy_stopped_disables_only_legacy_dts():
    """legacy_stopped で無効にするのは新旧の突き合わせの 3 本だけ（品質チェックと 1 時間足は動かし続ける）"""
    for name, body in DTS_BLOCKS.items():
        short = name.rsplit('.', 1)[1]
        has = re.search(r'^\s*disabled\s*=\s*var\.legacy_stopped$', body, re.M)
        if short in LEGACY_DTS:
            assert has, name
        else:
            assert 'disabled' not in body, name


def test_legacy_stopped_removes_only_legacy_alerts():
    """legacy_stopped で消すアラートは旧系の m1・m2 だけ（新系の ingest_m1・ingest_m2 は残す）"""
    tf = dict(all_tf())['prod/monitoring.tf']
    m = re.search(r'^\s*legacy_alert_policies\s*=\s*\{(.*?)^\s*\}', tf, re.M | re.S)
    assert m
    assert re.findall(r'^\s*(\w+)\s*=', m.group(1), re.M) == ['m1', 'm2']
    assert ('alert_policies = merge({ for k, v in local.legacy_alert_policies : k => v if !var.legacy_stopped }, {'
            in tf)
    # 取り込みの import は作るポリシーだけに絞る（消したポリシーの ID が secret に残っていても plan が通る）
    imports = dict(all_tf())['prod/imports.tf']
    assert 'if contains(keys(local.alert_policies), k)' in imports


# ---- 取引カレンダー（#63） ----


def test_trading_calendar_resources():
    tf = dict(all_tf())['prod/calendar.tf']
    table = re.search(r'resource "google_bigquery_table" "trading_calendar" \{(.*?)\n\}', tf, re.S).group(1)
    # 版の履歴なので削除は二重に止める。列の正は schema/tokyo/trading_calendar.json
    assert 'deletion_protection = true' in table
    assert 'prevent_destroy = true' in table
    assert 'schema/tokyo/trading_calendar.json' in table
    assert 'time_partitioning' not in table
    job = re.search(r'resource "google_cloud_run_v2_job" "calendar_sync" \{(.*?)\n\}', tf, re.S).group(1)
    assert 'name                = "calendar-sync"' in job
    assert 'command = ["python", "-m", "optionchan_ingest.calendar_sync"]' in job
    assert 'service_account = local.sa_ingest' in job
    assert 'max_retries     = 0' in job
    # 定期起動は ingest サービスの 06:40 のリクエスト（Scheduler を足さない）
    assert 'google_cloud_scheduler_job' not in tf


def test_calendar_toml_sa_is_read_only():
    """#66: workflow calendar-toml の SA は最新版のテーブル・ビューの dataViewer と bigquery.jobUser だけ"""
    tf = dict(all_tf())
    granted = []
    for path, text in tf.items():
        for m in re.finditer(r'role\s*=\s*"([^"]+)"\s*\n\s*member\s*=\s*([^\n]+)', text):
            if 'calendar_toml' in m.group(2) or 'sa-calendar-toml@' in m.group(2):
                granted.append((path, m.group(1)))
    assert sorted(granted) == [
        ('bootstrap/calendar_toml.tf', 'roles/bigquery.jobUser'),
        ('prod/calendar.tf', 'roles/bigquery.dataViewer'),
    ]
    prod = tf['prod/calendar.tf']
    block = re.search(r'resource "google_bigquery_table_iam_member" "calendar_toml_reader" \{(.*?)\n\}', prod, re.S).group(1)
    assert 'google_bigquery_table.trading_calendar.table_id' in block
    assert 'google_bigquery_table.trading_calendar_latest.table_id' in block
    # 借用は master の ref だけ
    assert ':ref:refs/heads/master"' in tf['bootstrap/calendar_toml.tf']

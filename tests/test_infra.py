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

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

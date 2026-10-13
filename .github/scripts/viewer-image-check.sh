#!/usr/bin/env bash
# ビューアー（apps/viewer）のイメージの中身の検査（#52 の ci の viewer-image を切り出したもの。#53）。
# ci.yml の viewer-image と deploy.yml の apply job（push の前）で共用する。GCP の認証は使わない。
#
#   viewer-image-check.sh <ローカルのイメージ名>   例: viewer:ci
set -euo pipefail

img="${1:?usage: $0 <image>}"

# import だけで設定・BigQuery に触れないこと（GCP_PROJECT はダミー）
docker run --rm -e GCP_PROJECT=example-project "$img" python -c 'import optionchan_viewer.app'
pkg=$(docker run --rm "$img" python -c 'import optionchan_viewer, os; print(os.path.dirname(optionchan_viewer.__file__))')
# auth.txt・.git・テスト・node_modules がイメージに入っていないこと
found=$(docker run --rm --entrypoint sh "$img" -c \
  'find / -xdev \( -name auth.txt -o -name auth.json -o -name .git -o -name node_modules -o -path "*/optionchan_viewer/tests" \) 2>/dev/null || true' | wc -l)
test "$found" -eq 0
# テンプレートが CDN（外部の script）を参照しないこと
cdn=$(docker run --rm --entrypoint sh "$img" -c \
  "grep -rlE '<script[^>]*src=\"(https?:)?//' $pkg/templates || true" | wc -l)
test "$cdn" -eq 0
# 同梱したブラウザ用ライブラリ（package-lock.json から作ったもの。chart.js・lightweight-charts）が 2 つあること
n=$(docker run --rm --entrypoint sh "$img" -c "ls $pkg/static/vendor/*.js" | wc -l)
test "$n" -eq 2
echo "viewer のイメージの検査: OK"

#!/usr/bin/env bash
# Cloud Run のイメージ（取り込み ingest #33、ビューアー viewer #53）のタグと参照を求める。ci.yml と deploy.yml で共用する。
#
#   image.sh <name> paths    イメージの入力パスを 1 行 1 件で出す（apps/<name>/tests/test_image_inputs.py が使う）
#   image.sh <name> tag      入力パスの git のハッシュからタグ src-<20hex> を出す（同じ入力なら同じタグ）
#   image.sh <name> resolve  tag / image / exists を GITHUB_OUTPUT の形式（key=value）で出す。
#                            Artifact Registry にタグがあれば image は digest 参照・exists=true、
#                            無ければタグ参照・exists=false。権限・API などそれ以外の失敗は exit 1
#
# <name> は ingest か viewer。タグはコミット済みの内容（HEAD）から求める。作業ツリーの未コミットの変更は反映されない。
# resolve は gcloud を使う（CI では google-github-actions/auth の資格情報、ローカルでは所有者の資格情報）。
#
# 環境変数（resolve だけが使う。paths / tag は未設定でも動く）:
#   GCP_PROJECT_ID  GCP プロジェクト ID（必須。CI は vars.GCP_PROJECT_ID）
#   GCP_REGION      Artifact Registry のリージョン（既定 asia-northeast1）
set -euo pipefail

usage() {
  echo "usage: $0 {ingest|viewer} {paths|tag|resolve}" >&2
  exit 2
}

# Artifact Registry のリポジトリ（<region>-docker.pkg.dev/<project>/optionchan）。resolve の中でだけ求める
registry_repo() {
  if [ -z "${GCP_PROJECT_ID:-}" ]; then
    echo "環境変数 GCP_PROJECT_ID（GCP プロジェクト ID）が未設定" >&2
    exit 2
  fi
  printf '%s-docker.pkg.dev/%s/optionchan\n' "${GCP_REGION:-asia-northeast1}" "$GCP_PROJECT_ID"
}

IMAGE_NAME="${1:-}"
# イメージの入力。.dockerignore の許可リスト（! の行）と Dockerfile・.dockerignore 自身。
# 片方だけ直すとテストが落ちる（apps/<name>/tests/test_image_inputs.py）。
# ingest の並びを変えるとタグが変わり、同じ中身のイメージを作り直すことになる（並びは #33 のまま）
case "$IMAGE_NAME" in
  ingest)
    # ビルド context はリポジトリ直下（docker build -f apps/ingest/Dockerfile .）
    INPUT_PATHS=(
      apps/ingest/Dockerfile
      apps/ingest/pyproject.toml
      apps/ingest/uv.lock
      apps/ingest/src
      schema/tokyo
      .dockerignore
    )
    ;;
  viewer)
    # ビルド context は apps/viewer（docker build apps/viewer）
    INPUT_PATHS=(
      apps/viewer/Dockerfile
      apps/viewer/.dockerignore
      apps/viewer/pyproject.toml
      apps/viewer/uv.lock
      apps/viewer/package.json
      apps/viewer/package-lock.json
      apps/viewer/src
    )
    ;;
  *) usage ;;
esac

# gcloud のエラーは認証中のアカウント名やプロジェクト番号を含みうるので、そのまま出さない
MASK='s/[0-9]{9,}/<num>/g; s/[A-Za-z0-9._%+-]+@([A-Za-z0-9-]+\.)+[A-Za-z]{2,}/<email>/g'

cmd_paths() {
  printf '%s\n' "${INPUT_PATHS[@]}"
}

cmd_tag() {
  local root path hash hashes=""
  root=$(git rev-parse --show-toplevel)
  for path in "${INPUT_PATHS[@]}"; do
    # 入力が HEAD に無ければ失敗させる（タグが入力を表さなくなるため）
    if ! hash=$(git -C "$root" rev-parse --verify --quiet "HEAD:${path}"); then
      echo "HEAD に ${path} が無い" >&2
      exit 1
    fi
    hashes+="${path} ${hash}"$'\n'
  done
  printf 'src-%s\n' "$(printf '%s' "$hashes" | sha256sum | cut -c1-20)"
}

cmd_resolve() {
  local repo tag ref out rc=0
  repo=$(registry_repo)
  tag=$(cmd_tag)
  ref="${repo}/${IMAGE_NAME}:${tag}"
  out=$(CLOUDSDK_CORE_DISABLE_PROMPTS=1 gcloud artifacts docker images describe "$ref" \
    --format='value(image_summary.digest)' 2>&1) || rc=$?
  if [ "$rc" -eq 0 ] && [[ "$out" =~ ^sha256:[0-9a-f]{64}$ ]]; then
    printf 'tag=%s\nimage=%s\nexists=true\n' "$tag" "${repo}/${IMAGE_NAME}@${out}"
  elif [ "$rc" -ne 0 ] && grep -q 'Image not found' <<<"$out"; then
    printf 'tag=%s\nimage=%s\nexists=false\n' "$tag" "$ref"
  else
    echo "イメージの確認に失敗した（exit $rc）。出力（マスク済み）:" >&2
    sed -E "$MASK" <<<"$out" | tail -n 20 >&2
    exit 1
  fi
}

case "${2:-}" in
  paths) cmd_paths ;;
  tag) cmd_tag ;;
  resolve) cmd_resolve ;;
  *) usage ;;
esac

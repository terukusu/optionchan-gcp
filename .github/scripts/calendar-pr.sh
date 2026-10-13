#!/usr/bin/env bash
# 取引カレンダーの TOML を BigQuery の最新版に合わせる PR / Issue を作る・直す・閉じる（#66。workflow calendar-toml の propose job）。
#
# 使い方: calendar-pr.sh pr|issue
#   pr:    GitHub App のトークン（GH_TOKEN）でブランチ $BRANCH に push し、PR を作る・更新する。
#          App のトークンで作った PR・push は pull_request の workflow（必須チェック）を起動する
#   issue: App が無いとき。GITHUB_TOKEN（issues: write）で Issue を作る・更新する（PR は所有者が手で出す）
# 環境変数:
#   STATUS     read-calendar job の結果（changed / unchanged）
#   TITLE      PR のタイトル（changed のとき）
#   BODY_B64   PR の本文（base64。changed のとき）
#   TOML_B64   生成した config/trading_calendar.toml（base64。changed のとき。pr だけ）
#   GH_TOKEN   gh のトークン
#   BRANCH     PR のブランチ（pr だけ）
#   APP_SLUG   App の slug（pr だけ。コミットの author に使う）
# 最新版と TOML が一致したら（unchanged）、開いている PR / Issue を閉じる（PR のブランチも消す）。
set -euo pipefail

mode=${1:?pr か issue}
repo=${GITHUB_REPOSITORY:?}
toml=config/trading_calendar.toml
issue_title="取引カレンダーの TOML を BigQuery の最新版に合わせる PR が要る（#66）"

body_file=$(mktemp)
trap 'rm -f "$body_file"' EXIT
if [ "$STATUS" = changed ]; then
  : "${TITLE:?}" "${BODY_B64:?}"
  printf '%s' "$BODY_B64" | base64 -d > "$body_file"
elif [ "$STATUS" != unchanged ]; then
  echo "STATUS が想定外: ${STATUS}" >&2
  exit 1
fi

summary() { echo "$1" | tee -a "${GITHUB_STEP_SUMMARY:-/dev/null}"; }

propose_pr() {
  : "${BRANCH:?}" "${APP_SLUG:?}"
  local pr
  # --head はブランチ名だけで一致する（fork の同名ブランチの PR も返る）ので、このリポジトリのブランチで App が作った PR だけを扱う
  pr=$(gh pr list -R "$repo" --head "$BRANCH" --base master --state open --json number,isCrossRepository,author \
    | jq -r --arg s "$APP_SLUG" \
      '[.[] | select(.isCrossRepository == false and (.author.login == "app/\($s)" or .author.login == "\($s)[bot]"))][0].number // empty')

  if [ "$STATUS" = unchanged ]; then
    if [ -n "$pr" ]; then
      gh pr close "$pr" -R "$repo" --delete-branch \
        --comment "BigQuery の最新版と master の TOML が一致したので閉じる（workflow calendar-toml）。"
      summary "最新版と TOML が一致: PR #${pr} を閉じた"
    else
      summary "最新版と TOML が一致: 何もしない"
    fi
    return
  fi

  printf '%s' "${TOML_B64:?}" | base64 -d > "$toml"
  if git diff --quiet -- "$toml"; then
    # read-calendar job の後に master が先に更新された
    summary "master の TOML が既に最新版と一致: 何もしない"
    return
  fi

  # 開いている PR のブランチが同じ内容なら push しない（毎日 CI を回さない）
  if [ -n "$pr" ] && git fetch --quiet --depth=1 origin "refs/heads/${BRANCH}" \
    && git diff --quiet FETCH_HEAD -- "$toml"; then
    gh pr edit "$pr" -R "$repo" --title "$TITLE" --body-file "$body_file" > /dev/null
    summary "PR #${pr} は同じ内容: タイトル・本文だけ更新した"
    return
  fi

  local bot_id
  bot_id=$(gh api "users/${APP_SLUG}%5Bbot%5D" --jq .id)
  git config user.name "${APP_SLUG}[bot]"
  git config user.email "${bot_id}+${APP_SLUG}[bot]@users.noreply.github.com"
  git switch --quiet -c "$BRANCH"
  git add -- "$toml"
  git commit --quiet -m "$TITLE" -m "workflow calendar-toml（#66）が BigQuery の取引カレンダーの最新版から生成した。"
  # ブランチはこの workflow 専用。master の上に作り直す（前の内容は PR の履歴に残る）
  git push --quiet --force origin "HEAD:refs/heads/${BRANCH}"

  if [ -n "$pr" ]; then
    gh pr edit "$pr" -R "$repo" --title "$TITLE" --body-file "$body_file" > /dev/null
    summary "PR #${pr} を更新した"
  else
    gh pr create -R "$repo" --base master --head "$BRANCH" --title "$TITLE" --body-file "$body_file"
    summary "PR を作った: ${TITLE}"
  fi
}

propose_issue() {
  local issue
  # この workflow（GITHUB_TOKEN = github-actions）が立てた Issue だけを扱う
  issue=$(gh issue list -R "$repo" --state open --author app/github-actions --search "in:title \"${issue_title}\"" \
    --json number,title,author \
    | jq -r --arg t "$issue_title" \
      '[.[] | select(.title == $t and (.author.login == "app/github-actions" or .author.login == "github-actions[bot]"))][0].number // empty')

  if [ "$STATUS" = unchanged ]; then
    if [ -n "$issue" ]; then
      gh issue close "$issue" -R "$repo" \
        --comment "BigQuery の最新版と master の TOML が一致したので閉じる（workflow calendar-toml）。"
      summary "最新版と TOML が一致: Issue #${issue} を閉じた"
    else
      summary "最新版と TOML が一致: 何もしない"
    fi
    return
  fi

  {
    echo
    echo "---"
    echo
    echo "GitHub App が未設定なので PR は作っていない。手で PR を出す（docs/runbook.md「取引カレンダー（#63）」の「TOML」）:"
    echo
    echo '```sh'
    echo 'cd apps/ingest'
    # shellcheck disable=SC2016 # $P は Issue の本文にそのまま書く
    echo 'GCP_PROJECT=$P uv run --locked python -m optionchan_ingest.calendar_sync --update-toml   # TOML を書き換える'
    echo '```'
    echo
    echo "PR の差分: ${TITLE}"
  } >> "$body_file"
  if [ -n "$issue" ]; then
    gh issue edit "$issue" -R "$repo" --body-file "$body_file" > /dev/null
    summary "Issue #${issue} を更新した"
  else
    gh issue create -R "$repo" --title "$issue_title" --body-file "$body_file"
    summary "Issue を作った（App が未設定）"
  fi
}

case "$mode" in
  pr) propose_pr ;;
  issue) propose_issue ;;
  *)
    echo "使い方: $0 pr|issue" >&2
    exit 2
    ;;
esac

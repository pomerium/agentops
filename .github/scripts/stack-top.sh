#!/usr/bin/env bash
set -euo pipefail

out=${GITHUB_OUTPUT:-/dev/stdout}
emit() { echo "top=$1" >> "$out"; }

if [[ "${EVENT_NAME}" != "pull_request" ]]; then
  emit true
  exit 0
fi

if [[ "${HEAD_REPO}" != "${REPO}" ]]; then
  emit true
  exit 0
fi

above=$(gh pr list --repo "$REPO" --base "$HEAD_REF" --state open --json number \
  | jq -r --argjson self "$PR_NUMBER" 'map(select(.number != $self) | "#\(.number)") | join(" ")')
if [[ -n "$above" ]]; then
  echo "Stacked under $above; the top of the stack runs the checks." >&2
  emit false
else
  emit true
fi

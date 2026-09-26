#!/usr/bin/env bash
# Tests for stack-top.sh, against a fake `gh` that answers `pr list` from
# FAKE_PRS: a JSON array of the open PRs based on the queried branch, as
# `gh pr list --json number,headRepository,headRepositoryOwner` would print it.
set -uo pipefail

here=$(cd "$(dirname "$0")" && pwd)
bin=$(mktemp -d); trap 'rm -rf "$bin"' EXIT
cat > "$bin/gh" <<'FAKE'
#!/usr/bin/env bash
echo "${FAKE_PRS:-[]}"
FAKE
chmod +x "$bin/gh"

fails=0
# run NAME WANT [VAR=value ...]: run the gate with those inputs, expect top=WANT.
run() {
  local name=$1 want=$2; shift 2
  local got
  got=$(env PATH="$bin:$PATH" GITHUB_OUTPUT= REPO=pomerium/agentops "$@" \
    "$here/stack-top.sh" 2>/dev/null | sed -n 's/^top=//p')
  if [[ "$got" == "$want" ]]; then
    echo "ok   $name"
  else
    echo "FAIL $name: top=$got, want $want"
    fails=$((fails + 1))
  fi
}

run "a push always runs" true \
  EVENT_NAME=push
run "a PR with nothing stacked on it is the top" true \
  EVENT_NAME=pull_request HEAD_REF=feature PR_NUMBER=10 HEAD_REPO=pomerium/agentops \
  FAKE_PRS='[]'
run "a PR with a PR stacked on it is not the top" false \
  EVENT_NAME=pull_request HEAD_REF=feature PR_NUMBER=10 HEAD_REPO=pomerium/agentops \
  FAKE_PRS='[{"number":11,"headRepository":{"name":"agentops"},"headRepositoryOwner":{"login":"pomerium"}}]'

# A fork's PR from its own main: HEAD_REF is "main", so the query returns every
# open PR into this repo's main, itself included. None of them is stacked on it;
# a fork's branch cannot be the base of a PR here.
run "a fork PR from its main is the top" true \
  EVENT_NAME=pull_request HEAD_REF=main PR_NUMBER=20 HEAD_REPO=someone/agentops \
  FAKE_PRS='[{"number":20,"headRepository":{"name":"agentops"},"headRepositoryOwner":{"login":"someone"}},{"number":10,"headRepository":{"name":"agentops"},"headRepositoryOwner":{"login":"pomerium"}}]'
# However the query is answered, a PR is never stacked on itself.
run "a PR never counts itself as stacked on it" true \
  EVENT_NAME=pull_request HEAD_REF=feature PR_NUMBER=10 HEAD_REPO=pomerium/agentops \
  FAKE_PRS='[{"number":10,"headRepository":{"name":"agentops"},"headRepositoryOwner":{"login":"pomerium"}}]'

exit $((fails > 0))

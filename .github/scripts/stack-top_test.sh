#!/usr/bin/env bash
set -uo pipefail

here=$(cd "$(dirname "$0")" && pwd)
bin=$(mktemp -d); trap 'rm -rf "$bin"' EXIT
cat > "$bin/gh" <<'FAKE'
#!/usr/bin/env bash
echo "${FAKE_PRS:-[]}"
FAKE
chmod +x "$bin/gh"

fails=0
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

run "a fork PR from its main is the top" true \
  EVENT_NAME=pull_request HEAD_REF=main PR_NUMBER=20 HEAD_REPO=someone/agentops \
  FAKE_PRS='[{"number":20,"headRepository":{"name":"agentops"},"headRepositoryOwner":{"login":"someone"}},{"number":10,"headRepository":{"name":"agentops"},"headRepositoryOwner":{"login":"pomerium"}}]'
run "a PR never counts itself as stacked on it" true \
  EVENT_NAME=pull_request HEAD_REF=feature PR_NUMBER=10 HEAD_REPO=pomerium/agentops \
  FAKE_PRS='[{"number":10,"headRepository":{"name":"agentops"},"headRepositoryOwner":{"login":"pomerium"}}]'

exit $((fails > 0))

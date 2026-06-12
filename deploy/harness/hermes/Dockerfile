# Agent-sandbox harness running Hermes Agent (Nous Research) as an ACP agent.
#
# hermes-agent ships native ACP support as the `acp` extra: installing
# `hermes-agent[acp]` provides the `hermes-acp` entrypoint that speaks ACP on
# stdio (see hermes-agent.nousresearch.com/docs/user-guide/features/acp).
# Authenticates headlessly via provider env vars (ANTHROPIC_API_KEY,
# OPENAI_API_KEY, OPENROUTER_API_KEY, NOUS_API_KEY, ...), which the
# SandboxTemplate injects.
#
# CAVEAT: Hermes' ACP approval flow is editor-prompt-oriented; a config-level
# always-approve for fully unattended runs is not documented (session-scoped
# "allow always" exists). Validate prompt-free operation for your workflows
# before relying on this harness unattended.
#
# Build (context is deploy/harness so the shared git-checkout.sh is reachable):
#   docker build -t agentops-agent-hermes:dev -f deploy/harness/hermes/Dockerfile deploy/harness
#   (or: make harness-build HARNESS=hermes)
FROM python:3.12-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
      git ca-certificates curl jq ripgrep \
  && rm -rf /var/lib/apt/lists/*

# Pin the version the in-repo ACP registry manifest pins (uvx --from
# 'hermes-agent[acp]==0.16.0'); `command -v` fails the build if the entrypoint
# script disappears. boto3 is hermes' lazy-installed dep for provider.bedrock
# (pin from its lazy_deps manifest) — pre-bake it so the first session doesn't
# stall on a runtime pip install.
RUN pip install --no-cache-dir 'hermes-agent[acp]==0.16.0' boto3==1.42.89 \
  && command -v hermes-acp

COPY hermes/agent-entrypoint.sh /usr/local/bin/agent-entrypoint
COPY git-checkout.sh /usr/local/bin/git-checkout
RUN chmod +x /usr/local/bin/agent-entrypoint /usr/local/bin/git-checkout
ENV ACP_AGENT_CMD="agent-entrypoint"
# Non-root uid 1000, matching the claude-code harness contract: the pod's
# fsGroup grants uid 1000 access to the workspace volume. Hermes writes config
# under HOME (~/.hermes).
RUN useradd --create-home --uid 1000 agent
ENV HOME=/home/agent
RUN mkdir -p /workspace && chown -R agent:agent /workspace
USER agent
WORKDIR /workspace
# Idle so the pod stays Ready; the orchestrator exec's the ACP agent per session.
CMD ["sleep", "infinity"]

# Agent-sandbox harness running OpenCode (anomalyco/opencode) as an ACP agent.
#
# OpenCode speaks ACP natively (`opencode acp`). It authenticates headlessly
# via provider env vars (ANTHROPIC_API_KEY, OPENAI_API_KEY, ...), which the
# SandboxTemplate injects. A baked /home/node/.config/opencode/opencode.json
# forces permission=allow so MCP-backed sessions run unattended.
#
# Build (context is deploy/harness so the shared git-checkout.sh is reachable):
#   docker build -t agentops-agent-opencode:dev -f deploy/harness/opencode/Dockerfile deploy/harness
#   (or: make harness-build HARNESS=opencode)
FROM node:22-bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
      git ca-certificates curl jq ripgrep \
  && rm -rf /var/lib/apt/lists/*

# The npm package resolves the per-platform opencode binary via
# optionalDependencies. `command -v` fails the build if that stops working.
RUN npm install -g opencode-ai && command -v opencode

COPY opencode/agent-entrypoint.sh /usr/local/bin/agent-entrypoint
COPY opencode/opencode.json /home/node/.config/opencode/opencode.json
COPY git-checkout.sh /usr/local/bin/git-checkout
RUN chmod +x /usr/local/bin/agent-entrypoint /usr/local/bin/git-checkout \
  && chown -R node:node /home/node/.config
ENV ACP_AGENT_CMD="agent-entrypoint"
# Non-root `node` user (uid 1000), matching the claude-code harness contract:
# the pod's fsGroup grants uid 1000 access to the workspace volume.
ENV HOME=/home/node
RUN mkdir -p /workspace && chown -R node:node /workspace
USER node
WORKDIR /workspace
# Idle so the pod stays Ready; the orchestrator exec's the ACP agent per session.
CMD ["sleep", "infinity"]

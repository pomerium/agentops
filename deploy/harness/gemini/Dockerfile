# Agent-sandbox harness running Google Gemini CLI as an ACP agent.
#
# Gemini CLI speaks ACP natively (`gemini --acp`, see the CLI's
# docs/cli/acp-mode.md). It authenticates headlessly with GEMINI_API_KEY
# (GOOGLE_AI_API_KEY also honored), which the SandboxTemplate injects.
#
# Build (context is deploy/harness so the shared git-checkout.sh is reachable):
#   docker build -t agentops-agent-gemini:dev -f deploy/harness/gemini/Dockerfile deploy/harness
#   (or: make harness-build HARNESS=gemini)
FROM node:22-bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
      git ca-certificates curl jq ripgrep \
  && rm -rf /var/lib/apt/lists/*

# Pre-install the CLI so each session starts fast (no npx download at exec).
RUN npm install -g @google/gemini-cli && command -v gemini

COPY gemini/agent-entrypoint.sh /usr/local/bin/agent-entrypoint
COPY git-checkout.sh /usr/local/bin/git-checkout
RUN chmod +x /usr/local/bin/agent-entrypoint /usr/local/bin/git-checkout
ENV ACP_AGENT_CMD="agent-entrypoint"
# Non-root `node` user (uid 1000), matching the claude-code harness contract:
# the pod's fsGroup grants uid 1000 access to the workspace volume. Gemini
# writes config/cache under HOME (~/.gemini).
ENV HOME=/home/node
RUN mkdir -p /workspace && chown -R node:node /workspace
USER node
WORKDIR /workspace
# Idle so the pod stays Ready; the orchestrator exec's the ACP agent per session.
CMD ["sleep", "infinity"]

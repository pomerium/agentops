FROM node:22-bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
      git ca-certificates curl jq ripgrep \
  && rm -rf /var/lib/apt/lists/*

RUN npm install -g opencode-ai && command -v opencode

COPY opencode/agent-entrypoint.sh /usr/local/bin/agent-entrypoint
COPY opencode/opencode.json /home/node/.config/opencode/opencode.json
COPY git-checkout.sh /usr/local/bin/git-checkout
RUN chmod +x /usr/local/bin/agent-entrypoint /usr/local/bin/git-checkout \
  && chown -R node:node /home/node/.config
ENV ACP_AGENT_CMD="agent-entrypoint"
ENV HOME=/home/node
RUN mkdir -p /workspace && chown -R node:node /workspace
USER node
WORKDIR /workspace
CMD ["sleep", "infinity"]

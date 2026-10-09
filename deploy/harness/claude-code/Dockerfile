FROM node:22-bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
      git ca-certificates curl wget \
      jq ripgrep fd-find \
      vim less unzip build-essential \
      python3 python3-pip python3-venv \
  && rm -rf /var/lib/apt/lists/*

RUN ln -s "$(command -v fdfind)" /usr/local/bin/fd

ARG YQ_VERSION=v4.44.3
RUN arch="$(dpkg --print-architecture)" \
  && curl -fsSL "https://github.com/mikefarah/yq/releases/download/${YQ_VERSION}/yq_linux_${arch}" \
       -o /usr/local/bin/yq \
  && chmod +x /usr/local/bin/yq

RUN pip3 install --no-cache-dir --break-system-packages \
      pandas numpy pyyaml

RUN npm install -g @agentclientprotocol/claude-agent-acp@latest

COPY claude-code/agent-entrypoint.sh /usr/local/bin/agent-entrypoint
COPY git-checkout.sh /usr/local/bin/git-checkout
RUN chmod +x /usr/local/bin/agent-entrypoint /usr/local/bin/git-checkout
ENV ACP_AGENT_CMD="agent-entrypoint"
ENV HOME=/home/node
RUN mkdir -p /workspace && chown -R node:node /workspace
USER node
WORKDIR /workspace
CMD ["sleep", "infinity"]

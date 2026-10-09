FROM golang:1.26 AS build
ARG ACP_VERSION=v0.13.5
RUN CGO_ENABLED=0 GOBIN=/out go install github.com/coder/acp-go-sdk/example/agent@${ACP_VERSION}

FROM alpine:3.20
RUN apk add --no-cache git ca-certificates curl
COPY --from=build /out/agent /usr/local/bin/acp-agent
ENV ACP_AGENT_CMD=/usr/local/bin/acp-agent
WORKDIR /workspace
CMD ["sleep", "infinity"]

module github.com/pomerium/agentops/slackbot

go 1.26.2

require (
	connectrpc.com/connect v1.20.0
	github.com/cenkalti/backoff/v7 v7.0.1
	github.com/pomerium/agentops/harness v0.0.0
	github.com/slack-go/slack v0.25.0
	golang.org/x/time v0.15.0
	google.golang.org/protobuf v1.36.12
	sigs.k8s.io/yaml v1.6.0
)

require (
	github.com/gorilla/websocket v1.5.4-0.20250319132907-e064f32e3674 // indirect
	go.yaml.in/yaml/v2 v2.4.4 // indirect
	golang.org/x/net v0.59.0 // indirect
)

replace github.com/pomerium/agentops/harness => ../harness

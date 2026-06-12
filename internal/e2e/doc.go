// Package e2e holds opt-in end-to-end tests that exercise the agent-sandbox
// harness against real external services (the Claude harness image, the
// Anthropic API, and a public MCP server).
//
// The tests are guarded by the `e2e` build tag and the AGENTOPS_E2E
// environment variable, so the default `go test ./...` run compiles this
// package but executes nothing here. This file (without a build tag) keeps the
// package non-empty for the default build.
package e2e

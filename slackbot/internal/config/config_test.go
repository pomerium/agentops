package config

import "testing"

func TestTheHarnessAPITokenFileIsOptional(t *testing.T) {
	env := map[string]string{
		"SLACK_SIGNING_SECRET": "secret",
		"SLACK_BOT_TOKEN":      "token",
		"HARNESS_API_URL":      "http://127.0.0.1:8081",
	}
	cfg, err := Load(func(key string) string { return env[key] })
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Slack.HarnessAPITokenFile; got != "" {
		t.Fatalf("an unset token file = %q, want none: the client then sends no bearer token", got)
	}

	env["HARNESS_API_TOKEN_FILE"] = "/var/run/harness-api/token"
	if cfg, err = Load(func(key string) string { return env[key] }); err != nil {
		t.Fatal(err)
	}
	if got := cfg.Slack.HarnessAPITokenFile; got != "/var/run/harness-api/token" {
		t.Fatalf("token file = %q, want the configured path", got)
	}
}

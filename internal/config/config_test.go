package config_test

import (
	"log/slog"
	"testing"

	"github.com/pomerium/agentops/internal/config"
)

func envFrom(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := config.Load(envFrom(map[string]string{
		"SLACK_SIGNING_SECRET":    "sec",
		"SLACK_BOT_TOKEN":         "xoxb-1",
		"OAUTH_REDIRECT_BASE_URL": "https://app.example",
		"POD_NAMESPACE":           "agentops",
		"DB_PATH":                 "/data/agentops.db",
	}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.HTTPAddr != ":8080" {
		t.Errorf("default HTTPAddr = %q", cfg.HTTPAddr)
	}
	// DB_PATH has no default; it is taken verbatim from the environment.
	if cfg.DBPath != "/data/agentops.db" {
		t.Errorf("DBPath = %q", cfg.DBPath)
	}
	if cfg.LogLevel != slog.LevelInfo {
		t.Errorf("default LogLevel = %v, want INFO", cfg.LogLevel)
	}
	// Sidecar settings default to empty/zero; the orchestrator applies its
	// own defaults ("sidecar" container, `sidecar serve` command).
	if cfg.SidecarContainer != "" || cfg.AgentContainer != "" || cfg.SidecarCommand != nil {
		t.Errorf("sidecar settings not empty by default: %q %q %v",
			cfg.SidecarContainer, cfg.AgentContainer, cfg.SidecarCommand)
	}
}

func TestLoadSidecarSettings(t *testing.T) {
	cfg, err := config.Load(envFrom(map[string]string{
		"SLACK_SIGNING_SECRET":    "sec",
		"SLACK_BOT_TOKEN":         "xoxb-1",
		"OAUTH_REDIRECT_BASE_URL": "https://app.example",
		"POD_NAMESPACE":           "agentops",
		"DB_PATH":                 "/data/db",
		"SIDECAR_COMMAND":         "/bin/sidecar serve --verbose",
		"SIDECAR_CONTAINER":       "proxy",
		"AGENT_CONTAINER":         "agent",
	}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := cfg.SidecarCommand; len(got) != 3 || got[0] != "/bin/sidecar" || got[2] != "--verbose" {
		t.Errorf("SidecarCommand = %v", got)
	}
	if cfg.SidecarContainer != "proxy" {
		t.Errorf("SidecarContainer = %q", cfg.SidecarContainer)
	}
	if cfg.AgentContainer != "agent" {
		t.Errorf("AgentContainer = %q", cfg.AgentContainer)
	}
}

// Finding #10: AGENT_COMMAND/SIDECAR_COMMAND were split with strings.Fields,
// which breaks shell-quoted arguments. A wrapper like
// `/bin/sh -c 'exec /opt/sidecar serve'` must yield three argv tokens with the
// quotes stripped, not five tokens with literal quote characters.
func TestLoadCommandQuoting(t *testing.T) {
	cfg, err := config.Load(envFrom(map[string]string{
		"SLACK_SIGNING_SECRET":    "sec",
		"SLACK_BOT_TOKEN":         "xoxb-1",
		"OAUTH_REDIRECT_BASE_URL": "https://app.example",
		"POD_NAMESPACE":           "ns",
		"DB_PATH":                 "/x.db",
		"SIDECAR_COMMAND":         `/bin/sh -c 'exec /opt/sidecar serve'`,
		"AGENT_COMMAND":           `/bin/sh -c "exec acp-agent --flag"`,
	}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	wantSidecar := []string{"/bin/sh", "-c", "exec /opt/sidecar serve"}
	if len(cfg.SidecarCommand) != len(wantSidecar) {
		t.Fatalf("SidecarCommand = %#v, want %#v", cfg.SidecarCommand, wantSidecar)
	}
	for i, w := range wantSidecar {
		if cfg.SidecarCommand[i] != w {
			t.Errorf("SidecarCommand[%d] = %q, want %q", i, cfg.SidecarCommand[i], w)
		}
	}
	wantAgent := []string{"/bin/sh", "-c", "exec acp-agent --flag"}
	if len(cfg.AgentCommand) != len(wantAgent) {
		t.Fatalf("AgentCommand = %#v, want %#v", cfg.AgentCommand, wantAgent)
	}
	for i, w := range wantAgent {
		if cfg.AgentCommand[i] != w {
			t.Errorf("AgentCommand[%d] = %q, want %q", i, cfg.AgentCommand[i], w)
		}
	}
}

func TestLoadLogLevel(t *testing.T) {
	base := map[string]string{
		"SLACK_SIGNING_SECRET":    "sec",
		"SLACK_BOT_TOKEN":         "xoxb-1",
		"OAUTH_REDIRECT_BASE_URL": "https://app.example",
		"POD_NAMESPACE":           "ns",
		"DB_PATH":                 "/x.db",
	}
	withLevel := func(v string) map[string]string {
		m := map[string]string{}
		for k, val := range base {
			m[k] = val
		}
		m["LOG_LEVEL"] = v
		return m
	}
	cfg, err := config.Load(envFrom(withLevel("debug")))
	if err != nil || cfg.LogLevel != slog.LevelDebug {
		t.Errorf("LOG_LEVEL=debug -> level=%v err=%v", cfg.LogLevel, err)
	}
	if _, err := config.Load(envFrom(withLevel("verbose"))); err == nil {
		t.Error("expected error for invalid LOG_LEVEL")
	}
}

func TestLoadRequiresDBPath(t *testing.T) {
	_, err := config.Load(envFrom(map[string]string{
		"SLACK_SIGNING_SECRET":    "sec",
		"SLACK_BOT_TOKEN":         "xoxb-1",
		"OAUTH_REDIRECT_BASE_URL": "https://app.example",
		"POD_NAMESPACE":           "ns",
		// DB_PATH intentionally omitted.
	}))
	if err == nil {
		t.Error("expected error when DB_PATH is missing (it has no default)")
	}
}

func TestLoadRequiresSecrets(t *testing.T) {
	_, err := config.Load(envFrom(map[string]string{
		"SLACK_BOT_TOKEN":         "xoxb-1",
		"OAUTH_REDIRECT_BASE_URL": "https://app.example",
		"POD_NAMESPACE":           "ns",
	}))
	if err == nil {
		t.Error("expected error when SLACK_SIGNING_SECRET is missing")
	}
}

func TestLoadRequiresRedirectURL(t *testing.T) {
	_, err := config.Load(envFrom(map[string]string{
		"SLACK_SIGNING_SECRET": "sec",
		"SLACK_BOT_TOKEN":      "xoxb-1",
		"POD_NAMESPACE":        "ns",
	}))
	if err == nil {
		t.Error("expected error when OAUTH_REDIRECT_BASE_URL is missing")
	}
}

func TestLoadOverrides(t *testing.T) {
	cfg, err := config.Load(envFrom(map[string]string{
		"SLACK_SIGNING_SECRET":    "sec",
		"SLACK_BOT_TOKEN":         "xoxb-1",
		"OAUTH_REDIRECT_BASE_URL": "https://app.example/",
		"POD_NAMESPACE":           "ns",
		"HTTP_ADDR":               ":9000",
		"DB_PATH":                 "/tmp/x.db",
		"SESSION_TTL":             "30m",
	}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.HTTPAddr != ":9000" || cfg.DBPath != "/tmp/x.db" {
		t.Errorf("overrides not applied: %+v", cfg)
	}
	if cfg.SessionTTL.Minutes() != 30 {
		t.Errorf("SESSION_TTL = %v", cfg.SessionTTL)
	}
	// Trailing slash on the redirect base URL must be trimmed.
	if cfg.OAuthRedirectBaseURL != "https://app.example" {
		t.Errorf("redirect base url = %q", cfg.OAuthRedirectBaseURL)
	}
}

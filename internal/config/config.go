// Package config holds the process configuration for agentops, loaded
// from environment variables (which a flag layer in main may override).
package config

import (
	"fmt"
	"log/slog"
	"strings"
	"time"
)

// Config is the fully-resolved process configuration.
type Config struct {
	// SlackSigningSecret verifies inbound Slack requests.
	SlackSigningSecret string
	// SlackBotToken authenticates outbound Slack API calls.
	SlackBotToken string
	// OAuthRedirectBaseURL is the externally reachable base URL; the OAuth
	// redirect URI is this + "/oauth/callback".
	OAuthRedirectBaseURL string
	// Namespace is the Kubernetes namespace the app operates in (reads
	// AgentTemplates, creates SandboxClaims).
	Namespace string
	// DBPath is the SQLite database file path (required; no default).
	DBPath string
	// HTTPAddr is the listen address for the gateway server.
	HTTPAddr string
	// SessionTTL bounds a sandbox's lifetime.
	SessionTTL time.Duration
	// StreamInterval paces how often the in-progress agent reply message may be
	// replaced in Slack (AGENT_STREAM_INTERVAL); larger values reduce flicker on
	// long turns. The turn's final output is always delivered regardless.
	StreamInterval time.Duration
	// AgentCommand is the command exec'd in the sandbox pod to start the ACP
	// agent over stdio. Empty means use the orchestrator default.
	AgentCommand []string
	// AgentContainer is the sandbox pod container the agent command targets
	// (AGENT_CONTAINER). Empty means the orchestrator default ("agent").
	AgentContainer string
	// SidecarCommand is the command exec'd in the sidecar container to start
	// the secret-isolating proxy (SIDECAR_COMMAND). Empty means use the
	// orchestrator default.
	SidecarCommand []string
	// SidecarContainer is the sandbox pod container running the sidecar proxy
	// (SIDECAR_CONTAINER). Empty means the orchestrator default ("sidecar").
	SidecarContainer string
	// LogLevel is the minimum slog level: debug, info, warn, or error. Set
	// LOG_LEVEL=debug to see the full sandbox/ACP comms trace.
	LogLevel slog.Level
}

// Load builds a Config from the given environment accessor (use os.Getenv in
// production), applying defaults and validating required fields.
func Load(getenv func(string) string) (Config, error) {
	cfg := Config{
		SlackSigningSecret:   getenv("SLACK_SIGNING_SECRET"),
		SlackBotToken:        getenv("SLACK_BOT_TOKEN"),
		OAuthRedirectBaseURL: strings.TrimRight(getenv("OAUTH_REDIRECT_BASE_URL"), "/"),
		Namespace:            firstNonEmpty(getenv("POD_NAMESPACE"), getenv("NAMESPACE")),
		DBPath:               getenv("DB_PATH"),
		HTTPAddr:             firstNonEmpty(getenv("HTTP_ADDR"), ":8080"),
		SessionTTL:           time.Hour,
		StreamInterval:       2500 * time.Millisecond,
	}
	level, err := parseLevel(firstNonEmpty(getenv("LOG_LEVEL"), "info"))
	if err != nil {
		return Config{}, err
	}
	cfg.LogLevel = level
	if v := getenv("SESSION_TTL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return Config{}, fmt.Errorf("invalid SESSION_TTL %q: %w", v, err)
		}
		cfg.SessionTTL = d
	}
	if v := getenv("AGENT_COMMAND"); v != "" {
		cfg.AgentCommand, err = splitCommand(v)
		if err != nil {
			return Config{}, fmt.Errorf("invalid AGENT_COMMAND: %w", err)
		}
	}
	cfg.AgentContainer = getenv("AGENT_CONTAINER")
	if v := getenv("SIDECAR_COMMAND"); v != "" {
		cfg.SidecarCommand, err = splitCommand(v)
		if err != nil {
			return Config{}, fmt.Errorf("invalid SIDECAR_COMMAND: %w", err)
		}
	}
	cfg.SidecarContainer = getenv("SIDECAR_CONTAINER")
	if v := getenv("AGENT_STREAM_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return Config{}, fmt.Errorf("invalid AGENT_STREAM_INTERVAL %q: %w", v, err)
		}
		cfg.StreamInterval = d
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Validate checks that required fields are present.
func (c Config) Validate() error {
	var missing []string
	if c.SlackSigningSecret == "" {
		missing = append(missing, "SLACK_SIGNING_SECRET")
	}
	if c.SlackBotToken == "" {
		missing = append(missing, "SLACK_BOT_TOKEN")
	}
	if c.OAuthRedirectBaseURL == "" {
		missing = append(missing, "OAUTH_REDIRECT_BASE_URL")
	}
	if c.Namespace == "" {
		missing = append(missing, "POD_NAMESPACE")
	}
	if c.DBPath == "" {
		missing = append(missing, "DB_PATH")
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing required configuration: %s", strings.Join(missing, ", "))
	}
	return nil
}

// splitCommand splits a command line into argv tokens with POSIX-style
// quoting: whitespace separates tokens, single quotes preserve everything
// literally, double quotes preserve everything except a backslash before " or
// \, and a bare backslash escapes the next character. This replaces a naive
// strings.Fields split so a wrapper like `/bin/sh -c 'exec foo bar'` yields the
// three tokens the operator intended rather than splitting inside the quotes.
func splitCommand(s string) ([]string, error) {
	var args []string
	var cur strings.Builder
	inToken := false
	const (
		none = iota
		single
		double
	)
	quote := none
	escaped := false

	for _, r := range s {
		switch {
		case escaped:
			cur.WriteRune(r)
			escaped = false
		case quote == single:
			if r == '\'' {
				quote = none
			} else {
				cur.WriteRune(r)
			}
		case quote == double:
			switch r {
			case '"':
				quote = none
			case '\\':
				escaped = true
			default:
				cur.WriteRune(r)
			}
		case r == '\\':
			escaped = true
			inToken = true
		case r == '\'':
			quote = single
			inToken = true
		case r == '"':
			quote = double
			inToken = true
		case r == ' ' || r == '\t' || r == '\n' || r == '\r':
			if inToken {
				args = append(args, cur.String())
				cur.Reset()
				inToken = false
			}
		default:
			cur.WriteRune(r)
			inToken = true
		}
	}
	if quote != none {
		return nil, fmt.Errorf("unterminated quote")
	}
	if escaped {
		return nil, fmt.Errorf("trailing backslash")
	}
	if inToken {
		args = append(args, cur.String())
	}
	return args, nil
}

// parseLevel maps a LOG_LEVEL string to an slog.Level.
func parseLevel(s string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("invalid LOG_LEVEL %q (want debug|info|warn|error)", s)
	}
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

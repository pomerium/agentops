package config

import (
	"cmp"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

type Config struct {
	Slack    SlackConfig
	LogLevel slog.Level
}

type SlackConfig struct {
	SigningSecret         string
	BotToken              string
	HTTPAddr              string
	StreamInterval        time.Duration
	ChannelMapPath        string
	ChannelMapInterval    time.Duration
	HarnessAPIURL         string
	HarnessAPITokenFile   string
	HarnessAPIDialAddress string
	HarnessAPICAFile      string
}

func Load(getenv func(string) string) (Config, error) {
	cfg := Config{
		Slack: SlackConfig{
			SigningSecret:  getenv("SLACK_SIGNING_SECRET"),
			BotToken:       getenv("SLACK_BOT_TOKEN"),
			HTTPAddr:       cmp.Or(getenv("HTTP_ADDR"), ":8080"),
			StreamInterval: 2500 * time.Millisecond,

			ChannelMapPath:        cmp.Or(getenv("SLACK_CHANNEL_MAP"), "/etc/agentops/channels.yaml"),
			ChannelMapInterval:    30 * time.Second,
			HarnessAPIURL:         strings.TrimRight(getenv("HARNESS_API_URL"), "/"),
			HarnessAPITokenFile:   cmp.Or(getenv("HARNESS_API_TOKEN_FILE"), "/var/run/harness-api/token"),
			HarnessAPIDialAddress: getenv("HARNESS_API_DIAL_ADDRESS"),
			HarnessAPICAFile:      getenv("HARNESS_API_CA_FILE"),
		},
	}
	level, err := parseLevel(cmp.Or(getenv("LOG_LEVEL"), "info"))
	if err != nil {
		return Config{}, err
	}
	cfg.LogLevel = level
	if v := getenv("AGENT_STREAM_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return Config{}, fmt.Errorf("invalid AGENT_STREAM_INTERVAL %q: %w", v, err)
		}
		cfg.Slack.StreamInterval = d
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	var missing []string
	if c.Slack.SigningSecret == "" {
		missing = append(missing, "SLACK_SIGNING_SECRET")
	}
	if c.Slack.BotToken == "" {
		missing = append(missing, "SLACK_BOT_TOKEN")
	}
	if c.Slack.HarnessAPIURL == "" {
		missing = append(missing, "HARNESS_API_URL")
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing required configuration: %s", strings.Join(missing, ", "))
	}
	return nil
}

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

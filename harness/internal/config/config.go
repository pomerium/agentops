package config

import (
	"cmp"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Harness  HarnessConfig
	LogLevel slog.Level
}

type HarnessConfig struct {
	Namespace            string
	DBPath               string
	SessionTTL           time.Duration
	SessionIdleTTL       time.Duration
	SessionIdleWarnLead  time.Duration
	SuspendedTTL         time.Duration
	SandboxLease         time.Duration
	AgenticASURL         string
	AgenticASDialAddress string
	AgenticTokenFile     string
	AgenticCAFile        string
	AgenticRunTTL        time.Duration
	GRPCAddr             string
	ExternalURL          string
	AssertionIssuer      string
	AssertionAudience    string
	AssertionJWKSURL     string
	AssertionDialAddress string
	AssertionCAFile      string
	AttachGrace          time.Duration
	AttachWarnAfter      time.Duration
	HeartbeatInterval    time.Duration
	HeartbeatMissLimit   uint32

	APIAddr                 string
	AdminAddr               string
	APIAssertionIssuer      string
	APIAssertionAudience    string
	APIAssertionJWKSURL     string
	APIAssertionDialAddress string
	APIAssertionCAFile      string
}

func Load(getenv func(string) string) (Config, error) {
	cfg := Config{
		Harness: HarnessConfig{
			Namespace:           cmp.Or(getenv("POD_NAMESPACE"), getenv("NAMESPACE")),
			DBPath:              getenv("DB_PATH"),
			SessionTTL:          time.Hour,
			SessionIdleTTL:      15 * time.Minute,
			SessionIdleWarnLead: 2 * time.Minute,
			SuspendedTTL:        24 * time.Hour,
			SandboxLease:        time.Hour,
			AgenticRunTTL:       15 * time.Minute,
		},
	}
	level, err := parseLevel(cmp.Or(getenv("LOG_LEVEL"), "info"))
	if err != nil {
		return Config{}, err
	}
	cfg.LogLevel = level
	if err := parseDurations(getenv, false, []durationEnv{
		{"SESSION_TTL", &cfg.Harness.SessionTTL},
		{"SESSION_IDLE_TTL", &cfg.Harness.SessionIdleTTL},
		{"SESSION_IDLE_WARN_LEAD", &cfg.Harness.SessionIdleWarnLead},
		{"SUSPENDED_TTL", &cfg.Harness.SuspendedTTL},
		{"SANDBOX_LEASE", &cfg.Harness.SandboxLease},
		{"AGENTIC_RUN_TTL", &cfg.Harness.AgenticRunTTL},
	}); err != nil {
		return Config{}, err
	}
	cfg.Harness.AgenticASURL = strings.TrimRight(getenv("AGENTIC_AS_URL"), "/")
	cfg.Harness.AgenticASDialAddress = getenv("AGENTIC_AS_DIAL_ADDRESS")
	cfg.Harness.AgenticTokenFile = cmp.Or(getenv("AGENTIC_TOKEN_FILE"), "/var/run/agentic/token")
	cfg.Harness.AgenticCAFile = getenv("AGENTIC_CA_FILE")
	if err := cfg.Harness.loadAgentLink(getenv); err != nil {
		return Config{}, err
	}
	if err := cfg.Harness.loadClientAPI(getenv); err != nil {
		return Config{}, err
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c *HarnessConfig) loadAgentLink(getenv func(string) string) error {
	c.GRPCAddr = cmp.Or(getenv("HARNESS_GRPC_ADDR"), ":8090")
	c.ExternalURL = strings.TrimRight(getenv("HARNESS_EXTERNAL_URL"), "/")
	c.AssertionIssuer = getenv("HARNESS_ASSERTION_ISSUER")
	c.AssertionJWKSURL = getenv("HARNESS_ASSERTION_JWKS_URL")
	c.AssertionDialAddress = cmp.Or(getenv("HARNESS_ASSERTION_DIAL_ADDRESS"), c.AgenticASDialAddress)
	c.AssertionCAFile = cmp.Or(getenv("HARNESS_ASSERTION_CA_FILE"), c.AgenticCAFile)
	c.AttachGrace = 2 * time.Minute
	c.AttachWarnAfter = 90 * time.Second
	c.HeartbeatInterval = 20 * time.Second
	c.HeartbeatMissLimit = 3

	c.AssertionAudience = getenv("HARNESS_ASSERTION_AUDIENCE")
	if c.AssertionAudience == "" && c.ExternalURL != "" {
		host, err := hostOf(c.ExternalURL)
		if err != nil {
			return fmt.Errorf("invalid HARNESS_EXTERNAL_URL %q", c.ExternalURL)
		}
		c.AssertionAudience = host
	}
	if err := parseDurations(getenv, true, []durationEnv{
		{"HARNESS_ATTACH_GRACE", &c.AttachGrace},
		{"HARNESS_ATTACH_WARN_AFTER", &c.AttachWarnAfter},
		{"HARNESS_HEARTBEAT_INTERVAL", &c.HeartbeatInterval},
	}); err != nil {
		return err
	}
	if v := getenv("HARNESS_HEARTBEAT_MISS_LIMIT"); v != "" {
		n, err := strconv.ParseUint(v, 10, 32)
		if err != nil || n == 0 {
			return fmt.Errorf("invalid HARNESS_HEARTBEAT_MISS_LIMIT %q (want a positive integer)", v)
		}
		c.HeartbeatMissLimit = uint32(n)
	}
	return nil
}

func (c *HarnessConfig) loadClientAPI(getenv func(string) string) error {
	c.APIAddr = cmp.Or(getenv("HARNESS_API_ADDR"), ":8081")
	c.AdminAddr = cmp.Or(getenv("HARNESS_ADMIN_ADDR"), ":9090")
	c.APIAssertionIssuer = getenv("HARNESS_API_ASSERTION_ISSUER")
	c.APIAssertionJWKSURL = getenv("HARNESS_API_ASSERTION_JWKS_URL")
	c.APIAssertionDialAddress = cmp.Or(getenv("HARNESS_API_ASSERTION_DIAL_ADDRESS"), c.AgenticASDialAddress)
	c.APIAssertionCAFile = cmp.Or(getenv("HARNESS_API_ASSERTION_CA_FILE"), c.AgenticCAFile)

	c.APIAssertionAudience = getenv("HARNESS_API_ASSERTION_AUDIENCE")
	if c.APIAssertionAudience == "" && c.APIAssertionIssuer != "" {
		host, err := hostOf(c.APIAssertionIssuer)
		if err != nil {
			return fmt.Errorf("invalid HARNESS_API_ASSERTION_ISSUER %q", c.APIAssertionIssuer)
		}
		c.APIAssertionAudience = host
	}
	if c.APIAssertionIssuer != "" && c.APIAssertionIssuer == c.AssertionIssuer {
		return fmt.Errorf("HARNESS_API_ASSERTION_ISSUER and HARNESS_ASSERTION_ISSUER are both %q: "+
			"the client API and the Agent Link must be separate Pomerium routes, "+
			"or an assertion minted for one is accepted by the other", c.AssertionIssuer)
	}
	return nil
}

func hostOf(raw string) (string, error) {
	if !strings.Contains(raw, "//") {
		raw = "https://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return "", fmt.Errorf("invalid URL %q", raw)
	}
	return u.Host, nil
}

func (c Config) Validate() error {
	var missing []string
	if c.Harness.Namespace == "" {
		missing = append(missing, "POD_NAMESPACE")
	}
	if c.Harness.DBPath == "" {
		missing = append(missing, "DB_PATH")
	}
	if c.Harness.AgenticASURL == "" {
		missing = append(missing, "AGENTIC_AS_URL")
	}
	if c.Harness.ExternalURL == "" {
		missing = append(missing, "HARNESS_EXTERNAL_URL")
	}
	if c.Harness.AssertionIssuer == "" {
		missing = append(missing, "HARNESS_ASSERTION_ISSUER")
	}
	if c.Harness.APIAssertionIssuer == "" {
		missing = append(missing, "HARNESS_API_ASSERTION_ISSUER")
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

type durationEnv struct {
	env string
	dst *time.Duration
}

func parseDurations(getenv func(string) string, positive bool, ds []durationEnv) error {
	for _, d := range ds {
		v := getenv(d.env)
		if v == "" {
			continue
		}
		parsed, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("invalid %s %q: %w", d.env, v, err)
		}
		if positive && parsed <= 0 {
			return fmt.Errorf("invalid %s %q: must be positive", d.env, v)
		}
		*d.dst = parsed
	}
	return nil
}

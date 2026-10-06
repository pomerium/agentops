package config

import (
	"maps"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func envOf(overrides map[string]string) func(string) string {
	env := map[string]string{
		"POD_NAMESPACE":                "agents",
		"DB_PATH":                      "/data/harness.db",
		"AGENTIC_AS_URL":               "https://as.example.com",
		"HARNESS_EXTERNAL_URL":         "https://link.example.com",
		"HARNESS_ASSERTION_ISSUER":     "link.example.com",
		"HARNESS_API_ASSERTION_ISSUER": "api.example.com",
	}
	maps.Copy(env, overrides)
	return func(k string) string { return env[k] }
}

func TestLoad_RequiredOnly(t *testing.T) {
	t.Parallel()
	_, err := Load(envOf(nil))
	require.NoError(t, err)
}

func TestLoad_RejectsNonPositiveRunTTL(t *testing.T) {
	t.Parallel()
	for _, v := range []string{"-3m", "0", "0s"} {
		_, err := Load(envOf(map[string]string{"AGENTIC_RUN_TTL": v}))
		assert.ErrorContains(t, err, "AGENTIC_RUN_TTL", v)
	}
}

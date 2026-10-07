package main

import (
	"maps"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func envOf(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestLoadConfig_UnknownIdentity(t *testing.T) {
	t.Parallel()
	for _, bad := range []identity{"", "run", "workload", "User-Identity"} {
		_, err := loadConfig(bad, envOf(nil))
		assert.ErrorContains(t, err, "unknown identity", bad)
	}
}

func TestLoadConfig_UserDefaults(t *testing.T) {
	t.Parallel()
	cfg, err := loadConfig(userIdentity, envOf(map[string]string{
		"SIDECAR_HARNESS_URL": "https://harness.example.com",
		envAgenticASURL:       "https://agentic.example.com",
		envAgenticCAFile:      "/ca/pomerium.pem",
		envWorkloadCAFile:     "/ca/other.pem",
	}))
	require.NoError(t, err)
	assert.True(t, cfg.managed())
	assert.Equal(t, userConfig{
		HarnessURL:    "https://harness.example.com",
		HarnessCAFile: "/ca/pomerium.pem",
		ASURL:         "https://agentic.example.com",
		TokenFile:     "/var/run/agentic/token",
		ASCAFile:      "/ca/pomerium.pem",
		RunnerSocket:  "/var/run/agentops/runner.sock",
	}, cfg.User)
}

func TestLoadConfig_UserOverrides(t *testing.T) {
	t.Parallel()
	cfg, err := loadConfig(userIdentity, envOf(map[string]string{
		"SIDECAR_HARNESS_URL":          "https://harness.example.com",
		"SIDECAR_HARNESS_DIAL_ADDRESS": "pomerium.svc:443",
		"SIDECAR_HARNESS_CA_FILE":      "/ca/harness.pem",
		"SIDECAR_HARNESS_INSECURE":     "true",
		envAgenticASURL:                "https://agentic.example.com",
		envAgenticASDialAddr:           "pomerium.svc:443",
		envAgenticTokenFile:            "/tokens/agentic",
		envAgenticCAFile:               "/ca/pomerium.pem",
		envAgenticRotation:             "5s",
		"SIDECAR_RUNNER_SOCKET":        "/tmp/runner.sock",
	}))
	require.NoError(t, err)
	assert.Equal(t, userConfig{
		HarnessURL:      "https://harness.example.com",
		HarnessDialAddr: "pomerium.svc:443",
		HarnessCAFile:   "/ca/harness.pem",
		HarnessInsecure: true,
		ASURL:           "https://agentic.example.com",
		ASDialAddr:      "pomerium.svc:443",
		TokenFile:       "/tokens/agentic",
		ASCAFile:        "/ca/pomerium.pem",
		Rotation:        5 * time.Second,
		RunnerSocket:    "/tmp/runner.sock",
	}, cfg.User)
}

func TestLoadConfig_UserRejects(t *testing.T) {
	t.Parallel()
	both := func(extra map[string]string) map[string]string {
		m := map[string]string{"SIDECAR_HARNESS_URL": "https://harness.example.com", envAgenticASURL: "https://agentic.example.com"}
		maps.Copy(m, extra)
		return m
	}
	cases := map[string]struct {
		env     map[string]string
		wantErr string
	}{
		"no urls":        {nil, "SIDECAR_HARNESS_URL is required"},
		"no as url":      {map[string]string{"SIDECAR_HARNESS_URL": "https://harness.example.com"}, "SIDECAR_AGENTIC_AS_URL is required"},
		"no harness url": {map[string]string{envAgenticASURL: "https://agentic.example.com"}, "SIDECAR_HARNESS_URL is required"},
		"bad rotation":   {both(map[string]string{envAgenticRotation: "soon"}), "invalid SIDECAR_AGENTIC_ROTATION_INTERVAL"},
		"workload token": {both(map[string]string{envWorkloadTokenFile: "/var/run/egress/token"}), "belongs to `serve workload-identity`"},
		"workload aud":   {both(map[string]string{envWorkloadAudience: "pomerium-egress"}), "belongs to `serve workload-identity`"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := loadConfig(userIdentity, envOf(tc.env))
			assert.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestLoadConfig_WorkloadDefaults(t *testing.T) {
	t.Parallel()
	cfg, err := loadConfig(workloadIdentity, envOf(nil))
	require.NoError(t, err)
	assert.False(t, cfg.managed())
	assert.Equal(t, workloadConfig{TokenFile: "/var/run/egress/token", Audience: "pomerium-egress"}, cfg.Workload)
}

func TestLoadConfig_WorkloadOverrides(t *testing.T) {
	t.Parallel()
	cfg, err := loadConfig(workloadIdentity, envOf(map[string]string{
		envWorkloadTokenFile:    "/tokens/egress",
		envWorkloadAudience:     "my-egress",
		envWorkloadCAFile:       "/ca/pomerium.pem",
		envAgenticASDialAddr:    "pomerium.svc:443",
		"SIDECAR_RUNNER_SOCKET": "/var/run/agentops/runner.sock",
	}))
	require.NoError(t, err)
	assert.Equal(t, workloadConfig{TokenFile: "/tokens/egress", Audience: "my-egress", CAFile: "/ca/pomerium.pem"}, cfg.Workload)
}

func TestLoadConfig_WorkloadRejects(t *testing.T) {
	t.Parallel()
	cases := map[string]struct {
		env     map[string]string
		wantErr string
	}{
		"harness url":     {map[string]string{"SIDECAR_HARNESS_URL": "https://harness.example.com"}, "belongs to `serve user-identity`"},
		"as url":          {map[string]string{envAgenticASURL: "https://agentic.example.com"}, "belongs to `serve user-identity`"},
		"the AS audience": {map[string]string{envWorkloadAudience: "pomerium-agentic-as"}, "agentic AS audience"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := loadConfig(workloadIdentity, envOf(tc.env))
			assert.ErrorContains(t, err, tc.wantErr)
		})
	}
}

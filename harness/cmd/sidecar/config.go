package main

import (
	"cmp"
	"errors"
	"fmt"
	"time"

	"github.com/pomerium/agentops/harness/internal/runner"
	"github.com/pomerium/agentops/harness/internal/sidecar/harnessclient"
)

type identity string

const (
	userIdentity     identity = "user-identity"
	workloadIdentity identity = "workload-identity"
)

const (
	envAgenticASURL      = "SIDECAR_AGENTIC_AS_URL"
	envAgenticASDialAddr = "SIDECAR_AGENTIC_AS_DIAL_ADDRESS"
	envAgenticTokenFile  = "SIDECAR_AGENTIC_TOKEN_FILE"
	envAgenticCAFile     = "SIDECAR_AGENTIC_CA_FILE"
	envAgenticRotation   = "SIDECAR_AGENTIC_ROTATION_INTERVAL"
	envWorkloadTokenFile = "SIDECAR_WORKLOAD_TOKEN_FILE"
	envWorkloadAudience  = "SIDECAR_WORKLOAD_AUDIENCE"
	envWorkloadCAFile    = "SIDECAR_WORKLOAD_CA_FILE"
	defaultAgenticToken  = "/var/run/agentic/token"
	defaultWorkloadToken = "/var/run/egress/token"
	defaultWorkloadAud   = "pomerium-egress"
	agenticASAudience    = "pomerium-agentic-as"
)

type config struct {
	Identity identity
	User     userConfig
	Workload workloadConfig
}

func (c config) managed() bool { return c.Identity == userIdentity }

type userConfig struct {
	HarnessURL      string
	HarnessDialAddr string
	HarnessCAFile   string
	HarnessInsecure bool
	ASURL           string
	ASDialAddr      string
	TokenFile       string
	ASCAFile        string
	Rotation        time.Duration
	RunnerSocket    string
}

type workloadConfig struct {
	TokenFile string
	Audience  string
	CAFile    string
}

func loadConfig(id identity, getenv func(string) string) (config, error) {
	switch id {
	case userIdentity:
		user, err := loadUserConfig(getenv)
		return config{Identity: id, User: user}, err
	case workloadIdentity:
		workload, err := loadWorkloadConfig(getenv)
		return config{Identity: id, Workload: workload}, err
	default:
		return config{}, fmt.Errorf("unknown identity %q: want %q or %q", id, userIdentity, workloadIdentity)
	}
}

func loadUserConfig(getenv func(string) string) (userConfig, error) {
	if err := refuse(getenv, workloadIdentity, envWorkloadTokenFile, envWorkloadAudience); err != nil {
		return userConfig{}, err
	}
	cfg := userConfig{
		HarnessURL:      getenv(harnessclient.EnvURL),
		HarnessDialAddr: getenv(harnessclient.EnvDialAddress),
		HarnessInsecure: getenv(harnessclient.EnvInsecure) == "true",
		ASURL:           getenv(envAgenticASURL),
		ASDialAddr:      getenv(envAgenticASDialAddr),
		TokenFile:       cmp.Or(getenv(envAgenticTokenFile), defaultAgenticToken),
		ASCAFile:        getenv(envAgenticCAFile),
		RunnerSocket:    cmp.Or(getenv(runner.SocketEnv), runner.DefaultSocket),
	}
	cfg.HarnessCAFile = cmp.Or(getenv(harnessclient.EnvCAFile), cfg.ASCAFile)
	for _, name := range []string{harnessclient.EnvURL, envAgenticASURL} {
		if getenv(name) == "" {
			return userConfig{}, fmt.Errorf("%s is required for %s", name, userIdentity)
		}
	}
	if v := getenv(envAgenticRotation); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return userConfig{}, fmt.Errorf("invalid %s %q: %w", envAgenticRotation, v, err)
		}
		cfg.Rotation = d
	}
	return cfg, nil
}

func loadWorkloadConfig(getenv func(string) string) (workloadConfig, error) {
	if err := refuse(getenv, userIdentity, harnessclient.EnvURL, envAgenticASURL); err != nil {
		return workloadConfig{}, err
	}
	cfg := workloadConfig{
		TokenFile: cmp.Or(getenv(envWorkloadTokenFile), defaultWorkloadToken),
		Audience:  cmp.Or(getenv(envWorkloadAudience), defaultWorkloadAud),
		CAFile:    getenv(envWorkloadCAFile),
	}
	if cfg.Audience == agenticASAudience {
		return workloadConfig{}, errors.New(envWorkloadAudience + " is the agentic AS audience; egress needs an audience of its own, e.g. " + defaultWorkloadAud)
	}
	return cfg, nil
}

func refuse(getenv func(string) string, other identity, names ...string) error {
	for _, name := range names {
		if getenv(name) != "" {
			return fmt.Errorf("%s is set, but it belongs to `serve %s`; unset it, or run that instead", name, other)
		}
	}
	return nil
}

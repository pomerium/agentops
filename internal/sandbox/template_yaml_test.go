package sandbox_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	sigsyaml "sigs.k8s.io/yaml"

	"github.com/pomerium/agentops/api/v1alpha1"
	"github.com/pomerium/agentops/internal/sandbox"
)

// The example SandboxTemplates are a contract with the orchestrator and the
// harness; these tests pin their security and naming invariants so a template
// edit can't silently reintroduce secrets into the agent container or break
// the exec container names.
//
// Four example templates exist:
//
//   - claude-code: the generic harness+sidecar pair, no git working context.
//   - pomerium-zero-claude-code: claude-code plus the baked-in working
//     context — a git-init init container with the repo URL/ref and the
//     git-credentials secretKeyRef. Since credentials and repo are defined
//     together here, the SandboxTemplate is tied to one agent template by
//     design.
//   - gstack-claude-code: like pomerium-zero-claude-code but baking a PUBLIC
//     repo — git-init carries the repo URL/ref only, no credentials Secret
//     (git-checkout runs an unauthenticated shallow fetch when GIT_TOKEN is
//     absent).
//   - google-skills-claude-code: another public-repo bake (google/skills),
//     referenced by the gcloud AgentTemplate.

func loadTemplate(t *testing.T, file string) *corev1.PodSpec {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "deploy", "examples", file))
	if err != nil {
		t.Fatalf("read template: %v", err)
	}
	// Only the pod spec matters here; unmarshal the relevant subtree to stay
	// independent of the agent-sandbox CRD Go types.
	var tmpl struct {
		Spec struct {
			EnvVarsInjectionPolicy string `json:"envVarsInjectionPolicy"`
			PodTemplate            struct {
				Spec corev1.PodSpec `json:"spec"`
			} `json:"podTemplate"`
		} `json:"spec"`
	}
	if err := sigsyaml.Unmarshal(data, &tmpl); err != nil {
		t.Fatalf("unmarshal template: %v", err)
	}
	if tmpl.Spec.EnvVarsInjectionPolicy != "Allowed" {
		t.Fatalf("envVarsInjectionPolicy = %q, want Allowed", tmpl.Spec.EnvVarsInjectionPolicy)
	}
	return &tmpl.Spec.PodTemplate.Spec
}

// templateFiles lists every example SandboxTemplate; the shared invariants
// below must hold for all of them.
var templateFiles = []string{
	"sandboxtemplate-claude-code.yaml",
	"sandboxtemplate-pomerium-zero-claude-code.yaml",
	"sandboxtemplate-gstack-claude-code.yaml",
	"sandboxtemplate-google-skills-claude-code.yaml",
}

func envByName(c *corev1.Container) map[string]corev1.EnvVar {
	m := map[string]corev1.EnvVar{}
	for _, e := range c.Env {
		m[e.Name] = e
	}
	return m
}

func TestTemplateContainerContract(t *testing.T) {
	for _, file := range templateFiles {
		t.Run(file, func(t *testing.T) {
			spec := loadTemplate(t, file)
			if len(spec.Containers) < 2 {
				t.Fatalf("template has %d containers, want agent + sidecar", len(spec.Containers))
			}
			// The names must match the claim env targeting and the
			// orchestrator's exec defaults.
			if spec.Containers[0].Name != sandbox.AgentContainerName {
				t.Errorf("containers[0].name = %q, want %q", spec.Containers[0].Name, sandbox.AgentContainerName)
			}
			if spec.Containers[1].Name != sandbox.SidecarContainerName {
				t.Errorf("containers[1].name = %q, want %q", spec.Containers[1].Name, sandbox.SidecarContainerName)
			}
		})
	}
}

func TestTemplateAgentHasNoSecrets(t *testing.T) {
	for _, file := range templateFiles {
		t.Run(file, func(t *testing.T) {
			spec := loadTemplate(t, file)
			agent := &spec.Containers[0]

			// The whole point of the sidecar: nothing in the agent container
			// may come from a Secret.
			for _, e := range agent.Env {
				if e.ValueFrom != nil {
					t.Errorf("agent env %s uses valueFrom (%+v); secrets belong in the sidecar container", e.Name, e.ValueFrom)
				}
			}
			if len(agent.EnvFrom) != 0 {
				t.Errorf("agent container uses envFrom; secrets belong in the sidecar container")
			}

			env := envByName(agent)
			if got := env["ANTHROPIC_BASE_URL"].Value; got != "http://127.0.0.1:9999" {
				t.Errorf("agent ANTHROPIC_BASE_URL = %q, want the sidecar listener http://127.0.0.1:9999", got)
			}
			// Claude Code refuses to start a turn without *some* credential
			// ("Authentication required"), even though envoy injects the real
			// key upstream. The agent therefore needs a non-secret placeholder
			// that envoy overwrites (OVERWRITE_IF_EXISTS_OR_ADD).
			placeholder, ok := env["ANTHROPIC_API_KEY"]
			if !ok || placeholder.Value == "" {
				t.Fatal("agent needs a literal placeholder ANTHROPIC_API_KEY (Claude Code requires a credential to be present; envoy replaces it upstream)")
			}
			if low := strings.ToLower(placeholder.Value); strings.HasPrefix(low, "sk-ant-") {
				t.Errorf("agent ANTHROPIC_API_KEY %q looks like a real key; it must be a placeholder", placeholder.Value)
			}
		})
	}
}

func TestTemplateSidecarHoldsTheSecret(t *testing.T) {
	for _, file := range templateFiles {
		t.Run(file, func(t *testing.T) {
			spec := loadTemplate(t, file)
			sidecar := &spec.Containers[1]
			env := envByName(sidecar)

			if got := env["SIDECAR_HTTP_ANTHROPIC_PORT"].Value; got != "9999" {
				t.Errorf("sidecar anthropic port = %q, want 9999 (must match the agent's ANTHROPIC_BASE_URL)", got)
			}
			if got := env["SIDECAR_HTTP_ANTHROPIC_UPSTREAM_URL"].Value; got != "https://api.anthropic.com" {
				t.Errorf("sidecar anthropic upstream = %q", got)
			}
			key, ok := env["SIDECAR_HTTP_ANTHROPIC_HEADER_X_API_KEY"]
			if !ok || key.ValueFrom == nil || key.ValueFrom.SecretKeyRef == nil {
				t.Fatalf("sidecar SIDECAR_HTTP_ANTHROPIC_HEADER_X_API_KEY must come from a secretKeyRef, got %+v", key)
			}
		})
	}
}

// TestGenericTemplateHasNoGitInit: the generic claude-code template defines no
// working context — git repo + credentials are what tie a SandboxTemplate to
// a specific agent template.
func TestGenericTemplateHasNoGitInit(t *testing.T) {
	spec := loadTemplate(t, "sandboxtemplate-claude-code.yaml")
	if len(spec.InitContainers) != 0 {
		t.Errorf("generic claude-code template must have no init containers, got %+v", spec.InitContainers)
	}
}

// TestAgentTemplateGitInit: the agent-specific SandboxTemplate bakes the full
// working context — repo URL/ref as plain env, credentials via secretKeyRef —
// into the git-init init container. Nothing git-related passes through
// agentops or the SandboxClaim.
func TestAgentTemplateGitInit(t *testing.T) {
	spec := loadTemplate(t, "sandboxtemplate-pomerium-zero-claude-code.yaml")

	if len(spec.InitContainers) != 1 || spec.InitContainers[0].Name != sandbox.GitInitContainerName {
		t.Fatalf("template needs exactly one init container named %q, got %+v", sandbox.GitInitContainerName, spec.InitContainers)
	}
	gitInit := &spec.InitContainers[0]

	var mountsWorkspace bool
	for _, m := range gitInit.VolumeMounts {
		if m.MountPath == "/workspace" {
			mountsWorkspace = true
		}
	}
	if !mountsWorkspace {
		t.Error("git-init must mount the workspace volume at /workspace to share the checkout with the agent")
	}

	env := envByName(gitInit)
	if env["GIT_REPO_URL"].Value == "" {
		t.Error("git-init must define GIT_REPO_URL: the repo is part of the template now, not the template")
	}
	// The token is REQUIRED: checkouts must be authenticated, and a missing
	// Secret must fail the pod rather than silently cloning unauthenticated.
	token, ok := env["GIT_TOKEN"]
	if !ok || token.ValueFrom == nil || token.ValueFrom.SecretKeyRef == nil {
		t.Fatalf("git-init GIT_TOKEN must come from a secretKeyRef, got %+v", token)
	}
	if token.ValueFrom.SecretKeyRef.Optional != nil && *token.ValueFrom.SecretKeyRef.Optional {
		t.Error("git-init GIT_TOKEN secretKeyRef must NOT be optional: checkouts must be authenticated")
	}
	for _, e := range gitInit.Env {
		if e.ValueFrom == nil && e.Value != "" && (e.Name == "GIT_TOKEN" || e.Name == "GIT_USERNAME") {
			t.Errorf("git-init env %s has a literal value; credentials must come from the Secret", e.Name)
		}
	}
}

// TestAgentTemplateSessionConfigExample: the gcloud example demonstrates
// sessionConfig; pin that it round-trips into the CRD type so an example edit
// that drifts from the schema fails here instead of at session launch.
func TestAgentTemplateSessionConfigExample(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "deploy", "examples", "agenttemplate-gcloud.yaml"))
	if err != nil {
		t.Fatalf("read example: %v", err)
	}
	var tmpl v1alpha1.AgentTemplate
	if err := sigsyaml.Unmarshal(data, &tmpl); err != nil {
		t.Fatalf("unmarshal example: %v", err)
	}
	if got := tmpl.Spec.SessionConfig["model"]; got != "sonnet" {
		t.Errorf("sessionConfig.model = %q, want sonnet (full config: %v)", got, tmpl.Spec.SessionConfig)
	}
}

// TestPublicRepoTemplateGitInit: the gstack template bakes a PUBLIC repo, so
// its git-init defines the repo URL but deliberately carries no credentials —
// no Secret may be referenced anywhere in the init container.
func TestPublicRepoTemplateGitInit(t *testing.T) {
	spec := loadTemplate(t, "sandboxtemplate-gstack-claude-code.yaml")

	if len(spec.InitContainers) != 1 || spec.InitContainers[0].Name != sandbox.GitInitContainerName {
		t.Fatalf("template needs exactly one init container named %q, got %+v", sandbox.GitInitContainerName, spec.InitContainers)
	}
	gitInit := &spec.InitContainers[0]

	var mountsWorkspace bool
	for _, m := range gitInit.VolumeMounts {
		if m.MountPath == "/workspace" {
			mountsWorkspace = true
		}
	}
	if !mountsWorkspace {
		t.Error("git-init must mount the workspace volume at /workspace to share the checkout with the agent")
	}

	env := envByName(gitInit)
	if env["GIT_REPO_URL"].Value == "" {
		t.Error("git-init must define GIT_REPO_URL: the repo is part of the template now, not the template")
	}
	for _, e := range gitInit.Env {
		if e.ValueFrom != nil {
			t.Errorf("git-init env %s uses valueFrom (%+v); a public-repo template must not reference any Secret", e.Name, e.ValueFrom)
		}
	}
	if _, ok := env["GIT_TOKEN"]; ok {
		t.Error("git-init must not define GIT_TOKEN: this template checks out a public repo unauthenticated")
	}
}

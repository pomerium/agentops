package sandbox_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	sigsyaml "sigs.k8s.io/yaml"

	"github.com/pomerium/agentops/harness/apis/v1alpha1"
	"github.com/pomerium/agentops/harness/internal/sandbox"
)

func loadTemplate(t *testing.T, file string) *corev1.PodSpec {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "deploy", "examples", file))
	if err != nil {
		t.Fatalf("read template: %v", err)
	}

	var tmpl struct {
		Metadata struct {
			Annotations map[string]string `json:"annotations"`
		} `json:"metadata"`
		Spec struct {
			PodTemplate struct {
				Spec corev1.PodSpec `json:"spec"`
			} `json:"podTemplate"`
		} `json:"spec"`
	}
	if err := sigsyaml.Unmarshal(data, &tmpl); err != nil {
		t.Fatalf("unmarshal template: %v", err)
	}
	if got := tmpl.Metadata.Annotations["agents.pomerium.com/inject"]; got != "true" {
		t.Fatalf("agents.pomerium.com/inject = %q, want \"true\": without it the Component adds no sidecar", got)
	}
	return &tmpl.Spec.PodTemplate.Spec
}

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
			if len(spec.Containers) != 1 || spec.Containers[0].Name != sandbox.AgentContainerName {
				t.Fatalf("template must hold exactly one container, %q; the Component adds %q", sandbox.AgentContainerName, sandbox.SidecarContainerName)
			}
		})
	}
}

func TestTemplateAgentHasNoSecrets(t *testing.T) {
	for _, file := range templateFiles {
		t.Run(file, func(t *testing.T) {
			spec := loadTemplate(t, file)
			agent := &spec.Containers[0]

			for _, e := range agent.Env {
				if e.ValueFrom != nil {
					t.Errorf("agent env %s uses valueFrom (%+v); the agent container holds no secret, the Pomerium route adds the key", e.Name, e.ValueFrom)
				}
			}
			if len(agent.EnvFrom) != 0 {
				t.Errorf("agent container uses envFrom; the agent container holds no secret, the Pomerium route adds the key")
			}
			if key := envByName(agent)["ANTHROPIC_API_KEY"].Value; strings.HasPrefix(strings.ToLower(key), "sk-ant-") {
				t.Errorf("agent ANTHROPIC_API_KEY %q looks like a real key", key)
			}
		})
	}
}

func TestTemplateTranscriptSurvivesThePod(t *testing.T) {
	for _, file := range templateFiles {
		t.Run(file, func(t *testing.T) {
			spec, claimed := loadTemplateWithClaims(t, file)
			agent := &spec.Containers[0]

			dir := envByName(agent)["CLAUDE_CONFIG_DIR"].Value
			if dir == "" {
				t.Fatal("agent must set CLAUDE_CONFIG_DIR: without it the agent writes its transcript to HOME, on the container's ephemeral layer, and the conversation dies with the pod")
			}

			mount := mountFor(agent, dir)
			if mount == nil {
				t.Fatalf("CLAUDE_CONFIG_DIR %q is not inside any of the agent's volume mounts %+v", dir, agent.VolumeMounts)
			}
			if !claimed[mount.Name] {
				t.Errorf("CLAUDE_CONFIG_DIR %q sits on volume %q, which has no volumeClaimTemplate; the transcript must be on a PVC that survives suspend/resume", dir, mount.Name)
			}
			if mount.ReadOnly {
				t.Errorf("CLAUDE_CONFIG_DIR %q sits on read-only mount %q; the agent has to write its transcript there", dir, mount.Name)
			}
		})
	}
}

func mountFor(c *corev1.Container, dir string) *corev1.VolumeMount {
	var best *corev1.VolumeMount
	for i := range c.VolumeMounts {
		m := &c.VolumeMounts[i]
		if dir != m.MountPath && !strings.HasPrefix(dir, strings.TrimSuffix(m.MountPath, "/")+"/") {
			continue
		}
		if best == nil || len(m.MountPath) > len(best.MountPath) {
			best = m
		}
	}
	return best
}

func loadTemplateWithClaims(t *testing.T, file string) (*corev1.PodSpec, map[string]bool) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "deploy", "examples", file))
	if err != nil {
		t.Fatalf("read template: %v", err)
	}
	var tmpl struct {
		Spec struct {
			VolumeClaimTemplates []corev1.PersistentVolumeClaim `json:"volumeClaimTemplates"`
		} `json:"spec"`
	}
	if err := sigsyaml.Unmarshal(data, &tmpl); err != nil {
		t.Fatalf("unmarshal template: %v", err)
	}
	claimed := map[string]bool{}
	for _, vct := range tmpl.Spec.VolumeClaimTemplates {
		claimed[vct.Name] = true
	}
	return loadTemplate(t, file), claimed
}

func TestGenericTemplateHasNoGitInit(t *testing.T) {
	spec := loadTemplate(t, "sandboxtemplate-claude-code.yaml")
	if len(spec.InitContainers) != 0 {
		t.Errorf("generic claude-code template must have no init containers, got %+v", spec.InitContainers)
	}
}

func TestAgentTemplateGitInit(t *testing.T) {
	spec := loadTemplate(t, "sandboxtemplate-pomerium-zero-claude-code.yaml")

	if len(spec.InitContainers) != 1 || spec.InitContainers[0].Name != sandbox.GitInitContainerName {
		t.Fatalf("template needs exactly one init container named %q, got %+v", sandbox.GitInitContainerName, spec.InitContainers)
	}
	gitInit := &spec.InitContainers[0]

	if err := sharedWorkspaceError(spec, gitInit); err != nil {
		t.Error(err)
	}

	env := envByName(gitInit)
	if env["GIT_REPO_URL"].Value == "" {
		t.Error("git-init must define GIT_REPO_URL: the repo is part of the template now, not the template")
	}

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

func TestAgentTemplateSessionConfigExample(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "deploy", "examples", "agenttemplate-gcloud.yaml"))
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

func TestPublicRepoTemplateGitInit(t *testing.T) {
	spec := loadTemplate(t, "sandboxtemplate-gstack-claude-code.yaml")

	if len(spec.InitContainers) != 1 || spec.InitContainers[0].Name != sandbox.GitInitContainerName {
		t.Fatalf("template needs exactly one init container named %q, got %+v", sandbox.GitInitContainerName, spec.InitContainers)
	}
	gitInit := &spec.InitContainers[0]

	if err := sharedWorkspaceError(spec, gitInit); err != nil {
		t.Error(err)
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

func TestSharedWorkspaceRejectsACheckoutTheAgentCannotSee(t *testing.T) {
	spec := func(gitVolume string, readOnly bool) (*corev1.PodSpec, *corev1.Container) {
		s := &corev1.PodSpec{
			InitContainers: []corev1.Container{{
				Name:         sandbox.GitInitContainerName,
				VolumeMounts: []corev1.VolumeMount{{Name: gitVolume, MountPath: "/workspace", ReadOnly: readOnly}},
			}},
			Containers: []corev1.Container{{
				Name:         sandbox.AgentContainerName,
				VolumeMounts: []corev1.VolumeMount{{Name: "workspace", MountPath: "/workspace"}},
			}},
		}
		return s, &s.InitContainers[0]
	}
	if s, g := spec("workspace", false); sharedWorkspaceError(s, g) != nil {
		t.Errorf("shared writable workspace rejected: %v", sharedWorkspaceError(s, g))
	}
	if s, g := spec("separate", false); sharedWorkspaceError(s, g) == nil {
		t.Error("a git-init workspace on another volume was accepted")
	}
	if s, g := spec("workspace", true); sharedWorkspaceError(s, g) == nil {
		t.Error("a read-only git-init workspace was accepted")
	}
}

func sharedWorkspaceError(spec *corev1.PodSpec, gitInit *corev1.Container) error {
	mountAt := func(c *corev1.Container) *corev1.VolumeMount {
		for i := range c.VolumeMounts {
			if c.VolumeMounts[i].MountPath == "/workspace" {
				return &c.VolumeMounts[i]
			}
		}
		return nil
	}
	var agent *corev1.Container
	for i := range spec.Containers {
		if spec.Containers[i].Name == sandbox.AgentContainerName {
			agent = &spec.Containers[i]
		}
	}
	if agent == nil {
		return fmt.Errorf("template has no %q container", sandbox.AgentContainerName)
	}
	want := mountAt(agent)
	got := mountAt(gitInit)
	switch {
	case want == nil:
		return fmt.Errorf("the %q container must mount the workspace at /workspace", sandbox.AgentContainerName)
	case got == nil:
		return fmt.Errorf("git-init must mount the workspace at /workspace")
	case got.Name != want.Name:
		return fmt.Errorf("git-init mounts volume %q at /workspace but the agent mounts %q: the agent would not see the checkout", got.Name, want.Name)
	case got.ReadOnly:
		return fmt.Errorf("git-init mounts the workspace read-only and cannot check out into it")
	}
	return nil
}

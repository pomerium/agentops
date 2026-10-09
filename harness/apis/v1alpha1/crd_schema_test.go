package v1alpha1_test

import (
	"os"
	"path/filepath"
	"testing"

	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	sigsyaml "sigs.k8s.io/yaml"
)

func repoFile(t *testing.T, parts ...string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(append([]string{"..", "..", ".."}, parts...)...))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func agentTemplateValidator(t *testing.T) validation.SchemaValidator {
	t.Helper()
	var crd apiextensionsv1.CustomResourceDefinition
	if err := sigsyaml.Unmarshal(repoFile(t, "config", "crd", "bases", "agents.pomerium.com_agenttemplates.yaml"), &crd); err != nil {
		t.Fatal(err)
	}
	var schema apiextensions.JSONSchemaProps
	if err := apiextensionsv1.Convert_v1_JSONSchemaProps_To_apiextensions_JSONSchemaProps(
		crd.Spec.Versions[0].Schema.OpenAPIV3Schema, &schema, nil); err != nil {
		t.Fatal(err)
	}
	v, _, err := validation.NewSchemaValidator(&schema)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func validate(t *testing.T, v validation.SchemaValidator, doc []byte) field.ErrorList {
	t.Helper()
	var obj map[string]any
	if err := sigsyaml.Unmarshal(doc, &obj); err != nil {
		t.Fatal(err)
	}
	return validation.ValidateCustomResource(nil, obj, v)
}

func TestAnAgentTemplateMCPServerMustUseHTTPS(t *testing.T) {
	v := agentTemplateValidator(t)
	for url, wantValid := range map[string]bool{
		"https://mcp.example.com/mcp": true,
		"http://mcp.example.com/mcp":  false,
		"mcp.example.com/mcp":         false,
	} {
		doc := []byte(`
apiVersion: agents.pomerium.com/v1alpha1
kind: AgentTemplate
metadata: {name: t}
spec:
  warmPoolRef: {name: claude-code}
  requiredMCPServers:
    - name: example
      url: ` + url + `
`)
		errs := validate(t, v, doc)
		if valid := len(errs) == 0; valid != wantValid {
			t.Errorf("url %q: valid = %v, want %v (errors: %v)", url, valid, wantValid, errs)
		}
	}
}

func TestTheShippedAgentTemplatesMatchTheSchema(t *testing.T) {
	v := agentTemplateValidator(t)
	for _, file := range []string{
		"deploy/sandbox/agenttemplate.yaml",
		"deploy/examples/agenttemplate-deploy-service.yaml",
		"deploy/examples/agenttemplate-gcloud.yaml",
		"deploy/examples/agenttemplate-gstack.yaml",
	} {
		if errs := validate(t, v, repoFile(t, filepath.FromSlash(file))); len(errs) > 0 {
			t.Errorf("%s: %v", file, errs)
		}
	}
}

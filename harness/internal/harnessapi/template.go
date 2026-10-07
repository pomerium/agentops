package harnessapi

import (
	"strings"

	v1alpha1 "github.com/pomerium/agentops/harness/apis/v1alpha1"
	"github.com/pomerium/agentops/harness/internal/sandbox"
)

func runIdentityEndpoints(tmpl *v1alpha1.AgentTemplate) []sandbox.ProxiedEndpoint {
	servers := tmpl.Spec.RequiredMCPServers
	if len(servers) == 0 {
		return nil
	}
	eps := make([]sandbox.ProxiedEndpoint, 0, len(servers))
	for i, srv := range servers {
		eps = append(eps, sandbox.ProxiedEndpoint{
			Name:        srv.Name,
			ListenPort:  sandbox.MCPListenPort(i),
			UpstreamURL: srv.URL,
			DialAddress: srv.DialAddress,
		})
	}
	return eps
}

func mcpServerURLs(tmpl *v1alpha1.AgentTemplate) []string {
	seen := map[string]bool{}
	var servers []string
	for _, s := range tmpl.Spec.RequiredMCPServers {
		if s.URL == "" || seen[s.URL] {
			continue
		}
		seen[s.URL] = true
		servers = append(servers, s.URL)
	}
	return servers
}

func composeSystemPrompt(templatePrompt, appendix string) string {
	templatePrompt = strings.TrimRight(templatePrompt, "\n")
	appendix = strings.TrimRight(appendix, "\n")
	switch {
	case appendix == "":
		return templatePrompt
	case templatePrompt == "":
		return appendix
	default:
		return templatePrompt + "\n\n" + appendix
	}
}

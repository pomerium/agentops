package sandbox

import (
	"fmt"
	"net/url"

	agentlinkpb "github.com/pomerium/agentops/harness/internal/agentlink/pb"
)

const MCPPortBase uint32 = 9100

func MCPListenPort(i int) uint32 { return MCPPortBase + uint32(i) }

const mcpEndpointPrefix = "mcp-"

func SidecarEndpointName(name string) string { return mcpEndpointPrefix + name }

type ProxiedEndpoint struct {
	Name        string
	ListenPort  uint32
	UpstreamURL string
	DialAddress string
}

func RewriteMCPServers(endpoints []ProxiedEndpoint) []*agentlinkpb.McpServer {
	servers := make([]*agentlinkpb.McpServer, 0, len(endpoints))
	for _, ep := range endpoints {
		localURL := fmt.Sprintf("http://127.0.0.1:%d", ep.ListenPort)
		if u, err := url.Parse(ep.UpstreamURL); err == nil {
			localURL += u.RequestURI()
			if u.Path == "" && u.RawQuery == "" {
				localURL = fmt.Sprintf("http://127.0.0.1:%d", ep.ListenPort)
			}
		}
		servers = append(servers, &agentlinkpb.McpServer{Name: ep.Name, Url: localURL})
	}
	return servers
}

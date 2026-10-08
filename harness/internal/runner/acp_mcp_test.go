package runner

import (
	"encoding/json"
	"strings"
	"testing"

	agentlinkpb "github.com/pomerium/agentops/harness/internal/agentlink/pb"
)

func TestMCPServersSendAnEmptyHeaderList(t *testing.T) {
	servers := mcpServers([]*agentlinkpb.McpServer{{Name: "agno", Url: "http://127.0.0.1:9100/mcp"}})
	if len(servers) != 1 || servers[0].Http == nil {
		t.Fatalf("servers = %+v, want one HTTP server", servers)
	}
	raw, err := json.Marshal(servers[0])
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(raw), `"headers":[]`) || strings.Contains(string(raw), `"headers":null`) {
		t.Errorf("server = %s, want an empty headers array", raw)
	}
	if !strings.Contains(string(raw), `"url":"http://127.0.0.1:9100/mcp"`) || !strings.Contains(string(raw), `"type":"http"`) {
		t.Errorf("server = %s, want the loopback URL over http", raw)
	}
}

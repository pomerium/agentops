package sandbox

import (
	"encoding/json"
	"strings"
	"testing"

	acp "github.com/coder/acp-go-sdk"
)

func TestNormalizeMCPHeaders(t *testing.T) {
	servers := []acp.McpServer{
		{Http: &acp.McpServerHttpInline{Name: "agno", Type: "http", Url: "https://docs.agno.com/mcp"}},
		{Sse: &acp.McpServerSseInline{Name: "events", Type: "sse", Url: "https://example.com/sse"}},
	}

	if raw, _ := json.Marshal(servers[0]); !strings.Contains(string(raw), `"headers":null`) {
		t.Fatalf("precondition: expected nil headers to marshal as null, got %s", raw)
	}

	normalizeMCPHeaders(servers)

	if servers[0].Http.Headers == nil || servers[1].Sse.Headers == nil {
		t.Fatal("normalizeMCPHeaders left a nil Headers slice")
	}
	for _, s := range servers {
		raw, err := json.Marshal(s)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if strings.Contains(string(raw), `"headers":null`) {
			t.Errorf("headers still marshal to null: %s", raw)
		}
		if !strings.Contains(string(raw), `"headers":[]`) {
			t.Errorf("expected empty headers array, got %s", raw)
		}
	}
}

func TestNormalizeMCPHeadersPreservesExisting(t *testing.T) {
	servers := []acp.McpServer{{
		Http: &acp.McpServerHttpInline{
			Name: "github", Type: "http", Url: "https://github-mcp.example.com/mcp",
			Headers: []acp.HttpHeader{{Name: "Authorization", Value: "Bearer t"}},
		},
	}}

	normalizeMCPHeaders(servers)

	if got := servers[0].Http.Headers; len(got) != 1 || got[0].Name != "Authorization" || got[0].Value != "Bearer t" {
		t.Fatalf("existing headers were modified: %+v", got)
	}
}

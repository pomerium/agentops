package envparse_test

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/pomerium/agentops/internal/sidecar/envparse"
)

func TestParse(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		environ []string
		want    []envparse.Endpoint
		wantErr string
	}{
		{
			name:    "empty environment",
			environ: []string{"PATH=/usr/bin", "HOME=/root"},
			want:    nil,
		},
		{
			name: "single endpoint with headers",
			environ: []string{
				"SIDECAR_HTTP_ANTHROPIC_PORT=9999",
				"SIDECAR_HTTP_ANTHROPIC_UPSTREAM_URL=https://api.anthropic.com",
				"SIDECAR_HTTP_ANTHROPIC_HEADER_X_API_KEY=sk-secret",
				"PATH=/usr/bin",
			},
			want: []envparse.Endpoint{{
				Name:        "anthropic",
				ListenPort:  9999,
				UpstreamURL: "https://api.anthropic.com",
				Headers:     map[string]string{"x-api-key": "sk-secret"},
			}},
		},
		{
			name: "multiple headers and authorization",
			environ: []string{
				"SIDECAR_HTTP_API_PORT=9000",
				"SIDECAR_HTTP_API_UPSTREAM_URL=https://example.com",
				"SIDECAR_HTTP_API_HEADER_AUTHORIZATION=Bearer tok",
				"SIDECAR_HTTP_API_HEADER_X_CUSTOM_THING=v",
			},
			want: []envparse.Endpoint{{
				Name:        "api",
				ListenPort:  9000,
				UpstreamURL: "https://example.com",
				Headers: map[string]string{
					"authorization":  "Bearer tok",
					"x-custom-thing": "v",
				},
			}},
		},
		{
			name: "name with underscore",
			environ: []string{
				"SIDECAR_HTTP_MY_API_PORT=9001",
				"SIDECAR_HTTP_MY_API_UPSTREAM_URL=http://internal:8080",
			},
			want: []envparse.Endpoint{{
				Name:        "my_api",
				ListenPort:  9001,
				UpstreamURL: "http://internal:8080",
				Headers:     map[string]string{},
			}},
		},
		{
			name: "multiple endpoints sorted by name",
			environ: []string{
				"SIDECAR_HTTP_ZETA_PORT=9002",
				"SIDECAR_HTTP_ZETA_UPSTREAM_URL=http://z:1",
				"SIDECAR_HTTP_ALPHA_PORT=9003",
				"SIDECAR_HTTP_ALPHA_UPSTREAM_URL=http://a:1",
			},
			want: []envparse.Endpoint{
				{Name: "alpha", ListenPort: 9003, UpstreamURL: "http://a:1", Headers: map[string]string{}},
				{Name: "zeta", ListenPort: 9002, UpstreamURL: "http://z:1", Headers: map[string]string{}},
			},
		},
		{
			name: "missing port",
			environ: []string{
				"SIDECAR_HTTP_API_UPSTREAM_URL=https://example.com",
			},
			wantErr: "PORT",
		},
		{
			name: "missing upstream url",
			environ: []string{
				"SIDECAR_HTTP_API_PORT=9000",
			},
			wantErr: "UPSTREAM_URL",
		},
		{
			name: "invalid port",
			environ: []string{
				"SIDECAR_HTTP_API_PORT=not-a-port",
				"SIDECAR_HTTP_API_UPSTREAM_URL=https://example.com",
			},
			wantErr: "port",
		},
		{
			name: "port out of range",
			environ: []string{
				"SIDECAR_HTTP_API_PORT=70000",
				"SIDECAR_HTTP_API_UPSTREAM_URL=https://example.com",
			},
			wantErr: "port",
		},
		{
			name: "invalid upstream scheme",
			environ: []string{
				"SIDECAR_HTTP_API_PORT=9000",
				"SIDECAR_HTTP_API_UPSTREAM_URL=ftp://example.com",
			},
			wantErr: "scheme",
		},
		{
			name: "header without endpoint",
			environ: []string{
				"SIDECAR_HTTP_API_HEADER_AUTHORIZATION=Bearer tok",
			},
			wantErr: "PORT",
		},
		{
			name: "duplicate listen ports across endpoints",
			environ: []string{
				"SIDECAR_HTTP_A_PORT=9000",
				"SIDECAR_HTTP_A_UPSTREAM_URL=http://a:1",
				"SIDECAR_HTTP_B_PORT=9000",
				"SIDECAR_HTTP_B_UPSTREAM_URL=http://b:1",
			},
			wantErr: "port",
		},
		{
			name: "malformed key missing suffix",
			environ: []string{
				"SIDECAR_HTTP_API=oops",
			},
			wantErr: "SIDECAR_HTTP_API",
		},
		{
			// Finding #9: a NAME containing the reserved _HEADER_ segment is
			// silently mis-split — SIDECAR_HTTP_MY_HEADER_SVC_* parses as headers
			// "svc-port"/"svc-upstream-url" on a phantom endpoint "my" rather than
			// an endpoint "my_header_svc". The result is a header-only partial
			// with no PORT/URL of its own; reject it with a message that names the
			// _HEADER_ cause instead of a generic "missing PORT".
			name: "name containing reserved _HEADER_ segment",
			environ: []string{
				"SIDECAR_HTTP_MY_HEADER_SVC_PORT=9443",
				"SIDECAR_HTTP_MY_HEADER_SVC_UPSTREAM_URL=https://svc.internal",
			},
			wantErr: "_HEADER_",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := envparse.Parse(tt.environ)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("Parse() = %v, want error containing %q", got, tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error %q does not contain %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("Parse() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

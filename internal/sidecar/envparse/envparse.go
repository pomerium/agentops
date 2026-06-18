// Package envparse extracts proxy endpoint definitions from SIDECAR_HTTP_*
// environment variables on the sidecar container. This lets the sandbox
// template route secrets (e.g. the Anthropic API key) straight into the
// sidecar via secretKeyRef without ever passing through agentops or
// the agent container.
//
// Grammar (NAME is matched case-sensitively and lowercased in the result):
//
//	SIDECAR_HTTP_<NAME>_PORT=9999
//	SIDECAR_HTTP_<NAME>_UPSTREAM_URL=https://api.example.com
//	SIDECAR_HTTP_<NAME>_DIAL_ADDRESS=host[:port]   (optional)
//	SIDECAR_HTTP_<NAME>_HEADER_<HEADER_NAME>=value
//
// Header names map underscores to dashes (X_API_KEY → x-api-key), so NAME
// itself must not contain the literal segment "_HEADER_".
package envparse

import (
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
)

const prefix = "SIDECAR_HTTP_"

const (
	suffixPort        = "_PORT"
	suffixUpstreamURL = "_UPSTREAM_URL"
	suffixDialAddress = "_DIAL_ADDRESS"
	infixHeader       = "_HEADER_"
)

// Endpoint is one localhost-listener → upstream mapping parsed from the
// environment. Headers may hold secrets: never log them.
type Endpoint struct {
	Name        string
	ListenPort  uint32
	UpstreamURL string
	DialAddress string
	Headers     map[string]string
}

// Parse scans environ (os.Environ() form, "KEY=value") for SIDECAR_HTTP_*
// variables and assembles complete endpoints, sorted by name. It returns an
// error on incomplete endpoints, malformed keys or values, and duplicate
// listen ports. Error messages never include variable values, which may be
// secrets.
func Parse(environ []string) ([]Endpoint, error) {
	type partial struct {
		port    string
		hasPort bool
		url     string
		hasURL  bool
		dial    string
		headers map[string]string
	}
	partials := map[string]*partial{}
	get := func(name string) *partial {
		p, ok := partials[name]
		if !ok {
			p = &partial{headers: map[string]string{}}
			partials[name] = p
		}
		return p
	}

	for _, kv := range environ {
		key, value, ok := strings.Cut(kv, "=")
		if !ok || !strings.HasPrefix(key, prefix) {
			continue
		}
		rest := strings.TrimPrefix(key, prefix)
		switch {
		case strings.Contains(rest, infixHeader):
			name, hdr, _ := strings.Cut(rest, infixHeader)
			if name == "" || hdr == "" {
				return nil, fmt.Errorf("malformed sidecar env var %q", key)
			}
			headerName := strings.ToLower(strings.ReplaceAll(hdr, "_", "-"))
			get(name).headers[headerName] = value
		case strings.HasSuffix(rest, suffixUpstreamURL):
			name := strings.TrimSuffix(rest, suffixUpstreamURL)
			if name == "" {
				return nil, fmt.Errorf("malformed sidecar env var %q", key)
			}
			p := get(name)
			p.url, p.hasURL = value, true
		case strings.HasSuffix(rest, suffixDialAddress):
			name := strings.TrimSuffix(rest, suffixDialAddress)
			if name == "" {
				return nil, fmt.Errorf("malformed sidecar env var %q", key)
			}
			get(name).dial = value
		case strings.HasSuffix(rest, suffixPort):
			name := strings.TrimSuffix(rest, suffixPort)
			if name == "" {
				return nil, fmt.Errorf("malformed sidecar env var %q", key)
			}
			p := get(name)
			p.port, p.hasPort = value, true
		default:
			return nil, fmt.Errorf("unrecognized sidecar env var %q: expected %s<NAME>%s, %s<NAME>%s, %s<NAME>%s, or %s<NAME>%s<HEADER>",
				key, prefix, suffixPort, prefix, suffixUpstreamURL, prefix, suffixDialAddress, prefix, infixHeader)
		}
	}

	if len(partials) == 0 {
		return nil, nil
	}
	endpoints := make([]Endpoint, 0, len(partials))
	for name, p := range partials {
		// A partial assembled only from _HEADER_ vars (no PORT, no URL of its
		// own) is the signature of a NAME that contains the reserved _HEADER_
		// segment: SIDECAR_HTTP_MY_HEADER_SVC_PORT mis-splits into a "svc-port"
		// header on a phantom endpoint "my". Reject it with an actionable message
		// rather than the generic missing-PORT error below.
		if !p.hasPort && !p.hasURL && len(p.headers) > 0 {
			return nil, fmt.Errorf("sidecar endpoint %q has only header vars and no %s/%s; endpoint names must not contain the reserved %q segment", name, suffixPort, suffixUpstreamURL, infixHeader)
		}
		if !p.hasPort {
			return nil, fmt.Errorf("sidecar endpoint %q: missing %s%s%s", name, prefix, name, suffixPort)
		}
		if !p.hasURL {
			return nil, fmt.Errorf("sidecar endpoint %q: missing %s%s%s", name, prefix, name, suffixUpstreamURL)
		}
		port, err := strconv.ParseUint(p.port, 10, 16)
		if err != nil || port == 0 {
			return nil, fmt.Errorf("sidecar endpoint %q: invalid listen port", name)
		}
		u, err := url.Parse(p.url)
		if err != nil {
			return nil, fmt.Errorf("sidecar endpoint %q: invalid upstream URL: %w", name, err)
		}
		if u.Scheme != "http" && u.Scheme != "https" {
			return nil, fmt.Errorf("sidecar endpoint %q: upstream URL scheme must be http or https", name)
		}
		if u.Host == "" {
			return nil, fmt.Errorf("sidecar endpoint %q: upstream URL has no host", name)
		}
		endpoints = append(endpoints, Endpoint{
			Name:        strings.ToLower(name),
			ListenPort:  uint32(port),
			UpstreamURL: p.url,
			DialAddress: p.dial,
			Headers:     p.headers,
		})
	}

	sort.Slice(endpoints, func(i, j int) bool { return endpoints[i].Name < endpoints[j].Name })

	seen := map[uint32]string{}
	for _, ep := range endpoints {
		if other, dup := seen[ep.ListenPort]; dup {
			return nil, fmt.Errorf("sidecar endpoints %q and %q share listen port %d", other, ep.Name, ep.ListenPort)
		}
		seen[ep.ListenPort] = ep.Name
	}
	return endpoints, nil
}

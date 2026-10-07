package envparse

import (
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
)

const (
	prefix               = "SIDECAR_HTTP_"
	suffixPort           = "_PORT"
	suffixUpstreamURL    = "_UPSTREAM_URL"
	suffixDialAddress    = "_DIAL_ADDRESS"
	suffixInjectRunToken = "_INJECT_RUN_TOKEN"
	infixHeader          = "_HEADER_"
)

type Endpoint struct {
	Name           string
	ListenPort     uint32
	UpstreamURL    string
	DialAddress    string
	Headers        map[string]string
	InjectRunToken bool
}

func Parse(environ []string) ([]Endpoint, error) {
	type partial struct {
		port           string
		hasPort        bool
		url            string
		hasURL         bool
		dial           string
		injectRunToken bool
		headers        map[string]string
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
		case strings.HasSuffix(rest, suffixInjectRunToken):
			name := strings.TrimSuffix(rest, suffixInjectRunToken)
			if name == "" {
				return nil, fmt.Errorf("malformed sidecar env var %q", key)
			}
			b, err := strconv.ParseBool(value)
			if err != nil {
				return nil, fmt.Errorf("sidecar endpoint %q: invalid %s value (want true/false)", name, suffixInjectRunToken)
			}
			get(name).injectRunToken = b
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
			Name:           strings.ToLower(name),
			ListenPort:     uint32(port),
			UpstreamURL:    p.url,
			DialAddress:    p.dial,
			Headers:        p.headers,
			InjectRunToken: p.injectRunToken,
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

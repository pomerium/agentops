package server

import (
	"context"
	"fmt"
	"sort"

	"github.com/pomerium/agentops/harness/internal/sidecar/envoyconfig"
	"github.com/pomerium/agentops/harness/internal/sidecar/envparse"
)

type Process interface {
	Exited() <-chan error
	Stop()
}

type Proxy interface {
	Start(ctx context.Context, endpoints []envoyconfig.Endpoint) (Process, error)
}

func EndpointsFromEnv(eps []envparse.Endpoint) []envoyconfig.Endpoint {
	out := make([]envoyconfig.Endpoint, 0, len(eps))
	for _, ep := range eps {
		out = append(out, endpointFromEnv(ep))
	}
	return out
}

func Endpoints(sets ...[]envoyconfig.Endpoint) ([]envoyconfig.Endpoint, error) {
	byName := map[string]bool{}
	byPort := map[uint32]string{}
	var out []envoyconfig.Endpoint
	for _, set := range sets {
		for _, ep := range set {
			if byName[ep.Name] {
				return nil, fmt.Errorf("duplicate endpoint name %q", ep.Name)
			}
			if other, taken := byPort[ep.ListenPort]; taken {
				return nil, fmt.Errorf("endpoint %q: listen port %d already used by %q", ep.Name, ep.ListenPort, other)
			}
			byName[ep.Name] = true
			byPort[ep.ListenPort] = ep.Name
			out = append(out, ep)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func endpointFromEnv(ep envparse.Endpoint) envoyconfig.Endpoint {
	return envoyconfig.Endpoint{
		Name:           ep.Name,
		ListenPort:     ep.ListenPort,
		UpstreamURL:    ep.UpstreamURL,
		DialAddress:    ep.DialAddress,
		Headers:        ep.Headers,
		InjectRunToken: ep.InjectRunToken,
	}
}

// Package envoyconfig renders the sidecar's endpoint list into a fully static
// envoy bootstrap: one loopback listener per endpoint proxying every request
// to its upstream with secret headers injected. There is no xDS — config
// changes mean a new bootstrap and a new envoy process.
//
// The shape mirrors pomerium's config/envoyconfig, reduced to the static
// subset this sidecar needs.
package envoyconfig

import (
	"fmt"
	"net"
	"net/url"
	"sort"
	"strconv"
	"time"

	bootstrapv3 "github.com/envoyproxy/go-control-plane/envoy/config/bootstrap/v3"
	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpointv3 "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	listenerv3 "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	routerv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/router/v3"
	hcmv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	tlsv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/transport_sockets/tls/v3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
)

// systemCABundle is the CA bundle the upstream TLS contexts validate against.
// The envoy distroless base image ships the system roots at this path; an
// empty validation context would disable verification entirely (envoy only
// verifies when a trust store is configured), exposing the injected credential
// headers to a MITM.
const systemCABundle = "/etc/ssl/certs/ca-certificates.crt"

// Endpoint is one loopback-listener → upstream mapping. Headers may hold
// secrets: never log them.
type Endpoint struct {
	Name        string
	ListenPort  uint32
	UpstreamURL string
	// DialAddress optionally overrides the host[:port] envoy connects to, while
	// SNI and the Host header still derive from UpstreamURL. Empty means dial
	// the host:port parsed from UpstreamURL.
	DialAddress string
	Headers     map[string]string
}

// BuildBootstrap renders endpoints into a static envoy bootstrap. The admin
// interface binds a unix-domain socket at adminSocket (on the sidecar's own
// filesystem) rather than a loopback TCP port: the agent container shares the
// pod network namespace, so a TCP admin port would let it read the injected
// secret headers via /config_dump or kill the proxy via /quitquitquit.
func BuildBootstrap(endpoints []Endpoint, adminSocket string) (*bootstrapv3.Bootstrap, error) {
	listeners := make([]*listenerv3.Listener, 0, len(endpoints))
	clusters := make([]*clusterv3.Cluster, 0, len(endpoints))

	seenNames := map[string]bool{}
	seenPorts := map[uint32]bool{}
	for _, ep := range endpoints {
		if ep.Name == "" {
			return nil, fmt.Errorf("endpoint with listen port %d has no name", ep.ListenPort)
		}
		if ep.ListenPort == 0 || ep.ListenPort > 65535 {
			return nil, fmt.Errorf("endpoint %q: invalid listen port %d", ep.Name, ep.ListenPort)
		}
		if seenNames[ep.Name] {
			return nil, fmt.Errorf("duplicate endpoint name %q", ep.Name)
		}
		if seenPorts[ep.ListenPort] {
			return nil, fmt.Errorf("endpoint %q: listen port %d already in use", ep.Name, ep.ListenPort)
		}
		seenNames[ep.Name] = true
		seenPorts[ep.ListenPort] = true

		upstream, err := parseUpstream(ep)
		if err != nil {
			return nil, err
		}
		listener, err := buildListener(ep, upstream)
		if err != nil {
			return nil, err
		}
		cluster, err := buildCluster(ep, upstream)
		if err != nil {
			return nil, err
		}
		listeners = append(listeners, listener)
		clusters = append(clusters, cluster)
	}

	return &bootstrapv3.Bootstrap{
		Node: &corev3.Node{Id: "sidecar", Cluster: "sidecar"},
		Admin: &bootstrapv3.Admin{
			Address: pipeAddress(adminSocket),
		},
		StaticResources: &bootstrapv3.Bootstrap_StaticResources{
			Listeners: listeners,
			Clusters:  clusters,
		},
	}, nil
}

type upstream struct {
	host string
	port uint32
	tls  bool
	// dialHost/dialPort are the address envoy actually connects to. They default
	// to host/port and are overridden by Endpoint.DialAddress so the sandbox can
	// reach an in-cluster Service while host/port (SNI + Host header) stay public.
	dialHost string
	dialPort uint32
}

// authority is the value for the upstream Host header. Per HTTP convention the
// port is omitted only for the scheme's default port (80/443); a host-routing
// gateway listening on any other port expects the port in the Host header, so
// it must be preserved.
func (u upstream) authority() string {
	if (u.tls && u.port == 443) || (!u.tls && u.port == 80) {
		return u.host
	}
	return net.JoinHostPort(u.host, strconv.FormatUint(uint64(u.port), 10))
}

func parseUpstream(ep Endpoint) (upstream, error) {
	u, err := url.Parse(ep.UpstreamURL)
	if err != nil {
		return upstream{}, fmt.Errorf("endpoint %q: invalid upstream URL: %w", ep.Name, err)
	}
	if u.Hostname() == "" {
		return upstream{}, fmt.Errorf("endpoint %q: upstream URL has no host", ep.Name)
	}
	up := upstream{host: u.Hostname()}
	switch u.Scheme {
	case "http":
		up.port = 80
	case "https":
		up.port, up.tls = 443, true
	default:
		return upstream{}, fmt.Errorf("endpoint %q: upstream URL scheme must be http or https", ep.Name)
	}
	if p := u.Port(); p != "" {
		port, err := parsePort(p)
		if err != nil {
			return upstream{}, fmt.Errorf("endpoint %q: invalid upstream port %q", ep.Name, p)
		}
		up.port = port
	}

	// The dial target defaults to the public host:port; DialAddress overrides it
	// without touching SNI/authority. A bare host inherits the URL's port.
	up.dialHost, up.dialPort = up.host, up.port
	if ep.DialAddress != "" {
		host, portStr, err := net.SplitHostPort(ep.DialAddress)
		if err != nil {
			// No port component: treat the whole value as a host, keep the URL port.
			up.dialHost = ep.DialAddress
		} else {
			port, err := parsePort(portStr)
			if err != nil {
				return upstream{}, fmt.Errorf("endpoint %q: invalid dial address port %q", ep.Name, portStr)
			}
			up.dialHost, up.dialPort = host, port
		}
		if up.dialHost == "" {
			return upstream{}, fmt.Errorf("endpoint %q: dial address has no host", ep.Name)
		}
	}
	return up, nil
}

func parsePort(s string) (uint32, error) {
	port, err := strconv.ParseUint(s, 10, 16)
	if err != nil || port == 0 {
		return 0, fmt.Errorf("invalid port %q", s)
	}
	return uint32(port), nil
}

func clusterName(ep Endpoint) string { return "ep-" + ep.Name }

func buildListener(ep Endpoint, up upstream) (*listenerv3.Listener, error) {
	// Explicit zero timeouts: the defaults (15s route timeout) would kill SSE
	// MCP streams and streaming LLM responses mid-flight.
	action := &routev3.RouteAction{
		ClusterSpecifier: &routev3.RouteAction_Cluster{Cluster: clusterName(ep)},
		HostRewriteSpecifier: &routev3.RouteAction_HostRewriteLiteral{
			HostRewriteLiteral: up.authority(),
		},
		Timeout:     durationpb.New(0),
		IdleTimeout: durationpb.New(0),
	}

	route := &routev3.Route{
		Match: &routev3.RouteMatch{
			PathSpecifier: &routev3.RouteMatch_Prefix{Prefix: "/"},
		},
		Action:              &routev3.Route_Route{Route: action},
		RequestHeadersToAdd: buildHeaders(ep.Headers),
	}

	// The router filter is the terminal filter actually forwarding requests;
	// envoy accepts an HCM without it and then hangs every request.
	routerAny, err := anypb.New(&routerv3.Router{})
	if err != nil {
		return nil, fmt.Errorf("endpoint %q: marshal router filter: %w", ep.Name, err)
	}

	hcm := &hcmv3.HttpConnectionManager{
		CodecType:  hcmv3.HttpConnectionManager_AUTO,
		StatPrefix: ep.Name,
		HttpFilters: []*hcmv3.HttpFilter{{
			Name:       "envoy.filters.http.router",
			ConfigType: &hcmv3.HttpFilter_TypedConfig{TypedConfig: routerAny},
		}},
		RouteSpecifier: &hcmv3.HttpConnectionManager_RouteConfig{
			RouteConfig: &routev3.RouteConfiguration{
				Name: clusterName(ep),
				VirtualHosts: []*routev3.VirtualHost{{
					Name:    clusterName(ep),
					Domains: []string{"*"},
					Routes:  []*routev3.Route{route},
				}},
			},
		},
	}
	hcmAny, err := anypb.New(hcm)
	if err != nil {
		return nil, fmt.Errorf("endpoint %q: marshal HCM: %w", ep.Name, err)
	}

	return &listenerv3.Listener{
		Name:    "ep-" + ep.Name,
		Address: socketAddress("127.0.0.1", ep.ListenPort),
		FilterChains: []*listenerv3.FilterChain{{
			Filters: []*listenerv3.Filter{{
				Name:       "envoy.filters.network.http_connection_manager",
				ConfigType: &listenerv3.Filter_TypedConfig{TypedConfig: hcmAny},
			}},
		}},
	}, nil
}

// buildHeaders renders the injected headers in sorted order so the generated
// bootstrap is deterministic.
func buildHeaders(headers map[string]string) []*corev3.HeaderValueOption {
	if len(headers) == 0 {
		return nil
	}
	names := make([]string, 0, len(headers))
	for name := range headers {
		names = append(names, name)
	}
	sort.Strings(names)
	opts := make([]*corev3.HeaderValueOption, 0, len(names))
	for _, name := range names {
		opts = append(opts, &corev3.HeaderValueOption{
			Header:       &corev3.HeaderValue{Key: name, Value: headers[name]},
			AppendAction: corev3.HeaderValueOption_OVERWRITE_IF_EXISTS_OR_ADD,
		})
	}
	return opts
}

func buildCluster(ep Endpoint, up upstream) (*clusterv3.Cluster, error) {
	cluster := &clusterv3.Cluster{
		Name:                 clusterName(ep),
		ClusterDiscoveryType: &clusterv3.Cluster_Type{Type: clusterv3.Cluster_LOGICAL_DNS},
		DnsLookupFamily:      clusterv3.Cluster_V4_PREFERRED,
		ConnectTimeout:       durationpb.New(10 * time.Second),
		LoadAssignment: &endpointv3.ClusterLoadAssignment{
			ClusterName: clusterName(ep),
			Endpoints: []*endpointv3.LocalityLbEndpoints{{
				LbEndpoints: []*endpointv3.LbEndpoint{{
					HostIdentifier: &endpointv3.LbEndpoint_Endpoint{
						Endpoint: &endpointv3.Endpoint{
							Address: socketAddress(up.dialHost, up.dialPort),
						},
					},
				}},
			}},
		},
	}

	if up.tls {
		tlsCtx := &tlsv3.UpstreamTlsContext{
			Sni: up.host,
			CommonTlsContext: &tlsv3.CommonTlsContext{
				ValidationContextType: &tlsv3.CommonTlsContext_ValidationContext{
					// An empty context disables verification (envoy only verifies
					// when a trust store is set); validate against the image's
					// system CA bundle so credential headers can't leak to a MITM.
					ValidationContext: &tlsv3.CertificateValidationContext{
						TrustedCa: &corev3.DataSource{
							Specifier: &corev3.DataSource_Filename{Filename: systemCABundle},
						},
					},
				},
			},
		}
		tlsAny, err := anypb.New(proto.Message(tlsCtx))
		if err != nil {
			return nil, fmt.Errorf("endpoint %q: marshal TLS context: %w", ep.Name, err)
		}
		cluster.TransportSocket = &corev3.TransportSocket{
			Name:       "envoy.transport_sockets.tls",
			ConfigType: &corev3.TransportSocket_TypedConfig{TypedConfig: tlsAny},
		}
	}
	return cluster, nil
}

// pipeAddress is a unix-domain socket address. Mode 0 lets envoy create the
// socket with default permissions; it lives on the sidecar's filesystem, which
// the agent container does not share.
func pipeAddress(path string) *corev3.Address {
	return &corev3.Address{
		Address: &corev3.Address_Pipe{
			Pipe: &corev3.Pipe{Path: path},
		},
	}
}

func socketAddress(host string, port uint32) *corev3.Address {
	return &corev3.Address{
		Address: &corev3.Address_SocketAddress{
			SocketAddress: &corev3.SocketAddress{
				Address:       host,
				PortSpecifier: &corev3.SocketAddress_PortValue{PortValue: port},
			},
		},
	}
}

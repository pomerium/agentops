package pomeriumtls

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"
)

const dialTimeout = 10 * time.Second

func TLSConfig(caFile, serverName string) (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: serverName}
	if caFile == "" {
		return cfg, nil
	}
	pemBytes, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read CA file %q: %w", caFile, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		return nil, fmt.Errorf("no certificates found in CA file %q", caFile)
	}
	cfg.RootCAs = pool
	return cfg, nil
}

func Dial(ctx context.Context, network, dialAddr string) (net.Conn, error) {
	return (&net.Dialer{Timeout: dialTimeout}).DialContext(ctx, network, dialAddr)
}

func Transport(dialAddr, caFile string) (*http.Transport, error) {
	tlsCfg, err := TLSConfig(caFile, "")
	if err != nil {
		return nil, err
	}
	transport := &http.Transport{TLSClientConfig: tlsCfg}
	if dialAddr != "" {
		transport.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
			return Dial(ctx, network, dialAddr)
		}
	}
	return transport, nil
}

package pomeriumtls

import (
	"crypto/tls"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTLSConfig(t *testing.T) {
	t.Parallel()
	cfg, err := TLSConfig("", "example.com")
	require.NoError(t, err)
	assert.Equal(t, uint16(tls.VersionTLS12), cfg.MinVersion)
	assert.Equal(t, "example.com", cfg.ServerName)
	assert.Nil(t, cfg.RootCAs)

	_, err = TLSConfig(filepath.Join(t.TempDir(), "missing.pem"), "")
	assert.ErrorContains(t, err, "read CA file")

	empty := filepath.Join(t.TempDir(), "empty.pem")
	require.NoError(t, os.WriteFile(empty, []byte("not a cert"), 0o600))
	_, err = TLSConfig(empty, "")
	assert.ErrorContains(t, err, "no certificates found")
}

func TestTransportDialsOverrideWithPrivateCA(t *testing.T) {
	t.Parallel()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	}))
	t.Cleanup(srv.Close)

	caFile := filepath.Join(t.TempDir(), "ca.pem")
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	require.NoError(t, os.WriteFile(caFile, ca, 0o600))

	transport, err := Transport(srv.Listener.Addr().String(), caFile)
	require.NoError(t, err)
	resp, err := (&http.Client{Transport: transport}).Get("https://example.com/")
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusTeapot, resp.StatusCode)
}

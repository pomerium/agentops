package envoyconfig

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	credinjv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/credential_injector/v3"
	hcmv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	genericv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/http/injected_credentials/generic/v3"
	tlsv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/transport_sockets/tls/v3"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
)

const RunTokenSecretName = "run_token"

func hasAuthorizationHeader(headers map[string]string) bool {
	for name := range headers {
		if strings.EqualFold(name, "authorization") {
			return true
		}
	}
	return false
}

func credentialInjectorFilter(sdsPath string) (*hcmv3.HttpFilter, error) {
	generic := &genericv3.Generic{
		Credential: &tlsv3.SdsSecretConfig{
			Name: RunTokenSecretName,
			SdsConfig: &corev3.ConfigSource{
				ConfigSourceSpecifier: &corev3.ConfigSource_PathConfigSource{
					PathConfigSource: &corev3.PathConfigSource{
						Path:             sdsPath,
						WatchedDirectory: &corev3.WatchedDirectory{Path: filepath.Dir(sdsPath)},
					},
				},
			},
		},
	}
	genericAny, err := anypb.New(generic)
	if err != nil {
		return nil, fmt.Errorf("marshal generic credential: %w", err)
	}

	injector := &credinjv3.CredentialInjector{
		Overwrite:                     true,
		AllowRequestWithoutCredential: false,
		Credential: &corev3.TypedExtensionConfig{
			Name:        "envoy.http.injected_credentials.generic",
			TypedConfig: genericAny,
		},
	}
	injectorAny, err := anypb.New(injector)
	if err != nil {
		return nil, fmt.Errorf("marshal credential_injector: %w", err)
	}
	return &hcmv3.HttpFilter{
		Name:       "envoy.filters.http.credential_injector",
		ConfigType: &hcmv3.HttpFilter_TypedConfig{TypedConfig: injectorAny},
	}, nil
}

func RenderRunTokenSecret(bearer string) ([]byte, error) {
	secret := &tlsv3.Secret{
		Name: RunTokenSecretName,
		Type: &tlsv3.Secret_GenericSecret{
			GenericSecret: &tlsv3.GenericSecret{
				Secret: &corev3.DataSource{
					Specifier: &corev3.DataSource_InlineString{InlineString: bearer},
				},
			},
		},
	}
	secretAny, err := anypb.New(secret)
	if err != nil {
		return nil, fmt.Errorf("marshal run_token secret: %w", err)
	}
	resp := &discoveryv3.DiscoveryResponse{Resources: []*anypb.Any{secretAny}}
	data, err := protojson.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("marshal SDS response: %w", err)
	}
	return data, nil
}

func WriteRunTokenSecret(sdsPath, bearer string) error {
	data, err := RenderRunTokenSecret(bearer)
	if err != nil {
		return err
	}
	watchedDir := filepath.Dir(sdsPath)
	tmp, err := os.CreateTemp(filepath.Dir(watchedDir), ".run_token-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp secret file: %w", err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temp secret file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp secret file: %w", err)
	}
	if err := os.Rename(tmpName, sdsPath); err != nil {
		return fmt.Errorf("rename secret into watched directory: %w", err)
	}
	return nil
}

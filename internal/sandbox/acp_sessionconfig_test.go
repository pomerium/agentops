package sandbox

import (
	"strings"
	"testing"

	acp "github.com/coder/acp-go-sdk"
)

func selectOption(id string) acp.SessionConfigOption {
	return acp.SessionConfigOption{Select: &acp.SessionConfigOptionSelect{
		Id: acp.SessionConfigId(id), Name: id, Type: "select",
	}}
}

func boolOption(id string) acp.SessionConfigOption {
	return acp.SessionConfigOption{Boolean: &acp.SessionConfigOptionBoolean{
		Id: acp.SessionConfigId(id), Name: id, Type: "boolean",
	}}
}

const testSessionID = acp.SessionId("sess-1")

func TestSessionConfigRequests_SelectOption(t *testing.T) {
	reqs, err := sessionConfigRequests(testSessionID,
		[]acp.SessionConfigOption{selectOption("model")},
		map[string]string{"model": "opus"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(reqs) != 1 {
		t.Fatalf("got %d requests, want 1", len(reqs))
	}
	v := reqs[0].ValueId
	if v == nil {
		t.Fatalf("request is not the ValueId variant: %#v", reqs[0])
	}
	if v.ConfigId != "model" || v.SessionId != testSessionID || v.Value != "opus" {
		t.Errorf("ValueId request = %+v, want configId=model sessionId=%s value=opus", v, testSessionID)
	}
}

func TestSessionConfigRequests_BooleanOption(t *testing.T) {
	reqs, err := sessionConfigRequests(testSessionID,
		[]acp.SessionConfigOption{boolOption("verbose")},
		map[string]string{"verbose": "true"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(reqs) != 1 {
		t.Fatalf("got %d requests, want 1", len(reqs))
	}
	b := reqs[0].Boolean
	if b == nil {
		t.Fatalf("request is not the Boolean variant: %#v", reqs[0])
	}
	if b.ConfigId != "verbose" || b.SessionId != testSessionID || b.Value != true || b.Type != "boolean" {
		t.Errorf("Boolean request = %+v, want configId=verbose value=true type=boolean", b)
	}
}

// A configured id the harness does not advertise must fail the launch — a
// session running with silently unapplied config would mislead the template
// author. The error must name the bad id and list what IS advertised, so the
// YAML can be fixed from the Slack error alone.
func TestSessionConfigRequests_UnknownIDIsError(t *testing.T) {
	_, err := sessionConfigRequests(testSessionID,
		[]acp.SessionConfigOption{selectOption("model"), boolOption("verbose")},
		map[string]string{"frobnicate": "on"})
	if err == nil {
		t.Fatal("expected error for unadvertised option id")
	}
	for _, want := range []string{"frobnicate", "model", "verbose"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

func TestSessionConfigRequests_NoAdvertisedOptionsIsError(t *testing.T) {
	_, err := sessionConfigRequests(testSessionID, nil, map[string]string{"model": "opus"})
	if err == nil {
		t.Fatal("expected error when agent advertises no config options")
	}
}

func TestSessionConfigRequests_BadBooleanValueIsError(t *testing.T) {
	_, err := sessionConfigRequests(testSessionID,
		[]acp.SessionConfigOption{boolOption("verbose")},
		map[string]string{"verbose": "yep"})
	if err == nil {
		t.Fatal("expected error for unparseable boolean value")
	}
	if !strings.Contains(err.Error(), "verbose") {
		t.Errorf("error %q does not name the option id", err)
	}
}

// The adapter rebuilds dependent options (e.g. effort) when the model changes,
// so "model" must be applied before everything else; the rest is sorted for
// determinism.
func TestSessionConfigRequests_ModelAppliedFirst(t *testing.T) {
	reqs, err := sessionConfigRequests(testSessionID,
		[]acp.SessionConfigOption{selectOption("effort"), selectOption("mode"), selectOption("model")},
		map[string]string{"mode": "default", "effort": "high", "model": "opus"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var order []string
	for _, r := range reqs {
		order = append(order, string(r.ValueId.ConfigId))
	}
	want := []string{"model", "effort", "mode"}
	if len(order) != len(want) {
		t.Fatalf("got order %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("got order %v, want %v", order, want)
		}
	}
}

func TestSessionConfigRequests_EmptyConfigIsNoop(t *testing.T) {
	for _, cfg := range []map[string]string{nil, {}} {
		reqs, err := sessionConfigRequests(testSessionID, nil, cfg)
		if err != nil {
			t.Fatalf("unexpected error for empty config: %v", err)
		}
		if len(reqs) != 0 {
			t.Fatalf("got %d requests for empty config, want 0", len(reqs))
		}
	}
}

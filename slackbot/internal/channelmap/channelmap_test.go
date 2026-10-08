package channelmap_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/pomerium/agentops/slackbot/internal/channelmap"
)

func write(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "channels.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write map: %v", err)
	}
	return path
}

func TestTemplateFor(t *testing.T) {
	ctx := context.Background()
	m, err := channelmap.New(write(t, "channels:\n  C1: deploy\n  C2: support\n"), nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	for channel, want := range map[string]string{"C1": "deploy", "C2": "support"} {
		got, err := m.TemplateFor(ctx, channel)
		if err != nil {
			t.Fatalf("TemplateFor(%s): %v", channel, err)
		}
		if got != want {
			t.Errorf("TemplateFor(%s) = %q, want %q", channel, got, want)
		}
	}
	if _, err := m.TemplateFor(ctx, "C9"); !errors.Is(err, channelmap.ErrChannelNotBound) {
		t.Errorf("an unbound channel: got %v, want ErrChannelNotBound", err)
	}
}

func TestUnnamedChannelsAreUnboundWithoutADefault(t *testing.T) {
	ctx := context.Background()
	m, err := channelmap.New(write(t, "channels:\n  C1: deploy\n"), nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := m.TemplateFor(ctx, "C-other"); !errors.Is(err, channelmap.ErrChannelNotBound) {
		t.Errorf("got %v, want ErrChannelNotBound", err)
	}

	withDefault, err := channelmap.New(write(t, "default: general\nchannels:\n  C1: deploy\n"), nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	got, err := withDefault.TemplateFor(ctx, "C-other")
	if err != nil {
		t.Fatalf("TemplateFor with a default: %v", err)
	}
	if got != "general" {
		t.Errorf("TemplateFor with a default = %q, want general", got)
	}
}

func TestMissingFileBindsNothing(t *testing.T) {
	ctx := context.Background()
	m, err := channelmap.New(filepath.Join(t.TempDir(), "absent.yaml"), nil)
	if err != nil {
		t.Fatalf("New on a missing file: %v", err)
	}
	if _, err := m.TemplateFor(ctx, "C1"); !errors.Is(err, channelmap.ErrChannelNotBound) {
		t.Errorf("got %v, want ErrChannelNotBound", err)
	}
}

func TestABadEditKeepsThePreviousMap(t *testing.T) {
	ctx := context.Background()
	path := write(t, "channels:\n  C1: deploy\n")
	m, err := channelmap.New(path, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := os.WriteFile(path, []byte("channels: [this is not a map\n"), 0o600); err != nil {
		t.Fatalf("write bad map: %v", err)
	}
	if _, err := channelmap.New(path, nil); err == nil {
		t.Fatal("an unparseable map should fail to load")
	}
	got, err := m.TemplateFor(ctx, "C1")
	if err != nil || got != "deploy" {
		t.Errorf("after a bad edit: got %q/%v, want the previous binding", got, err)
	}
}

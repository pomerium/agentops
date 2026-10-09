package channelmap

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestARemovedFileUnbindsEveryChannel(t *testing.T) {
	path := filepath.Join(t.TempDir(), "channels.yaml")
	if err := os.WriteFile(path, []byte("default: general\nchannels:\n  C1: deploy\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	m, err := New(path, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := m.reload(); err != nil {
		t.Fatalf("reload of a removed file: %v", err)
	}
	for _, id := range []string{"C1", "C-other"} {
		if name, err := m.TemplateFor(context.Background(), id); !errors.Is(err, ErrChannelNotBound) {
			t.Errorf("%s after the file was removed: got %q/%v, want ErrChannelNotBound", id, name, err)
		}
	}
}

func TestABadEditKeepsThePreviousMap(t *testing.T) {
	path := filepath.Join(t.TempDir(), "channels.yaml")
	if err := os.WriteFile(path, []byte("channels:\n  C1: deploy\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	m, err := New(path, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := os.WriteFile(path, []byte("channels: [this is not a map\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := m.reload(); err == nil {
		t.Fatal("reload of an unparseable map should fail")
	}
	if got, err := m.TemplateFor(context.Background(), "C1"); err != nil || got != "deploy" {
		t.Errorf("after a bad edit: got %q/%v, want the previous binding", got, err)
	}
}

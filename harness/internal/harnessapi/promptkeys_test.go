package harnessapi

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPromptKeysRepeatWaitsForTheFirst(t *testing.T) {
	ctx := context.Background()
	p := newPromptKeys()
	release := make(chan struct{})
	var starts atomic.Int32
	start := func() (string, error) {
		starts.Add(1)
		<-release
		return "t1", nil
	}

	results := make([]string, 5)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		results[0], _ = p.do(ctx, "s1", "k", start)
	}()
	for starts.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	for i := 1; i < len(results); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i], _ = p.do(ctx, "s1", "k", start)
		}()
	}
	close(release)
	wg.Wait()

	if n := starts.Load(); n != 1 {
		t.Errorf("one key started %d prompts", n)
	}
	for i, got := range results {
		if got != "t1" {
			t.Errorf("call %d got turn %q, want the first prompt's t1", i, got)
		}
	}
}

func TestPromptKeysForgetARefusal(t *testing.T) {
	ctx := context.Background()
	p := newPromptKeys()
	refused := errors.New("refused")
	if _, err := p.do(ctx, "s1", "k", func() (string, error) { return "", refused }); !errors.Is(err, refused) {
		t.Fatalf("do = %v, want the refusal", err)
	}
	turn, err := p.do(ctx, "s1", "k", func() (string, error) { return "t2", nil })
	if err != nil || turn != "t2" {
		t.Errorf("retry after a refusal = (%q, %v), want a fresh decision (t2, nil)", turn, err)
	}
}

func TestPromptKeysExpireAndArePerSession(t *testing.T) {
	ctx := context.Background()
	p := newPromptKeys()
	now := time.Unix(1_700_000_000, 0)
	p.now = func() time.Time { return now }
	turn := func(id string) func() (string, error) {
		return func() (string, error) { return id, nil }
	}

	if got, _ := p.do(ctx, "s1", "k", turn("t1")); got != "t1" {
		t.Fatalf("first prompt got %q", got)
	}
	now = now.Add(promptKeyTTL - time.Second)
	if got, _ := p.do(ctx, "s1", "k", turn("t2")); got != "t1" {
		t.Errorf("a repeat inside the TTL got %q, want t1", got)
	}
	if got, _ := p.do(ctx, "s2", "k", turn("t3")); got != "t3" {
		t.Errorf("the same key on another session got %q, want its own turn t3", got)
	}
	now = now.Add(2 * time.Second)
	if got, _ := p.do(ctx, "s1", "k", turn("t4")); got != "t4" {
		t.Errorf("a repeat past the TTL got %q, want a new turn t4", got)
	}
}

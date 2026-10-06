package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/pomerium/agentops/harness/internal/sessionstore"
	sqlcgen "github.com/pomerium/agentops/harness/internal/sessionstore/sqlite/sqlc"
)

func TestIsUniqueViolation(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if err := s.CreateSession(ctx, sessionstore.Session{ID: "a", ClientID: "c", ConversationRef: "one"}); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	for name, params := range map[string]sqlcgen.CreateSessionParams{
		"live conversation": {ID: "b", ClientID: "c", ConversationRef: "one", Status: "pending"},
		"primary key":       {ID: "a", ClientID: "c", ConversationRef: "two", Status: "ended"},
	} {
		err := s.q.CreateSession(ctx, params)
		if !isUniqueViolation(err) {
			t.Errorf("%s: isUniqueViolation(%v) = false", name, err)
		}
	}

	_, err = s.db.ExecContext(ctx, "INSERT INTO session_events (session_id, seq, event_type) VALUES ('a', 1, NULL)")
	if err == nil || isUniqueViolation(err) {
		t.Errorf("NOT NULL failure %v classified as a unique violation", err)
	}
	if isUniqueViolation(nil) || isUniqueViolation(errors.New("boom")) {
		t.Error("isUniqueViolation matched a non-constraint error")
	}
}

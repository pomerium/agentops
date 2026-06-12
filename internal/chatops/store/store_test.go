package store_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/pomerium/agentops/internal/chatops/store"
)

func newTestStore(t *testing.T) *store.Store {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	s, err := store.Open(context.Background(), dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestOpenRunsMigrations(t *testing.T) {
	// Opening twice against the same path must be idempotent (migrations
	// already applied).
	dbPath := filepath.Join(t.TempDir(), "test.db")
	ctx := context.Background()
	s1, err := store.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	_ = s1.Close()
	s2, err := store.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	_ = s2.Close()
}

func TestMCPCredentialRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	exp := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	cred := store.MCPCredential{
		SlackUserID:  "U123",
		ServerName:   "github",
		ServerURL:    "https://gh.example/mcp",
		AccessToken:  "access-1",
		RefreshToken: "refresh-1",
		TokenType:    "Bearer",
		Expiry:       exp,
		AuthURL:      "https://auth.example/authorize",
		TokenURL:     "https://auth.example/token",
		ClientID:     "client-1",
		ClientSecret: "secret-1",
		Scopes:       "read write",
	}
	if err := s.SaveMCPCredential(ctx, cred); err != nil {
		t.Fatalf("SaveMCPCredential: %v", err)
	}

	got, err := s.GetMCPCredential(ctx, "U123", "github")
	if err != nil {
		t.Fatalf("GetMCPCredential: %v", err)
	}
	if got.AccessToken != "access-1" || got.RefreshToken != "refresh-1" {
		t.Errorf("token mismatch: %+v", got)
	}
	if !got.Expiry.Equal(exp) {
		t.Errorf("expiry mismatch: got %v want %v", got.Expiry, exp)
	}
	if got.ClientID != "client-1" || got.TokenURL != "https://auth.example/token" {
		t.Errorf("registration fields mismatch: %+v", got)
	}

	// Upsert: update the access token, keep the key.
	cred.AccessToken = "access-2"
	if err := s.SaveMCPCredential(ctx, cred); err != nil {
		t.Fatalf("SaveMCPCredential (update): %v", err)
	}
	got, err = s.GetMCPCredential(ctx, "U123", "github")
	if err != nil {
		t.Fatalf("GetMCPCredential after update: %v", err)
	}
	if got.AccessToken != "access-2" {
		t.Errorf("upsert did not update access token: %q", got.AccessToken)
	}

	list, err := s.ListMCPCredentials(ctx, "U123")
	if err != nil {
		t.Fatalf("ListMCPCredentials: %v", err)
	}
	if len(list) != 1 {
		t.Errorf("expected 1 credential, got %d", len(list))
	}

	if err := s.DeleteMCPCredential(ctx, "U123", "github"); err != nil {
		t.Fatalf("DeleteMCPCredential: %v", err)
	}
	if _, err := s.GetMCPCredential(ctx, "U123", "github"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("expected ErrNotFound after delete, got %v", err)
	}
}

func TestGetMCPCredentialNotFound(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	if _, err := s.GetMCPCredential(ctx, "nobody", "nothing"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestNoExpiryStoredAsZero(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	cred := store.MCPCredential{
		SlackUserID: "U1", ServerName: "s1", ServerURL: "u",
		AccessToken: "a",
		// Expiry left zero -> "never expires"
	}
	if err := s.SaveMCPCredential(ctx, cred); err != nil {
		t.Fatalf("SaveMCPCredential: %v", err)
	}
	got, err := s.GetMCPCredential(ctx, "U1", "s1")
	if err != nil {
		t.Fatalf("GetMCPCredential: %v", err)
	}
	if !got.Expiry.IsZero() {
		t.Errorf("expected zero expiry, got %v", got.Expiry)
	}
}

func TestOAuthFlowRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	flow := store.OAuthFlow{
		FlowID:         "flow-1",
		State:          "state-abc",
		SlackUserID:    "U123",
		SlackChannelID: "C1",
		SlackThreadTS:  "168.001",
		ServerName:     "github",
		ServerURL:      "https://gh.example/mcp",
		AuthURL:        "https://auth.example/authorize",
		TokenURL:       "https://auth.example/token",
		ClientID:       "client-1",
		PKCEVerifier:   "verifier-xyz",
		RedirectURI:    "https://app.example/oauth/callback",
		ExpiresAt:      time.Now().Add(10 * time.Minute).UTC().Truncate(time.Second),
	}
	if err := s.CreateOAuthFlow(ctx, flow); err != nil {
		t.Fatalf("CreateOAuthFlow: %v", err)
	}

	byState, err := s.GetOAuthFlowByState(ctx, "state-abc")
	if err != nil {
		t.Fatalf("GetOAuthFlowByState: %v", err)
	}
	if byState.FlowID != "flow-1" || byState.PKCEVerifier != "verifier-xyz" {
		t.Errorf("flow mismatch: %+v", byState)
	}

	byID, err := s.GetOAuthFlow(ctx, "flow-1")
	if err != nil {
		t.Fatalf("GetOAuthFlow: %v", err)
	}
	if byID.State != "state-abc" {
		t.Errorf("flow-by-id mismatch: %+v", byID)
	}

	if err := s.DeleteOAuthFlow(ctx, "flow-1"); err != nil {
		t.Fatalf("DeleteOAuthFlow: %v", err)
	}
	if _, err := s.GetOAuthFlow(ctx, "flow-1"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("expected ErrNotFound after delete, got %v", err)
	}
}

func TestDeleteExpiredOAuthFlows(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	now := time.Now().UTC().Truncate(time.Second)

	old := store.OAuthFlow{
		FlowID: "old", State: "s-old", SlackUserID: "U", SlackChannelID: "C",
		SlackThreadTS: "1", ServerName: "g", ServerURL: "u",
		AuthURL: "a", TokenURL: "t", ClientID: "c", PKCEVerifier: "v", RedirectURI: "r",
		ExpiresAt: now.Add(-time.Minute),
	}
	fresh := old
	fresh.FlowID = "fresh"
	fresh.State = "s-fresh"
	fresh.ExpiresAt = now.Add(time.Hour)
	if err := s.CreateOAuthFlow(ctx, old); err != nil {
		t.Fatalf("create old: %v", err)
	}
	if err := s.CreateOAuthFlow(ctx, fresh); err != nil {
		t.Fatalf("create fresh: %v", err)
	}
	if err := s.DeleteExpiredOAuthFlows(ctx, now); err != nil {
		t.Fatalf("DeleteExpiredOAuthFlows: %v", err)
	}
	if _, err := s.GetOAuthFlow(ctx, "old"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("expected old flow deleted, got %v", err)
	}
	if _, err := s.GetOAuthFlow(ctx, "fresh"); err != nil {
		t.Errorf("expected fresh flow to survive, got %v", err)
	}
}

func TestSessionLifecycle(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	sess := store.Session{
		ID:             "sess-1",
		SlackChannelID: "C1",
		SlackThreadTS:  "168.100",
		SlackUserID:    "U123",
		WorkflowName:   "deploy-service",
		Status:         "pending",
	}
	if err := s.CreateSession(ctx, sess); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	got, err := s.GetSessionByThread(ctx, "C1", "168.100")
	if err != nil {
		t.Fatalf("GetSessionByThread: %v", err)
	}
	if got.ID != "sess-1" {
		t.Errorf("session mismatch: %+v", got)
	}

	if err := s.UpdateSessionSandbox(ctx, "sess-1", "claim-1", "sbx-1", "launching"); err != nil {
		t.Fatalf("UpdateSessionSandbox: %v", err)
	}
	if err := s.UpdateSessionACP(ctx, "sess-1", "acp-sess-1", "running"); err != nil {
		t.Fatalf("UpdateSessionACP: %v", err)
	}
	got, err = s.GetSession(ctx, "sess-1")
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if got.SandboxClaimName != "claim-1" || got.ACPSessionID != "acp-sess-1" || got.Status != "running" {
		t.Errorf("session updates not persisted: %+v", got)
	}

	active, err := s.ListActiveSessions(ctx)
	if err != nil {
		t.Fatalf("ListActiveSessions: %v", err)
	}
	if len(active) != 1 {
		t.Errorf("expected 1 active session, got %d", len(active))
	}

	if err := s.UpdateSessionStatus(ctx, "sess-1", "ended"); err != nil {
		t.Fatalf("UpdateSessionStatus: %v", err)
	}
	active, err = s.ListActiveSessions(ctx)
	if err != nil {
		t.Fatalf("ListActiveSessions after end: %v", err)
	}
	if len(active) != 0 {
		t.Errorf("expected 0 active sessions after end, got %d", len(active))
	}
}

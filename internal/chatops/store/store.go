// Package store is the persistence layer for agentops. It wraps the
// sqlc-generated query layer over a single-writer modernc.org/sqlite database,
// applies goose migrations in-process at startup, and exposes domain types that
// shield callers from the generated row structs.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/pressly/goose/v3"
	_ "modernc.org/sqlite" // register the "sqlite" sql driver

	migrations "github.com/pomerium/agentops/internal/chatops/db/migrations"
	sqlcgen "github.com/pomerium/agentops/internal/chatops/db/sqlc"
)

// ErrNotFound is returned when a lookup matches no row.
var ErrNotFound = errors.New("store: not found")

// Store is the application's persistence handle. It is safe for concurrent use;
// writes are serialized by a single open connection.
type Store struct {
	db *sql.DB
	q  *sqlcgen.Queries
}

// Open opens (creating if necessary) the SQLite database at path, applies all
// pending migrations, and returns a ready Store. The database uses a single
// writer connection as required by SQLite.
func Open(ctx context.Context, path string) (*Store, error) {
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)", url.PathEscape(path))
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// SQLite supports a single writer; serialize access through one connection.
	db.SetMaxOpenConns(1)

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}

	if err := migrate(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}

	return &Store{db: db, q: sqlcgen.New(db)}, nil
}

func migrate(ctx context.Context, db *sql.DB) error {
	provider, err := goose.NewProvider(goose.DialectSQLite3, db, migrations.FS)
	if err != nil {
		return fmt.Errorf("goose provider: %w", err)
	}
	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	return nil
}

// Close releases the database handle.
func (s *Store) Close() error { return s.db.Close() }

// --- time helpers -----------------------------------------------------------

// toUnix converts t to unix seconds, mapping the zero time to 0 ("no value").
func toUnix(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.Unix()
}

// fromUnix converts unix seconds to a UTC time, mapping 0 to the zero time.
func fromUnix(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(n, 0).UTC()
}

// --- MCP credentials ---------------------------------------------------------

// MCPCredential is a per-(slack user, server) OAuth credential plus the dynamic
// client registration needed to refresh it.
type MCPCredential struct {
	SlackUserID      string
	ServerName       string
	ServerURL        string
	AccessToken      string
	RefreshToken     string
	TokenType        string
	Expiry           time.Time
	AuthURL          string
	TokenURL         string
	ClientID         string
	ClientSecret     string
	Scopes           string
	RegistrationJSON string
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

func credFromRow(r sqlcgen.McpCredential) MCPCredential {
	return MCPCredential{
		SlackUserID:      r.SlackUserID,
		ServerName:       r.ServerName,
		ServerURL:        r.ServerUrl,
		AccessToken:      r.AccessToken,
		RefreshToken:     r.RefreshToken,
		TokenType:        r.TokenType,
		Expiry:           fromUnix(r.Expiry),
		AuthURL:          r.AuthUrl,
		TokenURL:         r.TokenUrl,
		ClientID:         r.ClientID,
		ClientSecret:     r.ClientSecret,
		Scopes:           r.Scopes,
		RegistrationJSON: r.RegistrationJson,
		CreatedAt:        fromUnix(r.CreatedAt),
		UpdatedAt:        fromUnix(r.UpdatedAt),
	}
}

// SaveMCPCredential inserts or updates a credential, keyed by
// (SlackUserID, ServerName).
func (s *Store) SaveMCPCredential(ctx context.Context, c MCPCredential) error {
	now := time.Now().Unix()
	if c.TokenType == "" {
		c.TokenType = "Bearer"
	}
	return s.q.UpsertMCPCredential(ctx, sqlcgen.UpsertMCPCredentialParams{
		SlackUserID:      c.SlackUserID,
		ServerName:       c.ServerName,
		ServerUrl:        c.ServerURL,
		AccessToken:      c.AccessToken,
		RefreshToken:     c.RefreshToken,
		TokenType:        c.TokenType,
		Expiry:           toUnix(c.Expiry),
		AuthUrl:          c.AuthURL,
		TokenUrl:         c.TokenURL,
		ClientID:         c.ClientID,
		ClientSecret:     c.ClientSecret,
		Scopes:           c.Scopes,
		RegistrationJson: c.RegistrationJSON,
		CreatedAt:        now,
		UpdatedAt:        now,
	})
}

// GetMCPCredential returns the credential for (user, server) or ErrNotFound.
func (s *Store) GetMCPCredential(ctx context.Context, slackUserID, serverName string) (MCPCredential, error) {
	r, err := s.q.GetMCPCredential(ctx, sqlcgen.GetMCPCredentialParams{
		SlackUserID: slackUserID,
		ServerName:  serverName,
	})
	if errors.Is(err, sql.ErrNoRows) {
		return MCPCredential{}, ErrNotFound
	}
	if err != nil {
		return MCPCredential{}, err
	}
	return credFromRow(r), nil
}

// ListMCPCredentials returns all credentials stored for a user.
func (s *Store) ListMCPCredentials(ctx context.Context, slackUserID string) ([]MCPCredential, error) {
	rows, err := s.q.ListMCPCredentialsByUser(ctx, slackUserID)
	if err != nil {
		return nil, err
	}
	out := make([]MCPCredential, 0, len(rows))
	for _, r := range rows {
		out = append(out, credFromRow(r))
	}
	return out, nil
}

// DeleteMCPCredential removes the credential for (user, server).
func (s *Store) DeleteMCPCredential(ctx context.Context, slackUserID, serverName string) error {
	return s.q.DeleteMCPCredential(ctx, sqlcgen.DeleteMCPCredentialParams{
		SlackUserID: slackUserID,
		ServerName:  serverName,
	})
}

// --- OAuth flows -------------------------------------------------------------

// OAuthFlow is in-flight authorization-code+PKCE state.
type OAuthFlow struct {
	FlowID               string
	State                string
	SlackUserID          string
	SlackTeamID          string
	SlackChannelID       string
	SlackThreadTS        string
	ServerName           string
	ServerURL            string
	WorkflowName         string
	AuthURL              string
	TokenURL             string
	RegistrationEndpoint string
	ClientID             string
	ClientSecret         string
	Scopes               string
	PKCEVerifier         string
	RedirectURI          string
	CreatedAt            time.Time
	ExpiresAt            time.Time
}

func flowFromRow(r sqlcgen.OauthFlow) OAuthFlow {
	return OAuthFlow{
		FlowID:               r.FlowID,
		State:                r.State,
		SlackUserID:          r.SlackUserID,
		SlackTeamID:          r.SlackTeamID,
		SlackChannelID:       r.SlackChannelID,
		SlackThreadTS:        r.SlackThreadTs,
		ServerName:           r.ServerName,
		ServerURL:            r.ServerUrl,
		WorkflowName:         r.WorkflowName,
		AuthURL:              r.AuthUrl,
		TokenURL:             r.TokenUrl,
		RegistrationEndpoint: r.RegistrationEndpoint,
		ClientID:             r.ClientID,
		ClientSecret:         r.ClientSecret,
		Scopes:               r.Scopes,
		PKCEVerifier:         r.PkceVerifier,
		RedirectURI:          r.RedirectUri,
		CreatedAt:            fromUnix(r.CreatedAt),
		ExpiresAt:            fromUnix(r.ExpiresAt),
	}
}

// CreateOAuthFlow persists a new in-flight flow.
func (s *Store) CreateOAuthFlow(ctx context.Context, f OAuthFlow) error {
	return s.q.CreateOAuthFlow(ctx, sqlcgen.CreateOAuthFlowParams{
		FlowID:               f.FlowID,
		State:                f.State,
		SlackUserID:          f.SlackUserID,
		SlackTeamID:          f.SlackTeamID,
		SlackChannelID:       f.SlackChannelID,
		SlackThreadTs:        f.SlackThreadTS,
		ServerName:           f.ServerName,
		ServerUrl:            f.ServerURL,
		WorkflowName:         f.WorkflowName,
		AuthUrl:              f.AuthURL,
		TokenUrl:             f.TokenURL,
		RegistrationEndpoint: f.RegistrationEndpoint,
		ClientID:             f.ClientID,
		ClientSecret:         f.ClientSecret,
		Scopes:               f.Scopes,
		PkceVerifier:         f.PKCEVerifier,
		RedirectUri:          f.RedirectURI,
		CreatedAt:            time.Now().Unix(),
		ExpiresAt:            toUnix(f.ExpiresAt),
	})
}

// GetOAuthFlowByState looks up a flow by its OAuth state parameter.
func (s *Store) GetOAuthFlowByState(ctx context.Context, state string) (OAuthFlow, error) {
	r, err := s.q.GetOAuthFlowByState(ctx, state)
	if errors.Is(err, sql.ErrNoRows) {
		return OAuthFlow{}, ErrNotFound
	}
	if err != nil {
		return OAuthFlow{}, err
	}
	return flowFromRow(r), nil
}

// GetOAuthFlow looks up a flow by its flow id.
func (s *Store) GetOAuthFlow(ctx context.Context, flowID string) (OAuthFlow, error) {
	r, err := s.q.GetOAuthFlow(ctx, flowID)
	if errors.Is(err, sql.ErrNoRows) {
		return OAuthFlow{}, ErrNotFound
	}
	if err != nil {
		return OAuthFlow{}, err
	}
	return flowFromRow(r), nil
}

// DeleteOAuthFlow removes a flow by id.
func (s *Store) DeleteOAuthFlow(ctx context.Context, flowID string) error {
	return s.q.DeleteOAuthFlow(ctx, flowID)
}

// DeleteExpiredOAuthFlows removes flows whose ExpiresAt is before now.
func (s *Store) DeleteExpiredOAuthFlows(ctx context.Context, now time.Time) error {
	return s.q.DeleteExpiredOAuthFlows(ctx, now.Unix())
}

// --- sessions ----------------------------------------------------------------

// Session binds an active Slack thread to a sandbox/ACP session.
type Session struct {
	ID               string
	SlackChannelID   string
	SlackThreadTS    string
	SlackUserID      string
	WorkflowName     string
	InitialPrompt    string
	SandboxClaimName string
	SandboxName      string
	ACPSessionID     string
	Status           string
	SlackResponseURL string
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

func sessionFromRow(r sqlcgen.Session) Session {
	return Session{
		ID:               r.ID,
		SlackChannelID:   r.SlackChannelID,
		SlackThreadTS:    r.SlackThreadTs,
		SlackUserID:      r.SlackUserID,
		WorkflowName:     r.WorkflowName,
		InitialPrompt:    r.InitialPrompt,
		SandboxClaimName: r.SandboxClaimName,
		SandboxName:      r.SandboxName,
		ACPSessionID:     r.AcpSessionID,
		Status:           r.Status,
		SlackResponseURL: r.SlackResponseUrl,
		CreatedAt:        fromUnix(r.CreatedAt),
		UpdatedAt:        fromUnix(r.UpdatedAt),
	}
}

// CreateSession persists a new session in the "pending" (or supplied) status.
func (s *Store) CreateSession(ctx context.Context, sess Session) error {
	now := time.Now().Unix()
	if sess.Status == "" {
		sess.Status = "pending"
	}
	return s.q.CreateSession(ctx, sqlcgen.CreateSessionParams{
		ID:               sess.ID,
		SlackChannelID:   sess.SlackChannelID,
		SlackThreadTs:    sess.SlackThreadTS,
		SlackUserID:      sess.SlackUserID,
		WorkflowName:     sess.WorkflowName,
		InitialPrompt:    sess.InitialPrompt,
		Status:           sess.Status,
		SlackResponseUrl: sess.SlackResponseURL,
		CreatedAt:        now,
		UpdatedAt:        now,
	})
}

// GetSession returns a session by id or ErrNotFound.
func (s *Store) GetSession(ctx context.Context, id string) (Session, error) {
	r, err := s.q.GetSession(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return Session{}, ErrNotFound
	}
	if err != nil {
		return Session{}, err
	}
	return sessionFromRow(r), nil
}

// GetSessionByThread returns the session bound to a Slack thread or ErrNotFound.
func (s *Store) GetSessionByThread(ctx context.Context, channelID, threadTS string) (Session, error) {
	r, err := s.q.GetSessionByThread(ctx, sqlcgen.GetSessionByThreadParams{
		SlackChannelID: channelID,
		SlackThreadTs:  threadTS,
	})
	if errors.Is(err, sql.ErrNoRows) {
		return Session{}, ErrNotFound
	}
	if err != nil {
		return Session{}, err
	}
	return sessionFromRow(r), nil
}

// UpdateSessionSandbox records the sandbox claim/name and a new status.
func (s *Store) UpdateSessionSandbox(ctx context.Context, id, claimName, sandboxName, status string) error {
	return s.q.UpdateSessionSandbox(ctx, sqlcgen.UpdateSessionSandboxParams{
		SandboxClaimName: claimName,
		SandboxName:      sandboxName,
		Status:           status,
		UpdatedAt:        time.Now().Unix(),
		ID:               id,
	})
}

// UpdateSessionACP records the ACP session id and a new status.
func (s *Store) UpdateSessionACP(ctx context.Context, id, acpSessionID, status string) error {
	return s.q.UpdateSessionACP(ctx, sqlcgen.UpdateSessionACPParams{
		AcpSessionID: acpSessionID,
		Status:       status,
		UpdatedAt:    time.Now().Unix(),
		ID:           id,
	})
}

// UpdateSessionStatus updates only the status field.
func (s *Store) UpdateSessionStatus(ctx context.Context, id, status string) error {
	return s.q.UpdateSessionStatus(ctx, sqlcgen.UpdateSessionStatusParams{
		Status:    status,
		UpdatedAt: time.Now().Unix(),
		ID:        id,
	})
}

// UpdateSessionResponseURL records the Slack response_url for a session, used to
// edit the ephemeral connect prompt in place once OAuth completes.
func (s *Store) UpdateSessionResponseURL(ctx context.Context, id, responseURL string) error {
	return s.q.UpdateSessionResponseURL(ctx, sqlcgen.UpdateSessionResponseURLParams{
		SlackResponseUrl: responseURL,
		UpdatedAt:        time.Now().Unix(),
		ID:               id,
	})
}

// ListActiveSessions returns sessions that are neither ended nor failed.
func (s *Store) ListActiveSessions(ctx context.Context) ([]Session, error) {
	rows, err := s.q.ListActiveSessions(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]Session, 0, len(rows))
	for _, r := range rows {
		out = append(out, sessionFromRow(r))
	}
	return out, nil
}

// DeleteSession removes a session by id.
func (s *Store) DeleteSession(ctx context.Context, id string) error {
	return s.q.DeleteSession(ctx, id)
}

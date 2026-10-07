package agentic

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func fakeJWT(t *testing.T, claims map[string]any) string {
	t.Helper()
	payload, err := json.Marshal(claims)
	require.NoError(t, err)
	enc := base64.RawURLEncoding.EncodeToString
	return enc([]byte(`{"alg":"RS256"}`)) + "." + enc(payload) + ".sig"
}

func newTestFilePoller(t *testing.T, file string, now *time.Time) *FilePoller {
	t.Helper()
	p, err := NewFilePoller(FilePollerConfig{
		TokenFile: file,
		Audience:  "pomerium-egress",
		Now:       func() time.Time { return *now },
	})
	require.NoError(t, err)
	return p
}

func TestFilePoller_FreshRead(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_000_000, 0)
	jwt := fakeJWT(t, map[string]any{"aud": []string{"pomerium-egress"}, "exp": now.Unix() + 600})
	file := writeTokenFile(t, jwt+"\n")

	res := newTestFilePoller(t, file, &now).Poll(context.Background())
	require.Equal(t, PollOk, res.Kind, res.Err)
	assert.Equal(t, "Bearer "+jwt, res.Token.Bearer)
	assert.Equal(t, 600*time.Second, res.Token.ExpiresIn)
	assert.Empty(t, res.Token.RunID)
}

func TestFilePoller_StringAudience(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_000_000, 0)
	file := writeTokenFile(t, fakeJWT(t, map[string]any{"aud": "pomerium-egress", "exp": now.Unix() + 60}))
	res := newTestFilePoller(t, file, &now).Poll(context.Background())
	require.Equal(t, PollOk, res.Kind, res.Err)
}

func TestFilePoller_Rotation(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_000_000, 0)
	first := fakeJWT(t, map[string]any{"aud": "pomerium-egress", "exp": now.Unix() + 600, "jti": "a"})
	file := writeTokenFile(t, first)
	p := newTestFilePoller(t, file, &now)

	res := p.Poll(context.Background())
	require.Equal(t, PollOk, res.Kind)
	assert.Equal(t, "Bearer "+first, res.Token.Bearer)

	now = now.Add(480 * time.Second)
	second := fakeJWT(t, map[string]any{"aud": "pomerium-egress", "exp": now.Unix() + 600, "jti": "b"})
	require.NoError(t, os.WriteFile(file, []byte(second), 0o600))

	res = p.Poll(context.Background())
	require.Equal(t, PollOk, res.Kind)
	assert.Equal(t, "Bearer "+second, res.Token.Bearer)
	assert.Equal(t, 600*time.Second, res.Token.ExpiresIn)
}

func TestFilePoller_ExpiresInIsRemainingLifetime(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_000_000, 0)
	file := writeTokenFile(t, fakeJWT(t, map[string]any{"aud": "pomerium-egress", "exp": now.Unix() + 600}))
	p := newTestFilePoller(t, file, &now)

	now = now.Add(500 * time.Second)
	res := p.Poll(context.Background())
	require.Equal(t, PollOk, res.Kind)
	assert.Equal(t, 100*time.Second, res.Token.ExpiresIn)
}

func TestFilePoller_Terminal(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_000_000, 0)
	valid := map[string]any{"aud": "pomerium-egress", "exp": now.Unix() + 600}
	cases := []struct {
		name       string
		file       func(t *testing.T) string
		wantReason string
		wantErr    string
	}{
		{"missing", func(t *testing.T) string { return filepath.Join(t.TempDir(), "absent") }, ReasonConfigError, "read projected token"},
		{"unreadable", func(t *testing.T) string {
			p := writeTokenFile(t, fakeJWT(t, valid))
			require.NoError(t, os.Chmod(p, 0o000))
			return p
		}, ReasonConfigError, "read projected token"},
		{"not a jwt", func(t *testing.T) string { return writeTokenFile(t, "opaque-token") }, ReasonConfigError, "not a JWT"},
		{"no exp", func(t *testing.T) string {
			return writeTokenFile(t, fakeJWT(t, map[string]any{"aud": "pomerium-egress"}))
		}, ReasonConfigError, "no exp"},
		{"agentic AS audience", func(t *testing.T) string {
			return writeTokenFile(t, fakeJWT(t, map[string]any{"aud": "pomerium-agentic-as", "exp": now.Unix() + 600}))
		}, ReasonConfigError, `want "pomerium-egress"`},
		{"expired", func(t *testing.T) string {
			return writeTokenFile(t, fakeJWT(t, map[string]any{"aud": "pomerium-egress", "exp": now.Unix() - 30}))
		}, ReasonRevokedOrExpired, "not rotating"},
		{"under a second left", func(t *testing.T) string {
			return writeTokenFile(t, fakeJWT(t, map[string]any{"aud": "pomerium-egress", "exp": now.Unix()}))
		}, ReasonRevokedOrExpired, "not rotating"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.name == "unreadable" && os.Geteuid() == 0 {
				t.Skip("root reads a 0000 file")
			}
			res := newTestFilePoller(t, tc.file(t), &now).Poll(context.Background())
			require.Equal(t, PollTerminal, res.Kind)
			assert.Equal(t, tc.wantReason, res.Reason)
			require.Error(t, res.Err)
			assert.Contains(t, res.Err.Error(), tc.wantErr)
		})
	}
}

func TestFilePoller_ErrorsNeverCarryTheToken(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_000_000, 0)
	jwt := fakeJWT(t, map[string]any{"aud": "other", "exp": now.Unix() + 600, "sub": "SECRET-SUBJECT"})
	res := newTestFilePoller(t, writeTokenFile(t, jwt), &now).Poll(context.Background())
	require.Equal(t, PollTerminal, res.Kind)
	assert.NotContains(t, res.Err.Error(), jwt)
	assert.False(t, strings.Contains(res.Err.Error(), "SECRET-SUBJECT"))
}

func TestNewFilePoller_RequiresFileAndAudience(t *testing.T) {
	t.Parallel()
	_, err := NewFilePoller(FilePollerConfig{Audience: "a"})
	require.Error(t, err)
	_, err = NewFilePoller(FilePollerConfig{TokenFile: "/x"})
	require.Error(t, err)
}

func TestFilePoller_LoopRefreshesBeforeExpiry(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_000_000, 0)
	first := fakeJWT(t, map[string]any{"aud": "pomerium-egress", "exp": now.Unix() + 600, "jti": "a"})
	file := writeTokenFile(t, first)
	second := fakeJWT(t, map[string]any{"aud": "pomerium-egress", "exp": now.Unix() + 480 + 600, "jti": "b"})

	var delivered []string
	var slept []time.Duration
	exp := now.Add(600 * time.Second)
	loop := NewLoop(LoopConfig{
		Poll: newTestFilePoller(t, file, &now),
		Sink: func(tk *Token) error { delivered = append(delivered, tk.Bearer); return nil },
		Now:  func() time.Time { return now },
		Sleep: func(ctx context.Context, d time.Duration) error {
			slept = append(slept, d)
			now = now.Add(d)
			assert.True(t, now.Before(exp), "slept past the held token's exp")
			if now.Sub(exp.Add(-600*time.Second)) >= 480*time.Second && len(delivered) == 1 {
				require.NoError(t, os.WriteFile(file, []byte(second), 0o600))
			}
			if len(delivered) == 2 {
				return context.Canceled
			}
			return nil
		},
	})
	require.ErrorIs(t, loop.Run(context.Background()), context.Canceled)
	assert.Equal(t, []string{"Bearer " + first, "Bearer " + second}, delivered)
	assert.Greater(t, len(slept), 2, "the unchanged token was re-read more than once before rotation")
}

package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/pomerium/agentops/harness/internal/sessionstore"
	"github.com/pomerium/agentops/harness/internal/sessionstore/sqlite"
	"github.com/pomerium/agentops/harness/internal/sessionstore/storetest"
)

func TestConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) storetest.Opener {
		path := filepath.Join(t.TempDir(), "test.db")
		return func(ctx context.Context) (sessionstore.Store, error) {
			s, err := sqlite.Open(ctx, path)
			if err != nil {
				return nil, err
			}
			return s, nil
		}
	})
}

func TestOpenRunsMigrations(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	ctx := context.Background()
	s1, err := sqlite.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	_ = s1.Close()
	s2, err := sqlite.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	_ = s2.Close()
}

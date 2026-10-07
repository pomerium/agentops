package runner

import (
	"errors"
	"os"
	"testing"
)

func TestAFailedSpawnClosesThePipesItOpened(t *testing.T) {
	for failAt := 1; failAt <= 3; failAt++ {
		var opened []*os.File
		calls := 0
		s := New()
		s.cfg.pipe = func() (*os.File, *os.File, error) {
			calls++
			if calls == failAt {
				return nil, nil, errors.New("too many open files")
			}
			r, w, err := os.Pipe()
			if err == nil {
				opened = append(opened, r, w)
			}
			return r, w, err
		}
		if _, err := s.spawn(); err == nil {
			t.Fatalf("pipe %d failed, but spawn succeeded", failAt)
		}
		for _, f := range opened {
			if _, err := f.Stat(); !errors.Is(err, os.ErrClosed) {
				t.Errorf("pipe %d failed, and %s is still open", failAt, f.Name())
			}
		}
	}
}

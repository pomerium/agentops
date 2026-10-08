package client

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/slack-go/slack"
)

func TestThreadRepliesReportsAnUnfinishedRead(t *testing.T) {
	page := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		page++
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"ok":true,"messages":[{"type":"message","ts":"%d.000000","text":"page %d"}],`+
			`"has_more":true,"response_metadata":{"next_cursor":"page-%d"}}`, page, page, page+1)
	}))
	defer srv.Close()

	p := &Poster{client: slack.New("test-token", slack.OptionAPIURL(srv.URL+"/"))}
	msgs, err := p.ThreadReplies(context.Background(), "C1", "1.000000", "", 50)
	if !errors.Is(err, ErrThreadTooLong) {
		t.Fatalf("an unfinished read returned %d messages and err %v, want ErrThreadTooLong", len(msgs), err)
	}
}

func TestThreadRepliesReadsEveryPage(t *testing.T) {
	page := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		page++
		more := page < 3
		next := ""
		if more {
			next = fmt.Sprintf("page-%d", page+1)
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"ok":true,"messages":[{"type":"message","ts":"%d.000000","text":"page %d"}],`+
			`"has_more":%t,"response_metadata":{"next_cursor":%q}}`, page, page, more, next)
	}))
	defer srv.Close()

	p := &Poster{client: slack.New("test-token", slack.OptionAPIURL(srv.URL+"/"))}
	msgs, err := p.ThreadReplies(context.Background(), "C1", "1.000000", "", 2)
	if err != nil {
		t.Fatalf("ThreadReplies: %v", err)
	}
	if len(msgs) != 2 || msgs[0].Text != "page 2" || msgs[1].Text != "page 3" {
		t.Fatalf("got %+v, want the newest two messages", msgs)
	}
}

func TestPostDMReturnsTheChannelSlackOpened(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"ok":true,"channel":"D0DM","ts":"1.000100"}`)
	}))
	defer srv.Close()

	p := &Poster{client: slack.New("test-token", slack.OptionAPIURL(srv.URL+"/"))}
	channel, ts, err := p.PostDM(context.Background(), "U1", slack.MsgOptionText("hi", false))
	if err != nil || channel != "D0DM" || ts != "1.000100" {
		t.Fatalf("PostDM = %q, %q, %v; want the DM channel Slack opened", channel, ts, err)
	}
}

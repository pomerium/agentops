package harnessapi_test

import (
	"errors"
	"testing"

	"github.com/pomerium/agentops/harness/api"
	pb "github.com/pomerium/agentops/harness/api/pb"
	v1alpha1 "github.com/pomerium/agentops/harness/apis/v1alpha1"
)

func TestCrossClientAccessIsDenied(t *testing.T) {
	mine, theirs := as(stubClient), as("other")
	h := newHarness(t)

	owned := launchRunning(t, h, "stub:conv-1")
	ref := byID(owned.GetId())

	if _, err := h.svc.GetSession(mine, &pb.GetSessionRequest{Ref: ref}); err != nil {
		t.Fatalf("the owning client must be able to read its own session: %v", err)
	}

	if _, err := h.svc.GetSession(theirs, &pb.GetSessionRequest{Ref: ref}); !errors.Is(err, api.ErrNotFound) {
		t.Errorf("GetSession across clients: got %v, want ErrNotFound", err)
	}
	if _, err := h.svc.Prompt(theirs, &pb.PromptRequest{Ref: ref, Content: "hi"}); !errors.Is(err, api.ErrNotFound) {
		t.Errorf("Prompt across clients: got %v, want ErrNotFound", err)
	}
	if _, err := h.svc.EndSession(theirs, &pb.EndSessionRequest{Ref: ref}); !errors.Is(err, api.ErrNotFound) {
		t.Errorf("EndSession across clients: got %v, want ErrNotFound", err)
	}
	if _, err := h.svc.RespondPermission(theirs, &pb.RespondPermissionRequest{
		Ref: ref, RequestId: "tc-1", OptionId: "allow",
	}); !errors.Is(err, api.ErrNotFound) {
		t.Errorf("RespondPermission across clients: got %v, want ErrNotFound", err)
	}
	if _, err := h.svc.ListEvents(theirs, &pb.ListEventsRequest{Ref: ref}); !errors.Is(err, api.ErrNotFound) {
		t.Errorf("Events across clients: got %v, want ErrNotFound", err)
	}
	if _, err := h.subscribe(theirs, &pb.SubscribeRequest{Ref: ref}); !errors.Is(err, api.ErrNotFound) {
		t.Errorf("Subscribe across clients: got %v, want ErrNotFound", err)
	}

	list, err := h.svc.ListSessions(theirs, &pb.ListSessionsRequest{})
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if n := len(list.GetSessions()); n != 0 {
		t.Errorf("another client enumerated %d sessions, want 0", n)
	}
	mineList, err := h.svc.ListSessions(mine, &pb.ListSessionsRequest{})
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(mineList.GetSessions()) == 0 {
		t.Error("the owning client should see its own sessions")
	}
}

func TestConversationRefsAreNamespacedPerClient(t *testing.T) {
	ctx := as(stubClient)
	h := newHarness(t)

	a, err := h.svc.CreateSession(ctx, &pb.CreateSessionRequest{
		Template: "deploy", ConversationRef: "conv-1",
		ApprovalPrompt: "ship the thing",
	})
	if err != nil {
		t.Fatalf("CreateSession(stub): %v", err)
	}
	b, err := h.svc.CreateSession(as("other"), &pb.CreateSessionRequest{
		Template: "deploy", ConversationRef: "conv-1",
		ApprovalPrompt: "ship the thing",
	})
	if err != nil {
		t.Fatalf("CreateSession(other) with the same ref must be allowed: %v", err)
	}
	if a.GetSession().GetId() == b.GetSession().GetId() {
		t.Fatal("two clients' identical conversation refs resolved to one session")
	}

	got, err := h.svc.GetSession(ctx, &pb.GetSessionRequest{Ref: &pb.SessionRef{ConversationRef: "conv-1"}})
	if err != nil {
		t.Fatalf("GetSession by conversation: %v", err)
	}
	if got.GetSession().GetId() != a.GetSession().GetId() {
		t.Errorf("conversation lookup crossed clients: got %q, want %q", got.GetSession().GetId(), a.GetSession().GetId())
	}
}

func TestClientBindingGatesTemplates(t *testing.T) {
	ctx := as(stubClient)
	h := newHarness(t)
	h.tmpl.bindings = map[string]*v1alpha1.ClientBinding{
		stubClient: {Spec: v1alpha1.ClientBindingSpec{Subject: stubClient, Templates: []string{"other-agent"}}},
	}

	_, err := h.svc.CreateSession(ctx, &pb.CreateSessionRequest{
		Template: "deploy", ConversationRef: "conv-1",
		ApprovalPrompt: "ship the thing",
	})
	if !errors.Is(err, api.ErrForbidden) {
		t.Errorf("a template outside the binding: got %v, want ErrForbidden", err)
	}

	tmpls, err := h.svc.ListTemplates(ctx, &pb.ListTemplatesRequest{})
	if err != nil {
		t.Fatalf("ListTemplates: %v", err)
	}
	if len(tmpls.GetTemplates()) != 0 {
		t.Errorf("ListTemplates returned %v; the binding allows none of the templates that exist", tmpls.GetTemplates())
	}
}

package apiserver

import (
	"context"
	"time"

	"connectrpc.com/connect"

	"github.com/pomerium/agentops/harness/api"
	pb "github.com/pomerium/agentops/harness/api/pb"
	"github.com/pomerium/agentops/harness/api/wire"
)

// handler serves the Harness API over Connect on top of the in-process
// implementation.
//
// Every method follows one shape: take the client id from the context (where the
// identity middleware put it, having read it off the verified assertion), build
// the service request around it, translate the answer. The client id is never
// taken from the message — no request message has a field for one, and this is
// the reason.
type handler struct {
	svc api.API
}

// clientID returns the verified caller, or the error every verb refuses with
// when the middleware has not run. Reached only through a misconfiguration, but
// stated rather than defaulted: a nameless caller must not become a client with
// an empty id, which would collide with every other nameless caller.
func clientID(ctx context.Context) (string, error) {
	id, ok := clientIDFrom(ctx)
	if !ok {
		return "", connect.NewError(connect.CodeUnauthenticated, ErrNoIdentity)
	}
	return id, nil
}

func (h *handler) CreateSession(ctx context.Context, req *connect.Request[pb.CreateSessionRequest]) (*connect.Response[pb.CreateSessionResponse], error) {
	id, err := clientID(ctx)
	if err != nil {
		return nil, err
	}
	m := req.Msg
	view, err := h.svc.CreateSession(ctx, api.CreateSessionRequest{
		ClientID:             id,
		Template:             m.GetTemplate(),
		ConversationRef:      m.GetConversationRef(),
		ParentSessionID:      m.GetParentSessionId(),
		ApprovalPrompt:       m.GetApprovalPrompt(),
		InitialPrompt:        m.GetInitialPrompt(),
		SystemPromptAppendix: m.GetSystemPromptAppendix(),
	})
	if err != nil {
		return nil, wire.ToConnect(err)
	}
	return connect.NewResponse(&pb.CreateSessionResponse{Session: wire.View(view)}), nil
}

func (h *handler) Prompt(ctx context.Context, req *connect.Request[pb.PromptRequest]) (*connect.Response[pb.PromptResponse], error) {
	id, err := clientID(ctx)
	if err != nil {
		return nil, err
	}
	res, err := h.svc.Prompt(ctx, api.PromptRequest{
		Ref:            wire.RefFrom(req.Msg.GetRef(), id),
		Content:        req.Msg.GetContent(),
		IdempotencyKey: req.Msg.GetIdempotencyKey(),
	})
	if err != nil {
		return nil, wire.ToConnect(err)
	}
	return connect.NewResponse(&pb.PromptResponse{TurnId: res.TurnID}), nil
}

func (h *handler) RespondPermission(ctx context.Context, req *connect.Request[pb.RespondPermissionRequest]) (*connect.Response[pb.RespondPermissionResponse], error) {
	id, err := clientID(ctx)
	if err != nil {
		return nil, err
	}
	if err := h.svc.RespondPermission(ctx, api.RespondPermissionRequest{
		Ref:       wire.RefFrom(req.Msg.GetRef(), id),
		RequestID: req.Msg.GetRequestId(),
		OptionID:  req.Msg.GetOptionId(),
	}); err != nil {
		return nil, wire.ToConnect(err)
	}
	return connect.NewResponse(&pb.RespondPermissionResponse{}), nil
}

func (h *handler) EndSession(ctx context.Context, req *connect.Request[pb.EndSessionRequest]) (*connect.Response[pb.EndSessionResponse], error) {
	id, err := clientID(ctx)
	if err != nil {
		return nil, err
	}
	if err := h.svc.EndSession(ctx, api.EndSessionRequest{
		Ref:    wire.RefFrom(req.Msg.GetRef(), id),
		Reason: req.Msg.GetReason(),
	}); err != nil {
		return nil, wire.ToConnect(err)
	}
	return connect.NewResponse(&pb.EndSessionResponse{}), nil
}

func (h *handler) GetSession(ctx context.Context, req *connect.Request[pb.GetSessionRequest]) (*connect.Response[pb.GetSessionResponse], error) {
	id, err := clientID(ctx)
	if err != nil {
		return nil, err
	}
	view, err := h.svc.GetSession(ctx, wire.RefFrom(req.Msg.GetRef(), id))
	if err != nil {
		return nil, wire.ToConnect(err)
	}
	return connect.NewResponse(&pb.GetSessionResponse{Session: wire.View(view)}), nil
}

func (h *handler) ListSessions(ctx context.Context, req *connect.Request[pb.ListSessionsRequest]) (*connect.Response[pb.ListSessionsResponse], error) {
	id, err := clientID(ctx)
	if err != nil {
		return nil, err
	}
	views, err := h.svc.ListSessions(ctx, api.ListSessionsRequest{
		ClientID:     id,
		LiveOnly:     req.Msg.GetLiveOnly(),
		UpdatedSince: wire.Time(req.Msg.GetUpdatedSince()),
	})
	if err != nil {
		return nil, wire.ToConnect(err)
	}
	out := &pb.ListSessionsResponse{Sessions: make([]*pb.SessionView, 0, len(views))}
	for _, v := range views {
		out.Sessions = append(out.Sessions, wire.View(v))
	}
	return connect.NewResponse(out), nil
}

func (h *handler) ListTemplates(ctx context.Context, _ *connect.Request[pb.ListTemplatesRequest]) (*connect.Response[pb.ListTemplatesResponse], error) {
	id, err := clientID(ctx)
	if err != nil {
		return nil, err
	}
	tmpls, err := h.svc.ListTemplates(ctx, id)
	if err != nil {
		return nil, wire.ToConnect(err)
	}
	out := &pb.ListTemplatesResponse{Templates: make([]*pb.TemplateSummary, 0, len(tmpls))}
	for _, t := range tmpls {
		out.Templates = append(out.Templates, &pb.TemplateSummary{Name: t.Name, Description: t.Description})
	}
	return connect.NewResponse(out), nil
}

func (h *handler) ListEvents(ctx context.Context, req *connect.Request[pb.ListEventsRequest]) (*connect.Response[pb.ListEventsResponse], error) {
	id, err := clientID(ctx)
	if err != nil {
		return nil, err
	}
	events, err := h.svc.ListEvents(ctx, api.EventsRequest{
		Ref:      wire.RefFrom(req.Msg.GetRef(), id),
		AfterSeq: req.Msg.GetAfterSeq(),
		Limit:    int(req.Msg.GetLimit()),
	})
	if err != nil {
		return nil, wire.ToConnect(err)
	}
	out := &pb.ListEventsResponse{Events: make([]*pb.Event, 0, len(events))}
	for _, ev := range events {
		out.Events = append(out.Events, wire.Event(ev))
	}
	return connect.NewResponse(out), nil
}

func (h *handler) Subscribe(ctx context.Context, req *connect.Request[pb.SubscribeRequest], stream *connect.ServerStream[pb.SubscribeResponse]) error {
	id, err := clientID(ctx)
	if err != nil {
		return err
	}
	sub, err := h.svc.Subscribe(ctx, api.SubscribeRequest{
		Ref:      wire.RefFrom(req.Msg.GetRef(), id),
		AfterSeq: req.Msg.GetAfterSeq(),
	})
	if err != nil {
		return wire.ToConnect(err)
	}
	defer sub.Close()

	// The opening keepalive. It is the acknowledgement that turns a
	// server-streaming call into something a client can open synchronously: until
	// a message arrives the client cannot distinguish an accepted subscription
	// from a refused one, and a refusal that reads as silence is the worst of both.
	if err := stream.Send(&pb.SubscribeResponse{Keepalive: true}); err != nil {
		return err
	}

	ticker := time.NewTicker(api.KeepaliveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-sub.Events():
			if !ok {
				// The session's log is finished. A clean end of stream, not an error:
				// there is nothing further to deliver and a client should stop asking.
				return nil
			}
			if err := stream.Send(&pb.SubscribeResponse{Event: wire.Event(ev)}); err != nil {
				return err
			}
		case <-ticker.C:
			if err := stream.Send(&pb.SubscribeResponse{Keepalive: true}); err != nil {
				return err
			}
		}
	}
}

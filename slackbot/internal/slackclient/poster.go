package client

import (
	"context"
	"errors"
	"fmt"

	"github.com/slack-go/slack"
)

var ErrThreadTooLong = errors.New("the thread has more replies than one read covers")

type Poster struct {
	client *slack.Client
}

func NewPoster(botToken string) *Poster {
	return &Poster{client: slack.New(botToken)}
}

func (p *Poster) PostMessage(ctx context.Context, channelID string, opts ...slack.MsgOption) (string, error) {
	_, ts, err := p.client.PostMessageContext(ctx, channelID, opts...)
	return ts, err
}

func (p *Poster) PostDM(ctx context.Context, userID string, opts ...slack.MsgOption) (string, string, error) {
	return p.client.PostMessageContext(ctx, userID, opts...)
}

func (p *Poster) PostEphemeral(ctx context.Context, channelID, userID string, opts ...slack.MsgOption) (string, error) {
	return p.client.PostEphemeralContext(ctx, channelID, userID, opts...)
}

func (p *Poster) UpdateMessage(ctx context.Context, channelID, ts string, opts ...slack.MsgOption) (string, error) {
	_, newTS, _, err := p.client.UpdateMessageContext(ctx, channelID, ts, opts...)
	return newTS, err
}

func (p *Poster) DeleteMessage(ctx context.Context, channelID, ts string) error {
	_, _, err := p.client.DeleteMessageContext(ctx, channelID, ts)
	return err
}

func (p *Poster) AddReaction(ctx context.Context, channelID, timestamp, emoji string) error {
	return p.client.AddReactionContext(ctx, emoji, slack.ItemRef{Channel: channelID, Timestamp: timestamp})
}

func (p *Poster) RemoveReaction(ctx context.Context, channelID, timestamp, emoji string) error {
	return p.client.RemoveReactionContext(ctx, emoji, slack.ItemRef{Channel: channelID, Timestamp: timestamp})
}

func (p *Poster) ThreadReplies(ctx context.Context, channelID, threadTS, since string, max int) ([]slack.Message, error) {
	var out []slack.Message
	cursor := ""
	for range maxThreadPages {
		msgs, hasMore, next, err := p.client.GetConversationRepliesContext(ctx, &slack.GetConversationRepliesParameters{
			ChannelID:          channelID,
			Timestamp:          threadTS,
			Oldest:             since,
			Limit:              threadPageSize,
			Cursor:             cursor,
			IncludeAllMetadata: true,
		})
		if err != nil {
			return nil, err
		}
		out = append(out, msgs...)
		if len(out) > max {
			out = out[:copy(out, out[len(out)-max:])]
		}
		if !hasMore || next == "" {
			return out, nil
		}
		cursor = next
	}
	return nil, fmt.Errorf("%w: more than %d replies in %s/%s", ErrThreadTooLong, maxThreadPages*threadPageSize, channelID, threadTS)
}

const (
	threadPageSize = 200
	maxThreadPages = 15
)

func (p *Poster) Permalink(ctx context.Context, channelID, ts string) (string, error) {
	return p.client.GetPermalinkContext(ctx, &slack.PermalinkParameters{Channel: channelID, Ts: ts})
}

func (p *Poster) BotIdentity(ctx context.Context) (userID, teamID string, err error) {
	resp, err := p.client.AuthTestContext(ctx)
	if err != nil {
		return "", "", err
	}
	return resp.UserID, resp.TeamID, nil
}

func (p *Poster) Respond(ctx context.Context, responseURL string, replaceOriginal bool, text string, blocks []slack.Block) error {
	msg := &slack.WebhookMessage{
		ResponseType:    "ephemeral",
		ReplaceOriginal: replaceOriginal,
		Text:            text,
	}
	if len(blocks) > 0 {
		msg.Blocks = &slack.Blocks{BlockSet: blocks}
	}
	return slack.PostWebhookContext(ctx, responseURL, msg)
}

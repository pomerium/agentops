// Package client adapts the slack-go client to the session.Poster
// interface used to post and update Slack messages.
package client

import (
	"context"

	"github.com/slack-go/slack"
)

// Poster posts and updates messages via the Slack Web API.
type Poster struct {
	client *slack.Client
}

// NewPoster builds a Poster from a bot token.
func NewPoster(botToken string) *Poster {
	return &Poster{client: slack.New(botToken)}
}

// PostMessage posts a message and returns its timestamp.
func (p *Poster) PostMessage(ctx context.Context, channelID string, opts ...slack.MsgOption) (string, error) {
	_, ts, err := p.client.PostMessageContext(ctx, channelID, opts...)
	return ts, err
}

// PostEphemeral posts a message visible only to userID and returns its
// timestamp.
func (p *Poster) PostEphemeral(ctx context.Context, channelID, userID string, opts ...slack.MsgOption) (string, error) {
	return p.client.PostEphemeralContext(ctx, channelID, userID, opts...)
}

// UpdateMessage updates an existing message and returns its timestamp.
func (p *Poster) UpdateMessage(ctx context.Context, channelID, ts string, opts ...slack.MsgOption) (string, error) {
	_, newTS, _, err := p.client.UpdateMessageContext(ctx, channelID, ts, opts...)
	return newTS, err
}

// AddReaction adds an emoji reaction (named without colons, e.g.
// "hourglass_flowing_sand") to the message at ts in channelID.
func (p *Poster) AddReaction(ctx context.Context, channelID, timestamp, emoji string) error {
	return p.client.AddReactionContext(ctx, emoji, slack.ItemRef{Channel: channelID, Timestamp: timestamp})
}

// RemoveReaction removes an emoji reaction from the message at ts in channelID.
func (p *Poster) RemoveReaction(ctx context.Context, channelID, timestamp, emoji string) error {
	return p.client.RemoveReactionContext(ctx, emoji, slack.ItemRef{Channel: channelID, Timestamp: timestamp})
}

// ThreadReplies returns up to max messages of a thread (root included),
// oldest first, following pagination.
func (p *Poster) ThreadReplies(ctx context.Context, channelID, threadTS string, max int) ([]slack.Message, error) {
	var out []slack.Message
	cursor := ""
	for {
		msgs, hasMore, next, err := p.client.GetConversationRepliesContext(ctx, &slack.GetConversationRepliesParameters{
			ChannelID: channelID,
			Timestamp: threadTS,
			Limit:     200,
			Cursor:    cursor,
		})
		if err != nil {
			return nil, err
		}
		out = append(out, msgs...)
		if !hasMore || len(out) >= max {
			break
		}
		cursor = next
	}
	if len(out) > max {
		out = out[:max]
	}
	return out, nil
}

// Permalink returns the canonical permalink for a message via chat.getPermalink.
func (p *Poster) Permalink(ctx context.Context, channelID, ts string) (string, error) {
	return p.client.GetPermalinkContext(ctx, &slack.PermalinkParameters{Channel: channelID, Ts: ts})
}

// BotUserID returns the bot's own Slack user id via auth.test. It is called
// once at startup so app_mention text can have the bot's mention stripped.
func (p *Poster) BotUserID(ctx context.Context) (string, error) {
	resp, err := p.client.AuthTestContext(ctx)
	if err != nil {
		return "", err
	}
	return resp.UserID, nil
}

// Respond posts (or, with replaceOriginal, edits in place) an ephemeral message
// via a Slack response_url. This is the only way to update an ephemeral message,
// which chat.update cannot touch.
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

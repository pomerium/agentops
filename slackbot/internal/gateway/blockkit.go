package gateway

import (
	"github.com/slack-go/slack"

	"github.com/pomerium/agentops/slackbot/internal/mdsplit"
)

const ActionPermission = "acp_permission"

const MaxSectionChars = 2900

func AgentMessageBlocks(text string) []slack.Block {
	if text == "" {
		text = "_(no output)_"
	}
	chunks := mdsplit.Split(text, MaxSectionChars)
	blocks := make([]slack.Block, 0, len(chunks))
	for _, c := range chunks {
		blocks = append(blocks, slack.NewSectionBlock(
			slack.NewTextBlockObject(slack.MarkdownType, c, false, false), nil, nil))
	}
	return blocks
}

type PermissionChoice struct {
	OptionID string
	Name     string
	Kind     string
}

func PermissionBlocks(sessionID, toolCallID, ownerUserID, title string, choices []PermissionChoice) []slack.Block {
	prompt := ":lock: The agent needs permission to: *" + title + "*"
	if ownerUserID != "" {
		prompt = ":lock: <@" + ownerUserID + ">, the agent needs your permission to: *" + title + "*"
	}
	section := slack.NewSectionBlock(
		slack.NewTextBlockObject(slack.MarkdownType, prompt, false, false),
		nil, nil,
	)
	elements := make([]slack.BlockElement, 0, len(choices))
	for _, ch := range choices {
		btn := slack.NewButtonBlockElement(
			ActionPermission,
			EncodePermissionValue(sessionID, toolCallID, ch.OptionID),
			slack.NewTextBlockObject(slack.PlainTextType, ch.Name, true, false),
		)
		if isAllowKind(ch.Kind) {
			btn = btn.WithStyle(slack.StylePrimary)
		} else if isRejectKind(ch.Kind) {
			btn = btn.WithStyle(slack.StyleDanger)
		}
		elements = append(elements, btn)
	}
	return []slack.Block{section, slack.NewActionBlock("acp_permission_actions", elements...)}
}

func isAllowKind(kind string) bool {
	switch kind {
	case "allow_once", "allow_always":
		return true
	}
	return false
}

func isRejectKind(kind string) bool {
	switch kind {
	case "reject_once", "reject_always":
		return true
	}
	return false
}

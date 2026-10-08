package gateway

import (
	"github.com/slack-go/slack"

	"github.com/pomerium/agentops/internal/mdsplit"
)

// ActionPermission is the Block Kit action_id used for permission approve/deny
// buttons. The button value carries the routing identifiers (see
// EncodePermissionValue).
const ActionPermission = "acp_permission"

// ActionConnectPrefix is the action_id prefix for the per-server "Connect"
// buttons in the auth prompt; the suffix is the server name and the button
// value is the connect URL.
const ActionConnectPrefix = "connect_"

// AuthStatus describes one required MCP server in the connect prompt: whether
// the user is already connected, and (when not) the short URL that opens the
// OAuth flow.
type AuthStatus struct {
	ServerName string
	Connected  bool
	URL        string // short connect URL; empty when Connected
}

// AuthPromptBlocks renders the "you need to connect these servers" message: one
// row per server, showing a "Connect" link button for servers still needing
// auth and a green checkmark for ones already connected.
func AuthPromptBlocks(workflow string, servers []AuthStatus) []slack.Block {
	header := slack.NewSectionBlock(
		slack.NewTextBlockObject(slack.MarkdownType,
			"To run *"+workflow+"* I need access to the following tools. Click to connect each one:",
			false, false),
		nil, nil,
	)
	blocks := []slack.Block{header}
	for _, s := range servers {
		if s.Connected {
			blocks = append(blocks,
				slack.NewSectionBlock(
					slack.NewTextBlockObject(slack.MarkdownType, "• :white_check_mark: Connected *"+s.ServerName+"*", false, false),
					nil, nil,
				),
			)
			continue
		}
		btn := slack.NewButtonBlockElement(ActionConnectPrefix+s.ServerName, s.URL,
			slack.NewTextBlockObject(slack.PlainTextType, "Connect "+s.ServerName, true, false)).
			WithURL(s.URL).
			WithStyle(slack.StylePrimary)
		blocks = append(blocks,
			slack.NewSectionBlock(
				slack.NewTextBlockObject(slack.MarkdownType, "• *"+s.ServerName+"*", false, false),
				nil,
				slack.NewAccessory(btn),
			),
		)
	}
	return blocks
}

// MaxSectionChars is a safe cap below Slack's 3000-char section-text limit.
const MaxSectionChars = 2900

// AgentMessageBlocks renders agent output as one or more markdown section
// blocks, splitting text so no single section exceeds Slack's 3000-char limit
// (which would otherwise make chat.update fail and the message stop updating).
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

// PermissionChoice is one option offered for a permission request.
type PermissionChoice struct {
	OptionID string
	Name     string
	Kind     string
}

// PermissionBlocks renders an interactive approve/deny prompt for a tool call.
// Each button's value encodes (sessionID, toolCallID, optionID) so the
// interaction handler can route the user's choice back into the ACP session.
func PermissionBlocks(sessionID, toolCallID, title string, choices []PermissionChoice) []slack.Block {
	section := slack.NewSectionBlock(
		slack.NewTextBlockObject(slack.MarkdownType, ":lock: The agent needs permission to: *"+title+"*", false, false),
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

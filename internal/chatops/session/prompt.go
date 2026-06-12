package session

import (
	_ "embed"
	"strings"
)

// slackFormattingRules is the app-level Slack mrkdwn formatting appendix. It is
// carried inside the binary (not duplicated into every AgentTemplate) and
// appended to each template's system prompt so the agent's replies render
// correctly in Slack. See slack_mrkdwn.md.
//
//go:embed slack_mrkdwn.md
var slackFormattingRules string

// composeSystemPrompt combines a template's own system prompt with the embedded
// Slack mrkdwn formatting rules. The template's instructions come first; the
// formatting appendix is added afterward so it constrains output without
// displacing the template's intent. Because the app always renders to Slack,
// the rules are injected even when the template defines no prompt of its own.
func composeSystemPrompt(templatePrompt string) string {
	rules := strings.TrimRight(slackFormattingRules, "\n")
	templatePrompt = strings.TrimRight(templatePrompt, "\n")
	if templatePrompt == "" {
		return rules
	}
	return templatePrompt + "\n\n" + rules
}

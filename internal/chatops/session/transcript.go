package session

import (
	"strings"

	"github.com/slack-go/slack"
)

// Transcript caps. A loop-in session seeds its first prompt with the origin
// thread's discussion; these bound how much of it is carried.
const (
	transcriptMaxMessages = 50
	transcriptMaxChars    = 8000
)

// transcriptTexts extracts the discussion texts to seed a loop-in session
// with: every human message strictly before the mention, oldest first.
// Bot messages are skipped (the bot must not quote itself or other apps), as
// are empty texts. Speakers are deliberately NOT attached — who said what must
// not become agent context (the invoking user's identity is only used for MCP
// credential brokering). In-text <@U…> mentions are message content and are
// kept verbatim.
func transcriptTexts(replies []slack.Message, beforeTS string) []string {
	var texts []string
	for _, msg := range replies {
		// Slack ts strings ("1690000000.0042") are fixed-width until 2286, so
		// lexicographic order is chronological order.
		if msg.Timestamp >= beforeTS {
			continue
		}
		if msg.BotID != "" || strings.TrimSpace(msg.Text) == "" {
			continue
		}
		texts = append(texts, msg.Text)
	}
	return texts
}

// composeLoopInPrompt builds the first-turn prompt of a loop-in session: the
// origin discussion (capped, most recent kept) framed by an instruction block,
// followed by the user's question. With no transcript it degrades to the bare
// question; with no question it asks the agent to pick up the discussion.
func composeLoopInPrompt(texts []string, question string) string {
	if question == "" {
		question = "Continue helping with the discussion above."
	}
	texts, truncated := capTranscript(texts)
	if len(texts) == 0 {
		return question
	}

	var b strings.Builder
	b.WriteString("You were looped into an ongoing Slack discussion. Prior messages from that discussion are below, oldest first. Speakers are intentionally not identified.\n\n<discussion>\n")
	if truncated {
		b.WriteString("[earlier messages omitted]\n---\n")
	}
	for i, t := range texts {
		if i > 0 {
			b.WriteString("\n---\n")
		}
		b.WriteString(t)
	}
	b.WriteString("\n</discussion>\n\nThe request: ")
	b.WriteString(question)
	return b.String()
}

// capTranscript keeps the most recent messages within both the message-count
// and character budgets, reporting whether anything was dropped.
func capTranscript(texts []string) ([]string, bool) {
	truncated := false
	if len(texts) > transcriptMaxMessages {
		texts = texts[len(texts)-transcriptMaxMessages:]
		truncated = true
	}
	chars := 0
	for i := len(texts) - 1; i >= 0; i-- {
		chars += len(texts[i])
		if chars > transcriptMaxChars {
			texts = texts[i+1:]
			truncated = true
			break
		}
	}
	return texts, truncated
}

package session

import "testing"

func TestComposeSystemPrompt_AppendsSlackFormatting(t *testing.T) {
	const user = "You are a helpful operations assistant."
	got := composeSystemPrompt(user)

	if !contains(got, user) {
		t.Fatalf("composed prompt does not contain the template prompt:\n%s", got)
	}
	// The Slack formatting appendix must be present...
	if !contains(got, "Slack") || !contains(got, "*text*") {
		t.Fatalf("composed prompt is missing the Slack mrkdwn rules:\n%s", got)
	}
	// ...and must come AFTER the template's own instructions.
	if idxUser, idxRules := index(got, user), index(got, "*text*"); idxUser > idxRules {
		t.Fatalf("Slack rules appear before the template prompt (user=%d rules=%d)", idxUser, idxRules)
	}
}

func TestComposeSystemPrompt_EmptyTemplatePromptStillFormats(t *testing.T) {
	// The app always renders to Slack, so the formatting rules must be injected
	// even when a template defines no system prompt of its own.
	got := composeSystemPrompt("")
	if !contains(got, "*text*") {
		t.Fatalf("empty template prompt produced no Slack rules:\n%q", got)
	}
}

func contains(s, sub string) bool { return index(s, sub) >= 0 }

func index(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

package sandbox

import "testing"

// The claude-agent-acp adapter reads the custom system prompt from the ACP
// session/new request's _meta.systemPrompt. Passing an OBJECT merges into the
// built-in "claude_code" preset (the adapter forces type/preset), so {append:…}
// appends our instructions to Claude Code's own prompt instead of replacing it.
// Passing a bare string would REPLACE the preset and strip Claude Code's tool
// scaffolding — which we must never do.
func TestSystemPromptMeta_AppendsToPreset(t *testing.T) {
	const prompt = "Format replies as Slack mrkdwn."
	meta := systemPromptMeta(prompt)

	sp, ok := meta["systemPrompt"].(map[string]any)
	if !ok {
		t.Fatalf("_meta.systemPrompt is not an object: %#v", meta["systemPrompt"])
	}
	if got := sp["append"]; got != prompt {
		t.Errorf("append = %v, want %q", got, prompt)
	}
	// Must NOT send a bare string (that would replace the preset).
	if _, isString := meta["systemPrompt"].(string); isString {
		t.Error("systemPrompt is a string; it would replace the claude_code preset")
	}
}

func TestSystemPromptMeta_EmptyPromptSendsNoMeta(t *testing.T) {
	if meta := systemPromptMeta(""); meta != nil {
		t.Errorf("empty prompt produced _meta = %#v, want nil", meta)
	}
}

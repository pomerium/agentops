package sandbox

import "testing"

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
	if _, isString := meta["systemPrompt"].(string); isString {
		t.Error("systemPrompt is a string; it would replace the claude_code preset")
	}
}

func TestSystemPromptMeta_EmptyPromptSendsNoMeta(t *testing.T) {
	if meta := systemPromptMeta(""); meta != nil {
		t.Errorf("empty prompt produced _meta = %#v, want nil", meta)
	}
}

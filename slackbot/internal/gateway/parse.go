package gateway

import (
	"encoding/base64"
	"encoding/json"
	"regexp"
	"strings"
)

var leadingMentionRE = regexp.MustCompile(`^\s*<@[^>]+>`)

func ParseMention(text, botUserID string) (prompt string) {
	rest := text
	if botUserID != "" {
		botRE := regexp.MustCompile(`<@` + regexp.QuoteMeta(botUserID) + `(\|[^>]*)?>`)
		if loc := botRE.FindStringIndex(text); loc != nil {
			rest = text[loc[1]:]
		} else {
			rest = leadingMentionRE.ReplaceAllString(text, "")
		}
	} else {
		rest = leadingMentionRE.ReplaceAllString(text, "")
	}
	return strings.TrimSpace(rest)
}

func encodeActionValue(parts ...string) string {
	b, err := json.Marshal(parts)
	if err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeActionValue(v string, want int) ([]string, bool) {
	b, err := base64.RawURLEncoding.DecodeString(v)
	if err != nil {
		return nil, false
	}
	var parts []string
	if err := json.Unmarshal(b, &parts); err != nil || len(parts) != want {
		return nil, false
	}
	return parts, true
}

func EncodePermissionValue(sessionID, toolCallID, optionID string) string {
	return encodeActionValue(sessionID, toolCallID, optionID)
}

func DecodePermissionValue(v string) (sessionID, toolCallID, optionID string, ok bool) {
	parts, ok := decodeActionValue(v, 3)
	if !ok {
		return "", "", "", false
	}
	return parts[0], parts[1], parts[2], true
}

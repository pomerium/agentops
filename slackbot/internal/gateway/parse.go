package gateway

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"regexp"
	"strings"
)

var leadingMentionRE = regexp.MustCompile(`^\s*<@[^>]+>`)

func ParseMention(text, botUserID string) (prompt string) {
	if botUserID != "" {
		botRE := regexp.MustCompile(`[ \t]*<@` + regexp.QuoteMeta(botUserID) + `(\|[^>]*)?>[ \t]*`)
		if botRE.MatchString(text) {
			return strings.TrimSpace(botRE.ReplaceAllString(text, " "))
		}
	}
	return strings.TrimSpace(leadingMentionRE.ReplaceAllString(text, ""))
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

const maxButtonValue = 2000

const permissionTokenPrefix = "pt."

func PermissionValue(sessionID, toolCallID, optionID string) string {
	if v := EncodePermissionValue(sessionID, toolCallID, optionID); len(v) <= maxButtonValue {
		return v
	}
	sum := sha256.Sum256([]byte(sessionID + "\x00" + toolCallID + "\x00" + optionID))
	return permissionTokenPrefix + hex.EncodeToString(sum[:16])
}

func IsPermissionToken(v string) bool { return strings.HasPrefix(v, permissionTokenPrefix) }

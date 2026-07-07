package gateway

import (
	"regexp"
	"strings"
)

// leadingMentionRE matches a single mention token at the start of a message,
// used to strip the bot's mention when its user id is unknown.
var leadingMentionRE = regexp.MustCompile(`^\s*<@[^>]+>`)

// ParseMention extracts the initial prompt from an app_mention's text: the
// whole message with the bot's mention stripped (the channel's binding, not
// the message, selects the agent template). For "<@U0BOT> say hi" with
// botUserID="U0BOT" this returns "say hi"; a bare mention yields "". Text
// before the mention is ignored, so "hey <@U0BOT> deploy now" still yields
// "deploy now". When botUserID is empty (the bot id could not be discovered)
// the leading "<@…>" token is stripped instead, so the common
// mention-leads-the-message case still parses.
func ParseMention(text, botUserID string) (prompt string) {
	rest := text
	if botUserID != "" {
		botRE := regexp.MustCompile(`<@` + regexp.QuoteMeta(botUserID) + `(\|[^>]*)?>`)
		if loc := botRE.FindStringIndex(text); loc != nil {
			rest = text[loc[1]:] // everything after the bot mention
		} else {
			rest = leadingMentionRE.ReplaceAllString(text, "")
		}
	} else {
		rest = leadingMentionRE.ReplaceAllString(text, "")
	}
	return strings.TrimSpace(rest)
}

// permissionValueSep separates the parts encoded in a permission button value.
const permissionValueSep = "\x1f" // ASCII unit separator, won't appear in ids

// EncodePermissionValue packs the routing identifiers for a permission button
// into a single Block Kit action value.
func EncodePermissionValue(sessionID, toolCallID, optionID string) string {
	return strings.Join([]string{sessionID, toolCallID, optionID}, permissionValueSep)
}

// DecodePermissionValue reverses EncodePermissionValue.
func DecodePermissionValue(v string) (sessionID, toolCallID, optionID string, ok bool) {
	parts := strings.Split(v, permissionValueSep)
	if len(parts) != 3 {
		return "", "", "", false
	}
	return parts[0], parts[1], parts[2], true
}

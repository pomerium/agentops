package app

import (
	"fmt"
	"strings"
	"time"

	"github.com/pomerium/agentops/harness/api"
	pb "github.com/pomerium/agentops/harness/api/pb"
)

const (
	msgStatusPreparing            = ":hourglass_flowing_sand: Getting ready…"
	msgStatusAwaitingApproval     = ":lock: Waiting for approval — I've sent you the link by DM."
	msgStatusReady                = ":rocket: Ready — reply in this thread to talk to me."
	msgStatusReadyWorking         = ":rocket: Ready. Working on it…"
	msgStatusFinished             = ":checkered_flag: Finished. @mention me here and I'll continue in a fresh session."
	msgStatusIdleEnded            = ":zzz: Nothing came through for a while, so I've closed this session and freed the workspace. @mention me here and I'll continue in a fresh session."
	msgStatusIdleSuspended        = ":zzz: Nothing came through for a while, so I've paused this session to free up the workspace — the conversation and your files are kept. @mention me here and I'll pick it up where we left off."
	msgStatusContinuing           = ":arrows_counterclockwise: Picking this back up — getting the workspace running again…"
	msgStatusCannotContinue       = ":warning: I couldn't pick this conversation back up, so I'm starting a fresh session right here with the discussion so far. Anything the earlier run had in progress starts over."
	msgStatusRetryContinue        = ":warning: I couldn't pick this conversation back up this time. It's still paused, with your files kept — @mention me here to try again."
	msgStatusWorkspaceUnreachable = ":warning: Approved, thanks — but the workspace hasn't connected back to me yet, and it should have by now. Still retrying. If this doesn't clear shortly, something is between us and an admin should check the app logs."
	msgStatusInterrupted          = ":warning: I had to restart, so this session is gone. @mention me here and I'll continue in a fresh session with the conversation so far (fresh workspace — anything in progress starts over)."
)

func statusWorkspaceReleased(window time.Duration) string {
	return fmt.Sprintf(":wastebasket: Nothing picked this back up within %s, so I've released the workspace — "+
		"that conversation and your files are gone. @mention me here and I'll continue in a fresh session.",
		humanWindow(window))
}

func readyStatusFor(working bool) string {
	if working {
		return msgStatusReadyWorking
	}
	return msgStatusReady
}

func statusStopped(reason string) string {
	return ":x: Stopped: " + reason + ". Nothing is left running."
}

const (
	stopPrepareFailed  = "I couldn't get a workspace ready"
	stopRunFailed      = "I couldn't create the approval request"
	stopDMFailed       = "I couldn't DM you the approval link — check your DM settings, then @mention me again"
	stopActivateFailed = "I couldn't start the workspace"
	stopNeverConnected = "the workspace started but never connected back to me, so an admin needs to check the app logs"
)

func statusLapsed(window time.Duration, workflow string) string {
	return fmt.Sprintf(":x: No approval came through in %s, so I've stopped and cleaned up. "+
		"If the approval page wouldn't let you approve, you don't have access to what *%s* needs — "+
		"ask an admin rather than trying again.", humanMinutes(window), workflow)
}

const msgHintLaunching = ":hourglass_flowing_sand: Not running yet — I'm still waiting for the approval (check your DMs). I won't see anything posted here before then."

func approvalDMText(userID, workflow, threadLink string) string {
	return fmt.Sprintf(":lock: <@%s>, you asked for *%s* in %s. Approve it and I'll start.",
		userID, workflow, threadLink)
}

func approvalDMFallback(userID, workflow, threadLink, approvalURL string) string {
	return approvalDMText(userID, workflow, threadLink) + " " + linkOr(approvalURL, "Review & approve")
}

func approvalDMDoneText(threadLink string) string {
	return ":white_check_mark: Approved — your agent is running in " + threadLink + "."
}

func approvalDMStaleText(threadLink string) string {
	return ":warning: Interrupted by a restart — do not approve. @mention me in " + threadLink +
		" and I'll start again."
}

func idleWarning(lead time.Duration) string {
	return fmt.Sprintf(":hourglass: Nothing's come through in a while, so I'll close this session in about %s "+
		"and free up the workspace. Reply and I'll keep it going.", humanMinutes(lead))
}

func idleWarningDM(lead time.Duration, threadLink string) string {
	return fmt.Sprintf(":hourglass: Your agent in %s has been idle for a while, so I'll close its session in about %s "+
		"and free up the workspace. @mention me there and I'll keep it going.", threadLink, humanMinutes(lead))
}

const (
	msgIdleKeptAlive = ":white_check_mark: Never mind — your session is going again, so I'm not closing it."
	msgIdleClosed    = ":zzz: That session has closed. @mention me in the thread and I'll pick it up from where it left off."
)

const (
	msgTurnFailed   = ":x: The agent encountered an error."
	msgTurnRejected = ":x: I couldn't send that to the agent just now. @mention me here to try again."
	msgHintTryAgain = ":hourglass_flowing_sand: I'm already picking this conversation up from another message. Give it a moment, then @mention me again if nothing happens."
)

const (
	msgNoAgentForChannel = ":wave: No agent is configured for this channel yet. Ask an admin to set one up for this channel."
	msgLookupFailed      = ":warning: I couldn't look up the agent for this channel — something is broken on my side. Ask an admin to check the app logs."
)

func missingAgent(name string) string {
	return fmt.Sprintf(":warning: This channel is set up to run agent `%s`, but no such agent exists. "+
		"This is a configuration problem — ask an admin to fix the channel's binding or create the agent.", name)
}

const msgHintEnded = ":information_source: Your session in this thread has ended. @mention me to start a new session here — I'll bring the conversation so far."

const msgHintDiscussion = ":information_source: Plain messages here are discussion — I'll read them. @mention me and I'll act on it in a session of your own, right in this thread."

const msgRoomMultiplayer = ":busts_in_silhouette: This thread is now multiplayer — I act on @mentions only, from anyone, each in their own session. Everything else is discussion I'll read along the way."

func hintPermissionNotYours(ownerUserID string) string {
	return fmt.Sprintf(":lock: That one's <@%s>'s call — the agent is using their access, so the choice has to be theirs. "+
		"If they're not around, @mention me and I'll pick this up in a session of your own.", ownerUserID)
}

func permissionResolvedText(p *pb.PermissionResolved) string {
	switch p.GetUnanswered() {
	case api.ResolutionExpired:
		return ":hourglass: Nobody answered in time, so I cancelled that tool call."
	case api.ResolutionSuperseded:
		return ":information_source: This session ended before that tool call was answered."
	default:
		return ":white_check_mark: Answered — thanks."
	}
}

const msgHintPermissionStale = ":information_source: This session has ended, so that button doesn't do anything any more. @mention me in the thread and I'll continue in a fresh session."

func joinNotice(userID, workflow string, carried int, flags carryoverFlags) string {
	line := fmt.Sprintf(":twisted_rightwards_arrows: <@%s> joined with their own agent (*%s*)", userID, workflow)
	switch {
	case carried > 0:
		line += fmt.Sprintf(" — I've read the %s in this thread.", plural(carried, "message"))
	default:
		line += " — I don't have this conversation, so I'm going on their message alone."
	}
	if flags.truncated && carried > 0 {
		line += " (The most recent part only; the beginning isn't included.)"
	}
	if flags.inflight {
		line += " Another agent was still answering, so its last reply may be incomplete."
	}
	return line + " Waiting for their approval — separate workspace, separate approval."
}

func retemplatedRefusal(workflow, threadWorkflow string) string {
	return fmt.Sprintf(":warning: This channel's agent changed since this thread started — I'm *%s* now, "+
		"this thread's agents are *%s* — so I can't join this thread. To use *%s*, @mention me in a new top-level message.",
		workflow, threadWorkflow, workflow)
}

func approvalAsk(originKind string) string {
	switch originKind {
	case originJoin:
		return "Join a Slack thread with an agent session of my own."
	default:
		return "Start an agent session in this Slack thread."
	}
}

func provenanceClause(carried int, link string) string {
	clause := fmt.Sprintf(" [continues another person's conversation: %s carried", plural(carried, "message"))
	if link != "" {
		clause += ", " + link
	}
	return clause + "]"
}

func attribution(ownerUserID string) string {
	return "↳ for <@" + ownerUserID + ">"
}

func ownerPrefixed(ownerUserID, text string) string {
	if ownerUserID == "" {
		return text
	}
	prefix := fmt.Sprintf("<@%s>'s agent: ", ownerUserID)
	if rest, ok := afterLeadingEmoji(text); ok {
		return text[:len(text)-len(rest)] + prefix + rest
	}
	return prefix + text
}

func afterLeadingEmoji(text string) (rest string, ok bool) {
	if !strings.HasPrefix(text, ":") {
		return "", false
	}
	end := strings.Index(text[1:], ": ")
	if end < 0 {
		return "", false
	}
	return text[end+3:], true
}

func plural(n int, noun string) string {
	if n == 1 {
		return "1 " + noun
	}
	return fmt.Sprintf("%d %ss", n, noun)
}

func humanWindow(d time.Duration) string {
	switch hours := int(d.Hours()); {
	case hours >= 48:
		return fmt.Sprintf("%d days", hours/24)
	case hours >= 24:
		return "1 day"
	case hours >= 2:
		return fmt.Sprintf("%d hours", hours)
	case hours >= 1:
		return "1 hour"
	default:
		return humanMinutes(d)
	}
}

func humanMinutes(d time.Duration) string {
	mins := int(d.Round(time.Minute).Minutes())
	switch {
	case mins <= 0:
		return d.String()
	case mins == 1:
		return "1 minute"
	default:
		return fmt.Sprintf("%d minutes", mins)
	}
}

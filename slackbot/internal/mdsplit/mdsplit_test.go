package mdsplit_test

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/pomerium/agentops/slackbot/internal/mdsplit"
)

func TestSplitShortTextUnchanged(t *testing.T) {
	in := "# Title\n\nA short paragraph.\n"
	got := mdsplit.Split(in, 1000)
	if len(got) != 1 || got[0] != in {
		t.Fatalf("short text should pass through unchanged, got %q", got)
	}
}

func TestSplitEveryPartWithinLimit(t *testing.T) {
	limit := 200
	var b strings.Builder
	for range 50 {
		b.WriteString("This is paragraph number with some words in it.\n\n")
	}
	for i, p := range mdsplit.Split(b.String(), limit) {
		if len(p) > limit {
			t.Errorf("part %d is %d bytes, exceeds limit %d", i, len(p), limit)
		}
	}
}

func TestSplitParagraphsAreLossless(t *testing.T) {
	var b strings.Builder
	for range 30 {
		b.WriteString("Paragraph with a handful of words.\n\n")
	}
	in := b.String()
	parts := mdsplit.Split(in, 300)
	if len(parts) < 2 {
		t.Fatalf("expected the text to split, got %d part(s)", len(parts))
	}
	if got := strings.Join(parts, ""); got != in {
		t.Errorf("rejoined parts differ from input:\n got %q\nwant %q", got, in)
	}
}

func TestSplitKeepsCodeBlockIntact(t *testing.T) {
	pre := strings.Repeat("filler line\n", 20) + "\n"
	code := "```go\nfunc main() {\n\tprintln(\"hi\")\n}\n```\n"
	parts := mdsplit.Split(pre+code, 180)

	var found bool
	for _, p := range parts {
		if strings.Contains(p, "```go") {
			found = true
			if !strings.Contains(p, "println(\"hi\")") || strings.Count(p, "```") != 2 {
				t.Errorf("code block was split across parts; part = %q", p)
			}
		}
	}
	if !found {
		t.Fatal("code block disappeared from output")
	}
}

func TestSplitReFencesOversizedCodeBlock(t *testing.T) {
	var body strings.Builder
	for range 100 {
		body.WriteString("line of code that is reasonably long here\n")
	}
	in := "```python\n" + body.String() + "```\n"
	parts := mdsplit.Split(in, 400)
	if len(parts) < 2 {
		t.Fatalf("expected the code block to split, got %d part(s)", len(parts))
	}
	for i, p := range parts {
		if !strings.HasPrefix(strings.TrimSpace(p), "```") {
			t.Errorf("part %d does not open with a fence: %q", i, p)
		}
		if strings.Count(p, "```") < 2 {
			t.Errorf("part %d is not a closed fenced block: %q", i, p)
		}
		if len(p) > 400 {
			t.Errorf("part %d is %d bytes, exceeds limit", i, len(p))
		}
	}
}

func TestSplitKeepsTableIntact(t *testing.T) {
	pre := strings.Repeat("intro line\n", 15) + "\n"
	table := "| a | b |\n| --- | --- |\n| 1 | 2 |\n| 3 | 4 |\n"
	parts := mdsplit.Split(pre+table, 140)

	var tableParts int
	for _, p := range parts {
		if strings.Contains(p, "| a | b |") || strings.Contains(p, "| 1 | 2 |") {
			tableParts++
		}
	}
	if tableParts != 1 {
		t.Errorf("table rows scattered across %d parts, want 1; parts=%v", tableParts, parts)
	}
}

func TestSplitRepeatsTableHeaderWhenOversized(t *testing.T) {
	var b strings.Builder
	b.WriteString("| name | value |\n| --- | --- |\n")
	for range 80 {
		b.WriteString("| some-key | some-value |\n")
	}
	parts := mdsplit.Split(b.String(), 300)
	if len(parts) < 2 {
		t.Fatalf("expected the table to split, got %d part(s)", len(parts))
	}
	for i, p := range parts {
		if !strings.Contains(p, "| name | value |") || !strings.Contains(p, "| --- | --- |") {
			t.Errorf("part %d is missing the repeated table header: %q", i, p)
		}
		if len(p) > 300 {
			t.Errorf("part %d is %d bytes, exceeds limit", i, len(p))
		}
	}
}

func TestSplitHardSplitsOversizedLine(t *testing.T) {
	in := strings.Repeat("x", 1000)
	parts := mdsplit.Split(in, 256)
	if len(parts) < 4 {
		t.Fatalf("expected several parts, got %d", len(parts))
	}
	for i, p := range parts {
		if len(p) > 256 {
			t.Errorf("part %d is %d bytes, exceeds limit", i, len(p))
		}
	}
	if got := strings.Join(parts, ""); got != in {
		t.Error("rune-split parts must rejoin to the original")
	}
}

func TestSplitRuneSafe(t *testing.T) {
	in := strings.Repeat("héllo wörld ", 100)
	for i, p := range mdsplit.Split(in, 50) {
		if !utf8.ValidString(p) {
			t.Errorf("part %d contains an invalid UTF-8 sequence: %q", i, p)
		}
	}
}

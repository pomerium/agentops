// Package mdsplit splits Markdown text into size-bounded parts on structural
// boundaries. It is used to fit agent output into Slack messages (and the
// section blocks within them), each of which has a hard byte ceiling.
//
// Splitting prefers, in order: between paragraphs (blank-line-separated
// blocks), then between lines, then — only for a single line longer than the
// limit — between runes. A fenced code block is never cut in the middle; an
// oversized one is re-split with its fence repeated so each part stays a valid
// code block. A table is likewise kept whole, repeating its header + separator
// row when it must be split, so neither half renders as broken.
package mdsplit

import (
	"strings"
	"unicode/utf8"
)

// Split breaks text into the fewest parts each at most limit bytes. It always
// returns at least one part. When every structural block already fits, joining
// the parts reproduces the input exactly.
func Split(text string, limit int) []string {
	if limit <= 0 || len(text) <= limit {
		return []string{text}
	}

	var parts []string
	var cur strings.Builder
	flush := func() {
		if cur.Len() > 0 {
			parts = append(parts, cur.String())
			cur.Reset()
		}
	}

	for _, blk := range blocks(text) {
		if len(blk) > limit {
			flush()
			parts = append(parts, splitBlock(blk, limit)...)
			continue
		}
		if cur.Len()+len(blk) > limit {
			flush()
		}
		cur.WriteString(blk)
	}
	flush()

	if len(parts) == 0 {
		return []string{""}
	}
	return parts
}

// blocks tokenizes text into the units that should ideally stay together: each
// fenced code block is one unit, and every other paragraph (a run of non-blank
// lines plus its trailing blank line) is one unit. Concatenating the units
// reproduces the input.
func blocks(text string) []string {
	lines := strings.SplitAfter(text, "\n")
	var out []string
	var cur strings.Builder
	flush := func() {
		if cur.Len() > 0 {
			out = append(out, cur.String())
			cur.Reset()
		}
	}

	for i := 0; i < len(lines); i++ {
		line := lines[i]
		if line == "" { // trailing artifact of SplitAfter
			continue
		}
		trimmed := strings.TrimSpace(line)

		if marker := fenceMarker(trimmed); marker != "" {
			flush()
			var cb strings.Builder
			cb.WriteString(line)
			for i+1 < len(lines) {
				i++
				cb.WriteString(lines[i])
				if isClosingFence(strings.TrimSpace(lines[i]), marker) {
					break
				}
			}
			out = append(out, cb.String())
			continue
		}

		cur.WriteString(line)
		if trimmed == "" { // blank line closes the current paragraph
			flush()
		}
	}
	flush()
	return out
}

// splitBlock hard-splits a single block that exceeds the limit, choosing the
// strategy that keeps the block's structure readable.
func splitBlock(blk string, limit int) []string {
	if marker := fenceMarker(strings.TrimSpace(firstLine(blk))); marker != "" {
		return splitCodeBlock(blk, marker, limit)
	}
	if isTable(blk) {
		return splitTable(blk, limit)
	}
	return packLines(strings.SplitAfter(blk, "\n"), limit)
}

// splitCodeBlock splits an oversized fenced block, repeating the opening fence
// and a closing fence on every part.
func splitCodeBlock(blk, marker string, limit int) []string {
	lines := strings.SplitAfter(blk, "\n")
	open := lines[0]
	rest := lines[1:]

	// Peel off the trailing closing fence so we can re-add it per part.
	closeLine := marker
	for last := len(rest) - 1; last >= 0; last-- {
		if rest[last] == "" {
			continue
		}
		if isClosingFence(strings.TrimSpace(rest[last]), marker) {
			closeLine = strings.TrimRight(rest[last], "\r\n")
			rest = rest[:last]
		}
		break
	}

	overhead := len(open) + len(closeLine) + 1 // +1 for the newline before close
	budget := limit - overhead
	if budget <= 0 {
		budget = limit / 2 // degenerate: fence longer than the limit
	}

	var parts []string
	for _, chunk := range packLines(rest, budget) {
		var b strings.Builder
		b.WriteString(open)
		b.WriteString(chunk)
		if !strings.HasSuffix(chunk, "\n") {
			b.WriteByte('\n')
		}
		b.WriteString(closeLine)
		parts = append(parts, b.String())
	}
	if len(parts) == 0 {
		parts = append(parts, open+closeLine)
	}
	return parts
}

// splitTable splits an oversized table, repeating the header and separator rows
// on every part.
func splitTable(blk string, limit int) []string {
	lines := nonEmpty(strings.SplitAfter(blk, "\n"))
	if len(lines) < 2 {
		return packLines(strings.SplitAfter(blk, "\n"), limit)
	}
	header := lines[0] + lines[1]
	rows := lines[2:]

	budget := limit - len(header)
	if budget <= 0 {
		return packLines(strings.SplitAfter(blk, "\n"), limit)
	}

	var parts []string
	for _, chunk := range packLines(rows, budget) {
		parts = append(parts, header+chunk)
	}
	if len(parts) == 0 {
		parts = append(parts, header)
	}
	return parts
}

// packLines greedily concatenates lines into parts of at most limit bytes,
// rune-splitting any single line that is itself too long.
func packLines(lines []string, limit int) []string {
	var parts []string
	var cur strings.Builder
	flush := func() {
		if cur.Len() > 0 {
			parts = append(parts, cur.String())
			cur.Reset()
		}
	}
	for _, ln := range lines {
		if ln == "" {
			continue
		}
		if len(ln) > limit {
			flush()
			parts = append(parts, runeSplit(ln, limit)...)
			continue
		}
		if cur.Len()+len(ln) > limit {
			flush()
		}
		cur.WriteString(ln)
	}
	flush()
	return parts
}

// runeSplit cuts s into pieces of at most limit bytes, never inside a rune.
func runeSplit(s string, limit int) []string {
	var parts []string
	for len(s) > limit {
		cut := limit
		for cut > 0 && !utf8.RuneStart(s[cut]) {
			cut--
		}
		if cut == 0 { // a single rune wider than the limit; cut anyway
			cut = limit
		}
		parts = append(parts, s[:cut])
		s = s[cut:]
	}
	if s != "" {
		parts = append(parts, s)
	}
	return parts
}

// fenceMarker returns the backtick/tilde run that opens a code fence, or "".
func fenceMarker(trimmed string) string {
	for _, c := range []byte{'`', '~'} {
		if n := runLen(trimmed, c); n >= 3 {
			return strings.Repeat(string(c), n)
		}
	}
	return ""
}

// isClosingFence reports whether trimmed is a closing fence for marker: the
// same character, at least as long, and nothing else on the line.
func isClosingFence(trimmed, marker string) bool {
	if marker == "" || trimmed == "" {
		return false
	}
	c := marker[0]
	if runLen(trimmed, c) != len(trimmed) {
		return false
	}
	return len(trimmed) >= len(marker)
}

// isTable reports whether blk is a pipe table: its second non-blank line is a
// separator row (dashes, optional colons and pipes).
func isTable(blk string) bool {
	lines := nonEmpty(strings.SplitAfter(blk, "\n"))
	if len(lines) < 2 {
		return false
	}
	s := strings.TrimSpace(lines[1])
	if s == "" {
		return false
	}
	dash := false
	for _, r := range s {
		switch r {
		case '-':
			dash = true
		case '|', ':', ' ', '\t':
		default:
			return false
		}
	}
	return dash
}

func runLen(s string, c byte) int {
	n := 0
	for n < len(s) && s[n] == c {
		n++
	}
	return n
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i+1]
	}
	return s
}

func nonEmpty(lines []string) []string {
	out := lines[:0:0]
	for _, l := range lines {
		if l != "" {
			out = append(out, l)
		}
	}
	return out
}

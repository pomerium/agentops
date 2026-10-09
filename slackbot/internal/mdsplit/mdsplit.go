package mdsplit

import (
	"strings"
	"unicode/utf8"
)

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
		if line == "" {
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
		if trimmed == "" {
			flush()
		}
	}
	flush()
	return out
}

func splitBlock(blk string, limit int) []string {
	if marker := fenceMarker(strings.TrimSpace(firstLine(blk))); marker != "" {
		return splitCodeBlock(blk, marker, limit)
	}
	if isTable(blk) {
		return splitTable(blk, limit)
	}
	return packLines(strings.SplitAfter(blk, "\n"), limit)
}

func splitCodeBlock(blk, marker string, limit int) []string {
	lines := strings.SplitAfter(blk, "\n")
	open := lines[0]
	rest := lines[1:]

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

	overhead := len(open) + len(closeLine) + 1
	budget := limit - overhead
	if budget <= 0 {
		budget = limit / 2
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

func runeSplit(s string, limit int) []string {
	var parts []string
	for len(s) > limit {
		cut := limit
		for cut > 0 && !utf8.RuneStart(s[cut]) {
			cut--
		}
		if cut == 0 {
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

func fenceMarker(trimmed string) string {
	for _, c := range []byte{'`', '~'} {
		if n := runLen(trimmed, c); n >= 3 {
			return strings.Repeat(string(c), n)
		}
	}
	return ""
}

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

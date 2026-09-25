package main

import (
	"fmt"
	"regexp"
	"strings"
)

// The .proto is the contract a client in any language reads first, so the value
// sets its free-string fields carry are rendered into its comments rather than
// left for a reader to go and find. Each region sits at the field it describes;
// the text between the markers is this renderer's and is replaced on every run.
const (
	protoBegin = "BEGIN GENERATED: %s (make vocab)"
	protoEnd   = "END GENERATED: %s"
	protoWidth = 80
)

var protoRegion = regexp.MustCompile(`(?m)^([ \t]*)// BEGIN GENERATED: (\S+) \(make vocab\)\n`)

// renderProto splices every generated region of the .proto. A region this
// renderer does not know, one it expects but cannot find, or one with no end
// marker is an error rather than a silently skipped list.
func renderProto(src source, current []byte) ([]byte, error) {
	regions, err := protoRegions(src)
	if err != nil {
		return nil, err
	}
	doc := string(current)
	seen := map[string]bool{}
	var out strings.Builder
	for {
		m := protoRegion.FindStringSubmatchIndex(doc)
		if m == nil {
			out.WriteString(doc)
			break
		}
		indent, name := doc[m[2]:m[3]], doc[m[4]:m[5]]
		body, ok := regions[name]
		if !ok {
			return nil, fmt.Errorf("unknown generated region %q", name)
		}
		if seen[name] {
			return nil, fmt.Errorf("generated region %q appears twice", name)
		}
		seen[name] = true
		endLine := indent + "// " + fmt.Sprintf(protoEnd, name) + "\n"
		rest := doc[m[1]:]
		end := strings.Index(rest, endLine)
		if end < 0 {
			return nil, fmt.Errorf("generated region %q has no end marker", name)
		}
		out.WriteString(doc[:m[1]])
		for _, line := range body {
			if line == "" {
				out.WriteString(indent + "//\n")
			} else {
				out.WriteString(indent + "// " + line + "\n")
			}
		}
		out.WriteString(endLine)
		doc = rest[end+len(endLine):]
	}
	for name := range regions {
		if !seen[name] {
			return nil, fmt.Errorf("generated region %q is missing from the proto", name)
		}
	}
	return []byte(out.String()), nil
}

// protoRegions is the body of every region, as comment lines without the "// "
// prefix, which renderProto adds at the region's own indentation.
func protoRegions(src source) (map[string][]string, error) {
	byName := map[string]payload{}
	for _, p := range src.Payloads {
		byName[p.Name] = p
	}
	named := map[string]bool{}
	regions := map[string][]string{}
	for _, v := range src.Vocabularies {
		var lines []string
		for _, val := range v.Values {
			doc := val.Doc
			if val.Live {
				doc += " (live: the session still holds cluster resources.)"
			}
			lines = append(lines, hanging(fmt.Sprintf("%q: ", val.Value), doc)...)
			if val.Payload == "" {
				continue
			}
			p, ok := byName[val.Payload]
			if !ok {
				return nil, fmt.Errorf("%s %q names payload %q, which payloads.yaml does not define", v.Name, val.Value, val.Payload)
			}
			named[p.Name] = true
			lines = append(lines, payloadLines(p, "    ")...)
		}
		regions["vocab:"+v.Name] = lines
	}

	// A payload no event names is a nested shape; it is listed once, after the
	// events, under its own name.
	var nested []string
	for _, p := range src.Payloads {
		if named[p.Name] {
			continue
		}
		nested = append(nested, hanging(p.Name+": ", firstSentence(p.Doc))...)
		nested = append(nested, payloadLines(p, "    ")...)
	}
	regions["payloads:nested"] = nested

	var sentinels []string
	for _, s := range src.Sentinels.Values {
		sentinels = append(sentinels, hanging(fmt.Sprintf("%q (%s): ", s.Value, s.Code), oneLine(s.Doc))...)
	}
	regions["sentinels"] = sentinels
	return regions, nil
}

// payloadLines lists a payload's JSON keys under its event: the key, its type,
// whether it may be absent, and what it means.
func payloadLines(p payload, indent string) []string {
	if len(p.Fields) == 0 {
		return []string{indent + "payload: {} (no keys)"}
	}
	lines := []string{indent + "payload:"}
	for _, f := range p.Fields {
		head := fmt.Sprintf("%s  %s (%s", indent, f.Name, f.Type)
		if f.Optional {
			head += ", absent when zero"
		}
		head += ")"
		if f.Doc == "" {
			lines = append(lines, head)
			continue
		}
		lines = append(lines, hanging(head+": ", oneLine(f.Doc))...)
	}
	return lines
}

// hanging wraps text after a lead-in, continuing lines under the lead-in's own
// indentation plus four, so a long entry still reads as one item.
func hanging(lead, text string) []string {
	cont := strings.Repeat(" ", len(lead)-len(strings.TrimLeft(lead, " "))+4)
	words := strings.Fields(oneLine(text))
	var lines []string
	line := lead
	for _, w := range words {
		if len(line)+len(w) > protoWidth-4 && strings.TrimSpace(line) != strings.TrimSpace(lead) {
			lines = append(lines, strings.TrimRight(line, " "))
			line = cont
		}
		line += w + " "
	}
	return append(lines, strings.TrimRight(line, " "))
}

func oneLine(s string) string { return strings.Join(strings.Fields(s), " ") }

func firstSentence(s string) string {
	first, _, _ := strings.Cut(oneLine(s), ". ")
	return strings.TrimSuffix(first, ".") + "."
}

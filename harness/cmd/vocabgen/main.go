// Command vocabgen renders the Harness API's two hand-written schemas — the
// string vocabularies in proto/vocabulary.yaml and the event payload shapes in
// proto/payloads.yaml — into every place they are named: the Go api package, the
// Go wire mapping, the TypeScript SDK, the Python SDK and docs/clients.md.
//
// It exists because all of it was previously declared several times over, in
// four languages and one table of prose, with nothing to notice when one copy
// gained a value or a field the others did not.
//
// The two halves are rendered differently on purpose. The vocabularies become
// CONSTANTS and never an enum something parses or validates against, because a
// client must tolerate a value it has not heard of. The payloads become the
// structs the harness itself marshals, because the producer's own types are the
// only definition of a payload a consumer can be held to.
//
// Run it through `make generate` (or `make vocab`); the outputs are committed,
// and `make vocab-check` — which runs this with -check — fails if they no longer
// match the source.
package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"go/format"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"connectrpc.com/connect"
	"sigs.k8s.io/yaml"
)

// source is the two schema files composed: the vocabularies and the payload
// shapes they name.
type source struct {
	Vocabularies []vocabulary
	Sentinels    sentinels
	// Payloads is proto/payloads.yaml — the shape of each event's JSON body,
	// which the events here name and nothing else knows.
	Payloads []payload
}

// vocabulary is one named set of strings — the event types, the session states,
// one of the reason sets.
type vocabulary struct {
	// Name is the symbol the TypeScript and Python SDKs export, and the Go type
	// where there is one. Every other identifier is derived from it or from the
	// value.
	Name string `json:"name"`
	// GoTyped makes the Go constants a named string type (Name) rather than
	// untyped constants.
	GoTyped bool `json:"go_typed"`
	// GoPrefix is what Go prepends to the derived name, because Go has no
	// namespace to hang them off: "Event" + "StateChanged".
	GoPrefix string `json:"go_prefix"`
	Doc      string `json:"doc"`
	// DocsRegion names the generated region of docs/clients.md that lists these
	// values. Empty means the documentation does not list them.
	DocsRegion string  `json:"docs_region"`
	Values     []value `json:"values"`
}

type value struct {
	Value string `json:"value"`
	Doc   string `json:"doc"`
	// Payload names the shape in proto/payloads.yaml that the event carries.
	// It is the only place the two schemas are joined, and it is checked rather
	// than believed.
	Payload string `json:"payload"`
	// Live marks a value that still owns cluster resources. Only a vocabulary
	// with a Go type may carry it: the set is emitted as a predicate on that type.
	Live bool `json:"live"`
}

type sentinels struct {
	Name string `json:"name"`
	// StripPrefix is dropped from the wire name to make the SDK identifier:
	// "ErrNotFound" is Sentinel.NotFound and Sentinel.NOT_FOUND.
	StripPrefix string `json:"strip_prefix"`
	Doc         string `json:"doc"`
	// HTTPStatus is keyed by Connect code, not by sentinel name, because the
	// companion's own REST facade maps codes — a per-name list here is how the
	// documentation comes to promise a status nothing returns.
	HTTPStatus map[string]int `json:"http_status"`
	Values     []sentinel     `json:"values"`
}

type sentinel struct {
	Value   string `json:"value"`
	Message string `json:"message"`
	Doc     string `json:"doc"`
	// Code is the Connect code's wire name ("not_found"), checked against
	// connect's own parser and rendered as connect.CodeNotFound.
	Code string `json:"code"`
	// Means is the documentation table's gloss. Empty takes the first sentence
	// of Doc, which is what most of them would say anyway.
	Means string `json:"means"`
}

func main() {
	root := flag.String("root", ".", "path to the repository root")
	check := flag.Bool("check", false, "report artifacts that are out of date instead of writing them")
	flag.Parse()

	if err := run(*root, *check); err != nil {
		fmt.Fprintln(os.Stderr, "vocabgen:", err)
		os.Exit(1)
	}
}

// artifacts are the generated files, in one list so that adding an output
// cannot leave a check behind. render receives the file's current content,
// which the documentation regions are spliced into and the rest ignore.
var artifacts = []struct {
	path   string
	render func(source, []byte) ([]byte, error)
}{
	{"harness/api/vocab.go", renderGoAPI},
	{"harness/api/wire/vocab.go", renderGoWire},
	{"harness/api/payloads.go", renderGoPayloads},
	{"docs/clients.md", renderDocs},
	{"proto/harnessapi/v1/harnessapi.proto", renderProto},
}

func run(root string, check bool) error {
	src, err := load(root)
	if err != nil {
		return err
	}

	var stale []string
	for _, a := range artifacts {
		path := filepath.Join(root, a.path)
		current, err := os.ReadFile(path)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		want, err := a.render(src, current)
		if err != nil {
			return fmt.Errorf("%s: %w", a.path, err)
		}
		if bytes.Equal(current, want) {
			continue
		}
		if check {
			stale = append(stale, a.path)
			continue
		}
		// Written only when it changed, so a no-op run leaves every mtime alone
		// and nothing downstream rebuilds for nothing.
		if err := os.WriteFile(path, want, 0o644); err != nil {
			return err
		}
	}
	if len(stale) > 0 {
		return fmt.Errorf("out of date with proto/%s:\n  %s", strings.Join(schemas, ", proto/"), strings.Join(stale, "\n  "))
	}
	return nil
}

// The two schemas: the strings the API speaks, and the shapes those strings
// name.
const (
	vocabSchema   = "vocabulary.yaml"
	payloadSchema = "payloads.yaml"
)

var schemas = []string{vocabSchema, payloadSchema}

// load reads each schema into its OWN type before composing them, so that each
// file can only declare what it owns: read into one shared value, payloads.yaml
// could redefine the vocabularies and strict mode would have nothing to object
// to, because the field exists.
func load(root string) (source, error) {
	var vocab struct {
		Vocabularies []vocabulary `json:"vocabularies"`
		Sentinels    sentinels    `json:"sentinels"`
	}
	var shapes struct {
		Payloads []payload `json:"payloads"`
	}
	if err := readSchema(root, vocabSchema, &vocab); err != nil {
		return source{}, err
	}
	if err := readSchema(root, payloadSchema, &shapes); err != nil {
		return source{}, err
	}
	src := source{Vocabularies: vocab.Vocabularies, Sentinels: vocab.Sentinels, Payloads: shapes.Payloads}
	return src, validate(src)
}

// readSchema is strict, so a mistyped key fails here rather than becoming an
// artifact that silently omits something.
func readSchema(root, name string, into any) error {
	raw, err := os.ReadFile(filepath.Join(root, "proto", name))
	if err != nil {
		return err
	}
	if err := yaml.UnmarshalStrict(raw, into); err != nil {
		return fmt.Errorf("proto/%s: %w", name, err)
	}
	return nil
}

// validate rejects a source the renderers would otherwise turn into something
// that does not compile, or into a comment that quietly says nothing.
func validate(src source) error {
	// Every value and every payload key is documented. These are rendered into
	// the .proto, which is what a client in any language reads first, and an
	// undocumented key there is a field a client has to reverse-engineer.
	for _, v := range src.Vocabularies {
		for _, val := range v.Values {
			if strings.TrimSpace(val.Doc) == "" {
				return fmt.Errorf("%s.%s: every value needs a doc", v.Name, val.Value)
			}
		}
	}
	for _, p := range src.Payloads {
		for _, f := range p.Fields {
			if strings.TrimSpace(f.Doc) == "" {
				return fmt.Errorf("payload %s.%s: every field needs a doc", p.Name, f.Name)
			}
		}
	}
	for _, v := range src.Vocabularies {
		for _, val := range v.Values {
			if val.Payload != "" && src.findPayload(val.Payload) == nil {
				return fmt.Errorf("%s.%s: no %s in proto/payloads.yaml", v.Name, val.Value, val.Payload)
			}
			if val.Live && !v.GoTyped {
				return fmt.Errorf("%s.%s: live needs go_typed, which is what the predicate hangs off", v.Name, val.Value)
			}
		}
	}
	for _, s := range src.Sentinels.Values {
		if _, ok := src.Sentinels.HTTPStatus[s.Code]; !ok {
			return fmt.Errorf("%s: no http_status for code %q", s.Value, s.Code)
		}
		// connect's own parser, so a typo fails here rather than becoming a Go
		// identifier that does not exist.
		var code connect.Code
		if err := code.UnmarshalText([]byte(s.Code)); err != nil {
			return fmt.Errorf("%s: %w", s.Value, err)
		}
	}
	return validatePayloads(src)
}

// --- Go ----------------------------------------------------------------------

// banner marks a generated file with the schema it came from — "do not edit"
// is only actionable when it says where to edit instead.
func banner(schema string) string {
	return "Code generated by cmd/vocabgen from proto/" + schema + ". DO NOT EDIT."
}

func renderGoAPI(src source, _ []byte) ([]byte, error) {
	var b strings.Builder
	fmt.Fprintf(&b, "// %s\n\npackage api\n\nimport \"errors\"\n", banner(vocabSchema))

	for _, v := range src.Vocabularies {
		b.WriteString("\n")
		b.WriteString(comment("// ", v.Doc))
		if v.GoTyped {
			fmt.Fprintf(&b, "type %s string\n\n", v.Name)
		}
		b.WriteString("const (\n")
		for _, val := range v.Values {
			ident := v.GoPrefix + pascal(val.Value)
			b.WriteString(indent(comment("// ", src.valueDoc(ident+": ", val, goPayload))))
			if v.GoTyped {
				fmt.Fprintf(&b, "\t%s %s = %q\n", ident, v.Name, val.Value)
			} else {
				fmt.Fprintf(&b, "\t%s = %q\n", ident, val.Value)
			}
		}
		b.WriteString(")\n")

		// A switch rather than a map: it inlines, where a map index is a call and
		// a package-level map is writable by anything in the package.
		if live := liveValues(v); len(live) > 0 {
			fn := "live" + v.Name
			b.WriteString("\n")
			b.WriteString(comment("// ", fmt.Sprintf(
				"%s reports whether a %s is one of the live values. The exported %s.Live is what callers ask.",
				fn, v.Name, v.Name)))
			fmt.Fprintf(&b, "func %s(v %s) bool {\n\tswitch v {\n\tcase ", fn, v.Name)
			for i, val := range live {
				if i > 0 {
					b.WriteString(", ")
				}
				b.WriteString(v.GoPrefix + pascal(val.Value))
			}
			b.WriteString(":\n\t\treturn true\n\t}\n\treturn false\n}\n")
		}
	}

	b.WriteString("\n")
	b.WriteString(comment("// ", src.Sentinels.Doc))
	b.WriteString("var (\n")
	for _, s := range src.Sentinels.Values {
		b.WriteString(indent(comment("// ", s.Value+": "+s.Doc)))
		fmt.Fprintf(&b, "\t%s = errors.New(%q)\n", s.Value, s.Message)
	}
	b.WriteString(")\n")

	return format.Source([]byte(b.String()))
}

func renderGoWire(src source, _ []byte) ([]byte, error) {
	var b strings.Builder
	fmt.Fprintf(&b, "// %s\n\n", banner(vocabSchema))
	b.WriteString(`package wire

import (
	"connectrpc.com/connect"

	"github.com/pomerium/agentops/harness/api"
)

// published is the error set, in one table read from both directions: the name
// travels on the wire, the code is what a transport-only client sees.
//
// The name is what makes the mapping injective. Several sentinels share a code —
// a missing session and an unknown permission request are both not-found-ish,
// forbidden and quota-exceeded are both refusals — so a client branching on
// errors.Is needs the sentinel that was actually raised rather than the nearest
// code. A slice rather than two maps because the match order has to be
// deterministic: an error wrapping two sentinels must classify the same way
// every time.
var published = []struct {
	name string
	err  error
	code connect.Code
}{
`)
	for _, s := range src.Sentinels.Values {
		fmt.Fprintf(&b, "\t{%q, api.%s, connect.Code%s},\n", s.Value, s.Value, pascal(s.Code))
	}
	b.WriteString("}\n")
	return format.Source([]byte(b.String()))
}

func goPayload(name string) string { return payloadClause(name, goForm) }

// --- TypeScript --------------------------------------------------------------

func renderTypeScript(src source, _ []byte) ([]byte, error) {
	var b strings.Builder
	fmt.Fprintf(&b, "// %s\n", banner(vocabSchema))

	for _, v := range src.Vocabularies {
		b.WriteString("\n")
		b.WriteString(jsdoc(v.Doc))
		fmt.Fprintf(&b, "export const %s = {\n", v.Name)
		for _, val := range v.Values {
			b.WriteString(indent(jsdoc(sentence(src.valueDoc("", val, tsPayload)))))
			fmt.Fprintf(&b, "\t%s: %q,\n", pascal(val.Value), val.Value)
		}
		b.WriteString("} as const;\n")

		if live := liveValues(v); len(live) > 0 {
			b.WriteString("\n")
			b.WriteString(jsdoc(fmt.Sprintf(
				"The %s values marked live in the vocabulary. `isLive` is what callers ask.", v.Name)))
			fmt.Fprintf(&b, "export const live%ss: ReadonlySet<string> = new Set([\n", v.Name)
			for _, val := range live {
				fmt.Fprintf(&b, "\t%s.%s,\n", v.Name, pascal(val.Value))
			}
			b.WriteString("]);\n")
		}
	}

	b.WriteString("\n")
	b.WriteString(jsdoc(src.Sentinels.Doc))
	fmt.Fprintf(&b, "export const %s = {\n", src.Sentinels.Name)
	for _, s := range src.Sentinels.Values {
		b.WriteString(indent(jsdoc(sentence(s.Doc))))
		fmt.Fprintf(&b, "\t%s: %q,\n", src.Sentinels.key(s), s.Value)
	}
	b.WriteString("} as const;\n")

	// Two spaces per level, like the rest of the SDK; the builders above use tabs
	// so the nesting stays legible in this file.
	return []byte(strings.ReplaceAll(b.String(), "\t", "  ")), nil
}

func tsPayload(name string) string { return payloadClause(name, tsForm) }

// --- Python ------------------------------------------------------------------

func renderPython(src source, _ []byte) ([]byte, error) {
	var b strings.Builder
	fmt.Fprintf(&b, "\"\"\"%s\"\"\"\n\nfrom __future__ import annotations\n\nfrom typing import Final\n", banner(vocabSchema))

	for _, v := range src.Vocabularies {
		fmt.Fprintf(&b, "\n\nclass %s:\n", v.Name)
		b.WriteString(indent(pydoc(v.Doc)))
		b.WriteString("\n")
		for _, val := range v.Values {
			b.WriteString(indent(comment("#: ", sentence(src.valueDoc("", val, pyPayload)))))
			fmt.Fprintf(&b, "\t%s: Final = %q\n", strings.ToUpper(val.Value), val.Value)
		}

		if live := liveValues(v); len(live) > 0 {
			b.WriteString("\n\n")
			b.WriteString(comment("#: ", fmt.Sprintf(
				"The %s values marked live in the vocabulary. `is_live` is what callers ask.", v.Name)))
			fmt.Fprintf(&b, "LIVE_%sS: Final = frozenset(\n\t{\n", screaming(v.Name))
			for _, val := range live {
				fmt.Fprintf(&b, "\t\t%s.%s,\n", v.Name, strings.ToUpper(val.Value))
			}
			b.WriteString("\t}\n)\n")
		}
	}

	fmt.Fprintf(&b, "\n\nclass %s:\n", src.Sentinels.Name)
	b.WriteString(indent(pydoc(src.Sentinels.Doc)))
	b.WriteString("\n")
	for _, s := range src.Sentinels.Values {
		b.WriteString(indent(comment("#: ", sentence(s.Doc))))
		fmt.Fprintf(&b, "\t%s: Final = %q\n", screaming(src.Sentinels.key(s)), s.Value)
	}

	// The names as a set, for the one thing every Python consumer does with them:
	// decide whether a name that arrived is one it knows. Reflecting over the
	// class to rebuild this is a comprehension that was already written twice.
	b.WriteString("\n\n")
	b.WriteString(comment("#: ", "Every published name. An unrecognized one is tolerated rather than "+
		"trusted: the set is additive, and a client that raised on a new one would break on an upgrade."))
	fmt.Fprintf(&b, "%s_NAMES: Final = frozenset(\n\t{\n", screaming(src.Sentinels.Name))
	for _, s := range src.Sentinels.Values {
		fmt.Fprintf(&b, "\t\t%s.%s,\n", src.Sentinels.Name, screaming(src.Sentinels.key(s)))
	}
	b.WriteString("\t}\n)\n")

	return []byte(strings.ReplaceAll(b.String(), "\t", "    ")), nil
}

func pyPayload(name string) string { return payloadClause(name, pyForm) }

// --- docs --------------------------------------------------------------------

// Regions of docs/clients.md are rewritten in place, between HTML comments that
// render as nothing. Generating them rather than testing them is the choice that
// makes a stale document impossible instead of merely detectable: the fix for a
// failing check would be to copy the values across by hand, which is the habit
// this whole command exists to end.
const (
	beginMarker = "<!-- BEGIN GENERATED: %s (make generate) -->"
	endMarker   = "<!-- END GENERATED: %s -->"
)

func renderDocs(src source, current []byte) ([]byte, error) {
	doc := string(current)

	for _, v := range src.Vocabularies {
		if v.DocsRegion == "" {
			continue
		}
		var values []string
		for _, val := range v.Values {
			values = append(values, "`"+val.Value+"`")
		}
		var err error
		if doc, err = replaceRegion(doc, v.DocsRegion, wrapJoin(values, " · ", 76)); err != nil {
			return nil, err
		}
	}

	shapes, err := payloadTable(src)
	if err != nil {
		return nil, err
	}
	if doc, err = replaceRegion(doc, "payload-shapes", shapes); err != nil {
		return nil, err
	}

	var table strings.Builder
	table.WriteString("| sentinel | Connect code | HTTP (companion) | means |\n|---|---|---|---|\n")
	for _, s := range src.Sentinels.Values {
		fmt.Fprintf(&table, "| `%s` | `%s` | %d | %s |\n",
			s.Value, s.Code, src.Sentinels.HTTPStatus[s.Code], means(s))
	}
	if doc, err = replaceRegion(doc, "sentinels", table.String()); err != nil {
		return nil, err
	}
	return []byte(doc), nil
}

// means is the table's gloss: the explicit one where the table wants its own
// terser voice, and otherwise the first sentence of the doc every other language
// already shows.
func means(s sentinel) string {
	if s.Means != "" {
		return s.Means
	}
	first, _, _ := strings.Cut(s.Doc, ". ")
	return strings.TrimSuffix(strings.TrimSpace(first), ".")
}

func replaceRegion(doc, name, body string) (string, error) {
	begin := fmt.Sprintf(beginMarker, name)
	end := fmt.Sprintf(endMarker, name)
	i := strings.Index(doc, begin)
	j := strings.Index(doc, end)
	if i < 0 || j < i {
		return "", fmt.Errorf("no %q region", name)
	}
	return doc[:i+len(begin)] + "\n" + strings.TrimRight(body, "\n") + "\n" + doc[j:], nil
}

// --- names -------------------------------------------------------------------

func pascal(s string) string {
	var b strings.Builder
	for _, word := range strings.Split(s, "_") {
		if word == "" {
			continue
		}
		b.WriteString(strings.ToUpper(word[:1]) + word[1:])
	}
	return b.String()
}

// screaming breaks a PascalCase name at its capitals and shouts it: NotFound is
// NOT_FOUND.
func screaming(s string) string {
	var b strings.Builder
	for i, r := range s {
		if i > 0 && r >= 'A' && r <= 'Z' {
			b.WriteRune('_')
		}
		b.WriteRune(r)
	}
	return strings.ToUpper(b.String())
}

// key is the SDK identifier for a sentinel: the wire name without the prefix Go
// needs and a namespaced language does not.
func (s sentinels) key(v sentinel) string { return strings.TrimPrefix(v.Value, s.StripPrefix) }

func liveValues(v vocabulary) []value {
	var out []value
	for _, val := range v.Values {
		if val.Live {
			out = append(out, val)
		}
	}
	return out
}

// --- text --------------------------------------------------------------------

// valueDoc is one constant's comment: the prefix a language wants in front of it
// (Go names the identifier), the one sentence every language shows, and the
// payload clause in the form that language can follow.
func (src source) valueDoc(prefix string, val value, payload func(string) string) string {
	if val.Doc == "" {
		return ""
	}
	doc := prefix + strings.TrimSpace(val.Doc)
	if val.Payload != "" {
		doc += " " + payload(val.Payload)
	}
	return doc
}

// sentence capitalizes the first letter, for the languages whose comments read
// as sentences rather than as a continuation of the identifier.
func sentence(s string) string {
	if s == "" {
		return s
	}
	r := []rune(s)
	return strings.ToUpper(string(r[0])) + string(r[1:])
}

// comment renders text as a comment block with the given prefix, wrapped to fit
// 80 columns once indented, and preserving paragraph breaks. Empty text gets no
// comment rather than an empty one.
func comment(prefix, text string) string {
	if strings.TrimSpace(text) == "" {
		return ""
	}
	var b strings.Builder
	for _, line := range strings.Split(wrapParagraphs(text, 76-len(prefix)), "\n") {
		b.WriteString(strings.TrimRight(prefix+line, " ") + "\n")
	}
	return b.String()
}

// jsdoc renders a JSDoc block, on one line when it fits on one.
func jsdoc(text string) string {
	if strings.TrimSpace(text) == "" {
		return ""
	}
	// A {@link Foo} reference is one token: wrapped across a line break it stops
	// being a link and starts being two words of prose.
	const glue = "\x00"
	lines := strings.Split(wrapParagraphs(strings.ReplaceAll(text, "{@link ", "{@link"+glue), 72), "\n")
	var b strings.Builder
	if len(lines) == 1 {
		fmt.Fprintf(&b, "/** %s */\n", lines[0])
	} else {
		b.WriteString("/**\n")
		for _, line := range lines {
			b.WriteString(strings.TrimRight(" * "+line, " ") + "\n")
		}
		b.WriteString(" */\n")
	}
	return strings.ReplaceAll(b.String(), glue, " ")
}

// pydoc renders a docstring, keeping the source's paragraph breaks.
func pydoc(text string) string {
	lines := strings.Split(wrapParagraphs(text, 72), "\n")
	if len(lines) == 1 {
		return `"""` + lines[0] + "\"\"\"\n"
	}
	var b strings.Builder
	b.WriteString(`"""` + lines[0] + "\n")
	for _, line := range lines[1:] {
		b.WriteString(strings.TrimRight(line, " ") + "\n")
	}
	b.WriteString("\"\"\"\n")
	return b.String()
}

// wrapParagraphs wraps each blank-line-separated paragraph to width, leaving the
// blank lines between them.
func wrapParagraphs(text string, width int) string {
	var out []string
	for _, para := range strings.Split(strings.TrimSpace(text), "\n\n") {
		out = append(out, wrapJoin(strings.Fields(para), " ", width))
	}
	return strings.Join(out, "\n\n")
}

// wrapJoin joins words with sep, breaking the line before width is exceeded.
func wrapJoin(words []string, sep string, width int) string {
	var b strings.Builder
	line := 0
	for i, w := range words {
		switch {
		case i == 0:
		case line+len(sep)+len([]rune(w)) > width:
			b.WriteString(strings.TrimRight(sep, " ") + "\n")
			line = 0
		default:
			b.WriteString(sep)
			line += len(sep)
		}
		b.WriteString(w)
		line += len([]rune(w))
	}
	return b.String()
}

func indent(block string) string {
	if block == "" {
		return ""
	}
	var b strings.Builder
	for _, line := range strings.Split(strings.TrimRight(block, "\n"), "\n") {
		if line == "" {
			b.WriteString("\n")
			continue
		}
		b.WriteString("\t" + line + "\n")
	}
	return b.String()
}

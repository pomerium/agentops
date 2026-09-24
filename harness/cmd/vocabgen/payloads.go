// The event payload shapes, from proto/payloads.yaml.
//
// The vocabularies next door are strings a client compares against; these are
// the JSON bodies themselves, and the difference is what the Go side does with
// them. The strings are rendered as constants nothing parses; the shapes are
// rendered as the structs the harness MARSHALS — the producer's own types, so
// the wire and the SDKs cannot come apart without the source saying so.

package main

import (
	"fmt"
	"go/format"
	"regexp"
	"sort"
	"strings"
)

// payload is one event's JSON body.
type payload struct {
	Name   string  `json:"name"`
	Doc    string  `json:"doc"`
	Fields []field `json:"fields"`
}

// field is one key of one payload. Name is the key on the wire; every
// identifier is derived from it.
type field struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Optional bool   `json:"optional"`
	Doc      string `json:"doc"`
}

// names is what one declaration is called in each language it is rendered into.
// One row per type rather than one switch per language: a switch missing an arm
// renders as an empty type in a file nothing compiles until much later, where a
// row missing a column does not build.
type names struct{ Go, TS, Py, Docs string }

// scalars are the field types that name themselves, and what each language
// calls them. A timestamp crosses as the RFC 3339 string encoding/json writes;
// a duration crosses as the nanosecond count time.Duration marshals to.
var scalars = map[string]names{
	"string":    {"string", "string", "str", "string"},
	"bool":      {"bool", "boolean", "bool", "bool"},
	"int":       {"int64", "number", "int", "int"},
	"float":     {"float64", "number", "float", "float"},
	"duration":  {"time.Duration", "number", "int", "int (ns)"},
	"timestamp": {"time.Time", "string", "str", "timestamp"},
	"json":      {"json.RawMessage", "unknown", "Any", "json"},
}

// ftype is a field's type, resolved against the scalars, the vocabularies and
// the other payloads. Exactly one of the three is set.
type ftype struct {
	scalar   string
	vocab    vocabulary
	payload  string
	repeated bool
}

func (src source) resolve(f field) (ftype, error) {
	name := strings.TrimPrefix(f.Type, "[]")
	t := ftype{repeated: name != f.Type}
	switch _, isScalar := scalars[name]; {
	case isScalar:
		t.scalar = name
	case src.findPayload(name) != nil:
		t.payload = name
	default:
		v, ok := src.findVocabulary(name)
		if !ok {
			return t, fmt.Errorf("%s: unknown type %q", f.Name, f.Type)
		}
		t.vocab = v
	}
	return t, nil
}

// base is the type's name in each language before any repetition: a row of the
// table for a scalar, and the name itself for a vocabulary or another payload.
func (t ftype) base() names {
	switch {
	case t.payload != "":
		return names{t.payload, t.payload, t.payload, t.payload}
	case t.vocab.Name != "":
		// A vocabulary with a Go type of its own carries it there; everywhere
		// else it is the free string it is on the wire.
		n := names{"string", "string", "str", "string"}
		if t.vocab.GoTyped {
			n.Go = t.vocab.Name
		}
		return n
	}
	return scalars[t.scalar]
}

func (src source) findPayload(name string) *payload {
	for i, p := range src.Payloads {
		if p.Name == name {
			return &src.Payloads[i]
		}
	}
	return nil
}

func (src source) findVocabulary(name string) (vocabulary, bool) {
	for _, v := range src.Vocabularies {
		if v.Name == name {
			return v, true
		}
	}
	return vocabulary{}, false
}

// carrier is the vocabulary whose values name payloads, and the event of it
// that carries each one — read from the vocabularies rather than restated in
// payloads.yaml. validatePayloads has already refused a second carrier and a
// payload claimed twice, so this is a lookup and not a search.
func (src source) carrier() (vocabulary, map[string]value) {
	var carrier vocabulary
	out := map[string]value{}
	for _, v := range src.Vocabularies {
		for _, val := range v.Values {
			if val.Payload != "" {
				carrier, out[val.Payload] = v, val
			}
		}
	}
	return carrier, out
}

var snakeCase = regexp.MustCompile(`^[a-z][a-z0-9]*(_[a-z0-9]+)*$`)

// validatePayloads rejects a schema that would generate something that does not
// compile, or that would quietly change the wire.
func validatePayloads(src source) error {
	var carrier string
	claimed := map[string]string{}
	for _, v := range src.Vocabularies {
		for _, val := range v.Values {
			if val.Payload == "" {
				continue
			}
			// One vocabulary carries payloads (the event types) and one event
			// carries each shape. Both are what makes "the payload of this
			// event" a lookup — in the generated table, and in the documentation
			// that lists a shape under the event a reader has in hand.
			if carrier != "" && carrier != v.Name {
				return fmt.Errorf("%s and %s both name payloads; one vocabulary carries them", carrier, v.Name)
			}
			if first, ok := claimed[val.Payload]; ok {
				return fmt.Errorf("%s carries %s, which %s already carries", val.Value, val.Payload, first)
			}
			carrier, claimed[val.Payload] = v.Name, val.Value
		}
	}

	nested := map[string]bool{}
	for _, p := range src.Payloads {
		if err := checkRefs(src, p.Doc, p.Name); err != nil {
			return err
		}
		for _, f := range p.Fields {
			t, err := src.resolve(f)
			if err != nil {
				return fmt.Errorf("%s.%w", p.Name, err)
			}
			if err := checkRefs(src, f.Doc, p.Name+"."+f.Name); err != nil {
				return err
			}
			if !snakeCase.MatchString(f.Name) {
				return fmt.Errorf("%s.%s: keys are snake_case, and the key is the contract", p.Name, f.Name)
			}
			// The unit lives in the key because JSON has no duration type, and
			// the two have to agree or the published convention is a lie.
			if (t.scalar == "duration") != strings.HasSuffix(f.Name, "_ns") {
				return fmt.Errorf("%s.%s: a duration's key ends in _ns and an _ns key is a duration", p.Name, f.Name)
			}
			// omitempty is what optional renders to, and encoding/json ignores
			// it on a struct: a timestamp marked optional would still cross as
			// the zero time, and three languages would call it absent.
			if f.Optional && t.scalar == "timestamp" && !t.repeated {
				return fmt.Errorf("%s.%s: a timestamp is always written, so it cannot be optional", p.Name, f.Name)
			}
			if t.payload != "" {
				nested[t.payload] = true
			}
		}
	}
	for _, p := range src.Payloads {
		if _, ok := claimed[p.Name]; !ok && !nested[p.Name] {
			return fmt.Errorf("%s: no event carries it and nothing nests it", p.Name)
		}
	}
	return nil
}

// checkRefs rejects a [Name] that names nothing. It renders as a link in three
// languages, and a typo would render as a link to nowhere in all three.
func checkRefs(src source, doc, where string) error {
	for _, m := range crossRef.FindAllStringSubmatch(doc, -1) {
		if _, ok := src.findVocabulary(m[1]); ok {
			continue
		}
		if src.findPayload(m[1]) == nil {
			return fmt.Errorf("%s: [%s] names no vocabulary and no payload", where, m[1])
		}
	}
	return nil
}

// --- identifiers ---------------------------------------------------------------

// initialisms are the words Go shouts. The list is short on purpose: it holds
// what the keys actually contain, so that every Go identifier is DERIVED from
// its key and no payload needs an override to keep the name it published.
var initialisms = map[string]string{"id": "ID", "url": "URL", "usd": "USD"}

// goFieldName is the exported Go name for a key. These names are public API —
// slackbot and companion read them — so the derivation is part of the contract
// too, not a formatting preference.
func goFieldName(f field, t ftype) string {
	words := strings.Split(f.Name, "_")
	// The _ns suffix says the unit JSON cannot. time.Duration says it in the
	// type, so carrying it into the Go name would say it twice.
	if t.scalar == "duration" && words[len(words)-1] == "ns" {
		words = words[:len(words)-1]
	}
	var b strings.Builder
	for _, w := range words {
		if up, ok := initialisms[w]; ok {
			b.WriteString(up)
			continue
		}
		b.WriteString(pascal(w))
	}
	return b.String()
}

// crossRef is a [Name] in a doc: a reference to a vocabulary or a payload,
// written once and linked in each language's own syntax.
var crossRef = regexp.MustCompile(`\[(\w+)\]`)

// The form each language links a name in. One set for both the cross-references
// in a doc and the "Payload:" clause the event constants carry, because they are
// the same act.
const (
	goForm = "$1"
	tsForm = "{@link $1}"
	pyForm = "``$1``"
)

// ref renders a doc's cross-references with the given form. Every language links
// differently and none of them can read another's syntax.
func ref(text, form string) string { return crossRef.ReplaceAllString(text, form) }

// link is one name in that form, for a reference the source did not write in
// brackets because there was no prose to put it in.
func link(name, form string) string { return ref("["+name+"]", form) }

// payloadClause is what an event type's doc says about the payload it carries.
func payloadClause(name, form string) string { return "Payload: " + link(name, form) + "." }

// --- Go ----------------------------------------------------------------------

func goType(t ftype) string {
	if t.repeated {
		return "[]" + t.base().Go
	}
	return t.base().Go
}

func renderGoPayloads(src source, _ []byte) ([]byte, error) {
	var b strings.Builder
	fmt.Fprintf(&b, "// %s\n\n", banner(payloadSchema))
	b.WriteString(comment("// ", `The payload types, which the harness marshals with encoding/json — so these
declarations ARE the wire format, and what a client of any language decodes is
what this file says. Keys are snake_case; a duration is a numeric count of
nanoseconds, named with an _ns suffix.`))
	b.WriteString("\npackage api\n")

	if imports := goImports(src); len(imports) > 0 {
		b.WriteString("\nimport (\n")
		for _, imp := range imports {
			fmt.Fprintf(&b, "\t%q\n", imp)
		}
		b.WriteString(")\n")
	}

	for _, p := range src.Payloads {
		b.WriteString("\n")
		b.WriteString(comment("// ", ref(p.Doc, goForm)))
		if len(p.Fields) == 0 {
			fmt.Fprintf(&b, "type %s struct{}\n", p.Name)
			continue
		}
		fmt.Fprintf(&b, "type %s struct {\n", p.Name)
		for _, f := range p.Fields {
			t, err := src.resolve(f)
			if err != nil {
				return nil, err
			}
			name := goFieldName(f, t)
			if f.Doc != "" {
				b.WriteString(indent(comment("// ", ref(name+": "+f.Doc, goForm))))
			}
			tag := f.Name
			if f.Optional {
				tag += ",omitempty"
			}
			fmt.Fprintf(&b, "\t%s %s `json:%q`\n", name, goType(t), tag)
		}
		b.WriteString("}\n")
	}

	// One zero value of every payload, so that a test can walk them all. It is
	// generated rather than written next to the test because a list kept by hand
	// is a list a new payload is missing from, which is the whole complaint this
	// command answers.
	carrier, _ := src.carrier()
	name := "payloadBy" + carrier.Name
	b.WriteString("\n")
	b.WriteString(comment("// ", fmt.Sprintf(
		"%s is a zero value of every payload the log carries, by the %s that carries it. Nothing in the API reads it — the wire-conventions test walks it, so a payload added to the schema cannot slip past unexercised.",
		name, carrier.Name)))
	fmt.Fprintf(&b, "var %s = map[%s]any{\n", name, carrier.Name)
	for _, val := range carrier.Values {
		if val.Payload != "" {
			fmt.Fprintf(&b, "\t%s: %s{},\n", carrier.GoPrefix+pascal(val.Value), val.Payload)
		}
	}
	b.WriteString("}\n")

	return format.Source([]byte(b.String()))
}

// goImports is what the rendered types need, which depends on which types the
// schema actually uses: an import for a kind nothing declares does not compile.
func goImports(src source) []string {
	var needJSON, needTime bool
	for _, p := range src.Payloads {
		for _, f := range p.Fields {
			t, err := src.resolve(f)
			if err != nil {
				continue // reported by validate, which runs before any renderer
			}
			needJSON = needJSON || t.scalar == "json"
			needTime = needTime || t.scalar == "timestamp" || t.scalar == "duration"
		}
	}
	var out []string
	if needJSON {
		out = append(out, "encoding/json")
	}
	if needTime {
		out = append(out, "time")
	}
	return out
}

// --- TypeScript --------------------------------------------------------------

func tsType(t ftype) string {
	if t.repeated {
		return t.base().TS + "[]"
	}
	return t.base().TS
}

func renderTSPayloads(src source, _ []byte) ([]byte, error) {
	var b strings.Builder
	fmt.Fprintf(&b, "// %s\n\n", banner(payloadSchema))
	b.WriteString(jsdoc(`The event payload shapes, as they arrive: keys snake_case, exactly as the
log holds them. They are not the proto3-mapped envelope around them.

Duration fields are NANOSECOND integers, named with a _ns suffix — that is
the wire's unit, not milliseconds.`))

	for _, p := range src.Payloads {
		b.WriteString("\n")
		b.WriteString(jsdoc(ref(p.Doc, tsForm)))
		if len(p.Fields) == 0 {
			// An interface with no members would accept any object at all.
			fmt.Fprintf(&b, "export type %s = Record<string, never>;\n", p.Name)
			continue
		}
		fmt.Fprintf(&b, "export interface %s {\n", p.Name)
		for _, f := range p.Fields {
			t, err := src.resolve(f)
			if err != nil {
				return nil, err
			}
			if f.Doc != "" {
				b.WriteString(indent(jsdoc(sentence(ref(f.Doc, tsForm)))))
			}
			opt := ""
			if f.Optional {
				opt = "?"
			}
			fmt.Fprintf(&b, "\t%s%s: %s;\n", f.Name, opt, tsType(t))
		}
		b.WriteString("}\n")
	}

	return []byte(strings.ReplaceAll(b.String(), "\t", "  ")), nil
}

// --- Python ------------------------------------------------------------------

func pyType(t ftype) string {
	if t.repeated {
		return "list[" + t.base().Py + "]"
	}
	return t.base().Py
}

func renderPyPayloads(src source, _ []byte) ([]byte, error) {
	var b strings.Builder
	b.WriteString(pydoc(banner(payloadSchema) + "\n\n" +
		"The event payload shapes, as TypedDicts.\n\n"+
		"A TypedDict describes the dict the payload already is — it is a reading of "+
		"it, not a wrapper around it, so ``payload_of(event, AgentMessage)`` costs "+
		"nothing and an unknown key still survives to whoever wants it.\n\n"+
		"Keys are snake_case, exactly as the log holds them. Duration fields are "+
		"NANOSECOND integers, named with a _ns suffix. A key that can be absent is "+
		"declared apart from the ones that cannot, because reading a missing key is "+
		"a KeyError and the type is what warns you first."))
	b.WriteString("\nfrom __future__ import annotations\n\nfrom typing import Any, TypedDict\n")

	exported := make([]string, 0, len(src.Payloads))
	for _, p := range src.Payloads {
		exported = append(exported, p.Name)
	}
	sort.Strings(exported)
	b.WriteString("\n__all__ = [\n")
	for _, name := range exported {
		fmt.Fprintf(&b, "\t%q,\n", name)
	}
	b.WriteString("]\n")

	for _, p := range src.Payloads {
		var required, optional []field
		for _, f := range p.Fields {
			if f.Optional {
				optional = append(optional, f)
			} else {
				required = append(required, f)
			}
		}

		// The public class always carries the doc, and carries the optional keys
		// when there are any. Splitting them off a private base is how a
		// TypedDict says NotRequired on the Python this SDK supports, which is
		// 3.10 and up.
		base, own := "TypedDict", p.Fields
		if len(required) > 0 && len(optional) > 0 {
			base, own = "_"+p.Name+"Required", optional
			fmt.Fprintf(&b, "\n\nclass %s(TypedDict):\n", base)
			b.WriteString(indent(pydoc(fmt.Sprintf("The keys of %s that are always present.", p.Name))))
			b.WriteString("\n")
			if err := src.writePyFields(&b, required); err != nil {
				return nil, err
			}
		}

		total := ""
		if len(optional) > 0 {
			total = ", total=False"
		}
		fmt.Fprintf(&b, "\n\nclass %s(%s%s):\n", p.Name, base, total)
		b.WriteString(indent(pydoc(ref(p.Doc, pyForm))))
		if len(own) == 0 {
			continue
		}
		b.WriteString("\n")
		if err := src.writePyFields(&b, own); err != nil {
			return nil, err
		}
	}

	return []byte(strings.ReplaceAll(b.String(), "\t", "    ")), nil
}

func (src source) writePyFields(b *strings.Builder, fields []field) error {
	for _, f := range fields {
		t, err := src.resolve(f)
		if err != nil {
			return err
		}
		if f.Doc != "" {
			b.WriteString(indent(comment("#: ", sentence(ref(f.Doc, pyForm)))))
		}
		fmt.Fprintf(b, "\t%s: %s\n", f.Name, pyType(t))
	}
	return nil
}

// --- docs --------------------------------------------------------------------

func docsType(t ftype) string {
	if t.repeated {
		return t.base().Docs + "[]"
	}
	return t.base().Docs
}

// payloadTable is docs/clients.md's answer to "what is in the payload", by the
// event a reader has in hand. The nested shapes follow the events that reach
// them, named rather than inlined, because that is how the fields are named in
// every language too.
func payloadTable(src source) (string, error) {
	_, carried := src.carrier()
	var events, nested []payload
	for _, p := range src.Payloads {
		if _, ok := carried[p.Name]; ok {
			events = append(events, p)
		} else {
			nested = append(nested, p)
		}
	}

	var b strings.Builder
	b.WriteString("| event or shape | payload keys (`?` = omitted when empty) |\n|---|---|\n")
	for _, p := range append(events, nested...) {
		var cells []string
		for _, f := range p.Fields {
			t, err := src.resolve(f)
			if err != nil {
				return "", err
			}
			opt := ""
			if f.Optional {
				opt = "?"
			}
			cells = append(cells, fmt.Sprintf("`%s%s` %s", f.Name, opt, docsType(t)))
		}
		if len(cells) == 0 {
			cells = []string{"— empty"}
		}
		label := "`" + p.Name + "`"
		if val, ok := carried[p.Name]; ok {
			label = "`" + val.Value + "`"
		}
		fmt.Fprintf(&b, "| %s | %s |\n", label, strings.Join(cells, " · "))
	}
	return b.String(), nil
}

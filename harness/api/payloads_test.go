package api

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

// The payloads are generated from proto/payloads.yaml, so no test has to notice
// them drifting from the SDKs: they cannot. What a test does have to notice is
// the GENERATOR drifting from the conventions they are published under —
// snake_case keys, nanosecond durations, numbers that stay numbers. Those are
// promises to clients in three other languages, and the day someone swaps
// encoding/json for a marshaler with its own opinions (protojson quotes every
// int64 and camelCases every name) nothing else here would say a word.
//
// It walks the generated table rather than a list of its own, so a payload added
// to the schema is exercised without anybody remembering to add it. Stdlib only,
// like the rest of the package.

func TestPayloadWireConventions(t *testing.T) {
	for event, p := range payloadByEventType {
		t.Run(string(event), func(t *testing.T) {
			typ := reflect.TypeOf(p)
			raw, err := json.Marshal(populate(typ).Interface())
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			checkObject(t, typ, raw)

			// Byte-identical replay is the log's promise, and a payload that does
			// not survive a decode/encode round trip cannot keep it.
			v := reflect.New(typ)
			if err := json.Unmarshal(raw, v.Interface()); err != nil {
				t.Fatalf("unmarshal %s: %v", raw, err)
			}
			again, err := json.Marshal(v.Elem().Interface())
			if err != nil {
				t.Fatalf("re-marshal: %v", err)
			}
			if string(again) != string(raw) {
				t.Errorf("round trip changed the bytes:\n  before %s\n  after  %s", raw, again)
			}
		})
	}
}

// checkObject holds one marshaled payload to the published conventions, and
// recurses into the shapes nested in it.
func checkObject(t *testing.T, typ reflect.Type, raw []byte) {
	t.Helper()
	var keys map[string]json.RawMessage
	if err := json.Unmarshal(raw, &keys); err != nil {
		t.Fatalf("%s: %v", typ.Name(), err)
	}

	for i := range typ.NumField() {
		f := typ.Field(i)
		key, _, _ := strings.Cut(f.Tag.Get("json"), ",")
		if key == "" {
			t.Fatalf("%s.%s has no json key", typ.Name(), f.Name)
		}
		// The key's own spelling is the generator's business (it refuses a key
		// that is not snake_case, and a duration whose key does not end in _ns).
		// What is checked here is that the MARSHALER wrote that key and not one
		// of its own: protojson would have written waitedNs and failed right
		// here.
		body, ok := keys[key]
		if !ok {
			t.Fatalf("%s.%s: %q is set and still absent from the JSON", typ.Name(), f.Name, key)
		}

		switch {
		case f.Type == reflect.TypeOf(time.Duration(0)):
			// The wire's unit is nanoseconds, because JSON has no duration and a
			// client reading it as milliseconds is off by a million.
			var ns int64
			if err := json.Unmarshal(body, &ns); err != nil {
				t.Errorf("%s.%s: a duration is a number, not %s", typ.Name(), f.Name, body)
			}
		case f.Type.Kind() == reflect.Int64, f.Type.Kind() == reflect.Float64:
			if strings.Contains(string(body), `"`) {
				t.Errorf("%s.%s: a number crosses as a number, not %s", typ.Name(), f.Name, body)
			}
		case nests(f.Type):
			checkObject(t, f.Type, body)
		case f.Type.Kind() == reflect.Slice && nests(f.Type.Elem()):
			var elems []json.RawMessage
			if err := json.Unmarshal(body, &elems); err != nil {
				t.Fatalf("%s.%s: %v", typ.Name(), f.Name, err)
			}
			for _, elem := range elems {
				checkObject(t, f.Type.Elem(), elem)
			}
		}
	}
}

// TestPayloadDurationsAreNanoseconds is the same promise as one legible line of
// JSON, because the conventions above are checked by machinery and read by
// nobody. Ninety seconds is ninety billion, unquoted.
func TestPayloadDurationsAreNanoseconds(t *testing.T) {
	raw, err := json.Marshal(LaunchStalled{Waited: 90 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"waited_ns":90000000000}`; string(raw) != want {
		t.Errorf("got %s, want %s", raw, want)
	}
}

// nests reports whether a type is another payload — a shape whose own keys are
// on the wire, rather than a time.Time that marshals to one string of its own.
func nests(t reflect.Type) bool {
	return t.Kind() == reflect.Struct && t != reflect.TypeOf(time.Time{})
}

// populate fills every field with a non-zero value, so that omitempty cannot
// hide a key from the checks above.
func populate(typ reflect.Type) reflect.Value {
	v := reflect.New(typ).Elem()
	for i := range typ.NumField() {
		f := v.Field(i)
		switch {
		case f.Type() == reflect.TypeOf(json.RawMessage(nil)):
			f.SetBytes([]byte(`{"tool":"input"}`))
		case f.Type() == reflect.TypeOf(time.Time{}):
			f.Set(reflect.ValueOf(time.Now().UTC().Truncate(time.Second)))
		case f.Kind() == reflect.String:
			f.SetString("x")
		case f.Kind() == reflect.Bool:
			f.SetBool(true)
		case f.Kind() == reflect.Int64:
			f.SetInt(7)
		case f.Kind() == reflect.Float64:
			f.SetFloat(0.25)
		case f.Kind() == reflect.Slice:
			f.Set(reflect.Append(f, populate(f.Type().Elem())))
		case f.Kind() == reflect.Struct:
			f.Set(populate(f.Type()))
		}
	}
	return v
}

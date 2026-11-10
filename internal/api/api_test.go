package api

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestValidateKey(t *testing.T) {
	tests := []struct {
		name string
		key  string
		ok   bool
	}{
		{"empty", "", false},
		{"simple", "order-123", true},
		{"max length", strings.Repeat("a", maxKeyLen), true},
		{"too long", strings.Repeat("a", maxKeyLen+1), false},
		{"space", "a b", false},
		{"control char", "a\x00b", false},
		{"non-ascii", "ключ", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateKey(tt.key); (err == nil) != tt.ok {
				t.Fatalf("validateKey(%q) err=%v, want ok=%v", tt.key, err, tt.ok)
			}
		})
	}
}

func TestCanonicalObject(t *testing.T) {
	a, err := canonicalObject(json.RawMessage(`{"b":1, "a":{"y":2,"x":1}}`))
	if err != nil {
		t.Fatal(err)
	}
	b, err := canonicalObject(json.RawMessage(`{"a":{"x":1,"y":2},"b":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if string(a) != string(b) {
		t.Fatalf("key order changed the canonical form: %s vs %s", a, b)
	}
	// Large integers must survive (UseNumber), or two different payloads
	// could hash equal after float rounding.
	c, _ := canonicalObject(json.RawMessage(`{"n":9007199254740993}`))
	if !strings.Contains(string(c), "9007199254740993") {
		t.Fatalf("precision lost: %s", c)
	}
	for _, bad := range []string{`[]`, `"x"`, `null`, `1`, `{`} {
		if _, err := canonicalObject(json.RawMessage(bad)); err == nil {
			t.Errorf("payload %s accepted", bad)
		}
	}
	if e, _ := canonicalObject(nil); string(e) != `{}` {
		t.Errorf("empty payload => %s", e)
	}
}

func TestCursorRoundTrip(t *testing.T) {
	for _, seq := range []int64{1, 42, 1 << 62} {
		got, err := decodeCursor(encodeCursor(seq))
		if err != nil || got != seq {
			t.Fatalf("seq %d => %d, %v", seq, got, err)
		}
	}
	for _, bad := range []string{"", "!!", "djI6MQ", "djE6LTE", "djE6YWJj"} { // v2:1, v1:-1, v1:abc
		if _, err := decodeCursor(bad); err == nil {
			t.Errorf("cursor %q accepted", bad)
		}
	}
}

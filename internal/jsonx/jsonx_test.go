package jsonx

import (
	"strings"
	"testing"
)

func TestDecodeStrict(t *testing.T) {
	type obj struct {
		A int `json:"a"`
	}
	cases := []struct {
		name    string
		in      string
		wantErr string
	}{
		{"plain", `{"a":1}`, ""},
		{"json whitespace after", "{\"a\":1} \t\r\n", ""},
		{"unknown field", `{"a":1,"b":2}`, "unknown field"},
		{"second value", `{"a":1}{"a":2}`, "trailing data"},
		{"garbage after", `{"a":1}x`, "trailing data"},
		{"non-json space after", "{\"a\":1} ", "trailing data"},
		{"empty", ``, "EOF"},
		{"truncated", `{"a":`, "EOF"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var v obj
			err := DecodeStrict([]byte(tc.in), &v)
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("DecodeStrict(%q) = %v, want nil", tc.in, err)
			case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
				t.Fatalf("DecodeStrict(%q) = %v, want an error containing %q", tc.in, err, tc.wantErr)
			}
		})
	}
}

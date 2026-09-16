package tournament

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// corpus is the canonical seed corpus: one command per op with sorted,
// non-nil lists, so Decode(Encode(x)) == x holds exactly.
func corpus() []Command {
	rules := Rules{
		EntryFee: 500, RakeBps: 1000, PrizeBps: []uint32{5000, 3000, 2000},
		MinEntrants: 3, MaxEntrants: 100, MaxScore: 100000, MinAge: 18,
		TieBreak: EarliestSubmission, Exclusions: Exclusions{Version: 7, Jurisdictions: []string{"XX", "YY"}},
	}
	return []Command{
		{Key: "k-create", ReceivedAt: 1700000000000, Op: CreateTournament{ID: "t-2026-09-16-001", Seed: 1234567890123, Rules: rules}},
		{Key: "k-join", ReceivedAt: 2, Op: Join{Tournament: "t1", Player: Player{ID: "p-17", Jurisdiction: "TR", Age: 31}}},
		{Key: "k-score", ReceivedAt: 3, Op: SubmitScore{Tournament: "t1", Player: "p-17", Score: 8120, DealSeed: 1234567890123, InputDigest: Digest{0xab, 0xcd}}},
		{Key: "k-close", ReceivedAt: 4, Op: Close{Tournament: "t1"}},
		{Key: "k-settle", ReceivedAt: 5, Op: Settle{Tournament: "t1", Exclusions: Exclusions{Version: 8, Jurisdictions: []string{"XX", "YY", "ZZ"}}}},
		{Key: "k-settle-empty", ReceivedAt: 6, Op: Settle{Tournament: "t1", Exclusions: Exclusions{Version: 9, Jurisdictions: []string{}}}},
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	for _, c := range corpus() {
		enc, err := Encode(c)
		if err != nil {
			t.Fatalf("Encode(%s): %v", OpName(c.Op), err)
		}
		dec, err := Decode(enc)
		if err != nil {
			t.Fatalf("Decode(%s): %v", enc, err)
		}
		if !reflect.DeepEqual(dec, c) {
			t.Errorf("round trip changed the command:\n got %+v\nwant %+v", dec, c)
		}
		enc2, _ := Encode(dec)
		if !bytes.Equal(enc, enc2) {
			t.Errorf("Encode is not stable: %s vs %s", enc, enc2)
		}
		var w map[string]any
		if err := json.Unmarshal(enc, &w); err != nil {
			t.Fatal(err)
		}
		if w["op"] != OpName(c.Op) || w["key"] != string(c.Key) {
			t.Errorf("wire form %s lacks op or key", enc)
		}
	}
}

func TestEncodeIsCanonical(t *testing.T) {
	a := Command{Key: "k", Op: Settle{Tournament: "t", Exclusions: Exclusions{Version: 1, Jurisdictions: []string{"ZZ", "AA", "ZZ", "MM"}}}}
	b := Command{Key: "k", Op: Settle{Tournament: "t", Exclusions: Exclusions{Version: 1, Jurisdictions: []string{"MM", "AA", "ZZ"}}}}
	ea, _ := Encode(a)
	eb, _ := Encode(b)
	if !bytes.Equal(ea, eb) {
		t.Errorf("two orderings of the same list encode differently:\n%s\n%s", ea, eb)
	}
	dec, err := Decode(ea)
	if err != nil {
		t.Fatal(err)
	}
	if got := dec.Op.(Settle).Exclusions.Jurisdictions; !reflect.DeepEqual(got, []string{"AA", "MM", "ZZ"}) {
		t.Errorf("decoded list = %v", got)
	}
	c := Command{Key: "k", Op: CreateTournament{ID: "t", Rules: Rules{Exclusions: Exclusions{Jurisdictions: []string{"B", "A", "A"}}}}}
	ec, _ := Encode(c)
	if !strings.Contains(string(ec), `"jurisdictions":["A","B"]`) {
		t.Errorf("create exclusions not canonical: %s", ec)
	}
	if orig := c.Op.(CreateTournament).Rules.Exclusions.Jurisdictions; !reflect.DeepEqual(orig, []string{"B", "A", "A"}) {
		t.Error("Encode modified the caller's command")
	}
	n := Command{Key: "k", Op: Settle{Tournament: "t"}}
	en, _ := Encode(n)
	if !strings.Contains(string(en), `"jurisdictions":[]`) {
		t.Errorf("nil list should encode as []: %s", en)
	}
	if _, err := Encode(Command{Key: "k"}); err == nil {
		t.Error("Encode accepted a nil op")
	}
}

func TestDecodeRejects(t *testing.T) {
	valid, _ := Encode(corpus()[1])
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"empty", ``, "EOF"},
		{"not json", `{`, "unexpected"},
		{"unknown envelope field", `{"key":"k","received_at":0,"op":"close","body":{"tournament":"t"},"extra":1}`, "unknown field"},
		{"unknown body field", `{"key":"k","received_at":0,"op":"close","body":{"tournament":"t","x":1}}`, "unknown field"},
		{"unknown op", `{"key":"k","received_at":0,"op":"refund","body":{}}`, "unknown op"},
		{"missing body", `{"key":"k","received_at":0,"op":"close"}`, "missing body"},
		{"empty key", `{"key":"","received_at":0,"op":"close","body":{"tournament":"t"}}`, "key is empty"},
		{"key with space", `{"key":"a b","received_at":0,"op":"close","body":{"tournament":"t"}}`, "printable"},
		{"long key", `{"key":"` + strings.Repeat("k", 129) + `","received_at":0,"op":"close","body":{"tournament":"t"}}`, "limit"},
		{"trailing data", string(valid) + ` {}`, "trailing"},
		{"body wrong type", `{"key":"k","received_at":0,"op":"close","body":[1]}`, "cannot unmarshal"},
		{"bad digest length", `{"key":"k","received_at":0,"op":"submit_score","body":{"tournament":"t","player":"p","score":1,"deal_seed":1,"input_digest":"abcd"}}`, "64 hex"},
		{"bad digest chars", `{"key":"k","received_at":0,"op":"submit_score","body":{"tournament":"t","player":"p","score":1,"deal_seed":1,"input_digest":"` + strings.Repeat("zz", 32) + `"}}`, "digest"},
		{"received_at as string", `{"key":"k","received_at":"now","op":"close","body":{"tournament":"t"}}`, "cannot unmarshal"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Decode([]byte(tc.in))
			if err == nil {
				t.Fatalf("Decode accepted %q", tc.in)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}

func TestDecodeOp(t *testing.T) {
	op, err := DecodeOp("join", []byte(`{"tournament":"t","player":{"id":"p","jurisdiction":"TR","age":20}}`))
	if err != nil {
		t.Fatal(err)
	}
	if j, ok := op.(Join); !ok || j.Player.ID != "p" {
		t.Errorf("DecodeOp = %#v", op)
	}
	if _, err := DecodeOp("join", []byte(`{"tournament":"t","bogus":1}`)); err == nil {
		t.Error("DecodeOp accepted an unknown field")
	}
	if _, err := DecodeOp("nope", []byte(`{}`)); err == nil {
		t.Error("DecodeOp accepted an unknown op")
	}
}

func TestFingerprint(t *testing.T) {
	base := corpus()[0]
	same := base
	same.Key = "other-key"
	same.ReceivedAt = 999
	op := same.Op.(CreateTournament)
	op.Seed = 0
	op.Rules.Exclusions.Jurisdictions = []string{"YY", "XX"}
	same.Op = op
	if Fingerprint(base) != Fingerprint(same) {
		t.Error("key, receipt time, seed and list order must not affect the fingerprint")
	}
	diff := base
	op = diff.Op.(CreateTournament)
	op.Rules.EntryFee++
	diff.Op = op
	if Fingerprint(base) == Fingerprint(diff) {
		t.Error("a payload change did not change the fingerprint")
	}
	j1 := Command{Key: "k", Op: Join{Tournament: "t", Player: Player{ID: "p", Jurisdiction: "TR", Age: 30}}}
	j2 := Command{Key: "k", Op: Join{Tournament: "t", Player: Player{ID: "p", Jurisdiction: "TR", Age: 31}}}
	if Fingerprint(j1) == Fingerprint(j2) {
		t.Error("age change did not change the fingerprint")
	}
	c1 := Command{Key: "k", Op: Close{Tournament: "t"}}
	s1 := Command{Key: "k", Op: Settle{Tournament: "t"}}
	if Fingerprint(c1) == Fingerprint(s1) {
		t.Error("different ops share a fingerprint")
	}
	if Fingerprint(Command{}) == Fingerprint(c1) {
		t.Error("a nil op fingerprints like a close")
	}
}

func TestDigestJSON(t *testing.T) {
	d := Digest{0x01, 0xff}
	enc, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	want := `"01ff` + strings.Repeat("00", 30) + `"`
	if string(enc) != want {
		t.Errorf("Marshal = %s, want %s", enc, want)
	}
	var back Digest
	if err := json.Unmarshal(enc, &back); err != nil || back != d {
		t.Errorf("Unmarshal = %v, %v", back, err)
	}
	if d.String() != "01ff"+strings.Repeat("00", 30) {
		t.Errorf("String = %s", d.String())
	}
}

func TestStatusJSON(t *testing.T) {
	for s, name := range statusNames {
		enc, err := json.Marshal(Status(s))
		if err != nil || string(enc) != `"`+name+`"` {
			t.Errorf("Marshal(%d) = %s, %v", s, enc, err)
		}
		var back Status
		if err := json.Unmarshal(enc, &back); err != nil || back != Status(s) {
			t.Errorf("Unmarshal(%s) = %v, %v", enc, back, err)
		}
	}
	if _, err := json.Marshal(Status(9)); err == nil {
		t.Error("Marshal accepted an unknown status")
	}
	var s Status
	if err := json.Unmarshal([]byte(`"gone"`), &s); err == nil {
		t.Error("Unmarshal accepted an unknown status")
	}
	if Status(9).String() != "status(9)" || Settled.String() != "settled" {
		t.Error("String rendering")
	}
}

func TestTournamentOfAndOpName(t *testing.T) {
	for _, c := range corpus() {
		if TournamentOf(c.Op) == "" {
			t.Errorf("TournamentOf(%s) is empty", OpName(c.Op))
		}
	}
	if OpName(nil) != "" || TournamentOf(nil) != "" {
		t.Error("nil op should have no name and no tournament")
	}
}

// FuzzDecode: Decode never panics, and whatever it accepts re-encodes to
// bytes that decode to the same command.
func FuzzDecode(f *testing.F) {
	for _, c := range append(corpus(), playCorpus()...) {
		enc, _ := Encode(c)
		f.Add(enc)
	}
	f.Add([]byte(`{"key":"k","received_at":0,"op":"close","body":{"tournament":"t"}}`))
	f.Add([]byte(`{`))
	f.Add([]byte(``))
	f.Fuzz(func(t *testing.T, b []byte) {
		c, err := Decode(b)
		if err != nil {
			return
		}
		enc, err := Encode(c)
		if err != nil {
			t.Fatalf("Encode of a decoded command failed: %v", err)
		}
		again, err := Decode(enc)
		if err != nil {
			t.Fatalf("Decode(Encode(x)) failed: %v", err)
		}
		enc2, _ := Encode(again)
		if !bytes.Equal(enc, enc2) {
			t.Fatalf("canonical form not stable:\n%s\n%s", enc, enc2)
		}
		if Fingerprint(c) != Fingerprint(again) {
			t.Fatal("fingerprint changed across a round trip")
		}
	})
}

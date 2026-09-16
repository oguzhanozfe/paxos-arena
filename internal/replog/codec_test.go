package replog

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
)

func sampleEnvelopes() []Envelope {
	b := paxos.Ballot{Round: 12, Node: 3}
	return []Envelope{
		{From: 1, To: 2, Msg: Prepare{Ballot: b, FromSlot: 7}},
		{From: 2, To: 1, Msg: Promise{Ballot: b, Accepted: []paxos.PValue{
			{Ballot: paxos.Ballot{Round: 11, Node: 1}, Slot: 7, Value: paxos.Value("seven")},
			{Ballot: paxos.Ballot{Round: 11, Node: 1}, Slot: 9, Value: paxos.Value{}},
		}, CommitIndex: 6}},
		{From: 2, To: 1, Msg: Promise{Ballot: b}},
		{From: 3, To: 1, Msg: Accept{Ballot: b, Slot: 8, Value: paxos.Value{0, 1, 2, 255}}},
		{From: 3, To: 1, Msg: Accept{Ballot: b, Slot: 8}},
		{From: 1, To: 3, Msg: Accepted{Ballot: b, Slot: 8}},
		{From: 1, To: 3, Msg: Nack{Ballot: b, Promised: paxos.Ballot{Round: 13, Node: 2}, Slot: 8}},
		{From: 1, To: 3, Msg: Nack{Ballot: b, Promised: paxos.Ballot{Round: 13, Node: 2}, LeaseRemaining: 37 * time.Millisecond}},
		{From: 3, To: 2, Msg: Learn{Slot: 8, Ballot: b, Value: paxos.Value("x")}},
		{From: 2, To: 3, Msg: LearnRequest{FromSlot: 5, MaxCount: 256}},
		{From: 3, To: 2, Msg: Heartbeat{Ballot: b, CommitIndex: 8, ReadSeq: 4}},
		{From: 2, To: 3, Msg: HeartbeatAck{Ballot: b, Promised: b, CommitIndex: 7, ReadSeq: 4}},
	}
}

func TestCodecRoundTrip(t *testing.T) {
	for _, env := range sampleEnvelopes() {
		t.Run(TypeName(env.Msg), func(t *testing.T) {
			b, err := Encode(env)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if !strings.Contains(string(b), `"type":"`+TypeName(env.Msg)+`"`) {
				t.Errorf("wire form lacks the type name: %s", b)
			}
			got, err := Decode(b)
			if err != nil {
				t.Fatalf("Decode(%s): %v", b, err)
			}
			if !reflect.DeepEqual(got, env) {
				t.Errorf("round trip mismatch:\n got %#v\nwant %#v\nwire %s", got, env, b)
			}
		})
	}
}

func TestCodecWireShape(t *testing.T) {
	b, err := Encode(Envelope{From: 1, To: 2, Msg: Accept{Ballot: paxos.Ballot{Round: 3, Node: 1}, Slot: 4, Value: paxos.Value("hi")}})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"from":1,"to":2,"type":"accept","body":{"ballot":{"round":3,"node":1},"slot":4,"value":"aGk="}}`
	if string(b) != want {
		t.Errorf("wire form\n got %s\nwant %s", b, want)
	}
}

func TestEncodeRejectsUnknownMessage(t *testing.T) {
	type other struct{}
	var m Message = fakeMessage{}
	if _, err := Encode(Envelope{From: 1, To: 2, Msg: m}); err == nil {
		t.Error("Encode accepted a message type outside the nine")
	}
	_ = other{}
}

type fakeMessage struct{}

func (fakeMessage) msg() {}

func TestDecodeRejects(t *testing.T) {
	cases := []struct {
		name string
		in   string
	}{
		{"empty", ""},
		{"not json", "{"},
		{"array", "[]"},
		{"unknown type", `{"from":1,"to":2,"type":"vote","body":{}}`},
		{"missing type", `{"from":1,"to":2,"body":{}}`},
		{"unknown envelope field", `{"from":1,"to":2,"type":"accepted","body":{"ballot":{"round":1,"node":1},"slot":1},"extra":1}`},
		{"unknown body field", `{"from":1,"to":2,"type":"accepted","body":{"ballot":{"round":1,"node":1},"slot":1,"extra":true}}`},
		{"unknown ballot field", `{"from":1,"to":2,"type":"accepted","body":{"ballot":{"round":1,"node":1,"x":1},"slot":1}}`},
		{"trailing data", `{"from":1,"to":2,"type":"accepted","body":{"ballot":{"round":1,"node":1},"slot":1}} x`},
		{"second value", `{"from":1,"to":2,"type":"accepted","body":{"ballot":{"round":1,"node":1},"slot":1}}{}`},
		{"missing body", `{"from":1,"to":2,"type":"accepted"}`},
		{"zero from", `{"from":0,"to":2,"type":"accepted","body":{"ballot":{"round":1,"node":1},"slot":1}}`},
		{"zero to", `{"from":1,"to":0,"type":"accepted","body":{"ballot":{"round":1,"node":1},"slot":1}}`},
		{"string slot", `{"from":1,"to":2,"type":"accepted","body":{"ballot":{"round":1,"node":1},"slot":"1"}}`},
		{"negative round", `{"from":1,"to":2,"type":"accepted","body":{"ballot":{"round":-1,"node":1},"slot":1}}`},
		{"bad base64 value", `{"from":1,"to":2,"type":"accept","body":{"ballot":{"round":1,"node":1},"slot":1,"value":"***"}}`},
		{"body is a number", `{"from":1,"to":2,"type":"accept","body":5}`},
		{"body is an array", `{"from":1,"to":2,"type":"prepare","body":[1]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Decode([]byte(tc.in)); err == nil {
				t.Errorf("Decode(%q) returned no error", tc.in)
			}
		})
	}
}

func TestDecodeToleratesWhitespace(t *testing.T) {
	in := "  {\"from\":1,\"to\":2,\"type\":\"learn_request\",\"body\":{\"from_slot\":3,\"max_count\":4}} \n"
	env, err := Decode([]byte(in))
	if err != nil {
		t.Fatal(err)
	}
	want := Envelope{From: 1, To: 2, Msg: LearnRequest{FromSlot: 3, MaxCount: 4}}
	if !reflect.DeepEqual(env, want) {
		t.Errorf("got %#v, want %#v", env, want)
	}
}

// FuzzDecode checks that Decode never panics and that any input it accepts
// re-encodes to something it decodes to the same envelope.
func FuzzDecode(f *testing.F) {
	for _, env := range sampleEnvelopes() {
		b, err := Encode(env)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(b)
	}
	f.Add([]byte(""))
	f.Add([]byte("{}"))
	f.Add([]byte(`{"from":1,"to":2,"type":"accept","body":null}`))
	f.Fuzz(func(t *testing.T, in []byte) {
		env, err := Decode(in)
		if err != nil {
			return
		}
		out, err := Encode(env)
		if err != nil {
			t.Fatalf("Encode of a decoded envelope failed: %v (%#v)", err, env)
		}
		again, err := Decode(out)
		if err != nil {
			t.Fatalf("Decode(Encode(x)) failed: %v\nx=%#v\nwire=%s", err, env, out)
		}
		if !reflect.DeepEqual(again, env) {
			t.Fatalf("Decode(Encode(x)) != x:\n got %#v\nwant %#v", again, env)
		}
	})
}

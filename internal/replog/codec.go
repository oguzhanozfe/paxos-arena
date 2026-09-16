package replog

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/oguzhanozfe/paxos-arena/internal/jsonx"
	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
)

// wireEnvelope is the JSON form of an Envelope:
// {"from":1,"to":2,"type":"accept","body":{...}}.
type wireEnvelope struct {
	From paxos.NodeID    `json:"from"`
	To   paxos.NodeID    `json:"to"`
	Type string          `json:"type"`
	Body json.RawMessage `json:"body"`
}

// Encode renders an Envelope in the wire form. Value fields are base64
// through encoding/json's default []byte handling.
func Encode(env Envelope) ([]byte, error) {
	typ := TypeName(env.Msg)
	if typ == "" {
		return nil, fmt.Errorf("replog: cannot encode message of type %T", env.Msg)
	}
	body, err := json.Marshal(env.Msg)
	if err != nil {
		return nil, fmt.Errorf("replog: encode %s body: %w", typ, err)
	}
	return json.Marshal(wireEnvelope{From: env.From, To: env.To, Type: typ, Body: body})
}

// Decode parses the wire form. It rejects unknown message types, unknown
// fields, trailing data and zero node identifiers, and never panics.
func Decode(b []byte) (Envelope, error) {
	var w wireEnvelope
	if err := strictUnmarshal(b, &w); err != nil {
		return Envelope{}, fmt.Errorf("replog: decode envelope: %w", err)
	}
	if w.From == 0 || w.To == 0 {
		return Envelope{}, errors.New("replog: decode envelope: from and to must be non-zero")
	}
	if len(w.Body) == 0 {
		return Envelope{}, errors.New("replog: decode envelope: missing body")
	}
	env := Envelope{From: w.From, To: w.To}
	var err error
	switch w.Type {
	case "prepare":
		var m Prepare
		err = strictUnmarshal(w.Body, &m)
		env.Msg = m
	case "promise":
		var m Promise
		err = strictUnmarshal(w.Body, &m)
		env.Msg = m
	case "accept":
		var m Accept
		err = strictUnmarshal(w.Body, &m)
		env.Msg = m
	case "accepted":
		var m Accepted
		err = strictUnmarshal(w.Body, &m)
		env.Msg = m
	case "nack":
		var m Nack
		err = strictUnmarshal(w.Body, &m)
		env.Msg = m
	case "learn":
		var m Learn
		err = strictUnmarshal(w.Body, &m)
		env.Msg = m
	case "learn_request":
		var m LearnRequest
		err = strictUnmarshal(w.Body, &m)
		env.Msg = m
	case "heartbeat":
		var m Heartbeat
		err = strictUnmarshal(w.Body, &m)
		env.Msg = m
	case "heartbeat_ack":
		var m HeartbeatAck
		err = strictUnmarshal(w.Body, &m)
		env.Msg = m
	default:
		return Envelope{}, fmt.Errorf("replog: decode envelope: unknown message type %q", w.Type)
	}
	if err != nil {
		return Envelope{}, fmt.Errorf("replog: decode %s body: %w", w.Type, err)
	}
	return env, nil
}

// strictUnmarshal is jsonx.DecodeStrict: one JSON value, no unknown
// fields, nothing but whitespace after it.
func strictUnmarshal(b []byte, v any) error { return jsonx.DecodeStrict(b, v) }

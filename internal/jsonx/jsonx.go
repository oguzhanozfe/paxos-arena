// Package jsonx holds the one strict JSON decoding rule every layer of the
// module applies: the wire codecs of replog and tournament and the request
// bodies of the HTTP API. It sits below every other package of the module
// and imports only the standard library.
package jsonx

import (
	"bytes"
	"encoding/json"
	"errors"
)

// DecodeStrict decodes exactly one JSON value from b into v. It rejects
// unknown object fields and anything but JSON whitespace (space, tab, line
// feed, carriage return) after the value.
func DecodeStrict(b []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	off := dec.InputOffset()
	if off < 0 || off > int64(len(b)) {
		return errors.New("decoder offset out of range")
	}
	for _, c := range b[off:] {
		switch c {
		case ' ', '\t', '\n', '\r':
		default:
			return errors.New("trailing data after JSON value")
		}
	}
	return nil
}

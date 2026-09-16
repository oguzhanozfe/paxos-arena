package session

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// MaxTokenLen bounds a token Verify parses. A token the play API issues is
// under 250 characters.
const MaxTokenLen = 1024

// ErrBadKeySpec is a keyring entry that is not "id=hex".
var ErrBadKeySpec = errors.New("session: a keyring entry is not id=hex")

// ErrBadDevice is a device secret that is not 64 lower-case hex characters.
var ErrBadDevice = errors.New("session: the device secret is not 64 lower-case hex characters")

var b64 = base64.RawURLEncoding.Strict()

// ValidKeyID reports whether id is 1 to MaxKeyIDLen characters from a-z,
// 0-9 and '-'.
func ValidKeyID(id KeyID) bool {
	if len(id) < 1 || len(id) > MaxKeyIDLen {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		if !('a' <= c && c <= 'z' || '0' <= c && c <= '9' || c == '-') {
			return false
		}
	}
	return true
}

// ParseKeyring parses a keyring: entries "id=hex" separated by commas or
// line breaks, blank entries and lines starting with '#' ignored. The first
// key signs. It refuses an empty ring (ErrNoKeys), a malformed entry
// (ErrBadKeySpec), a bad key id (ErrBadKeyID), a key shorter than
// MinKeyBytes (ErrWeakKey) and a repeated id (ErrDuplicateKeyID). Errors
// never contain key material.
func ParseKeyring(spec string) (Keyring, error) {
	var ring Keyring
	seen := make(map[KeyID]bool)
	for _, line := range strings.Split(spec, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		for _, entry := range strings.Split(line, ",") {
			entry = strings.TrimSpace(entry)
			if entry == "" {
				continue
			}
			id, hexKey, ok := strings.Cut(entry, "=")
			if !ok {
				return Keyring{}, ErrBadKeySpec
			}
			kid := KeyID(strings.TrimSpace(id))
			if !ValidKeyID(kid) {
				return Keyring{}, ErrBadKeyID
			}
			secret, err := hex.DecodeString(strings.TrimSpace(hexKey))
			if err != nil {
				return Keyring{}, fmt.Errorf("%w: key %q is not hex", ErrBadKeySpec, kid)
			}
			if len(secret) < MinKeyBytes {
				return Keyring{}, fmt.Errorf("%w: key %q has %d bytes", ErrWeakKey, kid, len(secret))
			}
			if seen[kid] {
				return Keyring{}, fmt.Errorf("%w: %q", ErrDuplicateKeyID, kid)
			}
			seen[kid] = true
			ring.Keys = append(ring.Keys, Key{ID: kid, Secret: secret})
		}
	}
	if err := ring.Validate(); err != nil {
		return Keyring{}, err
	}
	return ring, nil
}

// Validate checks the rules ParseKeyring enforces on a ring built in code.
func (k Keyring) Validate() error {
	if len(k.Keys) == 0 {
		return ErrNoKeys
	}
	seen := make(map[KeyID]bool, len(k.Keys))
	for _, key := range k.Keys {
		if !ValidKeyID(key.ID) {
			return ErrBadKeyID
		}
		if len(key.Secret) < MinKeyBytes {
			return fmt.Errorf("%w: key %q has %d bytes", ErrWeakKey, key.ID, len(key.Secret))
		}
		if seen[key.ID] {
			return fmt.Errorf("%w: %q", ErrDuplicateKeyID, key.ID)
		}
		seen[key.ID] = true
	}
	return nil
}

func (k Keyring) find(id KeyID) (Key, bool) {
	for _, key := range k.Keys {
		if key.ID == id {
			return key, true
		}
	}
	return Key{}, false
}

func mac(secret []byte, signed string) []byte {
	m := hmac.New(sha256.New, secret)
	m.Write([]byte(signed))
	return m.Sum(nil)
}

// encodeClaims is the canonical payload: the four fields in order, no
// whitespace.
func encodeClaims(c Claims) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(c); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte{'\n'}), nil
}

// Sign returns the token carrying c, signed with the ring's first key.
func (k Keyring) Sign(c Claims) (string, error) {
	if len(k.Keys) == 0 {
		return "", ErrNoKeys
	}
	key := k.Keys[0]
	payload, err := encodeClaims(c)
	if err != nil {
		return "", fmt.Errorf("session: encode claims: %w", err)
	}
	signed := Version + "." + string(key.ID) + "." + b64.EncodeToString(payload)
	return signed + "." + b64.EncodeToString(mac(key.Secret, signed)), nil
}

// Verify checks token at nowMs, Unix milliseconds on the verifying replica,
// in the order of the contract's section 2.3, and returns its claims. The
// mac is compared in constant time. Every error means the client needs a
// new session; the play API answers ErrExpired with session_expired and
// every other error with session_invalid.
func (k Keyring) Verify(token string, nowMs int64) (Claims, error) {
	if len(token) > MaxTokenLen {
		return Claims{}, ErrMalformed
	}
	parts := strings.Split(token, ".")
	if len(parts) != 4 || parts[0] != Version || !ValidKeyID(KeyID(parts[1])) {
		return Claims{}, ErrMalformed
	}
	payload, err := b64.DecodeString(parts[2])
	if err != nil {
		return Claims{}, ErrMalformed
	}
	sum, err := b64.DecodeString(parts[3])
	if err != nil || len(sum) != sha256.Size {
		return Claims{}, ErrMalformed
	}
	var c Claims
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil || dec.More() {
		return Claims{}, ErrMalformed
	}
	// Exactly the canonical encoding: every field present, in order, with
	// no whitespace, no duplicates and nothing after the object.
	if canon, err := encodeClaims(c); err != nil || !bytes.Equal(canon, payload) {
		return Claims{}, ErrMalformed
	}
	key, ok := k.find(KeyID(parts[1]))
	if !ok {
		return Claims{}, ErrUnknownKey
	}
	signed := token[:len(parts[0])+1+len(parts[1])+1+len(parts[2])]
	if !hmac.Equal(mac(key.Secret, signed), sum) {
		return Claims{}, ErrBadMAC
	}
	if c.ExpiresAtMs < c.IssuedAtMs || c.ExpiresAtMs-c.IssuedAtMs > MaxTTL.Milliseconds() {
		return Claims{}, ErrLifetime
	}
	leeway := Leeway.Milliseconds()
	if c.IssuedAtMs > nowMs+leeway {
		return Claims{}, ErrNotYetValid
	}
	if nowMs > c.ExpiresAtMs+leeway {
		return Claims{}, ErrExpired
	}
	return c, nil
}

// ValidDeviceID reports whether id is DeviceIDLen lower-case hex characters.
func ValidDeviceID(id string) bool { return lowerHex(id, DeviceIDLen) }

// ValidDeviceSecret reports whether s is DeviceSecretLen lower-case hex
// characters.
func ValidDeviceSecret(s string) bool { return lowerHex(s, DeviceSecretLen) }

func lowerHex(s string, n int) bool {
	if len(s) != n {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !('0' <= c && c <= '9' || 'a' <= c && c <= 'f') {
			return false
		}
	}
	return true
}

// NewVerifier returns the verifier of a device secret given as 64
// lower-case hex characters: SHA-256 over VerifierDomain, a zero byte and
// the 32 bytes the hex encodes.
func NewVerifier(secretHex string) (Verifier, error) {
	if !ValidDeviceSecret(secretHex) {
		return Verifier{}, ErrBadDevice
	}
	secret, err := hex.DecodeString(secretHex)
	if err != nil {
		return Verifier{}, ErrBadDevice
	}
	h := sha256.New()
	h.Write([]byte(VerifierDomain))
	h.Write([]byte{0})
	h.Write(secret)
	var v Verifier
	copy(v[:], h.Sum(nil))
	return v, nil
}

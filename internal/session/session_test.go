package session

import (
	"encoding/base64"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
)

// Appendix A of docs/UNITY-INTEGRATION.md.
const (
	vectorKey     = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
	vectorToken   = "v1.k1.eyJwaWQiOiJwLW16eHc2eXRib2k0ZHFuYnJnbTJ3cXpsbW40IiwiZGlkIjoiOWY4NmQwODE4ODRjN2Q2NTlhMmZlYWEwYzU1YWQwMTUiLCJpYXQiOjE3ODkyMDAwMDAwMDAsImV4cCI6MTc4OTIwMzYwMDAwMH0.5CWAjViqDUMjFQmhATnN_JiC085QYywGbSPGTGBVjiE"
	vectorPayload = `{"pid":"p-mzxw6ytboi4dqnbrgm2wqzlmn4","did":"9f86d081884c7d659a2feaa0c55ad015","iat":1789200000000,"exp":1789203600000}`
	vectorSecret  = "2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae"
	vectorVerif   = "eefcb1443d2b7d069bf765df3584178eb2b4c9b5125242a75bf53c0c7e3da2b8"
)

var vectorClaims = Claims{Player: "p-mzxw6ytboi4dqnbrgm2wqzlmn4", Device: "9f86d081884c7d659a2feaa0c55ad015",
	IssuedAtMs: 1789200000000, ExpiresAtMs: 1789203600000}

func ring(t *testing.T, spec string) Keyring {
	t.Helper()
	k, err := ParseKeyring(spec)
	if err != nil {
		t.Fatalf("ParseKeyring: %v", err)
	}
	return k
}

func TestAppendixToken(t *testing.T) {
	k := ring(t, "k1="+vectorKey)
	tok, err := k.Sign(vectorClaims)
	if err != nil {
		t.Fatal(err)
	}
	if tok != vectorToken {
		t.Fatalf("token =\n%s\nwant\n%s", tok, vectorToken)
	}
	payload, _ := base64.RawURLEncoding.DecodeString(strings.Split(tok, ".")[2])
	if string(payload) != vectorPayload {
		t.Errorf("payload = %s", payload)
	}
	c, err := k.Verify(vectorToken, vectorClaims.IssuedAtMs+1000)
	if err != nil || c != vectorClaims {
		t.Fatalf("Verify = %+v, %v", c, err)
	}
}

func TestAppendixVerifier(t *testing.T) {
	v, err := NewVerifier(vectorSecret)
	if err != nil {
		t.Fatal(err)
	}
	if hex.EncodeToString(v[:]) != vectorVerif {
		t.Errorf("verifier = %x", v)
	}
	for _, bad := range []string{"", vectorSecret[:63], vectorSecret + "0", strings.ToUpper(vectorSecret), strings.Repeat("g", 64)} {
		if _, err := NewVerifier(bad); !errors.Is(err, ErrBadDevice) {
			t.Errorf("NewVerifier(%q) = %v", bad, err)
		}
	}
}

func TestDeviceID(t *testing.T) {
	if !ValidDeviceID("9f86d081884c7d659a2feaa0c55ad015") {
		t.Error("valid id refused")
	}
	for _, bad := range []string{"", "9f86d081884c7d659a2feaa0c55ad01", "9F86D081884C7D659A2FEAA0C55AD015", "9f86d081884c7d659a2feaa0c55ad01z", "9f86d081884c7d659a2feaa0c55ad0155"} {
		if ValidDeviceID(bad) {
			t.Errorf("ValidDeviceID(%q) = true", bad)
		}
	}
}

// resign replaces the payload of a token and signs it again with key, so
// that the payload rules are tested past the mac.
func resign(t *testing.T, payload string) string {
	t.Helper()
	signed := "v1.k1." + base64.RawURLEncoding.EncodeToString([]byte(payload))
	key, _ := hex.DecodeString(vectorKey)
	return signed + "." + base64.RawURLEncoding.EncodeToString(mac(key, signed))
}

func TestVerifyFailures(t *testing.T) {
	k := ring(t, "k1="+vectorKey)
	now := vectorClaims.IssuedAtMs + 60_000
	parts := strings.Split(vectorToken, ".")
	flip := func(s string, i int) string {
		b := []byte(s)
		if b[i] == 'A' {
			b[i] = 'B'
		} else {
			b[i] = 'A'
		}
		return string(b)
	}
	longLived := Claims{Player: "p", Device: "d", IssuedAtMs: now, ExpiresAtMs: now + MaxTTL.Milliseconds() + 1}
	longTok, _ := k.Sign(longLived)
	backwards, _ := k.Sign(Claims{Player: "p", Device: "d", IssuedAtMs: now, ExpiresAtMs: now - 1})
	cases := []struct {
		name  string
		token string
		now   int64
		want  error
	}{
		{"empty", "", now, ErrMalformed},
		{"three parts", strings.Join(parts[:3], "."), now, ErrMalformed},
		{"five parts", vectorToken + ".x", now, ErrMalformed},
		{"version", "v2." + strings.Join(parts[1:], "."), now, ErrMalformed},
		{"bad kid", "v1.K1." + strings.Join(parts[2:], "."), now, ErrMalformed},
		{"payload not base64url", "v1.k1.eyJ+." + parts[3], now, ErrMalformed},
		{"payload padded", "v1.k1." + parts[2] + "=." + parts[3], now, ErrMalformed},
		{"mac not base64url", "v1.k1." + parts[2] + ".5CWA*", now, ErrMalformed},
		{"mac short", "v1.k1." + parts[2] + "." + parts[3][:20], now, ErrMalformed},
		{"too long", vectorToken + strings.Repeat("A", MaxTokenLen), now, ErrMalformed},
		{"payload with space", resign(t, `{"pid":"p", "did":"d","iat":1,"exp":2}`), 1, ErrMalformed},
		{"payload reordered", resign(t, `{"did":"d","pid":"p","iat":1,"exp":2}`), 1, ErrMalformed},
		{"payload missing field", resign(t, `{"pid":"p","did":"d","iat":1}`), 1, ErrMalformed},
		{"payload extra field", resign(t, `{"pid":"p","did":"d","iat":1,"exp":2,"adm":true}`), 1, ErrMalformed},
		{"payload duplicate field", resign(t, `{"pid":"p","did":"d","iat":1,"exp":2,"exp":3}`), 1, ErrMalformed},
		{"payload float", resign(t, `{"pid":"p","did":"d","iat":1.0,"exp":2}`), 1, ErrMalformed},
		{"payload escaped", resign(t, `{"pid":"`+string(rune(92))+`u0070","did":"d","iat":1,"exp":2}`), 1, ErrMalformed},
		{"payload upper-case name", resign(t, `{"PID":"p","did":"d","iat":1,"exp":2}`), 1, ErrMalformed},
		{"payload trailing", resign(t, `{"pid":"p","did":"d","iat":1,"exp":2}{}`), 1, ErrMalformed},
		{"unknown key", "v1.k2." + strings.Join(parts[2:], "."), now, ErrUnknownKey},
		{"tampered payload", "v1.k1." + resignPayloadOnly(parts[2]) + "." + parts[3], now, ErrBadMAC},
		{"tampered mac", "v1.k1." + parts[2] + "." + flip(parts[3], 5), now, ErrBadMAC},
		{"lifetime above 24h", longTok, now, ErrLifetime},
		{"expiry before issue", backwards, now, ErrLifetime},
		{"issued 30001 ms ahead", vectorToken, vectorClaims.IssuedAtMs - 30_001, ErrNotYetValid},
		{"issued 30000 ms ahead", vectorToken, vectorClaims.IssuedAtMs - 30_000, nil},
		{"30000 ms past expiry", vectorToken, vectorClaims.ExpiresAtMs + 30_000, nil},
		{"30001 ms past expiry", vectorToken, vectorClaims.ExpiresAtMs + 30_001, ErrExpired},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := k.Verify(tc.token, tc.now)
			if !errors.Is(err, tc.want) || (tc.want == nil && err != nil) {
				t.Fatalf("Verify = %v, want %v", err, tc.want)
			}
		})
	}
}

// resignPayloadOnly changes one claim inside the payload without signing.
func resignPayloadOnly(p string) string {
	b, _ := base64.RawURLEncoding.DecodeString(p)
	s := strings.Replace(string(b), `"iat":1789200000000`, `"iat":1789100000000`, 1)
	return base64.RawURLEncoding.EncodeToString([]byte(s))
}

func TestWrongKey(t *testing.T) {
	other := ring(t, "k1="+strings.Repeat("ab", 32))
	if _, err := other.Verify(vectorToken, vectorClaims.IssuedAtMs); !errors.Is(err, ErrBadMAC) {
		t.Errorf("a token under another k1 = %v, want ErrBadMAC", err)
	}
}

func TestRotation(t *testing.T) {
	old := "k1=" + vectorKey
	next := "k2=" + strings.Repeat("cd", 32)
	step1 := ring(t, old+","+next) // signs k1, verifies both
	step2 := ring(t, next+"\n"+old)
	step3 := ring(t, next)
	now := vectorClaims.IssuedAtMs
	t1, _ := step1.Sign(vectorClaims)
	t2, _ := step2.Sign(vectorClaims)
	if !strings.HasPrefix(t1, "v1.k1.") || !strings.HasPrefix(t2, "v1.k2.") {
		t.Fatalf("signing keys: %s %s", t1[:6], t2[:6])
	}
	for name, r := range map[string]Keyring{"step1": step1, "step2": step2} {
		for _, tok := range []string{t1, t2} {
			if _, err := r.Verify(tok, now); err != nil {
				t.Errorf("%s verifying %s: %v", name, tok[:6], err)
			}
		}
	}
	if _, err := step3.Verify(t1, now); !errors.Is(err, ErrUnknownKey) {
		t.Errorf("removed key still verifies: %v", err)
	}
	if _, err := step3.Verify(t2, now); err != nil {
		t.Errorf("step3 verifying k2: %v", err)
	}
}

func TestParseKeyring(t *testing.T) {
	good := strings.Repeat("00", 32)
	k := ring(t, "# rotated 2026-09\n\nk2="+good+"\n  k1 = "+vectorKey+" ,\n")
	if len(k.Keys) != 2 || k.Keys[0].ID != "k2" || k.Keys[1].ID != "k1" || len(k.Keys[0].Secret) != 32 {
		t.Fatalf("keys = %+v", k.Keys)
	}
	for _, tc := range []struct {
		spec string
		want error
	}{
		{"", ErrNoKeys},
		{"# only a comment\n", ErrNoKeys},
		{" , ", ErrNoKeys},
		{"k1", ErrBadKeySpec},
		{"k1=zz", ErrBadKeySpec},
		{"=" + good, ErrBadKeyID},
		{"K1=" + good, ErrBadKeyID},
		{"k_1=" + good, ErrBadKeyID},
		{strings.Repeat("k", 17) + "=" + good, ErrBadKeyID},
		{"k1=" + good[:62], ErrWeakKey},
		{"k1=" + good + ",k1=" + vectorKey, ErrDuplicateKeyID},
	} {
		_, err := ParseKeyring(tc.spec)
		if !errors.Is(err, tc.want) {
			t.Errorf("ParseKeyring(%q) = %v, want %v", tc.spec, err, tc.want)
		}
		if err != nil && strings.Contains(err.Error(), good) {
			t.Errorf("error %q contains key material", err)
		}
	}
	if _, err := (Keyring{}).Sign(vectorClaims); !errors.Is(err, ErrNoKeys) {
		t.Errorf("Sign on an empty ring = %v", err)
	}
	if err := (Keyring{Keys: []Key{{ID: "k1", Secret: make([]byte, 31)}}}).Validate(); !errors.Is(err, ErrWeakKey) {
		t.Errorf("Validate short key = %v", err)
	}
}

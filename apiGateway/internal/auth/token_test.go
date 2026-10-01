package auth

import (
	"encoding/base64"
	"strings"
	"testing"
	"time"
)

func testSigner(t *testing.T, ttl time.Duration) *Signer {
	t.Helper()
	// 32 bytes of hex, the minimum accepted key length.
	s, err := NewSigner(strings.Repeat("ab", 32), ttl)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	return s
}

func TestNewSignerRejectsBadKeys(t *testing.T) {
	cases := map[string]string{
		"empty":      "",
		"not hex":    strings.Repeat("zz", 32),
		"too short":  strings.Repeat("ab", 8),
		"odd length": strings.Repeat("abc", 10),
		"whitespace": "   ",
	}
	for name, key := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := NewSigner(key, time.Minute); err == nil {
				t.Fatalf("NewSigner(%q) accepted an invalid key", key)
			}
		})
	}
}

func TestNewSignerDefaultsTTL(t *testing.T) {
	s := testSigner(t, 0)
	if s.TTL() != TokenTTL {
		t.Fatalf("TTL() = %v, want the %v default", s.TTL(), TokenTTL)
	}
	var nilSigner *Signer
	if nilSigner.TTL() != TokenTTL { // nil signer also reports the default
		t.Fatalf("nil TTL() = %v, want %v", nilSigner.TTL(), TokenTTL)
	}
}

func TestIssueAndVerifyRoundTrip(t *testing.T) {
	s := testSigner(t, 10*time.Minute)
	tok, exp, err := s.Issue("shreyas")
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if !LooksSigned(tok) {
		t.Fatalf("issued token %q is not recognised as signed", tok)
	}
	got, err := s.Verify(tok)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if got != "shreyas" {
		t.Fatalf("Verify subject = %q, want %q", got, "shreyas")
	}
	if d := time.Until(exp); d <= 0 || d > 10*time.Minute+time.Second {
		t.Fatalf("expiry %v is not ~10min away", d)
	}
}

func TestIssueRejectsEmptyIdentity(t *testing.T) {
	s := testSigner(t, time.Minute)
	if _, _, err := s.Issue("  "); err == nil {
		t.Fatal("Issue accepted an empty identity")
	}
}

func TestVerifyRejectsTamperedToken(t *testing.T) {
	s := testSigner(t, time.Minute)
	tok, _, err := s.Issue("shreyas")
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	payload, sig, _ := strings.Cut(tok, ".")

	t.Run("flipped signature", func(t *testing.T) {
		raw, _ := base64.RawURLEncoding.DecodeString(sig)
		raw[0] ^= 0xff
		bad := payload + "." + base64.RawURLEncoding.EncodeToString(raw)
		if _, err := s.Verify(bad); err == nil {
			t.Fatal("Verify accepted a tampered signature")
		}
	})
	t.Run("swapped payload", func(t *testing.T) {
		other, _, err := s.Issue("attacker")
		if err != nil {
			t.Fatalf("Issue: %v", err)
		}
		otherPayload, _, _ := strings.Cut(other, ".")
		// Keep shreyas's signature, swap in attacker's payload.
		bad := otherPayload + "." + sig
		if _, err := s.Verify(bad); err == nil {
			t.Fatal("Verify accepted a payload swapped under a valid signature")
		}
	})
	t.Run("unsigned token", func(t *testing.T) {
		if _, err := s.Verify("just-a-plain-token"); err == nil {
			t.Fatal("Verify accepted an unsigned token")
		}
	})
}

func TestVerifyRejectsOtherKey(t *testing.T) {
	mine := testSigner(t, time.Minute)
	theirs, err := NewSigner(strings.Repeat("cd", 32), time.Minute)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	tok, _, err := theirs.Issue("shreyas")
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if _, err := mine.Verify(tok); err == nil {
		t.Fatal("Verify accepted a token signed with a different key")
	}
}

func TestVerifyRejectsExpired(t *testing.T) {
	s := testSigner(t, time.Minute)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return base }
	tok, _, err := s.Issue("shreyas")
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}

	// Still valid just inside the window.
	s.now = func() time.Time { return base.Add(time.Minute) }
	if _, err := s.Verify(tok); err != nil {
		t.Fatalf("Verify rejected a token inside its TTL: %v", err)
	}
	// Expired past the window plus skew.
	s.now = func() time.Time { return base.Add(time.Minute + clockSkew + time.Second) }
	if _, err := s.Verify(tok); err == nil {
		t.Fatal("Verify accepted an expired token")
	}
}

func TestVerifyRejectsFarFutureIssuedAt(t *testing.T) {
	s := testSigner(t, time.Hour)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return base }
	tok, _, err := s.Issue("shreyas")
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	// A clock far behind the issuer would see iat in its future.
	s.now = func() time.Time { return base.Add(-clockSkew - time.Minute) }
	if _, err := s.Verify(tok); err == nil {
		t.Fatal("Verify accepted a token issued far in the future")
	}
}

func TestLooksSigned(t *testing.T) {
	cases := map[string]bool{
		"abc.def":     true,
		"abc":         false,
		"abc.":        false,
		".def":        false,
		"":            false,
		"has space.x": false,
		"a.b.c":       true,
	}
	for tok, want := range cases {
		if got := LooksSigned(tok); got != want {
			t.Errorf("LooksSigned(%q) = %v, want %v", tok, got, want)
		}
	}
}

func TestNilSignerIsInert(t *testing.T) {
	var s *Signer
	if _, _, err := s.Issue("shreyas"); err == nil {
		t.Fatal("nil Signer issued a token")
	}
	if _, err := s.Verify("a.b"); err == nil {
		t.Fatal("nil Signer verified a token")
	}
}

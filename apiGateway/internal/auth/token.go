package auth

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Signer issues and verifies short-lived bearer tokens so that external clients
// (a human in Postman, say) never have to hold a long-lived shared secret.
//
// Why HMAC-signed tokens rather than JWT: the format below carries no algorithm
// field, so there is nothing for an attacker to downgrade - the classic JWT
// failure mode where a server trusts the token's own `alg` header and can be
// handed `alg=none`, or an RS256 public key reinterpreted as an HMAC secret.
// The algorithm is fixed in code, the signature covers the encoded payload
// verbatim, and verification is one hmac.Equal.
//
// The signing key is a separate Secret from the static tokens, so it can be
// rotated independently and an apiGateway restart is not required to issue new
// tokens (the middleware re-reads it per request, like the token file).
//
// Scope: this is deliberately applied to external access tokens only. The
// in-cluster service tokens (MQ, ClickHouse) keep using the static shared-secret
// path in auth.go and do not expire - see docs/operations.md for why expiring
// them would add an outage class without adding security.

// TokenTTL is the default access-token lifetime. Short enough that a leaked
// token is quickly worthless, long enough that a client only refreshes every
// few minutes.
const TokenTTL = 15 * time.Minute

// clockSkew is the tolerance applied to exp/iat comparisons, to absorb modest
// clock drift between the issuer and any verifier.
const clockSkew = 30 * time.Second

// minKeyLen is the minimum accepted signing-key length in bytes. HMAC-SHA256
// gains nothing from keys longer than its 64-byte block, but a key this short
// is the minimum we consider reasonable for a shared secret.
const minKeyLen = 32

// claims is the signed body of an access token.
type claims struct {
	Subject   string `json:"sub"`
	IssuedAt  int64  `json:"iat"`
	ExpiresAt int64  `json:"exp"`
	TokenID   string `json:"jti"`
}

// Signer mints and verifies access tokens. A nil *Signer is valid and means
// "signed tokens disabled", mirroring how a nil *Authenticator means auth off.
type Signer struct {
	key []byte
	ttl time.Duration
	// now is swappable for tests; nil means time.Now.
	now func() time.Time
}

// NewSigner builds a Signer from a hex-encoded key of at least 32 bytes and a
// token lifetime. A non-positive ttl falls back to TokenTTL.
func NewSigner(hexKey string, ttl time.Duration) (*Signer, error) {
	hexKey = strings.TrimSpace(hexKey)
	if hexKey == "" {
		return nil, fmt.Errorf("empty token signing key")
	}
	key, err := hex.DecodeString(hexKey)
	if err != nil {
		return nil, fmt.Errorf("token signing key must be hex: %w", err)
	}
	if len(key) < minKeyLen {
		return nil, fmt.Errorf("token signing key must be at least %d bytes, got %d", minKeyLen, len(key))
	}
	if ttl <= 0 {
		ttl = TokenTTL
	}
	return &Signer{key: key, ttl: ttl}, nil
}

// TTL reports the access-token lifetime this Signer issues.
func (s *Signer) TTL() time.Duration {
	if s == nil || s.ttl <= 0 {
		return TokenTTL
	}
	return s.ttl
}

func (s *Signer) clock() time.Time {
	if s == nil || s.now == nil {
		return time.Now()
	}
	return s.now()
}

// Issue returns a signed access token for identity, valid for the Signer's TTL.
func (s *Signer) Issue(identity string) (string, time.Time, error) {
	if s == nil {
		return "", time.Time{}, fmt.Errorf("token signing is not configured")
	}
	identity = strings.TrimSpace(identity)
	if identity == "" {
		return "", time.Time{}, fmt.Errorf("cannot issue a token for an empty identity")
	}
	now := s.clock().UTC()
	exp := now.Add(s.TTL())

	// A random token id makes two tokens issued in the same second distinct, so
	// a caller cannot be recognised by token equality alone.
	var tid [16]byte
	if _, err := rand.Read(tid[:]); err != nil {
		return "", time.Time{}, fmt.Errorf("generate token id: %w", err)
	}
	body, err := json.Marshal(claims{
		Subject:   identity,
		IssuedAt:  now.Unix(),
		ExpiresAt: exp.Unix(),
		TokenID:   hex.EncodeToString(tid[:]),
	})
	if err != nil {
		return "", time.Time{}, fmt.Errorf("encode claims: %w", err)
	}
	enc := base64.RawURLEncoding
	payload := enc.EncodeToString(body)
	return payload + "." + enc.EncodeToString(s.sign(payload)), exp, nil
}

// sign returns the HMAC-SHA256 of payload under the signing key.
func (s *Signer) sign(payload string) []byte {
	mac := hmac.New(sha256.New, s.key)
	mac.Write([]byte(payload))
	return mac.Sum(nil)
}

// Verify checks a presented token's signature and expiry and returns the
// identity it was issued for. The error is deliberately coarse: distinguishing
// "bad signature" from "expired" tells an attacker which half to attack, and
// neither case needs operator action beyond re-issuing.
func (s *Signer) Verify(presented string) (string, error) {
	if s == nil {
		return "", fmt.Errorf("token signing is not configured")
	}
	payload, sig, found := strings.Cut(presented, ".")
	if !found || payload == "" || sig == "" {
		return "", fmt.Errorf("malformed token")
	}
	got, err := base64.RawURLEncoding.DecodeString(sig)
	if err != nil {
		return "", fmt.Errorf("malformed token")
	}
	// Constant-time: a byte-by-byte early return would leak the correct
	// signature prefix.
	if subtle.ConstantTimeCompare(got, s.sign(payload)) != 1 {
		return "", fmt.Errorf("invalid token")
	}
	body, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil {
		return "", fmt.Errorf("malformed token")
	}
	var c claims
	if err := json.Unmarshal(body, &c); err != nil {
		return "", fmt.Errorf("malformed token")
	}
	if c.Subject == "" {
		return "", fmt.Errorf("malformed token")
	}
	now := s.clock()
	// Reject a token that is already expired, and one that claims to be issued
	// in the future by more than the skew tolerance.
	if now.After(time.Unix(c.ExpiresAt, 0).Add(clockSkew)) {
		return "", fmt.Errorf("invalid token")
	}
	if time.Unix(c.IssuedAt, 0).After(now.Add(clockSkew)) {
		return "", fmt.Errorf("invalid token")
	}
	return c.Subject, nil
}

// LooksSigned reports whether a presented token is in the signed-token shape,
// so the caller can avoid running a static token through the base64/HMAC path.
// This is a shape test only and says nothing about validity.
func LooksSigned(presented string) bool {
	payload, sig, found := strings.Cut(presented, ".")
	return found && payload != "" && sig != "" && !strings.Contains(presented, " ")
}

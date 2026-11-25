// Package bootstrap defines the credential-limited worker handoff. JIT remains
// opaque and is decoded only by the native runner inside the disposable VM.
package bootstrap

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
)

// JIT is one environment string at native exec. Stay below Linux's 128KiB
// per-string limit and leave room in macOS's total argument/environment budget.
const MaxJITBytes = 64 << 10
const Lifetime = 10 * time.Minute

// Token is a per-lease 256-bit bearer capability. Only Encoded deliberately
// reveals it, for injection into a worker's trusted bootstrap environment.
type Token struct{ bytes [32]byte }

func NewToken() (Token, error) {
	var token Token
	_, err := rand.Read(token.bytes[:])
	return token, err
}

func ParseToken(raw string) (Token, error) {
	var token Token
	if len(raw) != 43 {
		return token, errors.New("invalid bootstrap token")
	}
	b, err := base64.RawURLEncoding.Strict().DecodeString(raw)
	if err != nil || len(b) != len(token.bytes) {
		return token, errors.New("invalid bootstrap token")
	}
	copy(token.bytes[:], b)
	clear(b)
	if token == (Token{}) {
		return token, errors.New("invalid bootstrap token")
	}
	return token, nil
}

func (t Token) Encoded() string            { return base64.RawURLEncoding.EncodeToString(t.bytes[:]) }
func (t Token) Hash() [32]byte             { return sha256.Sum256(t.bytes[:]) }
func (Token) Format(s fmt.State, _ rune)   { _, _ = io.WriteString(s, "[redacted bootstrap token]") }
func (Token) MarshalJSON() ([]byte, error) { return []byte(`"[redacted bootstrap token]"`), nil }
func (Token) MarshalText() ([]byte, error) { return []byte("[redacted bootstrap token]"), nil }

func Path(leaseID string) (string, error) {
	id, err := uuid.Parse(leaseID)
	if err != nil || id == uuid.Nil || id.String() != leaseID {
		return "", errors.New("bootstrap requires a canonical lease UUID")
	}
	return "/v1/leases/" + leaseID + "/jit", nil
}

// ValidJIT checks encoding and bounds, not its decoded files or service-side
// validity. Decoding credentials is the native runner's responsibility.
func ValidJIT(b []byte) bool {
	if len(b) == 0 || len(b) > MaxJITBytes || len(b)%4 != 0 || strings.ContainsAny(string(b), "\r\n") {
		return false
	}
	_, err := io.Copy(io.Discard, base64.NewDecoder(base64.StdEncoding.Strict(), strings.NewReader(string(b))))
	return err == nil
}

// Fetch makes one authenticated HTTPS request using only the caller's explicit
// public trust roots. No proxy environment, redirects, retries or response-body
// errors can route or expose the bootstrap token. An interrupted handoff is
// never retried here.
func Fetch(ctx context.Context, endpoint, leaseID string, token Token, roots *x509.CertPool) ([]byte, error) {
	path, err := Path(leaseID)
	u, parseErr := url.Parse(endpoint)
	if err != nil || parseErr != nil || roots == nil || u.Scheme != "https" || u.Host == "" || u.User != nil ||
		u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || (u.Path != "" && u.Path != "/") || token == (Token{}) {
		return nil, errors.New("invalid HTTPS bootstrap origin, lease, or token")
	}
	u.Path, u.RawPath = path, ""
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 30 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("bootstrap redirect refused") }}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, errors.New("invalid bootstrap request")
	}
	req.Header.Set("Authorization", "Bearer "+token.Encoded())
	resp, err := client.Do(req)
	if err != nil {
		return nil, errors.New("bootstrap request failed; do not retry")
	}
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != "application/octet-stream" {
		resp.Body.Close()
		return nil, errors.New("bootstrap response refused; do not retry")
	}
	b, readErr := io.ReadAll(io.LimitReader(resp.Body, MaxJITBytes+1))
	closeErr := resp.Body.Close()
	if readErr != nil || closeErr != nil || !ValidJIT(b) {
		clear(b)
		return nil, errors.New("bootstrap response incomplete or invalid; do not retry")
	}
	return b, nil
}

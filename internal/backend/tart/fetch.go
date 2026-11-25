package tart

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/cameronsjo/slipway/internal/bootstrap"
)

// fetchJIT makes the contract's one-use bootstrap request from the host, not
// from the guest.
//
// The pf anchor blocks every guest-to-host packet except DHCP and DNS, so a Mac
// guest cannot reach the bootstrap listener at all. The adapter runs in the
// same process as the JITBroker, so it dials the loopback listener directly
// while still presenting the advertised origin as the TLS server name: no
// /etc/hosts edit, no LAN exposure, and the certificate is validated against
// only the configured bundle.
//
// One request. No proxy, no redirect, no retry, TLS 1.2 floor. A failure here
// burns the capability by design -- the controller revokes the transfer and the
// lease stays charged.
func (b *Backend) fetchJIT(ctx context.Context, leaseID string, token bootstrap.Token) ([]byte, error) {
	path, err := bootstrap.Path(leaseID)
	if err != nil {
		return nil, err
	}
	origin, err := url.Parse(b.config.BootstrapURL)
	if err != nil {
		return nil, errors.New("tart bootstrap origin is unparsable")
	}
	// Dial the address the listener was actually told to bind, not a
	// reconstructed one. Config.validate has already pinned that address to
	// exactly 127.0.0.1 or ::1 and pinned the origin's port to match it, so
	// these are the same endpoint by construction rather than by coincidence.
	dialTarget := b.config.BootstrapBindAddress
	port := origin.Port()
	if port == "" {
		return nil, errors.New("tart bootstrap origin lacks an explicit port")
	}
	roots, err := b.bootstrapRoots()
	if err != nil {
		return nil, err
	}
	transport := &http.Transport{
		Proxy:               nil,
		DisableKeepAlives:   true,
		TLSHandshakeTimeout: 10 * time.Second,
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			if network != "tcp" && network != "tcp4" && network != "tcp6" {
				return nil, errors.New("tart bootstrap fetch refuses a non-TCP dial")
			}
			return (&net.Dialer{Timeout: 10 * time.Second}).DialContext(ctx, "tcp", dialTarget)
		},
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    roots,
			ServerName: origin.Hostname(),
		},
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 30 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return errors.New("tart bootstrap fetch refuses a redirect")
		}}
	endpoint := (&url.URL{Scheme: "https", Host: net.JoinHostPort(origin.Hostname(), port), Path: path}).String()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, errors.New("tart bootstrap request could not be built")
	}
	request.Header.Set("Authorization", "Bearer "+token.Encoded())
	response, err := client.Do(request)
	if err != nil {
		return nil, errors.New("tart bootstrap request failed; do not retry")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != "application/octet-stream" {
		return nil, errors.New("tart bootstrap response refused; do not retry")
	}
	payload, err := io.ReadAll(io.LimitReader(response.Body, bootstrap.MaxJITBytes+1))
	if err != nil || !bootstrap.ValidJIT(payload) {
		clear(payload)
		return nil, errors.New("tart bootstrap response incomplete or invalid; do not retry")
	}
	return payload, nil
}

func (b *Backend) bootstrapRoots() (*x509.CertPool, error) {
	pem, err := os.ReadFile(b.config.BootstrapCAFile)
	if err != nil {
		return nil, errors.New("tart bootstrap CA bundle is unreadable")
	}
	// Only the configured bundle. Never the host system trust store: on this
	// Mac that store carries every public CA, which would let anything with a
	// WebPKI certificate answer for the bootstrap hostname.
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		return nil, errors.New("tart bootstrap CA bundle contains no certificate")
	}
	return roots, nil
}

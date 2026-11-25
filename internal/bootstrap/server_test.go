package bootstrap

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func serverCertificate(t *testing.T) (string, string, *x509.CertPool) {
	t.Helper()
	certPEM, keyPEM := serverCertificatePEM(t)
	dir := t.TempDir()
	certFile, keyFile := filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key")
	if err := os.WriteFile(certFile, certPEM, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0600); err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(certPEM)
	return certFile, keyFile, roots
}

func serverCertificatePEM(t *testing.T) ([]byte, []byte) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IsCA:         true,
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
}

func TestBoundedBootstrapServerTLSAndShutdown(t *testing.T) {
	certFile, keyFile, roots := serverCertificate(t)
	var calls atomic.Int32
	server, err := NewServer(ServerConfig{Address: "127.0.0.1:0", CertificateFile: certFile, KeyFile: keyFile}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.TLS == nil {
			t.Error("handler received plaintext request")
		}
		fmt.Fprint(w, "ok")
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	if cap(server.listener.slots) != ServerMaxConnections || server.http.ReadHeaderTimeout != ServerReadHeaderTime ||
		server.http.ReadTimeout != ServerReadTime || server.http.WriteTimeout != ServerWriteTime ||
		server.http.IdleTimeout != ServerIdleTime || server.http.MaxHeaderBytes != ServerMaxHeaderBytes {
		t.Fatal("server resource bounds differ from contract")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	plain := &http.Client{Timeout: 2 * time.Second}
	if resp, err := plain.Get("http://" + server.Address().String()); err == nil {
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			t.Fatal("TLS listener accepted plaintext HTTP")
		}
	}
	if calls.Load() != 0 {
		t.Fatal("plaintext request reached JIT handler")
	}
	client := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}}}
	resp, err := client.Get("https://" + server.Address().String())
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || calls.Load() != 1 {
		t.Fatalf("TLS positive control failed: status=%d calls=%d", resp.StatusCode, calls.Load())
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not stop after cancellation")
	}
	if _, err := client.Get("https://" + server.Address().String()); err == nil {
		t.Fatal("server accepted a request after shutdown")
	}
}

// probeHandler stands in for the JITBroker. The listener must hand it every
// request it accepts at all; anything answered without the marker header came
// from a mux, a default route, or another handler this listener must not have.
// What the handler answers is the broker's business, tested in internal/controller.
type probeHandler struct {
	calls atomic.Int32
	path  string
}

func (h *probeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.calls.Add(1)
	w.Header().Set("X-Bootstrap-Probe", "jit-handler")
	if r.Method != http.MethodGet || r.URL.Path != h.path {
		http.Error(w, "bootstrap unavailable", http.StatusForbidden)
		return
	}
	fmt.Fprint(w, "jit")
}

// This proves routing only: every method and path this listener answers at all
// is answered by the handler NewServer was given, and nothing else is served.
func TestBootstrapServerServesOnlyTheJITHandler(t *testing.T) {
	certFile, keyFile, roots := serverCertificate(t)
	leasePath, err := Path("11111111-1111-4111-8111-111111111111")
	if err != nil {
		t.Fatal(err)
	}
	handler := &probeHandler{path: leasePath}
	server, err := NewServer(ServerConfig{Address: "127.0.0.1:0", CertificateFile: certFile, KeyFile: keyFile}, handler)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	if server.http.Handler != http.Handler(handler) {
		t.Fatal("listener wraps or replaces the JIT handler")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	client := &http.Client{Timeout: 5 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}},
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return errors.New("bootstrap listener must not redirect")
		}}
	origin := "https://" + server.Address().String()
	probe := func(method, path string) (*http.Response, string) {
		t.Helper()
		before := handler.calls.Load()
		req, err := http.NewRequest(method, origin+path, nil)
		if err != nil {
			t.Fatalf("%s %s: %v", method, path, err)
		}
		req.Header.Set("Authorization", "Bearer probe-capability")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", method, path, err)
		}
		body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if err != nil {
			t.Fatalf("%s %s: %v", method, path, err)
		}
		if resp.Header.Get("X-Bootstrap-Probe") != "jit-handler" {
			t.Fatalf("%s %s was answered by something other than the JIT handler: %d", method, path, resp.StatusCode)
		}
		if handler.calls.Load() != before+1 {
			t.Fatalf("%s %s did not reach the JIT handler exactly once", method, path)
		}
		return resp, string(body)
	}
	// Positive control: the canonical shape reaches the handler and gets its
	// answer, so a listener that answered nothing could not pass the probes.
	if resp, body := probe(http.MethodGet, leasePath); resp.StatusCode != http.StatusOK || body != "jit" {
		t.Fatalf("canonical JIT request did not reach the handler: status=%d body=%q", resp.StatusCode, body)
	}
	methods := []string{http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut,
		http.MethodPatch, http.MethodDelete, http.MethodOptions, http.MethodTrace}
	paths := []string{"/", "/healthz", "/health", "/metrics", "/debug/pprof/", "/v1", "/v1/leases",
		leasePath, leasePath + "/", leasePath + "/../jit"}
	for _, method := range methods {
		for _, path := range paths {
			if method == http.MethodGet && path == leasePath {
				continue
			}
			probe(method, path)
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not stop after cancellation")
	}
}

// A controller that closes and rebuilds its listener must get the address back.
func TestBootstrapServerCloseReleasesTheAddress(t *testing.T) {
	certFile, keyFile, _ := serverCertificate(t)
	handler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	server, err := NewServer(ServerConfig{Address: "127.0.0.1:0", CertificateFile: certFile, KeyFile: keyFile}, handler)
	if err != nil {
		t.Fatal(err)
	}
	address := server.Address().String()
	if err := server.Close(); err != nil {
		t.Fatalf("close reported an error: %v", err)
	}
	if err := server.Close(); err != nil {
		t.Fatalf("second close is not idempotent: %v", err)
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("closed server still holds %s: %v", address, err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
}

func contractText(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile("CONTRACT.md")
	if err != nil {
		t.Fatal(err)
	}
	return strings.Join(strings.Fields(string(raw)), " ")
}

// The contract's numbers are generated from the constants, so changing a bound
// in code without changing CONTRACT.md fails here rather than drifting quietly.
func TestBootstrapServerBoundsAreNonZeroAndMatchContract(t *testing.T) {
	certFile, keyFile, _ := serverCertificate(t)
	server, err := NewServer(ServerConfig{Address: "127.0.0.1:0", CertificateFile: certFile, KeyFile: keyFile},
		http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	for name, deadline := range map[string]time.Duration{
		"header deadline": server.http.ReadHeaderTimeout,
		"read deadline":   server.http.ReadTimeout,
		"write deadline":  server.http.WriteTimeout,
		"idle deadline":   server.http.IdleTimeout,
		"shutdown window": ServerShutdownTime,
		"capability life": Lifetime,
	} {
		if deadline <= 0 {
			t.Errorf("%s is unbounded", name)
		}
		if deadline > time.Hour {
			t.Errorf("%s is not a useful bound: %s", name, deadline)
		}
	}
	if server.http.MaxHeaderBytes <= 0 || server.http.MaxHeaderBytes > 64<<10 {
		t.Errorf("header size is unbounded or too large: %d", server.http.MaxHeaderBytes)
	}
	if cap(server.listener.slots) <= 0 || cap(server.listener.slots) > 64 {
		t.Errorf("connection count is unbounded or too large: %d", cap(server.listener.slots))
	}
	if server.http.ReadHeaderTimeout > server.http.ReadTimeout {
		t.Error("header deadline outlasts the read deadline")
	}
	contract := contractText(t)
	phrases := []string{
		fmt.Sprintf("at most %d simultaneous connections", cap(server.listener.slots)),
		fmt.Sprintf("%d-second header deadline", int(server.http.ReadHeaderTimeout/time.Second)),
		fmt.Sprintf("%d-second read deadline", int(server.http.ReadTimeout/time.Second)),
		fmt.Sprintf("%d-second write deadline", int(server.http.WriteTimeout/time.Second)),
		fmt.Sprintf("%d-second idle deadline", int(server.http.IdleTimeout/time.Second)),
		fmt.Sprintf("%d KiB maximum header size", server.http.MaxHeaderBytes>>10),
		fmt.Sprintf("at most %d KiB", MaxJITBytes>>10),
	}
	// Spelled-out durations cannot be generated, so pin the value beside the text.
	if ServerShutdownTime == 2*time.Minute {
		phrases = append(phrases, "at most two minutes")
	} else {
		t.Errorf("shutdown window %s no longer matches the contract's two minutes", ServerShutdownTime)
	}
	if Lifetime == 10*time.Minute {
		phrases = append(phrases, "expire ten minutes after preparation")
	} else {
		t.Errorf("capability lifetime %s no longer matches the contract's ten minutes", Lifetime)
	}
	for _, phrase := range phrases {
		if !strings.Contains(contract, phrase) {
			t.Errorf("CONTRACT.md does not state %q", phrase)
		}
	}
}

// The sections the delivery plan requires must exist as prose, not only in code.
func TestContractCoversTheBootstrapBoundary(t *testing.T) {
	contract := contractText(t)
	for name, phrase := range map[string]string{
		"endpoint placement":   "one dedicated bootstrap HTTPS listener",
		"TLS name":             "The advertised hostname is the TLS server name",
		"trust-root injection": "public PEM trust bundle",
		"token injection":      "SLIPWAY_BOOTSTRAP_TOKEN",
		"JIT-only routing":     "without a general controller mux",
		"expiry refusal":       "A revoked or expired capability receives the same credential-free refusal",
		"shutdown":             "first stops accepting bootstrap connections",
	} {
		if !strings.Contains(contract, phrase) {
			t.Errorf("CONTRACT.md is missing the %s section (%q)", name, phrase)
		}
	}
}

func TestBootstrapServerRejectsInvalidConfiguration(t *testing.T) {
	certFile, keyFile, _ := serverCertificate(t)
	handler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	for _, config := range []ServerConfig{
		{},
		{Address: "127.0.0.1", CertificateFile: certFile, KeyFile: keyFile},
		{Address: "127.0.0.1:0", CertificateFile: keyFile, KeyFile: certFile},
	} {
		if server, err := NewServer(config, handler); err == nil || server != nil {
			t.Fatalf("accepted invalid config: %+v", config)
		}
	}
	if server, err := NewServer(ServerConfig{Address: "127.0.0.1:0", CertificateFile: certFile, KeyFile: keyFile}, nil); err == nil || server != nil {
		t.Fatal("accepted nil JIT handler")
	}
	if err := os.Chmod(keyFile, 0644); err != nil {
		t.Fatal(err)
	}
	if server, err := NewServer(ServerConfig{Address: "127.0.0.1:0", CertificateFile: certFile, KeyFile: keyFile}, handler); err == nil || server != nil {
		t.Fatal("accepted world-readable TLS private key")
	}
	if err := os.Chmod(keyFile, 0600); err != nil {
		t.Fatal(err)
	}
}

// The bundle mounts the bootstrap keypair from a Kubernetes Secret volume,
// which projects each key as a symlink into a timestamped ..data/ directory,
// at defaultMode 0440 so the controller's group can read it. The old check
// Lstat'd for a regular file and rejected any group bit, so it refused both --
// the controller would have crash-looped at activation.
//
// Both directions are asserted here. One alone proves nothing: a check that
// accepts everything passes the positive case, and a check that accepts
// nothing passes the negative one.
func TestBootstrapServerLoadsSecretVolumeProjectedKeypair(t *testing.T) {
	certPEM, keyPEM := serverCertificatePEM(t)
	handler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})

	// Reproduce the Secret volume layout: real files under ..data, symlinks at
	// the mount root, group-readable at 0440.
	root := t.TempDir()
	data := filepath.Join(root, "..2026_09_13_00_00_00.123456789")
	if err := os.Mkdir(data, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(data, filepath.Join(root, "..data")); err != nil {
		t.Fatal(err)
	}
	projected := map[string][]byte{"tls.crt": certPEM, "tls.key": keyPEM}
	for name, content := range projected {
		if err := os.WriteFile(filepath.Join(data, name), content, 0440); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(filepath.Join("..data", name), filepath.Join(root, name)); err != nil {
			t.Fatal(err)
		}
	}
	certFile, keyFile := filepath.Join(root, "tls.crt"), filepath.Join(root, "tls.key")

	server, err := NewServer(ServerConfig{Address: "127.0.0.1:0", CertificateFile: certFile, KeyFile: keyFile}, handler)
	if err != nil || server == nil {
		t.Fatalf("refused a 0440 Secret-volume keypair the bundle actually mounts: %v", err)
	}
	server.Close()

	// A world-readable key is still refused, through the same symlinked path.
	if err := os.Chmod(filepath.Join(data, "tls.key"), 0444); err != nil {
		t.Fatal(err)
	}
	if server, err := NewServer(ServerConfig{Address: "127.0.0.1:0", CertificateFile: certFile, KeyFile: keyFile}, handler); err == nil || server != nil {
		t.Fatal("accepted a world-readable TLS private key")
	}

	// So is a key with no read bit at all, and one that is group-writable.
	for _, mode := range []os.FileMode{0000, 0620} {
		if err := os.Chmod(filepath.Join(data, "tls.key"), mode); err != nil {
			t.Fatal(err)
		}
		if server, err := NewServer(ServerConfig{Address: "127.0.0.1:0", CertificateFile: certFile, KeyFile: keyFile}, handler); err == nil || server != nil {
			t.Fatalf("accepted a TLS private key at mode %04o", mode)
		}
	}

	// And a path that resolves to something other than a regular file.
	if err := os.Chmod(filepath.Join(data, "tls.key"), 0440); err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(t.TempDir(), "tls.key")
	if err := os.Mkdir(directory, 0700); err != nil {
		t.Fatal(err)
	}
	if server, err := NewServer(ServerConfig{Address: "127.0.0.1:0", CertificateFile: certFile, KeyFile: directory}, handler); err == nil || server != nil {
		t.Fatal("accepted a directory as the TLS private key")
	}
}

func TestLimitedListenerCapsPerRemoteIPWithoutBlockingOtherTenants(t *testing.T) {
	l := &limitedListener{
		slots:    make(chan struct{}, ServerMaxConnections),
		maxPerIP: ServerMaxConnectionsPerIP,
		perIP:    make(map[string]int),
		done:     make(chan struct{}),
	}
	tenantA := "172.30.250.2"
	tenantB := "172.30.250.3"
	for i := 0; i < ServerMaxConnectionsPerIP; i++ {
		if !l.tryAcquire(tenantA) {
			t.Fatalf("tenant A acquire %d failed unexpectedly", i)
		}
	}
	if l.tryAcquire(tenantA) {
		t.Fatal("tenant A exceeded ServerMaxConnectionsPerIP")
	}
	// Another tenant must still acquire slots freely even while tenant A is capped.
	if !l.tryAcquire(tenantB) {
		t.Fatal("tenant B was blocked by tenant A hitting its per-IP cap")
	}
	// Releasing one slot for tenant A allows a fresh acquire.
	l.release(tenantA)
	if !l.tryAcquire(tenantA) {
		t.Fatal("tenant A could not re-acquire after release")
	}
	// Loopback returns empty host key (bounded only by maxTotal).
	if got := remoteHostKey(&net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12345}); got != "" {
		t.Fatalf("loopback host key = %q, want empty", got)
	}
	if got := remoteHostKey(&net.TCPAddr{IP: net.ParseIP("172.30.250.2"), Port: 12345}); got != tenantA {
		t.Fatalf("non-loopback host key = %q, want %q", got, tenantA)
	}
}

package tart

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cameronsjo/slipway/internal/bootstrap"
	"github.com/cameronsjo/slipway/internal/controller"
	"github.com/google/uuid"
)

// reviewedAnchorPath and assertScriptPath point at local testdata copies so
// `go test ./...` runs standalone after extraction, while still asserting
// byte-for-byte agreement with the homelab host files whenever those exist.
const (
	reviewedAnchorPath     = "testdata/tart-ci.anchor"
	reviewedAnchorRepoPath = "../../../../../hosts/m1max/pf/tart-ci.anchor"
	assertScriptPath       = "testdata/tart-pf-assert.sh"
	assertScriptRepoPath   = "../../../../../hosts/m1max/pf/tart-pf-assert.sh"
)

func readFixtureWithRepoSync(t *testing.T, testdataPath, repoPath string) []byte {
	t.Helper()
	data, err := os.ReadFile(testdataPath)
	if err != nil {
		t.Fatal(err)
	}
	if repoData, err := os.ReadFile(repoPath); err == nil {
		if !bytes.Equal(repoData, data) {
			t.Fatalf("%s drifted from %s; sync the testdata fixture", testdataPath, repoPath)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	return data
}

const (
	testLeaseID    = "11111111-2222-4333-8444-555555555555"
	testResourceID = "slipway-macos-" + testLeaseID
	testHostname   = "bootstrap.test"
)

// testJIT is a valid opaque JIT payload: base64, no newlines, length a multiple
// of four, which is all bootstrap.ValidJIT checks.
var testJIT = []byte(base64.StdEncoding.EncodeToString([]byte("jit-payload-for-the-pool-worker!")))

// invocation is one recorded external command. Every field a secret could reach
// is captured: the argv, the environment, and the stdin bytes.
type invocation struct {
	Argv  []string
	Env   []string
	Stdin []byte
}

func (i invocation) joined() string {
	return strings.Join(i.Argv, " ") + "\x00" + strings.Join(i.Env, " ") + "\x00" + string(i.Stdin)
}

// fakeRunner records every invocation and answers from a table of argv-prefix
// responders. Its default answers are a healthy M1 Max; each test overrides the
// one thing it is about.
type fakeRunner struct {
	mu          sync.Mutex
	calls       []invocation
	responses   map[string]func() ([]byte, error)
	blocking    []string
	startErr    error
	startedName string
	children    []*fakeProcess
	// streamErr, streamLine and streamBlocks configure the delivery child.
	streamErr    error
	streamLine   string
	streamBlocks bool
	streamStderr string
	// streamOutput is fed to the handshake writer in the "never accumulate"
	// test; totalStreamBytes counts what the adapter kept.
	deliveries []*fakeProcess
}

type fakeProcess struct {
	done      chan struct{}
	stopped   bool
	mu        sync.Mutex
	handshake chan string
	// blockHandshake models the real shape: `tart exec -i` does not return
	// until the job ends, so a delivery child that never handshakes and never
	// exits is the normal failure, not an exotic one.
	blockHandshake bool
	// diagnostic models the launcher's first stderr line.
	diagnostic string
}

func (p *fakeProcess) Stop() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.stopped {
		p.stopped = true
		close(p.done)
	}
	return nil
}

func (p *fakeProcess) Done() <-chan struct{} { return p.done }

func (p *fakeProcess) Diagnostic() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.diagnostic
}

func (p *fakeProcess) Handshake(ctx context.Context) (string, error) {
	if p.blockHandshake {
		select {
		case <-ctx.Done():
			return "", errors.New("owned child did not signal a successful launch in time")
		case <-p.done:
			return "", errors.New("owned child exited before it signalled a successful launch")
		}
	}
	select {
	case line := <-p.handshake:
		return line, nil
	case <-p.done:
		return "", errors.New("owned child exited before it signalled a successful launch")
	case <-ctx.Done():
		return "", errors.New("owned child did not signal a successful launch in time")
	}
}

func (p *fakeProcess) wasStopped() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stopped
}

func newFakeRunner() *fakeRunner {
	return &fakeRunner{responses: map[string]func() ([]byte, error){}}
}

// key turns an argv into the lookup key tests set responses against: the
// program's base name plus the non-flag arguments that select an operation.
func key(argv []string) string {
	if len(argv) == 0 {
		return ""
	}
	return strings.Join(append([]string{filepath.Base(argv[0])}, argv[1:]...), " ")
}

func (f *fakeRunner) Run(ctx context.Context, argv []string, env []string, stdin []byte) ([]byte, error) {
	f.mu.Lock()
	f.calls = append(f.calls, invocation{Argv: append([]string{}, argv...),
		Env: append([]string{}, env...), Stdin: append([]byte{}, stdin...)})
	responses := f.responses
	f.mu.Unlock()
	if ctx != nil {
		select {
		case <-ctx.Done():
			return nil, errors.New("fake command context was already done")
		default:
		}
	}
	full := key(argv)
	f.mu.Lock()
	blocking := append([]string{}, f.blocking...)
	f.mu.Unlock()
	for _, prefix := range blocking {
		if full == prefix || strings.HasPrefix(full, prefix+" ") {
			<-ctx.Done()
			return nil, errors.New("fake command was cancelled")
		}
	}
	// Longest matching prefix wins, so a test can override one specific `tart
	// exec` without the generic "tart exec" default racing it through map
	// iteration order.
	best, bestLen := (func() ([]byte, error))(nil), -1
	for prefix, respond := range responses {
		if respond == nil {
			continue
		}
		if (full == prefix || strings.HasPrefix(full, prefix+" ")) && len(prefix) > bestLen {
			best, bestLen = respond, len(prefix)
		}
	}
	if best != nil {
		return best()
	}
	return nil, fmt.Errorf("no fake response for %q", full)
}

func (f *fakeRunner) Start(_ context.Context, argv []string, env []string) (Process, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, invocation{Argv: append([]string{}, argv...), Env: append([]string{}, env...)})
	if f.startErr != nil {
		return nil, f.startErr
	}
	if len(argv) > 2 {
		f.startedName = argv[2]
	}
	child := &fakeProcess{done: make(chan struct{}), handshake: make(chan string, 1)}
	f.children = append(f.children, child)
	return child, nil
}

func (f *fakeRunner) StartStream(_ context.Context, argv []string, env []string, stdin []byte) (StreamProcess, error) {
	f.mu.Lock()
	f.calls = append(f.calls, invocation{Argv: append([]string{}, argv...),
		Env: append([]string{}, env...), Stdin: append([]byte{}, stdin...)})
	if f.streamErr != nil {
		err := f.streamErr
		f.mu.Unlock()
		return nil, err
	}
	line, blocks := f.streamLine, f.streamBlocks
	child := &fakeProcess{done: make(chan struct{}), handshake: make(chan string, 1),
		blockHandshake: blocks, diagnostic: f.streamStderr}
	if !blocks {
		child.handshake <- line
	}
	f.children = append(f.children, child)
	f.deliveries = append(f.deliveries, child)
	f.mu.Unlock()
	return child, nil
}

// setBlocking makes a command hang until its context is done, which is what a
// wedged pfctl looks like. Without the timeout under test, this blocks forever.
func (f *fakeRunner) setBlocking(prefix string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.responses[prefix] = nil
	f.blocking = append(f.blocking, prefix)
}

func (f *fakeRunner) set(prefix string, out string, err error) {
	f.respond(prefix, func() ([]byte, error) { return []byte(out), err })
}

// respond installs a responder with a side effect, for the commands whose real
// counterpart changes host state the adapter reads back.
func (f *fakeRunner) respond(prefix string, responder func() ([]byte, error)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.responses[prefix] = responder
}

// response returns the responder installed for prefix, so a test can wrap it.
func (f *fakeRunner) response(prefix string) func() ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.responses[prefix]
}

func (f *fakeRunner) recorded() []invocation {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]invocation{}, f.calls...)
}

// ran reports whether any recorded argv starts with the given key prefix.
func (f *fakeRunner) ran(prefix string) bool {
	for _, call := range f.recorded() {
		full := key(call.Argv)
		if full == prefix || strings.HasPrefix(full, prefix+" ") {
			return true
		}
	}
	return false
}

func (f *fakeRunner) indexOf(prefix string) int {
	for index, call := range f.recorded() {
		full := key(call.Argv)
		if full == prefix || strings.HasPrefix(full, prefix+" ") {
			return index
		}
	}
	return -1
}

// healthyHost is the default answer set: pf enabled and correctly ordered, an
// anchor with eight block rules, ample free memory, and a guest address inside
// the configured subnet.
func (f *fakeRunner) healthyHost(config Config) {
	f.set("sudo -n "+config.PfctlBinary+" -s info", "Status: Enabled for 0 days 01:02:03\n", nil)
	f.set("sudo -n "+config.PfctlBinary+" -a "+config.PfAnchor+" -s rules", healthyAnchorRules, nil)
	f.set("sudo -n "+config.PfctlBinary+" -s rules", healthyMainRuleset, nil)
	f.set("vm_stat", healthyVMStat, nil)
	f.set("tart list --format json", listing(vm{Name: "ci-guest", Running: true}), nil)
	f.streamLine = handshakeLine
	// The real-host fact that shapes the whole delivery design: `tart exec -i
	// <vm> <launcher>` does NOT return until the job ends, because the launcher
	// execs the native runner in place. Modelling it as an instant success
	// would let a Run-based delivery pass every test while stalling the daemon
	// for the length of a real job.
	f.setBlocking("tart exec -i " + testResourceID + " " + config.GuestLauncherPath)
	f.set("tart clone", "", nil)
	f.set("tart set", "", nil)
	f.set("tart exec", "", nil)
	f.set("tart ip", "192.168.2.5\n", nil)
	f.set("tart stop", "", nil)
	f.set("tart delete", "", nil)
}

// healthyAnchorRules is what `pfctl -a tart-ci -s rules` prints for the
// reviewed anchor: the LOADED ruleset, so pf's braced lists are already
// expanded -- the file's three `pass` lines carry two-element lists and load as
// six rules, and the eight `block drop` lines load as eight.
//
// Written out by hand rather than derived from the adapter's own counter. A
// fixture generated by the code under test could not tell a correct expansion
// from a wrong one applied consistently to both sides.
const healthyAnchorRules = `pass in quick on bridge100 inet proto udp from 0.0.0.0 to 255.255.255.255 port = 67
pass in quick on bridge100 inet proto udp from 0.0.0.0 to 255.255.255.255 port = 68
pass in quick on bridge100 inet proto udp from 192.168.2.0/24 to any port = 67
pass in quick on bridge100 inet proto udp from 192.168.2.0/24 to any port = 68
pass in quick on bridge100 inet proto tcp from 192.168.2.0/24 to 192.168.2.1 port = 53
pass in quick on bridge100 inet proto udp from 192.168.2.0/24 to 192.168.2.1 port = 53
block drop in quick on bridge100 inet from ! 192.168.2.0/24 to any
block drop in quick inet from 192.168.2.0/24 to <hostaddrs>
block drop in quick inet from 192.168.2.0/24 to 192.168.1.0/24
block drop in quick inet from 192.168.2.0/24 to 100.64.0.0/10
block drop in quick inet from 192.168.2.0/24 to 192.168.2.1
block drop in quick on bridge100 inet6 from any to <hostaddrs6>
block drop in quick on bridge100 inet6 from any to fe80::/10
block drop in quick on bridge100 inet6 from any to fc00::/7
`

// healthyMainRuleset is the shape `pfctl -s rules` prints on the real host:
// Apple's anchor is a wildcard path, ours is a bare name, and both are inside
// an `anchor "<name>" all` line.
const healthyMainRuleset = `scrub-anchor "com.apple/*" all fragment reassemble
anchor "tart-ci" all
anchor "com.apple.internet-sharing/*" all
anchor "com.apple/*" all
`

// vm models one `tart list --format json` entry. The real host emits BOTH
// State and Running, with capitalised keys, alongside Source/Size/Disk/Accessed.
type vm struct {
	Name    string
	Running bool
}

func listing(entries ...vm) string {
	items := make([]string, 0, len(entries))
	for _, entry := range entries {
		state := "stopped"
		if entry.Running {
			state = "running"
		}
		items = append(items, fmt.Sprintf(
			`{"Source":"local","Name":%q,"Disk":50,"Size":31,"SizeOnDisk":31,"Running":%t,"State":%q,"Accessed":"2026-09-20T12:00:00Z"}`,
			entry.Name, entry.Running, state))
	}
	return "[" + strings.Join(items, ",") + "]"
}

// healthyVMStat is a real vm_stat shape: 32 GiB of free plus inactive pages at
// a 16 KiB page size, comfortably over the 12 GiB floor.
const healthyVMStat = `Mach Virtual Memory Statistics: (page size of 16384 bytes)
Pages free:                                   1048576.
Pages active:                                 2000000.
Pages inactive:                               1048576.
Pages speculative:                              50000.
`

// starvedVMStat is the same shape with free plus inactive under the floor.
const starvedVMStat = `Mach Virtual Memory Statistics: (page size of 16384 bytes)
Pages free:                                     10000.
Pages active:                                 2000000.
Pages inactive:                                 10000.
`

// logSink captures every structured log record so a test can assert that no
// secret ever reached one.
type logSink struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (s *logSink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buffer.Write(p)
}

func (s *logSink) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buffer.String()
}

// fixture is one fully wired backend: temp tart home, temp ledger, verified
// host artifacts, and a loopback TLS bootstrap listener with its own CA.
type fixture struct {
	t        *testing.T
	backend  *Backend
	runner   *fakeRunner
	config   Config
	logs     *logSink
	token    bootstrap.Token
	jitCalls int
}

func newFixture(t *testing.T, adjust func(*Config)) *fixture {
	t.Helper()
	root := t.TempDir()
	tartHome := filepath.Join(root, "tart")
	ledger := filepath.Join(root, "ledger")
	if err := os.MkdirAll(filepath.Join(tartHome, "vms"), 0o700); err != nil {
		t.Fatal(err)
	}

	tarball := filepath.Join(root, "runner.tar.gz")
	writeArtifact(t, tarball, []byte("runner tarball bytes"))
	launcher := filepath.Join(root, "runner-pool-guest-launch")
	writeArtifact(t, launcher, []byte("guest launcher bytes"))

	// The reviewed anchor, byte-for-byte, so the content hash is a real one.
	anchorFile := filepath.Join(root, "tart-ci.anchor")
	anchorBytes := readFixtureWithRepoSync(t, reviewedAnchorPath, reviewedAnchorRepoPath)
	writeArtifact(t, anchorFile, anchorBytes)

	caPEM, serverCert := newLoopbackTLS(t, testHostname)
	caFile := filepath.Join(root, "bootstrap-ca.pem")
	if err := os.WriteFile(caFile, caPEM, 0o600); err != nil {
		t.Fatal(err)
	}

	token, err := bootstrap.NewToken()
	if err != nil {
		t.Fatal(err)
	}
	fix := &fixture{t: t, token: token, logs: &logSink{}}
	port := fix.serveBootstrap(serverCert)

	config := Config{
		TartBinary: "/opt/homebrew/bin/tart", SudoBinary: "/usr/bin/sudo",
		PfctlBinary: "/sbin/pfctl", VMStatBinary: "/usr/bin/vm_stat",
		PfAnchor: "tart-ci", PfConflictingAnchor: "com.apple.internet-sharing", PfMinBlockRules: MinAnchorBlockRules,
		PfAnchorFile: anchorFile, PfAnchorSHA256: digestOf(anchorBytes),
		BaseImageRef: "ghcr.io/cirruslabs/macos-tahoe-xcode@sha256:" + strings.Repeat("a", 64),
		TartHome:     tartHome, LedgerDir: ledger,
		GuestSubnet: "192.168.2.0/24", GuestGateway: "192.168.2.1",
		VMCPUCount: 4, VMMemoryMiB: 8192,
		PhysicalVMCapacity: 2, MaxOwnedVMs: 1, MemoryFloorBytes: 12 << 30,
		RunnerTarballPath: tarball, RunnerTarballSHA256: digestOf([]byte("runner tarball bytes")),
		GuestRunnerRoot: "/Users/admin/sjo-actions-runner",
		LauncherPath:    launcher, LauncherSHA256: digestOf([]byte("guest launcher bytes")),
		GuestLauncherPath: "/Users/admin/sjo-runner-pool-guest-launch", GuestUser: "admin",
		BootstrapURL:         "https://" + testHostname + ":" + port,
		BootstrapBindAddress: "127.0.0.1:" + port,
		BootstrapCAFile:      caFile,
		StartupPollInterval:  time.Millisecond, ReassertInterval: time.Hour, StopTimeout: time.Second,
		GateReadTimeout: 5 * time.Second, LaunchHandshakeTimeout: 2 * time.Second,
	}
	if adjust != nil {
		adjust(&config)
	}
	runner := newFakeRunner()
	runner.healthyHost(config)
	// Real `tart set --random-mac` rewrites the clone's config.json. The fake
	// does the same with a MAC no fixture VM carries, so the read-back after it
	// sees a randomized identity unless a test says otherwise.
	runner.respond("tart set", func() ([]byte, error) {
		writeVMConfig(t, tartHome, testResourceID, "darwin", freshMAC)
		return nil, nil
	})
	logger := slog.New(slog.NewJSONHandler(fix.logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	backend, err := New(context.Background(), runner, config, logger)
	if err != nil {
		t.Fatalf("fixture backend did not construct: %v", err)
	}
	fix.backend, fix.runner, fix.config = backend, runner, config
	// The default listing says ci-guest is running, so its on-disk shape must
	// exist too: a running VM with no readable config is an error by design.
	fix.writeVM("ci-guest", "darwin")
	return fix
}

// serveBootstrap stands in for the controller's own loopback JIT listener. It
// answers the contract's exact request shape once and counts every call, so a
// test can prove the capability was spent exactly once.
func (f *fixture) serveBootstrap(cert tls.Certificate) string {
	f.t.Helper()
	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}, NextProtos: []string{"http/1.1"}})
	if err != nil {
		f.t.Fatal(err)
	}
	path, err := bootstrap.Path(testLeaseID)
	if err != nil {
		f.t.Fatal(err)
	}
	// Captured once. A test that swaps fix.token afterwards is simulating a
	// capability the listener does not know, which must be refused.
	expected := f.token.Encoded()
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.jitCalls++
		if r.URL.Path != path || r.Header.Get("Authorization") != "Bearer "+expected {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", fmt.Sprint(len(testJIT)))
		_, _ = w.Write(testJIT)
	}), ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = server.Serve(listener) }()
	f.t.Cleanup(func() { _ = server.Close() })
	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		f.t.Fatal(err)
	}
	return port
}

func (f *fixture) intent() controller.WorkerCreateIntent {
	return controller.WorkerCreateIntent{
		LeaseID: testLeaseID, ResourceID: testResourceID, Slot: 0,
		BootstrapURL: f.config.BootstrapURL, BootstrapToken: f.token,
		StartupDeadline: time.Now().Add(time.Minute),
	}
}

// writeVM creates the on-disk shape of a VM: its directory and a config.json
// naming the guest OS.
func (f *fixture) writeVM(name, guestOS string) {
	f.t.Helper()
	writeVMConfig(f.t, f.config.TartHome, name, guestOS, baseImageMAC)
}

// baseImageMAC is what `tart clone` copies verbatim from the base image, and so
// what ci-guest and every un-randomized clone carry. freshMAC is what the fake
// `tart set --random-mac` assigns.
const (
	baseImageMAC = "aa:bb:cc:dd:ee:ff"
	freshMAC     = "02:5a:00:00:00:01"
)

func writeVMConfig(t *testing.T, tartHome, name, guestOS, mac string) {
	t.Helper()
	directory := filepath.Join(tartHome, "vms", name)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(map[string]string{"os": guestOS, "macAddress": mac})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "config.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func (f *fixture) writeLedgerEntry(resourceID, leaseID string) {
	f.t.Helper()
	if err := f.backend.writeLedger(resourceID, leaseID); err != nil {
		f.t.Fatal(err)
	}
}

func (f *fixture) ledgerHas(resourceID string) bool {
	f.t.Helper()
	owned, err := f.backend.readLedger()
	if err != nil {
		f.t.Fatalf("ledger unreadable: %v", err)
	}
	_, ok := owned[resourceID]
	return ok
}

func writeArtifact(t *testing.T, path string, content []byte) {
	t.Helper()
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}
}

func digestOf(content []byte) string {
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:])
}

func newLoopbackTLS(t *testing.T, hostname string) ([]byte, tls.Certificate) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "runner-pool test CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: hostname},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames: []string{hostname},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
		tls.Certificate{Certificate: [][]byte{leafDER}, PrivateKey: leafKey}
}

func mustUUID(t *testing.T) string {
	t.Helper()
	return uuid.NewString()
}

func mustDifferentToken(t *testing.T, current bootstrap.Token) bootstrap.Token {
	t.Helper()
	for range 8 {
		other, err := bootstrap.NewToken()
		if err != nil {
			t.Fatal(err)
		}
		if other.Encoded() != current.Encoded() {
			return other
		}
	}
	t.Fatal("could not mint a distinct bootstrap token")
	return bootstrap.Token{}
}

// errBrokenStage is the generic "this external command failed" a test uses when
// which command failed is the point and how it failed is not.
var errBrokenStage = errors.New("external command failed")

// readGuestLauncherSource reads the guest launcher's source so the adapter can
// pin the handshake word both sides must agree on.
func readGuestLauncherSource() (string, error) {
	data, err := os.ReadFile("../../../cmd/runner-pool-guest-launch/main.go")
	if err != nil {
		return "", err
	}
	return string(data), nil
}

package kubernetes

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type witnessProviderFixture struct {
	processes WitnessScope
	storage   WitnessScope
	capacity  CapacitySnapshot
	err       error
}

func (f *witnessProviderFixture) ListProcesses(context.Context, string, string) (WitnessScope, error) {
	return f.processes, f.err
}

func (f *witnessProviderFixture) ListWritableStorage(context.Context, string, string) (WitnessScope, error) {
	return f.storage, f.err
}

func (f *witnessProviderFixture) ObserveCapacity(context.Context, string) (CapacitySnapshot, error) {
	return f.capacity, f.err
}

func newWitnessFixture(t *testing.T, provider HostWitnessProvider) *HostWitnessClient {
	t.Helper()
	peerIdentity := func(net.Conn) (uint32, error) { return uint32(os.Geteuid()), nil }
	server := &HostWitnessServer{SocketPath: "/run/sjo-runner-pool/witness.sock", SocketGID: os.Getegid(), ExpectedClientUID: uint32(os.Geteuid()),
		Namespace: "github-runners", OwnerUID: "controller-a", Timeout: time.Second, Provider: provider, peerUID: peerIdentity}
	return &HostWitnessClient{SocketPath: server.SocketPath, ExpectedServerUID: uint32(os.Geteuid()), Timeout: time.Second,
		peerUID: peerIdentity, dialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			clientConnection, serverConnection := net.Pipe()
			go func() {
				_ = server.handle(ctx, serverConnection)
				_ = serverConnection.Close()
			}()
			return clientConnection, nil
		}}
}

func TestHostWitnessTransportAuthenticatesAndScopesAllSources(t *testing.T) {
	resource := WorkerResource{Namespace: "github-runners", Name: "slipway-linux-11111111-1111-4111-8111-111111111111", UID: "pod-uid",
		RuntimeClassName: "sjo-kata-qemu", Labels: map[string]string{LabelOwnerUID: "controller-a"}}
	provider := &witnessProviderFixture{
		processes: WitnessScope{Complete: true, Items: []PhysicalWitness{physicalWitness(resource)}},
		storage:   WitnessScope{Complete: true, Items: []PhysicalWitness{physicalWitness(resource)}},
		capacity:  physicalCapacity(),
	}
	client := newWitnessFixture(t, provider)
	processes, err := client.ListProcesses(context.Background(), "github-runners", "controller-a")
	if err != nil || !processes.Complete || len(processes.Items) != 1 {
		t.Fatalf("process witness failed: %+v %v", processes, err)
	}
	storage, err := client.ListWritableStorage(context.Background(), "github-runners", "controller-a")
	if err != nil || !storage.Complete || len(storage.Items) != 1 {
		t.Fatalf("storage witness failed: %+v %v", storage, err)
	}
	capacity, err := client.ObserveCapacity(context.Background(), "github-runners")
	if err != nil || !capacity.Complete || capacity.CPUMilliTotal == 0 {
		t.Fatalf("capacity witness failed: %+v %v", capacity, err)
	}
	if _, err := client.ListProcesses(context.Background(), "other", "controller-a"); err == nil {
		t.Fatal("host witness accepted a foreign namespace")
	}
	if _, err := client.ListProcesses(context.Background(), "github-runners", "controller-b"); err == nil {
		t.Fatal("host witness accepted a foreign owner")
	}
}

func TestHostWitnessFailsClosedForCollectorAndPeerFailures(t *testing.T) {
	provider := &witnessProviderFixture{processes: WitnessScope{Complete: false}, storage: WitnessScope{Complete: false}, capacity: CapacitySnapshot{Complete: false}}
	client := newWitnessFixture(t, provider)
	if _, err := client.ListProcesses(context.Background(), "github-runners", "controller-a"); err == nil {
		t.Fatal("incomplete process witness was accepted")
	}
	client.peerUID = func(net.Conn) (uint32, error) { return 99999, nil }
	if _, err := client.ObserveCapacity(context.Background(), "github-runners"); err == nil {
		t.Fatal("unexpected witness peer uid was accepted")
	}
}

func TestHostWitnessDoesNotReplaceExistingSocketPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "witness.sock")
	if err := os.WriteFile(path, []byte("foreign"), 0o600); err != nil {
		t.Fatal(err)
	}
	server := &HostWitnessServer{SocketPath: path, SocketGID: os.Getegid(), ExpectedClientUID: uint32(os.Geteuid()),
		Namespace: "github-runners", OwnerUID: "controller-a", Timeout: time.Second, Provider: &witnessProviderFixture{}}
	if err := server.Serve(context.Background()); err == nil {
		t.Fatal("server replaced an existing socket path")
	}
	contents, err := os.ReadFile(path)
	if err != nil || string(contents) != "foreign" {
		t.Fatalf("existing path changed: %q %v", contents, err)
	}
}

func TestHostWitnessRejectsUnavailablePeerCredentials(t *testing.T) {
	client := newWitnessFixture(t, &witnessProviderFixture{})
	client.peerUID = func(net.Conn) (uint32, error) { return 0, errors.New("peer credentials unavailable") }
	if _, err := client.ObserveCapacity(context.Background(), "github-runners"); err == nil {
		t.Fatal("missing peer credentials were accepted")
	}
}

// shortSocketPath keeps the path under the 108-byte sun_path limit, which a
// t.TempDir under a long test name can exceed.
func shortSocketPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "hw")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "w.sock")
}

func witnessServerAt(path string, provider HostWitnessProvider) *HostWitnessServer {
	return &HostWitnessServer{SocketPath: path, SocketGID: os.Getegid(), ExpectedClientUID: uint32(os.Geteuid()),
		Namespace: "github-runners", OwnerUID: "controller-a", Timeout: time.Second, Provider: provider}
}

// homelab#1098: a socket left by a killed witness no longer crash-loops the
// next one. Nothing listens on it, the dial is refused, and it is replaced.
func TestHostWitnessReplacesADeadSocketLeftByAKilledWitness(t *testing.T) {
	path := shortSocketPath(t)
	dead, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	dead.SetUnlinkOnClose(false)
	_ = dead.Close()
	if _, err := os.Lstat(path); err != nil {
		t.Fatalf("fixture: the dead socket file is gone: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- witnessServerAt(path, &witnessProviderFixture{}).Serve(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		connection, err := net.Dial("unix", path)
		if err == nil {
			_ = connection.Close()
			break
		}
		select {
		case err := <-served:
			t.Fatalf("the witness refused to start over a dead socket: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("the witness never listened on the replaced socket: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	if err := <-served; err != nil {
		t.Fatalf("the witness did not stop cleanly: %v", err)
	}
}

// A socket a live process still serves is never taken over.
func TestHostWitnessRefusesASocketALiveProcessServes(t *testing.T) {
	path := shortSocketPath(t)
	live, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer live.Close()
	go func() {
		for {
			connection, err := live.Accept()
			if err != nil {
				return
			}
			_ = connection.Close()
		}
	}()
	err = witnessServerAt(path, &witnessProviderFixture{}).Serve(context.Background())
	if err == nil || !strings.Contains(err.Error(), "live process") {
		t.Fatalf("a second witness started over a live socket: %v", err)
	}
	if _, err := net.Dial("unix", path); err != nil {
		t.Fatalf("the live socket was disturbed: %v", err)
	}
}

type hangingProvider struct{ seen chan error }

func (p *hangingProvider) ListProcesses(ctx context.Context, _, _ string) (WitnessScope, error) {
	<-ctx.Done()
	p.seen <- ctx.Err()
	return WitnessScope{}, ctx.Err()
}
func (p *hangingProvider) ListWritableStorage(ctx context.Context, _, _ string) (WitnessScope, error) {
	return p.ListProcesses(ctx, "", "")
}
func (p *hangingProvider) ObserveCapacity(ctx context.Context, _ string) (CapacitySnapshot, error) {
	_, err := p.ListProcesses(ctx, "", "")
	return CapacitySnapshot{}, err
}

// homelab#1098: a hung collector is cancelled at the request's deadline, so it
// cannot hold the one-at-a-time loop for every later inventory.
func TestAHungCollectorIsCancelledAtTheRequestDeadline(t *testing.T) {
	provider := &hangingProvider{seen: make(chan error, 1)}
	server := witnessServerAt("/unused", provider)
	server.Timeout = 100 * time.Millisecond
	server.peerUID = func(net.Conn) (uint32, error) { return uint32(os.Geteuid()), nil }
	client, connection := net.Pipe()
	defer client.Close()
	done := make(chan struct{})
	go func() {
		_ = server.handle(context.Background(), connection)
		close(done)
	}()
	go func() {
		_, _ = client.Write([]byte(`{"protocol":"` + hostWitnessProtocol + `","operation":"processes","namespace":"github-runners","owner_uid":"controller-a"}` + "\n"))
		_, _ = io.Copy(io.Discard, client)
	}()
	select {
	case err := <-provider.seen:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("the collector was stopped by %v, not the request deadline", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a hung collector ran past the request deadline under the server's context")
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the request never returned after its collector was cancelled")
	}
}

package tart

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/cameronsjo/slipway/internal/controller"
)

// C2. `tart exec -i <vm> <launcher>` does not return until the JOB ends,
// because the launcher execs the native runner in place. If Create waited on
// it, the sequential daemon cycle would stall for the length of the job -- no
// inventory, no cleanup, no deadline handling -- and any job over ten minutes
// would be quarantined while running correctly.
//
// The fake models exactly that: a delivery child that handshakes and then never
// exits. Create must return anyway.
func TestCreateReturnsWhileTheDeliveryChildIsStillRunning(t *testing.T) {
	fix := newFixture(t, nil)
	done := make(chan error, 1)
	go func() {
		_, err := fix.backend.Create(context.Background(), fix.intent())
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("create failed: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Create blocked on the delivery child, which does not return until the job ends")
	}
	// The child is still running, and the adapter still owns it.
	fix.runner.mu.Lock()
	deliveries := append([]*fakeProcess{}, fix.runner.deliveries...)
	fix.runner.mu.Unlock()
	if len(deliveries) != 1 {
		t.Fatalf("expected exactly one delivery child, saw %d", len(deliveries))
	}
	select {
	case <-deliveries[0].Done():
		t.Fatal("the delivery child exited; this test is no longer modelling a running job")
	default:
	}
	fix.backend.mu.Lock()
	owned := len(fix.backend.children[testResourceID])
	fix.backend.mu.Unlock()
	if owned != 2 {
		t.Fatalf("the adapter owns %d children for the VM, expected the run child and the delivery child", owned)
	}
}

// Destroy reaps both children, not just the `tart run` one. A delivery child
// left behind holds an open `tart exec` against a VM that is being deleted.
func TestDestroyReapsBothOwnedChildren(t *testing.T) {
	fix := newFixture(t, nil)
	if _, err := fix.backend.Create(context.Background(), fix.intent()); err != nil {
		t.Fatalf("create failed: %v", err)
	}
	if _, err := fix.backend.Destroy(context.Background(),
		controller.WorkerObservation{LeaseID: testLeaseID, ResourceID: testResourceID}); err != nil {
		t.Fatalf("destroy failed: %v", err)
	}
	fix.runner.mu.Lock()
	children := append([]*fakeProcess{}, fix.runner.children...)
	fix.runner.mu.Unlock()
	if len(children) != 2 {
		t.Fatalf("expected two owned children, saw %d", len(children))
	}
	for index, child := range children {
		if !child.wasStopped() {
			t.Fatalf("owned child %d survived Destroy", index)
		}
	}
}

// C2, the memory half. The delivery child's stdout is a whole CI job's output.
// The adapter must never accumulate it: a run that buffered it would grow the
// controller by the size of the job log.
func TestDeliveryOutputIsNeverAccumulated(t *testing.T) {
	// The production writer is what has to hold this property, so drive it
	// directly with 64 MiB rather than the fake.
	found := make(chan string, 1)
	writer := &handshakeWriter{found: found}
	if _, err := writer.Write([]byte(handshakeLine + "\n")); err != nil {
		t.Fatal(err)
	}
	if line := <-found; line != handshakeLine {
		t.Fatalf("the handshake line was not read back: %q", line)
	}
	chunk := make([]byte, 1<<20)
	for i := range chunk {
		chunk[i] = 'x'
	}
	for range 64 {
		if _, err := writer.Write(chunk); err != nil {
			t.Fatal(err)
		}
	}
	writer.mu.Lock()
	retained := len(writer.seen)
	writer.mu.Unlock()
	if retained != 0 {
		t.Fatalf("the writer retained %d bytes of job output", retained)
	}
}

// A child that floods stdout with no newline must not be buffered either.
func TestHandshakeWriterCapsALinelessFlood(t *testing.T) {
	found := make(chan string, 1)
	writer := &handshakeWriter{found: found}
	chunk := make([]byte, 1<<20)
	for i := range chunk {
		chunk[i] = 'x'
	}
	for range 8 {
		if _, err := writer.Write(chunk); err != nil {
			t.Fatal(err)
		}
	}
	writer.mu.Lock()
	retained := len(writer.seen)
	writer.mu.Unlock()
	if retained > maxHandshakeBytes {
		t.Fatalf("the writer retained %d bytes, above the %d-byte cap", retained, maxHandshakeBytes)
	}
	select {
	case line := <-found:
		t.Fatalf("a flood with no newline produced a handshake: %q", line)
	default:
	}
}

// The adapter and the guest launcher must agree on the word. They are separate
// packages on purpose -- the launcher is pushed into an untrusted guest and
// depends on nothing but internal/bootstrap -- so the constant is duplicated
// and pinned here.
func TestHandshakeLineMatchesTheGuestLauncher(t *testing.T) {
	source, err := readGuestLauncherSource()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(source, `const handshakeLine = "`+handshakeLine+`"`) {
		t.Fatalf("cmd/runner-pool-guest-launch does not declare handshakeLine as %q", handshakeLine)
	}
	if !strings.Contains(source, "Fprintln(os.Stdout, handshakeLine)") {
		t.Fatal("cmd/runner-pool-guest-launch does not print the handshake to stdout")
	}
}

// I2. A refusal once the VM is running must stop it now, not leave it up until
// some later cycle notices. The ledger entry stays either way, so the slot is
// still charged and Inventory still reports it.
func TestEveryPostRunRefusalStopsTheVM(t *testing.T) {
	cases := map[string]func(*fixture){
		"guest address outside the subnet": func(f *fixture) { f.runner.set("tart ip", "192.168.1.9\n", nil) },
		"guest address is the gateway":     func(f *fixture) { f.runner.set("tart ip", "192.168.2.1\n", nil) },
		"staging push fails": func(f *fixture) {
			f.runner.set("tart exec -i "+testResourceID+" /usr/bin/tar", "", errBrokenStage)
		},
		"launcher push fails": func(f *fixture) {
			f.runner.set("tart exec -i "+testResourceID+" /bin/dd", "", errBrokenStage)
		},
		"JIT fetch refused":    func(f *fixture) { f.token = mustDifferentToken(f.t, f.token) },
		"no launch handshake":  func(f *fixture) { f.runner.streamBlocks = true },
		"wrong handshake line": func(f *fixture) { f.runner.streamLine = "READY" },
	}
	for name, breakIt := range cases {
		t.Run(name, func(t *testing.T) {
			fix := newFixture(t, func(c *Config) { c.LaunchHandshakeTimeout = 50 * time.Millisecond })
			breakIt(fix)
			intent := fix.intent()
			if name == "JIT fetch refused" {
				intent.BootstrapToken = fix.token
			}
			if _, err := fix.backend.Create(context.Background(), intent); err == nil {
				t.Fatal("create succeeded on a path this test expects to fail")
			}
			if !fix.runner.ran("tart stop " + testResourceID) {
				t.Fatal("a refusal after the VM was running left it running")
			}
			fix.runner.mu.Lock()
			children := append([]*fakeProcess{}, fix.runner.children...)
			fix.runner.mu.Unlock()
			if len(children) == 0 {
				t.Fatal("no owned child was started, so this test proved nothing")
			}
			for index, child := range children {
				if !child.wasStopped() {
					t.Fatalf("owned child %d survived the refusal", index)
				}
			}
			// Still charged: the VM may not be gone, and only Destroy may say so.
			if !fix.ledgerHas(testResourceID) {
				t.Fatal("a post-run refusal released the slot")
			}
		})
	}
}

// Positive control: a healthy create stops nothing.
func TestAHealthyCreateStopsNothing(t *testing.T) {
	fix := newFixture(t, nil)
	if _, err := fix.backend.Create(context.Background(), fix.intent()); err != nil {
		t.Fatalf("create failed: %v", err)
	}
	if fix.runner.ran("tart stop") {
		t.Fatal("a healthy create stopped its own VM")
	}
}

// A `tart run` child that dies must fail Create promptly, not at the ten-minute
// startup deadline.
func TestAnEarlyTartRunDeathFailsCreatePromptly(t *testing.T) {
	fix := newFixture(t, nil)
	fix.runner.set("tart exec "+testResourceID+" /usr/bin/true", "", errBrokenStage)
	intent := fix.intent()
	intent.StartupDeadline = time.Now().Add(time.Hour)
	go func() {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			fix.runner.mu.Lock()
			children := append([]*fakeProcess{}, fix.runner.children...)
			fix.runner.mu.Unlock()
			if len(children) > 0 {
				_ = children[0].Stop()
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	done := make(chan error, 1)
	go func() {
		_, err := fix.backend.Create(context.Background(), intent)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("create succeeded with a dead tart run child")
		}
	case <-time.After(20 * time.Second):
		t.Fatal("create waited out the startup deadline instead of noticing the dead child")
	}
}

// The launcher's stderr was discarded by design, which made the first live
// failure undiagnosable without a hand reproduction. Surface the first line on
// a handshake refusal -- bounded, printable ASCII only.
func TestAHandshakeRefusalSurfacesTheLauncherFirstStderrLine(t *testing.T) {
	fix := newFixture(t, func(c *Config) { c.LaunchHandshakeTimeout = 50 * time.Millisecond })
	fix.runner.streamBlocks = true
	fix.runner.streamStderr = "guest account password rotation failed at passwd, dscl exit 10"

	if _, err := fix.backend.Create(context.Background(), fix.intent()); err == nil {
		t.Fatal("create succeeded with no handshake")
	}
	logged := fix.logs.String()
	if !strings.Contains(logged, "dscl exit 10") {
		t.Fatalf("the launcher's stderr did not reach the refusal log: %s", logged)
	}
	if !strings.Contains(logged, "launcher_stderr") {
		t.Fatal("the refusal log has no launcher_stderr field")
	}
}

// Bounded and sanitized: a control byte could rewrite a log line, and the
// stream is not trusted just because the launcher is well behaved.
func TestTheLauncherDiagnosticIsBoundedAndPrintable(t *testing.T) {
	writer := &diagnosticWriter{}
	if _, err := writer.Write([]byte("rotation failed\x07\x1b[31m at passwd\nSECOND LINE\n")); err != nil {
		t.Fatal(err)
	}
	line := writer.firstLine()
	if strings.Contains(line, "SECOND LINE") {
		t.Fatalf("more than the first line was kept: %q", line)
	}
	for index := 0; index < len(line); index++ {
		if line[index] < 0x20 || line[index] > 0x7e {
			t.Fatalf("a non-printable byte survived sanitization: %q", line)
		}
	}
	if line != "rotation failed[31m at passwd" {
		t.Fatalf("unexpected sanitized line: %q", line)
	}

	flood := &diagnosticWriter{}
	if _, err := flood.Write(bytes.Repeat([]byte("x"), 10_000)); err != nil {
		t.Fatal(err)
	}
	if got := len(flood.firstLine()); got > maxDiagnosticBytes {
		t.Fatalf("the diagnostic kept %d bytes, above the %d cap", got, maxDiagnosticBytes)
	}
}

// And no secret may ride out on that path.
func TestTheLauncherDiagnosticCannotCarryTheJITOrCapability(t *testing.T) {
	fix := newFixture(t, func(c *Config) { c.LaunchHandshakeTimeout = 50 * time.Millisecond })
	fix.runner.streamBlocks = true
	// A misbehaving launcher that echoes what it was fed.
	fix.runner.streamStderr = string(testJIT) + " " + fix.token.Encoded()
	if _, err := fix.backend.Create(context.Background(), fix.intent()); err == nil {
		t.Fatal("create succeeded with no handshake")
	}
	// The cap alone is not the control -- assert the secrets specifically.
	assertLogsAreClean(t, fix, fix.token.Encoded(), string(testJIT))
}

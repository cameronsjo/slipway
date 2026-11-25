// Package tart contains the macOS Tart worker adapter.
//
// Every external program runs through Runner as an explicit argv slice with an
// explicit environment: no shell, no string interpolation, no inherited
// environment. The pool's isolation gate reads pf state through the
// argument-exact NOPASSWD entries that already exist on the host
// (hosts/m1max/pf/sudoers.d-tart-pf-assert); it mutates nothing and never
// invokes tart-pf-assert.sh, tart-ci-run or tart-ci-reprovision.sh, whose
// failure paths act on the legacy ci-guest by name.
package tart

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// handshakeLine is what the guest launcher prints to stdout after it has
// validated everything and immediately before it execs the native runner. It is
// the adapter's only evidence that delivery succeeded, because the exec call
// itself does not return until the job ends.
const handshakeLine = "SJO_LAUNCH_OK"

// maxHandshakeBytes bounds what the adapter will read looking for that line. A
// child that writes more without a newline is refused rather than buffered.
const maxHandshakeBytes = 256

// childWaitDelay is what makes a context deadline real.
//
// exec.CommandContext kills the process when the context ends, but Output()
// does not return until the stdout PIPE reaches EOF -- and a grandchild that
// inherited that pipe keeps it open after its parent dies. Measured: a 1-second
// deadline returned after 20 seconds. WaitDelay is the only thing that closes
// the descriptors and gives up, so without it the gate read timeout is a
// suggestion. It also bounds Stop() on the long-lived children.
const childWaitDelay = 3 * time.Second

// Runner executes external programs. Argv[0] is the absolute program path and
// env is the child's complete environment; neither is ever inherited.
//
// Run collects standard output and is only for short, bounded commands. Start
// launches an owned long-lived child. StartStream launches an owned child,
// writes stdin once, closes it, and discards the child's output except for a
// single bounded handshake line -- it must never accumulate a child's output,
// because the one caller runs a whole CI job.
type Runner interface {
	Run(ctx context.Context, argv []string, env []string, stdin []byte) ([]byte, error)
	Start(ctx context.Context, argv []string, env []string) (Process, error)
	StartStream(ctx context.Context, argv []string, env []string, stdin []byte) (StreamProcess, error)
}

// Process is a long-lived owned child. Stop asks it to exit; Done closes once
// it has, so a caller can notice an early death instead of waiting out a
// ten-minute deadline.
type Process interface {
	Stop() error
	Done() <-chan struct{}
}

// StreamProcess adds the one bounded read the delivery handshake needs.
type StreamProcess interface {
	Process
	// Handshake returns the child's first stdout line, waiting no longer than
	// ctx allows. It returns an error if the child exits first or writes
	// maxHandshakeBytes without a newline.
	Handshake(ctx context.Context) (string, error)
	// Diagnostic returns the child's first stderr line, sanitized and bounded.
	// It exists only so a handshake refusal can say WHY: the first live failure
	// took a hand reproduction to diagnose because this was discarded.
	Diagnostic() string
}

// ExecRunner is the production Runner. It is the only place in this package
// that reaches os/exec.
type ExecRunner struct{}

func (ExecRunner) Run(ctx context.Context, argv []string, env []string, stdin []byte) ([]byte, error) {
	if ctx == nil || len(argv) == 0 || argv[0] == "" {
		return nil, errors.New("external command requires a context and an absolute program path")
	}
	command := exec.CommandContext(ctx, argv[0], argv[1:]...)
	command.WaitDelay = childWaitDelay
	command.Env = append([]string{}, env...)
	if len(stdin) > 0 {
		command.Stdin = bytes.NewReader(stdin)
	}
	out, err := command.Output()
	if err != nil {
		// Standard error is deliberately dropped rather than folded into the
		// error: a failing child would otherwise be able to echo whatever it
		// was fed back into a controller log.
		return out, errors.New("external command failed: " + argv[0])
	}
	return out, nil
}

func (ExecRunner) Start(ctx context.Context, argv []string, env []string) (Process, error) {
	command, child, err := startChild(argv, env)
	if err != nil {
		return nil, err
	}
	// A real file, not io.Discard. io.Discard makes os/exec create a PIPE and
	// copy from it, so when the controller exits the orphaned `tart run` takes
	// SIGPIPE on its next write and dies with the VM. /dev/null is inherited as
	// a plain descriptor and outlives this process.
	devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		return nil, errors.New("cannot open the null device for an owned child")
	}
	command.Stdout, command.Stderr = devNull, devNull
	if err := startAndReap(command, child); err != nil {
		_ = devNull.Close()
		return nil, err
	}
	// The child holds its own duplicate of the descriptor.
	_ = devNull.Close()
	return child, nil
}

func (ExecRunner) StartStream(ctx context.Context, argv []string, env []string, stdin []byte) (StreamProcess, error) {
	command, child, err := startChild(argv, env)
	if err != nil {
		return nil, err
	}
	child.handshake = make(chan string, 1)
	// A capped scanner, not a buffer. Everything past the first line, and
	// everything past maxHandshakeBytes, goes nowhere: this child's stdout is a
	// whole CI job's output.
	command.Stdout = &handshakeWriter{found: child.handshake}
	// The launcher's stderr messages are fixed strings we wrote, so the first
	// line is safe to surface. It is still bounded and sanitized here rather
	// than trusted, because "safe by construction" is an invariant of the
	// launcher, not of whatever else might end up on that stream.
	child.diagnostic = &diagnosticWriter{}
	command.Stderr = child.diagnostic
	if len(stdin) > 0 {
		pipe, err := command.StdinPipe()
		if err != nil {
			return nil, errors.New("owned child stdin could not be opened: " + argv[0])
		}
		if err := startAndReap(command, child); err != nil {
			_ = pipe.Close()
			return nil, err
		}
		// Write and close before waiting for the handshake. The child cannot
		// finish validating until stdin reaches EOF.
		_, writeErr := pipe.Write(stdin)
		closeErr := pipe.Close()
		if writeErr != nil || closeErr != nil {
			_ = child.Stop()
			return nil, errors.New("owned child stdin could not be written: " + argv[0])
		}
		return child, nil
	}
	return child, startAndReap(command, child)
}

func startChild(argv []string, env []string) (*exec.Cmd, *execProcess, error) {
	if len(argv) == 0 || argv[0] == "" {
		return nil, nil, errors.New("owned child requires an absolute program path")
	}
	// Deliberately not CommandContext: this child outlives the call that starts
	// it, and its lifetime is owned by Stop and by Destroy.
	command := exec.Command(argv[0], argv[1:]...)
	command.WaitDelay = childWaitDelay
	command.Env = append([]string{}, env...)
	return command, &execProcess{command: command, done: make(chan struct{})}, nil
}

func startAndReap(command *exec.Cmd, child *execProcess) error {
	if err := command.Start(); err != nil {
		return errors.New("owned child failed to start: " + command.Path)
	}
	go func() {
		_ = command.Wait()
		close(child.done)
	}()
	return nil
}

type execProcess struct {
	command    *exec.Cmd
	done       chan struct{}
	handshake  chan string
	diagnostic *diagnosticWriter
	stopOnce   sync.Once
	stopErr    error
}

func (p *execProcess) Done() <-chan struct{} { return p.done }

func (p *execProcess) Stop() error {
	p.stopOnce.Do(func() {
		if p.command.Process != nil {
			p.stopErr = p.command.Process.Kill()
		}
	})
	return p.stopErr
}

func (p *execProcess) Handshake(ctx context.Context) (string, error) {
	if p.handshake == nil {
		return "", errors.New("this child was not started with a handshake")
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

func (p *execProcess) Diagnostic() string {
	if p.diagnostic == nil {
		return ""
	}
	return p.diagnostic.firstLine()
}

// maxDiagnosticBytes bounds the stderr line a refusal may quote.
const maxDiagnosticBytes = 200

// diagnosticWriter keeps the first stderr line, bounded, and discards the rest.
type diagnosticWriter struct {
	mu   sync.Mutex
	seen []byte
	done bool
}

func (w *diagnosticWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, b := range p {
		if w.done {
			break
		}
		if b == '\n' || len(w.seen) >= maxDiagnosticBytes {
			w.done = true
			break
		}
		w.seen = append(w.seen, b)
	}
	return len(p), nil
}

func (w *diagnosticWriter) firstLine() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return sanitizeDiagnostic(string(w.seen))
}

// sanitizeDiagnostic keeps printable ASCII only. Control bytes could rewrite a
// log line, and anything non-ASCII is not something the launcher emits.
func sanitizeDiagnostic(text string) string {
	kept := make([]byte, 0, len(text))
	for index := 0; index < len(text) && len(kept) < maxDiagnosticBytes; index++ {
		if text[index] >= 0x20 && text[index] <= 0x7e {
			kept = append(kept, text[index])
		}
	}
	return strings.TrimSpace(string(kept))
}

// handshakeWriter keeps at most maxHandshakeBytes, emits the first complete
// line once, and discards everything else for the life of the child.
type handshakeWriter struct {
	mu    sync.Mutex
	seen  []byte
	done  bool
	found chan<- string
}

func (w *handshakeWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.done {
		return len(p), nil
	}
	for index, b := range p {
		if b == '\n' {
			line := string(w.seen)
			w.seen, w.done = nil, true
			select {
			case w.found <- line:
			default:
			}
			_ = index
			return len(p), nil
		}
		if len(w.seen) < maxHandshakeBytes {
			w.seen = append(w.seen, b)
			continue
		}
		// Too much output with no newline. Stop scanning and discard the rest
		// rather than growing; Handshake will time out or see the child exit.
		w.seen, w.done = nil, true
		return len(p), nil
	}
	return len(p), nil
}

package kubernetes

import (
	"errors"
	"fmt"
	"log/slog"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Why this file exists.
//
// The pool fenced every cycle on `trusted backend inventory failed; platform
// admission fenced` and NOTHING said why. The controller discarded the cause,
// the witness wrote nothing at all, and both images are `scratch` -- no shell,
// no exec, and the admission policy correctly forbids a debug container. Three
// blind rollouts cost about forty minutes each. What is added here is the
// smallest set of facts that localises such a failure from the two pod logs
// alone: which collector, which tool, its exit status, and the first line of
// its stderr.

// Witness reason codes. CLOSED VOCABULARY: these cross the socket to the
// controller, so they are constants rather than free text. The witness's own
// error strings stay on the witness side, where the log can afford them.
const (
	WitnessReasonProcesses    = "processes"
	WitnessReasonStorage      = "storage"
	WitnessReasonCapacity     = "capacity"
	WitnessReasonFingerprints = "fingerprints"
	WitnessReasonScope        = "scope-rejected"
	WitnessReasonOperation    = "unsupported-operation"
)

// maxStderrBytes bounds the captured first line of a failed tool's stderr. A
// fenced pool retries every cycle, so an unbounded copy is a pod log filling at
// the retry rate; 200 bytes holds every real `ctr`, `crictl` and `lvs` refusal
// seen so far with room to spare.
const maxStderrBytes = 200

// maxToolNameBytes bounds the tool name carried in a reason code. It is the
// base name of an operator-configured absolute path, not attacker input, but it
// crosses the socket, so it is bounded and allowlisted like everything else
// that does.
const maxToolNameBytes = 32

// suppressionWindow is how long identical consecutive failures are collapsed
// into one summary. The pool reconciles on every lifecycle tick, so a steady
// fence would otherwise write the same line thousands of times an hour and bury
// the change that matters -- the moment the message becomes a DIFFERENT one.
const suppressionWindow = time.Minute

// hostCommandFailure carries what a failed host command actually tells you.
// Before this, runHostCommand used command.Output() and threw stderr away, so
// `ctr: flag provided but not defined: -quiet` -- the exact sentence naming the
// defect -- was discarded at the point it was produced. Output() already
// populates exec.ExitError.Stderr; nothing was captured because nothing read it.
type hostCommandFailure struct {
	Tool       string
	ExitStatus int
	Stderr     string
	cause      error
}

func (f *hostCommandFailure) Error() string {
	return fmt.Sprintf("trusted host command %s failed", f.Tool)
}

func (f *hostCommandFailure) Unwrap() error { return f.cause }

// newHostCommandFailure extracts the tool, exit status and bounded stderr.
// Exit status is -1 when the process never ran or was signalled, which is a
// different fault from a non-zero exit and must not be reported as exit 0.
func newHostCommandFailure(tool string, err error) *hostCommandFailure {
	failure := &hostCommandFailure{Tool: safeToolName(tool), ExitStatus: -1, cause: err}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		failure.ExitStatus = exitErr.ExitCode()
		failure.Stderr = boundedFirstLine(exitErr.Stderr)
	}
	return failure
}

// boundedFirstLine takes the FIRST line only, keeps printable ASCII only, and
// truncates to maxStderrBytes.
//
// Each of those three is load-bearing and none is stylistic. First line only:
// a tool that dumps its whole usage text, or a Go panic with a stack, would
// otherwise reach the log in full. Printable ASCII only: a control byte in a
// log line can rewrite a terminal, and a NUL truncates downstream readers.
// Bounded length: the retry loop is the amplifier.
//
// The first line is also where a tool puts its refusal and NOT where a tool
// puts a secret -- a credential arrives in an environment dump or a request
// echo, which is later output. That is the leak canary's subject.
func boundedFirstLine(stderr []byte) string {
	text := string(stderr)
	if index := strings.IndexAny(text, "\r\n"); index >= 0 {
		text = text[:index]
	}
	var builder strings.Builder
	for _, character := range []byte(text) {
		if builder.Len() >= maxStderrBytes {
			break
		}
		if character >= 0x20 && character < 0x7f {
			builder.WriteByte(character)
		}
	}
	return strings.TrimSpace(builder.String())
}

// safeToolName allowlists the characters a tool base name may contribute to a
// reason code, and REFUSES rather than strips. An allowlist rather than a
// denylist: the set of characters safe in a log line and a JSON field is small
// and known, while the set that is not keeps growing.
//
// Refusing matters more than it looks. Stripping turns `../../etc/passwd` into
// `....etcpasswd` -- a plausible-looking tool name that names nothing, which is
// a worse thing to print in a diagnostic than no name at all. An empty return
// drops the tool from the reason code and leaves the collector, which is true.
//
// Length is the one exception: over-long is truncated, because a real base name
// longer than the bound is a configuration oddity rather than a malformed one.
func safeToolName(tool string) string {
	if tool == "" {
		return ""
	}
	for _, character := range []byte(tool) {
		isLetter := (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z')
		isDigit := character >= '0' && character <= '9'
		if !isLetter && !isDigit && character != '.' && character != '_' && character != '-' {
			return ""
		}
	}
	if len(tool) > maxToolNameBytes {
		return tool[:maxToolNameBytes]
	}
	return tool
}

// safeContainerID bounds a containerd identity for a log line. It is its own
// function rather than safeToolName because that one truncates at 32 bytes --
// right for a tool base name, wrong for a 64-hex container id, where half an id
// is a worse thing to print than none. Hex only, exact length, or nothing.
func safeContainerID(id string) string {
	if len(id) != 64 {
		return ""
	}
	for _, character := range []byte(id) {
		isDigit := character >= '0' && character <= '9'
		isHex := character >= 'a' && character <= 'f'
		if !isDigit && !isHex {
			return ""
		}
	}
	return id
}

// fingerprintFailure marks a refusal that came from the runtime fingerprint
// check rather than from reading storage. The two are reported separately
// because they send an operator to different places: a fingerprint refusal is
// about the host's files or the measuring timer, a storage refusal is about
// containerd.
type fingerprintFailure struct{ cause error }

func (f *fingerprintFailure) Error() string { return f.cause.Error() }
func (f *fingerprintFailure) Unwrap() error { return f.cause }

// WitnessLogger writes one structured line per FAILED witness request to
// stderr, and nothing on success beyond a single startup line.
//
// What it must never carry: file contents, snapshot keys, pod names from other
// namespaces, and the environment. None of those is needed to localise a
// failure, and each is a standing invitation to leak. The fields below are the
// whole set.
type WitnessLogger struct {
	logger  *slog.Logger
	limiter *repeatLimiter
	// A SEPARATE limiter for skips. Sharing one would make a pass that skips
	// and then fails reset the other's window every time, so both would log on
	// every tick -- suppression that reads as configured and does nothing.
	skips *repeatLimiter
	// And a third for unpack notices, for the same reason. A pull runs for
	// minutes across many passes while failures come and go around it; one
	// shared limiter would let each reset the other's window.
	unpacks *repeatLimiter
}

func NewWitnessLogger(logger *slog.Logger, now func() time.Time) *WitnessLogger {
	if logger == nil {
		return nil
	}
	return &WitnessLogger{logger: logger, limiter: newRepeatLimiter(suppressionWindow, now),
		skips: newRepeatLimiter(suppressionWindow, now), unpacks: newRepeatLimiter(suppressionWindow, now)}
}

// Failure logs one failed collector request. collector is a reason-code
// constant; err is inspected for a host command failure and for a fingerprint
// refusal, and contributes nothing else beyond its own fixed string.
func (w *WitnessLogger) Failure(collector string, err error) {
	if w == nil || err == nil {
		return
	}
	attributes := []any{slog.String("collector", collector), slog.String("error", failureText(err))}
	key := collector + "\x00" + failureText(err)
	var command *hostCommandFailure
	if errors.As(err, &command) {
		attributes = append(attributes,
			slog.String("tool", command.Tool),
			slog.Int("exit_status", command.ExitStatus),
			slog.String("stderr", command.Stderr))
		key = collector + "\x00" + command.Tool + "\x00" + command.Stderr
	}
	decision := w.limiter.observe(key)
	if decision.suppressedSoFar > 0 {
		w.logger.Warn("host witness request failed, repeating",
			slog.String("collector", collector), slog.Int("repeats", decision.suppressedSoFar))
		return
	}
	if !decision.emit {
		return
	}
	w.logger.Warn("host witness request failed", attributes...)
}

// Startup writes the one INFO line the witness emits when nothing is wrong. It
// says which collectors are configured and whether every runtime fingerprint
// matched, so a witness that came up against a changed host is visible before
// the first request rather than after the first fence.
func (w *WitnessLogger) Startup(collectors []string, fingerprintCount int, fingerprintErr error) {
	if w == nil {
		return
	}
	if fingerprintErr != nil {
		w.logger.Warn("host witness started with a runtime fingerprint that does not match",
			slog.String("collectors", strings.Join(collectors, ",")),
			slog.Int("fingerprints", fingerprintCount),
			slog.String("error", failureText(fingerprintErr)))
		return
	}
	w.logger.Info("host witness ready",
		slog.String("collectors", strings.Join(collectors, ",")),
		slog.Int("fingerprints", fingerprintCount),
		slog.Bool("fingerprints_matched", true))
}

// failureText bounds an error's own message the same way a tool's stderr is
// bounded. These strings are fixed literals in this package today, but a future
// wrap could carry something longer or stranger, and the bound costs nothing.
func failureText(err error) string {
	return boundedFirstLine([]byte(err.Error()))
}

type limiterDecision struct {
	emit            bool
	suppressedSoFar int
}

// repeatLimiter collapses identical CONSECUTIVE messages: the first is logged,
// the rest are counted, and one summary is emitted per window. A different
// message resets immediately, so the transition an operator is waiting for --
// the error changing -- is never delayed.
type repeatLimiter struct {
	mu          sync.Mutex
	window      time.Duration
	now         func() time.Time
	key         string
	suppressed  int
	windowStart time.Time
}

func newRepeatLimiter(window time.Duration, now func() time.Time) *repeatLimiter {
	if now == nil {
		now = time.Now
	}
	return &repeatLimiter{window: window, now: now}
}

func (r *repeatLimiter) observe(key string) limiterDecision {
	r.mu.Lock()
	defer r.mu.Unlock()
	moment := r.now()
	if key != r.key {
		r.key = key
		r.suppressed = 0
		r.windowStart = moment
		return limiterDecision{emit: true}
	}
	r.suppressed++
	if moment.Sub(r.windowStart) < r.window {
		return limiterDecision{}
	}
	count := r.suppressed
	r.suppressed = 0
	r.windowStart = moment
	return limiterDecision{suppressedSoFar: count}
}

// Skipped records a tolerated mid-pass disappearance. It is a WARN rather than
// an INFO because a skip is the witness declining to inspect something it was
// asked to: ordinary on this node, never routine.
//
// It carries the containerd id, how many skips this pass has taken, and the
// bound it is heading for. Nothing else: no name, no namespace, no labels, and
// no error text, because the skip is not an error and there is nothing to
// quote. Rate-limited on the COLLECTOR AND COUNT rather than the id, so a churn
// storm collapses into one line plus a per-minute summary instead of one line
// per vanished container.
// Unpacking records how many in-flight image extractions the dedicated
// snapshotter is holding. Zero logs nothing; that is the ordinary state and a
// line per pass would drown the one that matters.
//
// It exists because a pull is the one time this collector deliberately DOES NOT
// account for storage it can see. Without a line, a pool that is briefly not
// certifying anything and a pool that is quietly ignoring real storage look the
// same from the log. WARN rather than INFO: nothing is wrong, but an operator
// reading a fence around this moment needs it in front of them.
//
// COUNT ONLY, never a key. An unpack key carries an image's chain ID, which
// names what is being pulled -- and the whole key is attacker-adjacent, since
// any workload that can create a snapshot here names its own.
func (w *WitnessLogger) Unpacking(collector string, count int) {
	if w == nil || count <= 0 {
		return
	}
	if w.unpacks.observe(collector + "\x00" + strconv.Itoa(count)).emit {
		w.logger.Warn("dedicated snapshotter is holding image unpack snapshots; a pull is in flight",
			slog.String("collector", collector),
			slog.Int("unpacking", count))
	}
}

// Pruned records a durable worker identity dropped from the ledger because all
// three legs -- Pod sandbox, runtime process, writable snapshot -- observed it
// absent in one complete pass. It is INFO rather than WARN: unlike a skip, this
// is the witness finishing something, and a pool that never logged it would
// leave an operator with a shrinking ledger and no account of who shrank it.
//
// It carries the sandbox id and the count for this pass. NOT the snapshot keys,
// the worker name, or the lease: the id is enough to follow the entry back
// through the earlier lines that named it.
func (w *WitnessLogger) Pruned(collector, sandboxID string, count int) {
	if w == nil {
		return
	}
	w.logger.Info("host witness pruned a worker identity that is gone on every leg",
		slog.String("collector", collector),
		slog.String("sandbox", safeContainerID(sandboxID)),
		slog.Int("pruned", count))
}

// IdentityReread records that a storage pass found writable storage naming a
// sandbox the ledger did not hold, and is re-reading the CRI Pod list once
// before deciding (homelab#1213). WARN: usually a worker created mid-pass, but
// the same line precedes a real refusal, and an operator reading that refusal
// needs to see the re-read happened. Collector only: the refusal that may
// follow does not name the sandbox either, and this line carries no more than
// it. Not rate-limited; it fires at most once per request.
func (w *WitnessLogger) IdentityReread(collector string) {
	if w == nil {
		return
	}
	w.logger.Warn("host witness found storage for an unrecorded worker; re-reading the Pod list once before deciding",
		slog.String("collector", collector))
}

func (w *WitnessLogger) Skipped(collector, containerID string, count, bound int) {
	if w == nil {
		return
	}
	if w.skips.observe(collector).emit {
		w.logger.Warn("host witness skipped a container that vanished mid-inventory",
			slog.String("collector", collector),
			slog.String("container", safeContainerID(containerID)),
			slog.Int("skipped", count),
			slog.Int("bound", bound))
	}
}

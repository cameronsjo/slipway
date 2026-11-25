package kubernetes

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// TestHelperProcess is the real-ExitError source. exec.Cmd.Output() populates
// exec.ExitError.Stderr, and the whole point of the change under test is that
// the code now reads it. A hand-built ExitError would prove the extraction
// compiles and nothing about whether Output() actually fills that field on this
// platform, which is the fact the fix rests on.
func TestHelperProcess(t *testing.T) {
	if os.Getenv("SJO_WANT_HELPER_PROCESS") != "1" {
		t.Skip("helper process; run only as a subprocess")
	}
	fmt.Fprintln(os.Stderr, os.Getenv("SJO_HELPER_STDERR"))
	fmt.Fprintln(os.Stderr, os.Getenv("SJO_HELPER_STDERR_LATER"))
	os.Exit(7)
}

func failingHostCommand(t *testing.T, firstLine, laterLines string) error {
	t.Helper()
	command := exec.Command(os.Args[0], "-test.run=TestHelperProcess")
	command.Env = append(os.Environ(),
		"SJO_WANT_HELPER_PROCESS=1",
		"SJO_HELPER_STDERR="+firstLine,
		"SJO_HELPER_STDERR_LATER="+laterLines)
	_, err := command.Output()
	if err == nil {
		t.Fatal("the helper process was expected to fail")
	}
	return newHostCommandFailure("ctr", err)
}

func TestHostCommandFailureCarriesToolExitStatusAndFirstStderrLine(t *testing.T) {
	// The line that would have ended the ctr investigation in one read.
	err := failingHostCommand(t, "ctr: flag provided but not defined: -quiet", "usage: ctr snapshots list")
	var failure *hostCommandFailure
	if !errors.As(err, &failure) {
		t.Fatalf("the failure does not carry host command detail: %v", err)
	}
	if failure.Tool != "ctr" {
		t.Fatalf("tool is %q", failure.Tool)
	}
	if failure.ExitStatus != 7 {
		t.Fatalf("exit status is %d, want the helper's 7", failure.ExitStatus)
	}
	if failure.Stderr != "ctr: flag provided but not defined: -quiet" {
		t.Fatalf("stderr first line is %q", failure.Stderr)
	}
}

// THE LEAK CANARY. A credential does not arrive on a tool's first line -- it
// arrives in an environment dump or a request echo, which is LATER output. So
// the bound that matters is "first line only", and this is what asserts it.
func TestATokenBeyondTheFirstStderrLineNeverReachesTheLog(t *testing.T) {
	const planted = "ghp_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	t.Setenv("SJO_CANARY_TOKEN", planted)

	err := failingHostCommand(t, "lvs: File locking initialisation failed", "GITHUB_TOKEN="+planted)
	var buffer bytes.Buffer
	logger := NewWitnessLogger(slog.New(slog.NewTextHandler(&buffer, nil)), time.Now)
	logger.Failure(WitnessReasonCapacity, err)

	written := buffer.String()
	if written == "" {
		t.Fatal("nothing was logged, so this canary reached nothing")
	}
	if strings.Contains(written, planted) {
		t.Fatalf("a token from a later stderr line reached the log: %q", written)
	}
	// The environment is never a log source either, and the token is in it.
	if strings.Contains(written, "GITHUB_TOKEN") || strings.Contains(written, "SJO_CANARY_TOKEN") {
		t.Fatalf("an environment variable name reached the log: %q", written)
	}
	// Positive control: without this, a logger that wrote nothing useful would
	// pass every assertion above.
	if !strings.Contains(written, "File locking initialisation failed") {
		t.Fatalf("the first stderr line did not reach the log: %q", written)
	}
}

func TestBoundedFirstLineDropsControlBytesAndTruncates(t *testing.T) {
	// A control byte in a log line can rewrite a terminal, and a NUL truncates
	// downstream readers, so neither may survive.
	cleaned := boundedFirstLine([]byte("lvs:\x1b[31m red\x00 text"))
	if strings.ContainsAny(cleaned, "\x00\x1b") {
		t.Fatalf("control bytes survived: %q", cleaned)
	}
	if cleaned != "lvs:[31m red text" {
		t.Fatalf("unexpected cleaning: %q", cleaned)
	}
	long := boundedFirstLine([]byte(strings.Repeat("x", maxStderrBytes*3)))
	if len(long) != maxStderrBytes {
		t.Fatalf("stderr was not truncated to %d bytes: %d", maxStderrBytes, len(long))
	}
	if boundedFirstLine([]byte("first\nsecond")) != "first" {
		t.Fatal("lines past the first survived")
	}
}

func TestRepeatLimiterLogsOnceThenSummarizesPerWindow(t *testing.T) {
	moment := time.Unix(1_700_000_000, 0)
	limiter := newRepeatLimiter(time.Minute, func() time.Time { return moment })

	if decision := limiter.observe("a"); !decision.emit {
		t.Fatal("the first message was suppressed")
	}
	for range 5 {
		if decision := limiter.observe("a"); decision.emit || decision.suppressedSoFar != 0 {
			t.Fatalf("a repeat inside the window was not suppressed: %+v", decision)
		}
	}
	moment = moment.Add(time.Minute)
	decision := limiter.observe("a")
	if decision.emit || decision.suppressedSoFar != 6 {
		t.Fatalf("the window summary did not count the suppressed repeats: %+v", decision)
	}
	// A DIFFERENT message resets immediately. The transition an operator waits
	// for is the error changing, and delaying that would be the worst possible
	// place to save a log line.
	if decision := limiter.observe("b"); !decision.emit {
		t.Fatal("a changed message was suppressed behind the previous one's window")
	}
}

func TestWitnessLoggerStartupLineNamesCollectorsAndFingerprints(t *testing.T) {
	var buffer bytes.Buffer
	logger := NewWitnessLogger(slog.New(slog.NewTextHandler(&buffer, nil)), time.Now)
	logger.Startup([]string{"processes", "storage", "capacity"}, 5, nil)
	written := buffer.String()
	for _, expected := range []string{"processes,storage,capacity", "fingerprints=5", "fingerprints_matched=true"} {
		if !strings.Contains(written, expected) {
			t.Fatalf("startup line lacks %q: %q", expected, written)
		}
	}
	buffer.Reset()
	logger.Startup([]string{"processes"}, 5, errors.New("dedicated runtime fingerprint changed"))
	if !strings.Contains(buffer.String(), "does not match") {
		t.Fatalf("a fingerprint mismatch was not reported at startup: %q", buffer.String())
	}
}

// --- the reason code that crosses the socket --------------------------------

func TestWitnessReasonCodeNamesTheCollectorAndTool(t *testing.T) {
	err := failingHostCommand(t, "lvs: File locking initialisation failed", "")
	if code := witnessReasonCode(WitnessReasonCapacity, err); code != "capacity/ctr" {
		t.Fatalf("reason code is %q", code)
	}
	fingerprint := &fingerprintFailure{cause: errors.New("dedicated runtime fingerprint changed")}
	if code := witnessReasonCode(WitnessReasonStorage, fingerprint); code != WitnessReasonFingerprints {
		t.Fatalf("a fingerprint refusal was reported as %q", code)
	}
	if code := witnessReasonCode(WitnessReasonStorage, errors.New("plain")); code != WitnessReasonStorage {
		t.Fatalf("a plain storage refusal was reported as %q", code)
	}
	if witnessReasonCode(WitnessReasonStorage, nil) != "" {
		t.Fatal("a success carried a reason code")
	}
}

func TestReceivedReasonCodesAreHeldToTheirGrammar(t *testing.T) {
	// The reason arrives over a socket. The server is peer-authenticated, so
	// this is not what keeps a hostile value out -- it is what keeps a
	// malformed one from reaching an error string unexamined.
	for received, want := range map[string]string{
		"capacity":     "capacity",
		"capacity/lvs": "capacity/lvs",
		// Refused, not stripped: "....etcpasswd" would be a plausible-looking
		// tool name that names nothing, which is worse to print than no name.
		"capacity/../../etc/passwd":                          "capacity",
		"capacity/lvs\x00rm -rf":                             "capacity",
		"not-a-collector":                                    "unrecognized",
		"fingerprints":                                       WitnessReasonFingerprints,
		strings.Repeat("storage/x", 40):                      "storage",
		"storage/" + strings.Repeat("x", maxToolNameBytes*2): "storage/" + strings.Repeat("x", maxToolNameBytes),
	} {
		if got := safeWitnessReason(received); got != want {
			t.Fatalf("safeWitnessReason(%q) = %q, want %q", received, got, want)
		}
	}
}

// --- the collector tag on a failed witness request --------------------------

type failingCapacitySource struct{ err error }

func (f *failingCapacitySource) ObserveCapacity(context.Context, string) (CapacitySnapshot, error) {
	return CapacitySnapshot{Complete: false}, f.err
}

func TestWitnessLogsTheCollectorToolAndExitStatusForAFailedCommand(t *testing.T) {
	err := failingHostCommand(t, "lvs: File locking initialisation failed", "")
	var buffer bytes.Buffer
	sources := HostWitnessSources{
		Processes: &fakeProcessSource{},
		Storage:   &fakeStorageSource{},
		Capacity:  &failingCapacitySource{err: err},
		Log:       NewWitnessLogger(slog.New(slog.NewTextHandler(&buffer, nil)), time.Now),
	}
	if _, observeErr := sources.ObserveCapacity(context.Background(), "github-runners"); observeErr == nil {
		t.Fatal("the failing capacity collector reported success")
	}
	written := buffer.String()
	for _, expected := range []string{
		`collector=capacity`,
		`tool=ctr`,
		`exit_status=7`,
		"File locking initialisation failed",
	} {
		if !strings.Contains(written, expected) {
			t.Fatalf("witness log lacks %q: %q", expected, written)
		}
	}
}

func TestWitnessWritesNothingOnASuccessfulRequest(t *testing.T) {
	// A fenced pool retries every cycle. A logger that also spoke on success
	// would bury the failures in its own noise.
	var buffer bytes.Buffer
	sources := HostWitnessSources{
		Processes: &fakeProcessSource{},
		Storage:   &fakeStorageSource{},
		Capacity:  &fakeCapacitySource{snapshot: CapacitySnapshot{Complete: true}},
		Log:       NewWitnessLogger(slog.New(slog.NewTextHandler(&buffer, nil)), time.Now),
	}
	if _, err := sources.ObserveCapacity(context.Background(), "github-runners"); err != nil {
		t.Fatal(err)
	}
	if buffer.Len() != 0 {
		t.Fatalf("a successful request was logged: %q", buffer.String())
	}
}

// --- the tolerated skip is no longer silent ---------------------------------
//
// #847 landed the vanished-container tolerance with no log line at all
// (security review, Important 2). A pool quietly dropping containers from its
// inventory looks exactly like a pool with fewer containers, and the only
// difference an operator could see was a fence that stopped happening.

func TestATeleratedSkipIsLoggedWithItsCountAndBound(t *testing.T) {
	collector, _, infos, snapshots := storageCollectorFixture(t)
	var buffer bytes.Buffer
	collector.SetLogger(NewWitnessLogger(slog.New(slog.NewTextHandler(&buffer, nil)), time.Now))
	collector.run, _ = storageRunnerWithVanishingContainer(collector, infos, snapshots,
		map[string]bool{testContainerA: true},
		func() ([]byte, error) {
			return []byte(snapshotTable(activeSnapshot(testSandboxA), committedSnapshot(testImageLayer))), nil
		})

	if _, err := collector.ListWritableStorage(context.Background(), "github-runners", "controller-a"); err != nil {
		t.Fatalf("the pass failed: %v", err)
	}
	written := buffer.String()
	for _, expected := range []string{
		"skipped a container that vanished",
		"collector=storage",
		"container=" + testContainerA,
		"skipped=1",
		fmt.Sprintf("bound=%d", maxVanishedContainers),
	} {
		if !strings.Contains(written, expected) {
			t.Fatalf("skip log lacks %q: %q", expected, written)
		}
	}
}

func TestAPassWithNoSkipsLogsNothing(t *testing.T) {
	// The positive control. Without it, a logger that wrote on every pass
	// would satisfy the test above.
	collector, _, _, _ := storageCollectorFixture(t)
	var buffer bytes.Buffer
	collector.SetLogger(NewWitnessLogger(slog.New(slog.NewTextHandler(&buffer, nil)), time.Now))
	if _, err := collector.ListWritableStorage(context.Background(), "github-runners", "controller-a"); err != nil {
		t.Fatal(err)
	}
	if buffer.Len() != 0 {
		t.Fatalf("a clean pass logged: %q", buffer.String())
	}
}

func TestSkipsAndFailuresDoNotResetEachOthersSuppression(t *testing.T) {
	// They use separate limiters. Sharing one would make a pass that skips and
	// then fails reset the other's window every time, so both would log on
	// every tick -- suppression that reads as configured and does nothing.
	moment := time.Unix(1_700_000_000, 0)
	var buffer bytes.Buffer
	logger := NewWitnessLogger(slog.New(slog.NewTextHandler(&buffer, nil)), func() time.Time { return moment })
	id := strings.Repeat("a", 64)

	logger.Skipped(WitnessReasonStorage, id, 1, maxVanishedContainers)
	logger.Failure(WitnessReasonStorage, errors.New("storage refused"))
	buffer.Reset()
	// Both repeat inside the window; neither may speak again.
	logger.Skipped(WitnessReasonStorage, id, 2, maxVanishedContainers)
	logger.Failure(WitnessReasonStorage, errors.New("storage refused"))
	if buffer.Len() != 0 {
		t.Fatalf("a repeat inside the window was logged: %q", buffer.String())
	}
}

func TestOnlyAWellFormedContainerIDReachesTheLog(t *testing.T) {
	// Half an id is a worse thing to print than none, so the bound is exact
	// length plus hex, not a truncation.
	if safeContainerID(strings.Repeat("a", 64)) != strings.Repeat("a", 64) {
		t.Fatal("a valid containerd id was rejected")
	}
	for _, bad := range []string{"", "short", strings.Repeat("a", 63), strings.Repeat("a", 65),
		strings.Repeat("g", 64), strings.Repeat("A", 64), "../../etc/passwd"} {
		if got := safeContainerID(bad); got != "" {
			t.Fatalf("safeContainerID(%q) = %q, want empty", bad, got)
		}
	}
}

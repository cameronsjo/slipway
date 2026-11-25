package tart

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// I-C. exec.CommandContext kills the process when the context ends, but
// Output() does not return until the stdout PIPE reaches EOF -- and a
// grandchild that inherited that pipe holds it open after its parent dies.
// Measured before the fix: a 1-second deadline returned after 20 seconds, so
// the gate read timeout was a suggestion and a wedged pfctl still blocked the
// re-assert loop.
//
// This runs the REAL ExecRunner against a helper process that forks exactly
// that shape. The fake-based TestAWedgedPfReadIsARefusalNotAHang stays as the
// shape test -- it proves the gate treats a timeout as a refusal, and it cannot
// prove anything about os/exec, because a fake never touches a pipe.
func TestExecRunnerReturnsWithinTheDeadlineDespiteAPipeHoldingGrandchild(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns a real process that lingers for 60s in the background")
	}
	const deadline = time.Second
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()

	start := time.Now()
	_, err := ExecRunner{}.Run(ctx, helperArgv("pipe-holder"), helperEnv(), nil)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("a command killed by its context deadline reported success")
	}
	// Deadline plus WaitDelay plus scheduling slack. Without WaitDelay this
	// waits for the sleeper, which is 60 seconds.
	budget := deadline + childWaitDelay + 5*time.Second
	if elapsed > budget {
		t.Fatalf("Run took %s, past the %s budget: the context deadline is not enforced", elapsed, budget)
	}
	t.Logf("Run returned in %s (deadline %s + WaitDelay %s)", elapsed, deadline, childWaitDelay)
}

// Positive control: the same runner returns a fast command's output normally,
// so the test above is not passing because Run is broken for everything.
func TestExecRunnerReturnsOutputForAWellBehavedCommand(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out, err := ExecRunner{}.Run(ctx, helperArgv("echo"), helperEnv(), nil)
	if err != nil {
		t.Fatalf("a well-behaved command failed: %v", err)
	}
	if strings.TrimSpace(string(out)) != "hello" {
		t.Fatalf("unexpected output: %q", out)
	}
}

// And stdin reaches the child, which is what the JIT delivery path depends on.
func TestExecRunnerWritesStdinToTheChild(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out, err := ExecRunner{}.Run(ctx, helperArgv("cat"), helperEnv(), []byte("payload\n"))
	if err != nil {
		t.Fatalf("stdin command failed: %v", err)
	}
	if strings.TrimSpace(string(out)) != "payload" {
		t.Fatalf("stdin did not reach the child: %q", out)
	}
}

// helperArgv re-execs this test binary in the standard TestHelperProcess shape.
func helperArgv(mode string) []string {
	return []string{os.Args[0], "-test.run=TestHelperProcess", "--", mode}
}

func helperEnv() []string {
	return []string{"SJO_TART_HELPER=1", "PATH=/usr/bin:/bin"}
}

// TestHelperProcess is not a real test. It is the child half of the three tests
// above, selected by SJO_TART_HELPER so a normal run skips it immediately.
func TestHelperProcess(t *testing.T) {
	if os.Getenv("SJO_TART_HELPER") != "1" {
		return
	}
	args := os.Args
	for index, argument := range args {
		if argument == "--" {
			args = args[index+1:]
			break
		}
	}
	if len(args) == 0 {
		os.Exit(2)
	}
	switch args[0] {
	case "echo":
		os.Stdout.WriteString("hello\n")
	case "cat":
		buffer := make([]byte, 4096)
		n, _ := os.Stdin.Read(buffer)
		os.Stdout.Write(buffer[:n])
	case "pipe-holder":
		// Fork a grandchild that inherits stdout and outlives us, then block.
		// This is the shape a wedged pfctl takes: the process the context kills
		// is not the one holding the pipe open.
		sleeper := exec.Command("/bin/sleep", "60")
		sleeper.Stdout, sleeper.Stderr = os.Stdout, os.Stderr
		if err := sleeper.Start(); err != nil {
			os.Exit(3)
		}
		select {}
	default:
		os.Exit(2)
	}
	os.Exit(0)
}

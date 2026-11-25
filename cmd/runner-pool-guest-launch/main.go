// runner-pool-guest-launch runs inside a fresh Tart guest. It is the worker-side
// half of the Tart bootstrap: the host fetches the JIT and writes it to this
// process's stdin over `tart exec -i`, because the pf anchor blocks every
// guest-to-host packet and the HTTPS bootstrap route is unreachable from a Mac
// guest.
//
// What it keeps from runner-pool-worker, unchanged and by calling the same
// functions: the refusal of a pre-configured runner tree, the ten-name
// environment allowlist, and the euid-0 refusal. What it adds: rotating the
// guest account password to a value nothing stores. What it never holds: the
// bootstrap capability, which stays on the host.
package main

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"syscall"

	"github.com/cameronsjo/slipway/internal/bootstrap"
)

var guestUserRE = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,30}$`)

// handshakeLine must equal internal/backend/tart's constant of the same name.
// It is duplicated rather than imported so this binary, which is pushed into an
// untrusted guest, depends on nothing but internal/bootstrap. A test in the
// adapter package pins the two together.
const handshakeLine = "SJO_LAUNCH_OK"

func run() error {
	root := flag.String("runner-root", "", "absolute native runner installation directory")
	user := flag.String("guest-user", "", "guest account whose password is rotated before the runner starts")
	dsclPath := flag.String("dscl", "/usr/bin/dscl", "absolute path to dscl")
	// The stock Cirrus image's published default password for its admin
	// account. This is a PUBLIC value -- it is in the image's own
	// documentation, which is exactly why it has to be rotated -- not a secret,
	// so carrying it as a flag default is not a credential in the repo.
	oldPassword := flag.String("guest-old-password", "admin", "the stock image's published password for -guest-user")
	flag.Parse()
	if flag.NArg() != 0 {
		return errors.New("guest launcher takes no positional arguments")
	}
	payload, err := io.ReadAll(io.LimitReader(os.Stdin, bootstrap.MaxJITBytes+1))
	if err != nil {
		return errors.New("cannot read the JIT payload from stdin")
	}
	defer clear(payload)
	executable, args, env, err := launchPlan(*root, *user, payload, os.Environ())
	if err != nil {
		clear(payload)
		return err
	}
	// Rotate the stock image's published account password to a value that is
	// never stored, printed, or written to a file.
	if err := rotatePassword(*dsclPath, *user, *oldPassword); err != nil {
		clear(payload)
		return err
	}
	if err := os.Chdir(*root); err != nil {
		clear(payload)
		return errors.New("cannot enter runner installation")
	}
	// The handshake. `tart exec -i` does not return until the job ends, so the
	// host has no other way to tell a successful launch from a hung one. It is
	// printed only here: after every validation has passed, after the password
	// rotation, and immediately before the exec that replaces this process.
	// It carries no job state and no credential -- it is one fixed word.
	if _, err := fmt.Fprintln(os.Stdout, handshakeLine); err != nil {
		clear(payload)
		return errors.New("cannot signal a successful launch to the host")
	}
	clear(payload)
	if err := syscall.Exec(executable, args, env); err != nil {
		return errors.New("native runner execution failed; do not retry JIT delivery")
	}
	return nil
}

// launchPlan is everything decided before any side effect: the euid refusal,
// the argument shape, the JIT shape, and then bootstrap.ValidateRunnerRoot and
// bootstrap.RunnerCommand called UNCHANGED. Those two are what carry the
// refusal of a pre-configured runner tree and the ten-name environment
// allowlist from the Linux worker to this one.
func launchPlan(root, user string, payload []byte, environ []string) (string, []string, []string, error) {
	// The listener must not run as root. Same refusal runner-pool-worker makes,
	// checked before anything the payload could influence.
	if os.Geteuid() == 0 {
		return "", nil, nil, errors.New("guest launcher must run as the unprivileged job account, not root")
	}
	if root == "" || !guestUserRE.MatchString(user) {
		return "", nil, nil, errors.New("guest launcher requires a runner root and a plain guest account name")
	}
	if !bootstrap.ValidJIT(payload) {
		return "", nil, nil, errors.New("stdin did not carry a valid JIT payload")
	}
	if _, err := bootstrap.ValidateRunnerRoot(root); err != nil {
		return "", nil, nil, err
	}
	return bootstrap.RunnerCommand(root, payload, environ)
}

// rotatePassword changes the guest account's password, then proves it changed.
//
// NO SUDO. Measured on the real Cirrus macOS 26.4 guest: `sudo -n dscl .
// -passwd /Users/admin` with the new password on stdin, and with it on argv,
// both fail `eDSAuthFailed` rc 10 -- even as root, macOS demands the account's
// OLD password. What works is running as the account itself with both
// passwords: `dscl . -passwd /Users/<user> <old> <new>`. `tart exec` already
// runs as that user, so this needs no privilege at all.
//
// Both passwords go on dscl's argv. That is deliberate and bounded: this runs
// inside a fresh single-tenant guest before any job code exists, and the
// process has exited before the runner listener starts, so there is no other
// principal to read `ps`. The old password is public anyway; the new one is
// discarded here and never leaves this process.
func rotatePassword(dsclPath, user, oldPassword string) error {
	secret, err := randomPassword()
	if err != nil {
		return err
	}
	defer clear(secret)
	newPassword := strings.TrimSuffix(string(secret), "\n")

	if code, err := runDscl(dsclPath, ".", "-passwd", "/Users/"+user, oldPassword, newPassword); err != nil {
		return fmt.Errorf("guest account password rotation failed at passwd, dscl exit %d", code)
	}
	// Prove it. A dscl that exits 0 without changing anything would otherwise
	// leave the published password live while the launch reported success.
	if code, err := runDscl(dsclPath, ".", "-authonly", user, newPassword); err != nil {
		return fmt.Errorf("guest account password rotation failed at authonly-new, dscl exit %d", code)
	}
	if _, err := runDscl(dsclPath, ".", "-authonly", user, oldPassword); err == nil {
		return errors.New("guest account password rotation failed at authonly-old, the published password still authenticates")
	}
	return nil
}

// runDscl runs one dscl command with no environment and no output captured,
// and returns its exit code so a failure can be named without echoing anything
// dscl printed -- its diagnostics quote the account and the attempt.
func runDscl(dsclPath string, args ...string) (int, error) {
	command := exec.Command(dsclPath, args...)
	command.Env = []string{}
	command.Stdout = io.Discard
	command.Stderr = io.Discard
	err := command.Run()
	if err == nil {
		return 0, nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode(), err
	}
	return -1, err
}

func randomPassword() ([]byte, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return nil, errors.New("cannot generate a guest account password")
	}
	defer clear(raw)
	encoded := make([]byte, base64.RawURLEncoding.EncodedLen(len(raw))+1)
	base64.RawURLEncoding.Encode(encoded, raw)
	encoded[len(encoded)-1] = '\n'
	return encoded, nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

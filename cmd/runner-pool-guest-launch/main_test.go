package main

import (
	"bytes"
	"encoding/base64"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

var validJIT = []byte(base64.StdEncoding.EncodeToString([]byte("a jit payload for the guest")))

// freshRunnerRoot is the tree ValidateRunnerRoot accepts: a real directory, a
// real bin/, an executable Runner.Listener, and none of the five prior
// registration files.
func freshRunnerRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "bin", "Runner.Listener"), []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	return root
}

func minimalEnvironment() []string {
	return []string{"HOME=/Users/admin", "PATH=/usr/bin:/bin"}
}

func TestLaunchPlanAcceptsAFreshTreeAndPassesTheJIT(t *testing.T) {
	root := freshRunnerRoot(t)
	executable, args, env, err := launchPlan(root, "admin", validJIT, minimalEnvironment())
	if err != nil {
		t.Fatalf("a fresh runner tree was refused: %v", err)
	}
	if executable != filepath.Join(root, "bin", "Runner.Listener") {
		t.Fatalf("the launcher chose the wrong entrypoint: %q", executable)
	}
	if len(args) != 2 || args[1] != "run" {
		t.Fatalf("the launcher built the wrong argv: %v", args)
	}
	if !slices.Contains(env, "ACTIONS_RUNNER_INPUT_JITCONFIG="+string(validJIT)) {
		t.Fatal("the JIT did not reach the runner environment")
	}
	// The JIT goes in the environment, never on the command line, where every
	// other process in the guest could read it from ps.
	for _, argument := range args {
		if strings.Contains(argument, string(validJIT)) {
			t.Fatalf("the JIT appeared on the runner command line: %v", args)
		}
	}
}

// A runner root carrying prior registration state is refused. The native
// listener loads .env before CommandSettings and can overwrite the JIT, so a
// reused tree is a credential hazard, not a tidiness problem.
func TestLaunchPlanRefusesAPreConfiguredRunnerRoot(t *testing.T) {
	for _, name := range []string{".env", ".runner", ".credentials", ".credentials_rsaparams", ".service"} {
		t.Run(name, func(t *testing.T) {
			root := freshRunnerRoot(t)
			if err := os.WriteFile(filepath.Join(root, name), []byte("x"), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, _, _, err := launchPlan(root, "admin", validJIT, minimalEnvironment()); err == nil {
				t.Fatalf("a runner root containing %s was accepted", name)
			}
		})
	}
}

// Only the ten allowlisted names cross into the runner. Everything else the
// guest happens to have in its environment is dropped.
func TestLaunchPlanPassesOnlyTheAllowlistedEnvironment(t *testing.T) {
	root := freshRunnerRoot(t)
	environment := append(minimalEnvironment(),
		"SLIPWAY_BOOTSTRAP_TOKEN=should-never-appear",
		"AWS_SECRET_ACCESS_KEY=should-never-appear",
		"GITHUB_TOKEN=should-never-appear",
		"TMPDIR=/tmp", "LANG=en_US.UTF-8")
	_, _, env, err := launchPlan(root, "admin", validJIT, environment)
	if err != nil {
		t.Fatalf("launch plan failed: %v", err)
	}
	for _, variable := range env {
		name, _, _ := strings.Cut(variable, "=")
		switch name {
		case "HOME", "USER", "LOGNAME", "PATH", "TMPDIR", "LANG", "LC_ALL", "TZ", "TERM",
			"DEVELOPER_DIR", "ACTIONS_RUNNER_INPUT_JITCONFIG":
		default:
			t.Fatalf("a name outside the allowlist reached the runner: %q", name)
		}
		if strings.Contains(variable, "should-never-appear") {
			t.Fatalf("an unallowlisted value reached the runner: %q", variable)
		}
	}
	// Positive control: the allowlist did let the allowed extras through, so
	// this test is not passing because the environment came out empty.
	if !slices.Contains(env, "TMPDIR=/tmp") || !slices.Contains(env, "LANG=en_US.UTF-8") {
		t.Fatalf("the allowlist dropped names it should pass: %v", env)
	}
}

func TestLaunchPlanRefusesBadArgumentsAndBadPayloads(t *testing.T) {
	root := freshRunnerRoot(t)
	cases := map[string]struct {
		root    string
		user    string
		payload []byte
	}{
		"no runner root":                        {"", "admin", validJIT},
		"empty guest user":                      {root, "", validJIT},
		"guest user with a shell metacharacter": {root, "admin; id", validJIT},
		"guest user with a slash":               {root, "../root", validJIT},
		"empty payload":                         {root, "admin", nil},
		"payload with a newline":                {root, "admin", []byte("abcd\nefgh")},
		"payload that is not base64":            {root, "admin", []byte("!!!!")},
		"relative runner root":                  {"runner", "admin", validJIT},
	}
	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			if _, _, _, err := launchPlan(testCase.root, testCase.user, testCase.payload, minimalEnvironment()); err == nil {
				t.Fatal("the launcher accepted input it should refuse")
			}
		})
	}
}

// The euid refusal cannot be exercised here without running the suite as root,
// which is itself the thing this binary refuses. Declared skip: the assertion
// below pins that the check reads the real euid rather than a constant, and M4
// exercises the real path on the host.
func TestLaunchPlanEuidCheckReadsTheRealProcessIdentity(t *testing.T) {
	if os.Geteuid() == 0 {
		root := freshRunnerRoot(t)
		if _, _, _, err := launchPlan(root, "admin", validJIT, minimalEnvironment()); err == nil {
			t.Fatal("the launcher accepted a root listener")
		}
		return
	}
	root := freshRunnerRoot(t)
	if _, _, _, err := launchPlan(root, "admin", validJIT, minimalEnvironment()); err != nil {
		t.Fatalf("a non-root listener was refused: %v", err)
	}
	t.Log("running as non-root: the euid-0 refusal is exercised on the host in M4, not here")
}

// The rotated password is random, never returned to a caller that stores it,
// and is handed to dscl on stdin rather than in argv, where ps would show it.
func TestRandomPasswordIsFreshEveryTime(t *testing.T) {
	first, err := randomPassword()
	if err != nil {
		t.Fatal(err)
	}
	second, err := randomPassword()
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(first, second) {
		t.Fatal("two rotations produced the same password")
	}
	if len(first) < 32 {
		t.Fatalf("the rotated password is only %d bytes", len(first))
	}
	if !bytes.HasSuffix(first, []byte("\n")) {
		t.Fatal("the password is not newline-terminated for dscl's stdin read")
	}
	if bytes.ContainsAny(first[:len(first)-1], " \t\r\n") {
		t.Fatal("the password contains whitespace dscl would treat as a terminator")
	}
}

// The live failure: `sudo -n dscl . -passwd /Users/admin` fails eDSAuthFailed
// even as root, because macOS demands the account's OLD password. The rotation
// must therefore run as the account itself, with both passwords, and no sudo.
func TestRotationUsesNoSudoAndSuppliesBothPasswords(t *testing.T) {
	source, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	if strings.Contains(text, "sudoPath") || strings.Contains(text, `"/usr/bin/sudo"`) {
		t.Fatal("the launcher still reaches for sudo; measured on the real guest, sudo cannot change the password")
	}
	for _, required := range []string{
		`"-passwd", "/Users/"+user, oldPassword, newPassword`,
		`"-authonly", user, newPassword`,
		`"-authonly", user, oldPassword`,
	} {
		if !strings.Contains(strings.ReplaceAll(text, " ", ""), strings.ReplaceAll(required, " ", "")) {
			t.Fatalf("the rotation does not perform %s", required)
		}
	}
}

// Verification is the point: a dscl that exits 0 without changing anything
// would leave the image's published password live while the launch reported
// success. Both directions must be asserted.
func TestRotationVerifiesBothDirections(t *testing.T) {
	source, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	if !strings.Contains(text, "authonly-new") || !strings.Contains(text, "authonly-old") {
		t.Fatal("the rotation does not name both verification steps in its failures")
	}
	if !strings.Contains(text, "the published password still authenticates") {
		t.Fatal("the rotation does not refuse when the old password still works")
	}
}

// The adapter surfaces the launcher's FIRST stderr line in a refusal log. That
// is only safe because every message this binary writes before the handshake is
// a fixed string we wrote. Pin that: no format verb may interpolate anything
// but an exit code.
func TestLauncherStderrCarriesNoVariableDataBeforeTheHandshake(t *testing.T) {
	source, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	verbs := regexp.MustCompile(`%[#+\-0-9.]*[a-zA-Z]`)
	for index, line := range strings.Split(string(source), "\n") {
		if !strings.Contains(line, "fmt.Errorf(") && !strings.Contains(line, "fmt.Fprintf(") {
			continue
		}
		for _, verb := range verbs.FindAllString(line, -1) {
			// %d is the dscl exit code: a small integer we chose to expose.
			if verb != "%d" {
				t.Fatalf("main.go:%d interpolates %s into an error that can reach the host log: %s",
					index+1, verb, strings.TrimSpace(line))
			}
		}
	}
	// Positive control: there IS a formatted error, so this is not passing
	// because the scan found nothing.
	if !strings.Contains(string(source), "fmt.Errorf(") {
		t.Fatal("no formatted errors were found, so this assertion proved nothing")
	}
}

// The old password default is the image's published value, and saying so in
// the flag help is what keeps it from reading as a leaked credential.
func TestOldPasswordDefaultIsDocumentedAsPublished(t *testing.T) {
	source, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(source), "published") {
		t.Fatal("the stock password default is not documented as the image's published value")
	}
}

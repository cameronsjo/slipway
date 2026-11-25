package bootstrap

import (
	"context"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
)

func TestTokenRedactionAndStrictParsing(t *testing.T) {
	token, err := NewToken()
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseToken(token.Encoded())
	if err != nil || parsed != token || parsed.Hash() != token.Hash() {
		t.Fatal("token roundtrip failed")
	}
	encoded, _ := json.Marshal(token)
	outputs := []string{fmt.Sprintf("%v %+v %#v %s %x %q", token, token, token, token, token, token), string(encoded)}
	for _, out := range outputs {
		if strings.Contains(out, token.Encoded()) || !strings.Contains(out, "redacted") {
			t.Fatal("token formatting exposed secret")
		}
	}
	for _, raw := range []string{"", token.Encoded() + "=", " " + token.Encoded(), strings.Repeat("A", 43), strings.Repeat("!", 43)} {
		if _, err := ParseToken(raw); err == nil {
			t.Fatal("invalid token accepted")
		}
	}
}

func TestFetchTLSBoundaryAndNoRetry(t *testing.T) {
	token, _ := NewToken()
	id := uuid.NewString()
	payload := base64.StdEncoding.EncodeToString([]byte(`{".runner":"test"}`))
	for _, mode := range []string{"ok", "untrusted", "missing roots", "redirect", "failure", "wrong type", "truncated", "oversized", "invalid encoding"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				path, _ := Path(id)
				if r.URL.Path != path || r.Header.Get("Authorization") != "Bearer "+token.Encoded() {
					t.Error("wrong bootstrap scope")
				}
				w.Header().Set("Content-Type", "application/octet-stream")
				switch mode {
				case "redirect":
					w.Header().Set("Location", "/stolen")
					w.WriteHeader(307)
				case "failure":
					w.WriteHeader(503)
					fmt.Fprint(w, token.Encoded())
				case "wrong type":
					w.Header().Set("Content-Type", "text/html")
					fmt.Fprint(w, payload)
				case "truncated":
					w.Header().Set("Content-Length", "500")
					fmt.Fprint(w, payload)
				case "oversized":
					fmt.Fprint(w, strings.Repeat("A", MaxJITBytes+4))
				case "invalid encoding":
					fmt.Fprint(w, "%%%%")
				default:
					fmt.Fprint(w, payload)
				}
			}))
			defer server.Close()
			roots := x509.NewCertPool()
			if mode != "untrusted" {
				roots.AddCert(server.Certificate())
			}
			if mode == "missing roots" {
				roots = nil
			}
			b, err := Fetch(context.Background(), server.URL, id, token, roots)
			if mode == "ok" {
				if err != nil || string(b) != payload {
					t.Fatalf("valid handoff failed: %v", err)
				}
			} else if err == nil || b != nil || strings.Contains(err.Error(), token.Encoded()) {
				t.Fatal("bad handoff accepted or exposed secret")
			}
			want := int32(1)
			if mode == "untrusted" || mode == "missing roots" {
				want = 0
			}
			if calls.Load() != want {
				t.Fatal("request was retried or certificate rejection failed")
			}
		})
	}
	for _, origin := range []string{"http://localhost", "https://u:p@localhost", "https://localhost/elsewhere", "https://localhost?", "https://localhost?x=1", "https://localhost/#fragment"} {
		if _, err := Fetch(context.Background(), origin, id, token, nil); err == nil {
			t.Fatal("invalid origin accepted")
		}
	}
}

func TestNativeLauncherArgumentsAndEnvironment(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "bin"), 0700); err != nil {
		t.Fatal(err)
	}
	// A real subprocess stands in for the native runner. It asserts the exec
	// contract without decoding JIT or contacting GitHub.
	script := `#!/bin/sh
test "$#" = 1 && test "$1" = run || exit 11
test -n "$ACTIONS_RUNNER_INPUT_JITCONFIG" || exit 12
test -z "$SLIPWAY_BOOTSTRAP_TOKEN$GITHUB_TOKEN$APP_KEY$ACTIONS_RUNNER_INPUT_TOKEN" || exit 13
printf '%s' "$ACTIONS_RUNNER_INPUT_JITCONFIG"
`
	if err := os.WriteFile(filepath.Join(root, "bin", "Runner.Listener"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	payload := []byte(base64.StdEncoding.EncodeToString([]byte("JIT-sentinel")))
	exe, args, env, err := RunnerCommand(root, payload, []string{"HOME=" + root, "PATH=/usr/bin:/bin", "SLIPWAY_BOOTSTRAP_TOKEN=bootstrap-secret", "GITHUB_TOKEN=gh-secret", "APP_KEY=app-secret", "ACTIONS_RUNNER_INPUT_TOKEN=registration-secret"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.Join(args, " "), string(payload)) {
		t.Fatal("JIT leaked into argv")
	}
	command := exec.Command(exe, args[1:]...)
	command.Env = env
	out, err := command.Output()
	if err != nil || string(out) != string(payload) {
		t.Fatalf("native exec contract failed: %v", err)
	}
	if _, _, _, err := RunnerCommand("relative", payload, env); err == nil {
		t.Fatal("relative root accepted")
	}
	if _, _, _, err := RunnerCommand(root, payload, []string{"PATH=/bin"}); err == nil {
		t.Fatal("missing HOME accepted")
	}
	maximum := []byte(strings.Repeat("A", MaxJITBytes))
	exe, args, env, err = RunnerCommand(root, maximum, []string{"HOME=" + root, "PATH=/usr/bin:/bin"})
	if err != nil {
		t.Fatal(err)
	}
	command = exec.Command(exe, args[1:]...)
	command.Env = env
	out, err = command.Output()
	if err != nil || len(out) != MaxJITBytes {
		t.Fatalf("maximum accepted handoff cannot execute: %v", err)
	}
	if _, _, _, err := RunnerCommand(root, maximum, []string{"HOME=" + root, "PATH=" + strings.Repeat("x", 96<<10)}); err == nil {
		t.Fatal("unbounded environment accepted")
	}
	for _, name := range []string{".env", ".runner", ".credentials", ".credentials_rsaparams", ".service"} {
		path := filepath.Join(root, name)
		if err := os.WriteFile(path, []byte("ACTIONS_RUNNER_INPUT_JITCONFIG=stale"), 0600); err != nil {
			t.Fatal(err)
		}
		if _, _, _, err := RunnerCommand(root, payload, env); err == nil {
			t.Fatalf("stale %s accepted", name)
		}
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
	}
	outside := filepath.Join(t.TempDir(), "Runner.Listener")
	if err := os.WriteFile(outside, []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	listener := filepath.Join(root, "bin", "Runner.Listener")
	if err := os.Remove(listener); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, listener); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := RunnerCommand(root, payload, env); err == nil {
		t.Fatal("accepted symlinked native runner entrypoint")
	}
	rootWithLinkedBin := t.TempDir()
	linkedBin := filepath.Join(t.TempDir(), "bin")
	if err := os.Mkdir(linkedBin, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(linkedBin, "Runner.Listener"), []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(linkedBin, filepath.Join(rootWithLinkedBin, "bin")); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := RunnerCommand(rootWithLinkedBin, payload, env); err == nil {
		t.Fatal("accepted symlinked runner bin directory")
	}
}

package main

import (
	"encoding/base64"
	"encoding/pem"
	"flag"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cameronsjo/slipway/internal/bootstrap"
	"github.com/google/uuid"
)

func TestWorkerSubprocess(t *testing.T) {
	if os.Getenv("SLIPWAY_TEST_WORKER") != "1" {
		return
	}
	for i, arg := range os.Args {
		if arg == "--" {
			os.Args = append([]string{"runner-pool-worker"}, os.Args[i+1:]...)
			break
		}
	}
	flag.CommandLine = flag.NewFlagSet("runner-pool-worker", flag.ExitOnError)
	main()
	os.Exit(0)
}

func TestWorkerEntrypointFetchesThenExecutesWithoutBootstrapSecret(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("successful worker execution requires a non-root job account")
	}
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "bin"), 0700); err != nil {
		t.Fatal(err)
	}
	script := `#!/bin/sh
test "$#" = 1 && test "$1" = run || exit 20
test -z "$SLIPWAY_BOOTSTRAP_TOKEN$GITHUB_TOKEN$APP_KEY$SLIPWAY_TEST_WORKER" || exit 21
printf '%s' "$ACTIONS_RUNNER_INPUT_JITCONFIG"
`
	if err := os.WriteFile(filepath.Join(root, "bin", "Runner.Listener"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	token, err := bootstrap.NewToken()
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.NewString()
	payload := base64.StdEncoding.EncodeToString([]byte("native-runner-input-sentinel"))
	var calls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		path, _ := bootstrap.Path(id)
		if r.URL.Path != path || r.Header.Get("Authorization") != "Bearer "+token.Encoded() {
			t.Error("worker requested wrong capability")
			w.WriteHeader(403)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		fmt.Fprint(w, payload)
	}))
	defer server.Close()
	ca := filepath.Join(root, "ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(os.Args[0], "-test.run=^TestWorkerSubprocess$", "--", "-controller", server.URL, "-lease", id, "-runner-root", root, "-ca-file", ca)
	command.Env = append(os.Environ(), "SLIPWAY_TEST_WORKER=1", "SLIPWAY_BOOTSTRAP_TOKEN="+token.Encoded(), "GITHUB_TOKEN=sentinel-gh", "APP_KEY=sentinel-key")
	output, err := command.CombinedOutput()
	if err != nil || string(output) != payload || calls.Load() != 1 {
		t.Fatalf("worker entrypoint failed: %v output=%q calls=%d", err, output, calls.Load())
	}
	if strings.Contains(string(output), token.Encoded()) {
		t.Fatal("worker printed bootstrap token")
	}
}

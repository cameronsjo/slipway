// runner-pool-worker runs inside a fresh job VM. It never contacts the App API.
package main

import (
	"context"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"os"
	"syscall"

	"github.com/cameronsjo/slipway/internal/bootstrap"
)

func run() error {
	endpoint := flag.String("controller", "", "HTTPS bootstrap origin")
	lease := flag.String("lease", "", "canonical lease UUID")
	root := flag.String("runner-root", "", "absolute native runner installation directory")
	ca := flag.String("ca-file", "", "required public PEM trust roots")
	flag.Parse()
	if os.Geteuid() == 0 || flag.NArg() != 0 || *ca == "" {
		return errors.New("worker must run as the unprivileged job account, without positional arguments")
	}
	raw := os.Getenv("SLIPWAY_BOOTSTRAP_TOKEN")
	if err := os.Unsetenv("SLIPWAY_BOOTSTRAP_TOKEN"); err != nil {
		return errors.New("cannot remove bootstrap environment")
	}
	token, err := bootstrap.ParseToken(raw)
	raw = ""
	if err != nil {
		return err
	}
	if _, err := bootstrap.ValidateRunnerRoot(*root); err != nil {
		return err
	}
	pem, err := os.ReadFile(*ca)
	if err != nil {
		return errors.New("cannot read public bootstrap CA")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		return errors.New("invalid public bootstrap CA")
	}
	payload, err := bootstrap.Fetch(context.Background(), *endpoint, *lease, token, roots)
	token = bootstrap.Token{}
	if err != nil {
		return err
	}
	defer clear(payload)
	executable, args, env, err := bootstrap.RunnerCommand(*root, payload, os.Environ())
	if err != nil {
		return err
	}
	if err := os.Chdir(*root); err != nil {
		return errors.New("cannot enter runner installation")
	}
	clear(payload)
	if err := syscall.Exec(executable, args, env); err != nil {
		return errors.New("native runner execution failed; do not retry JIT delivery")
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

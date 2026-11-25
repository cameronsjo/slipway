// runner-pool-host-tool enters the read-only host root and executes exactly
// one approved MS-A2 inspection tool. The installed filename selects the tool;
// callers cannot supply an arbitrary executable path.
package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

const hostRoot = "/host"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	if os.Geteuid() != 0 {
		return errors.New("host tool launcher requires root")
	}
	tool, err := selectedTool(filepath.Base(os.Args[0]))
	if err != nil {
		return err
	}
	if err := syscall.Chroot(hostRoot); err != nil {
		return errors.New("enter read-only host root")
	}
	if err := os.Chdir("/"); err != nil {
		return errors.New("enter host root directory")
	}
	arguments := append([]string{tool}, os.Args[1:]...)
	return syscall.Exec(tool, arguments, []string{"LANG=C", "PATH=/usr/sbin:/usr/bin:/sbin:/bin"})
}

func selectedTool(name string) (string, error) {
	switch name {
	case "sjo-host-crictl":
		return "/var/lib/rancher/k3s/data/current/bin/crictl", nil
	case "sjo-host-ctr":
		return "/var/lib/rancher/k3s/data/current/bin/ctr", nil
	default:
		return "", errors.New("host tool launcher has an unapproved installed name")
	}
}

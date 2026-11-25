//go:build linux

package kubernetes

import (
	"net"
	"os"
	"path/filepath"
	"testing"
)

func TestUnixPeerUIDReadsKernelCredential(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "peer.sock")
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socket, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	server := make(chan *net.UnixConn, 1)
	errors := make(chan error, 1)
	go func() {
		connection, err := listener.AcceptUnix()
		if err != nil {
			errors <- err
			return
		}
		server <- connection
	}()
	client, err := net.DialUnix("unix", nil, listener.Addr().(*net.UnixAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	var accepted *net.UnixConn
	select {
	case err := <-errors:
		t.Fatal(err)
	case accepted = <-server:
	}
	defer accepted.Close()
	uid, err := unixPeerUID(accepted)
	if err != nil || uid != uint32(os.Geteuid()) {
		t.Fatalf("kernel peer credential = %d, %v; want uid %d", uid, err, os.Geteuid())
	}
}

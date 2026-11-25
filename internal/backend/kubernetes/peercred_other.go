//go:build !linux

package kubernetes

import (
	"errors"
	"net"
)

func unixPeerUID(net.Conn) (uint32, error) {
	return 0, errors.New("host witness peer authentication requires Linux SO_PEERCRED")
}

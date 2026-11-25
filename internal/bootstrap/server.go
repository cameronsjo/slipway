package bootstrap

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"sync"
	"time"
)

const (
	ServerMaxConnections      = 16
	ServerMaxConnectionsPerIP = 4
	ServerMaxHeaderBytes      = 8 << 10
	ServerReadHeaderTime      = 5 * time.Second
	ServerReadTime            = 10 * time.Second
	ServerWriteTime           = 15 * time.Second
	ServerIdleTime            = 15 * time.Second
	ServerShutdownTime        = 2 * time.Minute
)

type ServerConfig struct {
	Address         string
	CertificateFile string
	KeyFile         string
}

// Server is a dedicated, bounded TLS listener for the JIT handler. It has no
// general mux, redirects, proxy integration, or plaintext mode.
type Server struct {
	http     *http.Server
	listener *limitedListener
}

func NewServer(config ServerConfig, jitHandler http.Handler) (*Server, error) {
	if jitHandler == nil || config.Address == "" || config.CertificateFile == "" || config.KeyFile == "" {
		return nil, errors.New("bootstrap server requires an address, TLS keypair and JIT handler")
	}
	if _, _, err := net.SplitHostPort(config.Address); err != nil {
		return nil, errors.New("bootstrap server address must include an explicit port")
	}
	if err := validateTLSMaterial(config.CertificateFile, false); err != nil {
		return nil, err
	}
	if err := validateTLSMaterial(config.KeyFile, true); err != nil {
		return nil, err
	}
	certificate, err := tls.LoadX509KeyPair(config.CertificateFile, config.KeyFile)
	if err != nil {
		return nil, errors.New("invalid bootstrap TLS keypair")
	}
	listener, err := net.Listen("tcp", config.Address)
	if err != nil {
		return nil, errors.New("bootstrap listener failed")
	}
	limited := &limitedListener{
		Listener: listener,
		slots:    make(chan struct{}, ServerMaxConnections),
		maxPerIP: ServerMaxConnectionsPerIP,
		perIP:    make(map[string]int),
		done:     make(chan struct{}),
	}
	server := &http.Server{
		Handler:           jitHandler,
		ErrorLog:          log.New(io.Discard, "", 0),
		ReadHeaderTimeout: ServerReadHeaderTime,
		ReadTimeout:       ServerReadTime,
		WriteTimeout:      ServerWriteTime,
		IdleTimeout:       ServerIdleTime,
		MaxHeaderBytes:    ServerMaxHeaderBytes,
		TLSConfig: &tls.Config{
			MinVersion:   tls.VersionTLS12,
			Certificates: []tls.Certificate{certificate},
			NextProtos:   []string{"http/1.1"},
		},
		TLSNextProto: map[string]func(*http.Server, *tls.Conn, http.Handler){},
	}
	server.SetKeepAlivesEnabled(false)
	return &Server{http: server, listener: limited}, nil
}

// validateTLSMaterial checks the file the controller will actually open.
//
// It used to Lstat and demand a regular file, then reject any private key with
// a group bit set. Both refusals fired against the bundle that mounts this
// material: a Kubernetes Secret volume projects each key as a symlink into a
// timestamped ..data/ directory, and the Deployment mounts it at defaultMode
// 0440 so the group can read it. The controller could not start, and the
// symptom would have been a crash loop at activation rather than a review
// finding.
//
// The two properties worth keeping are that the path resolves to a real file
// rather than a device, FIFO or directory, and that nothing outside the
// controller's own user and group can read the private key. Stat rather than
// Lstat gives the first without rejecting the projection; the permission rule
// below gives the second while permitting exactly the group read the Secret
// mount needs.
//
// What is deliberately NOT relaxed: any bit for "other" on the private key, and
// group write or execute. A world-readable key is still refused, which is the
// case the original check existed for.
func validateTLSMaterial(path string, private bool) error {
	subject := "certificate"
	if private {
		subject = "private key"
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() {
		return fmt.Errorf("bootstrap TLS %s must resolve to a regular file", subject)
	}
	if !private {
		return nil
	}
	mode := info.Mode().Perm()
	if mode&0007 != 0 {
		return errors.New("bootstrap TLS private key must not be readable, writable or executable by other")
	}
	if mode&0030 != 0 {
		return errors.New("bootstrap TLS private key must not be group-writable or group-executable")
	}
	if mode&0440 == 0 {
		return errors.New("bootstrap TLS private key is readable by neither its owner nor its group")
	}
	return nil
}

func (s *Server) Address() net.Addr { return s.listener.Addr() }

// Close also releases the bound address. http.Server.Close only closes the
// listeners it is already serving, so a Server closed before or without Serve
// would otherwise keep the port until the process exits.
func (s *Server) Close() error {
	err := s.http.Close()
	if listenerErr := s.listener.Close(); listenerErr != nil && !errors.Is(listenerErr, net.ErrClosed) {
		err = errors.Join(err, listenerErr)
	}
	return err
}

// Serve stops accepting first, then gives an active one-shot response and its
// caller's lifecycle cleanup the contract's bounded shutdown window.
func (s *Server) Serve(ctx context.Context) error {
	if ctx == nil {
		return errors.New("bootstrap server requires a context")
	}
	errCh := make(chan error, 1)
	tlsListener := tls.NewListener(s.listener, s.http.TLSConfig)
	go func() { errCh <- s.http.Serve(tlsListener) }()
	select {
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), ServerShutdownTime)
		defer cancel()
		err := s.http.Shutdown(shutdownCtx)
		if err != nil {
			err = errors.Join(err, s.http.Close())
		}
		serveErr := <-errCh
		if errors.Is(serveErr, http.ErrServerClosed) {
			serveErr = nil
		}
		return errors.Join(err, serveErr)
	}
}

type limitedListener struct {
	net.Listener
	slots     chan struct{}
	maxPerIP  int
	mu        sync.Mutex
	perIP     map[string]int
	done      chan struct{}
	closeOnce sync.Once
}

func (l *limitedListener) Accept() (net.Conn, error) {
	for {
		select {
		case <-l.done:
			return nil, net.ErrClosed
		default:
		}
		conn, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		host := remoteHostKey(conn.RemoteAddr())
		if !l.tryAcquire(host) {
			_ = conn.Close()
			continue
		}
		return &limitedConnection{Conn: conn, release: func() { l.release(host) }}, nil
	}
}

func (l *limitedListener) tryAcquire(host string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if host != "" && l.maxPerIP > 0 && l.perIP[host] >= l.maxPerIP {
		return false
	}
	select {
	case l.slots <- struct{}{}:
	default:
		return false
	}
	if host != "" {
		l.perIP[host]++
	}
	return true
}

func (l *limitedListener) release(host string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	select {
	case <-l.slots:
	default:
	}
	if host != "" {
		if l.perIP[host] <= 1 {
			delete(l.perIP, host)
		} else {
			l.perIP[host]--
		}
	}
}

// remoteHostKey returns the normalized IP string for non-loopback peers so a
// single worker pod cannot exhaust all bootstrap slots for other tenants
// (homelab#875 I1). Loopback returns "" so local multi-worker adapters (macOS)
// and in-process tests are bounded only by maxTotal.
func remoteHostKey(addr net.Addr) string {
	if addr == nil {
		return ""
	}
	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		host = addr.String()
	}
	ip := net.ParseIP(host)
	if ip == nil || ip.IsLoopback() {
		return ""
	}
	return ip.String()
}

func (l *limitedListener) Close() error {
	l.closeOnce.Do(func() { close(l.done) })
	return l.Listener.Close()
}

type limitedConnection struct {
	net.Conn
	once    sync.Once
	release func()
}

func (c *limitedConnection) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.release)
	return err
}

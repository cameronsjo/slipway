package kubernetes

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	hostWitnessProtocol = "sjo-runner-pool-host-witness/v1"
	hostWitnessMaxFrame = 1 << 20
)

// HostWitnessProvider is implemented by the privileged, host-side collector.
// The Unix transport keeps its CRI, process, storage, and node access outside
// both the controller and untrusted worker VMs.
type HostWitnessProvider interface {
	ListProcesses(context.Context, string, string) (WitnessScope, error)
	ListWritableStorage(context.Context, string, string) (WitnessScope, error)
	ObserveCapacity(context.Context, string) (CapacitySnapshot, error)
}

// HostWitnessSources composes separately testable collectors behind the one
// authenticated host endpoint. Log, when set, receives one line per FAILED
// request; a nil Log keeps the previous silent behaviour rather than panicking,
// which matters because every existing test constructs this struct directly.
type HostWitnessSources struct {
	Processes ProcessSource
	Storage   StorageSource
	Capacity  CapacitySource
	Log       *WitnessLogger
}

func (s HostWitnessSources) ListProcesses(ctx context.Context, namespace, ownerUID string) (WitnessScope, error) {
	if s.Processes == nil {
		return WitnessScope{Complete: false}, errors.New("host process collector is unavailable")
	}
	scope, err := s.Processes.ListProcesses(ctx, namespace, ownerUID)
	s.Log.Failure(WitnessReasonProcesses, err)
	return scope, err
}

func (s HostWitnessSources) ListWritableStorage(ctx context.Context, namespace, ownerUID string) (WitnessScope, error) {
	if s.Storage == nil {
		return WitnessScope{Complete: false}, errors.New("host storage collector is unavailable")
	}
	scope, err := s.Storage.ListWritableStorage(ctx, namespace, ownerUID)
	s.Log.Failure(witnessReasonFor(WitnessReasonStorage, err), err)
	return scope, err
}

func (s HostWitnessSources) ObserveCapacity(ctx context.Context, namespace string) (CapacitySnapshot, error) {
	if s.Capacity == nil {
		return CapacitySnapshot{Complete: false}, errors.New("host capacity collector is unavailable")
	}
	snapshot, err := s.Capacity.ObserveCapacity(ctx, namespace)
	s.Log.Failure(WitnessReasonCapacity, err)
	return snapshot, err
}

// witnessReasonFor refines a collector's reason code. Only the storage
// collector has a sub-stage today: it verifies the runtime fingerprints before
// it reads anything, so its failures split in two.
func witnessReasonFor(collector string, err error) string {
	var fingerprint *fingerprintFailure
	if err != nil && errors.As(err, &fingerprint) {
		return WitnessReasonFingerprints
	}
	return collector
}

var _ HostWitnessProvider = HostWitnessSources{}

type hostWitnessRequest struct {
	Protocol  string `json:"protocol"`
	Operation string `json:"operation"`
	Namespace string `json:"namespace"`
	OwnerUID  string `json:"owner_uid"`
}

type hostWitnessResponse struct {
	Protocol  string            `json:"protocol"`
	Operation string            `json:"operation"`
	Namespace string            `json:"namespace"`
	OwnerUID  string            `json:"owner_uid"`
	Witnesses *WitnessScope     `json:"witnesses,omitempty"`
	Capacity  *CapacitySnapshot `json:"capacity,omitempty"`
	Failure   string            `json:"failure,omitempty"`
	// Reason is the closed-vocabulary code that survives the hop, so the
	// controller's one line localises the fault without a second log. Failure
	// deliberately stays a byte count -- the collector's own sentence may
	// describe host paths and belongs in the witness log, not in the
	// controller's.
	//
	// Grammar: a WitnessReason* constant, optionally "/" and the base name of
	// the tool that failed. Both halves are bounded and character-allowlisted.
	//
	// PROTOCOL NOTE: both ends set DisallowUnknownFields, so an old client
	// would refuse a response carrying this field. That is acceptable here and
	// nowhere else: the controller and the witness are two images built from
	// one commit and published as a pair, and the admission policy pins both
	// digests, so a mixed pair cannot be admitted. omitempty keeps a successful
	// response byte-identical to the previous protocol.
	Reason string `json:"reason,omitempty"`
}

// HostWitnessClient authenticates the server with kernel peer credentials and
// implements the three non-Kubernetes inventory sources. Every observation is
// a fresh one-request connection, so a broken or replaced witness fails closed.
type HostWitnessClient struct {
	SocketPath        string
	ExpectedServerUID uint32
	Timeout           time.Duration
	peerUID           func(net.Conn) (uint32, error)
	dialContext       func(context.Context, string, string) (net.Conn, error)
}

var _ ProcessSource = (*HostWitnessClient)(nil)
var _ StorageSource = (*HostWitnessClient)(nil)
var _ CapacitySource = (*HostWitnessClient)(nil)

func (c *HostWitnessClient) ListProcesses(ctx context.Context, namespace, ownerUID string) (WitnessScope, error) {
	response, err := c.call(ctx, "processes", namespace, ownerUID)
	if err != nil {
		return WitnessScope{Complete: false}, err
	}
	if response.Witnesses == nil || response.Capacity != nil || !response.Witnesses.Complete {
		return WitnessScope{Complete: false}, errors.New("host process witness response is incomplete")
	}
	return *response.Witnesses, nil
}

func (c *HostWitnessClient) ListWritableStorage(ctx context.Context, namespace, ownerUID string) (WitnessScope, error) {
	response, err := c.call(ctx, "storage", namespace, ownerUID)
	if err != nil {
		return WitnessScope{Complete: false}, err
	}
	if response.Witnesses == nil || response.Capacity != nil || !response.Witnesses.Complete {
		return WitnessScope{Complete: false}, errors.New("host storage witness response is incomplete")
	}
	return *response.Witnesses, nil
}

func (c *HostWitnessClient) ObserveCapacity(ctx context.Context, namespace string) (CapacitySnapshot, error) {
	response, err := c.call(ctx, "capacity", namespace, "")
	if err != nil {
		return CapacitySnapshot{Complete: false}, err
	}
	if response.Capacity == nil || response.Witnesses != nil || !response.Capacity.Complete {
		return CapacitySnapshot{Complete: false}, errors.New("host capacity witness response is incomplete")
	}
	return *response.Capacity, nil
}

func (c *HostWitnessClient) call(ctx context.Context, operation, namespace, ownerUID string) (hostWitnessResponse, error) {
	if c == nil || ctx == nil || c.SocketPath == "" || c.Timeout <= 0 || namespace == "" {
		return hostWitnessResponse{}, errors.New("host witness client configuration is incomplete")
	}
	if operation != "capacity" && ownerUID == "" {
		return hostWitnessResponse{}, errors.New("host witness ownership scope is empty")
	}
	dial := c.dialContext
	if dial == nil {
		dialer := net.Dialer{Timeout: c.Timeout}
		dial = dialer.DialContext
	}
	connection, err := dial(ctx, "unix", c.SocketPath)
	if err != nil {
		return hostWitnessResponse{}, fmt.Errorf("connect to host witness: %w", err)
	}
	defer connection.Close()
	peerIdentity := c.peerUID
	if peerIdentity == nil {
		peerIdentity = unixPeerUID
	}
	uid, err := peerIdentity(connection)
	if err != nil || uid != c.ExpectedServerUID {
		if err == nil {
			err = fmt.Errorf("unexpected host witness uid %d", uid)
		}
		return hostWitnessResponse{}, fmt.Errorf("authenticate host witness: %w", err)
	}
	deadline := time.Now().Add(c.Timeout)
	if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(deadline) {
		deadline = contextDeadline
	}
	if err := connection.SetDeadline(deadline); err != nil {
		return hostWitnessResponse{}, err
	}
	request := hostWitnessRequest{Protocol: hostWitnessProtocol, Operation: operation, Namespace: namespace, OwnerUID: ownerUID}
	if err := json.NewEncoder(connection).Encode(request); err != nil {
		return hostWitnessResponse{}, fmt.Errorf("send host witness request: %w", err)
	}
	var response hostWitnessResponse
	decoder := json.NewDecoder(io.LimitReader(connection, hostWitnessMaxFrame))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&response); err != nil {
		return hostWitnessResponse{}, fmt.Errorf("read host witness response: %w", err)
	}
	if response.Protocol != hostWitnessProtocol || response.Operation != operation || response.Namespace != namespace || response.OwnerUID != ownerUID {
		return hostWitnessResponse{}, errors.New("host witness response scope or protocol mismatch")
	}
	if response.Failure != "" {
		// The reason is what makes the controller's single line enough. The
		// byte-count Failure stays too: it distinguishes a collector that
		// refused from one that returned nothing at all.
		return hostWitnessResponse{}, &hostWitnessRefusal{reason: safeWitnessReason(response.Reason), failure: response.Failure}
	}
	return response, nil
}

// hostWitnessRefusal is a witness that answered, authenticated and in scope,
// and refused: one of its collectors could not certify the host. Typed so
// Backend.Create can tell a refusal apart from a witness it could not reach or
// read. The text is the line it has always been.
type hostWitnessRefusal struct{ reason, failure string }

func (r *hostWitnessRefusal) Error() string {
	return fmt.Sprintf("host witness rejected request: reason=%s (%s)", r.reason, r.failure)
}

// HostWitnessServer authenticates each caller with SO_PEERCRED before reading
// one bounded JSON request. It serves a single immutable namespace/owner scope.
type HostWitnessServer struct {
	SocketPath        string
	SocketGID         int
	ExpectedClientUID uint32
	Namespace         string
	OwnerUID          string
	Timeout           time.Duration
	Provider          HostWitnessProvider
	peerUID           func(net.Conn) (uint32, error)
}

func (s *HostWitnessServer) Serve(ctx context.Context) error {
	if s == nil || ctx == nil || s.SocketPath == "" || s.SocketGID < 0 || s.Namespace == "" || s.OwnerUID == "" || s.Timeout <= 0 || s.Provider == nil {
		return errors.New("host witness server configuration is incomplete")
	}
	if err := s.clearStaleSocket(); err != nil {
		return err
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: s.SocketPath, Net: "unix"})
	if err != nil {
		return fmt.Errorf("listen for host witness requests: %w", err)
	}
	defer listener.Close()
	if err := os.Chmod(s.SocketPath, 0o660); err != nil {
		return fmt.Errorf("set host witness socket mode: %w", err)
	}
	if err := os.Chown(s.SocketPath, os.Geteuid(), s.SocketGID); err != nil {
		return fmt.Errorf("set host witness socket group: %w", err)
	}
	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()
	for {
		connection, err := listener.AcceptUnix()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("accept host witness request: %w", err)
		}
		if err := s.handle(ctx, connection); err != nil {
			_ = connection.Close()
			continue
		}
		_ = connection.Close()
	}
}

// clearStaleSocket leaves the path free for Listen, or refuses.
//
// A SIGKILL, an OOM kill or a node reboot mid-run leaves the socket file
// behind (homelab#1098, review D11). Refusing on any existing path made the
// witness crash-loop, and the pool fence, until someone removed it as root. So
// an existing path is removed only when it is proven to be a dead socket: it
// is a socket, and a dial to it is refused, which is the kernel's answer when
// nothing listens. A live witness answers the dial and still refuses this
// start; a regular file, a directory or any other error is left untouched.
func (s *HostWitnessServer) clearStaleSocket() error {
	info, err := os.Lstat(s.SocketPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect host witness socket: %w", err)
	}
	if info.Mode()&os.ModeSocket == 0 {
		return errors.New("host witness socket path exists and is not a socket; reconcile its owner before restart")
	}
	connection, err := net.DialTimeout("unix", s.SocketPath, time.Second)
	if err == nil {
		_ = connection.Close()
		return errors.New("host witness socket is served by a live process; refusing a second witness")
	}
	if !errors.Is(err, syscall.ECONNREFUSED) {
		return fmt.Errorf("probe existing host witness socket: %w", err)
	}
	if err := os.Remove(s.SocketPath); err != nil {
		return fmt.Errorf("remove stale host witness socket: %w", err)
	}
	return nil
}

func (s *HostWitnessServer) handle(ctx context.Context, connection net.Conn) error {
	peerIdentity := s.peerUID
	if peerIdentity == nil {
		peerIdentity = unixPeerUID
	}
	uid, err := peerIdentity(connection)
	if err != nil || uid != s.ExpectedClientUID {
		return errors.New("host witness client peer credential rejected")
	}
	if err := connection.SetDeadline(time.Now().Add(s.Timeout)); err != nil {
		return err
	}
	reader := bufio.NewReader(io.LimitReader(connection, hostWitnessMaxFrame))
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	var request hostWitnessRequest
	if err := decoder.Decode(&request); err != nil {
		return err
	}
	// Each collector call gets the request's own deadline, not the server's
	// lifetime context (review D11). Requests are served one at a time, and the
	// connection deadline bounds only I/O: a hung `ctr` or `lvs` under the
	// server context held the loop, and every later inventory fenced behind it.
	ctx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()
	response := hostWitnessResponse{Protocol: hostWitnessProtocol, Operation: request.Operation, Namespace: request.Namespace, OwnerUID: request.OwnerUID}
	if request.Protocol != hostWitnessProtocol || request.Namespace != s.Namespace || (request.Operation != "capacity" && request.OwnerUID != s.OwnerUID) || (request.Operation == "capacity" && request.OwnerUID != "") {
		response.Failure, response.Reason = "request scope or protocol rejected", WitnessReasonScope
	} else {
		switch request.Operation {
		case "processes":
			value, err := s.Provider.ListProcesses(ctx, s.Namespace, s.OwnerUID)
			response.Witnesses = &value
			response.Failure, response.Reason = safeWitnessFailure(err), witnessReasonCode(WitnessReasonProcesses, err)
		case "storage":
			value, err := s.Provider.ListWritableStorage(ctx, s.Namespace, s.OwnerUID)
			response.Witnesses = &value
			response.Failure, response.Reason = safeWitnessFailure(err), witnessReasonCode(WitnessReasonStorage, err)
		case "capacity":
			value, err := s.Provider.ObserveCapacity(ctx, s.Namespace)
			response.Capacity = &value
			response.Failure, response.Reason = safeWitnessFailure(err), witnessReasonCode(WitnessReasonCapacity, err)
		default:
			response.Failure, response.Reason = "unsupported host witness operation", WitnessReasonOperation
		}
	}
	if err := json.NewEncoder(connection).Encode(response); err != nil {
		return err
	}
	return nil
}

func safeWitnessFailure(err error) string {
	if err == nil {
		return ""
	}
	return "collector failed (" + strconv.Itoa(len(err.Error())) + " bytes)"
}

// witnessReasonCode builds the code that crosses the socket: the collector,
// refined to "fingerprints" where that applies, plus the base name of the tool
// that failed when a host command was the cause.
//
// The tool name is the only part not drawn from a constant. It is the base name
// of an operator-configured absolute path -- never worker or request input --
// and safeToolName allowlists its characters before it is sent.
func witnessReasonCode(collector string, err error) string {
	if err == nil {
		return ""
	}
	reason := witnessReasonFor(collector, err)
	var command *hostCommandFailure
	if errors.As(err, &command) && command.Tool != "" {
		return reason + "/" + command.Tool
	}
	return reason
}

// safeWitnessReason re-applies the grammar to a reason RECEIVED over the
// socket. The server is peer-credential authenticated, so this is not the
// boundary that keeps a hostile value out -- it is the one that keeps a
// malformed one from reaching a log line or an error string unexamined.
func safeWitnessReason(reason string) string {
	collector, tool, split := strings.Cut(reason, "/")
	switch collector {
	case WitnessReasonProcesses, WitnessReasonStorage, WitnessReasonCapacity,
		WitnessReasonFingerprints, WitnessReasonScope, WitnessReasonOperation:
	default:
		return "unrecognized"
	}
	if !split {
		return collector
	}
	if safe := safeToolName(tool); safe != "" {
		return collector + "/" + safe
	}
	return collector
}

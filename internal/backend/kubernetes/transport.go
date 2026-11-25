package kubernetes

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"

	"github.com/cameronsjo/slipway/internal/bootstrap"
)

const (
	listPageSize       int64 = 100
	attachPollInterval       = 250 * time.Millisecond
)

// PhysicalWorker is supplied by a trusted CRI/storage observer. This package
// does not inspect the node, containerd, or writable storage itself.
type PhysicalWorker struct {
	Name             string
	UID              string
	Labels           map[string]string
	RuntimeClassName string
	ProcessPresent   bool
	StoragePresent   bool
}

type PhysicalInventory struct {
	Complete bool
	Workers  []PhysicalWorker
}

// TrustedWitness supplies observations that the Kubernetes API cannot prove.
// Implementations must enumerate Kata sandboxes/processes and all writable
// worker storage, and must bound WaitAbsent internally. No worker receives this
// witness or Kubernetes credentials.
type TrustedWitness interface {
	Inventory(context.Context, string, string) (PhysicalInventory, error)
	Headroom(context.Context, string, Footprint) (Headroom, error)
	WaitAbsent(context.Context, DeleteRequest) (AbsenceWitness, error)
}

// Transport is the concrete in-cluster Kubernetes Pod transport. It owns the
// Kubernetes client and authenticated attach transport, while physical facts
// remain delegated to TrustedWitness.
type Transport struct {
	client  kubernetes.Interface
	config  *rest.Config
	witness TrustedWitness
	worker  Config

	// These hooks keep source tests independent of an API server. Production
	// construction leaves them nil so the bounded client-go implementations run.
	waitRunner func(context.Context, string, string, string, time.Time) (corev1.Pod, error)
	attach     func(context.Context, corev1.Pod, bootstrap.Token) error
}

var _ API = (*Transport)(nil)

// NewTransport builds a transport around an already authenticated in-cluster
// client. The rest.Config is retained solely for the authenticated attach
// stream; client-go's CoreV1 client performs the Pod API calls.
func NewTransport(config *rest.Config, client kubernetes.Interface, witness TrustedWitness, worker Config) (*Transport, error) {
	if config == nil || client == nil || witness == nil {
		return nil, errors.New("Kubernetes transport requires REST config, client, and trusted witness")
	}
	if err := worker.validate(); err != nil {
		return nil, err
	}
	return &Transport{client: client, config: config, witness: witness, worker: worker}, nil
}

// NewInCluster constructs the controller-side client using the Pod's service
// account. This reads credentials only in the controller process; worker Pods
// explicitly disable service-account token automounting.
func NewInCluster(worker Config, witness TrustedWitness) (*Transport, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, errors.New("cannot load in-cluster Kubernetes configuration")
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, errors.New("cannot create in-cluster Kubernetes client")
	}
	return NewTransport(config, client, witness, worker)
}

func (t *Transport) List(ctx context.Context, namespace, selector string) (ListResult, error) {
	if ctx == nil || namespace != t.worker.Namespace || selector != (&Backend{config: t.worker}).Selector() {
		return ListResult{Complete: false}, errors.New("Kubernetes list scope does not match the immutable worker configuration")
	}
	// THE WITNESS IS READ BEFORE THE PODS, AND THE ORDER IS THE POINT (#1109
	// review). The two reads are not atomic. A Kata process or worker storage
	// exists only for a Pod object that already exists, and a Pod object leaves
	// the list only by deletion. So with the witness first, every process it
	// reports belongs to a Pod created before the Pod list began: a live Pod is
	// in that list, and "witnessed, no Pod row" means the Pod really was deleted.
	// Read the other way round, a Pod created between the two reads has a
	// witnessed process and no Pod row, and Inventory reads that as an orphan --
	// which retires a lease running a real job.
	physical, err := t.witness.Inventory(ctx, namespace, selector)
	if err != nil || !physical.Complete {
		if err == nil {
			err = errors.New("trusted physical inventory is incomplete")
		}
		return ListResult{Complete: false}, fmt.Errorf("stage=%s: %w", inventoryStageListWitness, err)
	}
	pods, err := t.listAllPods(ctx, namespace, selector)
	if err != nil {
		return ListResult{Complete: false}, fmt.Errorf("stage=%s: %w", inventoryStageListPods, err)
	}

	physicalByName := make(map[string]PhysicalWorker, len(physical.Workers))
	for _, item := range physical.Workers {
		if item.Name == "" || item.UID == "" || physicalByName[item.Name].Name != "" {
			return ListResult{Complete: false}, errors.New("trusted physical inventory contains duplicate or empty identity")
		}
		physicalByName[item.Name] = item
	}
	seen := make(map[string]bool, len(pods))
	items := make([]WorkerResource, 0, len(pods)+len(physical.Workers))
	for _, pod := range pods {
		item, err := t.resourceFromPod(pod)
		if err != nil {
			return ListResult{Complete: false}, err
		}
		if seen[item.Name] {
			return ListResult{Complete: false}, errors.New("Kubernetes Pod listing contains duplicate identity")
		}
		seen[item.Name] = true
		if observed, ok := physicalByName[item.Name]; ok {
			if observed.UID != item.UID {
				return ListResult{Complete: false}, errors.New("Kubernetes Pod UID disagrees with physical witness")
			}
			item.ProcessPresent = observed.ProcessPresent
			item.StoragePresent = observed.StoragePresent
			delete(physicalByName, item.Name)
		}
		items = append(items, item)
	}
	for _, orphan := range physicalByName {
		if err := t.validatePhysical(orphan); err != nil {
			return ListResult{Complete: false}, err
		}
		items = append(items, WorkerResource{Namespace: namespace, Name: orphan.Name, UID: orphan.UID,
			Labels: cloneStrings(orphan.Labels), RuntimeClassName: orphan.RuntimeClassName,
			ProcessPresent: orphan.ProcessPresent, StoragePresent: orphan.StoragePresent})
	}
	return ListResult{Complete: true, Items: items}, nil
}

// Transport's own inventory sub-stages. The two halves of a listing fail for
// completely different reasons -- the Kubernetes API versus the host witness --
// and from outside they produced the same single fenced line.
const (
	inventoryStageListPods        = "list-pods"
	inventoryStageListWitness     = "list-witness"
	inventoryStageHeadroomWitness = "headroom-witness"
)

func (t *Transport) Headroom(ctx context.Context, namespace string, perWorker Footprint) (Headroom, error) {
	if ctx == nil || namespace != t.worker.Namespace {
		return Headroom{Complete: false}, errors.New("Kubernetes headroom scope does not match the immutable worker configuration")
	}
	// Headroom is entirely the witness's answer -- there is no Kubernetes half
	// here -- so the tag names the witness rather than implying a choice.
	headroom, err := t.witness.Headroom(ctx, namespace, perWorker)
	if err != nil {
		return Headroom{Complete: false}, fmt.Errorf("stage=%s: %w", inventoryStageHeadroomWitness, err)
	}
	return headroom, nil
}

func (t *Transport) Create(ctx context.Context, request CreateRequest) (WorkerResource, error) {
	if ctx == nil || request.BootstrapMode != MemoryOnlyBootstrap || request.StartupDeadline.IsZero() {
		return WorkerResource{}, errors.New("Kubernetes create requires a bounded memory-only bootstrap request")
	}
	if _, err := bootstrap.ParseToken(request.BootstrapToken.Encoded()); err != nil {
		return WorkerResource{}, errors.New("Kubernetes create requires a valid one-use bootstrap capability")
	}
	if specContainsCapability(request.Spec, request.BootstrapToken.Encoded()) {
		return WorkerResource{}, errors.New("Kubernetes Pod spec contains the bootstrap capability")
	}
	if err := t.validateCreateSpec(request.Spec); err != nil {
		return WorkerResource{}, err
	}
	if !time.Now().Before(request.StartupDeadline) {
		return WorkerResource{}, errors.New("Kubernetes worker startup deadline has expired")
	}
	createCtx, cancel := context.WithDeadline(ctx, request.StartupDeadline)
	defer cancel()
	pod, err := t.client.CoreV1().Pods(t.worker.Namespace).Create(createCtx, t.toPod(request.Spec), metav1.CreateOptions{})
	if err != nil {
		// NOT wrapped, and this is a security decision rather than a style one.
		// A client-go create error can quote the REQUEST BODY, and this request
		// body is a Pod spec built for a worker. Even though
		// specContainsCapability has already refused a spec carrying the
		// bootstrap token, "the error cannot quote what is not there" is a
		// weaker guarantee than "the error is a constant". See the leak canary.
		return WorkerResource{}, fmt.Errorf("create stage=%s: %w", createStagePod,
			errors.New("Kubernetes Pod create outcome is uncertain"))
	}
	if pod == nil || pod.UID == "" || pod.Namespace != t.worker.Namespace || pod.Name != request.Spec.Name {
		return WorkerResource{}, fmt.Errorf("create stage=%s: %w", createStagePod,
			errors.New("Kubernetes Pod create response lacks exact identity"))
	}
	if _, err := t.resourceFromPod(*pod); err != nil {
		return WorkerResource{}, fmt.Errorf("create stage=%s: %w", createStageValidatePod, err)
	}
	wait := t.waitRunner
	if wait == nil {
		wait = t.waitForRunner
	}
	ready, err := wait(createCtx, pod.Namespace, pod.Name, string(pod.UID), request.StartupDeadline)
	if err != nil {
		return WorkerResource{}, fmt.Errorf("create stage=%s: %w", createStageWaitRunner, err)
	}
	observed, err := t.resourceFromPod(ready)
	if err != nil {
		// A SECOND validate of the same Pod, and a different stage tag on
		// purpose: the create response and the ready Pod are two observations
		// minutes apart, and a Pod that validated at creation and not at
		// readiness has acquired something -- an annotation, a changed
		// runtimeClassName -- between them. Reporting both as "validate-pod"
		// would hide which observation disagreed.
		return WorkerResource{}, fmt.Errorf("create stage=%s: %w", createStageValidateReady, err)
	}
	if err := t.requirePhysicalWorker(createCtx, observed); err != nil {
		return WorkerResource{}, fmt.Errorf("create stage=%s: %w", createStagePhysicalWorker, err)
	}
	attach := t.attach
	if attach == nil {
		attach = t.attachBootstrap
	}
	if err := attach(createCtx, ready, request.BootstrapToken); err != nil {
		// The SUB-STEP now propagates, because the sub-step is what mattered:
		// this stage covered the identity handshake, the capability write, the
		// acknowledgement and the close, and reporting one sentence for all
		// four is what hid a worker printing its identity before anyone was
		// listening.
		//
		// WHAT PROPAGATES IS ONLY deliverBootstrap's OWN CONSTANTS -- prompt
		// failed, prompt unanswered, handshake failed, capability write failed,
		// acknowledgement failed, close uncertain. Each is a literal in this
		// file and carries nothing from the wire.
		//
		// WHAT DOES NOT PROPAGATE is the executor's or stream's own error, and
		// that boundary is the security one. A remotecommand stream error can
		// carry the SPDY URL, response headers, and in some failure modes the
		// bytes in flight -- which here ARE the single-use capability. Every
		// return inside deliverBootstrap replaces such an error with a constant
		// rather than wrapping it, and attachBootstrap does the same. The leak
		// canary pins both directions: the constants survive, the stream's
		// words never appear.
		//
		// The runner container's own state is appended because the two
		// hypotheses this stage cannot tell apart -- "the guest never spoke"
		// and "there was no guest to speak" -- are settled by it, and the
		// controller already holds `get pods`. Constants and integers only:
		// the phase and the reason come from a fixed allowlist, the exit code
		// is an int.
		return WorkerResource{}, fmt.Errorf("create stage=%s: %w (%s)", createStageAttach, err,
			t.runnerContainerState(createCtx, ready.Namespace, ready.Name, string(ready.UID)))
	}
	return observed, nil
}

// runnerContainerState describes the runner container at the moment an attach
// failed. Every word it can return is a literal in this function.
//
// A reason string is kubelet-generated and is NOT trusted as text: it is mapped
// through an allowlist, and anything unrecognized becomes `other`. That is the
// same rule the witness diagnostics follow, for the same reason -- the set of
// strings a component might emit keeps growing, so only the set we accept can
// be finite.
func (t *Transport) runnerContainerState(ctx context.Context, namespace, name, expectedUID string) string {
	pod, err := t.client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil || pod == nil {
		return "runner=unreadable"
	}
	if string(pod.UID) != expectedUID {
		return "runner=replaced"
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name != "runner" {
			continue
		}
		switch {
		case status.State.Running != nil:
			return "runner=running"
		case status.State.Waiting != nil:
			return "runner=waiting reason=" + allowedContainerReason(status.State.Waiting.Reason)
		case status.State.Terminated != nil:
			return "runner=terminated reason=" + allowedContainerReason(status.State.Terminated.Reason) +
				" exit=" + strconv.Itoa(int(status.State.Terminated.ExitCode))
		default:
			return "runner=unknown"
		}
	}
	return "runner=absent"
}

func allowedContainerReason(reason string) string {
	switch reason {
	case "ContainerCreating", "CrashLoopBackOff", "ImagePullBackOff", "ErrImagePull",
		"CreateContainerError", "CreateContainerConfigError", "RunContainerError",
		"Completed", "Error", "OOMKilled", "ContainerStatusUnknown", "DeadlineExceeded":
		return reason
	case "":
		return "none"
	default:
		return "other"
	}
}

// Create stages. Worker creation is five distinct operations and it reported
// one sentence for all of them: `worker creation outcome uncertain`. The
// vocabulary is fixed and each value names a real boundary above.
//
//	create-pod        the Pod API create and its response identity
//	validate-pod      the create response against the worker identity rules
//	wait-runner       waiting for the Pod to become attachable
//	validate-ready    the SAME rules against the ready Pod, a later observation
//	physical-worker   the CRI/QEMU and writable-storage proof before the token
//	attach            writing the single-use capability into the worker
const (
	createStagePod            = "create-pod"
	createStageValidatePod    = "validate-pod"
	createStageWaitRunner     = "wait-runner"
	createStageValidateReady  = "validate-ready"
	createStagePhysicalWorker = "physical-worker"
	createStageAttach         = "attach"
)

// requirePhysicalWorker records and verifies the CRI/QEMU and writable-storage
// identity before the single-use capability crosses into the VM. This also
// makes the durable host ledger part of successful creation rather than waiting
// for the next controller cycle.
func (t *Transport) requirePhysicalWorker(ctx context.Context, worker WorkerResource) error {
	physical, err := t.witness.Inventory(ctx, t.worker.Namespace, (&Backend{config: t.worker}).Selector())
	if err != nil {
		// Wrapped: this is the whole trusted-inventory tree -- the Pod scope,
		// both witness scopes and the headroom arithmetic -- and every error in
		// it is already a fixed string or a closed-vocabulary witness reason
		// (audited for the fence path, same tree).
		return fmt.Errorf("trusted physical inventory failed before bootstrap: %w", err)
	}
	if !physical.Complete {
		return errors.New("trusted physical inventory is incomplete before bootstrap")
	}
	found := false
	for _, item := range physical.Workers {
		if invalid := t.validatePhysical(item); invalid != nil {
			return fmt.Errorf("physical inventory contains an invalid worker before bootstrap: %w", invalid)
		}
		if item.Name != worker.Name {
			continue
		}
		if found {
			return errors.New("physical inventory contains duplicate worker identity before bootstrap")
		}
		found = true
		// THE THREE BOOLEANS, REPORTED SEPARATELY. This one sentence stood for
		// three independent failures -- a UID that disagrees with the Pod, a
		// Kata process the witness could not see in /proc, and a writable
		// snapshot it could not match -- which send an operator to three
		// different places. It is also the first check in the whole system that
		// a worker actually reaches, so nothing before the first real job could
		// have exercised it.
		//
		// Booleans and the two UIDs only. The UIDs are Kubernetes object
		// identities, already present in the Pod name and every log line about
		// it, and they are what distinguishes a stale witness observation from
		// a wrong one.
		if item.UID != worker.UID || !item.ProcessPresent || !item.StoragePresent {
			return fmt.Errorf(
				"worker physical identity is incomplete before bootstrap (uid_matches=%t process_present=%t storage_present=%t witness_uid=%q pod_uid=%q)",
				item.UID == worker.UID, item.ProcessPresent, item.StoragePresent, item.UID, worker.UID)
		}
	}
	if found {
		return nil
	}
	return errors.New("worker is absent from physical inventory before bootstrap")
}

func (t *Transport) Delete(ctx context.Context, request DeleteRequest) (AbsenceWitness, error) {
	if ctx == nil || request.Namespace != t.worker.Namespace || request.Name == "" || request.UID == "" {
		return AbsenceWitness{}, errors.New("Kubernetes delete requires an exact namespace, name, and UID")
	}
	pod, err := t.client.CoreV1().Pods(request.Namespace).Get(ctx, request.Name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return t.waitForPhysicalAbsence(ctx, request)
		}
		return AbsenceWitness{}, errors.New("Kubernetes owned Pod lookup failed; deletion outcome is unresolved")
	}
	item, err := t.resourceFromPod(*pod)
	if err != nil || item.UID != request.UID || !sameStrings(item.Labels, request.Labels) {
		return AbsenceWitness{}, errors.New("Kubernetes owned Pod identity or labels changed")
	}
	uid := types.UID(request.UID)
	policy := metav1.DeletePropagationForeground
	if err := t.client.CoreV1().Pods(request.Namespace).Delete(ctx, request.Name, metav1.DeleteOptions{
		Preconditions:     &metav1.Preconditions{UID: &uid},
		PropagationPolicy: &policy,
	}); err != nil {
		if apierrors.IsNotFound(err) {
			return t.waitForPhysicalAbsence(ctx, request)
		}
		return AbsenceWitness{}, errors.New("Kubernetes Pod delete outcome is uncertain")
	}
	return t.waitForPhysicalAbsence(ctx, request)
}

func (t *Transport) waitForPhysicalAbsence(ctx context.Context, request DeleteRequest) (AbsenceWitness, error) {
	witness, err := t.witness.WaitAbsent(ctx, request)
	if err != nil {
		return AbsenceWitness{}, err
	}
	if witness.UID != request.UID {
		return AbsenceWitness{}, errors.New("trusted absence witness UID mismatch")
	}
	return witness, nil
}

func (t *Transport) listAllPods(ctx context.Context, namespace, selector string) ([]corev1.Pod, error) {
	var pods []corev1.Pod
	options := metav1.ListOptions{LabelSelector: selector, Limit: listPageSize}
	for {
		page, err := t.client.CoreV1().Pods(namespace).List(ctx, options)
		if err != nil || page == nil {
			return nil, errors.New("Kubernetes Pod listing outcome is incomplete")
		}
		pods = append(pods, page.Items...)
		if page.Continue == "" {
			return pods, nil
		}
		options.Continue = page.Continue
	}
}

func (t *Transport) waitForRunner(ctx context.Context, namespace, name, expectedUID string, deadline time.Time) (corev1.Pod, error) {
	waitCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	ticker := time.NewTicker(attachPollInterval)
	defer ticker.Stop()
	for {
		pod, err := t.client.CoreV1().Pods(namespace).Get(waitCtx, name, metav1.GetOptions{})
		if err != nil {
			return corev1.Pod{}, errors.New("Kubernetes worker observation failed before bootstrap attach")
		}
		if string(pod.UID) != expectedUID {
			return corev1.Pod{}, errors.New("Kubernetes worker UID changed before bootstrap attach")
		}
		if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
			return corev1.Pod{}, errors.New("Kubernetes worker terminated before bootstrap attach")
		}
		for _, status := range pod.Status.ContainerStatuses {
			if status.Name == "runner" && status.State.Running != nil {
				return *pod, nil
			}
		}
		select {
		case <-waitCtx.Done():
			return corev1.Pod{}, errors.New("Kubernetes worker did not become attachable before its startup deadline")
		case <-ticker.C:
		}
	}
}

func (t *Transport) attachBootstrap(ctx context.Context, pod corev1.Pod, token bootstrap.Token) error {
	request := t.client.CoreV1().RESTClient().Post().Resource("pods").Namespace(pod.Namespace).Name(pod.Name).SubResource("attach")
	request.VersionedParams(&corev1.PodAttachOptions{Container: "runner", Stdin: true, Stdout: true, Stderr: false, TTY: false}, scheme.ParameterCodec)
	// Kubernetes 1.36's V5 WebSocket protocol can close stdin without waiting
	// for the runner process to exit. There is no fallback or retry once a
	// stream may have received the one-use capability.
	executor, err := remotecommand.NewWebSocketExecutor(t.config, "POST", request.URL().String())
	if err != nil {
		// Replaced, not wrapped. This error is constructed from the attach URL
		// that was just passed in, and now that deliverBootstrap's own
		// constants propagate to the controller log, returning it would put the
		// API server address and the exec query string there. A constant is the
		// only bound on that text worth trusting.
		return errors.New("bootstrap attach executor could not be constructed")
	}
	return deliverBootstrap(ctx, token, pod.Name, string(pod.UID), func(streamCtx context.Context, stdin io.Reader, stdout io.Writer) error {
		return executor.StreamWithContext(streamCtx, remotecommand.StreamOptions{Stdin: stdin, Stdout: stdout, Stderr: io.Discard, Tty: false})
	})
}

// deliverBootstrap proves the guest identity and an explicit ACK before the
// attach stream is canceled. StreamWithContext returning nil is insufficient:
// a server can close a stream before the worker consumes stdin. No retry is
// possible after the capability is written or an ACK is observed.
func deliverBootstrap(ctx context.Context, token bootstrap.Token, expectedName, expectedUID string, stream func(context.Context, io.Reader, io.Writer) error) error {
	return deliverBootstrapWithin(ctx, token, expectedName, expectedUID, stream, helloReplyTimeout)
}

// deliverBootstrapWithin takes the timeout as an argument so a test can pin it.
// A wait whose only setting is a package constant is one no test can exercise
// without sleeping for it, and a bound nobody watches expire is a bound nobody
// has seen work.
func deliverBootstrapWithin(ctx context.Context, token bootstrap.Token, expectedName, expectedUID string,
	stream func(context.Context, io.Reader, io.Writer) error, helloWithin time.Duration) error {
	if ctx == nil || stream == nil || expectedName == "" || expectedUID == "" {
		return errors.New("bootstrap attach requires context, stream, and identity")
	}
	if _, err := bootstrap.ParseToken(token.Encoded()); err != nil {
		return errors.New("bootstrap attach requires a valid one-use capability")
	}
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stdinReader, stdinWriter := io.Pipe()
	stdoutReader, stdoutWriter := io.Pipe()
	streamDone := make(chan error, 1)
	// streamClosed exists so the bounded helpers can notice the stream has
	// ended WITHOUT consuming streamDone, which the classifier below still
	// needs. A helper that read streamDone directly would swallow the one
	// value describing why the attach failed.
	streamClosed := make(chan struct{})
	go func() {
		err := stream(streamCtx, stdinReader, stdoutWriter)
		_ = stdoutWriter.CloseWithError(err)
		streamDone <- err
		close(streamClosed)
	}()
	closeWithCancel := func() {
		cancel()
		_ = stdinWriter.Close()
		_ = stdoutReader.Close()
	}
	defer closeWithCancel()
	reader := bufio.NewReaderSize(stdoutReader, 512)
	// THE CONTROLLER SPEAKS FIRST, and that is the fix rather than a style
	// choice. The worker used to print its identity once at startup and then
	// block on read; the controller attaches roughly ten seconds later, after
	// waitForRunner and the physical proof. A Kubernetes attach DOES NOT REPLAY
	// output emitted before the attach -- proven on the cluster with a
	// throwaway Kata pod whose stdout said `HELLO probe` in `kubectl logs` and
	// nothing at all on the attach stream. So the controller waited forever for
	// a line that had already been said, and every worker creation failed at
	// this step with `bootstrap attach outcome is uncertain`.
	//
	// Asking makes the handshake begin AFTER the stream exists, which is the
	// only ordering an attach can guarantee. The prompt carries no secret and
	// no identity: the worker still has to produce the name and UID, and this
	// side still compares them exactly, so the prompt cannot be replayed into a
	// forged identity.
	// BOUNDED, because an io.Pipe write blocks until someone reads it and a
	// stream that died before reading stdin would otherwise hold this call
	// until the ten-minute startup deadline. That is not hypothetical: it is
	// what the leak canary hit the moment the prompt was added, with a stream
	// that returned an error immediately. The same shape in production is an
	// executor that fails at connect.
	if err := writeBootstrapLineWithin(streamCtx, stdinWriter, helloPrompt+"\n", helloWithin, streamClosed); err != nil {
		// NOTHING SECRET HAS BEEN WRITTEN YET, so this is the one place the
		// stream's own failure may be described at all. It is described by
		// CLASS, never by text: classifyStreamFailure maps a known error to one
		// of a fixed set of words and everything else to `other`. Before the
		// class existed, an RBAC denial, a dead container and an unreachable
		// API server all arrived here as the same sentence, and the pipe write
		// that blocked behind the dead stream made each of them look like a
		// timeout.
		return errors.New("bootstrap attach identity prompt failed (stream=" +
			classifyStreamFailure(awaitStreamFailure(streamDone, streamClosed, streamClassGrace)) + ")")
	}
	hello, err := readBootstrapLineWithin(streamCtx, reader, helloWithin, streamClosed)
	if errors.Is(err, errBootstrapStreamEnded) || (errors.Is(err, errBootstrapLineTimeout) && streamEnded(streamClosed)) {
		// The prompt got out and the stream died before an answer came back.
		// That is a handshake failure, not a prompt failure, and the two are
		// worth keeping apart: one says nobody could be written to, the other
		// says the write landed and the stream then went away.
		return errors.New("bootstrap attach identity handshake failed (stream=" +
			classifyStreamFailure(awaitStreamFailure(streamDone, streamClosed, streamClassGrace)) + ")")
	}
	if errors.Is(err, errBootstrapLineTimeout) {
		// Distinguished from a handshake failure on purpose. A guest that never
		// answers is a worker problem -- a crashed entrypoint, an image without
		// the new protocol -- while a handshake failure is a wrong answer. They
		// send an operator to different places, and before the stage tags
		// existed both arrived as one sentence.
		return errors.New("bootstrap attach identity prompt was not answered in time")
	}
	if err != nil || hello != "HELLO "+expectedName+" "+expectedUID {
		return errors.New("bootstrap attach identity handshake failed")
	}
	// Bounded for the same reason. A guest that answered the prompt and then
	// stopped reading must not hold the capability write open indefinitely --
	// and this write is the one carrying the secret, so hanging here is the
	// worst place to hang.
	// NO CLASS FROM HERE DOWN. The capability is in flight, and a stream error
	// at this point can name what it was carrying. Everything after this line
	// stays exactly as opaque as it was.
	if err := writeBootstrapLineWithin(streamCtx, stdinWriter, "CAP "+token.Encoded()+"\n", helloWithin, streamClosed); err != nil {
		return errors.New("bootstrap attach capability write failed")
	}
	// BOUNDED, like every other step of this handshake. This read was the one
	// unbounded wait left: a guest that took the capability and then stopped
	// speaking held the attach -- and the lease -- until the ten-minute startup
	// deadline, which is the longest and least diagnosable way for a worker to
	// fail. The bound is the same one the rest of the handshake uses.
	//
	// STILL NO CLASS. The capability is already written, so the stream's own
	// error may not be described even by category: the reason it failed can name
	// what it was carrying. Every outcome here collapses to one constant, which
	// is why the error is discarded rather than inspected.
	ack, err := readBootstrapLineWithin(streamCtx, reader, helloWithin, streamClosed)
	if err != nil || ack != "ACK "+expectedName+" "+expectedUID {
		return errors.New("bootstrap attach acknowledgement failed")
	}
	// The ACK is the delivery witness. The worker continues from the shell
	// handoff, but the controller must not hold an attach stream open until the
	// job exits. A bounded close distinguishes successful handoff from a stuck
	// transport without replaying the capability.
	closeWithCancel()
	select {
	case <-streamDone:
		return nil
	case <-time.After(2 * time.Second):
		return errors.New("bootstrap attach close remained uncertain after ACK")
	}
}

// helloPrompt is what the controller writes first. A fixed token, not a
// nonce: it authenticates nothing and is not meant to. What authenticates the
// guest is the exact name and UID it must reply with, unchanged from before.
const helloPrompt = "HELLO?"

// helloReplyTimeout bounds the wait for the guest's identity. Thirty seconds is
// far longer than a healthy entrypoint needs (it is already blocked on read)
// and short enough that a worker which will never answer does not hold a lease
// until the ten-minute startup deadline.
const helloReplyTimeout = 30 * time.Second

// writeBootstrapLineWithin writes one handshake line or gives up. io.Pipe
// writes are synchronous, so a peer that is not reading blocks the writer for
// as long as it stays that way; nothing else in this handshake bounds that.
//
// The error is never returned to the caller's caller -- both call sites replace
// it with their own constant -- because on the capability write the bytes in
// flight ARE the secret, and a pipe error can name what it was writing.
func writeBootstrapLineWithin(ctx context.Context, writer io.Writer, line string, within time.Duration,
	streamClosed <-chan struct{}) error {
	done := make(chan error, 1)
	go func() {
		_, err := io.WriteString(writer, line)
		done <- err
	}()
	timer := time.NewTimer(within)
	defer timer.Stop()
	select {
	case err := <-done:
		return err
	case <-streamClosed:
		// FAIL FAST. A stream that has already returned will never read stdin,
		// so waiting out the full bound buys nothing and costs a lease thirty
		// seconds. That is not hypothetical: an RBAC denial returned in
		// milliseconds and the write behind it burned the whole timeout, which
		// is what made a permission error look like a silent guest.
		return errBootstrapStreamEnded
	case <-timer.C:
		return errBootstrapLineTimeout
	case <-ctx.Done():
		return ctx.Err()
	}
}

// errBootstrapStreamEnded means the attach stream returned before the handshake
// finished. It is a sentinel for control flow inside this file and is never
// returned to a caller: both call sites replace it with their own constant.
var errBootstrapStreamEnded = errors.New("bootstrap attach stream ended before the handshake")

// streamClassGrace is how long a failing handshake waits for the stream's own
// error before classifying it. The write and the stream can lose a race by
// microseconds; without a grace the class would read `none` for exactly the
// failures worth naming.
const streamClassGrace = 2 * time.Second

func streamEnded(streamClosed <-chan struct{}) bool {
	select {
	case <-streamClosed:
		return true
	default:
		return false
	}
}

// awaitStreamFailure returns the stream goroutine's error, or nil if it has not
// produced one within the grace. It consumes streamDone, so it must be called
// at most once per handshake -- every caller is a terminal return.
func awaitStreamFailure(streamDone <-chan error, streamClosed <-chan struct{}, grace time.Duration) error {
	if !streamEnded(streamClosed) {
		timer := time.NewTimer(grace)
		defer timer.Stop()
		select {
		case <-streamClosed:
		case <-timer.C:
			return nil
		}
	}
	select {
	case err := <-streamDone:
		return err
	default:
		return nil
	}
}

// classifyStreamFailure maps an attach stream error to ONE WORD from a closed
// set. No part of the error's own text is ever returned.
//
// This is an allowlist by construction rather than a denylist: an error that
// matches nothing known is `other`, so a new failure mode can only ever be
// under-described, never leaked. The status code is the single exception and it
// is bounded to three digits parsed as an integer, which cannot carry a URL, a
// header or a body.
//
// The vocabulary, and what each one sends an operator to look at:
//
//	none                   the stream had not failed yet; suspect the guest
//	forbidden              RBAC: the ServiceAccount lacks the attach verb
//	not-found              the Pod or container is gone
//	container-not-running  the runner container is not running yet, or exited
//	http-NNN               an upgrade refused with that status
//	timeout                the dial or upgrade timed out
//	tls                    certificate or handshake failure to the API server
//	refused                nothing accepted the connection
//	closed                 the peer closed mid-stream
//	other                  none of the above
func classifyStreamFailure(err error) string {
	if err == nil {
		return "none"
	}
	if status := apierrors.APIStatus(nil); errors.As(err, &status) {
		switch code := status.Status().Code; {
		case code == 403:
			return "forbidden"
		case code == 404:
			return "not-found"
		case code >= 100 && code <= 599:
			return "http-" + strconv.Itoa(int(code))
		}
	}
	text := strings.ToLower(err.Error())
	// Ordered most specific first: a denial's text also contains "cannot", and
	// an upgrade failure's text also contains "connection".
	for _, known := range []struct {
		substring string
		class     string
	}{
		{"cannot get resource", "forbidden"},
		{"cannot create resource", "forbidden"},
		{"forbidden", "forbidden"},
		{"is not running", "container-not-running"},
		{"container not found", "container-not-running"},
		{"not found", "not-found"},
		{"certificate", "tls"},
		{"tls", "tls"},
		{"x509", "tls"},
		{"connection refused", "refused"},
		{"timeout", "timeout"},
		{"deadline exceeded", "timeout"},
		{"connection reset", "closed"},
		{"use of closed", "closed"},
		{"unexpected eof", "closed"},
		{"eof", "closed"},
	} {
		if strings.Contains(text, known.substring) {
			return known.class
		}
	}
	if code := upgradeStatusCode(text); code != 0 {
		return "http-" + strconv.Itoa(code)
	}
	return "other"
}

// upgradeStatusCode pulls a three-digit HTTP status out of a websocket upgrade
// failure. It returns an INTEGER, so nothing from the error text can reach the
// log even when the match succeeds.
func upgradeStatusCode(text string) int {
	marker := "bad handshake status "
	index := strings.Index(text, marker)
	if index < 0 {
		return 0
	}
	digits := text[index+len(marker):]
	if len(digits) < 3 {
		return 0
	}
	code, err := strconv.Atoi(digits[:3])
	if err != nil || code < 100 || code > 599 {
		return 0
	}
	return code
}

var errBootstrapLineTimeout = errors.New("bootstrap handshake line did not arrive in time")

// readBootstrapLineWithin reads one handshake line or gives up. The read runs
// in its own goroutine because bufio.Reader has no deadline; the goroutine is
// left to finish on the pipe, which the deferred close always tears down.
func readBootstrapLineWithin(ctx context.Context, reader *bufio.Reader, within time.Duration,
	streamClosed <-chan struct{}) (string, error) {
	type result struct {
		line string
		err  error
	}
	done := make(chan result, 1)
	go func() {
		line, err := readBootstrapLine(reader)
		done <- result{line: line, err: err}
	}()
	timer := time.NewTimer(within)
	defer timer.Stop()
	select {
	case got := <-done:
		// The read usually unblocks on its own when the stream dies, because
		// the goroutine closes the stdout pipe with the stream's error. That
		// error is the raw one and never propagates; the caller replaces it.
		return got.line, got.err
	case <-streamClosed:
		return "", errBootstrapStreamEnded
	case <-timer.C:
		return "", errBootstrapLineTimeout
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// maxBootstrapLine bounds one handshake line. Every line this protocol has is a
// fixed word plus a Pod name and a UID, so 256 is generous by an order of
// magnitude.
const maxBootstrapLine = 256

// readBootstrapLine reads one handshake line, BOUNDED AS IT READS.
//
// It used to call ReadString and check the length afterwards. That bounds the
// RESULT, not the read: ReadString grows its own buffer until it finds a
// newline, so a peer that sends no newline at all makes this allocate without
// limit before any check runs. The bound has to be the loop's, not a test on
// what the loop produced.
func readBootstrapLine(reader *bufio.Reader) (string, error) {
	var line strings.Builder
	for line.Len() <= maxBootstrapLine {
		character, err := reader.ReadByte()
		if err != nil {
			return "", err
		}
		if character == '\n' {
			value := strings.TrimSuffix(line.String(), "\r")
			if len(value) == 0 {
				return "", errors.New("bootstrap handshake line exceeds bound")
			}
			return value, nil
		}
		if err := line.WriteByte(character); err != nil {
			return "", err
		}
	}
	return "", errors.New("bootstrap handshake line exceeds bound")
}

func (t *Transport) resourceFromPod(pod corev1.Pod) (WorkerResource, error) {
	if pod.Namespace != t.worker.Namespace || pod.UID == "" || pod.Name == "" {
		return WorkerResource{}, errors.New("Kubernetes Pod has incomplete identity")
	}
	if err := t.validateObservedPod(pod); err != nil {
		return WorkerResource{}, err
	}
	item := WorkerResource{Namespace: pod.Namespace, Name: pod.Name, UID: string(pod.UID),
		Labels: cloneStrings(pod.Labels), Annotations: cloneStrings(pod.Annotations), PodExists: true}
	if pod.Spec.RuntimeClassName != nil {
		item.RuntimeClassName = *pod.Spec.RuntimeClassName
	}
	// Only the `runner` container's own Terminated state counts. Pod phase is
	// not used: a Pod can be Running with its runner container already finished,
	// and Succeeded/Failed also cover a Pod the kubelet tore down for reasons
	// that say nothing about whether the runner ever took a job.
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name != "runner" || status.State.Terminated == nil {
			continue
		}
		item.RunnerTerminated = true
		item.RunnerExitCode = int(status.State.Terminated.ExitCode)
	}
	if err := t.validateWorkerIdentity(item); err != nil {
		return WorkerResource{}, err
	}
	return item, nil
}

func (t *Transport) validateObservedPod(pod corev1.Pod) error {
	if !sameStrings(pod.Spec.NodeSelector, map[string]string{"kubernetes.io/hostname": t.worker.NodeName, "kubernetes.io/arch": "amd64"}) ||
		(pod.Spec.NodeName != "" && pod.Spec.NodeName != t.worker.NodeName) ||
		pod.Spec.RuntimeClassName == nil || *pod.Spec.RuntimeClassName != t.worker.RuntimeClassName ||
		pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken ||
		pod.Spec.EnableServiceLinks == nil || *pod.Spec.EnableServiceLinks ||
		pod.Spec.HostNetwork || pod.Spec.HostPID || pod.Spec.HostIPC ||
		pod.Spec.RestartPolicy != corev1.RestartPolicyNever || len(pod.Spec.InitContainers) != 0 ||
		len(pod.Spec.EphemeralContainers) != 0 || len(pod.Spec.ImagePullSecrets) != 0 ||
		(pod.Spec.ShareProcessNamespace != nil && *pod.Spec.ShareProcessNamespace) {
		return errors.New("Kubernetes Pod violates the worker isolation boundary")
	}
	if pod.Spec.SecurityContext == nil || pod.Spec.SecurityContext.SeccompProfile == nil ||
		pod.Spec.SecurityContext.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		return errors.New("Kubernetes Pod lacks the required RuntimeDefault seccomp profile")
	}
	// Three volumes, not four. A worker still carrying the retired `docker`
	// volume fails here, List returns Complete: false and the controller fences
	// admission; that is why the homelab#983 cutover and its revert both run on
	// a drained pool rather than letting this validator tolerate either shape.
	if len(pod.Spec.Volumes) != 3 {
		return errors.New("Kubernetes Pod has an unexpected volume set")
	}
	expectedVolumes := workerVolumeSizes(t.worker.Volumes)
	for _, volume := range pod.Spec.Volumes {
		expectedSize, ok := expectedVolumes[volume.Name]
		if !ok || volume.EmptyDir == nil || volume.EmptyDir.Medium != corev1.StorageMediumMemory ||
			volume.EmptyDir.SizeLimit == nil || volume.EmptyDir.SizeLimit.Value() != expectedSize {
			return errors.New("Kubernetes Pod contains an unapproved or disk-backed volume")
		}
		delete(expectedVolumes, volume.Name)
	}
	if len(expectedVolumes) != 0 {
		return errors.New("Kubernetes Pod is missing a required memory-backed volume")
	}
	if len(pod.Spec.Containers) != 2 {
		return errors.New("Kubernetes Pod has an unexpected container set")
	}
	seen := make(map[string]bool, len(pod.Spec.Containers))
	for _, container := range pod.Spec.Containers {
		if seen[container.Name] {
			return errors.New("Kubernetes Pod contains duplicate container identity")
		}
		seen[container.Name] = true
		switch container.Name {
		case "runner":
			if err := t.validateObservedRunner(container, pod.Labels[LabelLeaseID]); err != nil {
				return err
			}
		case "dockerd":
			if err := t.validateObservedDocker(container); err != nil {
				return err
			}
		default:
			return errors.New("Kubernetes Pod contains an unexpected container")
		}
	}
	if !seen["runner"] || !seen["dockerd"] {
		return errors.New("Kubernetes Pod lacks runner or Docker container")
	}
	return nil
}

// workerVolumeSizes is the exact memory-backed volume set a worker carries,
// by name and size. The create check and the observed-Pod check both use it;
// the agreement test compares the policy's set to podSpec's.
func workerVolumeSizes(volumes VolumeBudget) map[string]int64 {
	return map[string]int64{"work": volumes.Work, "socket": volumes.Socket, "tmp": volumes.Tmp}
}

// coversDockerDataRoot reports whether a mount at path would hide or replace the
// dockerd data root: the root itself, anything under it, or any ancestor of it
// (a mount at /var/lib shadows /var/lib/sjo-docker just as surely).
// validateCreateSpec checks mount paths with it. validateObservedPod does not
// need it: its exact per-container mount maps already refuse any mount the
// controller did not build, the data root included.
func coversDockerDataRoot(path string) bool {
	clean := filepath.Clean(path)
	return clean == "/" || clean == dockerDataRoot ||
		strings.HasPrefix(clean, dockerDataRoot+"/") ||
		strings.HasPrefix(dockerDataRoot, clean+"/")
}

// runnerFieldEnv is the runner container's exact downward-API identity env set,
// shared between toKubernetesPod (injection) and validateObservedRunner
// (verification) so the two sides cannot drift (homelab#730).
func runnerFieldEnv() []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: "SLIPWAY_BOOTSTRAP_EXPECTED_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
		{Name: "SLIPWAY_BOOTSTRAP_EXPECTED_UID", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.uid"}}},
	}
}

func sameEnvVars(got, want []EnvVar) bool {
	if len(got) != len(want) {
		return false
	}
	wantMap := make(map[string]string, len(want))
	for _, item := range want {
		wantMap[item.Name] = item.Value
	}
	seen := make(map[string]bool, len(got))
	for _, item := range got {
		if item.Name == "" || seen[item.Name] {
			return false
		}
		expected, ok := wantMap[item.Name]
		if !ok || item.Value != expected {
			return false
		}
		seen[item.Name] = true
	}
	return len(seen) == len(wantMap)
}

func (t *Transport) validateObservedRunner(container corev1.Container, leaseID string) error {
	expectedPlain := make(map[string]string, 2)
	for _, item := range runnerContainerEnv(t.worker.RunnerRoot) {
		expectedPlain[item.Name] = item.Value
	}
	expectedFields := runnerFieldEnv()
	if container.Image != t.worker.RunnerImage || !sameStrings(envNames(container.Env), expectedPlain) ||
		len(container.Env) != len(expectedPlain)+len(expectedFields) || !uniqueEnvNames(container.Env) || len(container.EnvFrom) != 0 ||
		!sameStrings(mountNames(container.VolumeMounts), map[string]string{
			"work": filepath.Join(t.worker.RunnerRoot, "_work"), "socket": "/var/run", "tmp": "/tmp",
		}) || len(container.VolumeMounts) != 3 || len(container.VolumeDevices) != 0 {
		return errors.New("Kubernetes runner container does not match the approved worker shape")
	}
	if !sameStringsSlice(container.Command, []string{workerBootstrapCommand}) ||
		!sameStringsSlice(container.Args, []string{t.worker.WorkerCommand, "-controller", t.worker.BootstrapURL,
			"-lease", leaseID, "-runner-root", t.worker.RunnerRoot, "-ca-file", t.worker.CAFile}) ||
		container.Stdin != true || container.StdinOnce != true || container.TTY || !validContainerSecurity(container, false, true, false) {
		return errors.New("Kubernetes runner container bootstrap or privilege shape changed")
	}
	for _, field := range expectedFields {
		if !hasFieldEnv(container.Env, field.Name, field.ValueFrom.FieldRef.FieldPath) {
			return errors.New("Kubernetes runner container lacks the downward identity fields")
		}
	}
	return nil
}

func (t *Transport) validateObservedDocker(container corev1.Container) error {
	if container.Image != t.worker.DockerImage ||
		!sameStringsSlice(container.Command, []string{"dockerd-entrypoint.sh"}) ||
		len(container.Args) != len(dockerdArgs()) || !sameStringsSlice(container.Args, dockerdArgs()) ||
		len(container.Env) != 0 || len(container.EnvFrom) != 0 || len(container.VolumeDevices) != 0 ||
		!sameStrings(mountNames(container.VolumeMounts), map[string]string{
			"socket": "/var/run", "work": filepath.Join(t.worker.RunnerRoot, "_work"), "tmp": "/tmp",
		}) || len(container.VolumeMounts) != 3 || container.Stdin || container.StdinOnce || container.TTY ||
		!validContainerSecurity(container, true, false, true) {
		return errors.New("Kubernetes Docker container does not match the approved worker shape")
	}
	return nil
}

func validContainerSecurity(container corev1.Container, privileged, runAsNonRoot, allowPrivilegeEscalation bool) bool {
	security := container.SecurityContext
	return security != nil && security.Privileged != nil && *security.Privileged == privileged &&
		security.RunAsNonRoot != nil && *security.RunAsNonRoot == runAsNonRoot &&
		security.AllowPrivilegeEscalation != nil && *security.AllowPrivilegeEscalation == allowPrivilegeEscalation &&
		security.ReadOnlyRootFilesystem != nil && !*security.ReadOnlyRootFilesystem
}

func envNames(env []corev1.EnvVar) map[string]string {
	values := make(map[string]string, len(env))
	for _, item := range env {
		if item.ValueFrom == nil {
			values[item.Name] = item.Value
		}
	}
	return values
}

func uniqueEnvNames(env []corev1.EnvVar) bool {
	seen := make(map[string]bool, len(env))
	for _, item := range env {
		if item.Name == "" || seen[item.Name] {
			return false
		}
		seen[item.Name] = true
	}
	return true
}

func hasFieldEnv(env []corev1.EnvVar, name, fieldPath string) bool {
	for _, item := range env {
		if item.Name == name && item.Value == "" && item.ValueFrom != nil && item.ValueFrom.FieldRef != nil &&
			item.ValueFrom.FieldRef.FieldPath == fieldPath {
			return true
		}
	}
	return false
}

func mountNames(mounts []corev1.VolumeMount) map[string]string {
	values := make(map[string]string, len(mounts))
	for _, item := range mounts {
		if !item.ReadOnly && item.MountPath != "" {
			values[item.Name] = item.MountPath
		}
	}
	return values
}

func sameStringsSlice(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range right {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func (t *Transport) validateWorkerIdentity(item WorkerResource) error {
	if item.Namespace != t.worker.Namespace || item.Name == "" || item.UID == "" || item.RuntimeClassName != t.worker.RuntimeClassName || len(item.Annotations) != 0 {
		return errors.New("Kubernetes Pod does not satisfy the immutable worker identity")
	}
	backend := &Backend{config: t.worker}
	if err := backend.validateIdentity(item.Labels[LabelLeaseID], item.Name); err != nil {
		return err
	}
	expected := backend.labels(item.Labels[LabelLeaseID], item.Name)
	if !sameStrings(item.Labels, expected) {
		return errors.New("Kubernetes Pod ownership labels do not match")
	}
	return nil
}

func (t *Transport) validatePhysical(item PhysicalWorker) error {
	return t.validateWorkerIdentity(WorkerResource{Namespace: t.worker.Namespace, Name: item.Name, UID: item.UID,
		Labels: item.Labels, RuntimeClassName: item.RuntimeClassName})
}

func (t *Transport) validateCreateSpec(spec PodSpec) error {
	if spec.Namespace != t.worker.Namespace || spec.Name == "" || spec.RuntimeClassName != t.worker.RuntimeClassName ||
		spec.AutomountServiceAccountToken || spec.EnableServiceLinks || spec.HostNetwork || spec.HostPID || spec.HostIPC ||
		spec.RestartPolicy != "Never" || spec.SeccompProfile != "RuntimeDefault" || len(spec.Annotations) != 0 {
		return errors.New("Kubernetes Pod spec does not satisfy the worker admission boundary")
	}
	if !sameStrings(spec.NodeSelector, map[string]string{"kubernetes.io/hostname": t.worker.NodeName, "kubernetes.io/arch": "amd64"}) {
		return errors.New("Kubernetes Pod spec does not select the approved MS-A2 node")
	}
	leaseID := spec.Labels[LabelLeaseID]
	if !sameStrings(spec.Labels, (&Backend{config: t.worker}).labels(leaseID, spec.Name)) {
		return errors.New("Kubernetes Pod spec ownership labels do not match")
	}
	if len(spec.Containers) != 2 || len(spec.Volumes) != 3 {
		return errors.New("Kubernetes Pod spec has an unexpected container or volume shape")
	}
	seenRunner, seenDocker := false, false
	for _, container := range spec.Containers {
		switch container.Name {
		case "runner":
			if seenRunner || container.Image != t.worker.RunnerImage || len(container.Command) != 1 || container.Command[0] != workerBootstrapCommand ||
				len(container.Args) == 0 || container.Args[0] != t.worker.WorkerCommand || container.Privileged || !container.RunAsNonRoot || container.AllowPrivilegeEscalation {
				return errors.New("Kubernetes runner container identity or privilege changed")
			}
			if !sameEnvVars(container.Environment, runnerContainerEnv(t.worker.RunnerRoot)) {
				return errors.New("Kubernetes runner container environment does not match the approved allowlist")
			}
			seenRunner = true
		case "dockerd":
			if seenDocker || container.Image != t.worker.DockerImage || !container.Privileged || len(container.Command) != 1 || container.Command[0] != "dockerd-entrypoint.sh" {
				return errors.New("Kubernetes Docker container identity or privilege changed")
			}
			if !sameEnvVars(container.Environment, dockerdContainerEnv()) {
				return errors.New("Kubernetes Docker container environment does not match the approved allowlist")
			}
			seenDocker = true
		default:
			return errors.New("Kubernetes Pod spec contains an unexpected container")
		}
		for _, mount := range container.Mounts {
			if coversDockerDataRoot(mount.MountPath) {
				return errors.New("Kubernetes Pod spec mounts a volume at or under the dockerd data root")
			}
		}
	}
	if !seenRunner || !seenDocker {
		return errors.New("Kubernetes Pod spec lacks runner or Docker container")
	}
	expectedVolumes := workerVolumeSizes(t.worker.Volumes)
	for _, volume := range spec.Volumes {
		expectedSize, ok := expectedVolumes[volume.Name]
		if !ok || volume.Medium != "Memory" || volume.SizeBytes != expectedSize {
			return errors.New("Kubernetes Pod spec contains an unbounded or disk-backed volume")
		}
		delete(expectedVolumes, volume.Name)
	}
	if len(expectedVolumes) != 0 {
		return errors.New("Kubernetes Pod spec is missing a required memory-backed volume")
	}
	return nil
}

func specContainsCapability(spec PodSpec, capability string) bool {
	if capability == "" {
		return false
	}
	contains := func(values ...string) bool {
		for _, value := range values {
			if strings.Contains(value, capability) {
				return true
			}
		}
		return false
	}
	for key, value := range spec.Labels {
		if contains(key, value) {
			return true
		}
	}
	for key, value := range spec.Annotations {
		if contains(key, value) {
			return true
		}
	}
	if contains(spec.Namespace, spec.Name, spec.RuntimeClassName) {
		return true
	}
	for key, value := range spec.NodeSelector {
		if contains(key, value) {
			return true
		}
	}
	for _, container := range spec.Containers {
		if contains(container.Name, container.Image) {
			return true
		}
		if contains(container.Command...) || contains(container.Args...) {
			return true
		}
		for _, env := range container.Environment {
			if contains(env.Name, env.Value) {
				return true
			}
		}
		for _, mount := range container.Mounts {
			if contains(mount.Name, mount.MountPath) {
				return true
			}
		}
	}
	for _, volume := range spec.Volumes {
		if contains(volume.Name, volume.Medium) {
			return true
		}
	}
	return false
}

func (t *Transport) toPod(spec PodSpec) *corev1.Pod {
	runtimeClass := spec.RuntimeClassName
	automount := spec.AutomountServiceAccountToken
	serviceLinks := spec.EnableServiceLinks
	grace := spec.TerminationGracePeriodSec
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: spec.Namespace, Name: spec.Name,
		Labels: cloneStrings(spec.Labels), Annotations: cloneStrings(spec.Annotations)},
		Spec: corev1.PodSpec{RuntimeClassName: &runtimeClass, NodeSelector: cloneStrings(spec.NodeSelector),
			AutomountServiceAccountToken: &automount, EnableServiceLinks: &serviceLinks,
			HostNetwork: spec.HostNetwork, HostPID: spec.HostPID, HostIPC: spec.HostIPC,
			RestartPolicy: corev1.RestartPolicyNever, TerminationGracePeriodSeconds: &grace,
			SecurityContext: &corev1.PodSecurityContext{SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}}},
	}
	for _, volume := range spec.Volumes {
		size := resource.NewQuantity(volume.SizeBytes, resource.BinarySI)
		pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: volume.Name,
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory, SizeLimit: size}}})
	}
	for _, container := range spec.Containers {
		converted := corev1.Container{Name: container.Name, Image: container.Image, Command: append([]string(nil), container.Command...),
			Args: append([]string(nil), container.Args...), ImagePullPolicy: corev1.PullIfNotPresent,
			Resources: toResourceRequirements(container.Resources), Stdin: container.Name == "runner", StdinOnce: container.Name == "runner",
			SecurityContext: &corev1.SecurityContext{Privileged: boolPtr(container.Privileged),
				RunAsNonRoot: boolPtr(container.RunAsNonRoot), AllowPrivilegeEscalation: boolPtr(container.AllowPrivilegeEscalation),
				ReadOnlyRootFilesystem: boolPtr(container.ReadOnlyRootFilesystem)}}
		for _, env := range container.Environment {
			converted.Env = append(converted.Env, corev1.EnvVar{Name: env.Name, Value: env.Value})
		}
		for _, mount := range container.Mounts {
			converted.VolumeMounts = append(converted.VolumeMounts, corev1.VolumeMount{Name: mount.Name, MountPath: mount.MountPath, ReadOnly: mount.ReadOnly})
		}
		if container.Name == "runner" {
			converted.Env = append(converted.Env, runnerFieldEnv()...)
		}
		pod.Spec.Containers = append(pod.Spec.Containers, converted)
	}
	return pod
}

func toResourceRequirements(resources ContainerResources) corev1.ResourceRequirements {
	return corev1.ResourceRequirements{Requests: toResourceList(resources.Requests), Limits: toResourceList(resources.Limits)}
}

func toResourceList(footprint Footprint) corev1.ResourceList {
	return corev1.ResourceList{corev1.ResourceCPU: *resource.NewMilliQuantity(footprint.CPUMilli, resource.DecimalSI),
		corev1.ResourceMemory:           *resource.NewQuantity(footprint.MemoryBytes, resource.BinarySI),
		corev1.ResourceEphemeralStorage: *resource.NewQuantity(footprint.StorageBytes, resource.BinarySI)}
}

func boolPtr(value bool) *bool { return &value }

func cloneStrings(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	copy := make(map[string]string, len(values))
	for key, value := range values {
		copy[key] = value
	}
	return copy
}

func sameStrings(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range right {
		if left[key] != value {
			return false
		}
	}
	return true
}

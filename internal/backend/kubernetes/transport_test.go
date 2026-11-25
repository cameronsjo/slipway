package kubernetes

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	k8stesting "k8s.io/client-go/testing"

	"github.com/cameronsjo/slipway/internal/bootstrap"
	"github.com/cameronsjo/slipway/internal/controller"
)

type witnessFixture struct {
	inventory PhysicalInventory
	headroom  Headroom
	absence   AbsenceWitness
	seenScope string
	waitCalls int
}

func (w *witnessFixture) Inventory(_ context.Context, _, selector string) (PhysicalInventory, error) {
	w.seenScope = selector
	return w.inventory, nil
}

func (w *witnessFixture) Headroom(context.Context, string, Footprint) (Headroom, error) {
	return w.headroom, nil
}

func (w *witnessFixture) WaitAbsent(context.Context, DeleteRequest) (AbsenceWitness, error) {
	w.waitCalls++
	return w.absence, nil
}

func newTransportFixture(t *testing.T) (*Transport, Config, controller.WorkerCreateIntent, bootstrap.Token) {
	t.Helper()
	config := testConfig()
	intent, token := testIntent(config)
	client := fake.NewSimpleClientset()
	witness := &witnessFixture{inventory: PhysicalInventory{Complete: true}, headroom: enoughHeadroom(config, 2)}
	transport, err := NewTransport(&rest.Config{Host: "https://kubernetes.default.svc"}, client, witness, config)
	if err != nil {
		t.Fatal(err)
	}
	return transport, config, intent, token
}

func TestDeliverBootstrapRequiresGuestIdentityAndAcknowledgement(t *testing.T) {
	token, err := bootstrap.NewToken()
	if err != nil {
		t.Fatal(err)
	}
	var received atomic.Value
	// Updated for the controller-speaks-first protocol. The old shape wrote
	// HELLO before reading anything, which now DEADLOCKS against an io.Pipe --
	// both sides blocked writing while the other waits to be read. That is a
	// test-harness artifact of the unbuffered pipe, not production behaviour,
	// but it is a useful reminder that the ordering is load-bearing.
	stream := func(ctx context.Context, stdin io.Reader, stdout io.Writer) error {
		reader := bufio.NewReader(stdin)
		prompt, err := reader.ReadString('\n')
		if err != nil {
			return err
		}
		if strings.TrimSpace(prompt) != helloPrompt {
			return errors.New("unexpected identity prompt")
		}
		if _, err := io.WriteString(stdout, "HELLO worker-1 pod-uid\n"); err != nil {
			return err
		}
		line, err := reader.ReadString('\n')
		if err != nil {
			return err
		}
		received.Store(strings.TrimSpace(line))
		if line != "CAP "+token.Encoded()+"\n" {
			return errors.New("unexpected capability")
		}
		if _, err := io.WriteString(stdout, "ACK worker-1 pod-uid\n"); err != nil {
			return err
		}
		<-ctx.Done()
		return ctx.Err()
	}
	if err := deliverBootstrap(context.Background(), token, "worker-1", "pod-uid", stream); err != nil {
		t.Fatal(err)
	}
	if got := received.Load(); got == nil || got.(string) != "CAP "+token.Encoded() {
		t.Fatal("capability was not delivered only after identity")
	}

	badIdentity := func(_ context.Context, stdin io.Reader, stdout io.Writer) error {
		_, _ = bufio.NewReader(stdin).ReadString('\n')
		_, _ = io.WriteString(stdout, "HELLO other pod-uid\n")
		return nil
	}
	if err := deliverBootstrap(context.Background(), token, "worker-1", "pod-uid", badIdentity); err == nil {
		t.Fatal("accepted a bootstrap stream for another identity")
	}
}

func TestTransportListPaginatesAndMergesPhysicalWitness(t *testing.T) {
	transport, config, intent, _ := newTransportFixture(t)
	backend := &Backend{config: config}
	podOne := transport.toPod(backend.podSpec(intent))
	podOne.UID = "pod-uid-1"
	otherIntent, _ := testIntent(config)
	podTwo := transport.toPod(backend.podSpec(otherIntent))
	podTwo.UID = "pod-uid-2"
	orphan := PhysicalWorker{Name: "slipway-linux-orphan", UID: "pod-uid-orphan", Labels: backend.labels(testUUID(), "slipway-linux-orphan"), RuntimeClassName: config.RuntimeClassName, ProcessPresent: true, StoragePresent: true}
	witness := transport.witness.(*witnessFixture)
	witness.inventory.Workers = []PhysicalWorker{
		{Name: string(podOne.Name), UID: string(podOne.UID), Labels: podOne.Labels, RuntimeClassName: config.RuntimeClassName, ProcessPresent: true, StoragePresent: true},
		{Name: string(podTwo.Name), UID: string(podTwo.UID), Labels: podTwo.Labels, RuntimeClassName: config.RuntimeClassName},
		orphan,
	}
	client := transport.client.(*fake.Clientset)
	var calls atomic.Int32
	client.PrependReactor("list", "pods", func(_ k8stesting.Action) (bool, runtime.Object, error) {
		if calls.Add(1) == 1 {
			return true, &corev1.PodList{ListMeta: metav1.ListMeta{Continue: "next-page"}, Items: []corev1.Pod{*podOne}}, nil
		}
		return true, &corev1.PodList{Items: []corev1.Pod{*podTwo}}, nil
	})
	result, err := transport.List(context.Background(), config.Namespace, backend.Selector())
	if err != nil {
		t.Fatal(err)
	}
	if !result.Complete || calls.Load() != 2 || len(result.Items) != 3 || witness.seenScope != backend.Selector() {
		t.Fatalf("incomplete merged inventory: %+v calls=%d scope=%q", result, calls.Load(), witness.seenScope)
	}
	for _, item := range result.Items {
		if item.Name == orphan.Name && (item.PodExists || !item.ProcessPresent || !item.StoragePresent) {
			t.Fatal("orphan physical worker was not preserved as a physical observation")
		}
	}
}

// orderedWitness records that its Inventory ran, so a Pod list reactor can
// model a Pod created at that moment.
type orderedWitness struct {
	witnessFixture
	read atomic.Bool
}

func (w *orderedWitness) Inventory(ctx context.Context, namespace, selector string) (PhysicalInventory, error) {
	w.read.Store(true)
	return w.witnessFixture.Inventory(ctx, namespace, selector)
}

// THE WITNESS IS READ BEFORE THE PODS (#1109 review). A Pod created between the
// two reads is modelled here: the witness sees its process, and only a Pod list
// taken after the witness read contains it. Listed in that order, the Pod is
// live and is not an orphan. Listed the other way round, it came back
// PodExists:false, which Inventory reads as a Pod-less worker to retire.
func TestTransportListReadsTheWitnessBeforeThePods(t *testing.T) {
	config := testConfig()
	intent, _ := testIntent(config)
	backend := &Backend{config: config}
	witness := &orderedWitness{witnessFixture: witnessFixture{inventory: PhysicalInventory{Complete: true}, headroom: enoughHeadroom(config, 2)}}
	client := fake.NewSimpleClientset()
	transport, err := NewTransport(&rest.Config{Host: "https://kubernetes.default.svc"}, client, witness, config)
	if err != nil {
		t.Fatal(err)
	}
	pod := transport.toPod(backend.podSpec(intent))
	pod.UID = "pod-uid-new"
	witness.inventory.Workers = []PhysicalWorker{{Name: pod.Name, UID: string(pod.UID), Labels: pod.Labels,
		RuntimeClassName: config.RuntimeClassName, ProcessPresent: true, StoragePresent: true}}
	client.PrependReactor("list", "pods", func(_ k8stesting.Action) (bool, runtime.Object, error) {
		if !witness.read.Load() {
			return true, &corev1.PodList{}, nil
		}
		return true, &corev1.PodList{Items: []corev1.Pod{*pod}}, nil
	})
	result, err := transport.List(context.Background(), config.Namespace, backend.Selector())
	if err != nil {
		t.Fatal(err)
	}
	if !result.Complete || len(result.Items) != 1 {
		t.Fatalf("unexpected listing: %+v", result)
	}
	if item := result.Items[0]; !item.PodExists || !item.ProcessPresent {
		t.Fatalf("a Pod created before the Pod list was reported as an orphan: %+v", item)
	}
}

func TestTransportListRejectsIsolationMutation(t *testing.T) {
	transport, config, intent, _ := newTransportFixture(t)
	backend := &Backend{config: config}
	pod := transport.toPod(backend.podSpec(intent))
	pod.UID = "pod-uid-mutated"
	pod.Spec.HostNetwork = true
	client := transport.client.(*fake.Clientset)
	client.PrependReactor("list", "pods", func(_ k8stesting.Action) (bool, runtime.Object, error) {
		return true, &corev1.PodList{Items: []corev1.Pod{*pod}}, nil
	})
	result, err := transport.List(context.Background(), config.Namespace, backend.Selector())
	if err == nil || result.Complete {
		t.Fatal("accepted a worker Pod with host networking enabled")
	}
}

func TestTransportCreateStreamsCapabilityWithoutPuttingItInPodObject(t *testing.T) {
	transport, config, intent, token := newTransportFixture(t)
	backend := &Backend{config: config}
	spec := backend.podSpec(intent)
	client := transport.client.(*fake.Clientset)
	var created *corev1.Pod
	client.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		created = action.(k8stesting.CreateAction).GetObject().(*corev1.Pod).DeepCopy()
		created.UID = "pod-uid-create"
		created.Status.Phase = corev1.PodRunning
		created.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "runner", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
		return true, created, nil
	})
	transport.waitRunner = func(_ context.Context, _, _, expectedUID string, _ time.Time) (corev1.Pod, error) {
		if created == nil || string(created.UID) != expectedUID {
			return corev1.Pod{}, errors.New("wrong created identity")
		}
		return *created, nil
	}
	transport.witness.(*witnessFixture).inventory.Workers = []PhysicalWorker{{Name: intent.ResourceID, UID: "pod-uid-create",
		Labels: backend.labels(intent.LeaseID, intent.ResourceID), RuntimeClassName: config.RuntimeClassName,
		ProcessPresent: true, StoragePresent: true}}
	var attached bootstrap.Token
	transport.attach = func(_ context.Context, pod corev1.Pod, got bootstrap.Token) error {
		if string(pod.UID) != "pod-uid-create" {
			return errors.New("wrong attach identity")
		}
		attached = got
		return nil
	}
	got, err := transport.Create(context.Background(), CreateRequest{Spec: spec, BootstrapMode: MemoryOnlyBootstrap,
		BootstrapToken: token, StartupDeadline: time.Now().Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != intent.ResourceID || attached != token || created == nil {
		t.Fatalf("unexpected create result: %+v attached=%v pod=%v", got, attached == token, created != nil)
	}
	raw, err := json.Marshal(created)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), token.Encoded()) {
		t.Fatal("bootstrap capability entered the Pod object")
	}
	for _, container := range created.Spec.Containers {
		if container.Name != "runner" {
			continue
		}
		if !container.Stdin || !container.StdinOnce || len(container.Command) != 1 || container.Command[0] != workerBootstrapCommand {
			t.Fatal("runner container did not expose the bounded stdin bootstrap wrapper")
		}
		for _, env := range container.Env {
			if env.Name == "SLIPWAY_BOOTSTRAP_TOKEN" || env.Value == token.Encoded() {
				t.Fatal("bootstrap capability entered the Pod environment")
			}
		}
	}
}

func TestTransportCreateDoesNotDeliverCapabilityBeforePhysicalProof(t *testing.T) {
	transport, config, intent, token := newTransportFixture(t)
	backend := &Backend{config: config}
	client := transport.client.(*fake.Clientset)
	client.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		pod := action.(k8stesting.CreateAction).GetObject().(*corev1.Pod).DeepCopy()
		pod.UID = "pod-uid-unproven"
		return true, pod, nil
	})
	transport.waitRunner = func(_ context.Context, _, _, _ string, _ time.Time) (corev1.Pod, error) {
		pod := transport.toPod(backend.podSpec(intent))
		pod.UID = "pod-uid-unproven"
		return *pod, nil
	}
	attached := false
	transport.attach = func(context.Context, corev1.Pod, bootstrap.Token) error {
		attached = true
		return nil
	}
	for name, physical := range map[string]PhysicalInventory{
		"missing": {Complete: true},
		"no process": {Complete: true, Workers: []PhysicalWorker{{Name: intent.ResourceID, UID: "pod-uid-unproven",
			Labels: backend.labels(intent.LeaseID, intent.ResourceID), RuntimeClassName: config.RuntimeClassName, StoragePresent: true}}},
		"no storage": {Complete: true, Workers: []PhysicalWorker{{Name: intent.ResourceID, UID: "pod-uid-unproven",
			Labels: backend.labels(intent.LeaseID, intent.ResourceID), RuntimeClassName: config.RuntimeClassName, ProcessPresent: true}}},
	} {
		t.Run(name, func(t *testing.T) {
			transport.witness.(*witnessFixture).inventory = physical
			if _, err := transport.Create(context.Background(), CreateRequest{Spec: backend.podSpec(intent), BootstrapMode: MemoryOnlyBootstrap,
				BootstrapToken: token, StartupDeadline: time.Now().Add(time.Minute)}); err == nil {
				t.Fatal("worker capability was delivered without complete physical proof")
			}
			if attached {
				t.Fatal("attach ran before physical proof")
			}
		})
	}
}

func TestTransportDeleteChecksUIDAndDelegatesPhysicalAbsence(t *testing.T) {
	transport, config, intent, _ := newTransportFixture(t)
	backend := &Backend{config: config}
	pod := transport.toPod(backend.podSpec(intent))
	pod.UID = "pod-uid-delete"
	client := transport.client.(*fake.Clientset)
	client.PrependReactor("get", "pods", func(_ k8stesting.Action) (bool, runtime.Object, error) { return true, pod, nil })
	var deleted atomic.Int32
	var options metav1.DeleteOptions
	client.PrependReactor("delete", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		deleted.Add(1)
		options = action.(k8stesting.DeleteAction).GetDeleteOptions()
		return true, nil, nil
	})
	witness := transport.witness.(*witnessFixture)
	witness.absence = AbsenceWitness{UID: string(pod.UID), ProcessAbsent: true, StorageAbsent: true}
	absence, err := transport.Delete(context.Background(), DeleteRequest{Namespace: config.Namespace, Name: intent.ResourceID,
		UID: string(pod.UID), Labels: pod.Labels})
	if err != nil || !absence.ProcessAbsent || !absence.StorageAbsent || deleted.Load() != 1 {
		t.Fatalf("delete did not produce exact absence witness: %+v err=%v count=%d", absence, err, deleted.Load())
	}
	if options.Preconditions == nil || options.Preconditions.UID == nil || string(*options.Preconditions.UID) != string(pod.UID) {
		t.Fatal("delete did not carry a UID precondition")
	}
}

func TestTransportDeleteRequiresPhysicalWitnessWhenPodIsAlreadyGone(t *testing.T) {
	transport, config, intent, _ := newTransportFixture(t)
	backend := &Backend{config: config}
	witness := transport.witness.(*witnessFixture)
	witness.absence = AbsenceWitness{UID: "pod-uid-gone", ProcessAbsent: true, StorageAbsent: true}
	request := DeleteRequest{Namespace: config.Namespace, Name: intent.ResourceID, UID: "pod-uid-gone",
		Labels: backend.labels(intent.LeaseID, intent.ResourceID)}
	absence, err := transport.Delete(context.Background(), request)
	if err != nil || !absence.ProcessAbsent || !absence.StorageAbsent || witness.waitCalls != 1 {
		t.Fatalf("missing Pod did not require exact physical absence: %+v calls=%d err=%v", absence, witness.waitCalls, err)
	}
}

func testUUID() string {
	return "00000000-0000-4000-8000-000000000001"
}

// --- the Create path's leak canary and its stage tags -----------------------
//
// Creation is the ONE path that handles the single-use bootstrap capability.
// Wrapping its causes (#849 diagnostics) means an error that used to be a
// constant can now carry a cause, so the two places a cause could quote the
// token are pinned here rather than argued about: the Pod API create, whose
// request body is a worker Pod spec, and the ATTACH, whose bytes in flight are
// the capability itself.

func TestCreateStageTagsNameTheFailingOperation(t *testing.T) {
	transport, config, intent, token := newTransportFixture(t)
	backend := &Backend{config: config}
	client := transport.client.(*fake.Clientset)
	client.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		pod := action.(k8stesting.CreateAction).GetObject().(*corev1.Pod).DeepCopy()
		pod.UID = "pod-uid-staged"
		return true, pod, nil
	})
	transport.waitRunner = func(_ context.Context, _, _, _ string, _ time.Time) (corev1.Pod, error) {
		pod := transport.toPod(backend.podSpec(intent))
		pod.UID = "pod-uid-staged"
		return *pod, nil
	}
	transport.attach = func(context.Context, corev1.Pod, bootstrap.Token) error {
		return errors.New("stream closed")
	}

	// physical-worker: the witness has nothing.
	transport.witness.(*witnessFixture).inventory = PhysicalInventory{Complete: true}
	_, err := transport.Create(context.Background(), CreateRequest{Spec: backend.podSpec(intent),
		BootstrapMode: MemoryOnlyBootstrap, BootstrapToken: token, StartupDeadline: time.Now().Add(time.Minute)})
	if err == nil || !strings.Contains(err.Error(), "create stage="+createStagePhysicalWorker) {
		t.Fatalf("a physical-proof failure is not tagged: %v", err)
	}

	// attach: physical proof passes, the stream does not.
	transport.witness.(*witnessFixture).inventory = PhysicalInventory{Complete: true,
		Workers: []PhysicalWorker{{Name: intent.ResourceID, UID: "pod-uid-staged",
			Labels: backend.labels(intent.LeaseID, intent.ResourceID), RuntimeClassName: config.RuntimeClassName,
			ProcessPresent: true, StoragePresent: true}}}
	_, err = transport.Create(context.Background(), CreateRequest{Spec: backend.podSpec(intent),
		BootstrapMode: MemoryOnlyBootstrap, BootstrapToken: token, StartupDeadline: time.Now().Add(time.Minute)})
	if err == nil || !strings.Contains(err.Error(), "create stage="+createStageAttach) {
		t.Fatalf("an attach failure is not tagged: %v", err)
	}
}

func TestThePhysicalProofSaysWhichOfTheThreeFailed(t *testing.T) {
	// The line that settles a live failure in one read. Before this, a UID
	// disagreement, an unseen Kata process and an unmatched writable snapshot
	// all produced the same sentence -- and they send an operator to three
	// different places.
	transport, config, intent, token := newTransportFixture(t)
	backend := &Backend{config: config}
	client := transport.client.(*fake.Clientset)
	client.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		pod := action.(k8stesting.CreateAction).GetObject().(*corev1.Pod).DeepCopy()
		pod.UID = "pod-uid-proof"
		return true, pod, nil
	})
	transport.waitRunner = func(_ context.Context, _, _, _ string, _ time.Time) (corev1.Pod, error) {
		pod := transport.toPod(backend.podSpec(intent))
		pod.UID = "pod-uid-proof"
		return *pod, nil
	}
	transport.attach = func(context.Context, corev1.Pod, bootstrap.Token) error { return nil }

	base := PhysicalWorker{Name: intent.ResourceID, UID: "pod-uid-proof",
		Labels: backend.labels(intent.LeaseID, intent.ResourceID), RuntimeClassName: config.RuntimeClassName,
		ProcessPresent: true, StoragePresent: true}

	for name, expected := range map[string]string{
		"no process": "process_present=false",
		"no storage": "storage_present=false",
		"wrong uid":  "uid_matches=false",
	} {
		t.Run(name, func(t *testing.T) {
			item := base
			switch name {
			case "no process":
				item.ProcessPresent = false
			case "no storage":
				item.StoragePresent = false
			case "wrong uid":
				item.UID = "pod-uid-somethingelse"
			}
			transport.witness.(*witnessFixture).inventory = PhysicalInventory{Complete: true, Workers: []PhysicalWorker{item}}
			_, err := transport.Create(context.Background(), CreateRequest{Spec: backend.podSpec(intent),
				BootstrapMode: MemoryOnlyBootstrap, BootstrapToken: token, StartupDeadline: time.Now().Add(time.Minute)})
			if err == nil {
				t.Fatal("incomplete physical identity was accepted")
			}
			if !strings.Contains(err.Error(), expected) {
				t.Fatalf("refusal does not name %s: %v", expected, err)
			}
		})
	}

	// The positive control: with all three satisfied, Create succeeds. Without
	// it, a requirePhysicalWorker that refused everything would pass all three
	// cases above.
	transport.witness.(*witnessFixture).inventory = PhysicalInventory{Complete: true, Workers: []PhysicalWorker{base}}
	if _, err := transport.Create(context.Background(), CreateRequest{Spec: backend.podSpec(intent),
		BootstrapMode: MemoryOnlyBootstrap, BootstrapToken: token, StartupDeadline: time.Now().Add(time.Minute)}); err != nil {
		t.Fatalf("a complete physical proof was refused: %v", err)
	}
}

func TestTheBootstrapCapabilityNeverReachesACreateError(t *testing.T) {
	// THE CREATE-PATH LEAK CANARY. Two plants, because there are two ways the
	// token could reach an error: the attach cause quoting the bytes it was
	// writing, and the Pod API create error quoting the request body.
	transport, config, intent, token := newTransportFixture(t)
	backend := &Backend{config: config}
	secret := token.Encoded()
	if len(secret) < 16 {
		t.Fatalf("the fixture token is too short to be a canary: %d bytes", len(secret))
	}
	client := transport.client.(*fake.Clientset)

	// Plant 1: the attach fails with an error that QUOTES THE TOKEN, the way a
	// remotecommand stream error can quote the bytes in flight.
	client.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		pod := action.(k8stesting.CreateAction).GetObject().(*corev1.Pod).DeepCopy()
		pod.UID = "pod-uid-canary"
		return true, pod, nil
	})
	transport.waitRunner = func(_ context.Context, _, _, _ string, _ time.Time) (corev1.Pod, error) {
		pod := transport.toPod(backend.podSpec(intent))
		pod.UID = "pod-uid-canary"
		return *pod, nil
	}
	// Plant 1 goes through the REAL deliverBootstrap, not past it. Sub-step
	// constants now propagate under `create stage=attach:`, so the boundary
	// that matters moved inside: a stream error that quotes the bytes in
	// flight must be replaced there, and planting it on the `attach` seam
	// would bypass the very code under test.
	transport.attach = func(ctx context.Context, pod corev1.Pod, token bootstrap.Token) error {
		return deliverBootstrap(ctx, token, pod.Name, string(pod.UID),
			func(_ context.Context, stdin io.Reader, _ io.Writer) error {
				// Read the prompt first, so the plant lands on the IDENTITY
				// READ rather than on the prompt write -- the step where a
				// real stream error would carry the bytes in flight.
				_, _ = bufio.NewReader(stdin).ReadString('\n')
				return fmt.Errorf("upgrade request failed writing %q to https://apiserver/exec?stdin=true", secret)
			})
	}
	transport.witness.(*witnessFixture).inventory = PhysicalInventory{Complete: true,
		Workers: []PhysicalWorker{{Name: intent.ResourceID, UID: "pod-uid-canary",
			Labels: backend.labels(intent.LeaseID, intent.ResourceID), RuntimeClassName: config.RuntimeClassName,
			ProcessPresent: true, StoragePresent: true}}}

	_, err := transport.Create(context.Background(), CreateRequest{Spec: backend.podSpec(intent),
		BootstrapMode: MemoryOnlyBootstrap, BootstrapToken: token, StartupDeadline: time.Now().Add(time.Minute)})
	if err == nil {
		t.Fatal("a failing attach was accepted")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("the bootstrap capability reached the returned error: %v", err)
	}
	if strings.Contains(err.Error(), "apiserver/exec") {
		t.Fatalf("the attach URL reached the returned error: %v", err)
	}
	// Positive control, now two-part: the stage tag survives AND the sub-step
	// constant survives. Without the second half this canary would pass on an
	// error that lost everything, which is the whole point of letting the
	// sub-steps propagate.
	if !strings.Contains(err.Error(), "create stage="+createStageAttach) {
		t.Fatalf("the attach stage tag was lost with the cause: %v", err)
	}
	// A stream that dies at connect loses a race between the prompt write and
	// the identity read, and either can report it. Both constants are correct;
	// which one wins is scheduling. Seen red on the Linux CI runner under -race
	// with only the second accepted.
	if !strings.Contains(err.Error(), "identity prompt failed") && !strings.Contains(err.Error(), "handshake failed") {
		t.Fatalf("the attach sub-step constant did not survive: %v", err)
	}

	// Plant 2: the Pod API create fails with an error that quotes the REQUEST
	// BODY -- which on this path is a worker Pod spec.
	client.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, fmt.Errorf("the server rejected our request: POST https://apiserver/api/v1/pods body=%q", secret)
	})
	_, err = transport.Create(context.Background(), CreateRequest{Spec: backend.podSpec(intent),
		BootstrapMode: MemoryOnlyBootstrap, BootstrapToken: token, StartupDeadline: time.Now().Add(time.Minute)})
	if err == nil {
		t.Fatal("a failing Pod create was accepted")
	}
	for _, forbidden := range []string{secret, "apiserver", "api/v1/pods"} {
		if strings.Contains(err.Error(), forbidden) {
			t.Fatalf("the create request body or URL reached the returned error (%q): %v", forbidden, err)
		}
	}
	if !strings.Contains(err.Error(), "create stage="+createStagePod) {
		t.Fatalf("the create-pod stage tag was lost: %v", err)
	}
}

// --- the controller speaks first --------------------------------------------
//
// A Kubernetes attach does not replay output emitted before the attach. The
// worker used to print its identity at startup; the controller attaches ~10s
// later, so it waited forever for a line already said. Proven on the cluster
// with a throwaway Kata pod: `kubectl logs` showed the startup line, the attach
// stream showed nothing.

func TestDeliverBootstrapAsksBeforeTheGuestAnnouncesItself(t *testing.T) {
	token, err := bootstrap.NewToken()
	if err != nil {
		t.Fatal(err)
	}
	var order []string
	stream := func(ctx context.Context, stdin io.Reader, stdout io.Writer) error {
		reader := bufio.NewReader(stdin)
		// A guest that says nothing until asked -- the real entrypoint. If the
		// controller does not speak first this read blocks and the test times
		// out, which is precisely the live failure.
		prompt, err := reader.ReadString('\n')
		if err != nil {
			return err
		}
		order = append(order, "read:"+strings.TrimSpace(prompt))
		if strings.TrimSpace(prompt) != helloPrompt {
			return errors.New("unexpected prompt")
		}
		if _, err := io.WriteString(stdout, "HELLO worker-1 pod-uid\n"); err != nil {
			return err
		}
		order = append(order, "wrote:HELLO")
		line, err := reader.ReadString('\n')
		if err != nil {
			return err
		}
		if line != "CAP "+token.Encoded()+"\n" {
			return errors.New("unexpected capability")
		}
		order = append(order, "read:CAP")
		if _, err := io.WriteString(stdout, "ACK worker-1 pod-uid\n"); err != nil {
			return err
		}
		<-ctx.Done()
		return ctx.Err()
	}
	if err := deliverBootstrap(context.Background(), token, "worker-1", "pod-uid", stream); err != nil {
		t.Fatalf("the handshake failed: %v", err)
	}
	want := []string{"read:" + helloPrompt, "wrote:HELLO", "read:CAP"}
	if len(order) != len(want) {
		t.Fatalf("handshake order is %v, want %v", order, want)
	}
	for index := range want {
		if order[index] != want[index] {
			t.Fatalf("handshake order is %v, want %v", order, want)
		}
	}
}

func TestAWrongIdentityIsRefusedBeforeTheCapabilityIsWritten(t *testing.T) {
	// THE STAGED-BREAK SUBJECT. The capability must not reach a guest whose
	// identity did not match, and "the handshake returned an error" is not the
	// same claim -- it could have returned one after writing.
	token, err := bootstrap.NewToken()
	if err != nil {
		t.Fatal(err)
	}
	var wroteCapability atomic.Bool
	stream := func(_ context.Context, stdin io.Reader, stdout io.Writer) error {
		reader := bufio.NewReader(stdin)
		if _, err := reader.ReadString('\n'); err != nil {
			return err
		}
		if _, err := io.WriteString(stdout, "HELLO someone-else pod-uid\n"); err != nil {
			return err
		}
		line, err := reader.ReadString('\n')
		if err != nil {
			// EOF here is the PASSING shape: the controller closed without
			// writing anything after the wrong identity.
			return nil
		}
		if strings.HasPrefix(line, "CAP ") {
			wroteCapability.Store(true)
		}
		return nil
	}
	if err := deliverBootstrap(context.Background(), token, "worker-1", "pod-uid", stream); err == nil {
		t.Fatal("a wrong identity was accepted")
	}
	if wroteCapability.Load() {
		t.Fatal("the capability was written to a guest whose identity did not match")
	}
}

func TestASilentGuestIsDistinguishedFromAHandshakeFailure(t *testing.T) {
	// A guest that never answers is a worker problem -- a crashed entrypoint,
	// an image without the new protocol. A wrong answer is a different one.
	// Before the stage tags both arrived as one sentence.
	token, err := bootstrap.NewToken()
	if err != nil {
		t.Fatal(err)
	}
	silent := func(ctx context.Context, stdin io.Reader, _ io.Writer) error {
		_, _ = bufio.NewReader(stdin).ReadString('\n')
		<-ctx.Done()
		return ctx.Err()
	}
	done := make(chan error, 1)
	go func() {
		done <- deliverBootstrapWithin(context.Background(), token, "worker-1", "pod-uid", silent, 60*time.Millisecond)
	}()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "not answered in time") {
			t.Fatalf("a silent guest was not reported as unanswered: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a silent guest was never given up on")
	}
}

// A stream that dies before it ever reads stdin is the production shape that
// cost an operator a night. The controller's io.Pipe write blocked behind a
// dead reader, burned the full bound, and reported a silent guest -- for an
// RBAC denial that had already returned in milliseconds.
func TestAStreamThatDiesBeforeReadingStdinFailsImmediately(t *testing.T) {
	token, err := bootstrap.NewToken()
	if err != nil {
		t.Fatal(err)
	}
	stream := func(context.Context, io.Reader, io.Writer) error {
		return errors.New("unable to upgrade streaming request: pods \"w\" is forbidden: " +
			"User \"system:serviceaccount:github-runner-pool:runner-pool-controller\" cannot get resource \"pods/attach\"")
	}
	start := time.Now()
	// A generous bound: if the write still waits it out, this test takes ten
	// seconds and fails on elapsed time rather than on the message.
	err = deliverBootstrapWithin(context.Background(), token, "w", "uid", stream, 10*time.Second)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("a dead stream was accepted")
	}
	if elapsed > 5*time.Second {
		t.Fatalf("the handshake waited out its bound behind a dead stream: %s", elapsed)
	}
	if !strings.Contains(err.Error(), "stream=forbidden") {
		t.Fatalf("the stream's class was not reported: %v", err)
	}
}

func TestTheStreamClassNamesTheFailureWithoutQuotingIt(t *testing.T) {
	secret := "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	for _, testCase := range []struct {
		name  string
		err   error
		class string
	}{
		{"no failure yet", nil, "none"},
		{"rbac denial", errors.New("cannot get resource \"pods/attach\" in API group"), "forbidden"},
		{"api status", apierrors.NewNotFound(schema.GroupResource{Resource: "pods"}, "w"), "not-found"},
		{"container not running", errors.New("container runner is not running"), "container-not-running"},
		{"upgrade status", errors.New("websocket: bad handshake status 500 Internal Server Error"), "http-500"},
		{"tls", errors.New("x509: certificate signed by unknown authority"), "tls"},
		{"refused", errors.New("dial tcp 10.43.0.1:443: connect: connection refused"), "refused"},
		{"timeout", errors.New("net/http: request canceled (Client.Timeout exceeded)"), "timeout"},
		{"closed", errors.New("unexpected EOF"), "closed"},
		{"unknown", errors.New("something nobody has seen"), "other"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if got := classifyStreamFailure(testCase.err); got != testCase.class {
				t.Fatalf("classified as %q, expected %q", got, testCase.class)
			}
		})
	}
	// The classifier is on the pre-capability path, so it is a leak surface in
	// its own right: it must never return any part of what it read.
	planted := errors.New("upgrade failed writing " + secret + " to https://apiserver/exec?stdin=true")
	class := classifyStreamFailure(planted)
	for _, forbidden := range []string{secret, "apiserver", "exec?stdin"} {
		if strings.Contains(class, forbidden) {
			t.Fatalf("the class quoted the error it read (%q): %q", forbidden, class)
		}
	}
	if class != "other" {
		t.Fatalf("an unknown error classified as %q rather than other", class)
	}
}

// The two hypotheses an attach failure cannot tell apart -- the guest never
// spoke, versus there was no guest to speak -- are settled by the container's
// own state, and the controller has `get pods`.
func TestAFailedAttachReportsTheRunnerContainerState(t *testing.T) {
	transport, config, intent, token := newTransportFixture(t)
	backend := &Backend{config: config}
	client := transport.client.(*fake.Clientset)
	client.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		pod := action.(k8stesting.CreateAction).GetObject().(*corev1.Pod).DeepCopy()
		pod.UID = "pod-uid-state"
		return true, pod, nil
	})
	// The read-back the failure path performs, answered with a runner that
	// exited rather than one that is running. That is the shape this diagnostic
	// exists for: the attach cannot see it, and it explains the silence.
	client.PrependReactor("get", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		pod := transport.toPod(backend.podSpec(intent))
		pod.UID = "pod-uid-state"
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "runner",
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "Error", ExitCode: 2}}}}
		return true, pod, nil
	})
	transport.waitRunner = func(_ context.Context, _, _, _ string, _ time.Time) (corev1.Pod, error) {
		pod := transport.toPod(backend.podSpec(intent))
		pod.UID = "pod-uid-state"
		return *pod, nil
	}
	transport.attach = func(context.Context, corev1.Pod, bootstrap.Token) error {
		return errors.New("bootstrap attach identity handshake failed (stream=closed)")
	}
	transport.witness.(*witnessFixture).inventory = PhysicalInventory{Complete: true,
		Workers: []PhysicalWorker{{Name: intent.ResourceID, UID: "pod-uid-state",
			Labels: backend.labels(intent.LeaseID, intent.ResourceID), RuntimeClassName: config.RuntimeClassName,
			ProcessPresent: true, StoragePresent: true}}}

	_, err := transport.Create(context.Background(), CreateRequest{Spec: backend.podSpec(intent),
		BootstrapMode: MemoryOnlyBootstrap, BootstrapToken: token, StartupDeadline: time.Now().Add(time.Minute)})
	if err == nil {
		t.Fatal("a failing attach was accepted")
	}
	for _, want := range []string{"runner=terminated", "reason=Error", "exit=2"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("the runner container state did not reach the error (%q): %v", want, err)
		}
	}
}

func TestAKubeletReasonIsNotEchoedAsText(t *testing.T) {
	if got := allowedContainerReason("CrashLoopBackOff"); got != "CrashLoopBackOff" {
		t.Fatalf("a known reason was not kept: %q", got)
	}
	if got := allowedContainerReason("anything the kubelet feels like saying"); got != "other" {
		t.Fatalf("an unknown reason was echoed rather than mapped: %q", got)
	}
	if got := allowedContainerReason(""); got != "none" {
		t.Fatalf("an empty reason became %q", got)
	}
}

// --- follow-ups from the #854 review (#837) ---------------------------------

// TestThePromptWriteClassifySiteNeverQuotesTheStream is the canary the
// prompt-write site did not have while its sibling did. The prompt carries no
// secret, which is exactly why that site is allowed to describe the stream at
// all -- but it may describe it only BY CLASS, and a site that started wrapping
// the cause instead would look identical from the outside.
//
// The plant is a stream that returns immediately without ever reading stdin, so
// the prompt write fails on streamClosed and this site, not the handshake site,
// produces the error.
func TestThePromptWriteClassifySiteNeverQuotesTheStream(t *testing.T) {
	_, _, _, token := newTransportFixture(t)
	secret := token.Encoded()
	err := deliverBootstrap(context.Background(), token, "slipway-linux-probe", "pod-uid-prompt",
		func(context.Context, io.Reader, io.Writer) error {
			return fmt.Errorf("upgrade request failed writing %q to https://apiserver/exec?stdin=true", secret)
		})
	if err == nil {
		t.Fatal("a stream that died before reading stdin was accepted")
	}
	if !strings.Contains(err.Error(), "identity prompt failed") {
		t.Fatalf("the failure did not land on the prompt-write site: %v", err)
	}
	for _, forbidden := range []string{secret, "apiserver", "upgrade request"} {
		if strings.Contains(err.Error(), forbidden) {
			t.Fatalf("the prompt-write site quoted the stream (%q): %v", forbidden, err)
		}
	}
	// The positive control. Without it this passes on an error that lost the
	// class too, which is the thing the site exists to produce.
	if !strings.Contains(err.Error(), "stream=") {
		t.Fatalf("the stream class was lost: %v", err)
	}
}

// TestTheAcknowledgementReadIsBounded pins the bound added for #837. Before it,
// a guest that took the capability and then stopped speaking held the attach --
// and the lease -- until the ten-minute startup deadline.
//
// The guest here answers the prompt, reads the capability, and then says
// nothing. With the read unbounded this call never returns.
func TestTheAcknowledgementReadIsBounded(t *testing.T) {
	_, _, _, token := newTransportFixture(t)
	settled := make(chan error, 1)
	go func() {
		settled <- deliverBootstrapWithin(context.Background(), token, "slipway-linux-probe", "pod-uid-ack",
			func(_ context.Context, stdin io.Reader, stdout io.Writer) error {
				reader := bufio.NewReader(stdin)
				if _, err := reader.ReadString('\n'); err != nil {
					return err
				}
				if _, err := io.WriteString(stdout, "HELLO slipway-linux-probe pod-uid-ack\n"); err != nil {
					return err
				}
				// Consume the capability and then go silent, without closing
				// the stream. This is the shape the bound exists for.
				if _, err := reader.ReadString('\n'); err != nil {
					return err
				}
				<-time.After(time.Minute)
				return nil
			}, 200*time.Millisecond)
	}()
	select {
	case err := <-settled:
		if err == nil || !strings.Contains(err.Error(), "acknowledgement failed") {
			t.Fatalf("a silent guest did not fail the acknowledgement: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the acknowledgement read is unbounded: a silent guest held the attach open")
	}
}

// TestAKubeletReasonIsMappedOnTheRealPath is the half the direct unit test
// cannot cover. `allowedContainerReason` was exercised only by calling it, so
// deleting either call site left the suite green (#837, Important 2). These
// enter runnerContainerState through Create and assert on the returned error,
// which is the only place the mapping's absence would show.
func TestAKubeletReasonIsMappedOnTheRealPath(t *testing.T) {
	hostile := "Evicted: the kubelet said something nobody allowlisted"
	for name, status := range map[string]corev1.ContainerState{
		"waiting":    {Waiting: &corev1.ContainerStateWaiting{Reason: hostile}},
		"terminated": {Terminated: &corev1.ContainerStateTerminated{Reason: hostile, ExitCode: 7}},
	} {
		t.Run(name, func(t *testing.T) {
			transport, config, intent, token := newTransportFixture(t)
			backend := &Backend{config: config}
			client := transport.client.(*fake.Clientset)
			client.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
				pod := action.(k8stesting.CreateAction).GetObject().(*corev1.Pod).DeepCopy()
				pod.UID = "pod-uid-reason"
				return true, pod, nil
			})
			client.PrependReactor("get", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
				pod := transport.toPod(backend.podSpec(intent))
				pod.UID = "pod-uid-reason"
				pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "runner", State: status}}
				return true, pod, nil
			})
			transport.waitRunner = func(_ context.Context, _, _, _ string, _ time.Time) (corev1.Pod, error) {
				pod := transport.toPod(backend.podSpec(intent))
				pod.UID = "pod-uid-reason"
				return *pod, nil
			}
			transport.attach = func(context.Context, corev1.Pod, bootstrap.Token) error {
				return errors.New("bootstrap attach identity handshake failed (stream=closed)")
			}
			transport.witness.(*witnessFixture).inventory = PhysicalInventory{Complete: true,
				Workers: []PhysicalWorker{{Name: intent.ResourceID, UID: "pod-uid-reason",
					Labels: backend.labels(intent.LeaseID, intent.ResourceID), RuntimeClassName: config.RuntimeClassName,
					ProcessPresent: true, StoragePresent: true}}}

			_, err := transport.Create(context.Background(), CreateRequest{Spec: backend.podSpec(intent),
				BootstrapMode: MemoryOnlyBootstrap, BootstrapToken: token, StartupDeadline: time.Now().Add(time.Minute)})
			if err == nil {
				t.Fatal("a failing attach was accepted")
			}
			if strings.Contains(err.Error(), "kubelet said") || strings.Contains(err.Error(), "Evicted") {
				t.Fatalf("an unallowlisted kubelet reason was echoed as text: %v", err)
			}
			// The positive control: the state line reached the error at all, so
			// this cannot pass on a path that stopped reporting the container.
			if !strings.Contains(err.Error(), "runner="+name) || !strings.Contains(err.Error(), "reason=other") {
				t.Fatalf("the mapped container state did not reach the error: %v", err)
			}
		})
	}
}

// TestAHandshakeLineIsBoundedWhileItReads pins the read bound itself, and it
// pins it by COUNTING WHAT THE READER GAVE UP, not by the returned error.
//
// The first version of this test fed a long but TERMINATED line and asserted an
// error. Both implementations return an error for that -- the old one read the
// whole line and checked its length afterwards -- so reverting the fix left it
// green: an inert test that read as coverage of the bound (#870 review,
// Important 2).
//
// The distinguishing input is a line with NO NEWLINE AT ALL. The old ReadString
// path consumes until EOF however far away that is; the bounded loop stops just
// past the bound. countingReader is what makes the difference observable.
type countingReader struct {
	remaining int
	consumed  int
}

func (r *countingReader) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, io.EOF
	}
	count := len(p)
	if count > r.remaining {
		count = r.remaining
	}
	for index := 0; index < count; index++ {
		p[index] = 'A'
	}
	r.remaining -= count
	r.consumed += count
	return count, nil
}

func TestAHandshakeLineIsBoundedWhileItReads(t *testing.T) {
	// Effectively unbounded against a 256-byte bound: 4 MiB of a line that never
	// ends. A reader that must see the newline consumes all of it.
	source := &countingReader{remaining: 4 << 20}
	if _, err := readBootstrapLine(bufio.NewReaderSize(source, 512)); err == nil {
		t.Fatal("an unterminated handshake line was accepted")
	}
	// The bound is on the READ. One bufio fill beyond the bound is the most a
	// correct implementation can consume; the unbounded one consumes everything.
	ceiling := maxBootstrapLine + 512 + 1
	if source.consumed > ceiling {
		t.Fatalf("the reader consumed %d bytes against a %d-byte bound; the bound is on the result, not the read",
			source.consumed, maxBootstrapLine)
	}
	// The positive control: a line AT the bound is still read whole. Without it
	// a reader that refused everything would satisfy the assertion above.
	atBound := strings.Repeat("A", maxBootstrapLine)
	line, err := readBootstrapLine(bufio.NewReaderSize(strings.NewReader(atBound+"\n"), 512))
	if err != nil || line != atBound {
		t.Fatalf("a line at the bound was refused: %q %v", line, err)
	}
	// And one byte over is refused, so the bound is where it says it is.
	if _, err := readBootstrapLine(bufio.NewReaderSize(strings.NewReader(atBound+"A\n"), 512)); err == nil {
		t.Fatal("a line one byte over the bound was accepted")
	}
}

// ownWorkerPod is the Pod this adapter creates, as the API server returns it.
func ownWorkerPod(t *testing.T) (*Transport, Config, *corev1.Pod) {
	t.Helper()
	transport, config, intent, _ := newTransportFixture(t)
	pod := transport.toPod((&Backend{config: config}).podSpec(intent))
	pod.UID = "pod-uid-own"
	if err := transport.validateObservedPod(*pod); err != nil {
		t.Fatalf("fixture drifted: the adapter's own Pod fails its validator: %v", err)
	}
	return transport, config, pod
}

func containerIndex(t *testing.T, pod *corev1.Pod, name string) int {
	t.Helper()
	for index := range pod.Spec.Containers {
		if pod.Spec.Containers[index].Name == name {
			return index
		}
	}
	t.Fatalf("no container named %s", name)
	return -1
}

func memoryVolume(name string, size int64) corev1.Volume {
	return corev1.Volume{Name: name, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{
		Medium: corev1.StorageMediumMemory, SizeLimit: resource.NewQuantity(size, resource.BinarySI)}}}
}

// homelab#983. A `docker` volume is the retired shape, and anything mounted at
// the data root would put the image store back on whatever that volume is.
func TestTheObservedPodValidatorRefusesADockerVolumeAndTheDataRootMounted(t *testing.T) {
	for name, mutate := range map[string]func(*testing.T, *corev1.Pod){
		"a docker volume, unmounted": func(_ *testing.T, pod *corev1.Pod) {
			pod.Spec.Volumes = append(pod.Spec.Volumes, memoryVolume("docker", 4<<30))
		},
		"a docker volume at /var/lib/docker": func(t *testing.T, pod *corev1.Pod) {
			pod.Spec.Volumes = append(pod.Spec.Volumes, memoryVolume("docker", 4<<30))
			index := containerIndex(t, pod, "dockerd")
			pod.Spec.Containers[index].VolumeMounts = append(pod.Spec.Containers[index].VolumeMounts,
				corev1.VolumeMount{Name: "docker", MountPath: "/var/lib/docker"})
		},
		"an approved volume also mounted at the data root": func(t *testing.T, pod *corev1.Pod) {
			index := containerIndex(t, pod, "dockerd")
			pod.Spec.Containers[index].VolumeMounts = append(pod.Spec.Containers[index].VolumeMounts,
				corev1.VolumeMount{Name: "tmp", MountPath: "/var/lib/sjo-docker"})
		},
		"dockerd's tmp moved to the data root": func(t *testing.T, pod *corev1.Pod) {
			index := containerIndex(t, pod, "dockerd")
			for mount := range pod.Spec.Containers[index].VolumeMounts {
				if pod.Spec.Containers[index].VolumeMounts[mount].Name == "tmp" {
					pod.Spec.Containers[index].VolumeMounts[mount].MountPath = "/var/lib/sjo-docker"
				}
			}
		},
		"a subdirectory of the data root mounted in the runner": func(t *testing.T, pod *corev1.Pod) {
			index := containerIndex(t, pod, "runner")
			pod.Spec.Containers[index].VolumeMounts = append(pod.Spec.Containers[index].VolumeMounts,
				corev1.VolumeMount{Name: "work", MountPath: "/var/lib/sjo-docker/overlay2"})
		},
	} {
		t.Run(name, func(t *testing.T) {
			transport, _, pod := ownWorkerPod(t)
			mutate(t, pod)
			if err := transport.validateObservedPod(*pod); err == nil {
				t.Fatal("the observed-Pod validator accepted it")
			}
		})
	}
}

// oldShapeWorkerPod is the worker Pod the previous controller created: a
// fourth RAM volume `docker`, mounted at /var/lib/docker, and the data root
// left there.
func oldShapeWorkerPod(t *testing.T, pod *corev1.Pod) *corev1.Pod {
	t.Helper()
	old := pod.DeepCopy()
	old.Spec.Volumes = append([]corev1.Volume{memoryVolume("docker", 4<<30)}, old.Spec.Volumes...)
	index := containerIndex(t, old, "dockerd")
	dockerd := &old.Spec.Containers[index]
	dockerd.VolumeMounts = append([]corev1.VolumeMount{{Name: "docker", MountPath: "/var/lib/docker"}}, dockerd.VolumeMounts...)
	for arg := range dockerd.Args {
		if strings.HasPrefix(dockerd.Args[arg], "--data-root=") {
			dockerd.Args[arg] = "--data-root=/var/lib/docker"
		}
	}
	return old
}

// WHY THE CUTOVER NEEDS A DRAINED POOL, pinned. One old-shape worker in the
// listing fails validation, List returns Complete:false, and the controller
// fences admission -- and a fenced controller cannot delete the Pod that
// fenced it. If this test ever goes green-by-tolerance, the plan's drain step
// is no longer what makes the cutover safe, and that has to be a decision.
func TestAnOldShapeWorkerInTheListingLeavesTheInventoryIncomplete(t *testing.T) {
	transport, config, pod := ownWorkerPod(t)
	old := oldShapeWorkerPod(t, pod)
	old.UID = "pod-uid-old-shape"
	listing := []corev1.Pod{*pod}
	client := transport.client.(*fake.Clientset)
	client.PrependReactor("list", "pods", func(_ k8stesting.Action) (bool, runtime.Object, error) {
		return true, &corev1.PodList{Items: listing}, nil
	})
	selector := (&Backend{config: config}).Selector()
	// The positive control: the same listing path with today's shape is
	// complete, so the refusal below is the shape and not the fixture.
	if result, err := transport.List(context.Background(), config.Namespace, selector); err != nil || !result.Complete {
		t.Fatalf("fixture drifted: a listing of the adapter's own Pod is incomplete: %+v %v", result, err)
	}
	// The old-shape Pod alone, under the same name: nothing but its shape can
	// make this listing incomplete.
	listing = []corev1.Pod{*old}
	result, err := transport.List(context.Background(), config.Namespace, selector)
	if err == nil || result.Complete {
		t.Fatalf("an old-shape worker Pod was accepted into a complete inventory: %+v", result)
	}
}

func TestTheCreateSpecCheckRefusesADockerVolumeAndTheDataRootMounted(t *testing.T) {
	transport, config, intent, _ := newTransportFixture(t)
	backend := &Backend{config: config}
	if err := transport.validateCreateSpec(backend.podSpec(intent)); err != nil {
		t.Fatalf("fixture drifted: the adapter's own spec fails its create check: %v", err)
	}
	withDocker := backend.podSpec(intent)
	withDocker.Volumes = append(withDocker.Volumes, Volume{Name: "docker", Medium: "Memory", SizeBytes: 4 << 30})
	if err := transport.validateCreateSpec(withDocker); err == nil {
		t.Fatal("the create check accepted a fourth, docker volume")
	}
	for _, path := range []string{"/var/lib/sjo-docker", "/var/lib/sjo-docker/overlay2", "/var/lib", "/var/lib/", "/var", "/"} {
		covering := backend.podSpec(intent)
		covering.Containers[1].Mounts = append(covering.Containers[1].Mounts, VolumeMount{Name: "tmp", MountPath: path})
		if err := transport.validateCreateSpec(covering); err == nil {
			t.Fatalf("the create check accepted a volume mounted at %q, which hides the dockerd data root", path)
		}
	}
	// Positive control: a sibling path that only shares a string prefix is not an ancestor.
	if coversDockerDataRoot("/var/lib/sjo-dock") || coversDockerDataRoot("/var/lib/sjo-docker-other") || coversDockerDataRoot("/var/run") {
		t.Fatal("coversDockerDataRoot matched a path that neither contains nor sits under the data root")
	}
}

// The %w chain end to end (homelab#1213): a real *hostWitnessRefusal from the
// storage source travels through TrustedInventory, the Transport's list stage
// and Backend.Create, and still reads as ErrWitnessRefusedBeforeCreate. A
// flattening wrap anywhere on that path would silently put the race back on
// the breaker.
func TestAStorageRefusalReachesCreateThroughTheRealWitnessChain(t *testing.T) {
	config := testConfig()
	intent, _ := testIntent(config)
	refusal := &hostWitnessRefusal{reason: WitnessReasonStorage, failure: "collector failed (62 bytes)"}
	sources := physicalSources(PodScope{Complete: true}, WitnessScope{Complete: true}, WitnessScope{}, physicalCapacity())
	sources.Storage = &fakeStorageSource{err: refusal}
	inventory := newPhysicalInventory(t, sources)
	transport, err := NewTransport(&rest.Config{Host: "https://kubernetes.default.svc"}, fake.NewSimpleClientset(), inventory, config)
	if err != nil {
		t.Fatal(err)
	}
	backend, err := New(transport, config)
	if err != nil {
		t.Fatal(err)
	}
	_, err = backend.Create(context.Background(), intent)
	if !errors.Is(err, controller.ErrWitnessRefusedBeforeCreate) {
		t.Fatalf("the tag was lost on the way to Create: %v", err)
	}
	var got *hostWitnessRefusal
	if !errors.As(err, &got) || got != refusal {
		t.Fatalf("the original refusal is not reachable through the chain: %v", err)
	}
}

func TestWorkerEnvAllowlistEnforcedAtCreateAndObservation(t *testing.T) {
	config := testConfig()
	intent, _ := testIntent(config)
	transport := &Transport{worker: config}
	backend := &Backend{config: config}

	baseSpec := backend.podSpec(intent)
	if err := transport.validateCreateSpec(baseSpec); err != nil {
		t.Fatalf("approved podSpec rejected: %v", err)
	}

	// Extra harmless env var on runner must fail.
	extraRunner := backend.podSpec(intent)
	extraRunner.Containers[0].Environment = append(extraRunner.Containers[0].Environment, EnvVar{Name: "HARMLESS_EXTRA", Value: "1"})
	if err := transport.validateCreateSpec(extraRunner); err == nil {
		t.Fatal("validateCreateSpec accepted an extra env var on runner")
	}

	// Renamed token env var or token without Bearer prefix must fail.
	renamedToken := backend.podSpec(intent)
	renamedToken.Containers[0].Environment = append(renamedToken.Containers[0].Environment, EnvVar{Name: "SLIPWAY_BOOTSTRAP_TOKEN", Value: "raw-token-no-bearer"})
	if err := transport.validateCreateSpec(renamedToken); err == nil {
		t.Fatal("validateCreateSpec accepted a renamed token env var")
	}

	// Any env var on dockerd must fail.
	extraDocker := backend.podSpec(intent)
	extraDocker.Containers[1].Environment = append(extraDocker.Containers[1].Environment, EnvVar{Name: "DOCKER_TLS_CERTDIR", Value: ""})
	if err := transport.validateCreateSpec(extraDocker); err == nil {
		t.Fatal("validateCreateSpec accepted an env var on dockerd")
	}

	// Mutating an allowed value or dropping an allowed var must fail.
	mutatedVal := backend.podSpec(intent)
	mutatedVal.Containers[0].Environment[0].Value = "tcp://127.0.0.1:2375"
	if err := transport.validateCreateSpec(mutatedVal); err == nil {
		t.Fatal("validateCreateSpec accepted a mutated DOCKER_HOST value")
	}
	missingVar := backend.podSpec(intent)
	missingVar.Containers[0].Environment = missingVar.Containers[0].Environment[:1]
	if err := transport.validateCreateSpec(missingVar); err == nil {
		t.Fatal("validateCreateSpec accepted a runner missing a required env var")
	}

	// Observed runner container: exact injected set passes; SecretKeyRef or renamed fieldRef fails.
	pod := transport.toPod(baseSpec)
	runner := pod.Spec.Containers[0]
	if err := transport.validateObservedRunner(runner, intent.LeaseID); err != nil {
		t.Fatalf("validateObservedRunner rejected injected runner: %v", err)
	}
	withSecret := *runner.DeepCopy()
	withSecret.Env = append(withSecret.Env, corev1.EnvVar{
		Name: "LEAKED_SECRET",
		ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "github-runner-app"},
				Key:                  "APP_PRIVATE_KEY",
			},
		},
	})
	if err := transport.validateObservedRunner(withSecret, intent.LeaseID); err == nil {
		t.Fatal("validateObservedRunner accepted a secretKeyRef env var")
	}
	wrongField := *runner.DeepCopy()
	wrongField.Env[len(wrongField.Env)-1].Name = "SJO_BOOTSTRAP_EXPECTED_OTHER"
	if err := transport.validateObservedRunner(wrongField, intent.LeaseID); err == nil {
		t.Fatal("validateObservedRunner accepted a renamed downward-API fieldRef")
	}
}

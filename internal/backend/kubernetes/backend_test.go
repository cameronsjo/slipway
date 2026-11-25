package kubernetes

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cameronsjo/slipway/internal/bootstrap"
	"github.com/cameronsjo/slipway/internal/controller"
	"github.com/google/uuid"
)

type fakeAPI struct {
	list       ListResult
	headroom   Headroom
	created    WorkerResource
	witness    AbsenceWitness
	createErr  error
	deleteErr  error
	createReqs []CreateRequest
	deleteReqs []DeleteRequest
	listCalls  int
	listErr    error
}

func (f *fakeAPI) List(context.Context, string, string) (ListResult, error) {
	f.listCalls++
	return f.list, f.listErr
}

func (f *fakeAPI) Headroom(context.Context, string, Footprint) (Headroom, error) {
	return f.headroom, nil
}

func (f *fakeAPI) Create(_ context.Context, request CreateRequest) (WorkerResource, error) {
	f.createReqs = append(f.createReqs, request)
	if f.createErr != nil {
		return WorkerResource{}, f.createErr
	}
	return f.created, nil
}

func (f *fakeAPI) Delete(_ context.Context, request DeleteRequest) (AbsenceWitness, error) {
	f.deleteReqs = append(f.deleteReqs, request)
	if f.deleteErr != nil {
		return AbsenceWitness{}, f.deleteErr
	}
	return f.witness, nil
}

func testConfig() Config {
	return Config{
		Namespace:            "runner-pool",
		OwnerUID:             "controller-uid-1234",
		NodeName:             "homelab-ms-a2",
		RuntimeClassName:     "sjo-kata-qemu-runner",
		RunnerImage:          "ghcr.io/cameronsjo/runner@sha256:" + strings.Repeat("a", 64),
		DockerImage:          "docker.io/library/docker@sha256:" + strings.Repeat("b", 64),
		WorkerCommand:        "/usr/local/bin/runner-pool-worker",
		RunnerRoot:           "/actions-runner",
		CAFile:               "/etc/runner-pool/bootstrap-ca.pem",
		BootstrapURL:         "https://slipway-bootstrap.example.invalid:8443",
		Capacity:             2,
		RunnerResources:      ContainerResources{Requests: Footprint{CPUMilli: 500, MemoryBytes: 2 << 30, StorageBytes: 3 << 30}, Limits: Footprint{CPUMilli: 2000, MemoryBytes: 2 << 30, StorageBytes: 3 << 30}},
		DockerResources:      ContainerResources{Requests: Footprint{CPUMilli: 1500, MemoryBytes: 6 << 30, StorageBytes: 4 << 30}, Limits: Footprint{CPUMilli: 2000, MemoryBytes: 6 << 30, StorageBytes: 4 << 30}},
		Overhead:             Footprint{CPUMilli: 250, MemoryBytes: 512 << 20, StorageBytes: 512 << 20},
		Volumes:              VolumeBudget{Work: 2 << 30, Socket: 16 << 20, Tmp: 256 << 20},
		ContainerRootfsBytes: 20 << 30,
	}
}

func testIntent(config Config) (controller.WorkerCreateIntent, bootstrap.Token) {
	token, err := bootstrap.NewToken()
	if err != nil {
		panic(err)
	}
	id := uuid.NewString()
	return controller.WorkerCreateIntent{LeaseID: id, ResourceID: "slipway-linux-" + id, Slot: 0,
		BootstrapURL: config.BootstrapURL, BootstrapToken: token, StartupDeadline: time.Now().Add(10 * time.Minute)}, token
}

func ownedResource(config Config, intent controller.WorkerCreateIntent, uid string, pod, process, storage bool) WorkerResource {
	return WorkerResource{Namespace: config.Namespace, Name: intent.ResourceID, UID: uid,
		Labels: map[string]string{LabelManagedBy: ManagedByValue, LabelPlatform: PlatformLinux,
			LabelOwnerUID: config.OwnerUID, LabelLeaseID: intent.LeaseID, LabelResourceID: intent.ResourceID},
		RuntimeClassName: config.RuntimeClassName, PodExists: pod, ProcessPresent: process, StoragePresent: storage}
}

func enoughHeadroom(config Config, slots int64) Headroom {
	per := (&Backend{config: config}).perWorkerFootprint()
	// A real witness always supplies its own five-dimension verdict, and the
	// backend now refuses a Headroom without one. The fixture matches the
	// contract rather than the struct's zero value.
	return Headroom{Complete: true, CPUMilli: per.CPUMilli * slots, MemoryBytes: per.MemoryBytes * slots,
		StorageBytes: per.StorageBytes * slots, FreeSlots: int(slots), Binding: DimensionSlots}
}

func TestNewRejectsUnsafeKataConfiguration(t *testing.T) {
	base := testConfig()
	for name, mutate := range map[string]func(*Config){
		"legacy runtime":         func(c *Config) { c.RuntimeClassName = "kata-qemu" },
		"canary runtime":         func(c *Config) { c.RuntimeClassName = "sjo-kata-qemu-probe" },
		"mutable runner image":   func(c *Config) { c.RunnerImage = "runner:latest" },
		"missing bootstrap port": func(c *Config) { c.BootstrapURL = "https://slipway-bootstrap.example.invalid" },
		"disk-backed budget":     func(c *Config) { c.Volumes.Work = 0 },
		"over-limit worker":      func(c *Config) { c.RunnerResources.Limits.CPUMilli = 1 },
		"undersized rootfs":      func(c *Config) { c.ContainerRootfsBytes = 1 },
	} {
		t.Run(name, func(t *testing.T) {
			config := base
			mutate(&config)
			if _, err := New(&fakeAPI{}, config); err == nil {
				t.Fatal("accepted unsafe Kubernetes/Kata configuration")
			}
		})
	}
}

// A capacity value passes two independent ceiling checks: RuntimeConfig.Validate
// in cmd/runner-pool, and this one at backend construction. This one read 6 while
// the CLI read 2, so it admitted three values the process would never reach while
// its error text claimed to enforce the Linux ceiling. The ceiling is sixteen
// (homelab#960), one worker per slot address .2 to .17. Seventeen is named
// literally rather than derived from the constant, so raising the ceiling past
// the slot set turns this red instead of moving with it. Sixteen is the positive
// control: a test that only asserts a refusal cannot tell a working bound from a
// config that never validates, and it goes red if the ceiling falls back to four.
func TestBackendCapacityAboveTheLinuxCeilingIsRefused(t *testing.T) {
	accepted := testConfig()
	accepted.Capacity = 16
	if _, err := New(&fakeAPI{}, accepted); err != nil {
		t.Fatalf("the Linux ceiling of sixteen was refused: %v", err)
	}

	for _, capacity := range []int{17, 18} {
		refused := testConfig()
		refused.Capacity = capacity
		if _, err := New(&fakeAPI{}, refused); err == nil {
			t.Fatalf("backend accepted capacity %d, above the Linux ceiling of %d", capacity, LinuxCapacityCeiling)
		}
	}
}

func TestNewAcceptsZeroCapacityMaintenanceConfiguration(t *testing.T) {
	config := testConfig()
	config.Capacity = 0
	config.LegacySlots = 0
	if _, err := New(&fakeAPI{}, config); err != nil {
		t.Fatalf("zero-capacity maintenance configuration was rejected: %v", err)
	}
}

func TestCreateBuildsOwnedPinnedDockerPodAndUsesTransientBootstrap(t *testing.T) {
	config := testConfig()
	intent, token := testIntent(config)
	fake := &fakeAPI{list: ListResult{Complete: true}, headroom: enoughHeadroom(config, 2)}
	fake.created = ownedResource(config, intent, "pod-uid-1", true, false, false)
	backend, err := New(fake, config)
	if err != nil {
		t.Fatal(err)
	}
	worker, err := backend.Create(context.Background(), intent)
	if err != nil {
		t.Fatal(err)
	}
	if worker.LeaseID != intent.LeaseID || worker.ResourceID != intent.ResourceID || len(fake.createReqs) != 1 {
		t.Fatalf("bad create result or mutation count: %+v requests=%d", worker, len(fake.createReqs))
	}
	request := fake.createReqs[0]
	if request.BootstrapMode != MemoryOnlyBootstrap || request.BootstrapToken != token {
		t.Fatal("create did not carry the transient memory-only bootstrap capability")
	}
	raw, err := json.Marshal(request.Spec)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), token.Encoded()) || strings.Contains(string(raw), "SLIPWAY_BOOTSTRAP_TOKEN") {
		t.Fatal("bootstrap capability entered the Pod spec")
	}
	spec := request.Spec
	if spec.RuntimeClassName != config.RuntimeClassName || spec.AutomountServiceAccountToken || spec.EnableServiceLinks || spec.HostNetwork || spec.HostPID || spec.HostIPC || spec.RestartPolicy != "Never" {
		t.Fatalf("unsafe Pod boundary: %+v", spec)
	}
	if spec.Annotations != nil || len(spec.Containers) != 2 || len(spec.Volumes) != 3 {
		t.Fatal("Pod omitted the fixed two-container memory-backed shape")
	}
	privileged := 0
	for _, container := range spec.Containers {
		if container.Privileged {
			privileged++
			if container.Name != "dockerd" {
				t.Fatal("non-Docker container is privileged")
			}
		}
		for _, env := range container.Environment {
			if env.Name == "SLIPWAY_BOOTSTRAP_TOKEN" {
				t.Fatal("bootstrap token was emitted as an environment value")
			}
		}
	}
	if privileged != 1 {
		t.Fatal("Docker sidecar privilege was not explicit")
	}
	for _, volume := range spec.Volumes {
		if volume.Medium != "Memory" || volume.SizeBytes <= 0 {
			t.Fatalf("volume is not memory-backed and bounded: %+v", volume)
		}
	}
	for _, container := range spec.Containers {
		for _, arg := range container.Args {
			if arg == token.Encoded() {
				t.Fatal("bootstrap token entered container arguments")
			}
		}
	}
	if _, err := backend.Create(context.Background(), intent); err == nil || len(fake.createReqs) != 1 {
		t.Fatal("repeated create mutation was allowed without an inventory-confirmed worker")
	}
}

func TestInventoryRequiresCompleteOwnedPhysicalScopeAndChargesHeadroom(t *testing.T) {
	config := testConfig()
	intent, _ := testIntent(config)
	fake := &fakeAPI{list: ListResult{Complete: true}, headroom: enoughHeadroom(config, 4)}
	fake.list.Items = []WorkerResource{ownedResource(config, intent, "pod-uid-1", true, true, true)}
	backend, err := New(fake, config)
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := backend.Inventory(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// One free slot, not four: physical headroom is deliberately generous here
	// so the configured capacity of two minus one active worker is what decides.
	if !inventory.Complete || inventory.FreeSlots != 1 || len(inventory.Workers) != 1 {
		t.Fatalf("unexpected inventory: %+v", inventory)
	}

	controls := map[string]func(*fakeAPI){
		"incomplete listing": func(f *fakeAPI) { f.list.Complete = false },
		"foreign owner":      func(f *fakeAPI) { f.list.Items[0].Labels[LabelOwnerUID] = "other-controller" },
		"duplicate lease":    func(f *fakeAPI) { f.list.Items = append(f.list.Items, f.list.Items[0]) },
		"negative headroom":  func(f *fakeAPI) { f.list.Items = f.list.Items[:1]; f.headroom.CPUMilli = -1 },
	}
	for name, mutate := range controls {
		t.Run(name, func(t *testing.T) {
			f := &fakeAPI{list: ListResult{Complete: true, Items: []WorkerResource{ownedResource(config, intent, "pod-uid-1", true, true, true)}}, headroom: enoughHeadroom(config, 4)}
			mutate(f)
			b, err := New(f, config)
			if err != nil {
				t.Fatal(err)
			}
			got, err := b.Inventory(context.Background())
			if err == nil || got.Complete {
				t.Fatalf("accepted unsafe inventory: %+v", got)
			}
		})
	}

	// A complete scope may report a pending Pod before its Kata process starts;
	// it remains charged and visible until the Pod and physical witnesses vanish.
	fake.list.Items[0] = ownedResource(config, intent, "pod-uid-1", true, false, false)
	inventory, err = backend.Inventory(context.Background())
	if err != nil || len(inventory.Workers) != 1 {
		t.Fatalf("pending Pod was not retained as a charged worker: %+v err=%v", inventory, err)
	}
}

// A gone Pod cannot come back, so an item that survives only in the Kata
// process or storage witness must report as terminated with exit -1 rather
// than the false Terminated:false that transport.go's orphan path always
// carries (#1109) -- once the debounce has run out (#1109 review). This is a
// positive control too: an item with no witness at all still gets omitted, and
// a normal live Pod stays untouched however long it is listed.
func TestInventoryReportsAPodGoneWitnessSeenWorkerAsTerminated(t *testing.T) {
	config := testConfig()
	intent, _ := testIntent(config)

	for name, testCase := range map[string]struct {
		pod, process, storage bool
		wantWorkers           int
		wantTerminated        bool
		wantExitCode          int
	}{
		"process witness survives the Pod": {
			pod: false, process: true, storage: false,
			wantWorkers: 1, wantTerminated: true, wantExitCode: -1,
		},
		"no witness at all is omitted": {
			pod: false, process: false, storage: false,
			wantWorkers: 0,
		},
		"a live Pod is unaffected": {
			pod: true, process: true, storage: true,
			wantWorkers: 1, wantTerminated: false, wantExitCode: 0,
		},
	} {
		t.Run(name, func(t *testing.T) {
			fake := &fakeAPI{list: ListResult{Complete: true}, headroom: enoughHeadroom(config, 4)}
			fake.list.Items = []WorkerResource{ownedResource(config, intent, "pod-uid-1", testCase.pod, testCase.process, testCase.storage)}
			backend, clock := newPodlessBackend(t, fake, config)
			// Three cycles a minute apart: the third is past both the miss
			// count and the grace, so it carries the settled verdict.
			var inventory controller.BackendInventory
			for cycle := 0; cycle < 3; cycle++ {
				if cycle > 0 {
					clock.at = clock.at.Add(time.Minute)
				}
				var err error
				if inventory, err = backend.Inventory(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			if len(inventory.Workers) != testCase.wantWorkers {
				t.Fatalf("unexpected worker count: %+v", inventory.Workers)
			}
			if testCase.wantWorkers == 0 {
				return
			}
			worker := inventory.Workers[0]
			if worker.Terminated != testCase.wantTerminated || worker.ExitCode != testCase.wantExitCode {
				t.Fatalf("unexpected termination report: terminated=%v exit=%d, want terminated=%v exit=%d",
					worker.Terminated, worker.ExitCode, testCase.wantTerminated, testCase.wantExitCode)
			}
		})
	}
}

// newPodlessBackend builds a backend whose clock the test drives.
func newPodlessBackend(t *testing.T, api API, config Config) (*Backend, *fakeClock) {
	t.Helper()
	backend, err := New(api, config)
	if err != nil {
		t.Fatal(err)
	}
	clock := &fakeClock{at: time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC)}
	backend.now = clock.now
	return backend, clock
}

// podlessTerminated runs one Inventory at the clock's current time and reports
// whether the single listed worker came back terminated with exit -1.
func podlessTerminated(t *testing.T, backend *Backend) bool {
	t.Helper()
	inventory, err := backend.Inventory(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(inventory.Workers) != 1 {
		t.Fatalf("unexpected worker count: %+v", inventory.Workers)
	}
	worker := inventory.Workers[0]
	if worker.Terminated != (worker.ExitCode == -1) {
		t.Fatalf("terminated=%v with exit=%d: the orphan verdict must carry exit -1", worker.Terminated, worker.ExitCode)
	}
	return worker.Terminated
}

// ONE POD-LESS READ MUST NOT RETIRE A LEASE (#1109 review). The verdict tears
// down a lease that may be running a real job, so it needs both the miss count
// and the grace; either alone is not enough.
func TestAPodlessWorkerIsTerminatedOnlyAfterTheMissCountAndTheGrace(t *testing.T) {
	config := testConfig()
	intent, _ := testIntent(config)
	fake := &fakeAPI{list: ListResult{Complete: true, Items: []WorkerResource{
		ownedResource(config, intent, "pod-uid-1", false, true, true)}}, headroom: enoughHeadroom(config, 4)}
	backend, clock := newPodlessBackend(t, fake, config)

	if podlessTerminated(t, backend) {
		t.Fatal("a single Pod-less read reported the worker terminated")
	}
	// The miss count is met here, the grace is not.
	for step := 0; step < 3; step++ {
		clock.at = clock.at.Add(10 * time.Second)
		if podlessTerminated(t, backend) {
			t.Fatalf("reported terminated %s after the first Pod-less read, inside the grace", clock.at.Sub(time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC)))
		}
	}
	clock.at = time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC).Add(podlessWorkerGrace)
	if !podlessTerminated(t, backend) {
		t.Fatal("a worker Pod-less past both the miss count and the grace was not reported terminated")
	}
}

// A Pod that is listed again clears the state, so a later Pod-less spell starts
// its own count and grace from scratch.
func TestAPodThatReappearsResetsThePodlessDebounce(t *testing.T) {
	config := testConfig()
	intent, _ := testIntent(config)
	podless := ownedResource(config, intent, "pod-uid-1", false, true, true)
	live := ownedResource(config, intent, "pod-uid-1", true, true, true)
	fake := &fakeAPI{list: ListResult{Complete: true, Items: []WorkerResource{podless}}, headroom: enoughHeadroom(config, 4)}
	backend, clock := newPodlessBackend(t, fake, config)

	for step := 0; step < 2; step++ {
		if podlessTerminated(t, backend) {
			t.Fatal("reported terminated before the grace")
		}
		clock.at = clock.at.Add(time.Minute)
	}
	fake.list.Items = []WorkerResource{live}
	if podlessTerminated(t, backend) {
		t.Fatal("a listed Pod reported terminated")
	}
	clock.at = clock.at.Add(time.Minute)
	fake.list.Items = []WorkerResource{podless}
	// Four minutes after the first Pod-less read: without the reset, this is
	// the fourth miss and well past the grace.
	if podlessTerminated(t, backend) {
		t.Fatal("the Pod reappearing did not reset the debounce")
	}
	clock.at = clock.at.Add(podlessWorkerGrace)
	if !podlessTerminated(t, backend) {
		t.Fatal("the fresh Pod-less spell never reached its verdict")
	}
}

// A worker that leaves the listing entirely also clears the state (#1109
// review), same as a Pod reappearing above: nextPodless only ever carries
// items seen on the current read, so an absent item's entry is dropped when
// b.podless is replaced whole.
func TestAWorkerAbsentFromTheListingResetsThePodlessDebounce(t *testing.T) {
	config := testConfig()
	intent, _ := testIntent(config)
	podless := ownedResource(config, intent, "pod-uid-1", false, true, true)
	fake := &fakeAPI{list: ListResult{Complete: true, Items: []WorkerResource{podless}}, headroom: enoughHeadroom(config, 4)}
	backend, clock := newPodlessBackend(t, fake, config)

	if podlessTerminated(t, backend) {
		t.Fatal("a single Pod-less read reported the worker terminated")
	}

	clock.at = clock.at.Add(podlessWorkerGrace)
	fake.list.Items = nil
	inventory, err := backend.Inventory(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(inventory.Workers) != 0 {
		t.Fatalf("a worker absent from the listing was still reported: %+v", inventory.Workers)
	}

	fake.list.Items = []WorkerResource{podless}
	// Without the reset, the stale entry from the first read already meets
	// both the miss count and the grace by now, and would terminate here.
	if podlessTerminated(t, backend) {
		t.Fatal("a worker leaving the listing did not reset the Pod-less debounce")
	}
}

// Create's pre-mutation read is not a reconcile cycle, so it must not count as
// a miss; otherwise every admission would shorten the debounce.
func TestCreateInventoryDoesNotAdvanceThePodlessDebounce(t *testing.T) {
	config := testConfig()
	intent, _ := testIntent(config)
	fake := &fakeAPI{list: ListResult{Complete: true, Items: []WorkerResource{
		ownedResource(config, intent, "pod-uid-1", false, true, true)}}, headroom: enoughHeadroom(config, 4)}
	backend, _ := newPodlessBackend(t, fake, config)
	if podlessTerminated(t, backend) {
		t.Fatal("a single Pod-less read reported the worker terminated")
	}
	// Create finds the existing resource in its pre-mutation read and returns
	// it without a POST, so only that read runs.
	for step := 0; step < 3; step++ {
		if _, err := backend.Create(context.Background(), intent); err != nil {
			t.Fatal(err)
		}
	}
	if len(fake.createReqs) != 0 {
		t.Fatalf("Create posted %d Pods for a resource already listed", len(fake.createReqs))
	}
	if misses := backend.podless["pod-uid-1"].misses; misses != 1 {
		t.Fatalf("misses=%d after one reconcile read and three create reads, want 1", misses)
	}
}

func TestDestroyUsesOneUIDBoundMutationAndRequiresBothAbsenceWitnesses(t *testing.T) {
	config := testConfig()
	intent, _ := testIntent(config)
	fake := &fakeAPI{list: ListResult{Complete: true, Items: []WorkerResource{ownedResource(config, intent, "pod-uid-1", true, true, true)}}, headroom: enoughHeadroom(config, 2), witness: AbsenceWitness{UID: "pod-uid-1", ProcessAbsent: true, StorageAbsent: false}}
	backend, err := New(fake, config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := backend.Inventory(context.Background()); err != nil {
		t.Fatal(err)
	}
	absence, err := backend.Destroy(context.Background(), controller.WorkerObservation{LeaseID: intent.LeaseID, ResourceID: intent.ResourceID})
	if err != nil || absence.ProcessAbsent != true || absence.StorageAbsent != false || len(fake.deleteReqs) != 1 {
		t.Fatalf("unexpected partial cleanup witness: %+v err=%v deletes=%d", absence, err, len(fake.deleteReqs))
	}
	request := fake.deleteReqs[0]
	if request.UID != "pod-uid-1" || request.Namespace != config.Namespace || request.Labels[LabelOwnerUID] != config.OwnerUID || request.Labels[LabelLeaseID] != intent.LeaseID {
		t.Fatalf("delete was not bound to the reconciled owned identity: %+v", request)
	}
	// A REPEAT IS SENT, AND IT IS THE SAME EXACT MUTATION (homelab#1094). This
	// used to be refused, so a delete that failed once was never re-sent and
	// the Pod held its slot for good. The controller paces repeats; what the
	// adapter owes is that a repeat cannot reach any other Pod: same name,
	// same UID precondition, same owned labels.
	if _, err := backend.Destroy(context.Background(), controller.WorkerObservation{LeaseID: intent.LeaseID, ResourceID: intent.ResourceID}); err != nil || len(fake.deleteReqs) != 2 {
		t.Fatalf("a repeated delete was not sent: err=%v deletes=%d", err, len(fake.deleteReqs))
	}
	if again := fake.deleteReqs[1]; again.UID != request.UID || again.Name != request.Name || again.Namespace != request.Namespace ||
		again.Labels[LabelLeaseID] != request.Labels[LabelLeaseID] || again.Labels[LabelOwnerUID] != request.Labels[LabelOwnerUID] {
		t.Fatalf("the repeated delete was not bound to the same identity: first %+v, repeat %+v", request, again)
	}
}

// After a failed delete the next attempt is still UID-bound, and a worker
// whose UID this process never learned is refused rather than deleted by name.
func TestAFailedDeleteCanBeSentAgainAndAnUnknownUIDIsStillRefused(t *testing.T) {
	config := testConfig()
	intent, _ := testIntent(config)
	fake := &fakeAPI{list: ListResult{Complete: true, Items: []WorkerResource{ownedResource(config, intent, "pod-uid-1", true, true, true)}},
		headroom: enoughHeadroom(config, 2), deleteErr: errors.New("Kubernetes Pod delete outcome is uncertain")}
	backend, err := New(fake, config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := backend.Inventory(context.Background()); err != nil {
		t.Fatal(err)
	}
	worker := controller.WorkerObservation{LeaseID: intent.LeaseID, ResourceID: intent.ResourceID}
	if _, err := backend.Destroy(context.Background(), worker); err == nil {
		t.Fatal("fixture: the first delete did not fail")
	}
	fake.deleteErr = nil
	fake.witness = AbsenceWitness{UID: "pod-uid-1", ProcessAbsent: true, StorageAbsent: true}
	absence, err := backend.Destroy(context.Background(), worker)
	if err != nil || !absence.ProcessAbsent || !absence.StorageAbsent || len(fake.deleteReqs) != 2 || fake.deleteReqs[1].UID != "pod-uid-1" {
		t.Fatalf("the retry after a failed delete did not prove absence: %+v err=%v deletes=%d", absence, err, len(fake.deleteReqs))
	}

	// A fresh backend (a restarted controller) whose inventory no longer lists
	// the Pod has no UID for it and must not delete by name alone.
	fake.list = ListResult{Complete: true}
	restarted, err := New(fake, config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.Inventory(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.Destroy(context.Background(), worker); err == nil || len(fake.deleteReqs) != 2 {
		t.Fatalf("a delete without a known UID was sent: err=%v deletes=%d", err, len(fake.deleteReqs))
	}
}

// The ordering claim the lifecycle relies on, asserted rather than trusted:
// when the backend refuses for headroom, NO Pod was requested. That is what
// makes releasing the reservation safe.
func TestNoHeadroomRefusesBeforeAnyPodIsRequested(t *testing.T) {
	config := testConfig()
	intent, _ := testIntent(config)
	fake := &fakeAPI{list: ListResult{Complete: true}, headroom: enoughHeadroom(config, 0)}
	fake.headroom.FreeSlots = 0
	fake.headroom.Binding = DimensionMemory
	backend, err := New(fake, config)
	if err != nil {
		t.Fatal(err)
	}
	_, err = backend.Create(context.Background(), intent)
	if err == nil {
		t.Fatal("a full node accepted a create")
	}
	if !errors.Is(err, controller.ErrCapacity) {
		t.Fatalf("a full node did not report a capacity wait: %v", err)
	}
	if !strings.Contains(err.Error(), "short="+DimensionMemory) {
		t.Fatalf("the binding dimension was not named: %v", err)
	}
	if len(fake.createReqs) != 0 {
		t.Fatalf("a Pod was requested before the headroom refusal: %d request(s)", len(fake.createReqs))
	}
	// And the attempt marker is unset, so the next cycle may retry rather than
	// hitting "creation was already attempted".
	fake.headroom = enoughHeadroom(config, 2)
	fake.created = ownedResource(config, intent, "pod-uid-retry", true, false, false)
	if _, err := backend.Create(context.Background(), intent); err != nil {
		t.Fatalf("the retry after a capacity wait was refused: %v", err)
	}
}

// The backend half of the retirement invariant: an oversubscribed host yields a
// COMPLETE inventory with zero free slots, so the controller can still see --
// and retire -- the workers it owns.
func TestOversubscribedHeadroomStillListsOwnedWorkers(t *testing.T) {
	config := testConfig()
	intent, _ := testIntent(config)
	fake := &fakeAPI{
		list:     ListResult{Complete: true, Items: []WorkerResource{ownedResource(config, intent, "pod-uid-live", true, true, true)}},
		headroom: enoughHeadroom(config, 0),
	}
	fake.headroom.FreeSlots = 0
	fake.headroom.Binding = DimensionMemory
	backend, err := New(fake, config)
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := backend.Inventory(context.Background())
	if err != nil {
		t.Fatalf("an oversubscribed host fenced the inventory: %v", err)
	}
	if !inventory.Complete {
		t.Fatal("the inventory was incomplete, so no worker can be retired to recover")
	}
	if inventory.FreeSlots != 0 {
		t.Fatalf("admission was not fenced: %d free slots", inventory.FreeSlots)
	}
	if inventory.Binding != DimensionMemory {
		t.Fatalf("the binding dimension did not reach the controller: %q", inventory.Binding)
	}
	if len(inventory.Workers) != 1 || inventory.Workers[0].ResourceID != intent.ResourceID {
		t.Fatalf("the owned worker is missing from the inventory: %+v", inventory.Workers)
	}
}

// (nit a) The slot-verdict refusal, pinned. The witness clamps an
// oversubscribed host to zero before this point, so a negative FreeSlots or an
// unnamed dimension arriving here means an adapter disagreed with its own
// contract -- unknown, not full, and fail-closed.
func TestHeadroomWithoutATrustedSlotVerdictIsRefused(t *testing.T) {
	config := testConfig()
	backend, err := New(&fakeAPI{}, config)
	if err != nil {
		t.Fatal(err)
	}
	for _, testCase := range []struct {
		name   string
		break_ func(*Headroom)
	}{
		{"negative slot verdict", func(h *Headroom) { h.FreeSlots = -1 }},
		{"unnamed dimension", func(h *Headroom) { h.Binding = "" }},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			headroom := enoughHeadroom(config, 2)
			testCase.break_(&headroom)
			free, binding, err := backend.freeSlots(headroom, 0)
			if err == nil {
				t.Fatalf("an adapter that broke its own contract was accepted: free=%d binding=%q", free, binding)
			}
			if free != 0 || binding != DimensionUnknown {
				t.Fatalf("the refusal did not fail closed: free=%d binding=%q", free, binding)
			}
		})
	}
}

// (nit d) Already-attempted is decided BEFORE capacity, so a resource whose Pod
// POST has gone out can never come back as ErrCapacity -- which the lifecycle
// reads as "nothing was created, release the reservation".
func TestASecondCreateIsAlreadyAttemptedEvenOnAFullNode(t *testing.T) {
	config := testConfig()
	intent, _ := testIntent(config)
	fake := &fakeAPI{list: ListResult{Complete: true}, headroom: enoughHeadroom(config, 2)}
	fake.created = ownedResource(config, intent, "pod-uid-first", true, false, false)
	backend, err := New(fake, config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := backend.Create(context.Background(), intent); err != nil {
		t.Fatal(err)
	}
	// The node fills up between the two calls, which is exactly when the old
	// ordering would have answered ErrCapacity for a Pod that already exists.
	fake.headroom = enoughHeadroom(config, 0)
	fake.headroom.FreeSlots = 0
	fake.headroom.Binding = DimensionMemory
	fake.list = ListResult{Complete: true}
	_, err = backend.Create(context.Background(), intent)
	if err == nil {
		t.Fatal("a second create for the same resource was accepted")
	}
	if errors.Is(err, controller.ErrCapacity) {
		t.Fatalf("an already-created resource reported a capacity wait; the lifecycle would release its reservation: %v", err)
	}
	if !strings.Contains(err.Error(), "already attempted") {
		t.Fatalf("the second create did not report the attempt: %v", err)
	}
}

// THE FIRST REAL JOB DIED HERE. Set up job, checkout and "Identify the worker"
// all passed on a Kata worker; `docker version` then reported
// `permission denied while trying to connect to the docker API at
// unix:///var/run/docker.sock`.
//
// The dind image's own `docker` group is gid 2375 and it has no entry for 121
// at all, so the default socket is root:2375 mode 0660 while the runner is
// 1001:121. Different group, no world bits, denied.
func TestTheDockerSocketIsOwnedByTheGroupTheRunnerRunsAs(t *testing.T) {
	config := testConfig()
	intent, _ := testIntent(config)
	spec := (&Backend{config: config}).podSpec(intent)
	dockerd := spec.Containers[1]
	if dockerd.Name != "dockerd" {
		t.Fatalf("fixture drifted: second container is %q", dockerd.Name)
	}
	want := "--group=" + strconv.Itoa(workerDockerGID)
	if !slices.Contains(dockerd.Args, want) {
		t.Fatalf("dockerd does not hand its socket to the runner's group: %v", dockerd.Args)
	}
}

// One declaration. The gid lives in the worker image (`USER 1001:121`) and is
// recorded in the image lock's job_user; a second literal in Go would drift
// from it silently and the symptom would be a permission denial three steps
// into a real job.
func TestTheDockerGroupMatchesTheWorkerImagesJobUser(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "image", "linux", "image-lock.json"))
	if err != nil {
		t.Fatal(err)
	}
	var lock struct {
		JobUser struct {
			UID int `json:"uid"`
			GID int `json:"gid"`
		} `json:"job_user"`
	}
	if err := json.Unmarshal(raw, &lock); err != nil {
		t.Fatal(err)
	}
	if lock.JobUser.GID == 0 {
		t.Fatal("the image lock declares no job_user gid; this test would be vacuous")
	}
	if workerDockerGID != lock.JobUser.GID {
		t.Fatalf("dockerd is handed gid %d but the worker image runs as gid %d",
			workerDockerGID, lock.JobUser.GID)
	}
}

// The spec and its validator must be built from the same vector, or a Pod this
// adapter creates fails the check this adapter makes.
func TestTheObservedPodValidatorAcceptsTheSpecWeCreate(t *testing.T) {
	config := testConfig()
	intent, _ := testIntent(config)
	transport := &Transport{worker: config}
	pod := transport.toPod((&Backend{config: config}).podSpec(intent))
	pod.UID = "pod-uid-args"
	if err := transport.validateObservedPod(*pod); err != nil {
		t.Fatalf("the adapter's own Pod spec failed its own validator: %v", err)
	}
}

// homelab#983. dockerd's data root moved off the RAM emptyDir and onto a path
// on its own devmapper root filesystem. The image declares
// `VOLUME /var/lib/docker`, so leaving the root there puts it in guest RAM
// even with no volume mounted; the root has to be somewhere else. The literal
// is written out here on purpose: the agreement test compares the policy to
// dockerdArgs(), and this one pins what dockerdArgs() itself says.
func TestDockerdKeepsItsDataRootOnItsOwnDisk(t *testing.T) {
	want := []string{"dockerd", "--host=unix:///var/run/docker.sock", "--data-root=/var/lib/sjo-docker",
		"--exec-root=/var/run/docker", "--storage-driver=overlay2", "--ip6tables=false", "--group=121"}
	if got := dockerdArgs(); !slices.Equal(got, want) {
		t.Fatalf("dockerd argv is %q, want %q", got, want)
	}
	config := testConfig()
	intent, _ := testIntent(config)
	for _, container := range (&Backend{config: config}).podSpec(intent).Containers {
		for _, arg := range container.Args {
			if strings.HasPrefix(arg, "--data-root=/var/lib/docker") {
				t.Fatalf("%s still puts the data root under the image's /var/lib/docker VOLUME: %q", container.Name, arg)
			}
		}
	}
}

// The worker Pod carries exactly three memory-backed volumes. The `docker`
// volume is gone, and nothing is mounted at the data root: a mount there would
// replace the devmapper disk with whatever the volume is.
func TestTheWorkerPodHasExactlyWorkSocketAndTmp(t *testing.T) {
	config := testConfig()
	intent, _ := testIntent(config)
	spec := (&Backend{config: config}).podSpec(intent)
	var names []string
	for _, volume := range spec.Volumes {
		names = append(names, volume.Name)
		if volume.Medium != "Memory" || volume.SizeBytes <= 0 {
			t.Fatalf("volume %s is not memory-backed and bounded: %+v", volume.Name, volume)
		}
	}
	if !slices.Equal(names, []string{"work", "socket", "tmp"}) {
		t.Fatalf("worker volumes are %q, want exactly work, socket, tmp", names)
	}
	for _, container := range spec.Containers {
		for _, mount := range container.Mounts {
			for _, root := range []string{"/var/lib/sjo-docker", "/var/lib/docker"} {
				if mount.MountPath == root || strings.HasPrefix(mount.MountPath, root+"/") {
					t.Fatalf("%s mounts %s at %s", container.Name, mount.Name, mount.MountPath)
				}
			}
		}
	}
}

// T0 (docs/evidence/2026-09-25-runner-pool-worker-thin-devices/): a worker has
// THREE writable 10 GiB thin devices -- pause, runner and dockerd -- and
// dockerd is privileged in the guest, so the pause device is charged too. With
// the production figures (10 GiB rootfs, 0.5 GiB runtime overhead) that is
// 3 x 10 + 0.5 = 30.5 GiB, not the 20.5 GiB two devices gave.
func TestTheStorageChargeIsThreeWritableDevicesPlusOverhead(t *testing.T) {
	config := testConfig()
	config.ContainerRootfsBytes = 10 << 30
	if err := config.validate(); err != nil {
		t.Fatalf("fixture drifted: %v", err)
	}
	const want = int64(30<<30 + 512<<20)
	if got := (&Backend{config: config}).perWorkerFootprint().StorageBytes; got != want {
		t.Fatalf("per-worker storage charge is %d bytes (%.1f GiB), want %d (30.5 GiB)", got, float64(got)/(1<<30), want)
	}
}

// homelab#1213. A host witness refusal while Create reads its inventory is
// tagged ErrWitnessRefusedBeforeCreate, which the lifecycle reads as "nothing
// was created": no provisioning_failures row, the intent rolled back. The tag
// is only true because the read precedes the attempt marker and the POST, so
// these pin both halves -- no Pod requested, and no tag on any other failure.
func TestAWitnessRefusalBeforeThePostIsTaggedAndRequestsNoPod(t *testing.T) {
	config := testConfig()
	intent, _ := testIntent(config)
	refusal := &hostWitnessRefusal{reason: WitnessReasonStorage, failure: "collector failed (62 bytes)"}
	fake := &fakeAPI{headroom: enoughHeadroom(config, 2), listErr: fmt.Errorf("stage=%s: %w", inventoryStageListWitness, refusal)}
	backend, err := New(fake, config)
	if err != nil {
		t.Fatal(err)
	}
	_, err = backend.Create(context.Background(), intent)
	if !errors.Is(err, controller.ErrWitnessRefusedBeforeCreate) {
		t.Fatalf("a pre-create witness refusal was not tagged: %v", err)
	}
	if !strings.Contains(err.Error(), "reason=storage") {
		t.Fatalf("the witness's reason was lost from the error: %v", err)
	}
	if len(fake.createReqs) != 0 {
		t.Fatalf("a Pod was requested before the refusal: %d request(s)", len(fake.createReqs))
	}
	// No attempt marker, so the next cycle's create is a create, not "already attempted".
	fake.listErr, fake.list = nil, ListResult{Complete: true}
	fake.created = ownedResource(config, intent, "pod-uid-retry", true, false, false)
	if _, err := backend.Create(context.Background(), intent); err != nil {
		t.Fatalf("the retry after a witness refusal was refused: %v", err)
	}
}

func TestOnlyAPreCreateWitnessRefusalIsTagged(t *testing.T) {
	config := testConfig()
	for _, testCase := range []struct {
		name               string
		listErr, createErr error
	}{
		// A witness the controller could not reach is not a refusal.
		{"unreachable witness", errors.New("connect to host witness: connection refused"), nil},
		// Only the storage race is exempt; the other collectors keep counting.
		{"process refusal", &hostWitnessRefusal{reason: WitnessReasonProcesses, failure: "collector failed (40 bytes)"}, nil},
		{"capacity refusal", &hostWitnessRefusal{reason: WitnessReasonCapacity, failure: "collector failed (40 bytes)"}, nil},
		// After the POST a refusal says nothing about whether a Pod exists.
		{"refusal after the POST", nil, &hostWitnessRefusal{reason: WitnessReasonStorage, failure: "collector failed (62 bytes)"}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			intent, _ := testIntent(config)
			fake := &fakeAPI{list: ListResult{Complete: true}, headroom: enoughHeadroom(config, 2),
				listErr: testCase.listErr, createErr: testCase.createErr}
			backend, err := New(fake, config)
			if err != nil {
				t.Fatal(err)
			}
			_, err = backend.Create(context.Background(), intent)
			if err == nil {
				t.Fatal("the create succeeded")
			}
			if errors.Is(err, controller.ErrWitnessRefusedBeforeCreate) {
				t.Fatalf("tagged as nothing-created; the lifecycle would skip the failure count: %v", err)
			}
		})
	}
}

// A retry of a create whose POST already went out reads its inventory before
// the already-attempted check. A storage refusal there must not be tagged: the
// lifecycle would roll the intent back while a Pod may exist.
func TestAStorageRefusalOnAnAlreadyAttemptedCreateIsNotTagged(t *testing.T) {
	config := testConfig()
	intent, _ := testIntent(config)
	fake := &fakeAPI{list: ListResult{Complete: true}, headroom: enoughHeadroom(config, 2),
		createErr: errors.New("lost create response")}
	backend, err := New(fake, config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := backend.Create(context.Background(), intent); err == nil || len(fake.createReqs) != 1 {
		t.Fatalf("the first create did not POST and fail: err=%v posts=%d", err, len(fake.createReqs))
	}
	fake.listErr = &hostWitnessRefusal{reason: WitnessReasonStorage, failure: "collector failed (62 bytes)"}
	_, err = backend.Create(context.Background(), intent)
	if err == nil {
		t.Fatal("the retry succeeded")
	}
	if errors.Is(err, controller.ErrWitnessRefusedBeforeCreate) {
		t.Fatalf("a refusal on an already-POSTed resource was tagged nothing-created: %v", err)
	}
}

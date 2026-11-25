package kubernetes

import (
	"context"
	"errors"
	"testing"

	"github.com/cameronsjo/slipway/internal/controller"
)

type fakePodSource struct {
	scope PodScope
	err   error
}

func (f *fakePodSource) ListPods(context.Context, string, string) (PodScope, error) {
	return f.scope, f.err
}

type fakeProcessSource struct {
	scope WitnessScope
	err   error
}

func (f *fakeProcessSource) ListProcesses(context.Context, string, string) (WitnessScope, error) {
	return f.scope, f.err
}

type fakeStorageSource struct {
	scope WitnessScope
	err   error
}

func (f *fakeStorageSource) ListWritableStorage(context.Context, string, string) (WitnessScope, error) {
	return f.scope, f.err
}

type fakeCapacitySource struct {
	snapshot CapacitySnapshot
	err      error
}

func (f *fakeCapacitySource) ObserveCapacity(context.Context, string) (CapacitySnapshot, error) {
	return f.snapshot, f.err
}

func physicalBudget(config Config) AdmissionBudget {
	return AdmissionBudget{
		MaxWorkers: config.Capacity, LegacySlots: config.LegacySlots,
		PerWorker:                (&Backend{config: config}).perWorkerFootprint(),
		ContainerRootfsBytes:     config.ContainerRootfsBytes,
		PerWorkerImageCacheBytes: 2 << 30, PerWorkerLogBytes: 256 << 20,
	}
}

func physicalCapacity() CapacitySnapshot {
	return CapacitySnapshot{
		Complete:      true,
		CPUMilliTotal: 20000, CPUMilliOtherWorkloads: 1000, CPUMilliRuntimeOverhead: 500,
		MemoryBytesTotal: 64 << 30, MemoryBytesAvailable: 60 << 30, MemoryBytesOtherWorkloads: 4 << 30, MemoryBytesRuntimeOverhead: 1 << 30,
		WritableStorageBytesTotal: 400 << 30, WritableStorageBytesOtherWorkloads: 10 << 30, WritableStorageBytesRuntimeOverhead: 2 << 30,
		ContainerRootfsBytes: 20 << 30,
		ImageCacheBytesTotal: 20 << 30, ImageCacheBytesUsed: 4 << 30,
		LogBytesTotal: 4 << 30, LogBytesUsed: 128 << 20,
	}
}

func physicalSources(pods PodScope, processes, storage WitnessScope, capacity CapacitySnapshot) TrustedPhysicalSources {
	return TrustedPhysicalSources{
		Pods: &fakePodSource{scope: pods}, Processes: &fakeProcessSource{scope: processes},
		Storage: &fakeStorageSource{scope: storage}, Capacity: &fakeCapacitySource{snapshot: capacity},
	}
}

func podObject(resource WorkerResource) PodObject {
	return PodObject{Namespace: resource.Namespace, Name: resource.Name, UID: resource.UID,
		Labels: resource.Labels, Annotations: resource.Annotations, RuntimeClassName: resource.RuntimeClassName}
}

func physicalWitness(resource WorkerResource) PhysicalWitness {
	return PhysicalWitness{Namespace: resource.Namespace, Name: resource.Name, UID: resource.UID,
		Labels: resource.Labels, RuntimeClassName: resource.RuntimeClassName}
}

func newPhysicalInventory(t *testing.T, sources TrustedPhysicalSources) *TrustedInventory {
	t.Helper()
	config := testConfig()
	inventory, err := NewTrustedInventory(sources, config, physicalBudget(config))
	if err != nil {
		t.Fatal(err)
	}
	return inventory
}

// Two resources, not three, because the test config's capacity is two (#722 S9
// set the Linux ceiling to two; homelab#926 raised it to four). A
// process orphan and a storage orphan are still both represented -- the second
// resource is pod-absent with process present, and the owned one carries all
// three scopes -- so the merge across pod, CRI and storage is still exercised.
// The three-resource case is now an overcommit, and it has its own test below.
func TestPhysicalInventoryMergesPodCRIStorageAndHeadroom(t *testing.T) {
	config := testConfig()
	intent := controller.WorkerCreateIntent{LeaseID: "11111111-1111-4111-8111-111111111111", ResourceID: "slipway-linux-11111111-1111-4111-8111-111111111111"}
	owned := ownedResource(config, intent, "pod-uid-1", true, true, true)
	orphanIntent := controller.WorkerCreateIntent{LeaseID: "22222222-2222-4222-8222-222222222222", ResourceID: "slipway-linux-22222222-2222-4222-8222-222222222222"}
	orphan := ownedResource(config, orphanIntent, "pod-uid-2", false, true, false)
	storageOrphanIntent := controller.WorkerCreateIntent{LeaseID: "33333333-3333-4333-8333-333333333333", ResourceID: "slipway-linux-33333333-3333-4333-8333-333333333333"}
	storageOrphan := ownedResource(config, storageOrphanIntent, "pod-uid-3", false, false, true)
	inventory := newPhysicalInventory(t, physicalSources(
		PodScope{Complete: true, Items: []PodObject{podObject(owned)}},
		WitnessScope{Complete: true, Items: []PhysicalWitness{physicalWitness(owned), physicalWitness(orphan)}},
		WitnessScope{Complete: true, Items: []PhysicalWitness{physicalWitness(owned)}},
		physicalCapacity()))
	report, err := inventory.Observe(context.Background())
	if err != nil || !report.Complete || len(report.Resources) != 2 {
		t.Fatalf("merged report was incomplete or dropped an orphan: %+v %v", report, err)
	}
	if report.Headroom.FreeSlots != 0 || report.Headroom.ImageCacheBytes != 12<<30 || report.Headroom.LogBytes != 3456<<20 {
		t.Fatalf("headroom did not account for CPU/RAM/storage/cache/log dimensions: %+v", report.Headroom)
	}
	backendInventory, err := inventory.ControllerInventory(context.Background())
	if err != nil || !backendInventory.Complete || len(backendInventory.Workers) != 2 || backendInventory.FreeSlots != 0 {
		t.Fatalf("controller inventory dropped physical orphans: %+v %v", backendInventory, err)
	}

	// Three simultaneous owned resources exceed the capacity of two, so the
	// report fails closed rather than reporting a slot count it cannot justify.
	overcommitted := newPhysicalInventory(t, physicalSources(
		PodScope{Complete: true, Items: []PodObject{podObject(owned)}},
		WitnessScope{Complete: true, Items: []PhysicalWitness{physicalWitness(owned), physicalWitness(orphan)}},
		WitnessScope{Complete: true, Items: []PhysicalWitness{physicalWitness(owned), physicalWitness(storageOrphan)}},
		physicalCapacity()))
	if report, err := overcommitted.Observe(context.Background()); err == nil || report.Complete {
		t.Fatalf("three owned resources on a capacity-of-two pool reported complete: %+v %v", report, err)
	}
}

func TestPhysicalInventoryFailsClosedForPartialScopesAnd404(t *testing.T) {
	config := testConfig()
	intent := controller.WorkerCreateIntent{LeaseID: "11111111-1111-4111-8111-111111111111", ResourceID: "slipway-linux-11111111-1111-4111-8111-111111111111"}
	owned := ownedResource(config, intent, "pod-uid-1", true, true, true)
	cases := map[string]func(TrustedPhysicalSources){
		"pods":      func(s TrustedPhysicalSources) { s.Pods.(*fakePodSource).scope.Complete = false },
		"processes": func(s TrustedPhysicalSources) { s.Processes.(*fakeProcessSource).scope.Complete = false },
		"storage":   func(s TrustedPhysicalSources) { s.Storage.(*fakeStorageSource).scope.Complete = false },
		"capacity":  func(s TrustedPhysicalSources) { s.Capacity.(*fakeCapacitySource).snapshot.Complete = false },
		"pod 404":   func(s TrustedPhysicalSources) { s.Pods.(*fakePodSource).err = errors.New("pod list: 404 NotFound") },
		"cache overfull": func(s TrustedPhysicalSources) {
			capacity := s.Capacity.(*fakeCapacitySource)
			capacity.snapshot.ImageCacheBytesUsed = capacity.snapshot.ImageCacheBytesTotal + 1
		},
		"logs overfull": func(s TrustedPhysicalSources) {
			capacity := s.Capacity.(*fakeCapacitySource)
			capacity.snapshot.LogBytesUsed = capacity.snapshot.LogBytesTotal + 1
		},
		"negative other workload": func(s TrustedPhysicalSources) {
			s.Capacity.(*fakeCapacitySource).snapshot.CPUMilliOtherWorkloads = -1
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			sources := physicalSources(
				PodScope{Complete: true, Items: []PodObject{podObject(owned)}},
				WitnessScope{Complete: true, Items: []PhysicalWitness{physicalWitness(owned)}},
				WitnessScope{Complete: true, Items: []PhysicalWitness{physicalWitness(owned)}}, physicalCapacity())
			mutate(sources)
			got, err := newPhysicalInventory(t, sources).ControllerInventory(context.Background())
			if err == nil || got.Complete {
				t.Fatalf("partial/404 scope was treated as absence: %+v %v", got, err)
			}
		})
	}
}

func TestPhysicalInventoryRejectsDuplicateOrChangedWitnessIdentity(t *testing.T) {
	config := testConfig()
	intent := controller.WorkerCreateIntent{LeaseID: "11111111-1111-4111-8111-111111111111", ResourceID: "slipway-linux-11111111-1111-4111-8111-111111111111"}
	owned := ownedResource(config, intent, "pod-uid-1", true, true, true)
	duplicate := physicalSources(
		PodScope{Complete: true, Items: []PodObject{podObject(owned)}},
		WitnessScope{Complete: true, Items: []PhysicalWitness{physicalWitness(owned), physicalWitness(owned)}},
		WitnessScope{Complete: true, Items: []PhysicalWitness{physicalWitness(owned)}}, physicalCapacity())
	if _, err := newPhysicalInventory(t, duplicate).Observe(context.Background()); err == nil {
		t.Fatal("duplicate CRI witness was accepted")
	}
	changed := owned
	changed.UID = "different-pod-uid"
	changedSources := physicalSources(
		PodScope{Complete: true, Items: []PodObject{podObject(owned)}},
		WitnessScope{Complete: true, Items: []PhysicalWitness{physicalWitness(changed)}},
		WitnessScope{Complete: true, Items: []PhysicalWitness{physicalWitness(owned)}}, physicalCapacity())
	if _, err := newPhysicalInventory(t, changedSources).Observe(context.Background()); err == nil {
		t.Fatal("changed process identity was accepted")
	}
}

func TestPhysicalInventoryConfirmAbsentRequiresBothPhysicalWitnesses(t *testing.T) {
	config := testConfig()
	intent := controller.WorkerCreateIntent{LeaseID: "11111111-1111-4111-8111-111111111111", ResourceID: "slipway-linux-11111111-1111-4111-8111-111111111111"}
	owned := ownedResource(config, intent, "pod-uid-1", true, true, true)
	pods := &fakePodSource{scope: PodScope{Complete: true, Items: []PodObject{podObject(owned)}}}
	processes := &fakeProcessSource{scope: WitnessScope{Complete: true, Items: []PhysicalWitness{physicalWitness(owned)}}}
	storage := &fakeStorageSource{scope: WitnessScope{Complete: true, Items: []PhysicalWitness{physicalWitness(owned)}}}
	sources := TrustedPhysicalSources{Pods: pods, Processes: processes, Storage: storage, Capacity: &fakeCapacitySource{snapshot: physicalCapacity()}}
	inventory := newPhysicalInventory(t, sources)
	witness, err := inventory.ConfirmAbsent(context.Background(), controller.WorkerObservation{LeaseID: intent.LeaseID, ResourceID: intent.ResourceID}, owned.UID)
	if err != nil || witness.ProcessAbsent || witness.StorageAbsent {
		t.Fatalf("live process/storage was treated as absent: %+v %v", witness, err)
	}
	pods.scope.Items = nil
	processes.scope.Items = nil
	storage.scope.Items = nil
	witness, err = inventory.ConfirmAbsent(context.Background(), controller.WorkerObservation{LeaseID: intent.LeaseID, ResourceID: intent.ResourceID}, owned.UID)
	if err != nil || !witness.ProcessAbsent || !witness.StorageAbsent {
		t.Fatalf("complete physical absence was not proved: %+v %v", witness, err)
	}
	absence, err := inventory.WaitAbsent(context.Background(), DeleteRequest{Namespace: config.Namespace, Name: intent.ResourceID, UID: owned.UID, Labels: owned.Labels})
	if err != nil || absence.UID != owned.UID || !absence.ProcessAbsent || !absence.StorageAbsent {
		t.Fatalf("trusted cleanup witness did not prove exact absence: %+v %v", absence, err)
	}
	processes.scope.Items = []PhysicalWitness{physicalWitness(owned)}
	processes.scope.Items[0].UID = "replacement-uid"
	if _, err := inventory.ConfirmAbsent(context.Background(), controller.WorkerObservation{LeaseID: intent.LeaseID, ResourceID: intent.ResourceID}, owned.UID); err == nil {
		t.Fatal("replacement process identity was treated as absence")
	}
	processes.scope.Items = nil
	foreignIntent := controller.WorkerCreateIntent{LeaseID: "44444444-4444-4444-8444-444444444444", ResourceID: intent.ResourceID}
	foreign := ownedResource(config, foreignIntent, "foreign-pod-uid", false, true, false)
	processes.scope.Items = []PhysicalWitness{physicalWitness(foreign)}
	if _, err := inventory.ConfirmAbsent(context.Background(), controller.WorkerObservation{LeaseID: intent.LeaseID, ResourceID: intent.ResourceID}, owned.UID); err == nil {
		t.Fatal("same-name resource under a different lease was treated as absence")
	}
}

// homelab#983. The witness budget and the backend charge are computed in two
// places (cmd/runner-pool builds the budget by hand). A budget still charging
// two writable devices would let the witness admit a worker the backend
// charged three for, so construction refuses any per-worker figure that is not
// the backend's own.
func TestTrustedInventoryRefusesABudgetThatDiffersFromTheBackendCharge(t *testing.T) {
	config := testConfig()
	sources := physicalSources(PodScope{Complete: true}, WitnessScope{Complete: true}, WitnessScope{Complete: true}, physicalCapacity())
	if _, err := NewTrustedInventory(sources, config, physicalBudget(config)); err != nil {
		t.Fatalf("fixture drifted: the backend's own budget was refused: %v", err)
	}
	budget := physicalBudget(config)
	budget.PerWorker.StorageBytes = 2*config.ContainerRootfsBytes + config.Overhead.StorageBytes
	if _, err := NewTrustedInventory(sources, config, budget); err == nil {
		t.Fatal("a two-device storage budget was accepted beside a three-device backend charge")
	}
}

package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/cameronsjo/slipway/internal/controller"
)

// PodSource, ProcessSource, StorageSource, and CapacitySource are deliberately
// separate trust seams. A concrete implementation must authenticate the Pod
// API, CRI/QEMU witness, writable-storage witness, and host-capacity witness
// independently; this package never shells out using a Pod or label field.
type PodSource interface {
	ListPods(context.Context, string, string) (PodScope, error)
}

type ProcessSource interface {
	ListProcesses(context.Context, string, string) (WitnessScope, error)
}

type StorageSource interface {
	ListWritableStorage(context.Context, string, string) (WitnessScope, error)
}

type CapacitySource interface {
	ObserveCapacity(context.Context, string) (CapacitySnapshot, error)
}

type TrustedPhysicalSources struct {
	Pods      PodSource
	Processes ProcessSource
	Storage   StorageSource
	Capacity  CapacitySource
}

// PodObject is the bounded identity subset read from the Kubernetes Pod API.
// Its absence from a complete scope is meaningful only after the independently
// authenticated process and storage scopes have also been read.
type PodObject struct {
	Namespace        string
	Name             string
	UID              string
	Labels           map[string]string
	Annotations      map[string]string
	RuntimeClassName string
}

type PodScope struct {
	Complete bool
	Items    []PodObject
}

// PhysicalWitness identifies a process/sandbox or writable-storage allocation
// using the same durable Pod identity labels. A witness with no Pod object is
// retained as an orphan instead of being interpreted as absent.
type PhysicalWitness struct {
	Namespace        string
	Name             string
	UID              string
	Labels           map[string]string
	RuntimeClassName string
}

type WitnessScope struct {
	Complete bool
	Items    []PhysicalWitness
}

// CapacitySnapshot keeps current non-pool workloads and runtime overhead
// separate from the pool budget. Used cache/log bytes are independent limits;
// free slots are constrained by every dimension.
type CapacitySnapshot struct {
	Complete bool

	CPUMilliTotal           int64
	CPUMilliOtherWorkloads  int64
	CPUMilliRuntimeOverhead int64

	MemoryBytesTotal           int64
	MemoryBytesAvailable       int64
	MemoryBytesOtherWorkloads  int64
	MemoryBytesRuntimeOverhead int64

	WritableStorageBytesTotal           int64
	WritableStorageBytesUsed            int64
	WritableStorageBytesOtherWorkloads  int64
	WritableStorageBytesRuntimeOverhead int64
	ContainerRootfsBytes                int64

	ImageCacheBytesTotal int64
	ImageCacheBytesUsed  int64
	LogBytesTotal        int64
	LogBytesUsed         int64
}

type AdmissionBudget struct {
	MaxWorkers               int
	LegacySlots              int
	PerWorker                Footprint
	ContainerRootfsBytes     int64
	PerWorkerImageCacheBytes int64
	PerWorkerLogBytes        int64
}

// TrustedHeadroom is the residual capacity after current other workloads,
// runtime overhead, and already observed workers. Cache and logs are included
// even though they are not part of the controller's CPU/RAM/storage tuple.
type TrustedHeadroom struct {
	Complete             bool
	CPUMilli             int64
	MemoryBytes          int64
	WritableStorageBytes int64
	ImageCacheBytes      int64
	LogBytes             int64
	FreeSlots            int
	// Binding names which dimension ran out first, from the closed set below.
	// `FreeSlots: 0` used to be the whole story, and an operator holding it
	// could not tell a full thin pool from a node whose memory was committed to
	// idle CronJob limits -- two problems with nothing in common but a zero.
	Binding string
}

// The admission dimensions. A closed set: these are the only words that reach a
// log line or an error, and `slots` means the configured ceiling bound before
// any resource did.
const (
	DimensionCPU     = "cpu"
	DimensionMemory  = "memory"
	DimensionStorage = "storage"
	DimensionCache   = "cache"
	DimensionLogs    = "logs"
	DimensionSlots   = "slots"
	// DimensionUnknown is for a headroom observation that never completed, so
	// no dimension has been measured at all. It is not "nothing is short".
	DimensionUnknown = "unknown"
)

type PhysicalInventoryReport struct {
	Complete  bool
	Resources []WorkerResource
	Headroom  TrustedHeadroom
}

type TrustedInventory struct {
	sources TrustedPhysicalSources
	config  Config
	budget  AdmissionBudget
	// absence bounds WaitAbsent's poll; see absenceWait.
	absence absenceWait
}

// absenceWait is how long WaitAbsent keeps re-reading the trusted scopes for a
// deleted worker, and how often (homelab#951).
//
// It used to read once, straight after the Pod DELETE. The Pod object is
// always still there at that instant, so every teardown logged "lacks exact
// absence" and held its slot a whole controller cycle (about 60 s), while the
// Pod was measured gone 2-17 s after the DELETE on MS-A2 (2026-09-25).
//
// The poll deadline is 20 s: it covers that measured 17 s worst case. It is
// checked after each read, and one ConfirmAbsent read makes two host witness
// calls (process and storage scopes), each bounded by witness_timeout (30 s
// in controller-config.json). So the true worst case for one teardown is
// 20 s plus one read of up to about 60 s, about 80 s, and Maintain destroys
// workers one after another, so several slow teardowns add up in one cycle
// (filed for homelab#960). On expiry the last reading is returned, and a
// reading short of both proofs keeps the reservation exactly as before: the
// wait can only release a slot sooner, never on weaker evidence.
type absenceWait struct {
	deadline time.Duration
	interval time.Duration
	now      func() time.Time
	sleep    func(context.Context, time.Duration) error
}

func defaultAbsenceWait() absenceWait {
	return absenceWait{deadline: 28 * time.Second, interval: time.Second, now: time.Now, sleep: sleepContext}
}

// sleepContext waits d or until ctx ends, whichever is first.
func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func NewTrustedInventory(sources TrustedPhysicalSources, config Config, budget AdmissionBudget) (*TrustedInventory, error) {
	if sources.Pods == nil || sources.Processes == nil || sources.Storage == nil || sources.Capacity == nil {
		return nil, errors.New("trusted physical inventory requires four independent sources")
	}
	if err := config.validate(); err != nil {
		return nil, err
	}
	if budget.MaxWorkers != config.Capacity || budget.LegacySlots != config.LegacySlots ||
		!budget.PerWorker.positive() || budget.PerWorkerImageCacheBytes <= 0 || budget.PerWorkerLogBytes <= 0 ||
		budget.ContainerRootfsBytes != config.ContainerRootfsBytes ||
		budget.MaxWorkers < 0 || budget.LegacySlots < 0 || budget.LegacySlots > budget.MaxWorkers {
		return nil, errors.New("trusted physical inventory has invalid admission budget")
	}
	// The witness charges each worker the budget's figure and the backend
	// charges perWorkerFootprint; cmd/runner-pool computes the two separately.
	// They must be the same footprint, or the three-device storage charge
	// (homelab#983) holds on one side and not the other.
	if budget.PerWorker != (&Backend{config: config}).perWorkerFootprint() {
		return nil, errors.New("trusted physical inventory per-worker budget differs from the backend's per-worker charge")
	}
	return &TrustedInventory{sources: sources, config: config, budget: budget, absence: defaultAbsenceWait()}, nil
}

func (i *TrustedInventory) Observe(ctx context.Context) (PhysicalInventoryReport, error) {
	if i == nil || ctx == nil {
		return PhysicalInventoryReport{Complete: false}, errors.New("trusted physical inventory requires a context")
	}
	scope, err := i.readScopes(ctx)
	if err != nil {
		return PhysicalInventoryReport{Complete: false}, err
	}
	resources, err := i.mergeScopes(scope.pods, scope.processes, scope.storage)
	if err != nil {
		return PhysicalInventoryReport{Complete: false}, err
	}
	headroom, err := calculateTrustedHeadroom(scope.capacity, i.budget, countPresent(resources))
	if err != nil {
		return PhysicalInventoryReport{Complete: false}, err
	}
	return PhysicalInventoryReport{Complete: true, Resources: resources, Headroom: headroom}, nil
}

// ControllerInventory adapts the trusted report to the controller backend contract.
// Orphan process/storage witnesses are returned as workers so an outer
// controller cannot mistake a vanished Pod object for physical absence.
func (i *TrustedInventory) ControllerInventory(ctx context.Context) (controller.BackendInventory, error) {
	report, err := i.Observe(ctx)
	if err != nil {
		return controller.BackendInventory{Complete: false}, err
	}
	workers := make([]controller.WorkerObservation, 0, len(report.Resources))
	for _, resource := range report.Resources {
		if resource.PodExists || resource.ProcessPresent || resource.StoragePresent {
			workers = append(workers, controller.WorkerObservation{LeaseID: resource.Labels[LabelLeaseID], ResourceID: resource.Name})
		}
	}
	return controller.BackendInventory{Complete: true, FreeSlots: report.Headroom.FreeSlots,
		Binding: report.Headroom.Binding, Workers: workers}, nil
}

// Inventory implements the transport's TrustedWitness seam. Pod-only workers
// are intentionally omitted here: the transport already has their complete
// Pod listing, while orphan process/storage witnesses must be returned.
func (i *TrustedInventory) Inventory(ctx context.Context, namespace, selector string) (PhysicalInventory, error) {
	if i == nil || namespace != i.config.Namespace || selector != LabelManagedBy+"="+ManagedByValue+","+LabelPlatform+"="+PlatformLinux {
		return PhysicalInventory{Complete: false}, errors.New("trusted witness scope does not match immutable worker configuration")
	}
	report, err := i.Observe(ctx)
	if err != nil {
		return PhysicalInventory{Complete: false}, err
	}
	workers := make([]PhysicalWorker, 0, len(report.Resources))
	for _, resource := range report.Resources {
		if resource.ProcessPresent || resource.StoragePresent {
			workers = append(workers, PhysicalWorker{Name: resource.Name, UID: resource.UID,
				Labels: cloneStrings(resource.Labels), RuntimeClassName: resource.RuntimeClassName,
				ProcessPresent: resource.ProcessPresent, StoragePresent: resource.StoragePresent})
		}
	}
	return PhysicalInventory{Complete: true, Workers: workers}, nil
}

func (i *TrustedInventory) Headroom(ctx context.Context, namespace string, perWorker Footprint) (Headroom, error) {
	if i == nil || namespace != i.config.Namespace || !perWorker.positive() {
		return Headroom{Complete: false}, errors.New("trusted headroom scope or worker budget is invalid")
	}
	report, err := i.Observe(ctx)
	if err != nil {
		return Headroom{Complete: false}, err
	}
	if perWorker != i.budget.PerWorker {
		return Headroom{Complete: false}, errors.New("trusted headroom worker footprint differs from immutable admission budget")
	}
	// No DimensionUnknown arm here: Observe above returns an error for every
	// incomplete headroom, so report.Headroom.Complete is true on this line by
	// construction. A fail-closed branch nothing can enter is not protection,
	// it is coverage that reads like protection -- the caller's own refusal in
	// Backend.freeSlots is the one that can actually fire.
	return Headroom{Complete: report.Headroom.Complete, CPUMilli: report.Headroom.CPUMilli,
		MemoryBytes: report.Headroom.MemoryBytes, StorageBytes: report.Headroom.WritableStorageBytes,
		FreeSlots: report.Headroom.FreeSlots, Binding: report.Headroom.Binding}, nil
}

func (i *TrustedInventory) WaitAbsent(ctx context.Context, request DeleteRequest) (AbsenceWitness, error) {
	if i == nil || request.Namespace != i.config.Namespace || request.Name == "" || request.UID == "" {
		return AbsenceWitness{}, errors.New("trusted absence witness request is outside immutable scope")
	}
	leaseID := request.Labels[LabelLeaseID]
	if len(request.Labels) != 5 || request.Labels[LabelManagedBy] != ManagedByValue ||
		request.Labels[LabelPlatform] != PlatformLinux || request.Labels[LabelOwnerUID] != i.config.OwnerUID ||
		request.Labels[LabelResourceID] != request.Name {
		return AbsenceWitness{}, errors.New("trusted absence witness resource identity is not label-bound")
	}
	worker := controller.WorkerObservation{LeaseID: leaseID, ResourceID: request.Name}
	// Poll until both proofs hold or the bound passes (see absenceWait). An
	// error ends the wait at once, as the single read always did; cancellation
	// returns ctx's error, never a reading.
	deadline := i.absence.now().Add(i.absence.deadline)
	// A shorter shared budget from the maintain pass wins (#960): once it is
	// spent, this wait makes one read and returns it, as before #995.
	if budget, ok := controller.AbsenceBudget(ctx); ok && budget.Before(deadline) {
		deadline = budget
	}
	var last *AbsenceWitness
	for {
		absence, err := i.ConfirmAbsent(ctx, worker, request.UID)
		if err != nil {
			if last == nil {
				return AbsenceWitness{}, err
			}
			// A LATER READ FAILING ENDS THE WAIT LIKE THE DEADLINE DOES. The
			// first read already answered, and its answer is what the single
			// read always returned. Returning this error instead would fail
			// Maintain, and with it the whole cycle before PollSweep
			// (homelab#911), so polling would hand one transient witness error
			// about twenty chances per teardown to stop the pool. The last good
			// reading fails closed: short of both proofs, the reservation stays.
			slog.Warn("Absence re-read failed after an earlier good reading; keeping that reading.",
				"lease_id", leaseID, "resource_id", request.Name, "error", err,
				"process_absent", last.ProcessAbsent, "storage_absent", last.StorageAbsent)
			return *last, nil
		}
		witness := AbsenceWitness{UID: request.UID, ProcessAbsent: absence.ProcessAbsent, StorageAbsent: absence.StorageAbsent}
		last = &witness
		if (absence.ProcessAbsent && absence.StorageAbsent) || !i.absence.now().Before(deadline) {
			return witness, nil
		}
		if err := i.absence.sleep(ctx, i.absence.interval); err != nil {
			return AbsenceWitness{}, err
		}
	}
}

var _ TrustedWitness = (*TrustedInventory)(nil)

// ConfirmAbsent re-reads all trusted scopes. It does not interpret a source
// error, a Pod 404, or a partial witness as absence, and it rejects a UID
// replacement under the same durable resource name.
func (i *TrustedInventory) ConfirmAbsent(ctx context.Context, worker controller.WorkerObservation, expectedUID string) (controller.WorkerAbsence, error) {
	if i == nil || ctx == nil {
		return controller.WorkerAbsence{}, errors.New("absence confirmation requires a context")
	}
	if expectedUID == "" {
		return controller.WorkerAbsence{}, errors.New("absence confirmation requires the reconciled Pod UID")
	}
	if err := i.validateIdentity(worker.LeaseID, worker.ResourceID); err != nil {
		return controller.WorkerAbsence{}, err
	}
	scope, err := i.readWitnessScopes(ctx)
	if err != nil {
		return controller.WorkerAbsence{}, err
	}
	resources, err := i.mergeScopes(scope.pods, scope.processes, scope.storage)
	if err != nil {
		return controller.WorkerAbsence{}, err
	}
	var found *WorkerResource
	for n := range resources {
		resource := &resources[n]
		if resource.Name != worker.ResourceID {
			continue
		}
		if resource.Labels[LabelLeaseID] != worker.LeaseID {
			return controller.WorkerAbsence{}, errors.New("physical resource name is owned by a different lease")
		}
		if resource.UID != expectedUID {
			return controller.WorkerAbsence{}, errors.New("physical identity changed while confirming absence")
		}
		found = resource
		break
	}
	if found == nil {
		return controller.WorkerAbsence{LeaseID: worker.LeaseID, ResourceID: worker.ResourceID, ProcessAbsent: true, StorageAbsent: true}, nil
	}
	// A surviving Pod may recreate a process after a witness race. Keep both
	// proofs false until the complete scope no longer contains the Pod.
	return controller.WorkerAbsence{LeaseID: worker.LeaseID, ResourceID: worker.ResourceID,
		ProcessAbsent: !found.PodExists && !found.ProcessPresent,
		StorageAbsent: !found.PodExists && !found.StoragePresent}, nil
}

type physicalScopes struct {
	pods, processes, storage PodScopeOrWitness
	capacity                 CapacitySnapshot
}

type PodScopeOrWitness struct {
	Complete bool
	Pods     []PodObject
	Witness  []PhysicalWitness
}

func (i *TrustedInventory) readScopes(ctx context.Context) (physicalScopes, error) {
	scope, err := i.readWitnessScopes(ctx)
	if err != nil {
		return physicalScopes{}, err
	}
	capacity, err := i.sources.Capacity.ObserveCapacity(ctx, i.config.Namespace)
	if err != nil || !capacity.Complete {
		if err == nil {
			err = errors.New("host capacity scope is incomplete")
		}
		return physicalScopes{}, err
	}
	scope.capacity = capacity
	return scope, nil
}

func (i *TrustedInventory) readWitnessScopes(ctx context.Context) (physicalScopes, error) {
	pods, err := i.sources.Pods.ListPods(ctx, i.config.Namespace, LabelManagedBy+"="+ManagedByValue+","+LabelPlatform+"="+PlatformLinux)
	if err != nil || !pods.Complete {
		if err == nil {
			err = errors.New("Kubernetes Pod scope is incomplete")
		}
		return physicalScopes{}, err
	}
	processes, err := i.sources.Processes.ListProcesses(ctx, i.config.Namespace, i.config.OwnerUID)
	if err != nil || !processes.Complete {
		if err == nil {
			err = errors.New("CRI/QEMU process scope is incomplete")
		}
		return physicalScopes{}, err
	}
	storage, err := i.sources.Storage.ListWritableStorage(ctx, i.config.Namespace, i.config.OwnerUID)
	if err != nil || !storage.Complete {
		if err == nil {
			err = errors.New("writable-storage scope is incomplete")
		}
		return physicalScopes{}, err
	}
	return physicalScopes{
		pods:      PodScopeOrWitness{Complete: true, Pods: pods.Items},
		processes: PodScopeOrWitness{Complete: true, Witness: processes.Items},
		storage:   PodScopeOrWitness{Complete: true, Witness: storage.Items},
	}, nil
}

func (i *TrustedInventory) mergeScopes(pods, processes, storage PodScopeOrWitness) ([]WorkerResource, error) {
	type merged struct {
		resource WorkerResource
		podSeen  bool
	}
	items := make(map[string]*merged)
	nameLeases := make(map[string]string)
	keyFor := func(resource WorkerResource) (string, error) {
		leaseID := resource.Labels[LabelLeaseID]
		if err := i.validateResource(resource); err != nil {
			return "", err
		}
		if existingLease, ok := nameLeases[resource.Name]; ok && existingLease != leaseID {
			return "", errors.New("physical resource name is claimed by multiple leases")
		}
		nameLeases[resource.Name] = leaseID
		return leaseID + "\x00" + resource.Name, nil
	}
	ensure := func(resource WorkerResource) (*merged, error) {
		key, err := keyFor(resource)
		if err != nil {
			return nil, err
		}
		if current := items[key]; current != nil {
			if current.resource.UID != resource.UID || current.resource.RuntimeClassName != resource.RuntimeClassName {
				return nil, errors.New("physical witness identity changed across scopes")
			}
			return current, nil
		}
		copyLabels := make(map[string]string, len(resource.Labels))
		for name, value := range resource.Labels {
			copyLabels[name] = value
		}
		resource.Labels = copyLabels
		current := &merged{resource: resource}
		items[key] = current
		return current, nil
	}
	for _, pod := range pods.Pods {
		current, err := ensure(WorkerResource{Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID,
			Labels: pod.Labels, Annotations: pod.Annotations, RuntimeClassName: pod.RuntimeClassName, PodExists: true})
		if err != nil {
			return nil, err
		}
		if current.podSeen {
			return nil, errors.New("duplicate Kubernetes Pod identity")
		}
		current.podSeen = true
	}
	for _, witness := range processes.Witness {
		current, err := ensure(WorkerResource{Namespace: witness.Namespace, Name: witness.Name, UID: witness.UID,
			Labels: witness.Labels, RuntimeClassName: witness.RuntimeClassName})
		if err != nil {
			return nil, err
		}
		if current.resource.ProcessPresent {
			return nil, errors.New("duplicate CRI/QEMU process identity")
		}
		current.resource.ProcessPresent = true
	}
	for _, witness := range storage.Witness {
		current, err := ensure(WorkerResource{Namespace: witness.Namespace, Name: witness.Name, UID: witness.UID,
			Labels: witness.Labels, RuntimeClassName: witness.RuntimeClassName})
		if err != nil {
			return nil, err
		}
		if current.resource.StoragePresent {
			return nil, errors.New("duplicate writable-storage identity")
		}
		current.resource.StoragePresent = true
	}
	resources := make([]WorkerResource, 0, len(items))
	for _, item := range items {
		resources = append(resources, item.resource)
	}
	return resources, nil
}

func countPresent(resources []WorkerResource) int {
	count := 0
	for _, resource := range resources {
		if resource.PodExists || resource.ProcessPresent || resource.StoragePresent {
			count++
		}
	}
	return count
}

func (i *TrustedInventory) validateIdentity(leaseID, resourceID string) error {
	if err := (&Backend{config: i.config}).validateIdentity(leaseID, resourceID); err != nil {
		return err
	}
	return nil
}

func (i *TrustedInventory) validateResource(resource WorkerResource) error {
	if _, err := (&Backend{config: i.config}).validateResource(resource); err != nil {
		return fmt.Errorf("trusted physical inventory rejected resource: %w", err)
	}
	return nil
}

func calculateTrustedHeadroom(snapshot CapacitySnapshot, budget AdmissionBudget, active int) (TrustedHeadroom, error) {
	if !snapshot.Complete || active < 0 || active > budget.MaxWorkers-budget.LegacySlots || budget.MaxWorkers < budget.LegacySlots {
		return TrustedHeadroom{Complete: false}, errors.New("trusted capacity snapshot is incomplete or overcommitted")
	}
	values := []int64{
		snapshot.CPUMilliTotal, snapshot.CPUMilliOtherWorkloads, snapshot.CPUMilliRuntimeOverhead,
		snapshot.MemoryBytesTotal, snapshot.MemoryBytesAvailable, snapshot.MemoryBytesOtherWorkloads, snapshot.MemoryBytesRuntimeOverhead,
		snapshot.WritableStorageBytesTotal, snapshot.WritableStorageBytesUsed, snapshot.WritableStorageBytesOtherWorkloads, snapshot.WritableStorageBytesRuntimeOverhead,
		snapshot.ContainerRootfsBytes,
		snapshot.ImageCacheBytesTotal, snapshot.ImageCacheBytesUsed, snapshot.LogBytesTotal, snapshot.LogBytesUsed,
	}
	for _, value := range values {
		if value < 0 {
			return TrustedHeadroom{Complete: false}, errors.New("trusted capacity snapshot contains a negative value")
		}
	}
	if snapshot.ImageCacheBytesUsed > snapshot.ImageCacheBytesTotal || snapshot.LogBytesUsed > snapshot.LogBytesTotal {
		return TrustedHeadroom{Complete: false}, errors.New("trusted cache or log usage exceeds capacity")
	}
	if snapshot.WritableStorageBytesUsed > snapshot.WritableStorageBytesTotal {
		return TrustedHeadroom{Complete: false}, errors.New("trusted writable-storage usage exceeds capacity")
	}
	if snapshot.ContainerRootfsBytes != budget.ContainerRootfsBytes {
		return TrustedHeadroom{Complete: false}, errors.New("trusted rootfs size differs from the admission budget")
	}
	per := budget.PerWorker
	storageOther := snapshot.WritableStorageBytesOtherWorkloads
	if observedOther := snapshot.WritableStorageBytesUsed - int64(active)*per.StorageBytes; observedOther > storageOther {
		storageOther = observedOther
	}
	freeCPU := snapshot.CPUMilliTotal - snapshot.CPUMilliOtherWorkloads - snapshot.CPUMilliRuntimeOverhead - int64(active)*per.CPUMilli
	freeMemory := snapshot.MemoryBytesTotal - snapshot.MemoryBytesOtherWorkloads - snapshot.MemoryBytesRuntimeOverhead - int64(active)*per.MemoryBytes
	if snapshot.MemoryBytesAvailable < freeMemory {
		freeMemory = snapshot.MemoryBytesAvailable
	}
	freeStorage := snapshot.WritableStorageBytesTotal - storageOther - snapshot.WritableStorageBytesRuntimeOverhead - int64(active)*per.StorageBytes
	freeCache := snapshot.ImageCacheBytesTotal - snapshot.ImageCacheBytesUsed - int64(active)*budget.PerWorkerImageCacheBytes
	freeLogs := snapshot.LogBytesTotal - snapshot.LogBytesUsed - int64(active)*budget.PerWorkerLogBytes

	// AN OVERSUBSCRIBED HOST IS A KNOWN STATE, NOT AN UNREADABLE ONE, AND THE
	// DIFFERENCE IS THE WHOLE POINT OF THIS BLOCK.
	//
	// A negative dimension used to return Complete:false, which fenced the
	// ENTIRE inventory -- including the list of workers the controller owns.
	// Measured on MS-A2 2026-09-21: a legacy CronJob rollover pushed free
	// memory negative while one pool worker was running, the inventory was
	// refused, and `Maintain` could not retire the very worker whose
	// retirement would have restored the headroom. The controller fenced
	// itself out of its own recovery.
	//
	// So negative headroom now FENCES ADMISSION and nothing else: every free
	// value clamps to zero, FreeSlots is zero, and the worker list is returned
	// complete so retirement can proceed. Fail-closed is retained above for
	// every case where the inventory itself is UNKNOWN -- an incomplete
	// snapshot, a negative input, usage exceeding capacity, a rootfs mismatch.
	// Those say the host could not be read; this says the host was read and is
	// full.
	dimensions := []struct {
		name string
		free *int64
		per  int64
	}{
		{DimensionCPU, &freeCPU, per.CPUMilli},
		{DimensionMemory, &freeMemory, per.MemoryBytes},
		{DimensionStorage, &freeStorage, per.StorageBytes},
		{DimensionCache, &freeCache, budget.PerWorkerImageCacheBytes},
		{DimensionLogs, &freeLogs, budget.PerWorkerLogBytes},
	}
	binding := DimensionSlots
	oversubscribed := false
	var worstDeficit int64
	for _, dimension := range dimensions {
		if *dimension.free >= 0 {
			continue
		}
		// In whole workers, so dimensions with wildly different units compare.
		// Ceiling division: any overdraw at all is at least one worker's worth
		// of "this is the one that is short".
		deficit := (-*dimension.free + dimension.per - 1) / dimension.per
		if !oversubscribed || deficit > worstDeficit {
			oversubscribed, worstDeficit, binding = true, deficit, dimension.name
		}
		*dimension.free = 0
	}

	freeSlots := int64(budget.MaxWorkers - budget.LegacySlots - active)
	if !oversubscribed {
		// The binding dimension is the one that runs out first, and `slots`
		// stays the answer when the configured ceiling binds before any
		// resource does. Fixed iteration order, so ties are deterministic.
		for _, dimension := range dimensions {
			if capacity := *dimension.free / dimension.per; capacity < freeSlots {
				freeSlots, binding = capacity, dimension.name
			}
		}
	} else {
		freeSlots = 0
	}
	return TrustedHeadroom{Complete: true, CPUMilli: freeCPU, MemoryBytes: freeMemory,
		WritableStorageBytes: freeStorage, ImageCacheBytes: freeCache, LogBytes: freeLogs,
		FreeSlots: int(freeSlots), Binding: binding}, nil
}

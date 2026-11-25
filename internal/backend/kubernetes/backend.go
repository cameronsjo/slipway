// Package kubernetes contains the Linux Kata worker adapter.
//
// Pod mutations use client-go. Physical process, storage, and capacity facts
// come from separate trusted sources because the Kubernetes API cannot prove
// that a Kata VM and its writable data are gone.
package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/cameronsjo/slipway/internal/bootstrap"
	"github.com/cameronsjo/slipway/internal/controller"
	"github.com/google/uuid"
)

const (
	ManagedByValue = "slipway"
	PlatformLinux  = "linux"

	LabelManagedBy  = "slipway/managed-by"
	LabelPlatform   = "slipway/platform"
	LabelOwnerUID   = "slipway/owner-uid"
	LabelLeaseID    = "slipway/lease-id"
	LabelResourceID = "slipway/resource-id"

	workerBootstrapCommand = "/usr/local/bin/slipway-bootstrap"

	// workerDockerGID is the group the runner container runs with, baked into
	// the worker image (`USER 1001:121`) and declared in
	// image/linux/image-lock.json's job_user. dockerd must create its socket
	// owned by THIS group or the runner cannot talk to it.
	//
	// MEASURED, not assumed. The pinned dind image's own `docker` group is gid
	// 2375 and it has no entry for 121 at all, so the default socket is
	// root:2375 mode 0660 and the runner is denied -- which is exactly how the
	// first real job died, at `docker version`, after every earlier step passed.
	// dockerd 29.8.1 and 29.9.3 (#1349) resolve a NUMERIC gid here: the same image with
	// `--group=sjo-no-such-group` exits with `failed to load listeners: group
	// ... not found`, while `--group=121` gets past listener setup with no group
	// error despite no such group existing in the image.
	//
	// A test pins this against the image lock, so the two cannot drift.
	workerDockerGID = 121

	// LinuxCapacityCeiling is the second of the two ceiling checks a capacity
	// value passes: cmd/runner-pool refuses it at flag parse, this refuses it
	// at backend construction. It read 6 while the CLI read 2, so the backend
	// admitted three values the process would never reach and its own error
	// text ("must fit the Linux qualification ceiling") named a ceiling it was
	// not enforcing. Each Kata VM charges 30.5GiB of thin pool (three 10GiB
	// writable disks plus 0.5GiB, homelab#983), so sixteen workers charge 488GiB
	// of the 576GiB pool homelab#983 grew to, leaving image-cache headroom, and
	// the worker /27 has sixteen slot addresses; the controller's storage
	// arithmetic, not this ceiling, is what holds admission below the pool's
	// size. See #722 S9, homelab#960, homelab#983 and cmd/runner-pool/runtime.go.
	LinuxCapacityCeiling = 16

	// WritableDevicesPerWorker is how many thin-pool devices one worker can
	// write: the pause sandbox, the runner and dockerd each get their own
	// devmapper snapshot of ContainerRootfsBytes. Measured, not assumed:
	// docs/evidence/2026-09-25-runner-pool-worker-thin-devices/ saw three per
	// worker in ten of ten samples. The pause device is charged because dockerd
	// is privileged in the guest, and a read-only guest mount does not make the
	// device read-only; the test is whether a job CAN write it.
	WritableDevicesPerWorker = 3

	// dockerDataRoot is dockerd's --data-root. It is a path on dockerd's own
	// devmapper root filesystem, and it is NOT /var/lib/docker: the pinned
	// docker image declares `VOLUME /var/lib/docker`, and with nothing mounted
	// there the runtime materialises that volume in the guest's /run tmpfs,
	// which is RAM. Nothing may be mounted at or under this path; a mount there
	// would replace the disk with whatever the volume is.
	dockerDataRoot = "/var/lib/sjo-docker"

	// podlessWorkerGrace and podlessWorkerMisses debounce the orphan verdict
	// below (#1109 review). A worker witnessed with no Pod row is reported
	// Terminated only once it has been Pod-less for this many consecutive
	// complete inventories AND for at least this long. Transport.List reads the
	// witness before the Pods, which closes the create race at the source; this
	// guards whatever that ordering does not, such as a listing that omits a live
	// Pod. Same values, and the same shape, as the controller's vanished-worker
	// debounce for review T5 (homelab#1104).
	podlessWorkerGrace  = 2 * time.Minute
	podlessWorkerMisses = 2
)

// PerWorkerStorageBytes is the thin-pool storage one worker is charged:
// every writable device at its full virtual size, plus the runtime overhead.
// The backend's own slot arithmetic and the witness budget both use it, so the
// two charges cannot drift apart.
func PerWorkerStorageBytes(containerRootfsBytes, overheadStorageBytes int64) int64 {
	return WritableDevicesPerWorker*containerRootfsBytes + overheadStorageBytes
}

// MemoryOnlyBootstrap is the only supported worker capability delivery mode.
// The API implementation must deliver the value to the initial worker process
// over an authenticated in-memory stream; it must not put the token into a Pod
// object, Secret object, environment value, command line, journal, or image.
const MemoryOnlyBootstrap = "memory-only-authenticated-stream"

var (
	dnsLabelRE   = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`)
	labelValueRE = regexp.MustCompile(`^[A-Za-z0-9](?:[A-Za-z0-9_.-]{0,61}[A-Za-z0-9])?$`)
	digestRE     = regexp.MustCompile(`^.+@sha256:[0-9a-f]{64}$`)
)

// Footprint is the resource amount charged to one worker or to the trusted
// runtime overhead. Storage is the bounded writable worker allocation, not the
// host's unbounded free space.
type Footprint struct {
	CPUMilli     int64
	MemoryBytes  int64
	StorageBytes int64
}

func (f Footprint) add(other Footprint) Footprint {
	return Footprint{CPUMilli: f.CPUMilli + other.CPUMilli,
		MemoryBytes: f.MemoryBytes + other.MemoryBytes, StorageBytes: f.StorageBytes + other.StorageBytes}
}

func (f Footprint) positive() bool {
	return f.CPUMilli > 0 && f.MemoryBytes > 0 && f.StorageBytes > 0
}

// ContainerResources are explicit requests and limits. The adapter uses the
// requests for admission and emits both values in the Pod spec.
type ContainerResources struct {
	Requests Footprint
	Limits   Footprint
}

// VolumeBudget describes memory-backed writable paths shared only by this
// worker's Kata VM. Ordinary disk-backed emptyDir is not accepted by this
// adapter: under this runtime it is uncapped guest tmpfs or an unbounded host
// file. dockerd's image store is not here; it lives on dockerd's own devmapper
// root filesystem at dockerDataRoot (homelab#983).
type VolumeBudget struct {
	Work   int64
	Socket int64
	Tmp    int64
}

func (v VolumeBudget) total() int64 { return v.Work + v.Socket + v.Tmp }

// Config is immutable platform configuration. OwnerUID is the exact controller
// instance identity carried in every owned worker label. RuntimeClassName must
// name the separately fingerprinted production Kata handler; the canary and
// legacy handlers are rejected.
type Config struct {
	Namespace            string
	OwnerUID             string
	NodeName             string
	RuntimeClassName     string
	RunnerImage          string
	DockerImage          string
	WorkerCommand        string
	RunnerRoot           string
	CAFile               string
	BootstrapURL         string
	Capacity             int
	LegacySlots          int
	RunnerResources      ContainerResources
	DockerResources      ContainerResources
	Overhead             Footprint
	Volumes              VolumeBudget
	ContainerRootfsBytes int64
}

// PodSpec is the security-relevant subset of a Kubernetes Pod. The API
// implementation must serialize these fields without applying permissive
// defaults. BootstrapToken is intentionally absent from this type.
type PodSpec struct {
	Namespace                    string
	Name                         string
	Labels                       map[string]string
	Annotations                  map[string]string
	RuntimeClassName             string
	NodeSelector                 map[string]string
	AutomountServiceAccountToken bool
	EnableServiceLinks           bool
	HostNetwork                  bool
	HostPID                      bool
	HostIPC                      bool
	RestartPolicy                string
	TerminationGracePeriodSec    int64
	SeccompProfile               string
	Containers                   []Container
	Volumes                      []Volume
}

type Container struct {
	Name                     string
	Image                    string
	Command                  []string
	Args                     []string
	Environment              []EnvVar
	Resources                ContainerResources
	Privileged               bool
	RunAsNonRoot             bool
	AllowPrivilegeEscalation bool
	ReadOnlyRootFilesystem   bool
	Mounts                   []VolumeMount
}

// EnvVar contains only public, fixed worker environment. Per-job capability
// delivery is performed through CreateRequest, never by this value field.
type EnvVar struct {
	Name  string
	Value string
}

type Volume struct {
	Name      string
	Medium    string
	SizeBytes int64
}

type VolumeMount struct {
	Name      string
	MountPath string
	ReadOnly  bool
}

// WorkerResource is a complete observation from the Kubernetes Pod scope plus
// the trusted CRI/storage witness. List must include orphaned Kata processes or
// writable storage even when the Pod object has already disappeared.
type WorkerResource struct {
	Namespace        string
	Name             string
	UID              string
	Labels           map[string]string
	Annotations      map[string]string
	RuntimeClassName string
	PodExists        bool
	ProcessPresent   bool
	StoragePresent   bool
	// RunnerTerminated is set only from an observed Terminated container state
	// for the `runner` container. A Pod that exists with no such status -- still
	// creating, running, or only physically witnessed -- leaves it false.
	RunnerTerminated bool
	RunnerExitCode   int
}

type ListResult struct {
	Complete bool
	Items    []WorkerResource
}

type Headroom struct {
	Complete     bool
	CPUMilli     int64
	MemoryBytes  int64
	StorageBytes int64
	// FreeSlots and Binding are the witness's own five-dimension verdict. The
	// backend recomputes a slot count from the three byte figures above, but it
	// cannot see the image cache or the log budget, so a cache-bound host would
	// otherwise report a binding dimension it never measured.
	FreeSlots int
	Binding   string
}

// CreateRequest is transient. A concrete API implementation must consume the
// token through a memory-only authenticated stream while creating the fresh Pod
// and must never persist or serialize it. Create is a single mutation: callers
// reconcile an uncertain response through List instead of calling it again.
type CreateRequest struct {
	Spec            PodSpec
	BootstrapMode   string
	BootstrapToken  bootstrap.Token
	StartupDeadline time.Time
}

type DeleteRequest struct {
	Namespace string
	Name      string
	UID       string
	Labels    map[string]string
}

// AbsenceWitness is returned only after the API has issued one exact UID-bound
// delete and independently checked both the Kata process/sandbox and writable
// storage. A false field retains the controller reservation.
type AbsenceWitness struct {
	UID           string
	ProcessAbsent bool
	StorageAbsent bool
}

// API is the deliberately narrow trusted transport boundary. List must be a
// complete scope listing, Create and Delete must each perform at most one
// Kubernetes mutation, and Headroom must include current host/workload/runtime
// overhead and enforceable storage capacity. The API implementation owns Kube
// credentials; they are never part of a worker Pod.
type API interface {
	List(context.Context, string, string) (ListResult, error)
	Headroom(context.Context, string, Footprint) (Headroom, error)
	Create(context.Context, CreateRequest) (WorkerResource, error)
	Delete(context.Context, DeleteRequest) (AbsenceWitness, error)
}

// Backend implements controller.WorkerBackend for one Linux Kata namespace.
type Backend struct {
	api    API
	config Config

	now func() time.Time

	mu             sync.Mutex
	uids           map[string]string
	createAttempts map[string]bool
	// podless tracks witnessed workers with no Pod row, keyed by worker UID. It
	// is process-local: a restart only restarts the grace, it never skips it.
	// Only Inventory advances it, and the daemon never runs Inventory beside a
	// Create (both sit inside its lifecycle lock); the map is only ever
	// replaced whole, under b.mu, never written in place.
	podless map[string]podlessWorker
	// Creates may run concurrently with each other (homelab#1190 item 2), and
	// each one checks headroom from its own inventory read. A Pod another
	// create is still POSTing, or POSTed after this read began, is missing
	// from that read, so its slot would be counted free twice. unlisted
	// holds those creates, keyed by resource ID, so Create can subtract them;
	// see unlistedCreates. Guarded by mu.
	unlisted       map[string]unlistedCreate
	createSeq      uint64 // bumped as each create's POST returns
	createsRunning int    // creates between their inventory read and return
}

// unlistedCreate is one create an inventory read may not have listed: either
// still in flight, or finished at createSeq value finished.
type unlistedCreate struct {
	done     bool
	finished uint64
}

// podlessWorker is one witnessed worker that complete inventories list with no
// Pod object.
type podlessWorker struct {
	firstMissing time.Time
	misses       int
}

var _ controller.WorkerBackend = (*Backend)(nil)

func New(api API, config Config) (*Backend, error) {
	if api == nil {
		return nil, errors.New("kubernetes backend requires a trusted API")
	}
	if err := config.validate(); err != nil {
		return nil, err
	}
	return &Backend{api: api, config: config, now: time.Now, uids: make(map[string]string),
		createAttempts: make(map[string]bool),
		podless:        make(map[string]podlessWorker),
		unlisted:       make(map[string]unlistedCreate)}, nil
}

func (b *Backend) Selector() string {
	return LabelManagedBy + "=" + ManagedByValue + "," + LabelPlatform + "=" + PlatformLinux
}

// Inventory stages. A fenced pool produces ONE controller line, so that line
// has to say which of four independent things failed. The vocabulary is fixed
// and each value names a real boundary in the code below, not a narrative step:
//
//	stage=list       the combined Pod + physical listing (Transport.List)
//	stage=validate   this package's per-resource identity checks
//	stage=headroom   the capacity read (Transport.Headroom)
//	stage=free-slots the admission arithmetic over an accepted snapshot
//
// Transport adds a finer tag inside list and headroom -- inventoryStageListPods
// and friends -- because "the Kubernetes API refused" and "the host witness
// refused" are the two answers an operator most needs separated, and they were
// indistinguishable from outside.
const (
	inventoryStageList      = "list"
	inventoryStageValidate  = "validate"
	inventoryStageHeadroom  = "headroom"
	inventoryStageFreeSlots = "free-slots"
)

// Inventory is the lifecycle's once-per-cycle read (ReconcileInventory), and the
// only read that advances the Pod-less debounce. Create's pre-mutation read
// goes through inventory(ctx, false), so a create does not count as a miss.
func (b *Backend) Inventory(ctx context.Context) (controller.BackendInventory, error) {
	return b.inventory(ctx, true)
}

func (b *Backend) inventory(ctx context.Context, advancePodless bool) (controller.BackendInventory, error) {
	if ctx == nil {
		return controller.BackendInventory{Complete: false}, errors.New("inventory requires a context")
	}
	listed, err := b.api.List(ctx, b.config.Namespace, b.Selector())
	if err != nil || !listed.Complete {
		if err == nil {
			err = errors.New("kubernetes worker listing is incomplete")
		}
		return controller.BackendInventory{Complete: false}, fmt.Errorf("inventory stage=%s: %w", inventoryStageList, err)
	}

	now := b.now()
	// b.podless is replaced whole below, never written in place, so this
	// reference is a stable snapshot to read without the lock.
	b.mu.Lock()
	podless := b.podless
	b.mu.Unlock()
	seenLease := make(map[string]bool, len(listed.Items))
	seenResource := make(map[string]bool, len(listed.Items))
	nextUIDs := make(map[string]string, len(listed.Items))
	nextPodless := make(map[string]podlessWorker)
	workers := make([]controller.WorkerObservation, 0, len(listed.Items))
	for _, item := range listed.Items {
		leaseID, err := b.validateResource(item)
		if err != nil {
			return controller.BackendInventory{Complete: false}, fmt.Errorf("inventory stage=%s: %w", inventoryStageValidate, err)
		}
		if seenLease[leaseID] || seenResource[item.Name] {
			return controller.BackendInventory{Complete: false},
				fmt.Errorf("inventory stage=%s: %w", inventoryStageValidate, errors.New("kubernetes worker listing contains duplicate ownership"))
		}
		seenLease[leaseID] = true
		seenResource[item.Name] = true
		// A live Pod object is still a charged worker even while its Kata
		// process is starting. A record with no Pod, process, or storage is
		// the only state that can be omitted as positively absent: List is
		// required to combine API, CRI, and storage scopes completely.
		if !item.PodExists && !item.ProcessPresent && !item.StoragePresent {
			continue
		}
		nextUIDs[item.Name] = item.UID
		// A gone Pod cannot come back: RunnerTerminated is never set for an item
		// with no Pod (transport.go's orphan path), so a lease surviving only in
		// its Kata process or storage witness read as still running forever
		// (#1109). Report it terminated with exit -1, which reuses the existing
		// abnormal-exit path: lifecycle.go's RetireTerminatedWorkers retires a
		// running lease or quarantines a provisioning one on any non-zero exit.
		//
		// ONE POD-LESS READ IS NOT ENOUGH (#1109 review). That verdict tears
		// down a lease that may be running a real job, so it waits for
		// podlessWorkerMisses consecutive complete inventories and
		// podlessWorkerGrace of wall time. The state clears as soon as the Pod
		// is listed again or the item leaves the listing, because nextPodless
		// only carries items that are Pod-less on this read.
		terminated := item.RunnerTerminated
		exitCode := item.RunnerExitCode
		if !item.PodExists {
			entry, tracked := podless[item.UID]
			if advancePodless {
				if !tracked {
					entry = podlessWorker{firstMissing: now}
					slog.Warn("A witnessed worker has no Pod object; it is reported terminated if it stays that way.",
						"lease_id", leaseID, "resource_id", item.Name, "grace", podlessWorkerGrace.String())
				}
				entry.misses++
				nextPodless[item.UID] = entry
			}
			if entry.misses >= podlessWorkerMisses && now.Sub(entry.firstMissing) >= podlessWorkerGrace {
				terminated = true
				exitCode = -1
			}
		}
		workers = append(workers, controller.WorkerObservation{LeaseID: leaseID, ResourceID: item.Name,
			Terminated: terminated, ExitCode: exitCode})
	}

	headroom, err := b.api.Headroom(ctx, b.config.Namespace, b.perWorkerFootprint())
	if err != nil || !headroom.Complete {
		if err == nil {
			err = errors.New("kubernetes resource headroom is incomplete")
		}
		return controller.BackendInventory{Complete: false}, fmt.Errorf("inventory stage=%s: %w", inventoryStageHeadroom, err)
	}
	free, binding, err := b.freeSlots(headroom, len(workers))
	if err != nil {
		return controller.BackendInventory{Complete: false}, fmt.Errorf("inventory stage=%s: %w", inventoryStageFreeSlots, err)
	}
	b.mu.Lock()
	if advancePodless {
		b.uids = nextUIDs
		b.podless = nextPodless
	} else {
		// Create's read only adds. It can run beside another create whose Pod
		// it has not listed, and replacing the map would drop the UID that
		// create just recorded, so its Destroy would refuse until the next
		// full Inventory. Inventory, which never runs beside a create, is
		// what removes UIDs that left the listing.
		for name, uid := range nextUIDs {
			b.uids[name] = uid
		}
	}
	b.mu.Unlock()
	return controller.BackendInventory{Complete: true, FreeSlots: free, Binding: binding, Workers: workers}, nil
}

func (b *Backend) Create(ctx context.Context, intent controller.WorkerCreateIntent) (controller.WorkerObservation, error) {
	if ctx == nil {
		return controller.WorkerObservation{}, errors.New("worker creation requires a context")
	}
	if err := b.validateIntent(intent); err != nil {
		return controller.WorkerObservation{}, err
	}
	// readSeq marks where this create's inventory read begins; every create
	// that finishes after it may be missing from that read (unlistedCreates).
	b.mu.Lock()
	readSeq := b.createSeq
	b.createsRunning++
	b.mu.Unlock()
	defer b.createFinished()
	// Reconcile before the mutation. This prevents a repeated caller from
	// issuing a second POST after a prior response was lost.
	inventory, err := b.inventory(ctx, false)
	if err != nil {
		// A WITNESS REFUSAL HERE IS NOT A BROKEN CREATE PATH (homelab#1213).
		// This read happens before the create-attempt marker and before the Pod
		// POST, the same ordering the ErrCapacity return below relies on, so
		// nothing was requested. Tagging it lets the lifecycle roll the intent
		// back instead of writing a provisioning_failures row: three storage
		// refusals during one burst of creates tripped the breaker on
		// 2026-10-01 and held admission at zero for fifteen minutes.
		//
		// A STORAGE refusal only, the race that evidence shows. A process or
		// capacity refusal, a witness the controller could not reach or read, or
		// a failed Kubernetes listing keeps the counted path. And never for a
		// resource whose POST already went out: this inventory read runs before
		// the already-attempted check below, so a retry of an attempted create
		// lands here too, and "nothing was created" would be false for it. The
		// tag is never added anywhere after this point either.
		var refusal *hostWitnessRefusal
		if errors.As(err, &refusal) && refusal.reason == WitnessReasonStorage {
			b.mu.Lock()
			attempted := b.createAttempts[intent.ResourceID]
			b.mu.Unlock()
			if !attempted {
				return controller.WorkerObservation{}, fmt.Errorf("%w: %w", controller.ErrWitnessRefusedBeforeCreate, err)
			}
		}
		return controller.WorkerObservation{}, err
	}
	for _, worker := range inventory.Workers {
		if worker.ResourceID == intent.ResourceID {
			if worker.LeaseID != intent.LeaseID {
				return controller.WorkerObservation{}, errors.New("existing Kubernetes resource has a different lease")
			}
			return worker, nil
		}
	}
	// A SHORTAGE HERE IS A WAIT, NOT A FAILURE, AND THIS IS WHERE THAT IS
	// DECIDED. Everything above is a read; nothing has been mutated. The
	// create-attempt marker is set below this line and the Pod POST is after
	// that, so a caller receiving ErrCapacity from here knows -- from the
	// ORDER OF THE STATEMENTS, not from a claim -- that no Pod was requested
	// and the reservation is safe to release.
	//
	// Wrapping controller.ErrCapacity is what makes it distinguishable. Before
	// this, a full node returned a bare error, the lifecycle recorded a
	// provisioning failure, and three of those in fifteen minutes tripped a
	// breaker that silenced the pool for another fifteen. The node being busy
	// is the most ordinary thing that can happen to a pool.
	// ALREADY-ATTEMPTED IS CHECKED FIRST, and the order is the point. A resource
	// whose Pod POST has already gone out must never come back as ErrCapacity,
	// because the lifecycle reads ErrCapacity as "nothing was created, release
	// the reservation" -- which would be false. Ordering makes that true
	// instead of relying on the caller not to ask twice. The atomic
	// check-and-set stays below: this is a read, and the set must still happen
	// under one lock with its own check.
	b.mu.Lock()
	attempted := b.createAttempts[intent.ResourceID]
	b.mu.Unlock()
	if attempted {
		return controller.WorkerObservation{}, errors.New("worker creation was already attempted; reconcile inventory instead of retrying")
	}
	// The headroom check and the claim on it are one step under mu, so two
	// creates cannot both take the last slot: each subtracts the creates its
	// own read could not see, including the other one once it has claimed.
	b.mu.Lock()
	if b.createAttempts[intent.ResourceID] {
		b.mu.Unlock()
		return controller.WorkerObservation{}, errors.New("worker creation was already attempted; reconcile inventory instead of retrying")
	}
	if unseen := b.unlistedCreates(inventory.Workers, readSeq); inventory.FreeSlots-unseen < 1 {
		b.mu.Unlock()
		return controller.WorkerObservation{},
			fmt.Errorf("Kubernetes/Kata headroom is exhausted (short=%s, creates_unlisted=%d): %w", inventory.Binding, unseen, controller.ErrCapacity)
	}
	b.createAttempts[intent.ResourceID] = true
	b.unlisted[intent.ResourceID] = unlistedCreate{}
	b.mu.Unlock()
	spec := b.podSpec(intent)
	created, err := b.api.Create(ctx, CreateRequest{Spec: spec, BootstrapMode: MemoryOnlyBootstrap, BootstrapToken: intent.BootstrapToken,
		StartupDeadline: intent.StartupDeadline})
	// Finished whatever the outcome: a failed POST may still have made a Pod,
	// and any read that starts after this line lists it if it exists.
	b.mu.Lock()
	b.createSeq++
	b.unlisted[intent.ResourceID] = unlistedCreate{done: true, finished: b.createSeq}
	b.mu.Unlock()
	if err != nil {
		return controller.WorkerObservation{}, err
	}
	leaseID, err := b.validateResource(created)
	if err != nil || !created.PodExists {
		if err == nil {
			err = errors.New("Kubernetes create response lacks a live Pod identity")
		}
		return controller.WorkerObservation{}, err
	}
	if leaseID != intent.LeaseID || created.Name != intent.ResourceID {
		return controller.WorkerObservation{}, errors.New("Kubernetes create response identity mismatch")
	}
	b.mu.Lock()
	b.uids[created.Name] = created.UID
	b.mu.Unlock()
	return controller.WorkerObservation{LeaseID: intent.LeaseID, ResourceID: intent.ResourceID}, nil
}

// unlistedCreates counts creates that a read begun at readSeq may have
// missed and did not list: every one still in flight, and every one that
// finished after the read began. A create that finished before it is listed
// if its Pod exists, so it is never counted twice. Callers hold mu.
func (b *Backend) unlistedCreates(listed []controller.WorkerObservation, readSeq uint64) int {
	seen := make(map[string]bool, len(listed))
	for _, worker := range listed {
		seen[worker.ResourceID] = true
	}
	count := 0
	for resourceID, create := range b.unlisted {
		if !seen[resourceID] && (!create.done || create.finished > readSeq) {
			count++
		}
	}
	return count
}

// createFinished ends one create's claim on the unlisted table. Once no create
// is running, every later read begins after every recorded create finished,
// so none of them can be counted again and the table is emptied.
func (b *Backend) createFinished() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.createsRunning--
	if b.createsRunning == 0 {
		clear(b.unlisted)
	}
}

func (b *Backend) Destroy(ctx context.Context, worker controller.WorkerObservation) (controller.WorkerAbsence, error) {
	if ctx == nil {
		return controller.WorkerAbsence{}, errors.New("worker deletion requires a context")
	}
	if err := b.validateIdentity(worker.LeaseID, worker.ResourceID); err != nil {
		return controller.WorkerAbsence{}, err
	}
	b.mu.Lock()
	uid := b.uids[worker.ResourceID]
	if uid == "" {
		b.mu.Unlock()
		return controller.WorkerAbsence{}, errors.New("worker UID is unknown; complete inventory before deletion")
	}
	// NO ONE-SHOT GUARD HERE (homelab#1094). This used to refuse a second
	// Destroy for the same resource, so a delete that errored once was never
	// sent again and the Pod held its slot until someone deleted it by hand.
	// A repeat is safe: Transport.Delete looks the Pod up by name, refuses a UID
	// or label mismatch, deletes with a UID precondition, and treats NotFound
	// as done. The controller paces repeats (PendingWorkerRedeletes).
	b.mu.Unlock()
	witness, err := b.api.Delete(ctx, DeleteRequest{Namespace: b.config.Namespace, Name: worker.ResourceID, UID: uid,
		Labels: b.labels(worker.LeaseID, worker.ResourceID)})
	if err != nil {
		return controller.WorkerAbsence{}, err
	}
	if witness.UID != uid {
		return controller.WorkerAbsence{}, errors.New("Kubernetes delete response UID mismatch")
	}
	absence := controller.WorkerAbsence{LeaseID: worker.LeaseID, ResourceID: worker.ResourceID,
		ProcessAbsent: witness.ProcessAbsent, StorageAbsent: witness.StorageAbsent}
	if witness.ProcessAbsent && witness.StorageAbsent {
		b.mu.Lock()
		delete(b.uids, worker.ResourceID)
		b.mu.Unlock()
	}
	return absence, nil
}

func (b *Backend) podSpec(intent controller.WorkerCreateIntent) PodSpec {
	labels := b.labels(intent.LeaseID, intent.ResourceID)
	return PodSpec{
		Namespace: b.config.Namespace, Name: intent.ResourceID, Labels: labels,
		RuntimeClassName:             b.config.RuntimeClassName,
		NodeSelector:                 map[string]string{"kubernetes.io/hostname": b.config.NodeName, "kubernetes.io/arch": "amd64"},
		AutomountServiceAccountToken: false, EnableServiceLinks: false,
		HostNetwork: false, HostPID: false, HostIPC: false, RestartPolicy: "Never",
		TerminationGracePeriodSec: 30, SeccompProfile: "RuntimeDefault",
		Containers: []Container{
			{Name: "runner", Image: b.config.RunnerImage, Command: []string{workerBootstrapCommand},
				Args: []string{b.config.WorkerCommand, "-controller", intent.BootstrapURL, "-lease", intent.LeaseID,
					"-runner-root", b.config.RunnerRoot, "-ca-file", b.config.CAFile},
				Environment: runnerContainerEnv(b.config.RunnerRoot),
				Resources:   b.config.RunnerResources, RunAsNonRoot: true, AllowPrivilegeEscalation: false,
				ReadOnlyRootFilesystem: false,
				Mounts: []VolumeMount{{Name: "work", MountPath: filepath.Join(b.config.RunnerRoot, "_work")},
					{Name: "socket", MountPath: "/var/run"}, {Name: "tmp", MountPath: "/tmp"}},
			},
			{Name: "dockerd", Image: b.config.DockerImage, Command: []string{"dockerd-entrypoint.sh"},
				Args:        dockerdArgs(),
				Environment: dockerdContainerEnv(),
				Resources:   b.config.DockerResources, Privileged: true, AllowPrivilegeEscalation: true,
				ReadOnlyRootFilesystem: false,
				Mounts: []VolumeMount{{Name: "socket", MountPath: "/var/run"},
					{Name: "work", MountPath: filepath.Join(b.config.RunnerRoot, "_work")}, {Name: "tmp", MountPath: "/tmp"}},
			},
		},
		Volumes: []Volume{{Name: "work", Medium: "Memory", SizeBytes: b.config.Volumes.Work},
			{Name: "socket", Medium: "Memory", SizeBytes: b.config.Volumes.Socket},
			{Name: "tmp", Medium: "Memory", SizeBytes: b.config.Volumes.Tmp}},
	}
}

// runnerContainerEnv is the runner container's exact plain environment set, in
// one place. podSpec injects it and validateCreateSpec / validateObservedRunner
// assert exact equality against it so injection and validation cannot drift
// (homelab#730).
func runnerContainerEnv(runnerRoot string) []EnvVar {
	return []EnvVar{
		{Name: "DOCKER_HOST", Value: "unix:///var/run/docker.sock"},
		{Name: "RUNNER_TOOL_CACHE", Value: filepath.Join(runnerRoot, "_work", "_tool")},
	}
}

// dockerdContainerEnv is the dockerd sidecar's exact plain environment set
// (empty). Shared between podSpec and validateCreateSpec (homelab#730).
func dockerdContainerEnv() []EnvVar {
	return nil
}

// dockerdArgs is the daemon's exact argument vector, in one place. The Pod
// spec builds it and the observed-Pod validator compares against it, so a
// change to one cannot silently pass the other.
func dockerdArgs() []string {
	return []string{"dockerd", "--host=unix:///var/run/docker.sock", "--data-root=" + dockerDataRoot,
		"--exec-root=/var/run/docker", "--storage-driver=overlay2", "--ip6tables=false",
		"--group=" + strconv.Itoa(workerDockerGID)}
}

func (b *Backend) labels(leaseID, resourceID string) map[string]string {
	return map[string]string{LabelManagedBy: ManagedByValue, LabelPlatform: PlatformLinux,
		LabelOwnerUID: b.config.OwnerUID, LabelLeaseID: leaseID, LabelResourceID: resourceID}
}

func (b *Backend) validateResource(item WorkerResource) (string, error) {
	if item.Namespace != b.config.Namespace || item.Name == "" || !dnsLabelRE.MatchString(item.Name) || item.UID == "" {
		return "", errors.New("Kubernetes worker has invalid namespace, name, or UID")
	}
	if item.RuntimeClassName != b.config.RuntimeClassName {
		return "", errors.New("Kubernetes worker uses an unexpected Kata runtime class")
	}
	if len(item.Annotations) != 0 {
		return "", errors.New("Kubernetes worker contains uncontrolled annotations")
	}
	expect := item.Labels[LabelLeaseID]
	if item.Labels[LabelManagedBy] != ManagedByValue || item.Labels[LabelPlatform] != PlatformLinux ||
		item.Labels[LabelOwnerUID] != b.config.OwnerUID || item.Labels[LabelResourceID] != item.Name {
		return "", errors.New("Kubernetes worker ownership labels do not match")
	}
	if _, err := uuid.Parse(expect); err != nil || uuid.MustParse(expect).String() != expect {
		return "", errors.New("Kubernetes worker has a non-canonical lease UUID")
	}
	return expect, nil
}

func (b *Backend) validateIntent(intent controller.WorkerCreateIntent) error {
	if err := b.validateIdentity(intent.LeaseID, intent.ResourceID); err != nil {
		return err
	}
	if intent.BootstrapURL != b.config.BootstrapURL || intent.StartupDeadline.IsZero() {
		return errors.New("worker create intent does not match immutable bootstrap configuration")
	}
	if _, err := bootstrap.ParseToken(intent.BootstrapToken.Encoded()); err != nil {
		return errors.New("worker create intent lacks a valid bootstrap capability")
	}
	return nil
}

func (b *Backend) validateIdentity(leaseID, resourceID string) error {
	parsed, err := uuid.Parse(leaseID)
	if err != nil || parsed.String() != leaseID || !dnsLabelRE.MatchString(resourceID) {
		return errors.New("worker identity is not canonical")
	}
	return nil
}

func (b *Backend) perWorkerFootprint() Footprint {
	resources := b.config.RunnerResources.Requests.add(b.config.DockerResources.Requests).add(b.config.Overhead)
	resources.StorageBytes = PerWorkerStorageBytes(b.config.ContainerRootfsBytes, b.config.Overhead.StorageBytes)
	return resources
}

// freeSlots recomputes the slot count from the three byte figures the backend
// can see and caps it at the witness's own five-dimension verdict. It reports
// WHICH dimension bound, because `0` alone sent an operator looking at storage
// for a shortage that was memory committed to idle CronJob limits.
//
// Negative values are still refused here. The witness clamps an oversubscribed
// host to zero before this point, so a negative arriving here means an adapter
// disagreed with its own contract -- unknown, not full, and fail-closed.
func (b *Backend) freeSlots(headroom Headroom, active int) (int, string, error) {
	if !headroom.Complete || active < 0 || headroom.CPUMilli < 0 || headroom.MemoryBytes < 0 || headroom.StorageBytes < 0 {
		return 0, DimensionUnknown, errors.New("Kubernetes headroom is incomplete or negative")
	}
	if headroom.FreeSlots < 0 || headroom.Binding == "" {
		return 0, DimensionUnknown, errors.New("Kubernetes headroom lacks a trusted slot verdict")
	}
	if active > b.config.Capacity-b.config.LegacySlots {
		return 0, DimensionUnknown, errors.New("Kubernetes workers exceed the configured qualification ceiling")
	}
	per := b.perWorkerFootprint()
	// The witness first: it measured every dimension, including the image cache
	// and the log budget, which are invisible from here.
	free, binding := int64(headroom.FreeSlots), headroom.Binding
	for _, dimension := range []struct {
		name string
		free int64
		per  int64
	}{
		{DimensionCPU, headroom.CPUMilli, per.CPUMilli},
		{DimensionMemory, headroom.MemoryBytes, per.MemoryBytes},
		{DimensionStorage, headroom.StorageBytes, per.StorageBytes},
	} {
		if capacity := dimension.free / dimension.per; capacity < free {
			free, binding = capacity, dimension.name
		}
	}
	if logical := int64(b.config.Capacity - b.config.LegacySlots - active); free > logical {
		free, binding = logical, DimensionSlots
	}
	return int(free), binding, nil
}

func (c Config) validate() error {
	if !dnsLabelRE.MatchString(c.Namespace) || c.OwnerUID == "" || !labelValueRE.MatchString(c.OwnerUID) || c.NodeName == "" {
		return errors.New("Kubernetes backend requires a namespace, owner UID, and node")
	}
	if c.RuntimeClassName == "" || c.RuntimeClassName == "kata-qemu" || c.RuntimeClassName == "sjo-kata-qemu-probe" || !dnsLabelRE.MatchString(c.RuntimeClassName) {
		return errors.New("Kubernetes backend requires a dedicated non-canary Kata runtime class")
	}
	if !digestRE.MatchString(c.RunnerImage) || !digestRE.MatchString(c.DockerImage) {
		return errors.New("Kubernetes worker images must be pinned by sha256 digest")
	}
	if c.WorkerCommand == "" || !filepath.IsAbs(c.WorkerCommand) || c.RunnerRoot == "" || !filepath.IsAbs(c.RunnerRoot) || filepath.Clean(c.RunnerRoot) != c.RunnerRoot ||
		c.CAFile == "" || !filepath.IsAbs(c.CAFile) || filepath.Clean(c.CAFile) != c.CAFile {
		return errors.New("Kubernetes worker command, runner root, and CA path must be absolute")
	}
	if !validBootstrapOrigin(c.BootstrapURL) {
		return errors.New("Kubernetes backend requires the configured HTTPS bootstrap origin")
	}
	if c.Capacity < 0 || c.Capacity > LinuxCapacityCeiling || c.LegacySlots < 0 || c.LegacySlots > c.Capacity {
		return fmt.Errorf("Kubernetes backend capacity must be 0..%d to fit the Linux qualification ceiling", LinuxCapacityCeiling)
	}
	// The last clause is configuration consistency, not a bound: the declared
	// ephemeral-storage requests must cover the declared memory volumes.
	// Nothing enforces either figure on this host. Kata sizes guest tmpfs to the
	// whole VM, so a memory volume's cap is its writer's cgroup, not its
	// sizeLimit; and kubelet's eviction manager cannot read stats while a
	// devmapper worker runs, so no ephemeral-storage limit is enforced at all.
	// The one storage cap a job meets is ENOSPC at its own thin device.
	if !validResources(c.RunnerResources) || !validResources(c.DockerResources) || !c.Overhead.positive() ||
		c.ContainerRootfsBytes <= 0 || c.ContainerRootfsBytes < c.RunnerResources.Limits.StorageBytes || c.ContainerRootfsBytes < c.DockerResources.Limits.StorageBytes ||
		c.Volumes.Work <= 0 || c.Volumes.Socket <= 0 || c.Volumes.Tmp <= 0 || c.Volumes.total() <= 0 ||
		c.RunnerResources.Requests.StorageBytes+c.DockerResources.Requests.StorageBytes < c.Volumes.total() {
		return errors.New("Kubernetes worker resources and memory-backed storage budget are invalid")
	}
	return nil
}

func validResources(r ContainerResources) bool {
	return r.Requests.positive() && r.Limits.positive() && r.Limits.CPUMilli >= r.Requests.CPUMilli &&
		r.Limits.MemoryBytes >= r.Requests.MemoryBytes && r.Limits.StorageBytes >= r.Requests.StorageBytes
}

func validBootstrapOrigin(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.User != nil || u.Host == "" || (u.Path != "" && u.Path != "/") ||
		u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return false
	}
	host, port, err := net.SplitHostPort(u.Host)
	if err != nil || host == "" || net.ParseIP(host) != nil || port == "" {
		return false
	}
	return true
}

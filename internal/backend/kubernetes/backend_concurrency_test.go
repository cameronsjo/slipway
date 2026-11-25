package kubernetes

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/cameronsjo/slipway/internal/controller"
)

// gatedAPI is a Kubernetes API whose List and Create each wait on a gate the
// test controls, so two creates can be interleaved at an exact point. The
// listing is every Pod a finished Create made, as a real List would show.
type gatedAPI struct {
	config Config
	free   int

	mu       sync.Mutex
	pods     []WorkerResource
	creates  int
	listGate map[int]chan struct{} // List call n (1-based) waits on this
	lists    int
	postGate map[string]chan struct{} // Create for this resource waits on this
	posting  chan string              // a Create has sent its POST
}

func newGatedAPI(config Config, free int) *gatedAPI {
	return &gatedAPI{config: config, free: free, listGate: map[int]chan struct{}{},
		postGate: map[string]chan struct{}{}, posting: make(chan string, 8)}
}

func (a *gatedAPI) List(context.Context, string, string) (ListResult, error) {
	// The listing is taken when the read begins; a gated read returns that
	// view late, as a slow List would.
	a.mu.Lock()
	a.lists++
	gate := a.listGate[a.lists]
	items := append([]WorkerResource(nil), a.pods...)
	a.mu.Unlock()
	if gate != nil {
		<-gate
	}
	return ListResult{Complete: true, Items: items}, nil
}

// Headroom reports free as the test last set it. It does not fall as Pods are
// created, the way a witness that cannot yet see an unbooted Pod's memory
// would not, so only the backend's own in-flight accounting can refuse.
func (a *gatedAPI) Headroom(context.Context, string, Footprint) (Headroom, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return enoughHeadroom(a.config, int64(a.free)), nil
}

func (a *gatedAPI) Create(_ context.Context, request CreateRequest) (WorkerResource, error) {
	a.mu.Lock()
	a.creates++
	gate := a.postGate[request.Spec.Name]
	a.mu.Unlock()
	a.posting <- request.Spec.Name
	if gate != nil {
		<-gate
	}
	intent := controller.WorkerCreateIntent{LeaseID: request.Spec.Labels[LabelLeaseID], ResourceID: request.Spec.Name}
	pod := ownedResource(a.config, intent, "uid-"+request.Spec.Name, true, true, true)
	a.mu.Lock()
	a.pods = append(a.pods, pod)
	a.mu.Unlock()
	return pod, nil
}

func (a *gatedAPI) Delete(context.Context, DeleteRequest) (AbsenceWitness, error) {
	return AbsenceWitness{}, errors.New("not used")
}

// Two creates racing for the last slot: the second's read cannot list the
// first's Pod while its POST is still out, so without in-flight accounting
// both would see one free slot and both would boot.
func TestAConcurrentCreateCannotTakeASlotAnInFlightCreateClaimed(t *testing.T) {
	config := testConfig()
	api := newGatedAPI(config, 1)
	backend, err := New(api, config)
	if err != nil {
		t.Fatal(err)
	}
	first, _ := testIntent(config)
	second, _ := testIntent(config)
	release := make(chan struct{})
	api.postGate[first.ResourceID] = release

	firstDone := make(chan error, 1)
	go func() {
		_, err := backend.Create(context.Background(), first)
		firstDone <- err
	}()
	<-api.posting // the first create has claimed the slot and is POSTing

	_, err = backend.Create(context.Background(), second)
	if !errors.Is(err, controller.ErrCapacity) {
		t.Fatalf("second create beside an in-flight one = %v, want ErrCapacity: one free slot was taken twice", err)
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first create: %v", err)
	}
	if api.creates != 1 {
		t.Fatalf("Pods requested = %d, want 1", api.creates)
	}
}

// The narrower race: the second create's read begins before the first's POST
// returns, and its check runs after. The first is no longer in flight, but the
// read that the second trusts still does not list it.
func TestACreateThatFinishedAfterAReadBeganStillCountsAgainstThatRead(t *testing.T) {
	config := testConfig()
	api := newGatedAPI(config, 1)
	backend, err := New(api, config)
	if err != nil {
		t.Fatal(err)
	}
	first, _ := testIntent(config)
	second, _ := testIntent(config)
	releasePost := make(chan struct{})
	releaseList := make(chan struct{})
	api.postGate[first.ResourceID] = releasePost
	api.listGate[2] = releaseList // the second create's inventory read

	firstDone := make(chan error, 1)
	go func() {
		_, err := backend.Create(context.Background(), first)
		firstDone <- err
	}()
	<-api.posting

	secondDone := make(chan error, 1)
	go func() {
		_, err := backend.Create(context.Background(), second)
		secondDone <- err
	}()
	waitFor(t, func() bool { api.mu.Lock(); defer api.mu.Unlock(); return api.lists == 2 })

	close(releasePost) // the first Pod now exists, after the second read began
	if err := <-firstDone; err != nil {
		t.Fatalf("first create: %v", err)
	}
	close(releaseList) // the second read's view predates the first Pod
	if err := <-secondDone; !errors.Is(err, controller.ErrCapacity) {
		t.Fatalf("second create = %v, want ErrCapacity: its read missed a Pod that finished after the read began", err)
	}
}

// A create is counted only while some read could have missed it. Once no
// create runs, a fresh read lists the Pod itself, and counting it again would
// hold a slot the host really has free.
func TestAListedCreateIsNotCountedTwice(t *testing.T) {
	config := testConfig()
	api := newGatedAPI(config, 2)
	backend, err := New(api, config)
	if err != nil {
		t.Fatal(err)
	}
	first, _ := testIntent(config)
	if _, err := backend.Create(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	<-api.posting
	api.mu.Lock()
	api.free = 1 // the witness now counts the first Pod
	api.mu.Unlock()
	second, _ := testIntent(config)
	if _, err := backend.Create(context.Background(), second); err != nil {
		t.Fatalf("second create with one slot really free = %v", err)
	}
	<-api.posting
	backend.mu.Lock()
	left := len(backend.unlisted)
	backend.mu.Unlock()
	if left != 0 {
		t.Fatalf("unlisted creates kept after every create returned: %d", left)
	}
}

// A create's own inventory read only adds UIDs. The second create's read began
// before the first Pod existed, so it does not list it; had that read replaced
// the UID map, it would drop the UID the first create just recorded, and the
// first worker's Destroy would refuse until the next full Inventory.
func TestACreatesReadKeepsTheUIDAnotherCreateRecorded(t *testing.T) {
	config := testConfig()
	api := newGatedAPI(config, 2)
	backend, err := New(api, config)
	if err != nil {
		t.Fatal(err)
	}
	first, _ := testIntent(config)
	second, _ := testIntent(config)
	releasePost := make(chan struct{})
	releaseList := make(chan struct{})
	api.postGate[first.ResourceID] = releasePost
	api.listGate[2] = releaseList // the second create's inventory read

	firstDone := make(chan error, 1)
	go func() {
		_, err := backend.Create(context.Background(), first)
		firstDone <- err
	}()
	<-api.posting

	secondDone := make(chan error, 1)
	go func() {
		_, err := backend.Create(context.Background(), second)
		secondDone <- err
	}()
	waitFor(t, func() bool { api.mu.Lock(); defer api.mu.Unlock(); return api.lists == 2 })

	close(releasePost)
	if err := <-firstDone; err != nil {
		t.Fatalf("first create: %v", err)
	}
	close(releaseList) // the second read's listing predates the first Pod
	if err := <-secondDone; err != nil {
		t.Fatalf("second create with two free slots: %v", err)
	}
	<-api.posting
	backend.mu.Lock()
	uid, known := backend.uids[first.ResourceID]
	backend.mu.Unlock()
	if !known || uid != "uid-"+first.ResourceID {
		t.Fatalf("first worker's UID after a concurrent create's read = %q (known=%t); the read replaced the map", uid, known)
	}
}

func waitFor(t *testing.T, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !done() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for the interleaving")
		}
		time.Sleep(time.Millisecond)
	}
}

package kubernetes

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/cameronsjo/slipway/internal/controller"
)

// podsGoneAfter is a Pod source that still lists the worker for its first
// `present` reads and then no longer does: a deleted Pod that takes a few
// seconds to disappear, which is what MS-A2 showed (2-17 s, homelab#951).
type podsGoneAfter struct {
	pod     PodObject
	present int
	reads   int
}

func (p *podsGoneAfter) ListPods(context.Context, string, string) (PodScope, error) {
	p.reads++
	if p.reads <= p.present {
		return PodScope{Complete: true, Items: []PodObject{p.pod}}, nil
	}
	return PodScope{Complete: true}, nil
}

// fakeClock drives absenceWait without sleeping: each sleep advances it.
type fakeClock struct {
	at     time.Time
	sleeps int
}

func (c *fakeClock) now() time.Time { return c.at }
func (c *fakeClock) sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.sleeps++
	c.at = c.at.Add(d)
	return nil
}

type absenceWaitFixture struct {
	inventory *TrustedInventory
	pods      *podsGoneAfter
	clock     *fakeClock
	request   DeleteRequest
}

func newAbsenceWaitFixture(t *testing.T, present int) absenceWaitFixture {
	t.Helper()
	config := testConfig()
	intent := controller.WorkerCreateIntent{LeaseID: "11111111-1111-4111-8111-111111111111", ResourceID: "slipway-linux-11111111-1111-4111-8111-111111111111"}
	owned := ownedResource(config, intent, "pod-uid-1", true, false, false)
	pods := &podsGoneAfter{pod: podObject(owned), present: present}
	sources := TrustedPhysicalSources{Pods: pods, Processes: &fakeProcessSource{scope: WitnessScope{Complete: true}},
		Storage: &fakeStorageSource{scope: WitnessScope{Complete: true}}, Capacity: &fakeCapacitySource{snapshot: physicalCapacity()}}
	inventory := newPhysicalInventory(t, sources)
	clock := &fakeClock{at: time.Date(2026, 9, 25, 0, 0, 0, 0, time.UTC)}
	inventory.absence.now, inventory.absence.sleep = clock.now, clock.sleep
	return absenceWaitFixture{inventory: inventory, pods: pods, clock: clock,
		request: DeleteRequest{Namespace: config.Namespace, Name: intent.ResourceID, UID: owned.UID, Labels: owned.Labels}}
}

func TestWaitAbsentReturnsAsSoonAsAbsenceIsProven(t *testing.T) {
	f := newAbsenceWaitFixture(t, 3) // gone on the 4th read
	witness, err := f.inventory.WaitAbsent(context.Background(), f.request)
	if err != nil {
		t.Fatal(err)
	}
	if !witness.ProcessAbsent || !witness.StorageAbsent || witness.UID != f.request.UID {
		t.Fatalf("absence proven on the 4th poll was not returned: %+v", witness)
	}
	if f.pods.reads != 4 || f.clock.sleeps != 3 {
		t.Fatalf("reads=%d sleeps=%d, want 4 and 3: the wait must stop at the first proof", f.pods.reads, f.clock.sleeps)
	}
}

func TestWaitAbsentTimesOutWithTheLastReadingAndKeepsTheReservation(t *testing.T) {
	f := newAbsenceWaitFixture(t, 1<<20) // never gone
	witness, err := f.inventory.WaitAbsent(context.Background(), f.request)
	if err != nil {
		t.Fatal(err)
	}
	if witness.ProcessAbsent || witness.StorageAbsent {
		t.Fatalf("a Pod still listed was reported absent after the bound: %+v", witness)
	}
	waited := f.clock.at.Sub(time.Date(2026, 9, 25, 0, 0, 0, 0, time.UTC))
	if waited < f.inventory.absence.deadline || waited > f.inventory.absence.deadline+f.inventory.absence.interval {
		t.Fatalf("waited %s, want the %s bound (within one %s poll)", waited, f.inventory.absence.deadline, f.inventory.absence.interval)
	}
}

func TestWaitAbsentStopsWhenTheContextIsCancelled(t *testing.T) {
	f := newAbsenceWaitFixture(t, 1<<20)
	ctx, cancel := context.WithCancel(context.Background())
	f.inventory.absence.sleep = func(context.Context, time.Duration) error {
		cancel()
		return f.clock.sleep(ctx, time.Second)
	}
	_, err := f.inventory.WaitAbsent(ctx, f.request)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("a cancelled wait returned %v, want context.Canceled", err)
	}
	if f.pods.reads != 1 {
		t.Fatalf("the poll kept reading after cancellation: reads=%d", f.pods.reads)
	}
}

// The bound must stay under the host witness's per-call timeout in the
// deployed controller-config.json (checked via testdata/controller-config.json
// when extracted standalone, and verified byte-for-byte against the repo copy
// when running inside homelab), and must cover the 17 s worst case measured on
// MS-A2.
func TestTheAbsenceBoundSitsUnderTheWitnessTimeout(t *testing.T) {
	wait := defaultAbsenceWait()
	const testdataPath = "testdata/controller-config.json"
	const repoPath = "../../../../../hosts/ms-a2/github-runner-pool/controller-config.json"
	raw, err := os.ReadFile(testdataPath)
	if err != nil {
		t.Fatal(err)
	}
	if repoRaw, err := os.ReadFile(repoPath); err == nil {
		if string(repoRaw) != string(raw) {
			t.Fatalf("%s drifted from %s; sync the testdata fixture", testdataPath, repoPath)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	var deployed struct {
		WitnessTimeout string `json:"witness_timeout"`
	}
	if err := json.Unmarshal(raw, &deployed); err != nil {
		t.Fatal(err)
	}
	witnessTimeout, err := time.ParseDuration(deployed.WitnessTimeout)
	if err != nil {
		t.Fatalf("controller-config.json witness_timeout %q: %v", deployed.WitnessTimeout, err)
	}
	if wait.deadline >= witnessTimeout {
		t.Fatalf("absence bound %s is not under the witness timeout %s", wait.deadline, witnessTimeout)
	}
	if wait.deadline < 17*time.Second {
		t.Fatalf("absence bound %s does not cover the measured 17 s Pod disappearance", wait.deadline)
	}
	if wait.interval <= 0 || wait.interval >= wait.deadline {
		t.Fatalf("poll interval %s is not a positive fraction of the bound", wait.interval)
	}
}

// podsThenError lists the worker on its first read and fails every later
// read: a transient witness fault in the middle of a teardown's wait.
type podsThenError struct {
	pod   PodObject
	reads int
}

func (p *podsThenError) ListPods(context.Context, string, string) (PodScope, error) {
	p.reads++
	if p.reads == 1 {
		return PodScope{Complete: true, Items: []PodObject{p.pod}}, nil
	}
	return PodScope{}, errors.New("witness read failed")
}

func errorFixture(t *testing.T, pods PodSource) absenceWaitFixture {
	t.Helper()
	f := newAbsenceWaitFixture(t, 0)
	f.inventory.sources.Pods = pods
	return f
}

// homelab#951 review: after one good reading, a later read error ends the
// wait like the deadline does, returning that reading with no error. It
// fails closed (the reading is not absent, so the reservation stays) and it
// cannot fail Maintain and with it the whole cycle.
func TestALaterReadErrorReturnsTheLastGoodReading(t *testing.T) {
	config := testConfig()
	owned := ownedResource(config, controller.WorkerCreateIntent{
		LeaseID: "11111111-1111-4111-8111-111111111111", ResourceID: "slipway-linux-11111111-1111-4111-8111-111111111111"},
		"pod-uid-1", true, false, false)
	pods := &podsThenError{pod: podObject(owned)}
	f := errorFixture(t, pods)
	witness, err := f.inventory.WaitAbsent(context.Background(), f.request)
	if err != nil {
		t.Fatalf("a read error after a good reading failed the wait: %v", err)
	}
	if witness.ProcessAbsent || witness.StorageAbsent || witness.UID != f.request.UID {
		t.Fatalf("the last good reading was not returned as-is: %+v", witness)
	}
	if pods.reads != 2 {
		t.Fatalf("reads=%d, want 2", pods.reads)
	}
}

// The first read failing still returns the error, as the single read always did.
func TestAFirstReadErrorIsStillReturned(t *testing.T) {
	f := errorFixture(t, &fakePodSource{err: errors.New("witness read failed")})
	if _, err := f.inventory.WaitAbsent(context.Background(), f.request); err == nil {
		t.Fatal("a failed first read was reported as a reading")
	}
}

// #960: the maintain pass's shared budget ends a wait early with its latest
// reading, and a budget already spent still gets exactly one read (the
// pre-#995 single read). Neither reports absence it did not see.
func TestWaitAbsentStopsAtTheSharedBudget(t *testing.T) {
	f := newAbsenceWaitFixture(t, 1<<20) // never gone
	start := f.clock.at
	ctx := controller.WithAbsenceBudget(context.Background(), start.Add(5*time.Second))
	witness, err := f.inventory.WaitAbsent(ctx, f.request)
	if err != nil {
		t.Fatal(err)
	}
	if witness.ProcessAbsent || witness.StorageAbsent {
		t.Fatalf("a Pod still listed was reported absent at the budget: %+v", witness)
	}
	if waited := f.clock.at.Sub(start); waited != 5*time.Second {
		t.Fatalf("waited %s, want the 5s shared budget, not the %s per-wait bound", waited, f.inventory.absence.deadline)
	}
}

func TestWaitAbsentWithASpentBudgetReadsOnce(t *testing.T) {
	f := newAbsenceWaitFixture(t, 1<<20)
	ctx := controller.WithAbsenceBudget(context.Background(), f.clock.at.Add(-time.Second))
	witness, err := f.inventory.WaitAbsent(ctx, f.request)
	if err != nil {
		t.Fatal(err)
	}
	if witness.ProcessAbsent || witness.StorageAbsent || f.pods.reads != 1 || f.clock.sleeps != 0 {
		t.Fatalf("spent budget: reads=%d sleeps=%d witness=%+v, want one read, no sleep, not absent", f.pods.reads, f.clock.sleeps, witness)
	}
}

func TestALaterBudgetDoesNotExtendTheWait(t *testing.T) {
	f := newAbsenceWaitFixture(t, 1<<20)
	start := f.clock.at
	ctx := controller.WithAbsenceBudget(context.Background(), start.Add(time.Hour))
	if _, err := f.inventory.WaitAbsent(ctx, f.request); err != nil {
		t.Fatal(err)
	}
	if waited := f.clock.at.Sub(start); waited > f.inventory.absence.deadline+f.inventory.absence.interval {
		t.Fatalf("waited %s: a budget later than the per-wait bound must not extend it", waited)
	}
}

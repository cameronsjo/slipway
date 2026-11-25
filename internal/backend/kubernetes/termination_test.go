package kubernetes

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// TestListReportsRunnerContainerTermination pins the Kubernetes half of the
// terminated-worker signal. The controller retires a provisioning lease on this
// evidence instead of waiting five minutes for the unused-worker timeout, so a
// listing that loses it puts the pool back on the clock silently.
//
// The exit code travels with it because 0 is the diagnosis: a runner offered no
// work exits cleanly, and a non-zero exit is instead routed to the provisioning
// breaker.
func TestListReportsRunnerContainerTermination(t *testing.T) {
	for _, arm := range []struct {
		name       string
		state      corev1.ContainerState
		container  string
		terminated bool
		exit       int
	}{
		{name: "terminated-clean", container: "runner", exit: 0, terminated: true,
			state: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "Completed", ExitCode: 0}}},
		{name: "terminated-crashed", container: "runner", exit: 2, terminated: true,
			state: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "Error", ExitCode: 2}}},
		{name: "running", container: "runner",
			state: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}},
		{name: "waiting", container: "runner",
			state: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}}},
		// A sidecar that finished says nothing about the runner. Reading any
		// terminated container would retire a lease whose runner is still working.
		{name: "other-container-terminated", container: "sidecar",
			state: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 7}}},
	} {
		t.Run(arm.name, func(t *testing.T) {
			transport, config, intent, _ := newTransportFixture(t)
			backend := &Backend{config: config}
			pod := transport.toPod(backend.podSpec(intent))
			pod.UID = "pod-uid-termination"
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: arm.container, State: arm.state}}
			client := transport.client.(*fake.Clientset)
			client.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
				return true, &corev1.PodList{Items: []corev1.Pod{*pod}}, nil
			})
			transport.witness.(*witnessFixture).inventory = PhysicalInventory{Complete: true,
				Workers: []PhysicalWorker{{Name: pod.Name, UID: string(pod.UID), Labels: pod.Labels,
					RuntimeClassName: config.RuntimeClassName, ProcessPresent: true, StoragePresent: true}}}

			result, err := transport.List(context.Background(), config.Namespace, backend.Selector())
			if err != nil || !result.Complete || len(result.Items) != 1 {
				t.Fatalf("incomplete listing: %+v %v", result, err)
			}
			item := result.Items[0]
			if item.RunnerTerminated != arm.terminated || item.RunnerExitCode != arm.exit {
				t.Fatalf("termination not carried: terminated=%v exit=%d, want terminated=%v exit=%d",
					item.RunnerTerminated, item.RunnerExitCode, arm.terminated, arm.exit)
			}
		})
	}
}

// TestInventoryCarriesTerminationToTheController is the other half: the backend
// must hand the transport's observation to the controller's WorkerObservation,
// which is the only field the retirement decision reads.
func TestInventoryCarriesTerminationToTheController(t *testing.T) {
	config := testConfig()
	intent, _ := testIntent(config)
	resource := ownedResource(config, intent, "pod-uid-terminated", true, true, true)
	resource.RunnerTerminated = true
	resource.RunnerExitCode = 0
	fake := &fakeAPI{list: ListResult{Complete: true, Items: []WorkerResource{resource}}, headroom: enoughHeadroom(config, 4)}
	backend, err := New(fake, config)
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := backend.Inventory(context.Background())
	if err != nil || len(inventory.Workers) != 1 {
		t.Fatalf("unexpected inventory: %+v %v", inventory, err)
	}
	if !inventory.Workers[0].Terminated || inventory.Workers[0].ExitCode != 0 {
		t.Fatalf("termination did not reach the controller observation: %+v", inventory.Workers[0])
	}

	// The negative control. A live worker must report Terminated false, or the
	// controller retires a lease whose job is still running.
	live := ownedResource(config, intent, "pod-uid-live", true, true, true)
	liveBackend, err := New(&fakeAPI{list: ListResult{Complete: true, Items: []WorkerResource{live}},
		headroom: enoughHeadroom(config, 4)}, config)
	if err != nil {
		t.Fatal(err)
	}
	liveInventory, err := liveBackend.Inventory(context.Background())
	if err != nil || len(liveInventory.Workers) != 1 {
		t.Fatalf("unexpected inventory: %+v %v", liveInventory, err)
	}
	if liveInventory.Workers[0].Terminated {
		t.Fatal("a live worker was reported terminated")
	}
}

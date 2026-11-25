package kubernetes

import (
	"context"
	"fmt"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

// Other workloads are charged their CPU REQUEST and their max(request, limit)
// MEMORY. The asymmetry is deliberate: CPU is compressible, memory is not.
// conservativeResources carries the reasoning and the measured node numbers.
func TestKubernetesCapacityChargesCPUByRequestAndMemoryByLimit(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "homelab-ms-a2", UID: types.UID("node-uid")},
		Status: corev1.NodeStatus{Allocatable: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("16"), corev1.ResourceMemory: resource.MustParse("64Gi")}}}
	other := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "default"},
		Spec: corev1.PodSpec{
			NodeName: node.Name,
			Containers: []corev1.Container{{Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m"), corev1.ResourceMemory: resource.MustParse("1Gi")},
				Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("3Gi")},
			}}},
			InitContainers: []corev1.Container{{Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("250m"), corev1.ResourceMemory: resource.MustParse("512Mi")},
			}}},
			Resources: &corev1.ResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceCPU: resource.MustParse("50m"), corev1.ResourceMemory: resource.MustParse("64Mi"),
			}},
			Overhead: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("128Mi")},
		},
	}
	owned := other.DeepCopy()
	owned.Name, owned.Namespace = "pool", "github-runners"
	owned.Labels = map[string]string{LabelManagedBy: ManagedByValue, LabelPlatform: PlatformLinux, LabelOwnerUID: "controller-a"}
	finished := other.DeepCopy()
	finished.Name = "finished"
	finished.Status.Phase = corev1.PodSucceeded
	client := fake.NewSimpleClientset(node, &other, owned, finished)
	source, err := NewKubernetesCapacitySource(client, node.Name, "github-runners", "controller-a")
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := source.ObserveCapacity(context.Background(), "github-runners")
	if err != nil {
		t.Fatal(err)
	}
	// CPU: 500m request (NOT the 2000m limit) + 250m init + 50m pod-level
	// + 100m overhead. Memory keeps the larger of request and limit
	// throughout: 3Gi + 512Mi + 64Mi + 128Mi.
	if snapshot.CPUMilliTotal != 16000 || snapshot.CPUMilliOtherWorkloads != 900 ||
		snapshot.MemoryBytesTotal != 64<<30 || snapshot.MemoryBytesOtherWorkloads != (3<<30)+(512<<20)+(64<<20)+(128<<20) {
		t.Fatalf("unexpected Kubernetes capacity: %+v", snapshot)
	}
}

// --- the measured node -----------------------------------------------------
//
// The live failure: `Kubernetes capacity observation is incomplete: Kubernetes
// non-pool workloads exceed node allocatable capacity`, every cycle. Measured
// on MS-A2 over 31 non-terminal pods: allocatable 32000m / 90.0Gi, sum of
// requests 10835m / 16.2Gi, sum of max(request, limit) 77035m / 77.6Gi. The 13
// legacy CronJob runner pods each carry a 4500m CPU limit (one 16500m) while
// idle, so their limits alone put "other" at more than twice the node -- for as
// long as the legacy fleet coexists with the pool, which is the designed state
// for this pilot.

func capacityNode(t *testing.T, cpu, memory string) *corev1.Node {
	t.Helper()
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "homelab-ms-a2", UID: types.UID("node-uid")},
		Status: corev1.NodeStatus{Allocatable: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse(cpu), corev1.ResourceMemory: resource.MustParse(memory)}}}
}

func otherPod(name, cpuRequest, cpuLimit, memoryRequest, memoryLimit string) *corev1.Pod {
	requests := corev1.ResourceList{}
	limits := corev1.ResourceList{}
	if cpuRequest != "" {
		requests[corev1.ResourceCPU] = resource.MustParse(cpuRequest)
	}
	if cpuLimit != "" {
		limits[corev1.ResourceCPU] = resource.MustParse(cpuLimit)
	}
	if memoryRequest != "" {
		requests[corev1.ResourceMemory] = resource.MustParse(memoryRequest)
	}
	if memoryLimit != "" {
		limits[corev1.ResourceMemory] = resource.MustParse(memoryLimit)
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: corev1.PodSpec{NodeName: "homelab-ms-a2",
			Containers: []corev1.Container{{Resources: corev1.ResourceRequirements{Requests: requests, Limits: limits}}}},
	}
}

func observeOther(t *testing.T, node *corev1.Node, pods ...*corev1.Pod) (CapacitySnapshot, error) {
	t.Helper()
	objects := []runtime.Object{node}
	for _, pod := range pods {
		objects = append(objects, pod)
	}
	source, err := NewKubernetesCapacitySource(fake.NewSimpleClientset(objects...), node.Name, "github-runners", "controller-a")
	if err != nil {
		t.Fatal(err)
	}
	return source.ObserveCapacity(context.Background(), "github-runners")
}

func TestALegacyRunnerPodIsChargedItsRequestNotItsIdleLimit(t *testing.T) {
	// The exact shape of the 13 legacy pods.
	snapshot, err := observeOther(t, capacityNode(t, "32", "90Gi"),
		otherPod("legacy-runner", "100m", "4500m", "1Gi", "2Gi"))
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.CPUMilliOtherWorkloads != 100 {
		t.Fatalf("CPU charged %dm, want the 100m request rather than the 4500m idle limit", snapshot.CPUMilliOtherWorkloads)
	}
	// Memory is NOT compressible, so it keeps the worst case.
	if snapshot.MemoryBytesOtherWorkloads != 2<<30 {
		t.Fatalf("memory charged %d, want the 2Gi limit", snapshot.MemoryBytesOtherWorkloads)
	}
}

func TestACPULimitWithNoRequestIsChargedAsTheLimit(t *testing.T) {
	// The API server defaults requests to limits when a container sets a limit
	// and no request, so Requests is normally already right. This pins the
	// fallback for any shape that reaches the code with the request unset --
	// the one direction that must not silently under-count. Both spellings are
	// asserted: the stored shape the API server produces, and the bare one.
	stored := observeOrFail(t, capacityNode(t, "32", "90Gi"),
		otherPod("stored", "1500m", "1500m", "1Gi", "1Gi"))
	if stored.CPUMilliOtherWorkloads != 1500 {
		t.Fatalf("stored shape charged %dm, want 1500m", stored.CPUMilliOtherWorkloads)
	}
	bare := observeOrFail(t, capacityNode(t, "32", "90Gi"),
		otherPod("bare", "", "1500m", "", "1Gi"))
	if bare.CPUMilliOtherWorkloads != 1500 {
		t.Fatalf("a limit with no request charged %dm, want the 1500m limit rather than zero", bare.CPUMilliOtherWorkloads)
	}
	if bare.MemoryBytesOtherWorkloads != 1<<30 {
		t.Fatalf("a memory limit with no request charged %d, want 1Gi", bare.MemoryBytesOtherWorkloads)
	}
}

func observeOrFail(t *testing.T, node *corev1.Node, pods ...*corev1.Pod) CapacitySnapshot {
	t.Helper()
	snapshot, err := observeOther(t, node, pods...)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func TestTheMeasuredNodeIsComplete(t *testing.T) {
	// The regression that matters, modelled on the real numbers: requests
	// 10835m / 16.2Gi against 32000m / 90Gi allocatable, with limits summing to
	// 77035m. Under max(request, limit) this refuses; under the request rule it
	// observes.
	node := capacityNode(t, "32", "90Gi")
	// Sixteen pods standing in for the node's 31, chosen so BOTH measured sums
	// land exactly: requests 10835m and limits 77035m. The shape that makes
	// that possible is worth naming, because it is the real node's shape --
	// several workloads carry a CPU REQUEST AND NO LIMIT, so they add to the
	// request total and nothing to the limit total. Memory is request=limit
	// throughout and sums to 16Gi, close to the measured 16.2Gi and far inside
	// the node, so only CPU is on trial here.
	var pods []*corev1.Pod
	for index := range 13 {
		pods = append(pods, otherPod(fmt.Sprintf("legacy-%d", index), "100m", "4500m", "1Gi", "1Gi"))
	}
	pods = append(pods,
		otherPod("legacy-big", "100m", "16500m", "1Gi", "1Gi"),
		otherPod("limited", "2035m", "2035m", "1Gi", "1Gi"),
		// Request, no limit: counted in full by the new rule and not at all by
		// the old one.
		otherPod("request-only", "7400m", "", "1Gi", "1Gi"))

	var requestTotal, limitTotal int64
	for _, pod := range pods {
		resources := pod.Spec.Containers[0].Resources
		requestTotal += resources.Requests.Cpu().MilliValue()
		limitTotal += resources.Limits.Cpu().MilliValue()
	}
	// The fixture is self-guarding: if it drifts off the measured node the
	// assertions below stop describing the failure they were written for. It
	// has already earned this once, catching arithmetic that was 3000m out.
	if requestTotal != 10835 || limitTotal != 77035 {
		t.Fatalf("fixture no longer models the measured node: requests %dm, limits %dm", requestTotal, limitTotal)
	}

	snapshot, err := observeOther(t, node, pods...)
	if err != nil {
		t.Fatalf("the measured node was refused: %v", err)
	}
	if !snapshot.Complete || snapshot.CPUMilliOtherWorkloads != 10835 {
		t.Fatalf("the measured node did not observe its own requests: %+v", snapshot)
	}
}

func TestCPURequestsAloneStillRefuseWhenTheyExceedTheNode(t *testing.T) {
	// The refusal itself is untouched. Charging requests is not the same as
	// not charging: a node whose REQUESTS overcommit it is genuinely full, and
	// admitting a worker there is the overcommit this gate exists to stop.
	snapshot, err := observeOther(t, capacityNode(t, "2", "90Gi"),
		otherPod("greedy", "3000m", "3000m", "1Gi", "1Gi"))
	if err == nil || snapshot.Complete {
		t.Fatal("CPU requests exceeding allocatable were accepted")
	}
	if !strings.Contains(err.Error(), "exceed node allocatable") {
		t.Fatalf("refusal changed shape: %v", err)
	}
}

func TestMemoryLimitsStillRefuseWhenTheyExceedTheNode(t *testing.T) {
	// The other half of the asymmetry, asserted rather than assumed. Memory is
	// not compressible, so a limit beyond the node is a refusal even though the
	// request fits -- exactly the case the CPU rule now permits.
	snapshot, err := observeOther(t, capacityNode(t, "32", "8Gi"),
		otherPod("hungry", "100m", "200m", "1Gi", "16Gi"))
	if err == nil || snapshot.Complete {
		t.Fatal("a memory limit beyond the node was accepted")
	}
	if !strings.Contains(err.Error(), "exceed node allocatable") {
		t.Fatalf("refusal changed shape: %v", err)
	}
}

package kubernetes

import (
	"context"
	"errors"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

// KubernetesCapacitySource reads allocatable CPU/RAM and counts non-pool Pods
// on the selected node. Other workloads are charged their CPU REQUEST and their
// max(request, limit) MEMORY -- see conservativeResources for why those differ
// and what it measured. All init containers are summed, intentionally
// overestimating unusual Pod shapes instead of depending on
// scheduler-version-specific sidecar rules.
type KubernetesCapacitySource struct {
	client    kubernetes.Interface
	nodeName  string
	namespace string
	ownerUID  string
}

var _ CapacitySource = (*KubernetesCapacitySource)(nil)

func NewKubernetesCapacitySource(client kubernetes.Interface, nodeName, namespace, ownerUID string) (*KubernetesCapacitySource, error) {
	if client == nil || nodeName == "" || namespace == "" || ownerUID == "" {
		return nil, errors.New("Kubernetes capacity source configuration is incomplete")
	}
	return &KubernetesCapacitySource{client: client, nodeName: nodeName, namespace: namespace, ownerUID: ownerUID}, nil
}

func (s *KubernetesCapacitySource) ObserveCapacity(ctx context.Context, namespace string) (CapacitySnapshot, error) {
	if s == nil || ctx == nil || namespace != s.namespace {
		return CapacitySnapshot{Complete: false}, errors.New("Kubernetes capacity scope mismatch")
	}
	node, err := s.client.CoreV1().Nodes().Get(ctx, s.nodeName, metav1.GetOptions{})
	if err != nil || node == nil || node.UID == types.UID("") {
		return CapacitySnapshot{Complete: false}, errors.New("Kubernetes node capacity observation failed")
	}
	cpu := node.Status.Allocatable.Cpu().MilliValue()
	memory := node.Status.Allocatable.Memory().Value()
	if cpu <= 0 || memory <= 0 {
		return CapacitySnapshot{Complete: false}, errors.New("Kubernetes node allocatable CPU or memory is empty")
	}
	pods, err := s.listNodePods(ctx)
	if err != nil {
		return CapacitySnapshot{Complete: false}, err
	}
	var other Footprint
	for _, pod := range pods {
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed || s.isOwnedWorker(pod) {
			continue
		}
		other = other.add(conservativePodFootprint(pod))
	}
	if other.CPUMilli > cpu || other.MemoryBytes > memory {
		return CapacitySnapshot{Complete: false}, errors.New("Kubernetes non-pool workloads exceed node allocatable capacity")
	}
	return CapacitySnapshot{Complete: true, CPUMilliTotal: cpu, CPUMilliOtherWorkloads: other.CPUMilli,
		MemoryBytesTotal: memory, MemoryBytesOtherWorkloads: other.MemoryBytes}, nil
}

func (s *KubernetesCapacitySource) listNodePods(ctx context.Context) ([]corev1.Pod, error) {
	options := metav1.ListOptions{FieldSelector: fields.OneTermEqualSelector("spec.nodeName", s.nodeName).String(), Limit: listPageSize}
	var pods []corev1.Pod
	for {
		page, err := s.client.CoreV1().Pods("").List(ctx, options)
		if err != nil || page == nil {
			return nil, errors.New("Kubernetes node Pod listing is incomplete")
		}
		pods = append(pods, page.Items...)
		if page.Continue == "" {
			return pods, nil
		}
		options.Continue = page.Continue
	}
}

func (s *KubernetesCapacitySource) isOwnedWorker(pod corev1.Pod) bool {
	return pod.Namespace == s.namespace && pod.Labels[LabelManagedBy] == ManagedByValue &&
		pod.Labels[LabelPlatform] == PlatformLinux && pod.Labels[LabelOwnerUID] == s.ownerUID
}

func conservativePodFootprint(pod corev1.Pod) Footprint {
	var total Footprint
	for _, container := range pod.Spec.Containers {
		total = total.add(conservativeResources(container.Resources))
	}
	for _, container := range pod.Spec.InitContainers {
		total = total.add(conservativeResources(container.Resources))
	}
	if pod.Spec.Resources != nil {
		total = total.add(conservativeResources(*pod.Spec.Resources))
	}
	if pod.Spec.Overhead != nil {
		total.CPUMilli += pod.Spec.Overhead.Cpu().MilliValue()
		total.MemoryBytes += pod.Spec.Overhead.Memory().Value()
	}
	return total
}

// conservativeResources charges an OTHER workload's footprint. It is never
// used for the pool's own workers -- ObserveCapacity skips those -- so it
// answers one question: how much of this node is already spoken for.
//
// CPU AND MEMORY ARE CHARGED DIFFERENTLY, and the asymmetry is the whole point.
//
// CPU is COMPRESSIBLE. The scheduler reserves the REQUEST; a container using
// more than its limit is throttled, never killed, and a container below its
// limit is not occupying the difference. Charging the limit therefore charges
// capacity nobody holds.
//
// MEMORY is NOT. There is no throttling: a node that hands out more memory than
// it has kills containers. So memory keeps max(request, limit) -- the worst
// case a workload may legitimately reach.
//
// MEASURED ON THE NODE, 31 non-terminal pods (2026-09-21):
//
//	allocatable            32000m / 90.0Gi
//	sum of requests        10835m / 16.2Gi
//	sum of max(req, limit) 77035m / 77.6Gi
//
// Thirteen legacy CronJob runner pods each carry a 4500m CPU limit (one
// 16500m) while sitting idle. Under the old max() rule their limits alone put
// "other workloads" at 77035m against a 32000m node, so the refusal below fired
// on EVERY cycle -- permanently, while the legacy fleet coexists with the pool,
// which is the designed state for this pilot. Memory was never the problem:
// 77.6Gi of 90.0Gi still fits, and it keeps its conservative reading.
//
// A ZERO CPU REQUEST FALLS BACK TO THE LIMIT. The API server defaults requests
// to limits when a container sets a limit and no request, so `Requests` is
// normally already the right number to read -- but reading it unconditionally
// would charge zero for any shape that reaches this code with the request
// unset, which is the one direction that must not silently under-count.
//
// INIT CONTAINERS ARE SUMMED WITH THE APP CONTAINERS, which is NOT Kubernetes'
// own arithmetic (it takes max(init) alongside sum(app), because init
// containers run and exit before the app ones start). The simple sum is kept
// deliberately: it overestimates unusual Pod shapes rather than depending on
// scheduler-version-specific sidecar rules, and overestimating what others hold
// is the safe direction for a number that gates admission.
func conservativeResources(resources corev1.ResourceRequirements) Footprint {
	cpu := resources.Requests.Cpu().MilliValue()
	if cpu == 0 {
		cpu = resources.Limits.Cpu().MilliValue()
	}
	memory := resources.Requests.Memory().Value()
	if limit := resources.Limits.Memory().Value(); limit > memory {
		memory = limit
	}
	return Footprint{CPUMilli: cpu, MemoryBytes: memory}
}

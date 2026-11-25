package kubernetes

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"

	"github.com/cameronsjo/slipway/internal/admission"
)

// THE AGREEMENT TEST (homelab#983). The controller builds the worker Pod in Go;
// admission.yaml admits it by an exact allowlist written in CEL. Both pin the
// dockerd argv and the volume set, and until this test nothing compared them:
// admission-cases/accept-worker.yaml copies its dockerd args OUT of the policy,
// so it is admitted whatever the policy says and can never go red.
//
// This test shares no list with the policy. It builds the Pod through the real
// podSpec -> toPod path from the committed controller-config.json and the
// controller's own Deployment args, marshals it the way client-go sends it,
// and asks the offline evaluator for a verdict. Then it reads the two literal
// lists out of the policy and compares each to what the Go code builds, so a
// failure names the list that moved instead of only saying "denied".

const exactKataWorkerMessage = "sjo-runner-pool-exact-kata-worker-required"

func poolBundlePath(parts ...string) string {
	return filepath.Join(append([]string{"..", "..", "admission", "testdata", "bundle"}, parts...)...)
}

// bundleWorkerConfig is the Config the deployed controller runs with: the
// committed controller-config.json, plus -bootstrap-url and -capacity from the
// controller container's args in deployment.yaml.
func bundleWorkerConfig(t *testing.T) Config {
	t.Helper()
	raw, err := os.ReadFile(poolBundlePath("controller-config.json"))
	if err != nil {
		t.Fatal(err)
	}
	type footprint struct {
		CPUMilli     int64 `json:"cpu_milli"`
		MemoryBytes  int64 `json:"memory_bytes"`
		StorageBytes int64 `json:"storage_bytes"`
	}
	type resources struct {
		Requests footprint `json:"requests"`
		Limits   footprint `json:"limits"`
	}
	var file struct {
		Namespace            string    `json:"namespace"`
		OwnerUID             string    `json:"owner_uid"`
		NodeName             string    `json:"node_name"`
		RuntimeClassName     string    `json:"runtime_class_name"`
		RunnerImage          string    `json:"runner_image"`
		DockerImage          string    `json:"docker_image"`
		WorkerCommand        string    `json:"worker_command"`
		RunnerRoot           string    `json:"runner_root"`
		CAFile               string    `json:"ca_file"`
		RunnerResources      resources `json:"runner_resources"`
		DockerResources      resources `json:"docker_resources"`
		RuntimeOverhead      footprint `json:"runtime_overhead"`
		ContainerRootfsBytes int64     `json:"container_rootfs_bytes"`
		Volumes              struct {
			WorkBytes   int64 `json:"work_bytes"`
			SocketBytes int64 `json:"socket_bytes"`
			TmpBytes    int64 `json:"tmp_bytes"`
		} `json:"volumes"`
	}
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatal(err)
	}
	convert := func(value footprint) Footprint {
		return Footprint{CPUMilli: value.CPUMilli, MemoryBytes: value.MemoryBytes, StorageBytes: value.StorageBytes}
	}
	config := Config{
		Namespace: file.Namespace, OwnerUID: file.OwnerUID, NodeName: file.NodeName,
		RuntimeClassName: file.RuntimeClassName, RunnerImage: file.RunnerImage, DockerImage: file.DockerImage,
		WorkerCommand: file.WorkerCommand, RunnerRoot: file.RunnerRoot, CAFile: file.CAFile,
		RunnerResources:      ContainerResources{Requests: convert(file.RunnerResources.Requests), Limits: convert(file.RunnerResources.Limits)},
		DockerResources:      ContainerResources{Requests: convert(file.DockerResources.Requests), Limits: convert(file.DockerResources.Limits)},
		Overhead:             convert(file.RuntimeOverhead),
		Volumes:              VolumeBudget{Work: file.Volumes.WorkBytes, Socket: file.Volumes.SocketBytes, Tmp: file.Volumes.TmpBytes},
		ContainerRootfsBytes: file.ContainerRootfsBytes,
	}

	rawDeployment, err := os.ReadFile(poolBundlePath("deployment.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var deployment struct {
		Spec struct {
			Template struct {
				Spec struct {
					Containers []struct {
						Name string   `json:"name"`
						Args []string `json:"args"`
					} `json:"containers"`
				} `json:"spec"`
			} `json:"template"`
		} `json:"spec"`
	}
	if err := yaml.Unmarshal(rawDeployment, &deployment); err != nil {
		t.Fatal(err)
	}
	for _, container := range deployment.Spec.Template.Spec.Containers {
		if container.Name != "controller" {
			continue
		}
		for _, arg := range container.Args {
			if value, ok := strings.CutPrefix(arg, "-bootstrap-url="); ok {
				config.BootstrapURL = value
			}
			if value, ok := strings.CutPrefix(arg, "-capacity="); ok {
				if config.Capacity, err = strconv.Atoi(value); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	if config.BootstrapURL == "" {
		t.Fatal("deployment.yaml's controller carries no -bootstrap-url; the fixture cannot be built")
	}
	if err := config.validate(); err != nil {
		t.Fatalf("the committed controller config does not validate: %v", err)
	}
	return config
}

func policyEvaluator(t *testing.T) *admission.Evaluator {
	t.Helper()
	policy, err := admission.Load(poolBundlePath("admission.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := admission.NewEvaluator(policy)
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

func builtWorkerPod(t *testing.T, config Config) map[string]any {
	t.Helper()
	intent, _ := testIntent(config)
	pod := (&Transport{worker: config}).toPod((&Backend{config: config}).podSpec(intent))
	pod.APIVersion, pod.Kind = "v1", "Pod"
	raw, err := json.Marshal(pod)
	if err != nil {
		t.Fatal(err)
	}
	object, err := admission.DecodeObject(raw)
	if err != nil {
		t.Fatal(err)
	}
	return object
}

func TestTheWorkerPodTheControllerBuildsIsAdmittedByThePolicy(t *testing.T) {
	result, err := policyEvaluator(t).Evaluate(builtWorkerPod(t, bundleWorkerConfig(t)))
	if err != nil {
		t.Fatalf("evaluating the built worker Pod: %v", err)
	}
	if !result.Matched {
		t.Fatal("the policy did not select the built worker Pod; it would be judged by nothing")
	}
	if !result.Admitted() {
		t.Fatalf("the worker Pod the controller builds is denied by %v; every worker would fail admission", result.Denied)
	}
}

// policyList reads the one literal list `<prefix>[...]` out of the exact-worker
// rule. Exactly one occurrence, or the test cannot say which one the policy
// means.
func policyList(t *testing.T, expression, prefix string) []string {
	t.Helper()
	matches := regexp.MustCompile(regexp.QuoteMeta(prefix)+`\[([^\]]*)\]`).FindAllStringSubmatch(expression, -1)
	if len(matches) != 1 {
		t.Fatalf("the exact-worker rule carries %d `%s[...]` lists, expected exactly one", len(matches), prefix)
	}
	var values []string
	for _, item := range regexp.MustCompile(`'([^']*)'`).FindAllStringSubmatch(matches[0][1], -1) {
		values = append(values, item[1])
	}
	if len(values) == 0 {
		t.Fatalf("the `%s[...]` list in the exact-worker rule is empty", prefix)
	}
	return values
}

func exactWorkerExpression(t *testing.T) string {
	t.Helper()
	for _, validation := range policyEvaluator(t).Policy().Validations {
		if validation.Message == exactKataWorkerMessage {
			return validation.Expression
		}
	}
	t.Fatalf("admission.yaml has no %s validation", exactKataWorkerMessage)
	return ""
}

func TestThePolicyPinsTheDockerdArgvTheControllerBuilds(t *testing.T) {
	policy := policyList(t, exactWorkerExpression(t), "c.args == ")
	if !slices.Equal(policy, dockerdArgs()) {
		t.Fatalf("admission.yaml pins dockerd args %q; dockerdArgs() builds %q", policy, dockerdArgs())
	}
}

func TestThePolicyPinsTheVolumeSetTheControllerBuilds(t *testing.T) {
	expression := exactWorkerExpression(t)
	policy := policyList(t, expression, "v.name in ")
	intent, _ := testIntent(testConfig())
	var built []string
	for _, volume := range (&Backend{config: testConfig()}).podSpec(intent).Volumes {
		built = append(built, volume.Name)
	}
	slices.Sort(policy)
	slices.Sort(built)
	if !slices.Equal(policy, built) {
		t.Fatalf("admission.yaml allows volumes %q; podSpec builds %q", policy, built)
	}
	sizes := regexp.MustCompile(`size\(object\.spec\.volumes\) == (\d+)`).FindAllStringSubmatch(expression, -1)
	if len(sizes) != 1 || sizes[0][1] != strconv.Itoa(len(built)) {
		t.Fatalf("admission.yaml pins the volume count as %v; podSpec builds %d volumes", sizes, len(built))
	}
}

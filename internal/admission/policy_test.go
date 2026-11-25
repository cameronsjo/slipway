package admission_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/cameronsjo/slipway/internal/admission"
)

// The policy, the two real Pods and the five committed admission cases are all
// read from the bundle. Nothing in this file restates an expression, an image
// digest or a Pod shape: a copy is the drift these tests exist to catch.
func bundlePath(parts ...string) string {
	return filepath.Join(append([]string{"testdata", "bundle"}, parts...)...)
}

const (
	knownPodShape       = "sjo-runner-pool-known-pod-shape-required"
	noHostNamespaces    = "sjo-runner-pool-no-host-namespaces"
	noRuntimeOverrides  = "sjo-runner-pool-no-runtime-network-or-profile-overrides"
	noDebugNamespace    = "sjo-runner-pool-no-debug-or-shared-process-namespace"
	noIndirectWorkerEnv = "sjo-runner-pool-no-indirect-worker-environment"
	exactKataWorker     = "sjo-runner-pool-exact-kata-worker-required"
	exactControllerPod  = "sjo-runner-pool-exact-controller-witness-required"
)

func evaluator(t *testing.T) *admission.Evaluator {
	t.Helper()
	policy, err := admission.Load(bundlePath("admission.yaml"))
	if err != nil {
		t.Fatalf("loading the policy: %v", err)
	}
	compiled, err := admission.NewEvaluator(policy)
	if err != nil {
		t.Fatalf("compiling the policy: %v", err)
	}
	return compiled
}

func readObject(t *testing.T, path string) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	object, err := admission.DecodeObject(raw)
	if err != nil {
		t.Fatalf("decoding %s: %v", path, err)
	}
	return object
}

// admissionCase returns one of the committed cases together with the verdict
// its header line declares, so the Go evaluation and the header the Python
// suite and the kubectl recipe both read cannot drift apart.
func admissionCase(t *testing.T, name string) (map[string]any, string) {
	t.Helper()
	path := bundlePath("admission-cases", name)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	header := strings.Fields(strings.SplitN(string(raw), "\n", 2)[0])
	if len(header) == 0 {
		t.Fatalf("%s has no expected-verdict header", name)
	}
	verdict := header[len(header)-1]
	if verdict != "accept" && verdict != "deny" {
		t.Fatalf("%s declares verdict %q", name, verdict)
	}
	object, err := admission.DecodeObject(raw)
	if err != nil {
		t.Fatalf("decoding %s: %v", name, err)
	}
	return object, verdict
}

// controllerPod is the Pod the bundle's own Deployment creates: its template,
// given the name and namespace the API server would supply.
func controllerPod(t *testing.T) map[string]any {
	t.Helper()
	deployment := readObject(t, bundlePath("deployment.yaml"))
	template := child(t, child(t, deployment, "spec"), "template")
	metadata := child(t, template, "metadata")
	metadata["name"] = "runner-pool-controller-0"
	metadata["namespace"] = "github-runner-pool"
	return map[string]any{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata":   metadata,
		"spec":       template["spec"],
	}
}

func clone(t *testing.T, object map[string]any) map[string]any {
	t.Helper()
	encoded, err := json.Marshal(object)
	if err != nil {
		t.Fatalf("cloning: %v", err)
	}
	copied, err := admission.DecodeObject(encoded)
	if err != nil {
		t.Fatalf("cloning: %v", err)
	}
	return copied
}

func child(t *testing.T, parent map[string]any, key string) map[string]any {
	t.Helper()
	value, ok := parent[key].(map[string]any)
	if !ok {
		t.Fatalf("%s is not an object", key)
	}
	return value
}

func items(t *testing.T, parent map[string]any, key string) []any {
	t.Helper()
	value, ok := parent[key].([]any)
	if !ok {
		t.Fatalf("%s is not a list", key)
	}
	return value
}

func containerNamed(t *testing.T, pod map[string]any, name string) map[string]any {
	t.Helper()
	for _, entry := range items(t, child(t, pod, "spec"), "containers") {
		container, ok := entry.(map[string]any)
		if ok && container["name"] == name {
			return container
		}
	}
	t.Fatalf("no container named %s", name)
	return nil
}

func judge(t *testing.T, compiled *admission.Evaluator, pod map[string]any) admission.Result {
	t.Helper()
	result, err := compiled.Evaluate(pod)
	if err != nil {
		t.Fatalf("evaluating: %v", err)
	}
	if !result.Matched {
		t.Fatalf("the policy's matchConditions did not select this Pod")
	}
	return result
}

// An expression that does not compile is a policy that fails open or closed for
// the wrong reason, and nothing before this package would have noticed.
func TestEveryPolicyExpressionCompiles(t *testing.T) {
	compiled := evaluator(t)
	policy := compiled.Policy()
	if len(policy.Validations) == 0 || len(policy.Variables) == 0 || len(policy.MatchConditions) == 0 {
		t.Fatalf("the policy parsed as %d validations, %d variables, %d matchConditions",
			len(policy.Validations), len(policy.Variables), len(policy.MatchConditions))
	}
	if policy.FailurePolicy != "Fail" {
		t.Fatalf("failurePolicy is %q; these tests model Fail", policy.FailurePolicy)
	}
}

// The case the earlier Critical broke. The worker environment rule denied every
// legitimate worker, and it survived review because nothing evaluated it.
func TestTheRealControllerAndWorkerPodsAreAdmitted(t *testing.T) {
	compiled := evaluator(t)
	worker, verdict := admissionCase(t, "accept-worker.yaml")
	if verdict != "accept" {
		t.Fatalf("accept-worker.yaml declares %q", verdict)
	}
	for name, pod := range map[string]map[string]any{
		"the worker the controller creates": worker,
		"the controller the bundle deploys": controllerPod(t),
	} {
		result := judge(t, compiled, pod)
		if !result.Admitted() {
			t.Errorf("%s is denied by %v", name, result.Denied)
		}
	}
}

// fieldRef is required, not tolerated: transport.go validateObservedRunner
// refuses to attach the bootstrap capability to a Pod without
// SLIPWAY_BOOTSTRAP_EXPECTED_NAME/_UID. Every other indirection reaches the App key
// Secret in this namespace, or has no use on a worker.
func TestWorkerFieldRefIsAdmittedAndEveryOtherIndirectionIsDenied(t *testing.T) {
	compiled := evaluator(t)
	worker, _ := admissionCase(t, "accept-worker.yaml")
	runner := containerNamed(t, worker, "runner")
	fieldRefs := 0
	for _, entry := range items(t, runner, "env") {
		variable, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		source, ok := variable["valueFrom"].(map[string]any)
		if ok {
			if _, present := source["fieldRef"]; present {
				fieldRefs++
			}
		}
	}
	if fieldRefs != 2 {
		t.Fatalf("the accept case carries %d downward-API env vars, expected 2", fieldRefs)
	}
	if result := judge(t, compiled, worker); !result.Admitted() {
		t.Fatalf("the two downward-API env vars are denied by %v", result.Denied)
	}
	for _, name := range []string{
		"deny-worker-envfrom.yaml",
		"deny-worker-env-secretkeyref.yaml",
		"deny-worker-env-configmapkeyref.yaml",
		"deny-worker-env-resourcefieldref.yaml",
	} {
		pod, verdict := admissionCase(t, name)
		result := judge(t, compiled, pod)
		if verdict != "deny" {
			t.Errorf("%s declares %q", name, verdict)
		}
		if !reflectEqual(result.Denied, []string{noIndirectWorkerEnv}) {
			t.Errorf("%s is denied by %v, expected exactly %s", name, result.Denied, noIndirectWorkerEnv)
		}
	}
}

// negativeCase is a minimal mutation of a positive fixture that one named
// validation, and only that one, must deny. Asserting the exact denial set is
// what stops a fixture from passing for the wrong reason.
type negativeCase struct {
	name     string
	expected string
	build    func(t *testing.T) map[string]any
}

func negativeCases() []negativeCase {
	return []negativeCase{
		{
			// Neither label: the fail-closed case. A Pod the pool did not
			// create must not be admitted just because it breaks no other rule.
			name:     "a Pod carrying neither the controller nor the worker label",
			expected: knownPodShape,
			build: func(t *testing.T) map[string]any {
				pod := acceptWorker(t)
				delete(child(t, pod, "metadata"), "labels")
				return pod
			},
		},
		{
			name:     "a worker sharing the host network namespace",
			expected: noHostNamespaces,
			build: func(t *testing.T) map[string]any {
				pod := acceptWorker(t)
				child(t, pod, "spec")["hostNetwork"] = true
				return pod
			},
		},
		{
			name:     "a worker overriding the Kata runtime by annotation",
			expected: noRuntimeOverrides,
			build: func(t *testing.T) map[string]any {
				pod := acceptWorker(t)
				child(t, pod, "metadata")["annotations"] = map[string]any{
					"io.katacontainers.config.hypervisor.default_vcpus": "8",
				}
				return pod
			},
		},
		{
			name:     "a worker sharing its process namespace",
			expected: noDebugNamespace,
			build: func(t *testing.T) map[string]any {
				pod := acceptWorker(t)
				child(t, pod, "spec")["shareProcessNamespace"] = true
				return pod
			},
		},
		{
			name:     "a worker reaching a Secret through env[].valueFrom",
			expected: noIndirectWorkerEnv,
			build: func(t *testing.T) map[string]any {
				pod, _ := admissionCase(t, "deny-worker-env-secretkeyref.yaml")
				return pod
			},
		},
		{
			name:     "a worker escaping the Kata runtime class",
			expected: exactKataWorker,
			build: func(t *testing.T) map[string]any {
				pod := acceptWorker(t)
				child(t, pod, "spec")["runtimeClassName"] = "runc"
				return pod
			},
		},
		// homelab#983. The retired four-volume worker: a RAM `docker` volume at
		// /var/lib/docker and the data root left there. The policy is an exact
		// allowlist, not a union of the old and new shapes, so the old shape
		// is denied outright.
		{
			name:     "the retired four-volume worker shape",
			expected: exactKataWorker,
			build: func(t *testing.T) map[string]any {
				pod, verdict := admissionCase(t, "deny-worker-old-docker-volume.yaml")
				if verdict != "deny" {
					t.Fatalf("deny-worker-old-docker-volume.yaml declares %q", verdict)
				}
				return pod
			},
		},
		// The new args with a fourth writable volume beside the three: the
		// shape a revert of only the volume set would produce.
		{
			name:     "a worker with a fourth writable volume",
			expected: exactKataWorker,
			build: func(t *testing.T) map[string]any {
				pod, verdict := admissionCase(t, "deny-worker-fourth-writable-volume.yaml")
				if verdict != "deny" {
					t.Fatalf("deny-worker-fourth-writable-volume.yaml declares %q", verdict)
				}
				return pod
			},
		},
		{
			name:     "a controller container asking to be privileged",
			expected: exactControllerPod,
			build: func(t *testing.T) map[string]any {
				pod := clone(t, controllerPod(t))
				child(t, containerNamed(t, pod, "controller"), "securityContext")["privileged"] = true
				return pod
			},
		},
		// The controller arm pinned the image and left the COMMAND open, while
		// the worker arm pinned both from the start. An ENTRYPOINT is not a pin:
		// `command` overrides it, so a Pod carrying the controller's labels and
		// its reviewed digests could be given any command -- with the witness's
		// host-root mount and the controller's ServiceAccount beside it.
		{
			name:     "a controller Pod given a command other than its binary",
			expected: exactControllerPod,
			build: func(t *testing.T) map[string]any {
				pod := clone(t, controllerPod(t))
				containerNamed(t, pod, "controller")["command"] = []any{"/bin/sh", "-c", "sleep infinity"}
				return pod
			},
		},
		{
			name:     "a host witness given a command other than its binary",
			expected: exactControllerPod,
			build: func(t *testing.T) map[string]any {
				pod := clone(t, controllerPod(t))
				containerNamed(t, pod, "host-witness")["command"] = []any{"/usr/local/bin/sjo-host-ctr"}
				return pod
			},
		},
		{
			name:     "a controller Pod with no command at all",
			expected: exactControllerPod,
			build: func(t *testing.T) map[string]any {
				pod := clone(t, controllerPod(t))
				delete(containerNamed(t, pod, "controller"), "command")
				return pod
			},
		},
		{
			name:     "a controller Pod asking for interactive stdio",
			expected: exactControllerPod,
			build: func(t *testing.T) map[string]any {
				pod := clone(t, controllerPod(t))
				container := containerNamed(t, pod, "controller")
				container["stdin"], container["tty"] = true, true
				return pod
			},
		},
		{
			name:     "a host witness whose host root is writable",
			expected: exactControllerPod,
			build: func(t *testing.T) map[string]any {
				pod := clone(t, controllerPod(t))
				for _, mount := range containerNamed(t, pod, "host-witness")["volumeMounts"].([]any) {
					entry := mount.(map[string]any)
					if entry["name"] == "host-root" {
						entry["readOnly"] = false
					}
				}
				return pod
			},
		},
	}
}

func acceptWorker(t *testing.T) map[string]any {
	t.Helper()
	pod, _ := admissionCase(t, "accept-worker.yaml")
	return clone(t, pod)
}

func TestEachNegativeFixtureIsDeniedByItsRuleAndNoOther(t *testing.T) {
	compiled := evaluator(t)
	for _, testCase := range negativeCases() {
		t.Run(testCase.name, func(t *testing.T) {
			result := judge(t, compiled, testCase.build(t))
			if !reflectEqual(result.Denied, []string{testCase.expected}) {
				t.Fatalf("denied by %v, expected exactly %s", result.Denied, testCase.expected)
			}
		})
	}
}

// verify-manifests.py pins the set of validation messages, so a rule can be
// added without a test and the structural check stays green. This is the
// assertion that makes adding a rule fail the build until a negative fixture
// covers it.
func TestEveryValidationIsCoveredByANegativeFixture(t *testing.T) {
	compiled := evaluator(t)
	covered := map[string]bool{}
	for _, testCase := range negativeCases() {
		covered[testCase.expected] = true
	}
	var uncovered []string
	for _, validation := range compiled.Policy().Validations {
		if !covered[validation.Label()] {
			uncovered = append(uncovered, validation.Label())
		}
		delete(covered, validation.Label())
	}
	if len(uncovered) > 0 {
		sort.Strings(uncovered)
		t.Errorf("no negative fixture exercises %v", uncovered)
	}
	for label := range covered {
		t.Errorf("a fixture expects %s, which the policy no longer declares", label)
	}
}

// The binding excludes other namespaces before CEL runs; the matchCondition is
// the second layer. A Pod outside the pool is not judged at all, which is not
// the same as being admitted.
func TestThePolicySelectsOnlyThePoolNamespace(t *testing.T) {
	compiled := evaluator(t)
	pod := acceptWorker(t)
	child(t, pod, "metadata")["namespace"] = "default"
	result, err := compiled.Evaluate(pod)
	if err != nil {
		t.Fatalf("evaluating: %v", err)
	}
	if result.Matched {
		t.Fatalf("a Pod in the default namespace was selected by the policy")
	}
}

func reflectEqual(actual, expected []string) bool {
	if len(actual) != len(expected) {
		return false
	}
	left := append([]string(nil), actual...)
	right := append([]string(nil), expected...)
	sort.Strings(left)
	sort.Strings(right)
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

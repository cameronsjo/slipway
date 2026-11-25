package admission_test

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

// This package's whole claim is that its verdicts predict the API server's. The
// package doc states why: cel-go is held at the version k8s.io/apiserver pins
// for the cluster's Kubernetes minor, so the language level and standard
// library match the admission plugin's. A different cel-go is a different
// evaluator, and its agreement with the real plugin becomes a coincidence.
//
// Nothing in the build enforced that. k8s.io/apiserver is not in this module's
// graph at all -- runner-pool imports cel-go directly -- so the two versions
// could drift apart with every check still green, and the first symptom would
// be an expression that passes here and denies in the cluster (or worse, the
// reverse: the policy keeps the GitHub App private key out of a job VM).
//
// So the intent becomes a test. The pair below is one derivation, recorded
// together because neither half means anything alone.
const (
	// The Kubernetes minor the cluster runs: K3s v1.36.3+k3s1 on the MS-A2,
	// which is what k8s.io/api, apimachinery and client-go are pinned to.
	expectedKubernetes = "v0.36.3"

	// What k8s.io/apiserver@v0.36.3 requires. Re-derive rather than guess:
	//
	//   command curl -s https://proxy.golang.org/k8s.io/apiserver/@v/<ver>.mod | grep cel-go
	//
	// Verified 2026-09-16 against v0.36.3 -> github.com/google/cel-go v0.26.0.
	expectedCelGo = "v0.26.0"
)

// A bump that arrives on its own -- a dependabot PR, a stray `go get -u` -- is
// the case this catches. Raising cel-go alone silently unpins it from the API
// server; raising Kubernetes alone leaves cel-go describing a minor the cluster
// no longer runs. Either way the constants above stop matching go.mod and this
// goes red with the re-derivation recipe attached.
//
// Moving to a new Kubernetes minor legitimately fails this test too. That is
// the design: the fix is to re-derive the pair and edit both constants in one
// commit, which is a deliberate act with the evidence recorded, rather than a
// version drifting in unremarked.
func TestCelGoStaysPinnedToTheAPIServersVersion(t *testing.T) {
	// go.mod is read at run time. A copy of a version string in Go source is
	// exactly the drift this file exists to prevent.
	raw, err := os.ReadFile(filepath.Join("..", "..", "go.mod"))
	if err != nil {
		t.Fatalf("reading the module file: %v", err)
	}
	gomod := string(raw)

	for _, probe := range []struct {
		name   string
		module string
		want   string
		why    string
	}{
		{
			name:   "cel-go",
			module: "github.com/google/cel-go",
			want:   expectedCelGo,
			why: "this package evaluates the admission policy offline and its verdicts are only " +
				"predictive while cel-go matches the one k8s.io/apiserver@" + expectedKubernetes + " pins",
		},
		{
			name:   "k8s.io/api",
			module: "k8s.io/api",
			want:   expectedKubernetes,
			why: "the expected cel-go version is derived from this Kubernetes minor; if the cluster " +
				"moved, the derivation above is stale and cel-go must be re-derived with it",
		},
	} {
		t.Run(probe.name, func(t *testing.T) {
			// Anchor to a require-line: the module path, whitespace, then the
			// version. Matching the bare path would also hit a replace
			// directive or a comment and read the wrong version.
			pattern := regexp.MustCompile(`(?m)^\s*` + regexp.QuoteMeta(probe.module) + `\s+(v[0-9][^\s]*)`)
			match := pattern.FindStringSubmatch(gomod)
			if match == nil {
				t.Fatalf("%s is not required in go.mod; the derivation this test pins no longer describes the build", probe.module)
			}
			if got := match[1]; got != probe.want {
				t.Fatalf("%s is %s, expected %s.\n\n%s.\n\nRe-derive with:\n"+
					"  command curl -s https://proxy.golang.org/k8s.io/apiserver/@v/<kubernetes-version>.mod | grep cel-go\n"+
					"then update BOTH constants in this file in the same commit as the bump.",
					probe.module, got, probe.want, probe.why)
			}
		})
	}
}

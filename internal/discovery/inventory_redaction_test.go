package discovery

import (
	"strings"
	"testing"

	"github.com/cameronsjo/slipway/internal/controller"
)

// backend.diagnostic and capacity.admission_reason are the two report fields
// whose value is authored in the controller package. Every other diagnostic the
// report publishes passes through Redact or RedactStored at the point it is
// written into the body; before RedactInventory existed these two did not, and
// their safety rested on a convention held in a different package -- that every
// caller of the controller's observe passes one of six constants.
//
// That convention is true today and is not what these tests pin. They pin the
// publication point: whatever the controller hands over, only the six words can
// reach the body. A future edit in the controller passing err.Error() is then a
// readable diagnostic that says "unavailable", not an upstream error string on a
// route whose only control is a bearer token.
func TestRedactInventoryPublishesOnlyTheControllersVocabulary(t *testing.T) {
	// The vocabulary is read from the controller's own exported list rather
	// than restated here. A test carrying its own copy of the six words would
	// still pass after the controller changed one, which is the drift both this
	// test and the exported list exist to prevent.
	if len(controller.InventoryDiagnostics) == 0 {
		t.Fatal("the controller exports an empty inventory vocabulary; this test would assert nothing")
	}
	for _, word := range controller.InventoryDiagnostics {
		t.Run("passes/"+word, func(t *testing.T) {
			if got := RedactInventory(word); got != word {
				t.Fatalf("RedactInventory(%q) = %q, want it published unchanged -- "+
					"collapsing a vocabulary word costs a check the difference between "+
					"an incomplete inventory and an untracked resource", word, got)
			}
		})
	}

	for _, test := range []struct {
		name  string
		given string
	}{
		{
			// The case that motivated this: an upstream error reaching the
			// field, carrying whatever the adapter chose to say.
			name:  "an upstream error string",
			given: `Get "https://api.github.com/app/installations": dial tcp: lookup failed`,
		},
		{
			name:  "a credential-bearing broker URL",
			given: "https://broker.actions.githubusercontent.com/message/q?token=abcd",
		},
		{
			name:  "a driver error naming its engine",
			given: "sqlite: disk I/O error reading managed_sets",
		},
		{
			// An allowlist must match the whole value. A substring test would
			// publish this entire string because a vocabulary word appears in
			// it, which is the failure mode an allowlist is chosen to avoid.
			name:  "a string merely containing a vocabulary word",
			given: "journal-read-failed: /root/.config/gh/hosts.yml token=ghp_example",
		},
	} {
		t.Run("collapses/"+test.name, func(t *testing.T) {
			got := RedactInventory(test.given)
			if got != DiagnosticUnavailable {
				t.Fatalf("RedactInventory(%q) = %q, want %q", test.given, got, DiagnosticUnavailable)
			}
			// Assert on the published value, not only on the verdict: a future
			// implementation that returned a prefix or a wrapped form would
			// satisfy an equality check against the wrong constant but still
			// carry the input.
			for _, forbidden := range []string{"token=", "githubusercontent", "api.github.com", "sqlite", "ghp_"} {
				if strings.Contains(got, forbidden) {
					t.Fatalf("the collapsed value carried %q: %q", forbidden, got)
				}
			}
		})
	}

	// Absent stays absent. The report distinguishes "no diagnostic" from "a
	// diagnostic I will not print", and collapsing the empty case would make
	// every healthy backend read as degraded-but-unexplained.
	if got := RedactInventory(""); got != "" {
		t.Fatalf(`RedactInventory("") = %q, want ""`, got)
	}
}

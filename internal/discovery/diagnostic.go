package discovery

import (
	"context"
	"errors"

	"github.com/cameronsjo/slipway/internal/controller"
)

// Diagnostics published in the report are drawn from this fixed vocabulary and
// nowhere else. No upstream error text, no adapter message and no storage
// message reaches a body: the report sits behind a bearer token on a Traefik
// tier with no forward-auth, and an error string is the easiest way for a
// credential, a queue URL or a file path to leave the process by accident.
//
// This is githubapp.SafeError's rule applied to the composer: recognise the
// error, publish a word for it, and never print it.
const (
	// DiagnosticUnavailable is the catch-all. It deliberately says nothing
	// about the failure beyond the section it happened in, which the field's
	// position already says.
	DiagnosticUnavailable = "unavailable"
	// DiagnosticInventoryUnknown is the platform fence: no trusted inventory.
	DiagnosticInventoryUnknown = "inventory-unknown"
	// DiagnosticCapacity is the pool being full, which is not a fault.
	DiagnosticCapacity = "no-capacity"
	// DiagnosticNoDemand is an idle pool, which is not a fault either.
	DiagnosticNoDemand = "no-demand"
	// DiagnosticDiscoveryStale is the App snapshot fencing new work.
	DiagnosticDiscoveryStale = "discovery-stale"
	// DiagnosticCancelled and DiagnosticDeadline are shutdown and timeout.
	DiagnosticCancelled = "cancelled"
	DiagnosticDeadline  = "deadline"
)

// Redact turns any error into one word from the vocabulary above. An error it
// does not recognise becomes DiagnosticUnavailable rather than its own text,
// so adding an error type upstream cannot widen what this package publishes.
func Redact(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, controller.ErrInventoryUnknown):
		return DiagnosticInventoryUnknown
	case errors.Is(err, controller.ErrCapacity):
		return DiagnosticCapacity
	case errors.Is(err, controller.ErrNoDemand):
		return DiagnosticNoDemand
	case errors.Is(err, controller.ErrDiscoveryStale):
		return DiagnosticDiscoveryStale
	case errors.Is(err, context.Canceled):
		return DiagnosticCancelled
	case errors.Is(err, context.DeadlineExceeded):
		return DiagnosticDeadline
	default:
		return DiagnosticUnavailable
	}
}

// RedactStored maps a diagnostic string the controller itself journaled onto
// the same vocabulary. The controller's strings are its own fixed sentences,
// but they are sentences, and a body is not the place for them: a non-empty one
// becomes a single word. The distinction that matters to a check is present or
// absent, which this preserves exactly.
func RedactStored(diagnostic string) string {
	if diagnostic == "" {
		return ""
	}
	return DiagnosticUnavailable
}

// inventoryVocabulary is the controller's closed set of inventory diagnostics,
// built from the controller's own constants so the two cannot drift.
var inventoryVocabulary = func() map[string]bool {
	set := make(map[string]bool, len(controller.InventoryDiagnostics))
	for _, word := range controller.InventoryDiagnostics {
		set[word] = true
	}
	return set
}()

// RedactInventory publishes an inventory diagnostic only when it is one of the
// controller's six vocabulary words, and collapses anything else.
//
// RedactStored would also be safe here, but it is the wrong instrument: it
// answers "present or absent", and this field's six values are the whole reason
// a check can tell an incomplete inventory from an untracked resource. So this
// is an allowlist rather than a redactor -- it keeps the meaning that exists and
// refuses everything it does not recognise.
//
// Why it is needed at all, given every caller of the controller's observe passes
// one of these constants today: that is a convention held in a different
// package, and it is the only field the report publishes whose safety is not
// enforced where it is published. One future edit passing err.Error() would put
// an upstream error string on a route whose only control is a bearer token, with
// no test going red. An allowlist at the publication point cannot be undone from
// somewhere else.
func RedactInventory(diagnostic string) string {
	if diagnostic == "" {
		return ""
	}
	if inventoryVocabulary[diagnostic] {
		return diagnostic
	}
	return DiagnosticUnavailable
}

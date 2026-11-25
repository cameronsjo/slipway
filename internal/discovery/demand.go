package discovery

import "time"

// DefaultStallThreshold is the wait past which an unserved offer counts as
// stalled. It is published in the body beside the boolean it produces, so a
// Gatus check on demand.stalled is auditable against the number that produced
// it rather than against a constant a reader has to go find.
const DefaultStallThreshold = 300 * time.Second

// The four terms of the stall conjunction, in the order they are tested.
// StalledBlockedBy names the first one that was false, so a reader learns why
// the pool is not considered stalled without re-deriving it.
const (
	BlockedNoDemand        = "no-demand"
	BlockedNoFreeSlot      = "no-free-slot"
	BlockedNoHeadroom      = "no-headroom"
	BlockedWithinThreshold = "within-threshold"
)

// StallInputs are the four facts the conjunction needs, each already read from
// its own producer. They are passed in rather than fetched here so the decision
// is a pure function: this is the one field in the report that no other field
// can stand in for, and it has to be testable without a journal.
type StallInputs struct {
	// QueuedOffers is selectable queued demand across every repository.
	QueuedOffers int
	// FreeSlots is pool slots with no lease on them.
	FreeSlots int
	// TrustedHeadroom is the backend's own answer for how many more workers
	// fit. Zero means the backend is fenced or full, which is its own fault
	// reported in its own section, not a stall.
	TrustedHeadroom int
	// OldestOffer is the durable instant the oldest unacquired offer was made,
	// zero when there is none or when it predates journal version 11. A zero
	// instant is read as not yet past the threshold, so an upgrade cannot
	// manufacture a stall out of offers that predate the column.
	OldestOffer time.Time
	// Threshold is the wait past which an offer counts as stalled.
	Threshold time.Duration
}

// EvaluateStall separates the pool's two zero states, which look identical in
// every other field of the report and mean opposite things: zero workers with
// zero queued demand is the ordinary idle state of an ephemeral pool and must
// never alert, while zero workers with queued demand and free capacity is an
// outage.
//
// Stalled is true only when all four terms hold. Every elapsed quantity is
// derived from the offer's own durable instant, never from a process-local run
// length: a run length would contradict per-request composition and would reset
// a real, hours-old stall to zero on a controller restart, which is the moment
// a stall is most likely.
func EvaluateStall(now time.Time, in StallInputs) DemandHealth {
	threshold := in.Threshold
	if threshold <= 0 {
		threshold = DefaultStallThreshold
	}
	health := DemandHealth{
		Status:                Healthy,
		QueuedOffers:          in.QueuedOffers,
		OldestOfferSeconds:    -1,
		StallThresholdSeconds: int64(threshold.Seconds()),
	}
	waited := time.Duration(-1)
	if !in.OldestOffer.IsZero() {
		waited = now.UTC().Sub(in.OldestOffer.UTC())
		if waited < 0 {
			// A backwards clock does not age an offer. Report no wait rather
			// than a negative one, and let the threshold term refuse.
			waited = 0
		}
		health.OldestOfferSeconds = int64(waited.Seconds())
	}
	switch {
	case in.QueuedOffers <= 0:
		health.StalledBlockedBy = BlockedNoDemand
	case in.FreeSlots <= 0:
		health.StalledBlockedBy = BlockedNoFreeSlot
	case in.TrustedHeadroom <= 0:
		health.StalledBlockedBy = BlockedNoHeadroom
	case waited < 0 || waited <= threshold:
		health.StalledBlockedBy = BlockedWithinThreshold
	default:
		health.Stalled = true
		health.StalledSeconds = int64((waited - threshold).Seconds())
		health.Status = Degraded
	}
	return health
}

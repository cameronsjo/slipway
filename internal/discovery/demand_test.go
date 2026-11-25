package discovery

import (
	"testing"
	"time"
)

// B3: the five states the conjunction has to separate. Four of them are the
// pool working as designed and must never alert; only the fifth is an outage.
// stalled_blocked_by must name the right term in each of the four, because a
// reader who cannot tell "idle" from "full" from "fenced" is back to deriving
// it by hand from fields that read identically.
func TestStallSeparatesTheFourNotStalledStatesFromTheOutage(t *testing.T) {
	const threshold = 300 * time.Second
	now := origin
	past := now.Add(-threshold - time.Minute)
	within := now.Add(-threshold + time.Minute)

	for _, test := range []struct {
		name      string
		in        StallInputs
		stalled   bool
		blockedBy string
	}{
		{
			"idle, no demand", StallInputs{QueuedOffers: 0, FreeSlots: 2, TrustedHeadroom: 2, OldestOffer: past, Threshold: threshold},
			false, BlockedNoDemand,
		},
		{
			"demand, no free slot", StallInputs{QueuedOffers: 3, FreeSlots: 0, TrustedHeadroom: 2, OldestOffer: past, Threshold: threshold},
			false, BlockedNoFreeSlot,
		},
		{
			"demand, no trusted headroom", StallInputs{QueuedOffers: 3, FreeSlots: 2, TrustedHeadroom: 0, OldestOffer: past, Threshold: threshold},
			false, BlockedNoHeadroom,
		},
		{
			"demand, within threshold", StallInputs{QueuedOffers: 3, FreeSlots: 2, TrustedHeadroom: 2, OldestOffer: within, Threshold: threshold},
			false, BlockedWithinThreshold,
		},
		{
			"demand, past threshold", StallInputs{QueuedOffers: 3, FreeSlots: 2, TrustedHeadroom: 2, OldestOffer: past, Threshold: threshold},
			true, "",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			health := EvaluateStall(now, test.in)
			if health.Stalled != test.stalled {
				t.Errorf("stalled = %v, want %v", health.Stalled, test.stalled)
			}
			if health.StalledBlockedBy != test.blockedBy {
				t.Errorf("stalled_blocked_by = %q, want %q", health.StalledBlockedBy, test.blockedBy)
			}
			want := Healthy
			if test.stalled {
				want = Degraded
			}
			if health.Status != want {
				t.Errorf("verdict = %q, want %q", health.Status, want)
			}
			if health.StallThresholdSeconds != int64(threshold.Seconds()) {
				t.Errorf("threshold was not published: %d", health.StallThresholdSeconds)
			}
		})
	}
}

// An offer with no durable instant is a request offered before journal version
// 11. Reading it as an infinitely old wait would turn every upgrade into a page.
func TestStallTreatsAnAbsentOfferInstantAsNotYetPastTheThreshold(t *testing.T) {
	health := EvaluateStall(origin, StallInputs{QueuedOffers: 5, FreeSlots: 2, TrustedHeadroom: 2})
	if health.Stalled {
		t.Fatal("an offer with no durable instant was reported as stalled")
	}
	if health.StalledBlockedBy != BlockedWithinThreshold {
		t.Fatalf("stalled_blocked_by = %q, want %q", health.StalledBlockedBy, BlockedWithinThreshold)
	}
	if health.OldestOfferSeconds != -1 {
		t.Fatalf("oldest_offer_seconds = %d, want -1", health.OldestOfferSeconds)
	}
}

// stalled_seconds is measured from the offer's own durable instant, so it
// survives the restart that most often accompanies a real stall.
func TestStalledSecondsIsMeasuredFromTheDurableOffer(t *testing.T) {
	const threshold = 300 * time.Second
	health := EvaluateStall(origin, StallInputs{
		QueuedOffers: 1, FreeSlots: 1, TrustedHeadroom: 1,
		OldestOffer: origin.Add(-threshold - 2*time.Hour), Threshold: threshold,
	})
	if !health.Stalled {
		t.Fatal("a two-hour-old offer past the threshold was not reported as stalled")
	}
	if health.StalledSeconds != int64((2 * time.Hour).Seconds()) {
		t.Fatalf("stalled_seconds = %d, want %d", health.StalledSeconds, int64((2 * time.Hour).Seconds()))
	}
}

package discovery

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// fullyPopulatedReport sets every field of every section to a distinct non-zero
// value. Zero values would let a renamed or dropped tag round-trip unnoticed,
// which is the whole failure this pin exists to catch.
func fullyPopulatedReport() Report {
	return Report{
		Status:        Degraded,
		Reasons:       []string{"cycle", "demand"},
		Platform:      "linux",
		Generated:     "2026-09-13T12:00:00.000000001Z",
		UptimeSeconds: 3601,
		Cycle: CycleHealth{
			Status: Degraded, LastCompleted: "2026-09-13T11:50:00Z", AgeSeconds: 600,
			IntervalSeconds: 60, Stale: true, ConsecutiveFailures: 4, Diagnostic: DiagnosticUnavailable,
		},
		Discovery: DiscoveryHealth{
			Status: Degraded, LastSuccess: "2026-09-13T11:55:00Z", LastAttempt: "2026-09-13T11:59:00Z",
			AgeSeconds: 300, FreshnessSeconds: 180, Stale: true, Diagnostic: DiagnosticUnavailable,
		},
		Demand: DemandHealth{
			Status: Degraded, QueuedOffers: 5, OldestOfferSeconds: 900, StallThresholdSeconds: 300,
			Stalled: true, StalledBlockedBy: BlockedNoHeadroom, StalledSeconds: 600,
		},
		Capacity: CapacityHealth{
			Status: Degraded, Capacity: 2, LegacySlots: 1, PoolSlots: 1, Charged: 1, Free: 1,
			StuckCharged: 1, TrustedHeadroom: 1, Admission: "fenced", AdmissionReason: DiagnosticInventoryUnknown,
			ProvisioningSuppressed: true, SuppressedUntil: "2026-09-13T12:15:00Z",
		},
		Leases: PhaseCounts{
			Reserved: 1, AcquisitionIntent: 2, AcquisitionUnknown: 3, Acquired: 4,
			Provisioning: 5, Running: 6, CleanupPending: 7, Quarantined: 8,
		},
		Workers: WorkerHealth{
			Status: Degraded, Tracked: 9, Booting: 1, Running: 2, Destroying: 3,
			OverdueStartup: 4, OverdueCleanup: 5, UntrackedSeen: true, RetiredUnused: 6,
		},
		Backend: BackendHealth{
			Status: Degraded, Complete: true, ObservedAt: "2026-09-13T11:58:00Z", AgeSeconds: 120,
			FreshnessSeconds: 180, Stale: true, FreeSlots: 2, Diagnostic: DiagnosticUnavailable,
		},
		Bootstrap: BootstrapHealth{
			Status: Degraded, ListenerReady: true, Outstanding: 1, Delivered: 2, Expired: 3, Failed: 4,
		},
		PolicyDoc: PolicyHealth{
			Status: Degraded, Digest: "3b1f0c2d", LoadedAt: "2026-09-13T11:00:00Z", Degraded: true,
			DefaultEnabled: true, Entries: 12, UnknownEntries: 1, Diagnostic: DiagnosticUnavailable,
		},
		RepositoriesTotal:    3,
		RepositoriesEligible: 2,
		RepositoriesDegraded: 1,
		Repositories: []RepositoryHealth{{
			Status: Degraded, FullName: "cameronsjo/homelab", Eligible: true,
			Discovered: "2026-09-13T10:00:00Z",
			ScaleSet: ScaleSetHealth{
				Phase: "unverified", Label: "slipway-linux", Observed: "2026-09-13T11:59:00Z",
				Diagnostic: DiagnosticUnavailable,
			},
			Policy: Resolved{
				Enabled: true, MaxConcurrent: 2, Platforms: []string{"linux"},
				EnabledSource: SourceRepository, MaxConcurrentSource: SourceRepository,
				PlatformsSource: SourceDefault,
			},
			ChargedSlots: 1, AssignedJobs: 2, QueuedOffers: 3, SessionOpen: true,
			Diagnostic: DiagnosticUnavailable,
		}},
	}
}

// B9: the Gatus config names these keys as strings, so a struct-tag rename is
// invisible to the compiler and silently turns a live check into one that reads
// an absent field — indistinguishable from a passing one until an outage. The
// golden file is the only thing that can go red for that.
//
// Regenerate deliberately with -update after a tag change the design approves.
var update = false

func TestReportJSONShapeIsPinned(t *testing.T) {
	golden := filepath.Join("testdata", "report.golden.json")
	encoded, err := json.MarshalIndent(fullyPopulatedReport(), "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	encoded = append(encoded, '\n')
	if update {
		if err := os.WriteFile(golden, encoded, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != string(want) {
		t.Fatalf("the report's JSON shape changed.\n--- want ---\n%s\n--- got ---\n%s", want, encoded)
	}
}

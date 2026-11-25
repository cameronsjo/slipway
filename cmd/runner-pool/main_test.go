package main

import (
	"strings"
	"testing"

	"github.com/cameronsjo/slipway/internal/githubapp"
)

// The foundation probe reads the discovery snapshot, which under an
// all-repositories installation contains every repository in the account. The
// probe applies the pool's private-and-active rule itself.
func TestEligibleProbeRepositoryRequiresPrivateAndActive(t *testing.T) {
	repos := []githubapp.Repository{
		{ID: 1, FullName: "owner/private", Private: true},
		{ID: 2, FullName: "owner/public"},
		{ID: 3, FullName: "owner/archived", Private: true, Archived: true},
		{ID: 4, FullName: "owner/disabled", Private: true, Disabled: true},
	}
	for _, tc := range []struct {
		name, want string
	}{
		{"owner/public", "private, active repository"},
		{"owner/archived", "private, active repository"},
		{"owner/disabled", "private, active repository"},
		{"owner/absent", "not present in the selected or all-repositories installation"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selected, err := eligibleProbeRepository(repos, tc.name)
			if err == nil || selected != nil {
				t.Fatalf("accepted an ineligible repository: %+v", selected)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("refused for an unrelated reason: %v", err)
			}
		})
	}
	selected, err := eligibleProbeRepository(repos, "owner/private")
	if err != nil || selected == nil || selected.ID != 1 {
		t.Fatalf("private active repository refused: %+v %v", selected, err)
	}
}

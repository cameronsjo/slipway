package main

import "testing"

func TestSelectedToolAllowsOnlyFixedHostInspectors(t *testing.T) {
	tests := map[string]string{
		"sjo-host-crictl": "/var/lib/rancher/k3s/data/current/bin/crictl",
		"sjo-host-ctr":    "/var/lib/rancher/k3s/data/current/bin/ctr",
	}
	for name, expected := range tests {
		actual, err := selectedTool(name)
		if err != nil || actual != expected {
			t.Fatalf("selectedTool(%q) = %q, %v; want %q", name, actual, err, expected)
		}
	}
	// sjo-host-lvs was RETIRED, and it is asserted rejected rather than simply
	// deleted from the table above. The witness no longer runs lvs -- a root
	// host timer publishes the report and the witness reads the file -- so the
	// launcher must refuse the name. Dropping the row would leave a launcher
	// that still resolves it looking fully tested.
	for _, name := range []string{"runner-pool-host-tool", "sh", "../lvs", "sjo-host-lvs", ""} {
		if _, err := selectedTool(name); err == nil {
			t.Fatalf("selectedTool(%q) accepted an unapproved executable", name)
		}
	}
}

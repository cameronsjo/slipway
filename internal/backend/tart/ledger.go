package tart

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
)

// ledgerEntry is the whole durable record the adapter keeps per VM: the lease
// UUID and nothing else. No capability, no JIT, no bootstrap origin, no runner
// registration. It is written before `tart clone` so a clone that succeeds
// while the controller dies is still discoverable as owned.
type ledgerEntry struct {
	LeaseID string `json:"lease_id"`
}

func (b *Backend) ledgerPath(resourceID string) (string, error) {
	if !resourceNameRE.MatchString(resourceID) {
		return "", errors.New("tart resource name is not a pool VM name")
	}
	return filepath.Join(b.config.LedgerDir, resourceID+".json"), nil
}

func (b *Backend) writeLedger(resourceID, leaseID string) error {
	path, err := b.ledgerPath(resourceID)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(b.config.LedgerDir, 0o700); err != nil {
		return errors.New("tart ledger directory is not writable")
	}
	data, err := json.Marshal(ledgerEntry{LeaseID: leaseID})
	if err != nil {
		return errors.New("tart ledger entry could not be encoded")
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return errors.New("tart ledger entry could not be written")
	}
	return nil
}

func (b *Backend) removeLedger(resourceID string) error {
	path, err := b.ledgerPath(resourceID)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return errors.New("tart ledger entry could not be removed")
	}
	return nil
}

// readLedger returns resource name to lease UUID for every entry. A malformed
// or non-canonical entry is an error, not a skipped file: skipping one would
// turn an owned VM into an untracked one and fence the controller for a reason
// nobody could find.
func (b *Backend) readLedger() (map[string]string, error) {
	entries, err := os.ReadDir(b.config.LedgerDir)
	if errors.Is(err, os.ErrNotExist) {
		return map[string]string{}, nil
	}
	if err != nil {
		return nil, errors.New("tart ledger directory is unreadable")
	}
	owned := make(map[string]string, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".json") {
			continue
		}
		resourceID := strings.TrimSuffix(name, ".json")
		if !resourceNameRE.MatchString(resourceID) {
			return nil, errors.New("tart ledger contains an entry that is not a pool VM name")
		}
		// Bounded, non-blocking and no-follow: a FIFO planted in the ledger
		// directory would otherwise hang every Create and Inventory.
		data, err := readSmallRegularFile(filepath.Join(b.config.LedgerDir, name))
		if err != nil {
			return nil, errors.New("tart ledger entry is unreadable")
		}
		var record ledgerEntry
		if err := json.Unmarshal(data, &record); err != nil {
			return nil, errors.New("tart ledger entry is malformed")
		}
		parsed, err := uuid.Parse(record.LeaseID)
		if err != nil || parsed.String() != record.LeaseID {
			return nil, errors.New("tart ledger entry lacks a canonical lease UUID")
		}
		owned[resourceID] = record.LeaseID
	}
	return owned, nil
}

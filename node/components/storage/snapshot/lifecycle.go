// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package snapshot

// LifecycleState is the file's position in the storage-owned import
// lifecycle. See docs/plans/20260501-storage-lifecycle-spec.md for the
// full state machine and transition triggers.
//
// The zero value (LifecycleDeclared) is the natural initial state for
// a freshly-declared entry whose primary file has not yet been
// downloaded.
type LifecycleState int

const (
	// LifecycleDeclared — entry is in the manifest (peer-advertised or
	// local). Primary file not yet on disk.
	LifecycleDeclared LifecycleState = iota

	// LifecycleDownloading — primary file fetch is in progress.
	// Reserved for the lifecycle driver; AddFile does not derive this.
	LifecycleDownloading

	// LifecycleDownloaded — primary file is on disk. Dependent index
	// files (.idx, .kvi, .ef, .efi as required by the file's kind)
	// may not yet be present. Local is derived as
	// state >= LifecycleDownloaded.
	LifecycleDownloaded

	// LifecycleIndexing — index-build is in progress for the file's
	// dependencies. Reserved for the lifecycle driver.
	LifecycleIndexing

	// LifecycleIndexed — primary AND all dependent files are on disk.
	// Structural validators may now run.
	LifecycleIndexed

	// LifecycleValidating — validator chain is in progress. Reserved
	// for the lifecycle driver.
	LifecycleValidating

	// LifecycleAdvertisable — validation passed. The single Ready
	// gate: file is usable for internal reads AND eligible for
	// external publication. Advertisable is derived as
	// state == LifecycleAdvertisable.
	LifecycleAdvertisable
)

// String returns a human-readable name for telemetry and error messages.
func (s LifecycleState) String() string {
	switch s {
	case LifecycleDeclared:
		return "Declared"
	case LifecycleDownloading:
		return "Downloading"
	case LifecycleDownloaded:
		return "Downloaded"
	case LifecycleIndexing:
		return "Indexing"
	case LifecycleIndexed:
		return "Indexed"
	case LifecycleValidating:
		return "Validating"
	case LifecycleAdvertisable:
		return "Advertisable"
	}
	return "unknown"
}

// deriveStateFromFlags returns the LifecycleState that a given (Local,
// Advertisable) flag pair corresponds to. Used by AddFile to set State
// for back-compat with callers that still populate flags directly:
//
//   - Advertisable=true                     → LifecycleAdvertisable
//   - Advertisable=false, Local=true         → LifecycleDownloaded
//   - Advertisable=false, Local=false        → LifecycleDeclared
//
// The intermediate states (Downloading, Indexing, Indexed, Validating)
// cannot be inferred from the flags — they're reserved for the
// lifecycle driver to set explicitly via AdvanceTo. Existing callers
// that don't go through the driver land at one of the three stable
// states above.
func deriveStateFromFlags(local, advertisable bool) LifecycleState {
	switch {
	case advertisable:
		return LifecycleAdvertisable
	case local:
		return LifecycleDownloaded
	default:
		return LifecycleDeclared
	}
}

// applyStateToFlags sets Local and Advertisable on the entry to match
// the given state. Inverse of deriveStateFromFlags; used by the
// lifecycle driver's AdvanceTo to keep flags consistent.
func applyStateToFlags(e *FileEntry, state LifecycleState) {
	e.State = state
	e.Local = state >= LifecycleDownloaded
	e.Advertisable = state == LifecycleAdvertisable
}

// LifecycleState returns the current lifecycle state of the named
// entry, or (LifecycleDeclared, false) if no entry with that name
// exists. Convenience accessor for the lifecycle driver and tests.
func (inv *Inventory) LifecycleState(name string) (LifecycleState, bool) {
	inv.mu.RLock()
	defer inv.mu.RUnlock()
	if e := inv.findByNameLocked(name); e != nil {
		return e.State, true
	}
	return LifecycleDeclared, false
}

// AdvanceTo sets the named entry's lifecycle state to target, updating
// the derived Local and Advertisable flags consistently. Notifies
// subscribers via ChangeSet on actual state change.
//
// Allowed transitions:
//   - Forward to any LATER state. Skipping intermediate states is
//     permitted (e.g. Declared → Advertisable for a file discovered
//     fully validated at startup scan).
//   - Reset back to LifecycleDeclared from any state — used when a
//     file is re-classified as undownloaded (corruption-detected
//     re-download).
//
// Other backward transitions are rejected (returns false). Forward
// transitions when the entry is already at-or-past target are no-ops
// (returns false; not an error, just no work to do).
//
// Returns true if the state was changed and a ChangeSet was emitted.
func (inv *Inventory) AdvanceTo(name string, target LifecycleState) bool {
	inv.mu.Lock()
	e := inv.findByNameLocked(name)
	if e == nil {
		inv.mu.Unlock()
		return false
	}

	// Reject backward transitions except reset-to-Declared.
	if target < e.State && target != LifecycleDeclared {
		inv.mu.Unlock()
		return false
	}
	// No-op: already at or past target (and target isn't a reset).
	if target == e.State {
		inv.mu.Unlock()
		return false
	}

	applyStateToFlags(e, target)
	inv.recordTimingTransitionLocked(name, target, inv.now())
	inv.mu.Unlock()

	inv.notify(ChangeSet{Files: []string{name}})
	return true
}

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

import "strings"

// StepKey identifies a step group: files with the same StepKey are
// step-siblings, the natural unit produced by one retire / merge cycle.
//
// Block steps have empty Domain; state steps carry the domain. Files
// without step semantics (caplin, meta, salt) have FromStep=ToStep=0
// and are NOT subject to step-batch operations — they're singletons.
//
// See docs/plans/20260504-step-and-minimum-unified.md for the model
// driving this.
type StepKey struct {
	FromStep uint64
	ToStep   uint64
	Domain   Domain
}

// IsZero reports whether this key refers to a non-stepped (singleton)
// file group. Callers performing step-batch operations should skip
// zero keys.
func (k StepKey) IsZero() bool {
	return k.FromStep == 0 && k.ToStep == 0 && k.Domain == ""
}

// StepKey returns the file's step group identifier. For non-stepped
// files (caplin, meta, salt) the result is the zero StepKey, which
// callers should treat as "this file is a singleton, no batch
// grouping applies".
func (f *FileEntry) StepKey() StepKey {
	if f == nil {
		return StepKey{}
	}
	return StepKey{FromStep: f.FromStep, ToStep: f.ToStep, Domain: f.Domain}
}

// IsMinimum reports whether this file is part of its step's
// minimum publishable subset. Delegates to IsMinimumByName after
// gating on step presence — a file with no step has no
// minimum/extras dimension.
//
// See docs/plans/20260504-step-and-minimum-unified.md for the
// unified definition.
func (f *FileEntry) IsMinimum() bool {
	if f == nil || f.StepKey().IsZero() {
		return false
	}
	return IsMinimumByName(f.Name)
}

// IsMinimumByName is the name-only flavour of FileEntry.IsMinimum.
// Used by callers (downloader sort, chain.toml ordering) that have
// a name but not a full FileEntry.
//
// Mapping (from docs/plans/20260504-step-and-minimum-unified.md):
//
//   - State step minimum: .kv (domain primary), .kvi / .bt
//     (accessors). History files (.v, .ef, .efi) are extras.
//   - Block step minimum: files matching `-headers.seg` or
//     `-headers.idx`. Bodies, transactions, and their accessors
//     are extras.
//   - Non-stepped (caplin, meta, salt) and unrecognised patterns
//     return false.
//
// Initial mapping is "headers + state primary" — the smallest
// publishable subset that supports a useful consumer (header-only /
// current-state-query users).
func IsMinimumByName(name string) bool {
	if strings.HasSuffix(name, ".kv") ||
		strings.HasSuffix(name, ".kvi") ||
		strings.HasSuffix(name, ".bt") {
		return true
	}
	return strings.HasSuffix(name, "-headers.seg") ||
		strings.HasSuffix(name, "-headers.idx")
}

// StepGroup is the set of FileEntries that share a step key.
//
// Returned by Inventory.FilesAtStep. Callers that want to operate on
// the minimum subset (publish first, validate first) call Minimum();
// callers that want the rest call Extras().
type StepGroup struct {
	Key   StepKey
	Files []*FileEntry
}

// Minimum returns entries flagged IsMinimum. Order matches Files.
func (g StepGroup) Minimum() []*FileEntry {
	var out []*FileEntry
	for _, e := range g.Files {
		if e.IsMinimum() {
			out = append(out, e)
		}
	}
	return out
}

// Extras returns entries NOT flagged IsMinimum. Order matches Files.
func (g StepGroup) Extras() []*FileEntry {
	var out []*FileEntry
	for _, e := range g.Files {
		if !e.IsMinimum() {
			out = append(out, e)
		}
	}
	return out
}

// AllAtState reports whether every file in the group is at the given
// lifecycle state OR a later one. Empty group returns true (vacuously
// satisfied) — callers that need a non-empty check should test
// len(g.Files) > 0 first.
func (g StepGroup) AllAtState(state LifecycleState) bool {
	for _, e := range g.Files {
		if e.State < state {
			return false
		}
	}
	return true
}

// FilesAtStep returns the StepGroup for a key. The returned Files
// slice contains clones (safe to read concurrently); callers must not
// rely on slice aliasing back into the inventory.
//
// Returns an empty group (Files == nil) when the key is the zero
// StepKey or no files match — callers checking emptiness should use
// len(group.Files) == 0.
func (inv *Inventory) FilesAtStep(key StepKey) StepGroup {
	if key.IsZero() {
		return StepGroup{Key: key}
	}
	inv.mu.RLock()
	defer inv.mu.RUnlock()
	var matching []*FileEntry
	collect := func(slice []*FileEntry) {
		for _, e := range slice {
			if e.StepKey() == key {
				matching = append(matching, e.Clone())
			}
		}
	}
	if key.Domain == "" {
		collect(inv.blocks)
	} else {
		collect(inv.domains[key.Domain])
	}
	return StepGroup{Key: key, Files: matching}
}

// AdvanceStep atomically transitions every file in the named step
// group whose current state is < target to that target. Returns the
// names of files that actually changed state — callers can log /
// notify based on the returned set.
//
// Idempotent: files already at or past the target state are
// unchanged, no name returned. Files in the group at a state below
// target are advanced.
//
// Notification: a single ChangeSet covering all advanced names is
// emitted; subscribers can react once for the whole step.
func (inv *Inventory) AdvanceStep(key StepKey, target LifecycleState) []string {
	if key.IsZero() {
		return nil
	}
	inv.mu.Lock()
	now := inv.now()
	var advanced []string
	apply := func(slice []*FileEntry) {
		for _, e := range slice {
			if e.StepKey() != key {
				continue
			}
			if e.State < target {
				applyStateToFlags(e, target)
				inv.recordTimingTransition(e.Name, target, now)
				advanced = append(advanced, e.Name)
			}
		}
	}
	if key.Domain == "" {
		apply(inv.blocks)
	} else {
		apply(inv.domains[key.Domain])
	}
	inv.mu.Unlock()
	if len(advanced) > 0 {
		inv.notify(ChangeSet{Files: advanced})
	}
	return advanced
}

// AdvanceFiles atomically transitions every named file whose current
// state is < target to target. Returns the names that actually changed
// state. Used by the per-step batch hook to advance a SUBSET of a
// step group (e.g. just the minimum files, leaving extras at their
// current state for a separate later transition).
//
// Idempotent: files already at-or-past target are unchanged. Files
// that don't exist in the inventory are silently skipped.
func (inv *Inventory) AdvanceFiles(names []string, target LifecycleState) []string {
	if len(names) == 0 {
		return nil
	}
	inv.mu.Lock()
	now := inv.now()
	var advanced []string
	for _, name := range names {
		e := inv.findByNameLocked(name)
		if e == nil {
			continue
		}
		if e.State < target {
			applyStateToFlags(e, target)
			inv.recordTimingTransition(name, target, now)
			advanced = append(advanced, name)
		}
	}
	inv.mu.Unlock()
	if len(advanced) > 0 {
		inv.notify(ChangeSet{Files: advanced})
	}
	return advanced
}

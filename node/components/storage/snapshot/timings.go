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

import "time"

// FileTimings records lifecycle wall-clock timestamps for a single
// file. All zero values mean "not yet reached". The fields are filled
// in monotonically as the file progresses through the lifecycle —
// once set, a timestamp is never updated.
//
// Used by the future download orchestrator (per
// docs/plans/20260504-step-and-minimum-unified.md) to compute
// per-step and per-tier timing breakdowns:
//
//   - EnqueuedAt: file first observed by the inventory (AddFile).
//     Approximate "download considered" — actual download may not
//     have started yet if the orchestrator was queueing.
//   - DownloadCompletedAt: state reached LifecycleDownloaded
//     (bytes are on disk).
//   - IndexedAt: state reached LifecycleIndexed (deps satisfied,
//     accessory builds done).
//   - ValidatedAt: state reached LifecycleAdvertisable (batch
//     validation passed; file is publishable).
//
// Initial implementation derives bandwidth from these timestamps
// post-hoc:
//
//	downloadDurationApprox = DownloadCompletedAt - EnqueuedAt
//	bytesPerSec = file.Size / downloadDurationApprox
//
// This is rough — the gap between enqueued and download-started is
// not visible at this layer. The eventual integration with real-
// time torrent transfer events lives behind the same accessor;
// callers do not change.
type FileTimings struct {
	EnqueuedAt          time.Time
	DownloadCompletedAt time.Time
	IndexedAt           time.Time
	ValidatedAt         time.Time
}

// IsZero reports whether no timestamps have been recorded — i.e. the
// file is unknown to the inventory. Callers use this to distinguish
// "file not in inventory" from "file in inventory at LifecycleDeclared".
func (t FileTimings) IsZero() bool {
	return t.EnqueuedAt.IsZero() && t.DownloadCompletedAt.IsZero() &&
		t.IndexedAt.IsZero() && t.ValidatedAt.IsZero()
}

// StepTimings is the per-step view derived from the timings of every
// file in the step group. Computed on-demand from FilesAtStep — the
// inventory does not cache the derivation.
//
// Fields with the "Minimum" prefix are computed across the step's
// minimum subset (per IsMinimum); fields with the "All" prefix are
// computed across every file in the step.
//
// Zero timestamps in derived fields propagate "max" semantics: if
// any file in the relevant subset hasn't reached the target state,
// the derived timestamp is zero (not partially-completed).
type StepTimings struct {
	Key             StepKey
	FirstFileSeenAt time.Time // min(EnqueuedAt) across step files

	MinimumDownloadedAt time.Time // max(DownloadCompletedAt) across Minimum()
	AllDownloadedAt     time.Time // max(DownloadCompletedAt) across all

	MinimumIndexedAt time.Time // max(IndexedAt) across Minimum()
	AllIndexedAt     time.Time // max(IndexedAt) across all

	MinimumValidatedAt time.Time // max(ValidatedAt) across Minimum()
	AllValidatedAt     time.Time // max(ValidatedAt) across all
}

// FileTimings returns the recorded timings for the named file. The
// second return reports whether the inventory has seen this file at
// all; missing files yield zero timings + false.
func (inv *Inventory) FileTimings(name string) (FileTimings, bool) {
	inv.mu.RLock()
	defer inv.mu.RUnlock()
	t, ok := inv.timings[name]
	if !ok {
		return FileTimings{}, false
	}
	return *t, true
}

// StepTimings derives the per-step timing view from the per-file
// timings of the step's group members. Returns the zero StepTimings
// (with Key set) if no files match the key.
func (inv *Inventory) StepTimings(key StepKey) StepTimings {
	out := StepTimings{Key: key}
	if key.IsZero() {
		return out
	}
	group := inv.FilesAtStep(key)
	if len(group.Files) == 0 {
		return out
	}
	minimum := group.Minimum()

	// Helpers — track the running min/max across the relevant subset.
	// Zero in any file's relevant timestamp means "not all files in
	// that subset have reached the corresponding state yet"; the
	// derived timestamp stays zero in that case.
	minTs := func(set []*FileEntry, pick func(FileTimings) time.Time) time.Time {
		var out time.Time
		for _, f := range set {
			t, ok := inv.FileTimings(f.Name)
			if !ok {
				continue
			}
			ts := pick(t)
			if ts.IsZero() {
				continue
			}
			if out.IsZero() || ts.Before(out) {
				out = ts
			}
		}
		return out
	}
	maxTs := func(set []*FileEntry, pick func(FileTimings) time.Time) time.Time {
		var out time.Time
		for _, f := range set {
			t, ok := inv.FileTimings(f.Name)
			if !ok {
				return time.Time{} // missing file → not yet complete
			}
			ts := pick(t)
			if ts.IsZero() {
				return time.Time{} // unfinished file → not yet complete
			}
			if ts.After(out) {
				out = ts
			}
		}
		return out
	}

	out.FirstFileSeenAt = minTs(group.Files, func(t FileTimings) time.Time { return t.EnqueuedAt })

	out.MinimumDownloadedAt = maxTs(minimum, func(t FileTimings) time.Time { return t.DownloadCompletedAt })
	out.AllDownloadedAt = maxTs(group.Files, func(t FileTimings) time.Time { return t.DownloadCompletedAt })

	out.MinimumIndexedAt = maxTs(minimum, func(t FileTimings) time.Time { return t.IndexedAt })
	out.AllIndexedAt = maxTs(group.Files, func(t FileTimings) time.Time { return t.IndexedAt })

	out.MinimumValidatedAt = maxTs(minimum, func(t FileTimings) time.Time { return t.ValidatedAt })
	out.AllValidatedAt = maxTs(group.Files, func(t FileTimings) time.Time { return t.ValidatedAt })

	return out
}

// recordTimingTransition is called from inventory mutation paths to
// stamp the appropriate timestamp when state transitions occur.
// Caller must hold inv.mu.Lock().
func (inv *Inventory) recordTimingTransition(name string, target LifecycleState, now time.Time) {
	if inv.timings == nil {
		inv.timings = make(map[string]*FileTimings)
	}
	t, ok := inv.timings[name]
	if !ok {
		t = &FileTimings{}
		inv.timings[name] = t
	}
	switch target {
	case LifecycleDownloaded:
		if t.DownloadCompletedAt.IsZero() {
			t.DownloadCompletedAt = now
		}
	case LifecycleIndexed:
		if t.IndexedAt.IsZero() {
			t.IndexedAt = now
		}
	case LifecycleAdvertisable:
		if t.ValidatedAt.IsZero() {
			t.ValidatedAt = now
		}
	}
}

// recordEnqueue stamps EnqueuedAt the first time the inventory sees
// a file. Caller must hold inv.mu.Lock().
func (inv *Inventory) recordEnqueue(name string, now time.Time) {
	if inv.timings == nil {
		inv.timings = make(map[string]*FileTimings)
	}
	t, ok := inv.timings[name]
	if !ok {
		t = &FileTimings{}
		inv.timings[name] = t
	}
	if t.EnqueuedAt.IsZero() {
		t.EnqueuedAt = now
	}
}

// now returns the current time using the inventory's clock function.
// nowFn is nil in production (use time.Now) and may be overridden by
// tests for deterministic timestamps via SetClock.
func (inv *Inventory) now() time.Time {
	if inv.nowFn != nil {
		return inv.nowFn()
	}
	return time.Now()
}

// SetClock overrides the inventory's time source. Tests use this to
// produce deterministic timestamps. Production must NOT call this —
// the default time.Now is the right answer.
func (inv *Inventory) SetClock(fn func() time.Time) {
	inv.mu.Lock()
	defer inv.mu.Unlock()
	inv.nowFn = fn
}

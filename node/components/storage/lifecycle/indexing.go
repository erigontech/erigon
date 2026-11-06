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

package lifecycle

import (
	"context"
	"sync"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// IndexBuilder produces the dependent index files for a primary file
// at LifecycleDownloaded. Implementations wrap the existing build code
// from db/snapshotsync (BuildMissedIndices) and db/state (accessor
// builders); the driver invokes the wrapper without knowing the
// underlying details.
//
// Behaviour contract:
//   - On nil return: the caller can assume the primary's dependencies
//     have been emitted to disk and either are or will shortly be
//     reflected in the inventory (via OnFilesChange / explicit
//     AddFile by the production wiring).
//   - On error return: the build did not produce the deps. The driver
//     leaves the entry at LifecycleDownloaded; the next sweep retries.
//
// Implementations may batch internally — e.g. a single BuildMissedIndices
// invocation that handles many files in one pass, with per-file callers
// verifying their own results afterward. Step 4 does not specify the
// batching strategy; the production wrapper in step 6 chooses.
type IndexBuilder interface {
	BuildMissedIndices(ctx context.Context, primary *snapshot.FileEntry) error
}

// BuildOnIndexing returns a Handler suitable for Driver.OnIndexing.
//
// The entry advances to LifecycleIndexed once its accessors are on disk, which
// is what the gates waiting on that state assume. The question is asked of the
// disk rather than of FileEntry.Dependencies: an accessor carries its own
// version prefix, so its name is not derivable from the primary's. Entries the
// disk cannot answer for — those with no accessors, or when snapDir is unset —
// keep the Dependencies path.
//
// epoch supplies the driver's sweep number. The builder is a single global
// scan, so a second call within one sweep would run over the inputs the first
// already covered; the handler scans at most once per epoch and lets the
// remaining entries re-read the disk. A nil epoch disables that latch.
//
// An entry whose accessors are still missing after the scan stays at
// LifecycleDownloaded and is retried on the next sweep — not advancing is not
// an error. Builder errors propagate to the caller, which logs them at Debug
// per Driver.dispatch's contract.
//
// logger may be nil; on successful advance the handler emits an Info
// log line "advanced X to LifecycleIndexed" so each completed step is
// observable. nil logger silently skips the line.
func BuildOnIndexing(builder IndexBuilder, inv *snapshot.Inventory, snapDir string, epoch func() uint64, logger log.Logger) Handler {
	sweep := &sweepScope{snapDir: snapDir, epoch: epoch}
	return func(ctx context.Context, e *snapshot.FileEntry) error {
		if !canAskDisk(snapDir, e) {
			if depsAllLocal(inv, e.Dependencies) {
				advance(inv, e.Name, logger)
				return nil
			}
			if err := builder.BuildMissedIndices(ctx, e); err != nil {
				return err
			}
			if depsAllLocal(inv, e.Dependencies) {
				advance(inv, e.Name, logger)
			}
			return nil
		}

		if sweep.accessors().Has(e.Name) {
			advance(inv, e.Name, logger)
			return nil
		}
		if !sweep.claimScan() {
			return nil
		}
		if err := builder.BuildMissedIndices(ctx, e); err != nil {
			sweep.releaseScan()
			return err
		}
		// The scan wrote accessors the index predates, so ask the disk again.
		sweep.forgetAccessors()
		if sweep.accessors().Has(e.Name) {
			advance(inv, e.Name, logger)
		}
		return nil
	}
}

// sweepScope holds the work a sweep does once and every entry in it reuses:
// the reading of the snapshot directories, and the one global index build the
// sweep is allowed. A nil epoch means the caller cannot tell sweeps apart, so
// nothing is reused.
type sweepScope struct {
	snapDir string
	epoch   func() uint64

	mu       sync.Mutex
	at       uint64
	scanning bool
	index    *snapshot.AccessorIndex
}

func (s *sweepScope) now() uint64 {
	if s.epoch == nil {
		return 0
	}
	return s.epoch()
}

// enterLocked drops state belonging to an earlier sweep. Caller holds s.mu.
func (s *sweepScope) enterLocked(now uint64) {
	if s.epoch != nil && s.at == now {
		return
	}
	s.at, s.scanning, s.index = now, false, nil
}

// accessors reads the snapshot directories once per sweep. Asking per entry
// instead costs a directory read and a name parse per candidate, which over a
// sweep is quadratic in the number of files.
func (s *sweepScope) accessors() *snapshot.AccessorIndex {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.enterLocked(now)
	if s.index == nil || s.epoch == nil {
		s.index = snapshot.NewAccessorIndex(s.snapDir)
	}
	return s.index
}

func (s *sweepScope) forgetAccessors() {
	s.mu.Lock()
	s.index = nil
	s.mu.Unlock()
}

// claimScan reports whether the caller should run the global index build. One
// sweep gets one: a second would run over the inputs the first already covered.
func (s *sweepScope) claimScan() bool {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.enterLocked(now)
	if s.epoch != nil && s.scanning {
		return false
	}
	s.scanning = true
	return true
}

// releaseScan returns the sweep's claim after a failed build, so the next entry
// retries rather than waiting for a sweep that may find no other trigger.
func (s *sweepScope) releaseScan() {
	s.mu.Lock()
	s.scanning = false
	s.mu.Unlock()
}

// canAskDisk reports whether the snapshot directories can answer for this
// entry. Those they cannot speak for — caplin, accessors themselves, anything
// without a snapshot dir — keep the Dependencies path.
func canAskDisk(snapDir string, e *snapshot.FileEntry) bool {
	if snapDir == "" || e == nil || e.Kind == snapshot.KindCaplin {
		return false
	}
	return snapshot.HasAccessors(e.Name)
}

func depsAllLocal(inv *snapshot.Inventory, deps []string) bool {
	for _, depName := range deps {
		dep, ok := inv.GetByName(depName)
		if !ok || !dep.Local {
			return false
		}
	}
	return true
}

func advance(inv *snapshot.Inventory, name string, logger log.Logger) {
	if inv.AdvanceTo(name, snapshot.LifecycleIndexed) && logger != nil {
		logger.Info("[storage-lifecycle] advanced", "file", name, "to", "Indexed")
	}
}

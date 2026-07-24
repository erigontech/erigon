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

package storage

import (
	"os"
	"path/filepath"
	"strings"

	commondir "github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
)

// recoverOrphanRegenSidecars rolls back a mode-B unwind transaction
// interrupted by a crash. `.regen` files always drop (unfinished
// regen). `.old` files depend on the swap state: if the target `.kv`
// exists the swap completed and the .old is deleted; otherwise the
// swap was incomplete and the .old is renamed back into place.
// Sweeps snapDir and its per-kind subdirs. Idempotent, best-effort.
func (p *Provider) recoverOrphanRegenSidecars() {
	if p.snapDir == "" {
		return
	}
	logger := p.logger
	if logger == nil {
		logger = log.New()
	}

	type stats struct {
		regenRemoved   int
		oldRemovedDone int // case (a)
		oldRestored    int // case (b)
		errors         int
	}
	var totals stats

	walk := func(dir string) {
		entries, err := os.ReadDir(dir)
		if err != nil {
			if !os.IsNotExist(err) {
				logger.Warn("[storage] recoverOrphanRegenSidecars: read dir failed", "dir", dir, "err", err)
				totals.errors++
			}
			return
		}
		for _, de := range entries {
			if de.IsDir() {
				continue
			}
			name := de.Name()
			path := filepath.Join(dir, name)

			switch {
			case strings.HasSuffix(name, ".regen"):
				if err := commondir.RemoveFile(path); err != nil && !os.IsNotExist(err) {
					logger.Warn("[storage] recoverOrphanRegenSidecars: remove .regen failed", "path", path, "err", err)
					totals.errors++
					continue
				}
				totals.regenRemoved++

			case strings.HasSuffix(name, ".old"):
				original := strings.TrimSuffix(path, ".old")
				if _, err := os.Stat(original); err == nil {
					// Case (a): finalize completed, original is in place.
					if err := commondir.RemoveFile(path); err != nil && !os.IsNotExist(err) {
						logger.Warn("[storage] recoverOrphanRegenSidecars: remove .old (case a) failed", "path", path, "err", err)
						totals.errors++
						continue
					}
					totals.oldRemovedDone++
				} else if os.IsNotExist(err) {
					// Case (b): swap incomplete. Restore from .old.
					if err := os.Rename(path, original); err != nil {
						logger.Warn("[storage] recoverOrphanRegenSidecars: rename .old → original (case b) failed", "path", path, "original", original, "err", err)
						totals.errors++
						continue
					}
					totals.oldRestored++
				} else {
					logger.Warn("[storage] recoverOrphanRegenSidecars: stat original failed", "path", original, "err", err)
					totals.errors++
				}
			}
		}
	}

	// Top-level snapDir holds block-snapshot files (v1.1-*.seg + their
	// indexes). Per-kind subdirs hold state-domain files.
	walk(p.snapDir)
	for _, sub := range []string{"domain", "history", "idx", "accessor"} {
		walk(filepath.Join(p.snapDir, sub))
	}

	if totals.regenRemoved+totals.oldRemovedDone+totals.oldRestored+totals.errors > 0 {
		logger.Info("[storage] recoverOrphanRegenSidecars: startup sweep completed",
			"regen_removed", totals.regenRemoved,
			"old_finalized_dropped", totals.oldRemovedDone,
			"old_restored", totals.oldRestored,
			"errors", totals.errors,
		)
	}
}

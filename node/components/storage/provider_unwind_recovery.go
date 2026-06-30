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

// recoverOrphanRegenSidecars cleans up `.old` and `.regen` files left
// behind by a crash mid-Provider.Unwind or mid-FinalizeUnwind. Both
// kinds of sidecar are transient — they exist only during an active
// mode-B unwind transaction — so any of them on disk at startup is an
// unfinished transaction we need to roll back.
//
// Recovery rule for each orphan kind:
//
//   - `<name>.regen`: the regen scratch file written during
//     Provider.Unwind. If FinalizeUnwind ran, this would already be
//     renamed to its truncated final path. If we find it on disk at
//     startup, that mode-B's tx either never reached commit (Abort-
//     Unwind's cleanup didn't run) or FinalizeUnwind crashed before
//     the rename. Either way, drop the .regen; the next mode-B will
//     regenerate from the canonical file.
//
//   - `<name>.old`: the original broad .kv file renamed aside during
//     FinalizeUnwind's swap. If the swap completed, the .old would
//     have been unlinked at the end. If we find it on disk at
//     startup, FinalizeUnwind crashed somewhere between the rename
//     and the unlink. Recovery depends on whether the swap had
//     completed by the crash:
//
//     (a) `<name>.kv` exists on disk → swap completed (broad was
//     renamed to .old, regen successfully renamed into the
//     target spot, .old unlink failed/skipped). Drop the .old;
//     the new .kv is canonical.
//
//     (b) `<name>.kv` does NOT exist → swap incomplete (broad was
//     renamed to .old, but no replacement file landed at
//     <name>.kv). Restore the broad by renaming .old back to
//     the original name. The next mode-B will redo the regen.
//
// The .kv accessor sidecars (.bt, .kvi, .kvei) follow the same logic
// — they get the same .old dance in FinalizeUnwind. We sweep the
// same patterns over both the top-level snapDir AND the per-kind
// subdirs (domain/, history/, idx/, accessor/) where state-domain
// files live.
//
// Idempotent. Best-effort: failures are logged + skipped; a subsequent
// restart re-attempts cleanup of anything that survived.
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

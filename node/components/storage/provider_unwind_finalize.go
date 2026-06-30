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
	"context"
	"os"
	"path/filepath"
	"strings"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/db/snapshotsync/fileset"
	"github.com/erigontech/erigon/db/snaptype"
)

// FinalizeUnwind executes the FS / inventory / downloader / republish
// ops that Provider.Unwind staged for post-commit execution. Called
// by setHeadModeB after tx.Commit succeeds.
//
// Ops are intentionally best-effort: at this point the chain head has
// already moved (the tx is committed). A stale torrent or undeleted
// .seg leftover is recoverable on operator restart; treating them as
// fatal here would just risk leaving the chain in an inconsistent
// committed-but-not-finalized state.
//
// Drains Provider.pendingTrim. Safe to call when nothing is staged.
//
// Also drains Provider.pendingRebuild — those new files are already
// on disk (written during Unwind), so post-commit they're the
// canonical state; nothing to do beyond clearing the list.
//
// After the deletes land, refresh the in-memory snapshot view via
// AllSnapshots.OpenFolder so the rebuilt straddle file is picked up
// + the deleted old files drop out of the visible set. Without this
// refresh the view stays stale until the next process restart, and
// reads of the rebuilt range can hit the old file's mmap (which
// Linux keeps live across unlink) and return stale data.
func (p *Provider) FinalizeUnwind() error {
	p.pendingTrimLock.Lock()
	staged := p.pendingTrim
	rebuilt := p.pendingRebuild
	regen := p.pendingRegen
	p.pendingTrim = nil
	p.pendingRebuild = nil
	p.pendingRegen = nil
	p.pendingTrimLock.Unlock()

	hadRegen := regen != nil && len(regen.pairs) > 0
	if (staged == nil || len(staged.names) == 0) && rebuilt == nil && !hadRegen {
		return nil
	}

	if staged != nil && len(staged.names) > 0 {
		if p.Inventory != nil {
			for _, name := range staged.names {
				p.Inventory.RemoveFile(name)
			}
		}

		for _, path := range staged.paths {
			_ = dir.RemoveFile(path)
			_ = dir.RemoveFile(path + ".torrent")
		}

		sweptNames := p.sweepBlockOrphansPastBlock(staged.toBlock)

		deleteNames := staged.names
		if len(sweptNames) > 0 {
			deleteNames = append(append([]string{}, staged.names...), sweptNames...)
		}
		if p.downloaderClient != nil {
			if err := p.downloaderClient.Delete(context.Background(), deleteNames); err != nil && p.logger != nil {
				p.logger.Warn("[storage] Provider.FinalizeUnwind: downloaderClient.Delete failed (continuing)", "err", err, "files", len(deleteNames))
			}
		}

		if p.republishChainToml != nil {
			if err := p.republishChainToml(); err != nil && p.logger != nil {
				p.logger.Warn("[storage] Provider.FinalizeUnwind: republishChainToml failed (continuing)", "err", err)
			}
		}
	}

	// Boundary-step state-domain regen swap. Each pair was prepared
	// during Provider.Unwind as <finalPath>.regen — atomically rename
	// it over the original via a .old dance so the running process's
	// mmap of the old inode gets released by AllSnapshots.OpenFolder
	// (which keys close-decisions on FILE NAME, not inode). Without
	// the .old dance the new file inode replaces the old one under
	// the same filename, OpenFolder doesn't close the old mmap, and
	// reads keep serving pre-regen content until restart.
	//
	// Sequence per pair:
	//   1. rename finalPath → finalPath.old   (old inode now anonymous-named)
	//   2. AllSnapshots.OpenFolder()           (closes old mmap — file at finalPath is gone)
	//   3. rename regenPath → finalPath        (new inode at the live name)
	// Then a single Aggregator.BuildMissedAccessors rebuilds .kvi/.bt/
	// .kvei against the new content (old accessors were left in place
	// pointing at the OLD inode's offsets, which is wrong; rebuild
	// invalidates them).  Then a second OpenFolder picks up the new
	// content's mmap. Finally .old is unlinked, freeing the old inode.
	if hadRegen {
		// The old broad .kv we're retiring is at pair.oldBroadPath
		// (equals pair.finalPath in the aligned case where the
		// boundary file's ToStep matched the unwind-target step;
		// differs when the regen output got a truncated filename to
		// reflect its actual coverage — see regen_wire.go's
		// regenPair doc for the two shapes).
		var accessorOlds []string
		for _, pair := range regen.pairs {
			oldSidecar := pair.oldBroadPath + ".old"
			if err := os.Rename(pair.oldBroadPath, oldSidecar); err != nil && p.logger != nil {
				p.logger.Warn("[storage] Provider.FinalizeUnwind: rename old broad .kv → .old failed (continuing — regen will be retried on next mode-B)", "err", err, "path", pair.oldBroadPath)
			}
			accessorOlds = append(accessorOlds, p.renameAccessorsToOld(pair.oldBroadPath)...)
		}
		if p.AllSnapshots != nil {
			if err := p.AllSnapshots.OpenFolder(); err != nil && p.logger != nil {
				p.logger.Warn("[storage] Provider.FinalizeUnwind: pre-regen OpenFolder failed (continuing — restart will refresh)", "err", err)
			}
		}
		var regenBaseNames []string
		var removedBroadBaseNames []string
		for _, pair := range regen.pairs {
			if err := os.Rename(pair.regenPath, pair.finalPath); err != nil && p.logger != nil {
				p.logger.Warn("[storage] Provider.FinalizeUnwind: rename .regen → .kv failed (continuing — restart will recover from .old)", "err", err, "regen", pair.regenPath, "final", pair.finalPath)
				continue
			}
			// Regen rewrote the file; the seed's .torrent sidecar now claims
			// a stale hash and the downloader would yank the .kv to .part.
			_ = dir.RemoveFile(pair.finalPath + ".torrent")
			regenBaseNames = append(regenBaseNames, filepath.Base(pair.finalPath))
			// Truncated case: the old broad file's .torrent sidecar
			// would still advertise the now-retired file. Remove it
			// alongside the broad .kv itself (the .kv was renamed to
			// .old above and will be unlinked at the end of this
			// block; the .torrent has no .old indirection so we drop
			// it directly here).
			if pair.oldBroadPath != pair.finalPath {
				_ = dir.RemoveFile(pair.oldBroadPath + ".torrent")
				removedBroadBaseNames = append(removedBroadBaseNames, filepath.Base(pair.oldBroadPath))
			}
		}
		// Tell the downloader about both the new (regen'd) basenames
		// and the removed broad basenames in one call — the latter so
		// any in-flight torrent for the retired broad gets cancelled
		// and the .part file (if any) cleaned up.
		downloaderDeletes := regenBaseNames
		if len(removedBroadBaseNames) > 0 {
			downloaderDeletes = append(append([]string{}, regenBaseNames...), removedBroadBaseNames...)
		}
		if p.downloaderClient != nil && len(downloaderDeletes) > 0 {
			if err := p.downloaderClient.Delete(context.Background(), downloaderDeletes); err != nil && p.logger != nil {
				p.logger.Warn("[storage] Provider.FinalizeUnwind: downloaderClient.Delete for regen failed (continuing)", "err", err, "files", len(downloaderDeletes))
			}
		}
		// Truncated case: the broad's Inventory entry is now stale
		// (its file is on disk only as a .old sidecar awaiting unlink).
		// Drop it so visible-set readers don't keep advertising it.
		// The new (truncated) filename gets added to Inventory on the
		// Aggregator.OpenFolder pass below.
		if p.Inventory != nil {
			for _, name := range removedBroadBaseNames {
				p.Inventory.RemoveFile(name)
			}
		}
		if p.Aggregator != nil {
			if err := p.Aggregator.OpenFolder(); err != nil && p.logger != nil {
				p.logger.Warn("[storage] Provider.FinalizeUnwind: post-rename Aggregator.OpenFolder failed (continuing — accessor rebuild may be skipped)", "err", err)
			}
		}
		if p.Aggregator != nil {
			if err := p.Aggregator.BuildMissedAccessors(context.Background(), 1); err != nil && p.logger != nil {
				p.logger.Warn("[storage] Provider.FinalizeUnwind: BuildMissedAccessors failed (continuing — accessors will be built on next process start)", "err", err)
			}
		}
		for _, pair := range regen.pairs {
			oldSidecar := pair.oldBroadPath + ".old"
			if err := dir.RemoveFile(oldSidecar); err != nil && !os.IsNotExist(err) && p.logger != nil {
				p.logger.Warn("[storage] Provider.FinalizeUnwind: remove .old sidecar failed (harmless leftover; cleanup on next restart)", "err", err, "path", oldSidecar)
			}
		}
		for _, oldPath := range accessorOlds {
			if err := dir.RemoveFile(oldPath); err != nil && !os.IsNotExist(err) && p.logger != nil {
				p.logger.Warn("[storage] Provider.FinalizeUnwind: remove accessor .old sidecar failed (harmless leftover; cleanup on next restart)", "err", err, "path", oldPath)
			}
		}
	}

	// Refresh the in-memory snapshot view whenever files changed —
	// either trimmed (staged), rebuilt, or regenerated.
	// AllSnapshots.OpenFolder re-scans snapDir, picks up any new/
	// regenerated files, and (via closeWhatNotInList →
	// DirtySegment.close → Decompressor.Close → mmap.Munmap) releases
	// the mmap for files that were deleted from disk. Without this
	// refresh on the trim-only path the process keeps serving the OLD
	// (deleted) inode via its still-mapped segments — Linux keeps an
	// unlinked inode alive until every reference drops — so reads
	// continue to return blocks that should now be unreachable.
	// Live-rig 2026-06-03: post-mode-B with the empty-rebuild-range
	// fix (04c568a71d) deleted the straddle .seg/.idx files from disk
	// but the running process served blocks past the unwind target via
	// `(deleted)` mmaps, wedging the catch-up downloader until restart.
	//
	// Best-effort: a refresh failure is recoverable on next restart.
	hadTrim := staged != nil && len(staged.names) > 0
	hadRebuild := rebuilt != nil
	if (hadTrim || hadRebuild || hadRegen) && p.AllSnapshots != nil {
		if err := p.AllSnapshots.OpenFolder(); err != nil && p.logger != nil {
			p.logger.Warn("[storage] Provider.FinalizeUnwind: AllSnapshots.OpenFolder failed (continuing — restart will refresh)", "err", err)
		}
	}

	if p.logger != nil {
		fileCount := 0
		if staged != nil {
			fileCount = len(staged.names)
		}
		rebuildCount := 0
		if rebuilt != nil {
			rebuildCount = len(rebuilt.paths)
		}
		regenCount := 0
		if regen != nil {
			regenCount = len(regen.pairs)
		}
		p.logger.Info("[storage] Provider.FinalizeUnwind: deferred snapshot-trim ops executed", "deleted", fileCount, "rebuilt", rebuildCount, "regenerated", regenCount)
	}
	return nil
}

// sweepBlockOrphansPastBlock walks p.snapDir and removes any file
// whose parsed block range extends past the chunk-aligned tip, plus
// its .torrent sidecar. Returns the primary names removed so the
// caller can drop them from the downloader. Inventory entries are
// removed in lockstep.
//
// Delegates the predicate to fileset.StalePastTip — see
// db/snapshotsync/fileset/rules.go for the rules contract. A file
// with From ≤ toBlock < To still contains blocks past the new tip
// (a "straddler") and the rules module catches both that case and
// entirely-past orphans with one predicate.
//
// State-domain files live under domain/ and history/ subdirs; this
// sweep scopes to the top-level snapDir entries which is where block
// snapshot files (and their accessors) live.
func (p *Provider) sweepBlockOrphansPastBlock(toBlock uint64) []string {
	if p.snapDir == "" {
		return nil
	}
	// toBlock=0 is the sentinel for "no target set" — tests that stage
	// pendingTrim directly without going through unwindSnapshotsPastBlock
	// hit this path. Sweeping with toBlock=0 would delete every block
	// snapshot file.
	if toBlock == 0 {
		return nil
	}
	newTo := chunkAlignedToBlock(toBlock)
	entries, err := os.ReadDir(p.snapDir)
	if err != nil {
		return nil
	}
	type onDisk struct {
		name    string
		primary string
	}
	var disk []onDisk
	var ranges []fileset.Tagged
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		primary := strings.TrimSuffix(name, ".torrent")
		fi, _, ok := snaptype.ParseFileName(p.snapDir, primary)
		if !ok {
			continue
		}
		disk = append(disk, onDisk{name: name, primary: primary})
		ranges = append(ranges, fileset.Tagged{Range: fileset.Range{From: fi.From, To: fi.To}})
	}
	stale := fileset.StalePastTip(ranges, newTo)
	var removedPrimaries []string
	for _, idx := range stale {
		entry := disk[idx]
		_ = dir.RemoveFile(filepath.Join(p.snapDir, entry.name))
		if entry.primary == entry.name {
			if p.Inventory != nil {
				p.Inventory.RemoveFile(entry.name)
			}
			removedPrimaries = append(removedPrimaries, entry.name)
		}
	}
	if len(removedPrimaries) > 0 && p.logger != nil {
		p.logger.Info("[storage] Provider.FinalizeUnwind: orphan-past-toBlock sweep", "files", len(removedPrimaries), "toBlock", toBlock, "newTo", newTo)
	}
	return removedPrimaries
}

// renameAccessorsToOld renames every accessor file (.bt / .kvi /
// .kvei) sharing the regenerated .kv's domain + step-range to a .old
// sidecar. fileItemsWithMissedAccessors rebuilds only when the
// accessor file is absent under its canonical name; without renaming
// the stale accessor out of the way the rebuild predicate skips it
// and the new (truncated) .kv is served via the OLD accessor's
// offsets.
func (p *Provider) renameAccessorsToOld(finalPath string) []string {
	finalBase := filepath.Base(finalPath)
	dashIdx := strings.IndexByte(finalBase, '-')
	if dashIdx == -1 {
		return nil
	}
	suffix := strings.TrimSuffix(finalBase[dashIdx+1:], ".kv")
	if suffix == finalBase[dashIdx+1:] {
		return nil
	}
	finalDir := filepath.Dir(finalPath)
	var olds []string
	for _, ext := range []string{".bt", ".kvi", ".kvei"} {
		matches, err := filepath.Glob(filepath.Join(finalDir, "v*-"+suffix+ext))
		if err != nil {
			continue
		}
		for _, m := range matches {
			oldPath := m + ".old"
			if err := os.Rename(m, oldPath); err != nil {
				if !os.IsNotExist(err) && p.logger != nil {
					p.logger.Warn("[storage] Provider.FinalizeUnwind: rename accessor → .old failed (continuing — fileItemsWithMissedAccessors will see stale accessor on disk and skip the rebuild; manual cleanup may be required)", "err", err, "path", m)
				}
				continue
			}
			olds = append(olds, oldPath)
		}
	}
	return olds
}

// AbortUnwind drops the FS / inventory / downloader / republish ops
// staged by Provider.Unwind without executing any of them. Called by
// setHeadModeB on every error path where FinalizeUnwind will not run
// — guaranteeing that a failed/rolled-back mode-B leaves the datadir
// in the same state it was in before the call.
//
// Drains pendingRebuild too: rebuilt files were written to disk
// during Unwind (FS writes aren't tx-bound), so on the rollback path
// they must be deleted to restore the pre-mode-B datadir state.
//
// Safe to call when nothing is staged.
func (p *Provider) AbortUnwind() {
	p.pendingTrimLock.Lock()
	staged := p.pendingTrim
	rebuilt := p.pendingRebuild
	regen := p.pendingRegen
	p.pendingTrim = nil
	p.pendingRebuild = nil
	p.pendingRegen = nil
	p.pendingTrimLock.Unlock()

	if rebuilt != nil {
		for _, path := range rebuilt.paths {
			_ = dir.RemoveFile(path)
		}
	}

	// Regen .regen files were written during Unwind but not yet swapped
	// into place — they're tx-orphan FS artifacts on rollback. Drop them
	// so the pre-mode-B datadir is byte-identical to before the call.
	regenCount := 0
	if regen != nil {
		for _, pair := range regen.pairs {
			_ = dir.RemoveFile(pair.regenPath)
			regenCount++
		}
	}

	if (staged != nil && len(staged.names) > 0) || rebuilt != nil || regenCount > 0 {
		if p.logger != nil {
			stagedCount := 0
			if staged != nil {
				stagedCount = len(staged.names)
			}
			rebuiltCount := 0
			if rebuilt != nil {
				rebuiltCount = len(rebuilt.paths)
			}
			p.logger.Info("[storage] Provider.AbortUnwind: staged ops dropped", "staged", stagedCount, "rebuiltFilesDeleted", rebuiltCount, "regenFilesDeleted", regenCount)
		}
	}
}

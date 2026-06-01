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

	"github.com/erigontech/erigon/common/dir"
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
	p.pendingTrim = nil
	p.pendingRebuild = nil
	p.pendingTrimLock.Unlock()

	if (staged == nil || len(staged.names) == 0) && rebuilt == nil {
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

		if p.downloaderClient != nil {
			if err := p.downloaderClient.Delete(context.Background(), staged.names); err != nil && p.logger != nil {
				p.logger.Warn("[storage] Provider.FinalizeUnwind: downloaderClient.Delete failed (continuing)", "err", err, "files", len(staged.names))
			}
		}

		if p.republishChainToml != nil {
			if err := p.republishChainToml(); err != nil && p.logger != nil {
				p.logger.Warn("[storage] Provider.FinalizeUnwind: republishChainToml failed (continuing)", "err", err)
			}
		}
	}

	// Refresh the in-memory snapshot view if a rebuild happened.
	// AllSnapshots.OpenFolder re-scans snapDir, picks up the rebuilt
	// file, and drops the deleted old file from the visible set.
	// Best-effort: a refresh failure is recoverable on next restart.
	if rebuilt != nil && p.AllSnapshots != nil {
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
		p.logger.Info("[storage] Provider.FinalizeUnwind: deferred snapshot-trim ops executed", "deleted", fileCount, "rebuilt", rebuildCount)
	}
	return nil
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
	p.pendingTrim = nil
	p.pendingRebuild = nil
	p.pendingTrimLock.Unlock()

	if rebuilt != nil {
		for _, path := range rebuilt.paths {
			_ = dir.RemoveFile(path)
		}
	}

	if (staged != nil && len(staged.names) > 0) || rebuilt != nil {
		if p.logger != nil {
			stagedCount := 0
			if staged != nil {
				stagedCount = len(staged.names)
			}
			rebuiltCount := 0
			if rebuilt != nil {
				rebuiltCount = len(rebuilt.paths)
			}
			p.logger.Info("[storage] Provider.AbortUnwind: staged ops dropped", "staged", stagedCount, "rebuiltFilesDeleted", rebuiltCount)
		}
	}
}

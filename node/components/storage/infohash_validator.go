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
	"fmt"
	"os"
	"sync"

	"github.com/erigontech/erigon/db/integrity"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/erigontech/erigon/node/components/storage/validation"
)

// InfoHashValidator re-verifies a file's content against the piece
// hashes in its sibling .torrent — the startup pre-flight infohash
// check (docs/plans/20260522-publisher-startup-preflight.md). It is a
// validation.StepValidator in the lifecycle driver's chain: a file with
// a content/.torrent mismatch never reaches LifecycleAdvertisable, so
// the publisher never advertises bytes it does not actually hold.
//
// The torrent client piece-verifies a file at download time, but a
// bootstrap-local file carried over from a prior session — or any file
// since corrupted on disk — has had no such check. The validator runs
// uniformly on every file rather than tracking per-file provenance.
//
// A file that passes is recorded in `verified` keyed by its size+mtime;
// the step chain re-runs the whole batch on every sweep while any one
// file in the batch is still settling, and without the memo a large,
// already-good file would be SHA1'd from scratch on each retry.
type InfoHashValidator struct {
	// SnapDir is the snapshots directory the files live under.
	SnapDir string

	verified sync.Map // file name -> verifyStamp of the last passing check
}

// verifyStamp identifies a file's on-disk content cheaply: a piece-hash
// pass is still valid while size and mtime are unchanged.
type verifyStamp struct {
	size    int64
	modNano int64
}

// Name implements validation.StepValidator.
func (*InfoHashValidator) Name() string { return "infohash_match" }

// ValidateStep implements validation.StepValidator. Each Local file in
// the batch has its content piece-hashed against its .torrent.
func (v *InfoHashValidator) ValidateStep(ctx context.Context, files []*snapshot.FileEntry) error {
	for _, f := range files {
		if f == nil || !f.Local {
			continue
		}
		if err := v.checkFile(ctx, f.Name); err != nil {
			return fmt.Errorf("file %q: %w", f.Name, err)
		}
	}
	return nil
}

// checkFile piece-hashes the named file against its .torrent. Returns
// an error wrapping validation.ErrPause when the .torrent is not on
// disk yet — the seeder writes .torrent files asynchronously
// (provider.scanAndSeed), so at validation time a freshly-landed file
// may not have one. The lifecycle driver retries on the next sweep
// without ticking the quarantine counter; once the seeder has run, the
// real check applies. A file already piece-verified at its current
// size+mtime is accepted from the memo without re-hashing.
func (v *InfoHashValidator) checkFile(ctx context.Context, name string) error {
	if v.SnapDir == "" {
		return fmt.Errorf("InfoHashValidator: empty SnapDir")
	}
	dataPath := snapshot.ResolveExistingPath(v.SnapDir, name)
	if _, err := os.Stat(dataPath + ".torrent"); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("torrent not yet seeded: %w", validation.ErrPause)
		}
		return fmt.Errorf("stat torrent: %w", err)
	}
	di, err := os.Stat(dataPath)
	if err != nil {
		return fmt.Errorf("stat data file: %w", err)
	}
	stamp := verifyStamp{size: di.Size(), modNano: di.ModTime().UnixNano()}
	if cached, ok := v.verified.Load(name); ok && cached.(verifyStamp) == stamp {
		return nil
	}
	if err := integrity.VerifyFileAgainstTorrent(ctx, dataPath); err != nil {
		return fmt.Errorf("infohash mismatch: %w", err)
	}
	v.verified.Store(name, stamp)
	return nil
}

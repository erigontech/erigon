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
	"path/filepath"

	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/execution/chain"
)

// chunkAlignedToBlock returns the largest 1000-block-aligned value
// ≤ (toBlock+1). Block snapshot files are named in 1000-block units
// (see snaptype.FileInfo.As), so a rebuilt straddle file's new
// ToBlock must align to 1000. For toBlock+1 already aligned (e.g.
// toBlock=2,912,999 → 2,913,000), returns 2,913,000.
//
// For non-aligned toBlock+1 (e.g. toBlock=2,912,500 → 2,912,501),
// returns 2,912,000 — the leftover [2,912,000, toBlock] would need
// to be seeded into the writable DB. This headers-rebuild commit
// does NOT implement that seed: non-1000-aligned mode-B targets
// against a block-snapshot straddle return an explicit error from
// the planner rather than silently producing inconsistent state.
// Follow-up: leftover-seed for headers + bodies + transactions.
func chunkAlignedToBlock(toBlock uint64) uint64 {
	next := toBlock + 1
	return next - (next % uint64(snaptype.Erigon2MinSegmentSize))
}

// headersStraddleFile finds the single headers .seg file whose range
// straddles toBlock (FromBlock ≤ toBlock < ToBlock) by scanning the
// provider's inventory. Returns nil + nil if no straddle file exists
// (the inventory may have already trimmed the file in a prior step,
// or toBlock falls on a file boundary so no file straddles).
func (p *Provider) headersStraddleFile(toBlock uint64) (*snaptype.FileInfo, error) {
	if p.Inventory == nil {
		return nil, nil
	}
	for _, e := range p.Inventory.BlockFiles() {
		if e.FromBlock > toBlock || e.ToBlock <= toBlock {
			continue
		}
		// Straddle candidate. Filter to headers only (this commit's scope).
		info, _, ok := snaptype.ParseFileName(p.snapDir, e.Name)
		if !ok {
			continue
		}
		if info.Type == nil || info.Type.Enum() != snaptype2.Enums.Headers {
			continue
		}
		return &info, nil
	}
	return nil, nil
}

// sliceStraddleSeg writes the first (newToBlock - oldFI.From) entries
// from the old straddle .seg into a new .seg at newFI.Path. The new
// file's wire format matches what a fresh dumpRange at this range
// would produce (uncompressed when range < Erigon2MergeLimit-1).
//
// Returns the new FileInfo and the entry count. Does NOT build the
// .idx accessor — callers use buildStraddleAccessor for that. The
// split keeps the .seg slice testable without needing real headers /
// chain config / salt — the IndexBuilderFunc decodes entries.
func sliceStraddleSeg(ctx context.Context, oldFI snaptype.FileInfo, newToBlock uint64, snapDir, tmpDir string, logger log.Logger) (snaptype.FileInfo, uint64, error) {
	if oldFI.Type == nil {
		return snaptype.FileInfo{}, 0, fmt.Errorf("sliceStraddleSeg: oldFI has nil Type")
	}
	if newToBlock <= oldFI.From || newToBlock >= oldFI.To {
		return snaptype.FileInfo{}, 0, fmt.Errorf("sliceStraddleSeg: newToBlock=%d outside straddle (%d, %d)", newToBlock, oldFI.From, oldFI.To)
	}
	if newToBlock%uint64(snaptype.Erigon2MinSegmentSize) != 0 {
		return snaptype.FileInfo{}, 0, fmt.Errorf("sliceStraddleSeg: newToBlock=%d not aligned to %d (block snapshot file naming requires 1000-block alignment)", newToBlock, snaptype.Erigon2MinSegmentSize)
	}

	newFI := oldFI
	newFI.To = newToBlock
	newFI = newFI.As(oldFI.Type)

	oldPath := filepath.Join(snapDir, oldFI.Name())
	dec, err := seg.NewDecompressor(oldPath)
	if err != nil {
		return snaptype.FileInfo{}, 0, fmt.Errorf("open old %s: %w", oldPath, err)
	}
	defer dec.Close()
	defer dec.MadvSequential().DisableReadAhead()

	compressCfg := freezeblocks.BlockCompressCfg
	c, err := seg.NewCompressor(ctx, "mode-B straddle rebuild", newFI.Path, tmpDir, compressCfg, log.LvlInfo, logger)
	if err != nil {
		return snaptype.FileInfo{}, 0, fmt.Errorf("create new %s: %w", newFI.Path, err)
	}
	defer c.Close()

	noCompress := (newFI.To - newFI.From) < (snaptype.Erigon2MergeLimit - 1)
	g := dec.MakeGetter()
	keep := newFI.To - newFI.From
	var buf []byte
	var written uint64
	for i := uint64(0); i < keep; i++ {
		if !g.HasNext() {
			return snaptype.FileInfo{}, 0, fmt.Errorf("rebuild %s: source ran out at entry %d (wanted %d)", oldFI.Name(), i, keep)
		}
		buf, _ = g.Next(buf[:0])
		if noCompress {
			if err := c.AddUncompressedWord(buf); err != nil {
				return snaptype.FileInfo{}, 0, fmt.Errorf("AddUncompressedWord(%s, entry %d): %w", newFI.Name(), i, err)
			}
		} else {
			if err := c.AddWord(buf); err != nil {
				return snaptype.FileInfo{}, 0, fmt.Errorf("AddWord(%s, entry %d): %w", newFI.Name(), i, err)
			}
		}
		written++
	}
	if err := c.Compress(); err != nil {
		return snaptype.FileInfo{}, 0, fmt.Errorf("Compress(%s): %w", newFI.Name(), err)
	}
	return newFI, written, nil
}

// buildStraddleAccessor drives the new file's per-type IndexBuilderFunc
// to produce the .idx accessor at the matching path. The salt is read
// from newFI.Dir() by SnapType.BuildIndexes; chainCfg is forwarded but
// unused by the headers builder (the headers IndexBuilderFunc
// recomputes header.Hash() from the RLP it iterates).
func buildStraddleAccessor(ctx context.Context, newFI snaptype.FileInfo, chainCfg *chain.Config, tmpDir string, logger log.Logger) error {
	prog := &background.Progress{}
	if err := newFI.Type.BuildIndexes(ctx, newFI, nil, chainCfg, tmpDir, prog, log.LvlInfo, logger); err != nil {
		return fmt.Errorf("BuildIndexes(%s): %w", newFI.Name(), err)
	}
	return nil
}

// rebuildHeadersStraddleFile writes a new headers .seg covering
// [oldFI.From, newToBlock) from the first (newToBlock - oldFI.From)
// entries of the old file's content, then drives the headers
// IndexBuilderFunc to produce the matching .idx accessor.
//
// Pre-conditions:
//   - oldFI must be Headers (snaptype2.Enums.Headers)
//   - oldFI.From ≤ newToBlock < oldFI.To (strict straddle)
//   - newToBlock is a multiple of Erigon2MinSegmentSize (1000)
//
// Returns the new file's snaptype.FileInfo. The new file is written
// at its FINAL path (not a .tmp suffix) — atomicity with the mode-B
// tx is provided by the caller staging the new path for cleanup in
// AbortUnwind and the old path for deletion in FinalizeUnwind.
func rebuildHeadersStraddleFile(ctx context.Context, oldFI snaptype.FileInfo, newToBlock uint64, snapDir, tmpDir string, chainCfg *chain.Config, logger log.Logger) (snaptype.FileInfo, error) {
	if oldFI.Type == nil {
		return snaptype.FileInfo{}, fmt.Errorf("rebuildHeadersStraddleFile: oldFI has nil Type")
	}
	if oldFI.Type.Enum() != snaptype2.Enums.Headers {
		return snaptype.FileInfo{}, fmt.Errorf("rebuildHeadersStraddleFile: oldFI is not Headers (got %s)", oldFI.Type.Name())
	}
	newFI, _, err := sliceStraddleSeg(ctx, oldFI, newToBlock, snapDir, tmpDir, logger)
	if err != nil {
		return snaptype.FileInfo{}, err
	}
	if err := buildStraddleAccessor(ctx, newFI, chainCfg, tmpDir, logger); err != nil {
		return snaptype.FileInfo{}, err
	}
	return newFI, nil
}

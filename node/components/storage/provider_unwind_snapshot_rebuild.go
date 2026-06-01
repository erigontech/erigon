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
	"github.com/erigontech/erigon/db/version"
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

// straddleBlockFileForType finds the single block-snapshot .seg file
// of the given snaptype whose range straddles toBlock (FromBlock ≤
// toBlock < ToBlock) by scanning the provider's inventory. Returns
// nil + nil if no straddle file exists (target lands on a file
// boundary, or the file was already trimmed in a prior step).
//
// Headers / Bodies / Transactions are produced together by RetireBlocks
// at matching ranges, so a straddle in one implies a straddle in the
// other two — but the search runs per-type to keep the function
// straightforward and to tolerate any edge-case asymmetry from
// historical retire boundaries.
func (p *Provider) straddleBlockFileForType(toBlock uint64, typeEnum snaptype.Enum) (*snaptype.FileInfo, error) {
	if p.Inventory == nil {
		return nil, nil
	}
	for _, e := range p.Inventory.BlockFiles() {
		if e.FromBlock > toBlock || e.ToBlock <= toBlock {
			continue
		}
		info, _, ok := snaptype.ParseFileName(p.snapDir, e.Name)
		if !ok {
			continue
		}
		if info.Type == nil || info.Type.Enum() != typeEnum {
			continue
		}
		return &info, nil
	}
	return nil, nil
}

// headersStraddleFile is a thin convenience wrapper kept for the
// initial headers-rebuild commit's call site clarity.
func (p *Provider) headersStraddleFile(toBlock uint64) (*snaptype.FileInfo, error) {
	return p.straddleBlockFileForType(toBlock, snaptype2.Enums.Headers)
}

// rebuildBlockStraddles is the per-type-orchestrator for mode-B's
// straddle rebuilds. For each of Headers, Bodies, Transactions (in
// that order — Transactions reads the rebuilt bodies file), find the
// straddle file in the inventory and rebuild it to cover [FromBlock,
// newToBlock). Returns:
//
//   - rebuildPaths: every new file written (.seg + .idx + extra
//     accessors), for AbortUnwind cleanup if the mode-B tx rolls back.
//   - toRemoveStraddles: every old straddle file + its accessors,
//     appended to the caller's `toRemove` list for FinalizeUnwind
//     deletion post-commit.
//
// Non-1000-aligned toBlock returns an error: the leftover blocks
// [newToBlock, toBlock] would need to be seeded into the writable
// DB, which is a known follow-up.
func (p *Provider) rebuildBlockStraddles(ctx context.Context, toBlock, newToBlock uint64) (rebuildPaths []string, toRemoveStraddles []*storageSnapshotFileRef, err error) {
	// Rebuild order: Headers → Bodies → Transactions. Transactions'
	// IndexBuilderFunc reads the bodies file at the same range, so
	// the rebuilt bodies file must exist when transactions is rebuilt.
	specs := []struct {
		name    string
		enum    snaptype.Enum
		rebuild func(ctx context.Context, oldFI snaptype.FileInfo, newTo uint64, snapDir, tmpDir string, chainCfg *chain.Config, logger log.Logger) (snaptype.FileInfo, error)
	}{
		{"headers", snaptype2.Enums.Headers, rebuildHeadersStraddleFile},
		{"bodies", snaptype2.Enums.Bodies, rebuildBodiesStraddleFile},
		{"transactions", snaptype2.Enums.Transactions, rebuildTransactionsStraddleFile},
	}
	for _, s := range specs {
		straddle, ferr := p.straddleBlockFileForType(toBlock, s.enum)
		if ferr != nil {
			return nil, nil, fmt.Errorf("straddleBlockFileForType(%s, %d): %w", s.name, toBlock, ferr)
		}
		if straddle == nil {
			continue
		}
		// Alignment check applies only when there's an actual
		// straddle to rebuild. Without a block-snapshot file at this
		// range, mode-B doesn't touch block files and toBlock can
		// land at any value.
		if newToBlock != toBlock+1 {
			return nil, nil, fmt.Errorf("mode-B %s-straddle rebuild: toBlock+1=%d not aligned to %d-block boundary; leftover-seed for non-aligned cuts is a known follow-up", s.name, toBlock+1, snaptype.Erigon2MinSegmentSize)
		}
		if newToBlock <= straddle.From {
			// "Straddle" actually starts past toBlock — caller's
			// strictly-past collector handles it.
			continue
		}
		newFI, rerr := s.rebuild(ctx, *straddle, newToBlock, p.snapDir, p.snapTmpDir, p.ChainConfig, p.logger)
		if rerr != nil {
			return nil, nil, fmt.Errorf("rebuild %s straddle (%s → newTo=%d): %w", s.name, straddle.Name(), newToBlock, rerr)
		}
		rebuildPaths = append(rebuildPaths, newFI.Path)
		for _, idxName := range newFI.Type.IdxFileNames(newFI.From, newFI.To) {
			rebuildPaths = append(rebuildPaths, filepath.Join(newFI.Dir(), idxName))
		}
		toRemoveStraddles = append(toRemoveStraddles, &storageSnapshotFileRef{Name: straddle.Name()})
		for _, idxName := range straddle.Type.IdxFileNames(straddle.From, straddle.To) {
			toRemoveStraddles = append(toRemoveStraddles, &storageSnapshotFileRef{Name: idxName})
		}
	}
	return rebuildPaths, toRemoveStraddles, nil
}

// storageSnapshotFileRef is a minimal value the rebuildBlockStraddles
// orchestrator returns to the trim path. The trim path's caller wraps
// these as snapshot.FileEntry for the existing pendingTrim machinery.
// Avoids importing the snapshot package here.
type storageSnapshotFileRef struct {
	Name string
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

// rebuildBodiesStraddleFile mirrors rebuildHeadersStraddleFile for the
// bodies .seg. Bodies are 1-entry-per-block (each entry is the RLP-
// encoded BodyForStorage) so the same seg-slice primitive applies.
// The bodies IndexBuilderFunc indexes by block-number-varint → offset
// and doesn't decode entries, so the rebuilt file's accessor is
// straightforward.
//
// Reads of bodies past toBlock were already gated by canonical-num
// truncation in unwindDBPastBlock (no observable leak via hash); this
// rebuild eliminates the stale on-disk data so the snapshot view's
// reported frozenBlocks tip matches the canonical chain after mode B.
func rebuildBodiesStraddleFile(ctx context.Context, oldFI snaptype.FileInfo, newToBlock uint64, snapDir, tmpDir string, chainCfg *chain.Config, logger log.Logger) (snaptype.FileInfo, error) {
	if oldFI.Type == nil {
		return snaptype.FileInfo{}, fmt.Errorf("rebuildBodiesStraddleFile: oldFI has nil Type")
	}
	if oldFI.Type.Enum() != snaptype2.Enums.Bodies {
		return snaptype.FileInfo{}, fmt.Errorf("rebuildBodiesStraddleFile: oldFI is not Bodies (got %s)", oldFI.Type.Name())
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

// rebuildTransactionsStraddleFile is the harder of the three rebuilds:
// transactions .seg entries are VARIABLE per block (each block has
// 0..N user txs + 2 system txs). The number of entries to keep for
// the rebuilt [oldFI.From, newToBlock) range can only be computed
// from the BODIES file at that same new range — each body's
// BaseTxnID + TxCount gives the per-block tx span.
//
// Pre-condition: the bodies file at the new range MUST already exist
// on disk before this is called. The caller (the snapshot-trim
// orchestrator) rebuilds bodies first, so by the time we reach the
// transactions rebuild the new bodies file is in place.
//
// The Transactions IndexBuilderFunc reads the bodies file at the
// same range via version-mask pattern (see snaptype2/block_types.go
// Transactions registration), so the rebuilt bodies file is
// automatically picked up by BuildIndexes.
//
// Closes the eth_getTransactionByHash leak — without this, a tx hash
// recorded against a now-unwound block would still resolve to its
// stale entry via the .seg.idx hash→offset accessor.
func rebuildTransactionsStraddleFile(ctx context.Context, oldFI snaptype.FileInfo, newToBlock uint64, snapDir, tmpDir string, chainCfg *chain.Config, logger log.Logger) (snaptype.FileInfo, error) {
	if oldFI.Type == nil {
		return snaptype.FileInfo{}, fmt.Errorf("rebuildTransactionsStraddleFile: oldFI has nil Type")
	}
	if oldFI.Type.Enum() != snaptype2.Enums.Transactions {
		return snaptype.FileInfo{}, fmt.Errorf("rebuildTransactionsStraddleFile: oldFI is not Transactions (got %s)", oldFI.Type.Name())
	}
	if newToBlock <= oldFI.From || newToBlock >= oldFI.To {
		return snaptype.FileInfo{}, fmt.Errorf("rebuildTransactionsStraddleFile: newToBlock=%d outside straddle (%d, %d)", newToBlock, oldFI.From, oldFI.To)
	}
	if newToBlock%uint64(snaptype.Erigon2MinSegmentSize) != 0 {
		return snaptype.FileInfo{}, fmt.Errorf("rebuildTransactionsStraddleFile: newToBlock=%d not aligned to %d", newToBlock, snaptype.Erigon2MinSegmentSize)
	}

	// Locate the rebuilt bodies file at the matching new range. The
	// caller MUST have already produced it.
	newFI := oldFI
	newFI.To = newToBlock
	newFI = newFI.As(oldFI.Type)

	bodiesAtNewRange := newFI.As(snaptype2.Bodies)
	bodiesPathPattern, err := version.ReplaceVersionWithMask(bodiesAtNewRange.Path)
	if err != nil {
		return snaptype.FileInfo{}, fmt.Errorf("compute bodies pattern for tx rebuild: %w", err)
	}
	bodiesPath, _, ok, err := version.FindFilesWithVersionsByPattern(bodiesPathPattern)
	if err != nil {
		return snaptype.FileInfo{}, fmt.Errorf("find rebuilt bodies file for tx rebuild: %w", err)
	}
	if !ok {
		return snaptype.FileInfo{}, fmt.Errorf("rebuildTransactionsStraddleFile: rebuilt bodies file not found at %s — bodies must be rebuilt before transactions", bodiesAtNewRange.Path)
	}

	// Compute the count of tx entries to keep by reading the rebuilt
	// bodies' BaseTxnID + TxCount fields. Mirrors what the
	// Transactions IndexBuilderFunc does for verification.
	bodiesDec, err := seg.NewDecompressor(bodiesPath)
	if err != nil {
		return snaptype.FileInfo{}, fmt.Errorf("open rebuilt bodies %s: %w", bodiesPath, err)
	}
	defer bodiesDec.Close()
	// TxsAmountBasedOnBodiesSnapshots wants `len` = (block-count - 1)
	// because it iterates until i == len-1 to read the LAST body.
	blockCount := newFI.To - newFI.From
	if blockCount == 0 {
		return snaptype.FileInfo{}, fmt.Errorf("rebuildTransactionsStraddleFile: empty rebuilt range")
	}
	_, expectedCount, err := snaptype2.TxsAmountBasedOnBodiesSnapshots(bodiesDec, blockCount-1)
	if err != nil {
		return snaptype.FileInfo{}, fmt.Errorf("compute tx-entry count from rebuilt bodies: %w", err)
	}
	if expectedCount < 0 {
		return snaptype.FileInfo{}, fmt.Errorf("rebuildTransactionsStraddleFile: expectedCount=%d (negative)", expectedCount)
	}

	// Slice the old transactions .seg to the first `expectedCount`
	// entries (NOT (newTo - From) — entries are per-tx, not per-block).
	oldPath := filepath.Join(snapDir, oldFI.Name())
	dec, err := seg.NewDecompressor(oldPath)
	if err != nil {
		return snaptype.FileInfo{}, fmt.Errorf("open old tx %s: %w", oldPath, err)
	}
	defer dec.Close()
	defer dec.MadvSequential().DisableReadAhead()

	compressCfg := freezeblocks.BlockCompressCfg
	c, err := seg.NewCompressor(ctx, "mode-B tx-straddle rebuild", newFI.Path, tmpDir, compressCfg, log.LvlInfo, logger)
	if err != nil {
		return snaptype.FileInfo{}, fmt.Errorf("create new tx %s: %w", newFI.Path, err)
	}
	defer c.Close()

	noCompress := (newFI.To - newFI.From) < (snaptype.Erigon2MergeLimit - 1)
	g := dec.MakeGetter()
	var buf []byte
	for i := 0; i < expectedCount; i++ {
		if !g.HasNext() {
			return snaptype.FileInfo{}, fmt.Errorf("tx rebuild %s: source ran out at entry %d (wanted %d)", oldFI.Name(), i, expectedCount)
		}
		buf, _ = g.Next(buf[:0])
		if noCompress {
			if err := c.AddUncompressedWord(buf); err != nil {
				return snaptype.FileInfo{}, fmt.Errorf("AddUncompressedWord(%s, entry %d): %w", newFI.Name(), i, err)
			}
		} else {
			if err := c.AddWord(buf); err != nil {
				return snaptype.FileInfo{}, fmt.Errorf("AddWord(%s, entry %d): %w", newFI.Name(), i, err)
			}
		}
	}
	if err := c.Compress(); err != nil {
		return snaptype.FileInfo{}, fmt.Errorf("Compress(%s): %w", newFI.Name(), err)
	}

	// Build both transactions accessors (TxnHash + TxnHash2BlockNum)
	// via the registered IndexBuilderFunc. It reads the rebuilt
	// bodies file at the same range to recompute BaseTxnID for the
	// keyspace.
	if err := buildStraddleAccessor(ctx, newFI, chainCfg, tmpDir, logger); err != nil {
		return snaptype.FileInfo{}, err
	}
	return newFI, nil
}

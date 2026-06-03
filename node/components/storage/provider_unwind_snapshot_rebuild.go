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
	"encoding/binary"
	"fmt"
	"path/filepath"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

// chunkAlignedToBlock returns the largest 1000-block-aligned value
// ≤ (toBlock+1). Block snapshot files are named in 1000-block units
// (see snaptype.FileInfo.As), so a rebuilt straddle file's new
// ToBlock must align to 1000. For toBlock+1 already aligned (e.g.
// toBlock=2,912,999 → 2,913,000), returns 2,913,000.
//
// For non-aligned toBlock+1 (e.g. toBlock=2,912,500 → 2,912,501),
// returns 2,912,000. The leftover [2,912,000, toBlock] is seeded
// into the writable DB by seedLeftoverBlocks so canonical reads for
// those blocks resolve via the DB path post-mode-B.
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
// Non-1000-aligned toBlock: after the per-type rebuild, the leftover
// blocks [newToBlock, toBlock] are seeded into the writable DB by
// seedLeftoverBlocks (reads the OLD straddle files still on disk
// pre-FinalizeUnwind, writes headers / bodies / tx + senders for
// each leftover block). Requires headers + bodies + transactions
// straddles to be present as a matching triple.
func (p *Provider) rebuildBlockStraddles(ctx context.Context, tx kv.RwTx, toBlock, newToBlock uint64) (rebuildPaths []string, toRemoveStraddles []*storageSnapshotFileRef, err error) {
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

	// Capture the straddle FileInfos for each type before any
	// rebuild runs — seedLeftoverBlocks (the non-aligned-cut path)
	// reads ALL THREE old files in lockstep, so it needs the
	// pre-rebuild names of each.
	straddles := make(map[snaptype.Enum]*snaptype.FileInfo, len(specs))
	for _, s := range specs {
		fi, ferr := p.straddleBlockFileForType(toBlock, s.enum)
		if ferr != nil {
			return nil, nil, fmt.Errorf("straddleBlockFileForType(%s, %d): %w", s.name, toBlock, ferr)
		}
		straddles[s.enum] = fi
	}

	for _, s := range specs {
		straddle := straddles[s.enum]
		if straddle == nil {
			continue
		}
		if newToBlock <= straddle.From {
			// Aligned newTo lands at or below the straddle's lower
			// bound — the would-be rebuilt range [From, newTo) is
			// empty (or negative). Don't rebuild; trim the file
			// entirely. seedLeftoverBlocks will seed
			// [newToBlock = straddle.From, toBlock] into the writable
			// DB from the OLD file (still on disk until
			// FinalizeUnwind). The strictly-past collector misses
			// this file because FromBlock ≤ toBlock — it's the
			// straddle, not strictly past — so without queueing it
			// here the stale .seg stays on disk and the catch-up
			// downloader walks into blocks past the new head whose
			// TD was wiped (live-rig 2026-06-02 wedge).
			toRemoveStraddles = append(toRemoveStraddles, &storageSnapshotFileRef{Name: straddle.Name()})
			for _, idxName := range straddle.Type.IdxFileNames(straddle.From, straddle.To) {
				toRemoveStraddles = append(toRemoveStraddles, &storageSnapshotFileRef{Name: idxName})
			}
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

	// Non-aligned-cut leftover seed. When toBlock+1 isn't a 1000-
	// multiple, newToBlock = chunkAlignedToBlock(toBlock) < toBlock+1.
	// The rebuilt block-snapshot files cover [oldFI.From, newToBlock);
	// blocks in [newToBlock, toBlock] have NO snapshot source after
	// FinalizeUnwind deletes the old straddle files. seedLeftoverBlocks
	// walks the OLD files (still on disk; deletion is staged for
	// post-commit) and writes the leftover headers / bodies / tx data
	// + senders into the writable DB so canonical reads for those
	// blocks resolve via the DB path.
	//
	// Requires all three straddle types (headers + bodies + transactions)
	// to be present in the inventory at the same range — otherwise
	// the tx file's per-block tx-range cannot be reconstructed in
	// lockstep. In practice block snapshots are produced as a
	// matching triple by RetireBlocks; an asymmetric inventory is a
	// hard-error.
	if newToBlock < toBlock+1 && len(toRemoveStraddles) > 0 {
		// At least one block-snapshot straddle was rebuilt. The
		// non-aligned leftover [newToBlock, toBlock] now has no
		// snapshot source for those types. Seed them into the
		// writable DB.
		//
		// Require the full triple (headers + bodies + tx) because
		// the tx file's per-block tx-range is reconstructed in
		// lockstep with the bodies file at the same range. Block
		// snapshots are produced as a matching triple by
		// RetireBlocks; an asymmetric inventory is a hard-error.
		hFI := straddles[snaptype2.Enums.Headers]
		bFI := straddles[snaptype2.Enums.Bodies]
		tFI := straddles[snaptype2.Enums.Transactions]
		if hFI == nil || bFI == nil || tFI == nil {
			return nil, nil, fmt.Errorf("mode-B non-aligned cut at toBlock=%d: leftover seed requires headers + bodies + transactions straddle triple; got headers=%v bodies=%v transactions=%v", toBlock, hFI != nil, bFI != nil, tFI != nil)
		}
		if err := seedLeftoverBlocks(ctx, tx, p.snapDir, *hFI, *bFI, *tFI, newToBlock, toBlock); err != nil {
			return nil, nil, fmt.Errorf("seedLeftoverBlocks([%d, %d]): %w", newToBlock, toBlock, err)
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

// seedLeftoverBlocks writes block-data for [fromBlock, toBlockInclusive]
// into the writable DB by reading entries from the OLD straddle files
// still on disk. Used by mode-B's non-aligned-cut path: when toBlock+1
// isn't a 1000-multiple, the rebuilt block-snapshot files cover only
// up to chunkAlignedToBlock(toBlock) (the nearest 1000-boundary), and
// blocks in [chunkAlignedToBlock(toBlock), toBlock] must live in the
// writable DB instead.
//
// Walks the THREE old files (headers + bodies + transactions) in
// lockstep:
//   - Skip the first (fromBlock - oldFI.From) entries in headers + bodies
//   - From `fromBlock` onward, for each block N ≤ toBlockInclusive:
//   - Header: write the raw header RLP to kv.Headers + kv.HeaderNumber
//     (via rawdb.WriteHeaderRaw)
//   - Body: decode BodyForStorage, write to kv.BlockBody
//     (via rawdb.WriteBodyForStorage)
//   - Transactions: for each tx in [BaseTxnID, BaseTxnID + TxCount),
//     extract sender + tx_rlp from the tx file entry, write tx_rlp
//     to kv.EthTx and accumulate sender bytes for the block's
//     kv.Senders entry
//
// Pre-condition: the OLD straddle files must still be on disk when
// this runs. The caller (rebuildBlockStraddles) calls seedLeftoverBlocks
// AFTER the rebuild has written new files (which have different
// names — straddle was 002910-002920, rebuilt is 002910-002912) but
// BEFORE FinalizeUnwind deletes the old files. Same-tx writes ensure
// atomicity with the rest of mode-B.
//
// fromBlock + toBlockInclusive must lie within the headers straddle
// file's range [oldHeadersFI.From, oldHeadersFI.To). Caller validates.
func seedLeftoverBlocks(ctx context.Context, tx kv.RwTx, snapDir string, oldHeadersFI, oldBodiesFI, oldTxFI snaptype.FileInfo, fromBlock, toBlockInclusive uint64) error {
	if fromBlock > toBlockInclusive {
		return nil
	}
	if fromBlock < oldHeadersFI.From || toBlockInclusive >= oldHeadersFI.To {
		return fmt.Errorf("seedLeftoverBlocks: range [%d, %d] outside headers file [%d, %d)", fromBlock, toBlockInclusive, oldHeadersFI.From, oldHeadersFI.To)
	}
	if oldBodiesFI.From != oldHeadersFI.From || oldBodiesFI.To != oldHeadersFI.To {
		return fmt.Errorf("seedLeftoverBlocks: bodies range [%d, %d) does not match headers [%d, %d)", oldBodiesFI.From, oldBodiesFI.To, oldHeadersFI.From, oldHeadersFI.To)
	}
	if oldTxFI.From != oldHeadersFI.From || oldTxFI.To != oldHeadersFI.To {
		return fmt.Errorf("seedLeftoverBlocks: tx range [%d, %d) does not match headers [%d, %d)", oldTxFI.From, oldTxFI.To, oldHeadersFI.From, oldHeadersFI.To)
	}

	hPath := filepath.Join(snapDir, oldHeadersFI.Name())
	hdec, err := seg.NewDecompressor(hPath)
	if err != nil {
		return fmt.Errorf("open old headers %s: %w", hPath, err)
	}
	defer hdec.Close()

	bPath := filepath.Join(snapDir, oldBodiesFI.Name())
	bdec, err := seg.NewDecompressor(bPath)
	if err != nil {
		return fmt.Errorf("open old bodies %s: %w", bPath, err)
	}
	defer bdec.Close()

	tPath := filepath.Join(snapDir, oldTxFI.Name())
	tdec, err := seg.NewDecompressor(tPath)
	if err != nil {
		return fmt.Errorf("open old tx %s: %w", tPath, err)
	}
	defer tdec.Close()

	hg := hdec.MakeGetter()
	bg := bdec.MakeGetter()
	tg := tdec.MakeGetter()

	// Walk headers + bodies sequentially from oldHeadersFI.From. We
	// must iterate over EVERY entry in [oldHeadersFI.From, fromBlock)
	// to advance the tx getter to the right starting position (tx
	// entries aren't per-block, so we can't index-jump). For blocks
	// in [oldHeadersFI.From, fromBlock) we read bodies (need TxCount
	// to advance tx getter) but don't write to DB. For blocks in
	// [fromBlock, toBlockInclusive] we read all three + write.
	var hBuf, bBuf, tBuf []byte
	for n := oldHeadersFI.From; n <= toBlockInclusive; n++ {
		if !hg.HasNext() {
			return fmt.Errorf("seedLeftoverBlocks: headers source ran out at block %d", n)
		}
		hBuf, _ = hg.Next(hBuf[:0])
		if !bg.HasNext() {
			return fmt.Errorf("seedLeftoverBlocks: bodies source ran out at block %d", n)
		}
		bBuf, _ = bg.Next(bBuf[:0])

		body := new(types.BodyForStorage)
		if err := rlp.DecodeBytes(bBuf, body); err != nil {
			return fmt.Errorf("seedLeftoverBlocks: decode body at block %d: %w", n, err)
		}

		writeThisBlock := n >= fromBlock

		// Resolve canonical hash from writable DB (preserved across
		// mode-B's CanonicalHash truncation — entries ≤ toBlock survive).
		var hash common.Hash
		if writeThisBlock {
			h, err := rawdb.ReadCanonicalHash(tx, n)
			if err != nil {
				return fmt.Errorf("seedLeftoverBlocks: ReadCanonicalHash(%d): %w", n, err)
			}
			if h == (common.Hash{}) {
				return fmt.Errorf("seedLeftoverBlocks: no canonical hash for block %d (writable DB truncated below the seed range?)", n)
			}
			hash = h

			// Header word format = 1-byte sort-prefix + header RLP.
			// rawdb.WriteHeaderRaw writes kv.Headers + kv.HeaderNumber
			// (the hash→num index).
			if len(hBuf) < 1 {
				return fmt.Errorf("seedLeftoverBlocks: empty header entry at block %d", n)
			}
			if err := rawdb.WriteHeaderRaw(tx, n, hash, hBuf[1:], false /* skipIndexing */); err != nil {
				return fmt.Errorf("seedLeftoverBlocks: WriteHeaderRaw(%d): %w", n, err)
			}

			// Body: write the BodyForStorage RLP directly to kv.BlockBody.
			if err := rawdb.WriteBodyForStorage(tx, hash, n, body); err != nil {
				return fmt.Errorf("seedLeftoverBlocks: WriteBodyForStorage(%d): %w", n, err)
			}
		}

		// Walk this block's tx entries. txCount entries per block, written
		// by freezeblocks.DumpTxs in this order:
		//   - 1 entry for the first system tx (BaseTxnID). Empty if the
		//     source row in kv.EthTx was absent (DumpTxs calls collect(nil));
		//     otherwise has the user-tx wire format.
		//   - txCount-2 user txs. Always non-empty; wire format
		//     hashByte(1) + sender(20) + tx_rlp.
		//   - 1 entry for the last system tx (LastSystemTx). Same empty-or-full
		//     convention as the first.
		//
		// kv.Senders for a block is a packed slice of 20-byte sender
		// addresses with one entry per USER tx (system txs aren't
		// included). We mirror that by only appending sender bytes for
		// entries inside the [1, txCount-1) user range.
		//
		// Even when !writeThisBlock we MUST advance the tx getter to
		// keep the lockstep position in sync.
		txCount := uint64(body.TxCount)
		var sendersBuf []byte
		if writeThisBlock && txCount > 2 {
			sendersBuf = make([]byte, 0, (txCount-2)*20)
		}
		txnID := body.BaseTxnID.U64()
		for i := uint64(0); i < txCount; i++ {
			if !tg.HasNext() {
				return fmt.Errorf("seedLeftoverBlocks: tx source ran out at block %d tx %d/%d", n, i, txCount)
			}
			tBuf, _ = tg.Next(tBuf[:0])
			if !writeThisBlock {
				txnID++
				continue
			}
			isSystemSlot := i == 0 || i == txCount-1
			switch {
			case len(tBuf) == 0:
				// Empty entry — only legal for system tx slots
				// (DumpTxs writes nil when the source kv.EthTx row
				// was absent). For user-tx positions this would be
				// a malformed file.
				if !isSystemSlot {
					return fmt.Errorf("seedLeftoverBlocks: empty tx entry at block %d tx %d (user position; only system slots may be empty)", n, i)
				}
				// Nothing to write — kv.EthTx had no row for this
				// txnID at retire time, so leaving it absent now
				// matches that.
			case len(tBuf) < 21:
				return fmt.Errorf("seedLeftoverBlocks: tx entry at block %d tx %d shorter than sender prefix (len=%d)", n, i, len(tBuf))
			default:
				// tBuf = hashByte(1) + sender(20) + tx_rlp.
				if !isSystemSlot {
					sendersBuf = append(sendersBuf, tBuf[1:21]...)
				}
				var txIDBytes [8]byte
				binary.BigEndian.PutUint64(txIDBytes[:], txnID)
				if err := tx.Put(kv.EthTx, txIDBytes[:], tBuf[21:]); err != nil {
					return fmt.Errorf("seedLeftoverBlocks: write EthTx[txnID=%d] (block %d): %w", txnID, n, err)
				}
			}
			txnID++
		}
		if writeThisBlock && len(sendersBuf) > 0 {
			// kv.Senders key = block_num_u64 + hash (40 bytes).
			senderKey := make([]byte, 8+32)
			binary.BigEndian.PutUint64(senderKey[:8], n)
			copy(senderKey[8:], hash[:])
			if err := tx.Put(kv.Senders, senderKey, sendersBuf); err != nil {
				return fmt.Errorf("seedLeftoverBlocks: write Senders(%d): %w", n, err)
			}
		}
	}
	return nil
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

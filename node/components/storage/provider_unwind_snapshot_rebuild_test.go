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
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

// TestChunkAlignedToBlock pins the 1000-boundary rounding semantics.
func TestChunkAlignedToBlock(t *testing.T) {
	t.Parallel()
	require.Equal(t, uint64(2_913_000), chunkAlignedToBlock(2_912_999), "toBlock+1 already a 1000-boundary returns toBlock+1")
	require.Equal(t, uint64(2_912_000), chunkAlignedToBlock(2_912_500), "non-aligned toBlock+1 returns previous 1000-boundary")
	require.Equal(t, uint64(2_912_000), chunkAlignedToBlock(2_912_000), "toBlock that is itself a 1000-multiple → next is 1 past → returns prior boundary")
	require.Equal(t, uint64(0), chunkAlignedToBlock(998), "toBlock+1 < 1000 returns 0 (no aligned cut available)")
	require.Equal(t, uint64(1_000), chunkAlignedToBlock(999), "toBlock=999 → toBlock+1=1000 → 1000-aligned")
}

// makeFakeSegFile writes a .seg with `count` uncompressed words at
// `path`. Used to fabricate straddle-file fixtures in unit tests. The
// content of each word is opaque — sliceStraddleSeg copies words
// by position, not by content, so the IndexBuilderFunc isn't
// exercised here (it would need real header RLP).
func makeFakeSegFile(t *testing.T, ctx context.Context, path, tmpDir string, count int) {
	t.Helper()
	cfg := seg.DefaultCfg
	c, err := seg.NewCompressor(ctx, "test-fake-seg", path, tmpDir, cfg, log.LvlError, log.New())
	require.NoError(t, err)
	defer c.Close()
	for i := 0; i < count; i++ {
		// Word content: ASCII "block-NNNNN". Differentiates positions
		// for sequential read verification.
		word := fmt.Appendf(nil, "block-%05d", i)
		require.NoError(t, c.AddUncompressedWord(word))
	}
	require.NoError(t, c.Compress())
}

// TestSliceStraddleSeg_TruncatesEntries pins the core slice semantic:
// after sliceStraddleSeg the new .seg file holds exactly the first N
// entries from the source, where N = newToBlock - oldFI.From. The
// remaining entries (representing blocks past toBlock) are gone.
//
// Block-snapshot files have 1 entry per block (for headers, bodies,
// senders). Fixture: 10000-entry source representing a 10K-block
// straddle file. Truncates to 7000 entries (the 002000-002007 range
// after a mode-B target inside the 002000-002010 file).
func TestSliceStraddleSeg_TruncatesEntries(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	snapDir := t.TempDir()
	tmpDir := t.TempDir()

	// Fixture: 10K-entry headers .seg over block range
	// [2_000_000, 2_010_000). One entry per block.
	oldFI := snaptype.FileInfo{
		Version: snaptype2.Headers.Versions().Current,
		From:    2_000_000,
		To:      2_010_000,
		Type:    snaptype2.Headers,
		Ext:     ".seg",
	}
	oldFI = oldFI.As(snaptype2.Headers)
	oldFI.Path = filepath.Join(snapDir, oldFI.Name())
	makeFakeSegFile(t, ctx, oldFI.Path, tmpDir, 10_000)

	// Truncate to newTo = 2_007_000 (3000 blocks dropped from the tail).
	newFI, written, err := sliceStraddleSeg(ctx, oldFI, 2_007_000, snapDir, tmpDir, log.New())
	require.NoError(t, err)
	require.Equal(t, uint64(7_000), written, "must write exactly 7000 entries (newTo - From)")
	require.FileExists(t, newFI.Path)

	dec, err := seg.NewDecompressor(newFI.Path)
	require.NoError(t, err)
	defer dec.Close()
	require.EqualValues(t, 7_000, dec.Count())

	// Spot-check first, middle, and last entries match the source's
	// positions 0, 3500, 6999.
	g := dec.MakeGetter()
	var buf []byte
	pos := 0
	for g.HasNext() {
		buf, _ = g.Next(buf[:0])
		switch pos {
		case 0, 3_500, 6_999:
			require.Equal(t, fmt.Sprintf("block-%05d", pos), string(buf),
				"entry %d content must match source position", pos)
		}
		pos++
	}
	require.Equal(t, 7_000, pos, "read all 7000 entries; no more")
}

// makeBlockSnapshotTriple writes a matching triple of block-snapshot
// .seg files (headers + bodies + transactions) covering blocks
// [fromBlock, toBlock) into snapDir. The content is deterministic
// per block:
//   - Header word: 1-byte sort prefix + RLP-encoded synthetic header
//     whose Number = N and ParentHash = sha256-ish of N
//   - Body word: RLP-encoded BodyForStorage with BaseTxnID =
//     baseTxnID + cumulative tx count and TxCount = txCount (constant
//     per fixture for simplicity)
//   - Transaction words: txCount per block, each formatted as
//     1-byte prefix + 20-byte sender + 5-byte synthetic tx_rlp.
//     Total tx entries = blockCount * txCount.
//
// Returns the three FileInfo records pointing at the produced files.
//
// The fixture is structured so seedLeftoverBlocks can read every
// field it needs without needing a real chain. Used by the leftover-
// seed regression test.
func makeBlockSnapshotTriple(t *testing.T, ctx context.Context, snapDir, tmpDir string, fromBlock, toBlock uint64, baseTxnID uint64, txCount uint32) (h, b, tx snaptype.FileInfo) {
	t.Helper()
	hFI := snaptype.FileInfo{Version: snaptype2.Headers.Versions().Current, From: fromBlock, To: toBlock, Type: snaptype2.Headers, Ext: ".seg"}
	hFI = hFI.As(snaptype2.Headers)
	hFI.Path = filepath.Join(snapDir, hFI.Name())

	bFI := snaptype.FileInfo{Version: snaptype2.Bodies.Versions().Current, From: fromBlock, To: toBlock, Type: snaptype2.Bodies, Ext: ".seg"}
	bFI = bFI.As(snaptype2.Bodies)
	bFI.Path = filepath.Join(snapDir, bFI.Name())

	tFI := snaptype.FileInfo{Version: snaptype2.Transactions.Versions().Current, From: fromBlock, To: toBlock, Type: snaptype2.Transactions, Ext: ".seg"}
	tFI = tFI.As(snaptype2.Transactions)
	tFI.Path = filepath.Join(snapDir, tFI.Name())

	// Write headers.
	hc, err := seg.NewCompressor(ctx, "fixture-headers", hFI.Path, tmpDir, seg.DefaultCfg, log.LvlError, log.New())
	require.NoError(t, err)
	for n := fromBlock; n < toBlock; n++ {
		hdr := &types.Header{Number: *uint256.NewInt(n), Difficulty: *uint256.NewInt(1)}
		var parent common.Hash
		binary.BigEndian.PutUint64(parent[:8], n)
		hdr.ParentHash = parent
		hdrRlp, rerr := rlp.EncodeToBytes(hdr)
		require.NoError(t, rerr)
		word := append([]byte{0x42}, hdrRlp...) // 1-byte sort prefix + RLP
		require.NoError(t, hc.AddUncompressedWord(word))
	}
	require.NoError(t, hc.Compress())
	hc.Close()

	// Write bodies. BaseTxnID accumulates across blocks; TxCount
	// constant per fixture.
	bc, err := seg.NewCompressor(ctx, "fixture-bodies", bFI.Path, tmpDir, seg.DefaultCfg, log.LvlError, log.New())
	require.NoError(t, err)
	currentBase := baseTxnID
	for n := fromBlock; n < toBlock; n++ {
		body := &types.BodyForStorage{
			BaseTxnID: types.BaseTxnID(currentBase),
			TxCount:   txCount,
		}
		bodyRlp, rerr := rlp.EncodeToBytes(body)
		require.NoError(t, rerr)
		require.NoError(t, bc.AddUncompressedWord(bodyRlp))
		currentBase += uint64(txCount)
	}
	require.NoError(t, bc.Compress())
	bc.Close()

	// Write transactions. Real Erigon format per block from
	// freezeblocks.DumpTxs: txCount entries laid out as
	//   [system tx slot] + (txCount-2) user txs + [system tx slot]
	// System tx slots are empty bytes when the source kv.EthTx row was
	// absent at retire time (collect(nil) in DumpTxs). User txs have
	// 1-byte hashByte + 20-byte sender + tx_rlp.
	//
	// Fixture choices: empty system tx slots both sides (the common
	// case in practice); user txs follow the same hashByte+sender+rlp
	// shape DumpTxs would produce. The synthetic "txCount" param must
	// be >= 2 (one slot at each end).
	require.GreaterOrEqual(t, int(txCount), 2, "fixture requires txCount >= 2 to include the two system slots")
	tc, err := seg.NewCompressor(ctx, "fixture-txs", tFI.Path, tmpDir, seg.DefaultCfg, log.LvlError, log.New())
	require.NoError(t, err)
	totalTx := (toBlock - fromBlock) * uint64(txCount)
	for i := uint64(0); i < totalTx; i++ {
		positionInBlock := i % uint64(txCount)
		isSystemSlot := positionInBlock == 0 || positionInBlock == uint64(txCount)-1
		if isSystemSlot {
			// Empty system tx slot.
			require.NoError(t, tc.AddUncompressedWord(nil))
			continue
		}
		word := make([]byte, 0, 1+20+5)
		word = append(word, 0xAB) // hash prefix
		var sender [20]byte
		binary.BigEndian.PutUint64(sender[:8], i)
		word = append(word, sender[:]...)
		// fake tx_rlp: tag(0x80=empty list-like) + 4 byte tx index
		var txTag [5]byte
		txTag[0] = 0x80
		binary.BigEndian.PutUint32(txTag[1:], uint32(i))
		word = append(word, txTag[:]...)
		require.NoError(t, tc.AddUncompressedWord(word))
	}
	require.NoError(t, tc.Compress())
	tc.Close()

	return hFI, bFI, tFI
}

// TestSeedLeftoverBlocks_WritesHeadersBodiesTxsSenders pins the
// non-aligned-cut leftover-seed contract: when toBlock+1 isn't a
// 1000-multiple, blocks [chunkAlignedToBlock(toBlock), toBlock] are
// seeded from the OLD straddle files into the writable DB so
// canonical reads resolve post-mode-B.
//
// Fixture: 10-block triple covering [2_000_000, 2_000_010), 4
// txs/block (= 2 user txs + 2 system tx slots, matching what
// freezeblocks.DumpTxs writes), baseTxnID=1000. Seeds blocks
// [2_000_005, 2_000_007] (3 blocks → 12 tx entries, of which 6 are
// user txs and 6 are empty system tx slots). Asserts:
//   - kv.Headers, kv.HeaderNumber populated for the seeded range
//   - kv.BlockBody populated
//   - kv.EthTx populated for USER tx slot txnIDs only — block 2_000_005
//     spans 1020..1023 with user at 1021,1022; system slots 1020 and
//     1023 stay absent because the source snapshot entries were empty
//   - kv.Senders populated for each seeded block with the right
//     concatenated 20-byte senders (2 per block = 40 bytes)
func TestSeedLeftoverBlocks_WritesHeadersBodiesTxsSenders(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	snapDir := t.TempDir()
	tmpDir := t.TempDir()

	const (
		fromBlock = uint64(2_000_000)
		toBlock   = uint64(2_000_010)
		baseTxn   = uint64(1_000)
		txPer     = uint32(4)
	)
	hFI, bFI, tFI := makeBlockSnapshotTriple(t, ctx, snapDir, tmpDir, fromBlock, toBlock, baseTxn, txPer)

	// Pre-seed kv.HeaderCanonical for the leftover range — the seed
	// reads canonical hashes from there. Use simple deterministic
	// hashes so we can verify HeaderNumber writes later.
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	rwTx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	hashAt := func(n uint64) common.Hash {
		var h common.Hash
		binary.BigEndian.PutUint64(h[:8], n)
		copy(h[24:], "canonical-test")
		return h
	}
	for n := fromBlock; n < toBlock; n++ {
		require.NoError(t, rawdb.WriteCanonicalHash(rwTx, hashAt(n), n))
	}

	const (
		seedFrom = uint64(2_000_005)
		seedTo   = uint64(2_000_007)
	)
	require.NoError(t, seedLeftoverBlocks(ctx, rwTx, snapDir, hFI, bFI, tFI, seedFrom, seedTo))

	// kv.Headers populated for seeded range; NOT for blocks below seedFrom.
	for n := seedFrom; n <= seedTo; n++ {
		key := make([]byte, 8+32)
		binary.BigEndian.PutUint64(key[:8], n)
		copy(key[8:], func() []byte { h := hashAt(n); return h[:] }())
		got, err := rwTx.GetOne(kv.Headers, key)
		require.NoError(t, err)
		require.NotEmpty(t, got, "seeded block %d must have kv.Headers entry", n)
	}
	// Spot-check below the seed range: nothing seeded.
	belowKey := make([]byte, 8+32)
	binary.BigEndian.PutUint64(belowKey[:8], fromBlock)
	{
		h := hashAt(fromBlock)
		copy(belowKey[8:], h[:])
	}
	got, err := rwTx.GetOne(kv.Headers, belowKey)
	require.NoError(t, err)
	require.Empty(t, got, "block %d (below seedFrom) must NOT be seeded", fromBlock)

	// kv.HeaderNumber populated (WriteHeaderRaw with skipIndexing=false).
	for n := seedFrom; n <= seedTo; n++ {
		got, err := rwTx.GetOne(kv.HeaderNumber, func() []byte { h := hashAt(n); return h[:] }())
		require.NoError(t, err)
		require.NotEmpty(t, got, "seeded block %d must have kv.HeaderNumber entry (hash → num)", n)
	}

	// kv.BlockBody populated.
	for n := seedFrom; n <= seedTo; n++ {
		key := make([]byte, 8+32)
		binary.BigEndian.PutUint64(key[:8], n)
		copy(key[8:], func() []byte { h := hashAt(n); return h[:] }())
		got, err := rwTx.GetOne(kv.BlockBody, key)
		require.NoError(t, err)
		require.NotEmpty(t, got, "seeded block %d must have kv.BlockBody entry", n)
	}

	// kv.EthTx populated for USER tx slot txnIDs only. With
	// baseTxn=1000 and txPer=4 (4 entries per block: sys, user,
	// user, sys), block N maps to txnIDs [baseTxn + (N-fromBlock)*4,
	// baseTxn + (N-fromBlock+1)*4). User-tx slots are positions 1+2
	// (txnID%4 == 1 or 2 within the block).
	//
	//   block 2_000_005 → txnIDs 1020..1023; user slots 1021,1022
	//   block 2_000_006 → txnIDs 1024..1027; user slots 1025,1026
	//   block 2_000_007 → txnIDs 1028..1031; user slots 1029,1030
	checkEth := func(txnID uint64, expectPresent bool, msg string) {
		var keyBytes [8]byte
		binary.BigEndian.PutUint64(keyBytes[:], txnID)
		got, gerr := rwTx.GetOne(kv.EthTx, keyBytes[:])
		require.NoError(t, gerr)
		if expectPresent {
			require.NotEmpty(t, got, "txnID %d (%s) must have kv.EthTx entry", txnID, msg)
		} else {
			require.Empty(t, got, "txnID %d (%s) must NOT have kv.EthTx entry", txnID, msg)
		}
	}
	for _, userTxnID := range []uint64{1021, 1022, 1025, 1026, 1029, 1030} {
		checkEth(userTxnID, true, "user tx in seeded block")
	}
	for _, sysTxnID := range []uint64{1020, 1023, 1024, 1027, 1028, 1031} {
		checkEth(sysTxnID, false, "system tx slot in seeded block (empty source entry)")
	}
	// Spot-check below: txnID 1019 (last slot of block 2_000_004)
	// must NOT be seeded.
	checkEth(1019, false, "below seedFrom range")

	// kv.Senders populated with 40-byte len (2 senders × 20 bytes per
	// block) for each seeded block.
	for n := seedFrom; n <= seedTo; n++ {
		key := make([]byte, 8+32)
		binary.BigEndian.PutUint64(key[:8], n)
		copy(key[8:], func() []byte { h := hashAt(n); return h[:] }())
		got, err := rwTx.GetOne(kv.Senders, key)
		require.NoError(t, err)
		require.Equal(t, 40, len(got), "block %d senders entry must be 40 bytes (2 txs × 20 sender bytes)", n)
	}
}

// TestRebuildBodiesStraddleFile_RejectsWrongType pins that the
// bodies-specific entry point refuses a non-bodies FileInfo. Same
// pattern as headers.
func TestRebuildBodiesStraddleFile_RejectsWrongType(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fi := snaptype.FileInfo{
		Version: snaptype2.Headers.Versions().Current,
		From:    2_000_000,
		To:      2_010_000,
		Type:    snaptype2.Headers, // wrong type for bodies entry
		Ext:     ".seg",
	}
	fi = fi.As(snaptype2.Headers)
	_, err := rebuildBodiesStraddleFile(ctx, fi, 2_007_000, t.TempDir(), t.TempDir(), nil, log.New())
	require.Error(t, err)
	require.Contains(t, err.Error(), "not Bodies")
}

// TestRebuildTransactionsStraddleFile_RejectsWrongType pins the
// type guard on the tx entry.
func TestRebuildTransactionsStraddleFile_RejectsWrongType(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fi := snaptype.FileInfo{
		Version: snaptype2.Headers.Versions().Current,
		From:    2_000_000,
		To:      2_010_000,
		Type:    snaptype2.Bodies, // wrong type for tx entry
		Ext:     ".seg",
	}
	fi = fi.As(snaptype2.Bodies)
	_, err := rebuildTransactionsStraddleFile(ctx, fi, 2_007_000, t.TempDir(), t.TempDir(), nil, log.New())
	require.Error(t, err)
	require.Contains(t, err.Error(), "not Transactions")
}

// TestRebuildTransactionsStraddleFile_RejectsMissingRebuiltBodies
// pins the ordering contract: transactions rebuild requires the
// bodies file at the NEW range to exist. Without it (because bodies
// wasn't rebuilt first), the tx rebuild errors out with a clear
// message.
func TestRebuildTransactionsStraddleFile_RejectsMissingRebuiltBodies(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	snapDir := t.TempDir()
	tmpDir := t.TempDir()

	// Fake old transactions .seg fixture. The function reads bodies
	// BEFORE touching the source tx file, so the source contents
	// don't matter for this guard test — but the source path must
	// be visible to a Decompressor open. Build an empty seg.
	oldFI := snaptype.FileInfo{
		Version: snaptype2.Transactions.Versions().Current,
		From:    2_000_000,
		To:      2_010_000,
		Type:    snaptype2.Transactions,
		Ext:     ".seg",
	}
	oldFI = oldFI.As(snaptype2.Transactions)
	oldFI.Path = filepath.Join(snapDir, oldFI.Name())
	// Need at least one entry for seg.NewCompressor to produce a
	// valid file; the path-not-found check trips before we touch it.
	makeFakeSegFile(t, ctx, oldFI.Path, tmpDir, 1)

	_, err := rebuildTransactionsStraddleFile(ctx, oldFI, 2_007_000, snapDir, tmpDir, nil, log.New())
	require.Error(t, err)
	require.Contains(t, err.Error(), "rebuilt bodies file not found",
		"tx rebuild must fail explicitly when bodies hasn't been rebuilt first — order matters")
}

// TestSliceStraddleSeg_RejectsBadInputs pins guard-clause behavior.
func TestSliceStraddleSeg_RejectsBadInputs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	makeFI := func(from, to uint64) snaptype.FileInfo {
		fi := snaptype.FileInfo{
			Version: snaptype2.Headers.Versions().Current,
			From:    from,
			To:      to,
			Type:    snaptype2.Headers,
			Ext:     ".seg",
		}
		return fi.As(snaptype2.Headers)
	}

	cases := []struct {
		name       string
		oldFI      snaptype.FileInfo
		newToBlock uint64
		wantErr    string
	}{
		{"newToBlock ≤ From", makeFI(2_000_000, 2_010_000), 2_000_000, "outside straddle"},
		{"newToBlock ≥ To", makeFI(2_000_000, 2_010_000), 2_010_000, "outside straddle"},
		{"newToBlock past To", makeFI(2_000_000, 2_010_000), 2_020_000, "outside straddle"},
		{"non-1000-aligned newToBlock", makeFI(2_000_000, 2_010_000), 2_005_500, "not aligned"},
		{"nil Type", snaptype.FileInfo{From: 2_000_000, To: 2_010_000}, 2_005_000, "nil Type"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := sliceStraddleSeg(ctx, tc.oldFI, tc.newToBlock, t.TempDir(), t.TempDir(), log.New())
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

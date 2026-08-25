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

package freezeblocks

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/chain/networkname"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/rawdb"
)

// writeTailBlock re-inserts a block from its segment words into the DB at the ORIGINAL BaseTxnID
// (real txns only in EthTx; system-tx slots skipped) and it round-trips back through the reader.
func TestWriteTailBlock(t *testing.T) {
	_, tx := mdbxtest.NewTestTx(t)

	h := &types.Header{Number: *uint256.NewInt(5)}
	hRLP, err := rlp.EncodeToBytes(h)
	require.NoError(t, err)
	headerWord := append([]byte{0xaa}, hRLP...) // firstByte-of-hash + header rlp
	hash := h.Hash()

	body := types.BodyForStorage{BaseTxnID: types.BaseTxnID(100), TxCount: 4} // 2 real + 2 system
	bodyWord, err := rlp.EncodeToBytes(&body)
	require.NoError(t, err)

	w1, raw1, _ := tailTxWord(t, 0, 1)
	w2, raw2, _ := tailTxWord(t, 1, 2)
	txWords := [][]byte{{0}, w1, w2, {0}} // begin, real0, real1, end

	require.NoError(t, writeTailBlock(tx, headerWord, bodyWord, txWords))

	got := rawdb.ReadHeaderByNumber(tx, 5)
	require.NotNil(t, got)
	require.Equal(t, uint64(5), got.Number.Uint64())
	ch, err := rawdb.ReadCanonicalHash(tx, 5)
	require.NoError(t, err)
	require.Equal(t, hash, ch)

	bfs, err := rawdb.ReadBodyForStorageByKey(tx, dbutils.BlockBodyKey(5, hash))
	require.NoError(t, err)
	require.Equal(t, uint32(4), bfs.TxCount)
	require.Equal(t, uint64(100), bfs.BaseTxnID.U64())

	// real txns at base+1=101, base+2=102 (system slots 100 and 103 hold nothing)
	v1, err := tx.GetOne(kv.EthTx, hexutil.EncodeTs(101))
	require.NoError(t, err)
	require.Equal(t, raw1, v1)
	v2, err := tx.GetOne(kv.EthTx, hexutil.EncodeTs(102))
	require.NoError(t, err)
	require.Equal(t, raw2, v2)
	sys, err := tx.GetOne(kv.EthTx, hexutil.EncodeTs(100))
	require.NoError(t, err)
	require.Nil(t, sys)

	senders, err := rawdb.ReadSenders(tx, hash, 5)
	require.NoError(t, err)
	require.Len(t, senders, 2)
	require.Equal(t, byte(1), senders[0][0])
	require.Equal(t, byte(2), senders[1][0])
}

// The tail is written into the (pruned) key range below the DB's existing data. writeTailBlock must
// place it there with Put — filling the low keys without disturbing the higher, already-present data
// and without touching the EthTx sequence counter (which tracks the tip). This mirrors the real
// state: blocks [F,tip) and their high txn ids are in the DB, [1,F) were pruned, and the migration
// back-fills a tail block below F.
func TestWriteTailBlockBelowExistingData(t *testing.T) {
	_, tx := mdbxtest.NewTestTx(t)

	// Pre-existing high-range data: a header at a high block and an EthTx entry at a high txn id, with
	// the EthTx sequence advanced to the tip (as IncrementSequence would leave it during normal sync).
	const existingBlock, existingTxnID = uint64(900_000), uint64(5_000_000)
	existing := &types.Header{Number: *uint256.NewInt(existingBlock)}
	existingHash := existing.Hash()
	existingRLP, err := rlp.EncodeToBytes(existing)
	require.NoError(t, err)
	require.NoError(t, rawdb.WriteHeaderRaw(tx, existingBlock, existingHash, existingRLP, false))
	require.NoError(t, rawdb.WriteCanonicalHash(tx, existingHash, existingBlock))
	require.NoError(t, tx.Put(kv.EthTx, hexutil.EncodeTs(existingTxnID), []byte("existing-tx")))
	_, err = tx.IncrementSequence(kv.EthTx, existingTxnID+1) // sequence now sits at the tip
	require.NoError(t, err)
	seqBefore, err := tx.ReadSequence(kv.EthTx)
	require.NoError(t, err)

	// A tail block far below the existing data: block 5, BaseTxnID 100, TxCount 4 (2 real + 2 system).
	h := &types.Header{Number: *uint256.NewInt(5)}
	hRLP, err := rlp.EncodeToBytes(h)
	require.NoError(t, err)
	headerWord := append([]byte{0xaa}, hRLP...)
	body := types.BodyForStorage{BaseTxnID: types.BaseTxnID(100), TxCount: 4}
	bodyWord, err := rlp.EncodeToBytes(&body)
	require.NoError(t, err)
	w1, raw1, _ := tailTxWord(t, 0, 1)
	w2, _, _ := tailTxWord(t, 1, 2)
	txWords := [][]byte{{0}, w1, w2, {0}}

	require.NoError(t, writeTailBlock(tx, headerWord, bodyWord, txWords))

	// The tail block landed at the low keys.
	require.NotNil(t, rawdb.ReadHeaderByNumber(tx, 5))
	v, err := tx.GetOne(kv.EthTx, hexutil.EncodeTs(101)) // first real txn at base+1
	require.NoError(t, err)
	require.Equal(t, raw1, v)

	// The pre-existing high-range data is untouched.
	require.NotNil(t, rawdb.ReadHeaderByNumber(tx, existingBlock))
	ev, err := tx.GetOne(kv.EthTx, hexutil.EncodeTs(existingTxnID))
	require.NoError(t, err)
	require.Equal(t, []byte("existing-tx"), ev)

	// The EthTx sequence counter (the tip's next id) is unchanged — the tail used Put, not the sequence.
	seqAfter, err := tx.ReadSequence(kv.EthTx)
	require.NoError(t, err)
	require.Equal(t, seqBefore, seqAfter)

	// EthTx stays sorted: the back-filled low ids come first, the pre-existing high id stays last.
	c, err := tx.Cursor(kv.EthTx)
	require.NoError(t, err)
	defer c.Close()
	first, _, err := c.First()
	require.NoError(t, err)
	require.Equal(t, hexutil.EncodeTs(101), first)
	last, _, err := c.Last()
	require.NoError(t, err)
	require.Equal(t, hexutil.EncodeTs(existingTxnID), last)
}

// createSeqSeg writes a decimal .seg for [from,to) whose word for block K is big-endian K, so a
// repack can be verified block-by-block. No index is written (the migration reads segments
// sequentially via the raw Decompressor).
func createSeqSeg(t *testing.T, dir string, from, to uint64, name snaptype.Type, ver snaptype.Version, epoch bool, logger log.Logger) {
	cfg := seg.DefaultCfg
	cfg.MinPatternScore = 100
	c, err := seg.NewCompressor(t.Context(), "test", filepath.Join(dir, snaptype.SegmentFileName(ver, epoch, from, to, name.Enum())), dir, cfg, log.LvlDebug, logger)
	require.NoError(t, err)
	defer c.Close()
	c.DisableFsync()
	buf := make([]byte, 8)
	for k := from; k < to; k++ {
		binary.BigEndian.PutUint64(buf, k)
		require.NoError(t, c.AddWord(buf))
	}
	require.NoError(t, c.Compress())
}

// repackWordPerBlock re-segments two decimal headers segments covering [0,2000) into the planned
// epoch segment [0,1024), copying words verbatim, and hands the sub-1024 tail (1024..1999) to the
// DB callback.
func TestRepackWordPerBlock(t *testing.T) {
	dir := t.TempDir()
	logger := log.New()
	createSeqSeg(t, dir, 0, 1000, snaptype2.Headers, version.V1_0, false, logger)
	createSeqSeg(t, dir, 1000, 2000, snaptype2.Headers, version.V1_0, false, logger)

	m := &epochMigrator{
		dirs:    datadir.Dirs{Snap: dir, Tmp: dir},
		snCfg:   snapcfg.KnownCfgOrDevnet(networkname.Mainnet),
		workers: 1,
		lvl:     log.LvlDebug,
		logger:  logger,
	}
	_, _, hasDecimal, err := m.loadSegs()
	require.NoError(t, err)
	require.True(t, hasDecimal)
	const frozenMax = 2000 // isolating headers; the real orchestrator takes the min over all block types

	var tail []uint64
	require.NoError(t, m.repackWordPerBlock(t.Context(), snaptype2.Headers, 0, frozenMax,
		func(bn uint64, w []byte) error { tail = append(tail, binary.BigEndian.Uint64(w)); return nil }))

	// epoch segment [0,1024) has exactly blocks 0..1023 in order
	d, err := seg.NewDecompressor(snaptype2.Headers.FileInfo(dir, true, 0, 1024).Path)
	require.NoError(t, err)
	defer d.Close()
	require.Equal(t, 1024, d.Count())
	g := d.MakeGetter()
	for k := range uint64(1024) {
		w, _ := g.Next(nil)
		require.Equal(t, k, binary.BigEndian.Uint64(w))
	}
	// tail = blocks 1024..1999 (976 blocks) handed to the DB callback in order
	require.Len(t, tail, 976)
	require.Equal(t, uint64(1024), tail[0])
	require.Equal(t, uint64(1999), tail[975])

	// per-segment deletion: [0,1000) is fully covered by epoch [0,1024) and removed; the segment
	// straddling tailFrom ([1000,2000)) is kept for the tail and survives this step.
	require.NoFileExists(t, filepath.Join(dir, snaptype.SegmentFileName(version.V1_0, false, 0, 1000, snaptype2.Headers.Enum())))
	require.FileExists(t, filepath.Join(dir, snaptype.SegmentFileName(version.V1_0, false, 1000, 2000, snaptype2.Headers.Enum())))
}

// On resume, the type's decimal below the epoch frontier is already gone, so the run starts from
// the straddling segment: repack skips [firstFrom, startBlock) and, when the frontier is already at
// tailFrom, produces no epoch segment and just hands the tail to the DB callback.
func TestRepackWordPerBlockResume(t *testing.T) {
	dir := t.TempDir()
	logger := log.New()
	// [0,1000) was already migrated and deleted; only the straddling [1000,2000) remains.
	createSeqSeg(t, dir, 1000, 2000, snaptype2.Headers, version.V1_0, false, logger)

	m := &epochMigrator{
		dirs:    datadir.Dirs{Snap: dir, Tmp: dir},
		snCfg:   snapcfg.KnownCfgOrDevnet(networkname.Mainnet),
		workers: 1,
		lvl:     log.LvlDebug,
		logger:  logger,
	}
	_, _, _, err := m.loadSegs()
	require.NoError(t, err)
	var tail []uint64
	require.NoError(t, m.repackWordPerBlock(t.Context(), snaptype2.Headers, 1024, 2000,
		func(bn uint64, w []byte) error { tail = append(tail, binary.BigEndian.Uint64(w)); return nil }))

	// startBlock == tailFrom == 1024 → no epoch segment produced, tail = 1024..1999 from the straddle
	require.NoFileExists(t, snaptype2.Headers.FileInfo(dir, true, 0, 1024).Path)
	require.Len(t, tail, 976)
	require.Equal(t, uint64(1024), tail[0])
	require.Equal(t, uint64(1999), tail[975])
}

// When a type is already fully migrated (startBlock == frozenMax), the repack is a no-op even if
// its decimal is gone and, for transactions, the bodies decimal is gone too. This guards the
// resume where the final straddle deletion crashed partway: frozenMax collapses to tailFrom, and a
// type whose decimal already vanished must not try to read an empty stream.
func TestRepackNoopWhenAlreadyMigrated(t *testing.T) {
	dir := t.TempDir()
	logger := log.New()
	m := &epochMigrator{
		dirs:    datadir.Dirs{Snap: dir, Tmp: dir},
		snCfg:   snapcfg.KnownCfgOrDevnet(networkname.Mainnet),
		workers: 1,
		lvl:     log.LvlDebug,
		logger:  logger,
	}
	called := false
	require.NoError(t, m.repackWordPerBlock(t.Context(), snaptype2.Headers, 1024, 1024,
		func(uint64, []byte) error { called = true; return nil }))
	require.NoError(t, m.repackTransactions(t.Context(), 1024, 1024,
		func(uint64, uint64, [][]byte) error { called = true; return nil }))
	require.False(t, called)
}

// classifyByType splits one directory scan per type, and coverage folds each type's epoch prefix and
// the decimal run continuing it into (epochStart, coveredTo, runLen) in a single pass — the resume
// start, the frozenMax input, and the gap-free run a repack reads.
func TestClassifyByTypeAndCoverage(t *testing.T) {
	dir := t.TempDir()
	logger := log.New()
	// headers already migrated to epoch [0,1024); decimal straddle [1000,2000) still present.
	createSeqSeg(t, dir, 0, 1024, snaptype2.Headers, version.V1_1, true, logger)
	createSeqSeg(t, dir, 1000, 2000, snaptype2.Headers, version.V1_0, false, logger)
	// bodies: fully-decimal from 0, then a gap, then a post-gap orphan.
	createSeqSeg(t, dir, 0, 1000, snaptype2.Bodies, version.V1_0, false, logger)
	createSeqSeg(t, dir, 1000, 2000, snaptype2.Bodies, version.V1_0, false, logger)
	createSeqSeg(t, dir, 3000, 4000, snaptype2.Bodies, version.V1_0, false, logger) // gap at [2000,3000)

	all, err := snaptype.Segments(dir)
	require.NoError(t, err)

	epoch, decimal := classifyByType(all, snaptype2.Headers.Enum())
	require.Len(t, epoch, 1)
	require.Len(t, decimal, 1)
	epochStart, coveredTo, runLen := coverage(epoch, decimal)
	require.Equal(t, uint64(1024), epochStart) // resume producing from the epoch frontier
	require.Equal(t, uint64(2000), coveredTo)  // straddle decimal extends coverage to 2000
	require.Equal(t, 1, runLen)

	epoch, decimal = classifyByType(all, snaptype2.Bodies.Enum())
	require.Empty(t, epoch)
	require.Len(t, decimal, 3)
	epochStart, coveredTo, runLen = coverage(epoch, decimal)
	require.Equal(t, uint64(0), epochStart)
	require.Equal(t, uint64(2000), coveredTo) // gap at 2000 truncates coverage
	require.Equal(t, 2, runLen)               // post-gap [3000,4000) dropped from the run

	// transactions type: nothing on disk → no coverage, no decimal.
	epoch, decimal = classifyByType(all, snaptype2.Transactions.Enum())
	require.Empty(t, epoch)
	require.Empty(t, decimal)
	epochStart, coveredTo, runLen = coverage(epoch, decimal)
	require.Zero(t, epochStart)
	require.Zero(t, coveredTo)
	require.Zero(t, runLen)
}

// createBodyAndTxSegs writes a matching pair of decimal segments for [from,to): a bodies segment
// whose word for block K is rlp(BodyForStorage{BaseTxnID: K*txPerBlock, TxCount: txPerBlock}), and
// a transactions segment with txPerBlock words per block, each encoding (blockNum, txIndex).
func createBodyAndTxSegs(t *testing.T, dir string, from, to, txPerBlock uint64, ver snaptype.Version, logger log.Logger) {
	cfg := seg.DefaultCfg
	cfg.MinPatternScore = 100
	bc, err := seg.NewCompressor(t.Context(), "test", filepath.Join(dir, snaptype.SegmentFileName(ver, false, from, to, snaptype2.Bodies.Enum())), dir, cfg, log.LvlDebug, logger)
	require.NoError(t, err)
	defer bc.Close()
	bc.DisableFsync()
	tc, err := seg.NewCompressor(t.Context(), "test", filepath.Join(dir, snaptype.SegmentFileName(ver, false, from, to, snaptype2.Transactions.Enum())), dir, cfg, log.LvlDebug, logger)
	require.NoError(t, err)
	defer tc.Close()
	tc.DisableFsync()

	for k := from; k < to; k++ {
		body := types.BodyForStorage{BaseTxnID: types.BaseTxnID(k * txPerBlock), TxCount: uint32(txPerBlock)}
		w, err := rlp.EncodeToBytes(&body)
		require.NoError(t, err)
		require.NoError(t, bc.AddWord(w))
		for j := range txPerBlock {
			buf := make([]byte, 16)
			binary.BigEndian.PutUint64(buf[:8], k)
			binary.BigEndian.PutUint64(buf[8:], j)
			require.NoError(t, tc.AddWord(buf))
		}
	}
	require.NoError(t, bc.Compress())
	require.NoError(t, tc.Compress())
}

// repackTransactions splits the txn-granular transactions segment at block boundaries using the
// bodies' TxCount: [0,2000) at 2 txns/block → epoch [0,1024) with 2048 words, tail 976 blocks.
func TestRepackTransactions(t *testing.T) {
	dir := t.TempDir()
	logger := log.New()
	createBodyAndTxSegs(t, dir, 0, 1000, 2, version.V1_0, logger)
	createBodyAndTxSegs(t, dir, 1000, 2000, 2, version.V1_0, logger)

	m := &epochMigrator{
		dirs:    datadir.Dirs{Snap: dir, Tmp: dir},
		snCfg:   snapcfg.KnownCfgOrDevnet(networkname.Mainnet),
		workers: 1,
		lvl:     log.LvlDebug,
		logger:  logger,
	}
	_, _, _, err := m.loadSegs()
	require.NoError(t, err)
	type tailBlk struct {
		base uint64
		n    int
	}
	var tail []tailBlk
	require.NoError(t, m.repackTransactions(t.Context(), 0, 2000, func(bn, base uint64, words [][]byte) error {
		tail = append(tail, tailBlk{base, len(words)})
		return nil
	}))

	// epoch transactions segment [0,1024) holds 2*1024 words in (block, txIndex) order
	d, err := seg.NewDecompressor(snaptype2.Transactions.FileInfo(dir, true, 0, 1024).Path)
	require.NoError(t, err)
	defer d.Close()
	require.Equal(t, 2048, d.Count())
	g := d.MakeGetter()
	for k := range uint64(1024) {
		for j := range uint64(2) {
			w, _ := g.Next(nil)
			require.Equal(t, k, binary.BigEndian.Uint64(w[:8]))
			require.Equal(t, j, binary.BigEndian.Uint64(w[8:]))
		}
	}
	// tail: blocks 1024..1999, each with BaseTxnID = block*2 and 2 words
	require.Len(t, tail, 976)
	require.Equal(t, uint64(1024*2), tail[0].base)
	require.Equal(t, 2, tail[0].n)
	require.Equal(t, uint64(1999*2), tail[975].base)
}

// The plan tiles [0, floor(frozenMax,1024)) with epoch tiers (524288/65536/8192/1024) and leaves
// the sub-1024 remainder as the DB tail. For frozenMax=600000 the floor is 599040.
func TestPlanEpochSegments(t *testing.T) {
	snCfg := snapcfg.KnownCfgOrDevnet(networkname.Mainnet)

	segs, tailFrom := planEpochSegments(0, 600_000, snCfg)
	require.Equal(t, [][2]uint64{
		{0, 524_288},
		{524_288, 589_824},
		{589_824, 598_016},
		{598_016, 599_040},
	}, segs)
	require.Equal(t, uint64(599_040), tailFrom) // 600000 - 600000%1024

	// A frozenMax already below one tier has no epoch segments; everything is tail.
	segs, tailFrom = planEpochSegments(0, 500, snCfg)
	require.Empty(t, segs)
	require.Equal(t, uint64(0), tailFrom)

	// An exact tier boundary leaves no tail.
	segs, tailFrom = planEpochSegments(0, 524_288, snCfg)
	require.Equal(t, [][2]uint64{{0, 524_288}}, segs)
	require.Equal(t, uint64(524_288), tailFrom)
}

// tailTxWord builds a transactions-segment word — firstByte-of-hash + sender(20) + txn binary — for a
// real signed txn. writeTailBlock decodes each txn to get its hash for the kv.TxLookup entry, so the
// payload has to be a decodable transaction, not filler.
func tailTxWord(t *testing.T, nonce uint64, sender byte) (word, raw []byte, hash common.Hash) {
	t.Helper()
	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	txn, err := types.SignNewTx(key, *types.LatestSignerForChainID(uint256.NewInt(1337)), &types.LegacyTx{
		CommonTx: types.CommonTx{Nonce: nonce, GasLimit: 21000, Value: *uint256.NewInt(1)},
		GasPrice: *uint256.NewInt(1),
	})
	require.NoError(t, err)
	var buf bytes.Buffer
	require.NoError(t, txn.MarshalBinary(&buf))
	raw = buf.Bytes()
	word = append([]byte{0xaa, sender}, make([]byte, length.Addr-1)...)
	word = append(word, raw...)
	return word, raw, txn.Hash()
}

// HasDecimalBlockSegments is what read-only consumers (rpcdaemon) use to refuse a datadir that still
// needs converting, instead of converting it themselves. It must not fire on a decimal chain, where
// decimal is the correct regime rather than pending work.
func TestHasDecimalBlockSegments(t *testing.T) {
	write := func(t *testing.T, dir string, epoch bool, from, to uint64) {
		t.Helper()
		for _, typ := range snaptype2.BlockSnapshotTypes {
			name := typ.FileInfo(dir, epoch, from, to).Name()
			// non-empty: ParseDir skips zero-length files
			require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte{0}, 0o644))
		}
	}

	t.Run("decimal segments on an eth chain need converting", func(t *testing.T) {
		dirs := datadir.New(t.TempDir())
		write(t, dirs.Snap, false, 0, 1_000)
		got, err := HasDecimalBlockSegments(dirs, chain.AllProtocolChanges)
		require.NoError(t, err)
		require.True(t, got)
	})

	t.Run("epoch segments do not", func(t *testing.T) {
		dirs := datadir.New(t.TempDir())
		write(t, dirs.Snap, true, 0, 1_024)
		got, err := HasDecimalBlockSegments(dirs, chain.AllProtocolChanges)
		require.NoError(t, err)
		require.False(t, got)
	})

	t.Run("empty datadir does not", func(t *testing.T) {
		dirs := datadir.New(t.TempDir())
		got, err := HasDecimalBlockSegments(dirs, chain.AllProtocolChanges)
		require.NoError(t, err)
		require.False(t, got)
	})

	t.Run("a decimal chain never does", func(t *testing.T) {
		dirs := datadir.New(t.TempDir())
		write(t, dirs.Snap, false, 0, 1_000)
		got, err := HasDecimalBlockSegments(dirs, chainspec.Gnosis.Config)
		require.NoError(t, err)
		require.False(t, got)
	})
}

// A crash between a merge output landing and RemoveOverlaps can leave a merged segment on disk
// alongside the old subsegments it subsumes. sortedNoOverlaps must drop the contained subsegments so
// coverage sees a clean From-ascending run and does not mistake the first contained file for a gap —
// which previously truncated the run and deleted valid decimal segments beyond it.
func TestSortedNoOverlapsDropsContained(t *testing.T) {
	fi := func(from, to uint64) snaptype.FileInfo { return snaptype.FileInfo{From: from, To: to} }
	rng := func(s []snaptype.FileInfo) [][2]uint64 {
		out := make([][2]uint64, len(s))
		for i, f := range s {
			out[i] = [2]uint64{f.From, f.To}
		}
		return out
	}

	// merged [0,8192) coexists with its old subsegments; only the container survives, then the tail.
	in := []snaptype.FileInfo{fi(1024, 2048), fi(0, 8192), fi(0, 1024), fi(2048, 3072), fi(8192, 16384)}
	require.Equal(t, [][2]uint64{{0, 8192}, {8192, 16384}}, rng(sortedNoOverlaps(in)))

	// with the contained files gone, coverage counts the full contiguous run rather than stopping at
	// a false gap at the first subsegment.
	_, coveredTo, runLen := coverage(nil, sortedNoOverlaps(in))
	require.Equal(t, uint64(16384), coveredTo)
	require.Equal(t, 2, runLen)
}

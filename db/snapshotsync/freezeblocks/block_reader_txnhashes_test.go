// Copyright 2025 The Erigon Authors
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
	"path/filepath"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/db/version"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/types"
)

// txnHashesTestTxns builds signed txns of both storage shapes: legacy, stored as
// a bare RLP list, and typed, stored as that list wrapped in an RLP string.
func txnHashesTestTxns(t *testing.T, n int) []types.Transaction {
	t.Helper()
	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	to := crypto.PubkeyToAddress(key.PublicKey)
	chainID := uint256.NewInt(chainspec.Mainnet.Config.ChainID.Uint64())
	signer := types.LatestSignerForChainID(chainID)

	txns := make([]types.Transaction, 0, n)
	for i := range n {
		var unsigned types.Transaction
		if i%2 == 0 {
			unsigned = types.NewTransaction(uint64(i), to, &u256.Num1, 21000, &u256.Num1, []byte{byte(i)})
		} else {
			unsigned = types.NewEIP1559Transaction(*chainID, uint64(i), to, &u256.Num1, 21000, &u256.Num1, &u256.Num1, &u256.Num1, []byte{byte(i)})
		}
		signed, err := types.SignTx(unsigned, *signer, key)
		require.NoError(t, err)
		txns = append(txns, signed)
	}
	return txns
}

// createTxnSegmentFile writes a transactions segment holding real txn records in
// the layout DumpTxs produces - hash[0], sender, stored RLP - plus the TxnHash index
// baseTxnID is resolved through.
func createTxnSegmentFile(t *testing.T, from, to, baseTxnID uint64, txns []types.Transaction, dir string, ver snaptype.Version, logger log.Logger) {
	t.Helper()
	segPath := filepath.Join(dir, snaptype.SegmentFileName(ver, from, to, snaptype2.Enums.Transactions))

	compressCfg := seg.DefaultCfg
	compressCfg.MinPatternScore = 100
	c, err := seg.NewCompressor(t.Context(), "test", segPath, dir, compressCfg, log.LvlDebug, logger)
	require.NoError(t, err)
	defer c.Close()
	c.DisableFsync()

	var sender [20]byte
	var buf bytes.Buffer
	for _, txn := range txns {
		buf.Reset()
		require.NoError(t, txn.EncodeRLP(&buf))
		hash := txn.Hash()
		word := make([]byte, 0, 1+20+buf.Len())
		word = append(word, hash[0])
		word = append(word, sender[:]...)
		word = append(word, buf.Bytes()...)
		require.NoError(t, c.AddWord(word))
	}
	require.NoError(t, c.Compress())

	// Second pass for the record offsets the index maps ordinals to.
	d, err := seg.NewDecompressor(segPath)
	require.NoError(t, err)
	defer d.Close()
	offsets := make([]uint64, 0, len(txns))
	g := d.MakeGetter()
	var off uint64
	for g.HasNext() {
		offsets = append(offsets, off)
		_, off = g.Next(nil)
	}
	require.Len(t, offsets, len(txns))

	idx, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   len(txns),
		BucketSize: 10,
		TmpDir:     dir,
		IndexFile:  filepath.Join(dir, snaptype.IdxFileName(ver, from, to, snaptype2.Enums.Transactions.String())),
		BaseDataID: baseTxnID,
		LeafSize:   8,
		Enums:      true, // OrdinalLookup resolves txnID -> record offset
	}, logger)
	require.NoError(t, err)
	defer idx.Close()
	idx.DisableFsync()
	for i, txn := range txns {
		h := txn.Hash()
		require.NoError(t, idx.AddKey(h[:], offsets[i]))
	}
	require.NoError(t, idx.Build(t.Context()))

	// Unread by these tests, but a transactions segment stays invisible to OpenFolder
	// until every index of its type is on disk.
	blockNumIdx, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   len(txns),
		BucketSize: 10,
		TmpDir:     dir,
		IndexFile:  filepath.Join(dir, snaptype.IdxFileName(ver, from, to, snaptype2.Indexes.TxnHash2BlockNum.Name)),
		LeafSize:   8,
	}, logger)
	require.NoError(t, err)
	defer blockNumIdx.Close()
	blockNumIdx.DisableFsync()
	for i, txn := range txns {
		h := txn.Hash()
		require.NoError(t, blockNumIdx.AddKey(h[:], uint64(i)))
	}
	require.NoError(t, blockNumIdx.Build(t.Context()))
}

// openTxnHashesTestSegment builds a snapshot dir holding one transactions segment
// with txns in it and returns the visible segment plus a reader over it.
func openTxnHashesTestSegment(t *testing.T, baseTxnID uint64, txns []types.Transaction) (*BlockReader, *snapshotsync.VisibleSegment, func()) {
	t.Helper()
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)
	logger := log.New()
	ver := version.V1_0

	const from, to = 0, 1000
	createTestSegmentFile(t, from, to, snaptype2.Enums.Headers, dirs.Snap, ver, logger)
	createTestSegmentFile(t, from, to, snaptype2.Enums.Bodies, dirs.Snap, ver, logger)
	createTxnSegmentFile(t, from, to, baseTxnID, txns, dirs.Snap, ver, logger)

	snapshots := db.(HasBlockFiles).DebugBlockFiles()
	require.NoError(t, snapshots.OpenFolder())

	view := snapshots.View()
	seg, ok := view.Segment(snaptype2.Transactions, 1)
	require.True(t, ok)
	return NewBlockReader(snapshots, nil), seg, view.Close
}

// TestTxnHashesFromDB covers TxnHashes' dispatch rather than either reader: with no
// block files, every block resolves through the db, and a block absent from the db
// falls through to files that hold nothing and comes back as the not-found sentinel.
func TestTxnHashesFromDB(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)
	snapshots := db.(HasBlockFiles).DebugBlockFiles()
	require.NoError(t, snapshots.OpenFolder())
	require.Zero(t, snapshots.BlocksAvailable())
	r := NewBlockReader(snapshots, nil)

	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	txns := txnHashesTestTxns(t, 3)
	header := &types.Header{Number: *common.Num1}
	require.NoError(t, rawdb.WriteBody(tx, header.Hash(), 1, &types.Body{Transactions: txns}))

	hashes, err := r.TxnHashes(t.Context(), tx, header.Hash(), 1)
	require.NoError(t, err)
	require.Len(t, hashes, len(txns))
	for i, txn := range txns {
		require.Equal(t, txn.Hash(), hashes[i], "txn %d (type %d)", i, txn.Type())
	}

	missing, err := r.TxnHashes(t.Context(), tx, common.Hash{0xaa}, 1)
	require.NoError(t, err)
	require.Nil(t, missing)
}

// TestTxnHashesFromSnapshotTruncatedSegment pins that a segment holding fewer txns
// than the body claims is an error, where txsFromSnapshot returns the not-found sentinel.
func TestTxnHashesFromSnapshotTruncatedSegment(t *testing.T) {
	t.Parallel()
	const baseTxnID = 0
	txns := txnHashesTestTxns(t, 2)
	r, seg, closeView := openTxnHashesTestSegment(t, baseTxnID, txns)
	defer closeView()

	_, err := r.txnHashesFromSnapshot(baseTxnID, uint32(len(txns))+1, seg, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ended after 2 of 3")
}

// TestTxnHashesFromSnapshotMatchesTxsFromSnapshot pins the hash-only reader to the
// decoding one it mirrors, over the same segment records.
func TestTxnHashesFromSnapshotMatchesTxsFromSnapshot(t *testing.T) {
	t.Parallel()
	const baseTxnID = 0
	txns := txnHashesTestTxns(t, 4)
	r, seg, closeView := openTxnHashesTestSegment(t, baseTxnID, txns)
	defer closeView()

	decoded, _, err := r.txsFromSnapshot(baseTxnID, uint32(len(txns)), seg, nil)
	require.NoError(t, err)
	require.Len(t, decoded, len(txns))

	hashes, err := r.txnHashesFromSnapshot(baseTxnID, uint32(len(txns)), seg, nil)
	require.NoError(t, err)
	require.Len(t, hashes, len(txns))

	for i, txn := range decoded {
		require.Equal(t, txn.Hash(), hashes[i], "txn %d (type %d)", i, txn.Type())
	}
}

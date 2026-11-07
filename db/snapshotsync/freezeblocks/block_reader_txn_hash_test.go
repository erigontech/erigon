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
	"path/filepath"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/race"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/types"
)

// createTestTxnSegment writes a transactions file holding txns as txn ids 0..len(txns)-1,
// stored as the block files store them: hash prefix byte, sender, binary encoding.
func createTestTxnSegment(t *testing.T, from, to uint64, dir string, ver snaptype.Version, txns []types.Transaction, logger log.Logger) {
	t.Helper()
	segPath := filepath.Join(dir, snaptype.SegmentFileName(ver, from, to, snaptype2.Enums.Transactions))
	compressCfg := seg.DefaultCfg
	compressCfg.MinPatternScore = 100
	c, err := seg.NewCompressor(t.Context(), "test", segPath, dir, compressCfg, log.LvlDebug, logger)
	require.NoError(t, err)
	defer c.Close()
	c.DisableFsync()
	for _, txn := range txns {
		word := bytes.NewBuffer(append([]byte{txn.Hash()[0]}, make([]byte, 20)...))
		require.NoError(t, txn.MarshalBinary(word))
		require.NoError(t, c.AddWord(word.Bytes()))
	}
	require.NoError(t, c.Compress())

	d, err := seg.NewDecompressor(segPath)
	require.NoError(t, err)
	defer d.Close()
	offsets := make([]uint64, 0, len(txns))
	for g, pos := d.MakeGetter(), uint64(0); g.HasNext(); pos, _ = g.Skip() {
		offsets = append(offsets, pos)
	}
	for _, idxName := range []string{snaptype2.Indexes.TxnHash.Name, snaptype2.Indexes.TxnHash2BlockNum.Name} {
		idx, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
			KeyCount:   len(txns),
			BucketSize: 10,
			TmpDir:     dir,
			IndexFile:  filepath.Join(dir, snaptype.IdxFileName(ver, from, to, idxName)),
			LeafSize:   8,
			Enums:      true,
		}, logger)
		require.NoError(t, err)
		idx.DisableFsync()
		for i, txn := range txns {
			hash := txn.Hash()
			require.NoError(t, idx.AddKey(hash[:], offsets[i]))
		}
		require.NoError(t, idx.Build(t.Context()))
		idx.Close()
	}
}

// A transaction larger than the pooled buffer must not cost an allocation of its
// size on every call: the grown buffer goes back to the pool.
func TestTxnHashByIdxInBlockReusesGrownBuffer(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)
	logger := log.New()
	ver := version.V1_0

	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	signer := types.LatestSignerForChainID(nil)
	sign := func(nonce uint64, data []byte) types.Transaction {
		txn, signErr := types.SignTx(types.NewTransaction(nonce, common.Address{1}, uint256.NewInt(1), 1_000_000, uint256.NewInt(1), data), *signer, key)
		require.NoError(t, signErr)
		return txn
	}
	small, large := sign(0, nil), sign(1, make([]byte, 64*1024))
	createTestSegmentFile(t, 1, 1000, snaptype2.Enums.Headers, dirs.Snap, ver, logger)
	createTestSegmentFile(t, 1, 1000, snaptype2.Enums.Bodies, dirs.Snap, ver, logger)
	createTestTxnSegment(t, 1, 1000, dirs.Snap, ver, []types.Transaction{small, large}, logger)

	snapshots := db.(HasBlockFiles).DebugBlockFiles()
	require.NoError(t, snapshots.OpenFolder())
	blockReader := NewBlockReader(snapshots)
	tx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	allocs := func(txNum uint64, want common.Hash) float64 {
		hash, ok, err := blockReader.TxnHashByIdxInBlock(t.Context(), tx, 5, txNum, 0)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, want, hash)
		return testing.AllocsPerRun(100, func() {
			_, _, _ = blockReader.TxnHashByIdxInBlock(t.Context(), tx, 5, txNum, 0)
		})
	}
	smallAllocs, largeAllocs := allocs(0, small.Hash()), allocs(1, large.Hash())
	if !race.Enabled { // the race detector drops sync.Pool puts at random
		require.Equal(t, smallAllocs, largeAllocs)
	}
}

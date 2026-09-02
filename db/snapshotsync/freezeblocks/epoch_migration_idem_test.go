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
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

// A crash between tail commits is repaired by re-writing the tail on the next run, so writing the
// same tail block twice must leave the DB byte-identical — no duplicated rows, no shifted EthTx ids.
// Every table the tail touches is a plain (non-DupSort) table written with Put, and the EthTx
// sequence is deliberately not advanced; this pins that.
func TestWriteTailBlockIsIdempotent(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)

	h := &types.Header{Number: *uint256.NewInt(7)}
	hRLP, err := rlp.EncodeToBytes(h)
	require.NoError(t, err)
	headerWord := append([]byte{0xaa}, hRLP...)
	hash := h.Hash()

	body := types.BodyForStorage{BaseTxnID: types.BaseTxnID(200), TxCount: 4}
	bodyWord, err := rlp.EncodeToBytes(&body)
	require.NoError(t, err)

	w1, raw1, _ := tailTxWord(t, 0, 1)
	w2, raw2, _ := tailTxWord(t, 1, 2)
	txWords := [][]byte{{0}, w1, w2, {0}}

	counts := func() map[string]int {
		out := map[string]int{}
		require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
			for _, tbl := range []string{kv.Headers, kv.HeaderCanonical, kv.HeaderNumber, kv.BlockBody, kv.EthTx, kv.Senders, kv.TxLookup} {
				n, err := tx.Count(tbl)
				if err != nil {
					return err
				}
				out[tbl] = int(n)
			}
			return nil
		}))
		return out
	}
	seq := func() uint64 {
		var s uint64
		require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
			var err error
			s, err = tx.ReadSequence(kv.EthTx)
			return err
		}))
		return s
	}

	commit := func() {
		require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
			return writeTailBlock(tx, headerWord, bodyWord, txWords)
		}))
	}

	commit()
	after1, seq1 := counts(), seq()
	commit() // the re-run after a crash between per-type tail commits
	after2, seq2 := counts(), seq()

	require.Equal(t, after1, after2, "second write changed row counts: %v -> %v", after1, after2)
	require.Equal(t, seq1, seq2, "second write moved the EthTx sequence")

	// and the content still reads back correctly
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		got := rawdb.ReadHeaderByNumber(tx, 7)
		require.NotNil(t, got)
		require.Equal(t, uint64(7), got.Number.Uint64())

		ch, err := rawdb.ReadCanonicalHash(tx, 7)
		require.NoError(t, err)
		require.Equal(t, hash, ch)

		bfs, err := rawdb.ReadBodyForStorageByKey(tx, dbutils.BlockBodyKey(7, hash))
		require.NoError(t, err)
		require.Equal(t, uint32(4), bfs.TxCount)
		require.Equal(t, uint64(200), bfs.BaseTxnID.U64())

		for id, want := range map[uint64][]byte{201: raw1, 202: raw2} {
			v, err := tx.GetOne(kv.EthTx, hexutil.EncodeTs(id))
			require.NoError(t, err)
			require.Equal(t, want, v, "EthTx %d", id)
		}

		senders, err := rawdb.ReadSenders(tx, hash, 7)
		require.NoError(t, err)
		require.Len(t, senders, 2, "senders duplicated on re-write")
		return nil
	}))
}

// A tail block leaves the frozen range, so the transactions-to-block index no longer covers it and
// stage_txlookup will not rebuild kv.TxLookup for it (it resumes from its own progress at the tip).
// writeTailBlock therefore has to write the lookup entry itself, with the same blockNum||txNum shape
// the stage uses, or eth_getTransactionByHash returns nothing for the whole tail.
func TestWriteTailBlockWritesTxLookup(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)

	h := &types.Header{Number: *uint256.NewInt(11)}
	hRLP, err := rlp.EncodeToBytes(h)
	require.NoError(t, err)
	headerWord := append([]byte{0xaa}, hRLP...)

	const base = uint64(500)
	body := types.BodyForStorage{BaseTxnID: types.BaseTxnID(base), TxCount: 4} // 2 real + 2 system
	bodyWord, err := rlp.EncodeToBytes(&body)
	require.NoError(t, err)

	w1, _, hash1 := tailTxWord(t, 0, 1)
	w2, _, hash2 := tailTxWord(t, 1, 2)
	txWords := [][]byte{{0}, w1, w2, {0}}

	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return writeTailBlock(tx, headerWord, bodyWord, txWords)
	}))

	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		// txWords index 1 and 2 -> txn ids base+1, base+2, matching the EthTx keys
		for want, hash := range map[uint64]common.Hash{base + 1: hash1, base + 2: hash2} {
			blockNum, txNum, err := rawdb.ReadTxLookupEntry(tx, hash)
			require.NoError(t, err)
			require.NotNil(t, blockNum, "no TxLookup entry for %x", hash)
			require.Equal(t, uint64(11), *blockNum)
			require.Equal(t, want, *txNum)
		}
		return nil
	}))
}

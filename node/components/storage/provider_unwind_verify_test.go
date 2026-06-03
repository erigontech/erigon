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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/db/kv"
	dbcfg "github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
)

// seedTxNums writes TxNums entries [blockFrom, blockTo] each mapping
// blockNum to txNum = blockNum * 10 (just a deterministic value).
// Helper for the post-unwind verifier tests which need TxNums.Last to
// return a specific block.
func seedTxNums(t *testing.T, tx kv.RwTx, blockFrom, blockTo uint64) {
	t.Helper()
	for bn := blockFrom; bn <= blockTo; bn++ {
		require.NoError(t, rawdbv3.TxNums.Append(tx, bn, bn*10))
	}
}

// seedBlockTableEntry writes a single (blockNum, hash, value) row to a
// block-keyed table (kv.Headers / kv.BlockBody / kv.HeaderTD /
// kv.Senders). Helper for the post-unwind verifier tests.
func seedBlockTableEntry(t *testing.T, tx kv.RwTx, table string, blockNum uint64, hashFirstByte byte) {
	t.Helper()
	key := make([]byte, 8+length.Hash)
	binary.BigEndian.PutUint64(key[:8], blockNum)
	key[8] = hashFirstByte
	require.NoError(t, tx.Put(table, key, []byte{0xab}))
}

func TestVerifyPostUnwindDBImage_CleanState(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	const toBlock = uint64(2_910_208)
	const lastTxNum = uint64(103_848_485)

	// TxNums [0..toBlock]: Append requires the last entry's blockNum,
	// not arbitrary. Seed by appending toBlock at lastTxNum (a single
	// entry is enough — TxNums.Last just returns the highest blockNum).
	require.NoError(t, rawdbv3.TxNums.Append(tx, toBlock, lastTxNum))

	// kv.Headers / BlockBody / HeaderTD / Senders: a row at toBlock
	// and one a few blocks below — all within range.
	for _, table := range []string{kv.Headers, kv.BlockBody, kv.HeaderTD, kv.Senders} {
		seedBlockTableEntry(t, tx, table, toBlock-100, 0x11)
		seedBlockTableEntry(t, tx, table, toBlock, 0x22)
	}

	// kv.EthTx: keys are 8-byte uint64 txnIDs. Two entries at and
	// below lastTxNum.
	for _, txnID := range []uint64{lastTxNum - 1, lastTxNum} {
		var k [8]byte
		binary.BigEndian.PutUint64(k[:], txnID)
		require.NoError(t, tx.Put(kv.EthTx, k[:], []byte{0xcd}))
	}

	// Stage progress: all at toBlock.
	for _, s := range []stages.SyncStage{stages.Headers, stages.BlockHashes, stages.Bodies, stages.Senders, stages.Execution, stages.TxLookup, stages.Finish} {
		require.NoError(t, stages.SaveStageProgress(tx, s, toBlock))
	}

	require.NoError(t, verifyPostUnwindDBImage(ctx, tx, toBlock, lastTxNum))
}

func TestVerifyPostUnwindDBImage_TxNumsLastWrong(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	const toBlock = uint64(2_910_208)
	const lastTxNum = uint64(103_848_485)
	// TxNums.Last returns toBlock+1 — wipe didn't truncate cleanly.
	require.NoError(t, rawdbv3.TxNums.Append(tx, toBlock+1, lastTxNum+10))

	err = verifyPostUnwindDBImage(ctx, tx, toBlock, lastTxNum)
	require.Error(t, err)
	require.Contains(t, err.Error(), "TxNums.Last=")
}

func TestVerifyPostUnwindDBImage_HeadersOrphan(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	const toBlock = uint64(2_910_208)
	const lastTxNum = uint64(103_848_485)
	require.NoError(t, rawdbv3.TxNums.Append(tx, toBlock, lastTxNum))

	// kv.Headers has an entry at toBlock+5 — wipe missed it.
	seedBlockTableEntry(t, tx, kv.Headers, toBlock+5, 0x33)

	err = verifyPostUnwindDBImage(ctx, tx, toBlock, lastTxNum)
	require.Error(t, err)
	require.Contains(t, err.Error(), kv.Headers)
	require.Contains(t, err.Error(), "last blockNum=")
}

func TestVerifyPostUnwindDBImage_EthTxOrphan(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	const toBlock = uint64(2_910_208)
	const lastTxNum = uint64(103_848_485)
	require.NoError(t, rawdbv3.TxNums.Append(tx, toBlock, lastTxNum))

	// EthTx has a txnID past lastTxNum — wipe missed it.
	var k [8]byte
	binary.BigEndian.PutUint64(k[:], lastTxNum+100)
	require.NoError(t, tx.Put(kv.EthTx, k[:], []byte{0xab}))

	err = verifyPostUnwindDBImage(ctx, tx, toBlock, lastTxNum)
	require.Error(t, err)
	require.Contains(t, err.Error(), "EthTx last txnID=")
}

func TestVerifyPostUnwindDBImage_StagePastToBlock(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	const toBlock = uint64(2_910_208)
	const lastTxNum = uint64(103_848_485)
	require.NoError(t, rawdbv3.TxNums.Append(tx, toBlock, lastTxNum))

	// Execution stage progress is at toBlock+50 — stage wasn't reset.
	require.NoError(t, stages.SaveStageProgress(tx, stages.Execution, toBlock+50))

	err = verifyPostUnwindDBImage(ctx, tx, toBlock, lastTxNum)
	require.Error(t, err)
	require.Contains(t, err.Error(), "stage Execution progress=")
}

func TestVerifyPostUnwindDBImage_MultipleFailuresCombined(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	const toBlock = uint64(2_910_208)
	const lastTxNum = uint64(103_848_485)
	// Multiple violations — verifier should surface all in one error.
	require.NoError(t, rawdbv3.TxNums.Append(tx, toBlock+1, lastTxNum+10))
	seedBlockTableEntry(t, tx, kv.BlockBody, toBlock+5, 0x44)
	require.NoError(t, stages.SaveStageProgress(tx, stages.Finish, toBlock+50))

	err = verifyPostUnwindDBImage(ctx, tx, toBlock, lastTxNum)
	require.Error(t, err)
	require.Contains(t, err.Error(), "TxNums.Last=")
	require.Contains(t, err.Error(), kv.BlockBody)
	require.Contains(t, err.Error(), "stage Finish progress=")
}

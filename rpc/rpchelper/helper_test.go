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

package rpchelper

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/membatchwithdb"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/rpc"
)

type rawdbBlockReader struct {
	dbservices.FullBlockReader
}

func (rawdbBlockReader) CanonicalHash(_ context.Context, tx kv.Getter, blockNum uint64) (common.Hash, bool, error) {
	h, err := rawdb.ReadCanonicalHash(tx, blockNum)
	return h, h != (common.Hash{}), err
}

func (rawdbBlockReader) HeaderNumber(_ context.Context, tx kv.Getter, hash common.Hash) (*uint64, error) {
	return rawdb.ReadHeaderNumber(tx, hash), nil
}

var (
	byNumber = rpc.BlockNumberOrHashWithNumber
	sideHash = common.Hash{0x02, 2}
)

func hashOf(n uint64) common.Hash { return common.Hash{0x01, byte(n)} }

func byHash(h common.Hash) rpc.BlockNumberOrHash { return rpc.BlockNumberOrHashWithHash(h, true) }

// newExecutedChainTx returns a tx with canonical blocks 0..5, a side block at
// height 2 and execution progress at 3.
func newExecutedChainTx(t *testing.T) kv.TemporalRwTx {
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	tx, err := db.BeginTemporalRw(t.Context()) //nolint:gocritic
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)
	for n := uint64(0); n <= 5; n++ {
		require.NoError(t, rawdb.WriteCanonicalHash(tx, hashOf(n), n))
		require.NoError(t, rawdb.WriteHeaderNumber(tx, hashOf(n), n))
	}
	require.NoError(t, rawdb.WriteHeaderNumber(tx, sideHash, 2))
	require.NoError(t, stages.SaveStageProgress(tx, stages.Execution, 3))
	return tx
}

type failingBlockReader struct {
	rawdbBlockReader
	err error
}

func (r failingBlockReader) CanonicalHash(context.Context, kv.Getter, uint64) (common.Hash, bool, error) {
	return common.Hash{}, false, r.err
}

func TestResolveExecutedBlock(t *testing.T) {
	tx := newExecutedChainTx(t)
	rawdb.WriteForkchoiceHead(tx, hashOf(5))
	rawdb.WriteForkchoiceSafe(tx, hashOf(2))
	rawdb.WriteForkchoiceFinalized(tx, hashOf(1))

	for _, tc := range []struct {
		name       string
		sel        rpc.BlockNumberOrHash
		wantNumber uint64
		wantLatest bool
	}{
		{"number below execution", byNumber(2), 2, false},
		{"number at execution", byNumber(3), 3, true},
		{"earliest", byNumber(rpc.EarliestBlockNumber), 0, false},
		{"latestExecuted", byNumber(rpc.LatestExecutedBlockNumber), 3, true},
		{"safe", byNumber(rpc.SafeBlockNumber), 2, false},
		{"finalized", byNumber(rpc.FinalizedBlockNumber), 1, false},
		{"canonical hash", byHash(hashOf(3)), 3, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			number, hash, latest, err := ResolveExecutedBlock(t.Context(), true, tc.sel, tx, rawdbBlockReader{})
			require.NoError(t, err)
			require.Equal(t, tc.wantNumber, number)
			require.Equal(t, hashOf(tc.wantNumber), hash)
			require.Equal(t, tc.wantLatest, latest)
		})
	}

	for _, tc := range []struct {
		name string
		sel  rpc.BlockNumberOrHash
	}{
		{"number above execution", byNumber(4)},
		{"latest above execution", byNumber(rpc.LatestBlockNumber)},
		{"canonical hash above execution", byHash(hashOf(4))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, _, err := ResolveExecutedBlock(t.Context(), true, tc.sel, tx, rawdbBlockReader{})
			var notExecuted *BlockNotExecutedError
			require.ErrorAs(t, err, &notExecuted)
			require.Equal(t, uint64(3), notExecuted.LastExecuted)
		})
	}

	t.Run("unknown number", func(t *testing.T) {
		_, _, _, err := ResolveExecutedBlock(t.Context(), true, byNumber(9), tx, rawdbBlockReader{})
		require.ErrorAs(t, err, &rpc.BlockNotFoundErr{})
	})

	t.Run("non-canonical hash", func(t *testing.T) {
		_, _, _, err := ResolveExecutedBlock(t.Context(), true, byHash(sideHash), tx, rawdbBlockReader{})
		require.ErrorAs(t, err, &NonCanonicalHashError{})
	})

	t.Run("non-canonical hash not required canonical", func(t *testing.T) {
		number, hash, _, err := ResolveExecutedBlock(t.Context(), false, byHash(sideHash), tx, rawdbBlockReader{})
		require.NoError(t, err)
		require.Equal(t, uint64(2), number)
		require.Equal(t, sideHash, hash)
	})
}

func TestResolveExecutedBlockLatestWithoutForkchoiceHead(t *testing.T) {
	tx := newExecutedChainTx(t)

	number, hash, latest, err := ResolveExecutedBlock(t.Context(), true, byNumber(rpc.LatestBlockNumber), tx, rawdbBlockReader{})
	require.NoError(t, err)
	require.Equal(t, uint64(3), number)
	require.Equal(t, hashOf(3), hash)
	require.True(t, latest)
}

func TestResolveExecutedBlockReadsOnlyGivenTx(t *testing.T) {
	tx := newExecutedChainTx(t)
	overlay, err := membatchwithdb.NewMemoryBatch(tx)
	require.NoError(t, err)
	defer overlay.Close()
	require.NoError(t, stages.SaveStageProgress(overlay, stages.Execution, 5))

	_, _, _, err = ResolveExecutedBlock(t.Context(), true, byNumber(5), tx, rawdbBlockReader{})
	var notExecuted *BlockNotExecutedError
	require.ErrorAs(t, err, &notExecuted)
	require.Equal(t, uint64(3), notExecuted.LastExecuted)

	number, _, latest, err := ResolveExecutedBlock(t.Context(), true, byNumber(5), overlay.NewReadView(tx), rawdbBlockReader{})
	require.NoError(t, err)
	require.Equal(t, uint64(5), number)
	require.True(t, latest)
}

func TestResolveExecutedBlockKeepsReaderError(t *testing.T) {
	tx := newExecutedChainTx(t)
	readerErr := errors.New("block reader failure")

	_, _, _, err := ResolveExecutedBlock(t.Context(), true, byNumber(2), tx, failingBlockReader{err: readerErr})
	require.ErrorIs(t, err, readerErr)
	require.NotErrorAs(t, err, &rpc.BlockNotFoundErr{})
	var notExecuted *BlockNotExecutedError
	require.NotErrorAs(t, err, &notExecuted)
}

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
// Package l1test holds the L1 reader conformance suite (T-L1-1) and the
// chains it runs on.
package l1test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/l1/recording"
	"github.com/erigontech/erigon/execution/types"
)

// Filter is a log query the suite issues.
type Filter struct {
	Address common.Address
	Topics  [][]common.Hash
}

// Conformance runs the T-L1-1 scenarios against r and requires the answers
// want gives. Every implementation must pass it on the same chain.
func Conformance(t *testing.T, r l1.ChainReader, want *recording.MemoryChain, filters []Filter) {
	t.Helper()
	ctx := context.Background()

	wantID, err := want.ChainID(ctx)
	require.NoError(t, err)
	id, err := r.ChainID(ctx)
	require.NoError(t, err)
	require.Equal(t, wantID, id, "chain id")

	canonical := want.Canonical()
	head := canonical[len(canonical)-1].Header
	got, err := r.Head(ctx)
	require.NoError(t, err)
	require.Equal(t, head, *got, "head")

	wantFinal, wantErr := want.Finalized(ctx)
	gotFinal, err := r.Finalized(ctx)
	if wantErr != nil {
		require.ErrorIs(t, err, l1.ErrNotFound, "finalized before finality")
	} else {
		require.NoError(t, err)
		require.Equal(t, *wantFinal, *gotFinal, "finalized")
	}

	for _, b := range canonical {
		got, err := r.HeaderByNumber(ctx, b.Header.Number)
		require.NoError(t, err, "header %d", b.Header.Number)
		require.Equal(t, b.Header, *got, "header %d by number", b.Header.Number)
		got, err = r.HeaderByHash(ctx, b.Header.Hash)
		require.NoError(t, err, "header %x", b.Header.Hash)
		require.Equal(t, b.Header, *got, "header %d by hash", b.Header.Number)
	}
	_, err = r.HeaderByNumber(ctx, head.Number+1)
	require.ErrorIs(t, err, l1.ErrNotFound, "header above the head")
	for _, b := range want.Orphans() {
		got, err := r.HeaderByHash(ctx, b.Header.Hash)
		if err != nil {
			require.ErrorIs(t, err, l1.ErrNotFound, "orphan %x", b.Header.Hash)
			continue
		}
		require.Equal(t, b.Header, *got, "orphan %x", b.Header.Hash)
	}
	_, err = r.HeaderByHash(ctx, common.Hash{0xde, 0xad})
	require.ErrorIs(t, err, l1.ErrNotFound, "unknown header")

	for _, f := range filters {
		for _, span := range [][2]uint64{{0, head.Number}, {1, head.Number - 1}, {head.Number, head.Number}} {
			wantLogs, err := want.Logs(ctx, span[0], span[1], f.Address, f.Topics)
			require.NoError(t, err)
			gotLogs, err := r.Logs(ctx, span[0], span[1], f.Address, f.Topics)
			require.NoError(t, err)
			requireLogs(t, wantLogs, gotLogs, "logs of [%d, %d] from %x", span[0], span[1], f.Address)
		}
	}

	for _, b := range canonical {
		for i, tx := range b.Txs {
			got, err := r.TransactionByBlockHashAndIndex(ctx, b.Header.Hash, uint64(i))
			require.NoError(t, err, "transaction %d of block %d", i, b.Header.Number)
			require.Equal(t, tx.Hash(), got.Hash(), "transaction %d of block %d", i, b.Header.Number)
		}
		_, err := r.TransactionByBlockHashAndIndex(ctx, b.Header.Hash, uint64(len(b.Txs)))
		require.ErrorIs(t, err, l1.ErrNotFound, "transaction past the last of block %d", b.Header.Number)
	}
}

func requireLogs(t *testing.T, want, got []*types.Log, msg string, args ...any) {
	t.Helper()
	require.Len(t, got, len(want), append([]any{msg}, args...)...)
	for i := range want {
		w, g := want[i], got[i]
		require.Equal(t, w.Address, g.Address)
		require.Equal(t, w.Topics, g.Topics)
		require.Equal(t, []byte(w.Data), []byte(g.Data))
		require.Equal(t, w.BlockNumber, g.BlockNumber)
		require.Equal(t, w.BlockHash, g.BlockHash)
		require.Equal(t, w.TxHash, g.TxHash)
		require.Equal(t, w.TxIndex, g.TxIndex)
		require.Equal(t, w.Index, g.Index)
	}
}

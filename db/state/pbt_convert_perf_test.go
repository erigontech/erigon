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

package state_test

import (
	"bytes"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/v3/pbt"
	"github.com/erigontech/erigon/internal/commitmenttest/temporal"
)

type pbinCommitmentReadCounter struct {
	kv.TemporalRwTx
	reads *atomic.Uint64
}

func (tx pbinCommitmentReadCounter) GetLatest(domain kv.Domain, k []byte, opts kv.GetLatestOptions) ([]byte, kv.Step, error) {
	if domain == kv.CommitmentBinDomain && !bytes.Equal(k, pbt.GlobalRootKey()) && !bytes.Equal(k, commitment.KeyCommitmentState) {
		tx.reads.Add(1)
	}
	return tx.TemporalRwTx.GetLatest(domain, k, opts)
}

func TestConvertPBinDetachDoesNotReadMDBX(t *testing.T) {
	selectPBinConvertHash(t)
	db, agg := temporal.Open(t, 8)
	writePBinConvertState(t, db)
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, 1, unboundedFinalityCtx, false))
	agg.WaitForFiles()
	sourceTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer sourceTx.Rollback()
	targetTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer targetTx.Rollback()
	var reads atomic.Uint64
	_, err = state.ConvertPBin(t.Context(), state.PBinConvertOptions{
		SourceAggregator: agg,
		SourceTx:         sourceTx,
		TargetAggregator: agg,
		TargetTx:         pbinCommitmentReadCounter{TemporalRwTx: targetTx, reads: &reads},
		TargetDomain:     kv.CommitmentBinDomain,
		EndTxNum:         7,
		Hash:             eip8297.HashBytes,
	})
	require.NoError(t, err)
	require.Zero(t, reads.Load(), "detach must not read previous commitment values")
}

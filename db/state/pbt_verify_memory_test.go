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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/v3/pbt"
	"github.com/erigontech/erigon/internal/commitmenttest/temporal"
)

func TestPBinVerifyStorageRowsStayBounded(t *testing.T) {
	const (
		maxLiveRows  = 1024
		maxLiveBytes = 1 << 20
	)
	selectPBinBinOnlyHash(t)
	db, agg := temporal.Open(t, 8)
	writePBinConvertStateN(t, db, 4096)
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, 1, unboundedFinalityCtx, false))
	agg.WaitForFiles()
	sourceTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer sourceTx.Rollback()
	targetTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	_, err = state.ConvertPBin(t.Context(), state.PBinConvertOptions{
		SourceAggregator: agg,
		SourceTx:         sourceTx,
		TargetAggregator: agg,
		TargetTx:         targetTx,
		TargetDomain:     kv.CommitmentDomain,
		EndTxNum:         7,
		Hash:             eip8297.HashBytes,
	})
	require.NoError(t, err)
	targetTx.Rollback()
	verifyTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer verifyTx.Rollback()
	stats := new(pbt.VerifyStats)
	_, err = state.VerifyPBinDomainRootWithStats(t.Context(), verifyTx, agg, kv.CommitmentDomain, stats)
	require.NoError(t, err)
	require.Greater(t, stats.RangeRows.Load(), uint64(1))
	require.Less(t, stats.PeakRows.Load(), uint64(maxLiveRows),
		"verification retained too many rows: peak=%d bound=%d", stats.PeakRows.Load(), maxLiveRows)
	require.Less(t, stats.PeakBytes.Load(), uint64(maxLiveBytes),
		"verification retained too many bytes: peak=%d bound=%d", stats.PeakBytes.Load(), maxLiveBytes)
}

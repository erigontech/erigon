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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

func TestPBinR16ResumeConsultsBaseRowsForAbsent(t *testing.T) {
	selectPBinLeafStreamHash(t)
	baseDB, baseAgg := testDbAndAggregatorv3(t, pbinLeafStreamStepSize)
	sourceDB, sourceAgg := testDbAndAggregatorv3(t, pbinLeafStreamStepSize)
	targetDB, targetAgg := testDbAndAggregatorv3(t, pbinLeafStreamStepSize)
	base := []pbinLeafStreamAccount{
		{address: pbinLeafStreamAddress(0x11), nonce: 1, balance: 1},
		{address: pbinLeafStreamAddress(0x22), nonce: 1, balance: 1},
		{address: pbinLeafStreamAddress(0x33), nonce: 1, balance: 1},
	}
	updated := []pbinLeafStreamAccount{
		{address: pbinLeafStreamAddress(0x11), nonce: 2, balance: 2},
		{address: pbinLeafStreamAddress(0x22), nonce: 2, balance: 2},
		{address: pbinLeafStreamAddress(0x44), nonce: 1, balance: 1},
	}
	writePBinLeafStreamRange(t, baseDB, 0, 8, base)
	require.NoError(t, baseAgg.BuildFiles2(t.Context(), baseDB, 0, 1, unboundedFinalityCtx, false))
	baseAgg.WaitForFiles()
	writePBinLeafStreamRange(t, sourceDB, 0, 8, base)
	writePBinLeafStreamStates(t, sourceDB, 8, updated)
	require.NoError(t, sourceAgg.BuildFiles2(t.Context(), sourceDB, 0, 2, unboundedFinalityCtx, false))
	sourceAgg.WaitForFiles()
	writePBinLeafStreamRange(t, targetDB, 0, 8, base)
	writePBinLeafStreamStates(t, targetDB, 8, updated)
	writePBinLeafStreamStates(t, targetDB, 16, updated)
	require.NoError(t, targetAgg.BuildFiles2(t.Context(), targetDB, 0, 3, unboundedFinalityCtx, false))
	targetAgg.WaitForFiles()
	convertPBinBetween(t, baseDB, baseAgg, targetDB, targetAgg, 8)
	rows := pbinCommitmentRows(t, targetAgg)
	var baseKey []byte
	for key := range rows {
		if key != string(commitment.KeyCommitmentState) {
			baseKey = []byte(key)
			break
		}
	}
	require.NotEmpty(t, baseKey)
	var result atomic.Int32
	probe := func(isAbsent func([]byte) bool) {
		if isAbsent(baseKey) {
			result.Store(1)
			return
		}
		result.Store(2)
	}
	resumePBinAtWithAbsent(t, sourceDB, sourceAgg, targetDB, targetAgg, 16, probe)
	require.Equal(t, int32(2), result.Load(), "resume must consult an existing base row through Absent")
}

func resumePBinAtWithAbsent(t *testing.T, sourceDB kv.TemporalRwDB, sourceAgg *state.Aggregator, targetDB kv.TemporalRwDB, targetAgg *state.Aggregator, endTxNum uint64, probe state.PBinAbsentProbeFunc) common.Hash {
	t.Helper()
	sourceTx, err := sourceDB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer sourceTx.Rollback()
	targetTx, err := targetDB.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer targetTx.Rollback()
	root, err := state.ConvertPBin(t.Context(), state.PBinConvertOptions{
		SourceAggregator: sourceAgg,
		SourceTx:         sourceTx,
		TargetAggregator: targetAgg,
		TargetTx:         targetTx,
		TargetDB:         targetDB,
		TargetDomain:     kv.CommitmentDomain,
		EndTxNum:         endTxNum,
		Hash:             eip8297.HashBytes,
		RangeWriterLimits: &state.PBinRangeWriterLimits{
			AbsentProbe: probe,
		},
		Workers: 4,
		Resume:  &state.PBinResumeOptions{BaseTxNum: 8, SourceTxNum: endTxNum, Workers: 4, PostCancun: true},
	})
	require.NoError(t, err)
	return root
}

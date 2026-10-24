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
)

func TestPBinR14ResumeRejectsBasePastY(t *testing.T) {
	selectPBinLeafStreamHash(t)
	baseDB, baseAgg := testDbAndAggregatorv3(t, 8)
	targetDB, targetAgg := testDbAndAggregatorv3(t, 8)
	initial := []pbinLeafStreamAccount{{address: append(make([]byte, 19), 1), nonce: 1, balance: 1}}
	writePBinLeafStreamRange(t, baseDB, 0, 8, initial)
	require.NoError(t, baseAgg.BuildFiles2(t.Context(), baseDB, 0, 1, unboundedFinalityCtx, false))
	baseAgg.WaitForFiles()
	writePBinLeafStreamRange(t, targetDB, 0, 32, initial)
	require.NoError(t, targetAgg.BuildFiles2(t.Context(), targetDB, 0, 4, unboundedFinalityCtx, false))
	targetAgg.WaitForFiles()
	convertPBinBetween(t, baseDB, baseAgg, targetDB, targetAgg, 8)
	resumePBinAt(t, baseDB, baseAgg, targetDB, targetAgg, 24)
	sourceTx, err := baseDB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer sourceTx.Rollback()
	targetTx, err := targetDB.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer targetTx.Rollback()
	root, err := state.ConvertPBin(t.Context(), state.PBinConvertOptions{
		SourceAggregator: baseAgg,
		SourceTx:         sourceTx,
		TargetAggregator: targetAgg,
		TargetTx:         targetTx,
		TargetDB:         targetDB,
		TargetDomain:     kv.CommitmentDomain,
		EndTxNum:         16,
		Hash:             eip8297.HashBytes,
		Workers:          4,
		Resume:           &state.PBinResumeOptions{BaseTxNum: 8, SourceTxNum: 16, PostCancun: true, Workers: 4},
	})
	require.Error(t, err, "resume accepted base state at 24 beyond Y=16: err=%v root=%x", err, root)
}

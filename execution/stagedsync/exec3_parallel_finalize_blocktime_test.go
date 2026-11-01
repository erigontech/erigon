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

package stagedsync

import (
	"context"
	"math"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestParallelFinalizeResultCarriesBlockTimeToStepBoundary(t *testing.T) {
	const stepEdgeTxNum = uint64(15)
	binaryTrieTime := uint64(100)

	db := newResumeTestDB(t)
	config := chain.TestChainBerlinConfig
	pe, roTx := newResumeTestExec(t, db, config)
	beneficiary := accounts.InternAddress(common.Address{19: 0x42})
	pe.cfg.engine = rulesEngineWithFinalizeBalance{Engine: pe.cfg.engine, beneficiary: beneficiary}

	be, task := newParallelResultTestBlock(config)
	header := &types.Header{Number: *uint256.NewInt(1), GasLimit: 10_000_000, Time: binaryTrieTime}
	be.block = types.NewBlockFromStorage(common.Hash{}, header, nil, nil, nil, nil)
	be.applyResults = make(chan applyResult, 8)
	txTask := task.Task.(*exec.TxTask)
	txTask.Header = header
	txTask.TxNum = stepEdgeTxNum
	task.version.TxNum = stepEdgeTxNum

	result, err := be.nextResult(context.Background(), pe, &exec.TxResult{
		Task:  task,
		TxIn:  state.ReadSet{},
		TxOut: &state.WriteSet{},
	}, roTx)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NoError(t, result.Err)
	close(be.applyResults)

	dualDB, tx, _ := dualCalculatorTest(t)
	binDoms, err := execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithCommitmentDomain(kv.CommitmentBinDomain))
	require.NoError(t, err)
	t.Cleanup(binDoms.Close)
	binDoms.SetInMemHistoryReads(true)
	binDoms.SetDisableInlineTouchKey(true)
	out := make(chan commitmentResult, 8)
	cc, err := newCommitmentCalculator(t.Context(), t.Context(), binDoms, dualDB, &chain.Config{BinaryTrieTime: &binaryTrieTime}, "test", log.New(), false, math.MaxUint64, nil, nil, out)
	require.NoError(t, err)
	t.Cleanup(cc.Stop)
	cc.stopShadowDomain(kv.CommitmentDomain)

	sawFinalizeWrites := false
	for r := range be.applyResults {
		tr, ok := r.(*txResult)
		if !ok || tr.writes.IsEmpty() {
			continue
		}
		sawFinalizeWrites = true
		cc.handleMessage(t.Context(), tr)
	}
	require.True(t, sawFinalizeWrites, "the producer must emit a finalize result with writes")
	select {
	case res := <-out:
		require.NoError(t, res.err)
	default:
	}
}

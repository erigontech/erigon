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
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
)

func TestParallelResumedBlockEndLeavesInclusionListUnchecked(t *testing.T) {
	ctx := context.Background()
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	config := chain.TestChainBerlinConfig

	pe, roTx := newResumeTestExec(t, db, config)
	pe.cfg.engine = ethash.NewFaker()
	pe.cfg.vmConfig = &vm.Config{}
	pe.cfg.readAheader = exec.NewBlockReadAheader()

	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	signer := *types.LatestSignerForChainID(config.ChainID)
	included, err := types.SignTx(types.NewTransaction(0, common.Address{}, uint256.NewInt(0), params.TxGas, uint256.NewInt(0), nil), signer, key)
	require.NoError(t, err)
	omitted, err := types.SignTx(types.NewTransaction(1, common.Address{}, uint256.NewInt(0), params.TxGas, uint256.NewInt(0), nil), signer, key)
	require.NoError(t, err)

	header := &types.Header{Number: *uint256.NewInt(1), GasLimit: 10_000_000}
	blockHash := header.Hash()
	pe.cfg.readAheader.AddInclusionList(blockHash, types.Transactions{omitted})

	gasPool := new(protocol.GasPool).AddGas(10_000_000)
	be := newBlockExec(1, blockHash, gasPool, nil, newResultStream(), false, nil)
	endTask := &exec.TxTask{Header: header, Txs: types.Transactions{included}, TxNum: 2, TxIndex: 1, Config: config}
	eTaskEnd := &execTask{Task: endTask, index: 0}
	endVersion := &taskVersion{
		execTask: eTaskEnd,
		version:  state.Version{BlockNum: 1, TxIndex: 1, Incarnation: 1, TxNum: 2},
	}
	be.tasks = []*execTask{eTaskEnd}
	be.results = []*execResult{{TxResult: &exec.TxResult{Task: endVersion}}}
	be.finalizedResults[0] = &execResult{TxResult: &exec.TxResult{Task: endVersion}}
	be.execTasks.setComplete(0)
	be.validateTasks.setComplete(0)
	be.publishTasks.setComplete(0)

	_, err = be.nextResult(ctx, pe, &exec.TxResult{Task: endVersion}, roTx)
	require.NoError(t, err)

	_, ilHash, ok := pe.cfg.readAheader.ReadInclusionList(blockHash)
	require.True(t, ok)
	_, checked := pe.cfg.readAheader.ReadInclusionListResult(blockHash, ilHash)
	require.False(t, checked)
}

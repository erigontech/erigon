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
	"errors"
	"fmt"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/protocol/rules/merge"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
	"github.com/erigontech/erigon/node/shards"
)

type failingSerialWorkerDB struct {
	kv.TemporalRoDB
	err error
}

func (db failingSerialWorkerDB) BeginTemporalRo(context.Context) (kv.TemporalTx, error) {
	return nil, db.err
}

func TestSerialTaskOperationalErrorPassesThrough(t *testing.T) {
	cause := errors.New("worker database unavailable")
	config := chain.TestChainBerlinConfig
	logger := log.New()
	worker := exec.NewWorker(context.Background(), true, exec.NewWorkerMetrics(), failingSerialWorkerDB{err: cause}, nil,
		nil, config, nil, nil, nil, datadir.Dirs{}, logger)
	t.Cleanup(worker.Close)
	require.NoError(t, worker.ResetState(nil, nil, state.NewNoopReader(), state.NewNoopWriter(), nil))

	se := &serialExecutor{
		txExecutor: txExecutor{
			cfg: ExecuteBlockCfg{
				chainConfig: config,
			},
			logger: logger,
		},
		worker: worker,
	}
	header := &types.Header{Number: *uint256.NewInt(1), GasLimit: 10_000_000}
	task := &exec.TxTask{Header: header, TxNum: 1, TxIndex: 0}
	block := types.NewBlockFromStorage(common.Hash{}, header, nil, nil, nil, nil)

	_, err := se.executeBlock(context.Background(), block, []exec.Task{task}, false, false)

	require.ErrorIs(t, err, cause)
	require.NotErrorIs(t, err, rules.ErrInvalidBlock)
}

func newSerialFinalizeTestExec(t *testing.T, engine rules.Engine) (*serialExecutor, *exec.TxTask) {
	t.Helper()

	config := chain.TestChainOsakaConfig
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)
	tx, domains := temporaltest.NewTestTxSD(t, db)
	rs := state.NewStateV3Buffered(state.NewStateV3(domains, false, logger))
	worker := exec.NewWorker(t.Context(), false, exec.NewWorkerMetrics(), db, nil,
		nil, config, nil, nil, engine, dirs, logger)
	t.Cleanup(worker.Close)
	require.NoError(t, worker.ResetState(rs, nil, state.NewNoopReader(), state.NewNoopWriter(), nil))

	se := &serialExecutor{
		txExecutor: txExecutor{
			cfg: ExecuteBlockCfg{
				chainConfig:   config,
				engine:        engine,
				vmConfig:      &vm.Config{},
				notifications: shards.NewNotifications(nil),
			},
			doms:    domains,
			rs:      rs,
			applyTx: tx,
			logger:  logger,
		},
		worker: worker,
	}
	header := &types.Header{Number: *uint256.NewInt(1), GasLimit: 10_000_000}
	task := &exec.TxTask{
		Header:          header,
		TxNum:           1,
		TxIndex:         0,
		EvmBlockContext: evmtypes.BlockContext{BlockNumber: 1},
	}
	return se, task
}

func TestSerialFinalizeStateReadErrorPassesThrough(t *testing.T) {
	se, task := newSerialFinalizeTestExec(t, merge.New(ethash.NewFaker()))
	cause := errors.New("withdrawal account read failed")
	beneficiary := common.Address{19: 0x42}
	se.applyTx = failingAccountTemporalTx{TemporalTx: se.applyTx, address: beneficiary, err: cause}
	task.Withdrawals = []*types.Withdrawal{{Address: beneficiary, Amount: 1}}
	block := types.NewBlockFromStorage(common.Hash{}, task.Header, nil, nil, task.Withdrawals, nil)

	_, err := se.executeBlock(t.Context(), block, []exec.Task{task}, false, false)

	require.ErrorIs(t, err, cause)
	require.NotErrorIs(t, err, rules.ErrInvalidBlock)
}

func TestSerialFinalizeClassifiesRulesEngineError(t *testing.T) {
	for _, tc := range []struct {
		name      string
		engineErr func(error) error
	}{
		{
			name:      "plain error",
			engineErr: func(cause error) error { return cause },
		},
		{
			name: "preclassified invalid block",
			engineErr: func(cause error) error {
				return fmt.Errorf("%w: %w", rules.ErrInvalidBlock, cause)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cause := errors.New("rules engine finalize failed")
			engine := rulesEngineWithErrors{Engine: ethash.NewFaker(), finalizeErr: tc.engineErr(cause)}
			se, task := newSerialFinalizeTestExec(t, engine)
			block := types.NewBlockFromStorage(common.Hash{}, task.Header, nil, nil, nil, nil)

			_, err := se.executeBlock(t.Context(), block, []exec.Task{task}, false, false)

			require.ErrorIs(t, err, cause)
			require.ErrorIs(t, err, rules.ErrInvalidBlock)
		})
	}
}

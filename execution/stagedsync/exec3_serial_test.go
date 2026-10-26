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
	"errors"
	"fmt"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

type rulesEngineWithErrors struct {
	rules.Engine
	finalizeErr error
}

func (e rulesEngineWithErrors) Finalize(config *chain.Config, header *types.Header, ibs *state.IntraBlockState,
	uncles []*types.Header, receipts types.Receipts, withdrawals []*types.Withdrawal,
	chainReader rules.ChainReader, syscall rules.SystemCall, skipReceiptsEval bool, logger log.Logger,
) (types.FlatRequests, error) {
	return nil, e.finalizeErr
}

func newSerialFinalizeTestExec(t *testing.T, engine rules.Engine) (*serialExecutor, *exec.TxTask) {
	t.Helper()

	config := chain.TestChainOsakaConfig
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)
	tx, domains := temporaltest.NewTestTxSD(t, db)
	rs := state.NewStateV3Buffered(state.NewStateV3(domains, false, logger))
	worker := exec.NewWorkerContext(t.Context(), false, exec.NewWorkerMetrics(), db,
		nil, config, nil, engine, dirs, logger)
	t.Cleanup(func() { _ = worker.ResetTx(nil) })
	require.NoError(t, worker.ResetState(rs, nil, state.NewNoopReader(), state.NewNoopWriter(), nil))

	se := &serialExecutor{
		txExecutor: txExecutor{
			cfg: ExecuteBlockCfg{
				chainConfig: config,
				engine:      engine,
				vmConfig:    &vm.Config{},
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

// A plain rules-engine finalize error becomes ErrInvalidBlock (block verdict); one
// already carrying ErrInvalidBlock keeps it. Either way the underlying cause survives.
func TestSerialFinalizeClassifiesRulesEngineError(t *testing.T) {
	for _, tc := range []struct {
		name      string
		engineErr func(error) error
	}{
		{name: "plain error", engineErr: func(cause error) error { return cause }},
		{name: "preclassified invalid block", engineErr: func(cause error) error {
			return fmt.Errorf("%w: %w", rules.ErrInvalidBlock, cause)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cause := errors.New("rules engine finalize failed")
			engine := rulesEngineWithErrors{Engine: ethash.NewFaker(), finalizeErr: tc.engineErr(cause)}
			se, task := newSerialFinalizeTestExec(t, engine)

			_, err := se.executeBlock(t.Context(), []exec.Task{task}, false, false)

			require.ErrorIs(t, err, cause)
			require.ErrorIs(t, err, rules.ErrInvalidBlock)
		})
	}
}

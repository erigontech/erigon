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
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

// failingAccountStateReader makes every account read fail, driving TxTask.Execute's
// state-read-error -> Operational path.
type failingAccountStateReader struct {
	*state.NoopReader
	err error
}

func (r failingAccountStateReader) ReadAccountData(accounts.Address) (*accounts.Account, error) {
	return nil, r.err
}

// panickingAccountStateReader panics on an account read; the transition recovers it into
// protocol.ErrExecPanic, which TxTask.Execute routes as Operational.
type panickingAccountStateReader struct {
	*state.NoopReader
	panicValue any
}

func (r panickingAccountStateReader) ReadAccountData(accounts.Address) (*accounts.Account, error) {
	panic(r.panicValue)
}

// rulesEngineWithInitError carries a block-init error (rulesEngineWithErrors only
// carries a finalize error).
type rulesEngineWithInitError struct {
	rules.Engine
	initializeErr error
}

func (e rulesEngineWithInitError) Initialize(config *chain.Config, chainReader rules.ChainHeaderReader, header *types.Header,
	ibs *state.IntraBlockState, syscall rules.SysCallCustom, logger log.Logger, tracer *tracing.Hooks,
) error {
	return e.initializeErr
}

// rulesEngineWithFinalizeBalance makes Finalize touch an account (read-then-add) so a failing
// applyTx surfaces a finalize state-read error.
type rulesEngineWithFinalizeBalance struct {
	rules.Engine
	beneficiary accounts.Address
}

func (e rulesEngineWithFinalizeBalance) Finalize(_ *chain.Config, _ *types.Header, ibs *state.IntraBlockState,
	_ []*types.Header, _ types.Receipts, _ []*types.Withdrawal, _ rules.ChainReader, _ rules.SystemCall,
	_ bool, _ log.Logger,
) (types.FlatRequests, error) {
	return nil, ibs.AddBalance(e.beneficiary, *uint256.NewInt(1), tracing.BalanceIncreaseWithdrawal)
}

// newParallelResultTestBlock builds a single-task blockExecutor directly,
// avoiding the scheduler: processRequest only registers an executor
// once a block-end task arrives, so a tx-carrying task would leave pe.blockExecutors empty.
// The taskVersion carries the executor's versionMap so a direct task.Execute writes into the
// same map nextResult validates against.
func newParallelResultTestBlock(t *testing.T, pe *parallelExecutor, config *chain.Config, txs ...types.Transaction) (*blockExecutor, *taskVersion) {
	t.Helper()
	pe.workersCtx = t.Context()

	header := &types.Header{Number: *uint256.NewInt(1), GasLimit: 10_000_000}
	be := newBlockExec(1, common.Hash{}, new(protocol.GasPool).AddGas(header.GasLimit), nil, discardConsumers(16), false, nil)
	txTask := &exec.TxTask{
		Header:          header,
		TxNum:           1,
		TxIndex:         0,
		Config:          config,
		Txs:             txs,
		Logger:          log.New(),
		EvmBlockContext: evmtypes.BlockContext{BlockNumber: 1},
	}
	eTask := &execTask{Task: txTask, index: 0}
	be.tasks = []*execTask{eTask}
	be.results = make([]*execResult, 1)
	be.execTasks.pushPending(0)
	be.validateTasks.pushPending(0)
	be.publishTasks.pushPending(0)

	task := &taskVersion{
		execTask:   eTask,
		version:    state.Version{BlockNum: 1, TxNum: 1, TxIndex: 0},
		versionMap: be.versionMap,
	}
	return be, task
}

// driveToFinalize marks the single task complete and seeds the finalized snapshot so
// nextResult enters its block-completion/finalize branch.
func driveToFinalize(be *blockExecutor, task *taskVersion) {
	res := &execResult{TxResult: &exec.TxResult{Task: task}}
	be.results[0] = res
	be.finalizedResults[0] = res
	be.execTasks.setComplete(0)
	be.validateTasks.setComplete(0)
	be.publishTasks.setComplete(0)
}

// A block-init (TxIndex == -1) rules-engine error is a block-validity verdict, not an
// infrastructure failure: Operational stays false and the engine error is carried plain.
func TestParallelInitializeRulesEngineErrorUsesVerdictPath(t *testing.T) {
	config := chain.TestChainBerlinConfig
	engineErr := fmt.Errorf("epoch database read failed")
	engine := rulesEngineWithInitError{Engine: ethash.NewFaker(), initializeErr: engineErr}
	txTask := &exec.TxTask{
		Header:          &types.Header{Number: *uint256.NewInt(1)},
		TxIndex:         -1,
		Config:          config,
		Logger:          log.New(),
		EvmBlockContext: evmtypes.BlockContext{BlockNumber: 1},
	}
	task := &taskVersion{
		execTask: &execTask{Task: txTask},
		version:  state.Version{BlockNum: 1, TxIndex: -1},
	}
	ibs := state.New(state.NewNoopReader())
	t.Cleanup(ibs.Close)

	result := task.Execute(&vm.EVM{}, engine, nil, ibs, state.NewNoopWriter(), config, nil, datadir.Dirs{}, false)

	require.False(t, result.Operational)
	require.ErrorIs(t, result.Err, engineErr)
	require.NotErrorIs(t, result.Err, rules.ErrInvalidBlock)
}

// A rules-engine finalize error is a block-validity verdict: nextResult wraps it in
// rules.ErrInvalidBlock while preserving the cause.
func TestParallelFinalizeClassifiesRulesEngineError(t *testing.T) {
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
			db := newResumeTestDB(t)
			config := chain.TestChainBerlinConfig
			pe, roTx := newResumeTestExec(t, db, config)
			pe.cfg.vmConfig = &vm.Config{}
			cause := fmt.Errorf("epoch database write failed")
			pe.cfg.engine = rulesEngineWithErrors{Engine: pe.cfg.engine, finalizeErr: tc.engineErr(cause)}

			be, task := newParallelResultTestBlock(t, pe, config)
			driveToFinalize(be, task)

			result, err := be.nextResult(context.Background(), pe, &exec.TxResult{
				Task:  task,
				TxIn:  state.ReadSet{},
				TxOut: &state.WriteSet{},
			}, roTx)

			require.NoError(t, err)
			require.NotNil(t, result)
			require.ErrorIs(t, result.Err, rules.ErrInvalidBlock)
			require.ErrorContains(t, result.Err, cause.Error())
			require.False(t, result.Operational)
		})
	}
}

// A state-read failure during engine.Finalize (ibs.StateReadError) is an infrastructure
// failure: nextResult returns an operational block result that does NOT carry ErrInvalidBlock.
func TestParallelFinalizeStateReadErrorUsesOperationalBlockResult(t *testing.T) {
	db := newResumeTestDB(t)
	config := chain.TestChainBerlinConfig
	pe, roTx := newResumeTestExec(t, db, config)
	pe.cfg.vmConfig = &vm.Config{}
	cause := errors.New("withdrawal account read failed")
	beneficiary := accounts.InternAddress(common.Address{19: 0x42})
	pe.cfg.engine = rulesEngineWithFinalizeBalance{Engine: pe.cfg.engine, beneficiary: beneficiary}

	be, task := newParallelResultTestBlock(t, pe, config)
	driveToFinalize(be, task)

	result, err := be.nextResult(context.Background(), pe, &exec.TxResult{
		Task:  task,
		TxIn:  state.ReadSet{},
		TxOut: &state.WriteSet{},
	}, failingAccountTemporalTx{TemporalTx: roTx, address: beneficiary.Value(), err: cause})

	require.NoError(t, err)
	require.NotNil(t, result)
	require.True(t, result.Operational)
	require.ErrorIs(t, result.Err, cause)
	require.NotErrorIs(t, result.Err, rules.ErrInvalidBlock)
}

// A transaction-level state-read error sets result.Operational in TxTask.Execute; nextResult
// then returns an operational block result that does NOT carry ErrInvalidBlock.
func TestParallelStateReadErrorUsesOperationalBlockResult(t *testing.T) {
	db := newResumeTestDB(t)
	config := chain.TestChainBerlinConfig
	pe, roTx := newResumeTestExec(t, db, config)
	pe.cfg.vmConfig = &vm.Config{}
	cause := fmt.Errorf("account domain read failed")

	be, task := newParallelResultTestBlock(t, pe, config, signSelfSendTx(t, 0, 0, 1, 21_000, config, 0))

	ibs := state.New(failingAccountStateReader{NoopReader: state.NewNoopReader(), err: cause})
	t.Cleanup(ibs.Close)
	evm := &vm.EVM{}
	require.NoError(t, task.Reset(evm, ibs, nil))
	result := task.Execute(evm, pe.cfg.engine, nil, ibs, state.NewNoopWriter(), config, nil, datadir.Dirs{}, false)
	result.Task = task
	require.Zero(t, result.TxIn.Len())
	require.True(t, result.Operational)

	blockResult, err := be.nextResult(context.Background(), pe, result, roTx)

	require.NoError(t, err)
	require.NotNil(t, blockResult)
	require.Equal(t, be.blockNum, blockResult.BlockNum)
	require.True(t, blockResult.Operational)
	require.ErrorIs(t, blockResult.Err, cause)
	require.NotErrorIs(t, blockResult.Err, rules.ErrInvalidBlock)
}

// A panic inside the transition (recovered as protocol.ErrExecPanic) sets result.Operational;
// nextResult returns an operational block result even though the panic value wraps
// ErrInvalidBlock — the Operational routing wins.
func TestParallelTransitionPanicUsesOperationalBlockResult(t *testing.T) {
	db := newResumeTestDB(t)
	config := chain.TestChainBerlinConfig
	pe, roTx := newResumeTestExec(t, db, config)
	pe.cfg.vmConfig = &vm.Config{}
	panicValue := fmt.Errorf("%w: account reader panic", rules.ErrInvalidBlock)

	be, task := newParallelResultTestBlock(t, pe, config, signSelfSendTx(t, 0, 0, 1, 21_000, config, 0))

	ibs := state.New(panickingAccountStateReader{NoopReader: state.NewNoopReader(), panicValue: panicValue})
	t.Cleanup(ibs.Close)
	evm := &vm.EVM{}
	require.NoError(t, task.Reset(evm, ibs, nil))
	result := task.Execute(evm, pe.cfg.engine, nil, ibs, state.NewNoopWriter(), config, nil, datadir.Dirs{}, false)
	result.Task = task
	require.True(t, result.Operational)

	blockResult, err := be.nextResult(context.Background(), pe, result, roTx)

	require.NoError(t, err)
	require.NotNil(t, blockResult)
	require.Equal(t, be.blockNum, blockResult.BlockNum)
	require.True(t, blockResult.Operational)
	require.ErrorContains(t, blockResult.Err, "account reader panic")
	require.NotErrorIs(t, blockResult.Err, rules.ErrInvalidBlock)
}

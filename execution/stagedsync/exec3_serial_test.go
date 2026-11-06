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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/protocol"
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

func newSerialFinalizeTestExec(t *testing.T, config *chain.Config, engine rules.Engine) (*serialExecutor, *exec.TxTask) {
	t.Helper()

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
			se, task := newSerialFinalizeTestExec(t, chain.TestChainOsakaConfig, engine)

			_, err := se.executeBlock(t.Context(), []exec.Task{task}, false, false)

			require.ErrorIs(t, err, cause)
			require.ErrorIs(t, err, rules.ErrInvalidBlock)
		})
	}
}

// offByOne binds every transaction to one more gas than it has.
type offByOne struct{ rules.Engine }

func (offByOne) BindTxGas(txn types.Transaction, _ uint64) (types.Transaction, error) {
	return &types.LegacyTx{CommonTx: types.CommonTx{GasLimit: txn.GetGasLimit() + 1}}, nil
}

// TestSerialResumedBlockTxGas checks that the serial executor checks a bound
// gas limit in a block executed from its start or resumed before Amsterdam,
// and not in one resumed from Amsterdam, whose gas pool then lacks the gas of
// its earlier transactions.
func TestSerialResumedBlockTxGas(t *testing.T) {
	amsterdam := chain.TestChainOsakaConfig.Copy()
	amsterdam.AmsterdamTime = common.NewUint64(0)
	to := common.HexToAddress("0x0000000000000000000000000000000000000a11")
	for _, tc := range []struct {
		name    string
		config  *chain.Config
		start   int
		checked bool
	}{
		{"full", chain.TestChainOsakaConfig, 0, true},
		{"resumed", chain.TestChainOsakaConfig, 1, true},
		{"full/amsterdam", amsterdam, 0, true},
		{"resumed/amsterdam", amsterdam, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			se, task := newSerialFinalizeTestExec(t, tc.config, offByOne{ethash.NewFaker()})
			signer := types.LatestSignerForChainID(tc.config.ChainID)
			for range 2 {
				txn, err := types.SignTx(&types.LegacyTx{CommonTx: types.CommonTx{To: &to, GasLimit: 21_000}}, *signer, senderIsCoinbaseKey.key)
				require.NoError(t, err)
				task.Txs = append(task.Txs, txn)
			}
			// tx0's receipt, as the run that executed it left it.
			rwTx := se.applyTx.(kv.TemporalRwTx)
			require.NoError(t, rawtemporaldb.AppendReceiptMetadata(se.doms.AsPutDel(rwTx), 0, 21_000, 0, 0))
			require.NoError(t, se.doms.Flush(t.Context(), rwTx))
			task.TxIndex = tc.start
			_, err := se.executeBlock(t.Context(), []exec.Task{task}, false, false)
			if tc.checked {
				require.ErrorIs(t, err, protocol.ErrGasLimitPosition)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

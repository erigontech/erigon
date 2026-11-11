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

package execmoduletester

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/stageloop"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/gointerfaces"
)

func TestForkchoiceDoesNotPublishFailedStartupChanges(t *testing.T) {
	m := New(t)
	blocks, err := m.GenerateChain(1, nil)
	require.NoError(t, err)
	waitForHead := subscribeStateChangeHeads(t, m)
	address := common.Address{0xab}
	cause := errors.New("startup execution failed")
	stage := &stagedsync.Stage{
		ID: stages.Execution,
		Forward: func(_ bool, _ *stagedsync.StageState, _ stagedsync.Unwinder, sd *execctx.SharedDomains, tx kv.TemporalRwTx, _ log.Logger) error {
			account := accounts.NewAccount()
			account.Nonce = 19
			value := accounts.SerialiseV3(&account)
			sd.SetTxNum(sd.TxNum() + 1)
			require.NoError(t, sd.DomainPut(kv.AccountsDomain, tx, address[:], value, sd.TxNum(), nil))
			m.Notifications.Accumulator.StartChange(blocks.TopBlock.Header(), nil, false)
			m.Notifications.Accumulator.ChangeAccount(address, 0, value)
			return cause
		},
	}
	pipeline := stagedsync.New(m.BlockReader.TxnumReader(), m.Cfg().Sync, []*stagedsync.Stage{stage}, nil, nil, m.Log, stages.ModeApplyingBlocks)
	dispatcher := execmodule.NewDispatcher(m.ChainConfig, m.Notifications.Events, m.Notifications.StateChangesConsumer, m.Log)
	executor := execmodule.NewPipelineExecutor(pipeline, m.DB, m.BlockReader, m.ChainConfig, m.Engine, nil, nil, dispatcher, m.Log)
	hook := stageloop.NewHook(t.Context(), m.Notifications, pipeline, m.ChainConfig, m.Log, dispatcher, nil, nil, nil, m.BlockReader)
	require.ErrorIs(t, executor.ProcessFrozenBlocks(t.Context(), hook, false), cause)
	require.NoError(t, m.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
		value, _, err := tx.GetLatest(kv.AccountsDomain, address[:], kv.GetLatestOptions{})
		require.NoError(t, err)
		require.Empty(t, value, "failed startup must not commit the account")
		return nil
	}))

	status, err := m.InsertBlocks(t.Context(), blocks.Blocks)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	result, err := m.UpdateForkChoice(t.Context(), blocks.TopBlock.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)
	m.ExecModule.WaitIdle(t.Context())
	batch := waitForHead(blocks.TopBlock.Hash())
	for _, change := range batch.ChangeBatch {
		for _, account := range change.Changes {
			require.NotEqual(t, address, common.Address(gointerfaces.ConvertH160toAddress(account.Address)), "an accepted forkchoice must not publish rolled-back startup changes")
		}
	}
}

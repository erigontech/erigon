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
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
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

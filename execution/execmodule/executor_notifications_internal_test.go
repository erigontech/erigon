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

package execmodule

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/stageloop"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/shards"
)

type failNthCommitDB struct {
	kv.TemporalRwDB
	remaining int
	err       error
}

type failedCommitTx struct {
	kv.TemporalRwTx
	err error
}

func (tx failedCommitTx) Commit() error { return tx.err }

func (db *failNthCommitDB) BeginTemporalRw(ctx context.Context) (kv.TemporalRwTx, error) {
	tx, err := db.TemporalRwDB.BeginTemporalRw(ctx) //nolint:gocritic // The caller owns the returned transaction.
	if err != nil {
		return nil, err
	}
	db.remaining--
	if db.remaining == 0 {
		return failedCommitTx{tx, db.err}, nil
	}
	return tx, nil
}

func TestProcessFrozenBlocksKeepsCommittedNotifications(t *testing.T) {
	for _, tc := range []struct {
		failure string
		cycle   int
	}{
		{"execution", 1},
		{"execution", 2},
		{"prune", 1},
		{"prune", 2},
		{"commit", 1},
		{"commit", 2},
		{"success", 2},
	} {
		t.Run(fmt.Sprintf("%s/cycle_%d", tc.failure, tc.cycle), func(t *testing.T) {
			db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
			logger := log.New()
			notifications := shards.NewNotifications(nil)
			accumulator := notifications.Accumulator
			header := &types.Header{GasLimit: 30_000_000, BaseFee: uint256.NewInt(1_000_000_000)}
			require.NoError(t, db.UpdateTemporal(t.Context(), func(tx kv.TemporalRwTx) error {
				require.NoError(t, rawdb.WriteHeader(tx, header))
				require.NoError(t, rawdb.WriteCanonicalHash(tx, header.Hash(), header.Number.Uint64()))
				return rawdb.WriteHeadHeaderHash(tx, header.Hash())
			}))
			accumulator.StartChange(header, nil, false)

			cause := errors.New("startup cycle failed")
			address := common.Address{1}
			cycle := 0
			stage := &stagedsync.Stage{
				ID: stages.Execution,
				Forward: func(_ bool, _ *stagedsync.StageState, _ stagedsync.Unwinder, sd *execctx.SharedDomains, tx kv.TemporalRwTx, _ log.Logger) error {
					cycle++
					account := accounts.NewAccount()
					account.Nonce = uint64(cycle)
					value := accounts.SerialiseV3(&account)
					sd.SetTxNum(uint64(cycle))
					require.NoError(t, sd.DomainPut(kv.AccountsDomain, tx, address[:], value, uint64(cycle), nil))
					header := &types.Header{Number: *uint256.NewInt(uint64(cycle)), GasLimit: 30_000_000, BaseFee: uint256.NewInt(1_000_000_000)}
					accumulator.StartChange(header, nil, false)
					accumulator.ChangeAccount(address, 0, value)
					require.NoError(t, rawdb.WriteHeader(tx, header))
					require.NoError(t, rawdb.WriteCanonicalHash(tx, header.Hash(), header.Number.Uint64()))
					require.NoError(t, rawdb.WriteHeadHeaderHash(tx, header.Hash()))
					require.NoError(t, rawdbv3.TxNums.Append(tx, uint64(cycle), uint64(cycle)))
					require.NoError(t, stages.SaveStageProgress(tx, stages.Execution, uint64(cycle)))
					require.NoError(t, stages.SaveStageProgress(tx, stages.Finish, uint64(cycle)))
					if tc.failure == "execution" && cycle == tc.cycle {
						return cause
					}
					if cycle < tc.cycle {
						return &stagedsync.ErrLoopExhausted{}
					}
					return nil
				},
				Prune: func(context.Context, *stagedsync.PruneState, kv.RwTx, time.Duration, log.Logger) error {
					if tc.failure == "prune" && cycle == tc.cycle {
						return cause
					}
					return nil
				},
			}
			pipeline := stagedsync.New(rawdbv3.TxNums, ethconfig.Sync{}, []*stagedsync.Stage{stage}, nil, stagedsync.PruneOrder{stage.ID}, logger, stages.ModeApplyingBlocks)
			capture := new(stateChangesCapture)
			dispatcher := NewDispatcher(chain.AllProtocolChanges, nil, capture, nil, logger)
			pe := NewPipelineExecutor(pipeline, db, pinTestBlockReader{}, chain.AllProtocolChanges, nil, nil, nil, dispatcher, logger)
			hook := stageloop.NewHook(t.Context(), notifications, pipeline, chain.AllProtocolChanges, logger, dispatcher, nil, nil, nil, pinTestBlockReader{})
			if tc.failure == "commit" {
				pe.db = &failNthCommitDB{db, tc.cycle, cause}
			}

			err := pe.ProcessFrozenBlocks(t.Context(), hook, false)
			committed := tc.cycle
			if tc.failure == "success" {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, cause)
				require.Nil(t, capture.batch, "failed startup must leave committed notifications pending")
				committed--
				require.Len(t, accumulator.Changes(), committed+1, "preserve pending notifications until the next dispatch")
			}
			require.NoError(t, db.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
				progress, err := stages.GetStageProgress(tx, stages.Execution)
				require.NoError(t, err)
				require.Equal(t, uint64(committed), progress)
				value, _, err := tx.GetLatest(kv.AccountsDomain, address[:], kv.GetLatestOptions{})
				require.NoError(t, err)
				if committed == 0 {
					require.Empty(t, value)
				} else {
					var account accounts.Account
					require.NoError(t, accounts.DeserialiseV3(&account, value))
					require.Equal(t, uint64(committed), account.Nonce)
				}
				if tc.failure != "success" {
					return hook.SendNotifications(tx, 0)
				}
				return nil
			}))
			require.Empty(t, accumulator.Changes(), "delivered notifications must not be restored")
			require.NotNil(t, capture.batch)
			require.Len(t, capture.batch.ChangeBatch, committed+1, "retain earlier pending and committed changes, discard the failed cycle")
			for i, change := range capture.batch.ChangeBatch {
				require.Equal(t, uint64(i), change.BlockHeight)
				if i == 0 {
					require.Empty(t, change.Changes)
					continue
				}
				require.Len(t, change.Changes, 1)
				var account accounts.Account
				require.NoError(t, accounts.DeserialiseV3(&account, change.Changes[0].Data))
				require.Equal(t, uint64(i), account.Nonce)
			}
		})
	}
}

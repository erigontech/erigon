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
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/notifications"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/ethconfig"
)

type setHeadReader struct{ pinTestBlockReader }

func (setHeadReader) CanonicalHash(_ context.Context, tx kv.Getter, number uint64) (common.Hash, bool, error) {
	hash, err := rawdb.ReadCanonicalHash(tx, number)
	return hash, hash != (common.Hash{}), err
}

func TestSetHeadFailureRestoresPendingNotifications(t *testing.T) {
	for _, failure := range []string{"unwind", "commit"} {
		t.Run(failure, func(t *testing.T) {
			db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
			logger := log.New()
			head := &types.Header{Number: *uint256.NewInt(2)}
			target := &types.Header{Number: *uint256.NewInt(1)}
			require.NoError(t, db.UpdateTemporal(t.Context(), func(tx kv.TemporalRwTx) error {
				for block := range uint64(3) {
					require.NoError(t, rawdbv3.TxNums.Append(tx, block, 3*block+2))
				}
				rawdb.WriteHeadBlockHash(tx, head.Hash())
				require.NoError(t, stages.SaveStageProgress(tx, stages.Execution, 2))
				require.NoError(t, rawdb.WriteCanonicalHash(tx, target.Hash(), 1))
				diff := &changeset.StateChangeSet{}
				diff.Diffs[kv.AccountsDomain].DomainUpdate([]byte{1}, 0, nil)
				return changeset.WriteDiffSet(tx, 2, head.Hash(), diff)
			}))
			accum := notifications.NewAccumulator()
			accum.StartChange(head, nil, false)
			pending := append([]notifications.StateChange(nil), accum.Changes()...)
			cause := errors.New("set head failed")
			stage := &stagedsync.Stage{ID: stages.Execution, Unwind: func(_ *stagedsync.UnwindState, _ *stagedsync.StageState, _ *execctx.SharedDomains, _ kv.TemporalRwTx, _ log.Logger) error {
				accum.StartChange(target, nil, true)
				if failure == "unwind" {
					return cause
				}
				return nil
			}}
			pipeline := stagedsync.New(rawdbv3.TxNums, ethconfig.Defaults.Sync, []*stagedsync.Stage{stage}, stagedsync.UnwindOrder{stage.ID}, nil, logger, stages.ModeApplyingBlocks)
			module := &ExecModule{
				db: &failNthCommitDB{TemporalRwDB: db, remaining: 1, err: cause}, blockReader: setHeadReader{},
				semaphore: semaphore.NewWeighted(1), logger: logger,
				pipelineExecutor: &PipelineExecutor{sync: pipeline},
				accum:            &Accumulation{Accumulator: accum},
			}
			require.ErrorIs(t, module.SetHead(t.Context(), 1), cause)
			require.Equal(t, pending, accum.Changes(), "only changes from the failed transaction should be discarded")
			require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
				require.Equal(t, head.Hash(), rawdb.ReadHeadBlockHash(tx))
				return nil
			}))
		})
	}
}

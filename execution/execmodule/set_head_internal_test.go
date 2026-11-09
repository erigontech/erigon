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
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/ethconfig"
)

type commitErrorDB struct {
	kv.TemporalRwDB
	err error
}

func (db commitErrorDB) BeginTemporalRw(ctx context.Context) (kv.TemporalRwTx, error) {
	tx, err := db.TemporalRwDB.BeginTemporalRw(ctx) //nolint:gocritic // The caller owns the returned transaction.
	if err != nil {
		return nil, err
	}
	return commitErrorTx{tx, db.err}, nil
}

type commitErrorTx struct {
	kv.TemporalRwTx
	err error
}

func (tx commitErrorTx) Commit() error { return tx.err }

type setHeadBlockReader struct{ dbservices.FullBlockReader }

func (setHeadBlockReader) CanonicalHash(_ context.Context, tx kv.Getter, number uint64) (common.Hash, bool, error) {
	hash, err := rawdb.ReadCanonicalHash(tx, number)
	return hash, hash != (common.Hash{}), err
}

func TestSetHeadFailedCommitPreservesPendingNotifications(t *testing.T) {
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	logger := log.New()
	target := &types.Header{Number: *uint256.NewInt(1)}
	head := &types.Header{Number: *uint256.NewInt(2), ParentHash: target.Hash()}
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		for _, header := range []*types.Header{target, head} {
			require.NoError(t, rawdb.WriteHeader(tx, header))
			require.NoError(t, rawdb.WriteCanonicalHash(tx, header.Hash(), header.Number.Uint64()))
		}
		rawdb.WriteHeadBlockHash(tx, head.Hash())
		rawdb.WriteForkchoiceHead(tx, head.Hash())
		return stages.SaveStageProgress(tx, stages.Headers, head.Number.Uint64())
	}))
	accum := NewAccumulation()
	accum.Accumulator.StartChange(head, nil, false)
	want := accum.Accumulator.Changes()[0].ToProto()
	sync := stagedsync.New(ethconfig.Sync{}, []*stagedsync.Stage{{
		ID: stages.Headers,
		Unwind: func(u *stagedsync.UnwindState, _ *stagedsync.StageState, _ *execctx.SharedDomains, tx kv.TemporalRwTx, _ log.Logger) error {
			accum.Accumulator.StartChange(target, nil, true)
			return u.Done(tx)
		},
	}}, []stages.SyncStage{stages.Headers}, nil, logger, stages.ModeApplyingBlocks)
	commitErr := errors.New("commit failed")
	module := &ExecModule{
		db:               commitErrorDB{db, commitErr},
		blockReader:      setHeadBlockReader{},
		semaphore:        semaphore.NewWeighted(1),
		pipelineExecutor: &PipelineExecutor{sync: sync},
		accum:            accum,
		logger:           logger,
	}

	require.ErrorIs(t, module.SetHead(t.Context(), target.Number.Uint64()), commitErr)
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		require.Equal(t, head.Hash(), rawdb.ReadHeadBlockHash(tx))
		return nil
	}))
	require.Len(t, accum.Accumulator.Changes(), 1, "only changes committed before SetHead may remain pending")
	require.Equal(t, want, accum.Accumulator.Changes()[0].ToProto())
}

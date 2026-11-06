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

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/shards"
)

type stateChangeCapture struct {
	batch *remoteproto.StateChangeBatch
}

func (c *stateChangeCapture) SendStateChanges(_ context.Context, batch *remoteproto.StateChangeBatch) {
	c.batch = batch
}

func TestUnwindStateNotificationCompleteness(t *testing.T) {
	for _, tc := range []struct {
		name       string
		depth      uint64
		stream     bool
		incomplete bool
	}{
		{"within stream limit", 999, true, false},
		{"at stream limit", 1_000, true, true},
		{"stream disabled", 1, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, logger := t.Context(), log.New()
			db, tx := temporaltest.NewTestTx(t)
			br := freezeblocks.NewBlockReader(db.(freezeblocks.HasBlockFiles).DebugBlockFiles())
			doms, err := execctx.NewSharedDomains(ctx, tx, logger)
			require.NoError(t, err)
			defer doms.Close()

			const unwindTo = uint64(1)
			head := unwindTo + tc.depth
			key := dbutils.GenerateStoragePlainKey(types.FrameRecentRootVerifier, common.Hash{1})
			for block := uint64(0); block <= head; block++ {
				header := makeHeader(block, common.Hash{})
				require.NoError(t, rawdb.WriteHeader(tx, header))
				require.NoError(t, rawdb.WriteCanonicalHash(tx, header.Hash(), block))
				require.NoError(t, rawdbv3.TxNums.Append(tx, block, 2*block+1))
				cs := &changeset.StateChangeSet{}
				if block == head {
					doms.SetChangesetAccumulator(cs)
					doms.SetTxNum(2 * block)
					require.NoError(t, doms.DomainPut(kv.StorageDomain, tx, key, []byte{1}, 2*block, nil))
				}
				require.NoError(t, changeset.WriteDiffSet(tx, block, header.Hash(), cs))
			}
			accumulator := shards.NewAccumulator()
			cfg := ExecuteBlockCfg{blockReader: br, stateStream: tc.stream, notifications: &shards.Notifications{Accumulator: accumulator}}
			s := &StageState{ID: stages.Execution, BlockNumber: head}
			u := &UnwindState{ID: stages.Execution, UnwindPoint: unwindTo, CurrentBlockNumber: head}
			require.NoError(t, UnwindExecutionStage(u, s, doms, tx, ctx, cfg, logger))
			value, _, err := doms.GetLatest(kv.StorageDomain, tx, key)
			require.NoError(t, err)
			require.Empty(t, value, "the root written on the removed branch must be gone")

			// The replacement block does not rewrite the removed root. Its complete
			// notification must not hide omissions from the unwind in the same batch.
			accumulator.StartChange(makeHeader(unwindTo+1, common.Hash{2}), nil, false)
			capture := new(stateChangeCapture)
			accumulator.SendAndReset(ctx, capture, 1, 1, 1_000_000, 0)
			require.NotNil(t, capture.batch)
			require.Equal(t, tc.incomplete, capture.batch.StateChangesIncomplete)
			if !tc.incomplete {
				require.Len(t, capture.batch.ChangeBatch[0].Changes, 1)
				require.Len(t, capture.batch.ChangeBatch[0].Changes[0].StorageChanges, 1)
			}

			accumulator.StartChange(makeHeader(unwindTo+2, common.Hash{3}), nil, false)
			accumulator.SendAndReset(ctx, capture, 1, 1, 1_000_000, 0)
			require.False(t, capture.batch.StateChangesIncomplete, "omissions apply only to their batch")
		})
	}
}

func TestCatchUpStateNotificationCompleteness(t *testing.T) {
	db, tx := temporaltest.NewTestTx(t)
	ctx, logger := t.Context(), log.New()
	doms, err := execctx.NewSharedDomains(ctx, tx, logger)
	require.NoError(t, err)
	defer doms.Close()
	accumulator := shards.NewAccumulator()
	cfg := ExecuteBlockCfg{db: db, notifications: &shards.Notifications{Accumulator: accumulator}}
	header := makeHeader(100, common.Hash{})
	require.NoError(t, execV3Finalize(ctx, nil, cfg, doms, 200, 100, header, false, "test", logger))
	capture := new(stateChangeCapture)
	accumulator.SendAndReset(ctx, capture, 1, 1, 1_000_000, 0)
	require.NotNil(t, capture.batch)
	require.True(t, capture.batch.StateChangesIncomplete)
	require.Len(t, capture.batch.ChangeBatch, 1)
	require.Equal(t, uint64(100), capture.batch.ChangeBatch[0].BlockHeight)
	require.Empty(t, capture.batch.ChangeBatch[0].Changes)
}

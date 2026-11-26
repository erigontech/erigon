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

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
)

// TestHaltOnInitialSyncFailure pins the decision for a failed one-shot initial
// sync: a real pipeline failure halts the process, while routine shutdown and
// post-sync errors keep it up.
func TestHaltOnInitialSyncFailure(t *testing.T) {
	t.Parallel()

	operational := fmt.Errorf("[Execution] %w",
		errors.New("parallel exec finished with 1 scheduled block(s) that never reached apply-loop validation"))
	verdict := fmt.Errorf("[Execution] %w", fmt.Errorf("%w: gas mismatch, block=12", rules.ErrInvalidBlock))

	require.True(t, haltOnInitialSyncFailure(operational),
		"a real failure must halt regardless of its class — the node would otherwise stay up but never sync")
	require.True(t, haltOnInitialSyncFailure(verdict))
	require.False(t, haltOnInitialSyncFailure(nil))
	require.False(t, haltOnInitialSyncFailure(fmt.Errorf("[Execution] %w", context.Canceled)),
		"shutdown must not halt the node")
	stopped := fmt.Errorf("[Senders] %w", common.ErrStopped)
	require.False(t, haltOnInitialSyncFailure(stopped),
		"ETL shutdown must not halt the node")
	grpcCanceled := status.Error(codes.Canceled, context.Canceled.Error())
	require.False(t, haltOnInitialSyncFailure(grpcCanceled),
		"remote downloader cancellation must not halt the node")
	require.False(t, haltOnInitialSyncFailure(fmt.Errorf("[Snapshots] %w", grpcCanceled)))
	require.False(t, haltOnInitialSyncFailure(errors.Join(context.Canceled, stopped, grpcCanceled)),
		"joined shutdown signals must not halt the node")
	require.True(t, haltOnInitialSyncFailure(errors.Join(stopped, grpcCanceled, operational)),
		"a real failure must not be hidden by a shutdown signal")
	publicationErr := &initialSyncPublicationError{err: errors.New("notification dispatch failed")}
	require.False(t, haltOnInitialSyncFailure(publicationErr),
		"an error after initial sync completed must not halt the node")
}

type startupBlockReader struct {
	*freezeblocks.BlockReader
	frozen uint64
}

func (r startupBlockReader) FrozenBlocksInView(kv.Getter) uint64 { return r.frozen }

func TestStartupRecoveryDiscardsOnlyUnacceptedMutableBlocks(t *testing.T) {
	child := func(parent *types.Header, timestamp uint64) *types.Header {
		return &types.Header{Number: *uint256.NewInt(parent.Number.Uint64() + 1), ParentHash: parent.Hash(), Time: timestamp}
	}
	checkpoint := &types.Header{Number: *uint256.NewInt(1)}
	next := child(checkpoint, 2)
	tip := child(next, 3)
	side := child(checkpoint, 4)
	sideTip := child(side, 5)
	overlappingTip := child(next, 5)
	for _, tc := range []struct {
		name     string
		accepted common.Hash
		rejected *types.Header
		frozen   uint64
		executed uint64
		recover  bool
	}{
		{"accepted_other_branch", sideTip.Hash(), tip, 1, 1, true},
		{"accepted_parent_in_suffix", overlappingTip.Hash(), tip, 1, 1, false},
		{"frozen_block_in_suffix", checkpoint.Hash(), tip, 2, 1, false},
		{"rejected_checkpoint", checkpoint.Hash(), checkpoint, 0, 1, false},
		{"rejected_other_hash", checkpoint.Hash(), sideTip, 0, 1, false},
		{"execution_ahead_of_finish", checkpoint.Hash(), tip, 0, 2, false},
		{"unknown_accepted_head", common.Hash{}, tip, 0, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
			pe := PipelineExecutor{db: db, blockReader: startupBlockReader{freezeblocks.NewBlockReader(nil), tc.frozen}}
			require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
				for _, header := range []*types.Header{checkpoint, next, tip, side, sideTip, overlappingTip} {
					require.NoError(t, rawdb.WriteHeader(tx, header))
				}
				for _, header := range []*types.Header{checkpoint, next, tip} {
					require.NoError(t, rawdb.WriteCanonicalHash(tx, header.Hash(), header.Number.Uint64()))
				}
				rawdb.WriteForkchoiceHead(tx, tc.accepted)
				require.NoError(t, stages.SaveStageProgress(tx, stages.Execution, tc.executed))
				return stages.SaveStageProgress(tx, stages.Finish, checkpoint.Number.Uint64())
			}))
			bad := &stagedsync.RejectedBlockError{Number: tc.rejected.Number.Uint64(), Hash: tc.rejected.Hash(), Err: rules.ErrInvalidBlock}
			err := pe.discardRejectedStartupTail(t.Context(), bad)
			wantTip := tip.Hash()
			if tc.recover {
				require.NoError(t, err)
				wantTip = common.Hash{}
			} else {
				require.ErrorIs(t, err, bad)
			}
			require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
				canonical, err := rawdb.ReadCanonicalHash(tx, tip.Number.Uint64())
				require.NoError(t, err)
				require.Equal(t, wantTip, canonical)
				require.Equal(t, tc.accepted, rawdb.ReadForkchoiceHead(tx))
				return nil
			}))
		})
	}
}

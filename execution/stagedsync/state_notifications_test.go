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
	"encoding/binary"
	"testing"

	"github.com/holiman/uint256"
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
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/shards"
)

func TestUnwindCodeNotificationsUseRestoredState(t *testing.T) {
	for _, tc := range []struct {
		name    string
		removed bool
	}{
		{name: "restored code"},
		{name: "removed account", removed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, logger := t.Context(), log.New()
			_, tx := temporaltest.NewTestTx(t)
			doms, err := execctx.NewSharedDomains(ctx, tx, logger)
			require.NoError(t, err)
			defer doms.Close()
			address := common.Address{1}
			code := types.AddressToDelegation(accounts.InternAddress(common.Address{2}))
			account := accounts.Account{Nonce: 1, Balance: *uint256.NewInt(1), CodeHash: accounts.NewCode(code).Hash}
			if !tc.removed {
				require.NoError(t, doms.DomainPut(kv.AccountsDomain, tx, address[:], accounts.SerialiseV3(&account), 0, nil))
				require.NoError(t, doms.DomainPut(kv.CodeDomain, tx, address[:], code, 0, nil))
			}
			var diffs [kv.DomainLen][]kv.DomainEntryDiff
			// Step-local values can differ from the unwind target, even for the same address.
			for step := range uint64(2) {
				key := binary.BigEndian.AppendUint64(address[:], ^step)
				diffs[kv.CodeDomain] = append(diffs[kv.CodeDomain], kv.DomainEntryDiff{Key: string(key), Value: []byte{0xfe}})
			}
			accumulator := shards.NewAccumulator()
			header := makeHeader(1, common.Hash{})
			accumulator.StartChange(header, nil, true)
			require.NoError(t, stateChangesStreamAtUnwind(ctx, tx, doms.AsSnapshotGetter(tx), 1, 0, accumulator, &diffs, logger))
			batch := accumulator.Batch(header).ToProtoBatch()
			require.False(t, batch.StateChangesIncomplete)
			require.Len(t, batch.ChangeBatch[0].Changes, 1)
			change := batch.ChangeBatch[0].Changes[0]
			if tc.removed {
				require.Equal(t, remoteproto.Action_REMOVE, change.Action)
				require.Empty(t, change.Code)
			} else {
				require.Equal(t, remoteproto.Action_UPSERT_CODE, change.Action)
				require.Equal(t, accounts.SerialiseV3(&account), change.Data)
				require.Equal(t, code, change.Code)
			}
		})
	}
}

func TestUnwindStateNotificationCompleteness(t *testing.T) {
	for _, tc := range []struct {
		name       string
		depth      uint64
		stream     bool
		incomplete bool
		mode       stages.Mode
	}{
		{"within stream limit", 999, true, false, stages.ModeApplyingBlocks},
		{"at stream limit", 1_000, true, true, stages.ModeApplyingBlocks},
		{"stream disabled", 1, false, true, stages.ModeApplyingBlocks},
		{"validation with streaming", 1, true, false, stages.ModeForkValidation},
		{"validation without streaming", 1, false, false, stages.ModeForkValidation},
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
			s := &StageState{ID: stages.Execution, BlockNumber: head, State: &Sync{mode: tc.mode}}
			u := &UnwindState{ID: stages.Execution, UnwindPoint: unwindTo, CurrentBlockNumber: head}
			require.NoError(t, UnwindExecutionStage(u, s, doms, tx, ctx, cfg, logger))
			value, _, err := doms.GetLatest(kv.StorageDomain, tx, key)
			require.NoError(t, err)
			require.Empty(t, value, "the root written on the removed branch must be gone")

			// The replacement block does not rewrite the removed root. Its complete
			// notification must not hide omissions from the unwind in the same batch.
			header := makeHeader(unwindTo+1, common.Hash{2})
			accumulator.StartChange(header, nil, false)
			batch := accumulator.Batch(header).ToProtoBatch()
			require.Equal(t, tc.incomplete, batch.StateChangesIncomplete)
			if tc.mode == stages.ModeForkValidation {
				require.Len(t, batch.ChangeBatch, 1, "speculative unwinds must not be announced")
			} else if !tc.incomplete {
				require.Len(t, batch.ChangeBatch[0].Changes, 1)
				require.Len(t, batch.ChangeBatch[0].Changes[0].StorageChanges, 1)
			}

			accumulator.Reset()
			header = makeHeader(unwindTo+2, common.Hash{3})
			accumulator.StartChange(header, nil, false)
			require.False(t, accumulator.Batch(header).StateChangesIncomplete, "omissions apply only to their batch")
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
	require.NoError(t, execV3Finalize(ctx, nil, cfg, doms, 1, 200, 100, header, false, "test", logger))
	batch := accumulator.Batch(header).ToProtoBatch()
	require.True(t, batch.StateChangesIncomplete)
	require.Len(t, batch.ChangeBatch, 1)
	require.Equal(t, uint64(100), batch.ChangeBatch[0].BlockHeight)
	require.Empty(t, batch.ChangeBatch[0].Changes)
}

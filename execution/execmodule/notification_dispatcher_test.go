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
	"bytes"
	"context"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/notifications"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/shards"
)

type stateChangesCapture struct {
	batch *remoteproto.StateChangeBatch
}

func (c *stateChangesCapture) SendStateChanges(_ context.Context, batch *remoteproto.StateChangeBatch) {
	c.batch = batch
}

func TestDispatcherUsesSuppliedStateVersion(t *testing.T) {
	_, tx := temporaltest.NewTestTx(t)
	header := &types.Header{
		Number:   *uint256.NewInt(1),
		GasLimit: 30_000_000,
		BaseFee:  uint256.NewInt(1_000_000_000),
	}
	require.NoError(t, rawdb.WriteHeader(tx, header))
	require.NoError(t, rawdb.WriteHeadHeaderHash(tx, header.Hash()))
	rawdb.WriteHeadBlockHash(tx, header.Hash())
	headOfHeaders := types.CopyHeader(header)
	headOfHeaders.Number.SetUint64(2)
	require.NoError(t, rawdb.WriteHeader(tx, headOfHeaders))
	require.NoError(t, rawdb.WriteHeadHeaderHash(tx, headOfHeaders.Hash()))

	accumulator := notifications.NewAccumulator()
	accumulator.StartChange(header, nil, false)
	capture := new(stateChangesCapture)
	dispatcher := NewDispatcher(chain.AllProtocolChanges, nil, capture, log.New())

	const projectedStateVersion = uint64(7)
	require.NoError(t, dispatcher.Dispatch(
		t.Context(),
		tx,
		projectedStateVersion,
		accumulator,
		nil,
		0,
		1,
		nil,
	))
	require.NotNil(t, capture.batch)
	require.Equal(t, projectedStateVersion, capture.batch.StateVersionId)
	require.NotNil(t, capture.batch.TargetView)
	require.Equal(t, projectedStateVersion, capture.batch.TargetView.StateVersionId)
	require.Equal(t, header.Hash(), common.Hash(gointerfaces.ConvertH256ToHash(capture.batch.TargetView.BlockHash)))
}

func TestDispatcherResetsAfterBulkAdvance(t *testing.T) {
	_, tx := temporaltest.NewTestTx(t)
	accumulator := notifications.NewAccumulator()
	capture := new(stateChangesCapture)
	dispatcher := NewDispatcher(chain.AllProtocolChanges, nil, capture, log.New())
	header := &types.Header{GasLimit: 30_000_000, BaseFee: uint256.NewInt(1_000_000_000)}
	dispatcher.lastView = &remoteproto.ExecutionView{BlockHash: gointerfaces.ConvertHashToH256(header.Hash()), StateVersionId: 1}
	for _, height := range []uint64{1, 2} {
		header.ParentHash = header.Hash()
		header.Number.SetUint64(height)
		require.NoError(t, rawdb.WriteHeader(tx, header))
		accumulator.StartChange(header, nil, false)
	}
	rawdb.WriteHeadBlockHash(tx, header.Hash())
	require.NoError(t, dispatcher.Dispatch(t.Context(), tx, 2, accumulator, nil, 0, 2, nil))
	require.True(t, capture.batch.StateChangesIncomplete)
}

func TestDispatcherPublishesRewindTarget(t *testing.T) {
	_, tx := temporaltest.NewTestTx(t)
	capture := new(stateChangesCapture)
	dispatcher := NewDispatcher(chain.AllProtocolChanges, nil, capture, log.New())
	accumulator := notifications.NewAccumulator()
	for _, height := range []uint64{3, 2, 1} {
		header := &types.Header{
			Number: *uint256.NewInt(height), Time: height * 12, SlotNumber: &height,
			GasLimit: 30_000_000, BaseFee: uint256.NewInt(1_000_000_000),
			WithdrawalsHash: new(common.Hash), BlobGasUsed: new(uint64), ExcessBlobGas: new(uint64),
			ParentBeaconBlockRoot: new(common.Hash), RequestsHash: new(common.Hash), BlockAccessListHash: new(common.Hash),
		}
		require.NoError(t, rawdb.WriteHeader(tx, header))
		require.NoError(t, rawdb.WriteHeadHeaderHash(tx, header.Hash()))
		rawdb.WriteHeadBlockHash(tx, header.Hash())
		accumulator.StartChange(&types.Header{Number: *uint256.NewInt(4)}, [][]byte{{1}}, true)
		require.NoError(t, dispatcher.Dispatch(t.Context(), tx, 10-height, accumulator, nil, 4, height, &height))
		require.NotNil(t, capture.batch)
		require.NotNil(t, capture.batch.TargetView)
		require.Equal(t, height, capture.batch.TargetView.BlockHeight)
		require.Equal(t, height*12, capture.batch.TargetView.BlockTime)
		require.Equal(t, &height, capture.batch.TargetView.SlotNumber)
		require.True(t, capture.batch.StateChangesIncomplete)
		require.Len(t, capture.batch.ChangeBatch, 1)
		require.Equal(t, uint64(4), capture.batch.ChangeBatch[0].BlockHeight)
	}
}

func TestDispatcherResetWaitsForTargetAccountState(t *testing.T) {
	for _, name := range []string{"updated", "deleted"} {
		t.Run(name, func(t *testing.T) {
			_, tx := temporaltest.NewTestTx(t)
			ctx := t.Context()
			initial, err := execctx.NewSharedDomains(ctx, tx, log.New())
			require.NoError(t, err)
			defer initial.Close()
			address := common.Address{1}
			account := accounts.Account{Nonce: 1, Balance: *uint256.NewInt(100), CodeHash: accounts.EmptyCodeHash}
			committed := accounts.SerialiseV3(&account)
			require.NoError(t, initial.DomainPut(kv.AccountsDomain, tx, address[:], committed, 1, nil))
			require.NoError(t, initial.Flush(ctx, tx))
			doms, err := execctx.NewSharedDomains(ctx, tx, log.New())
			require.NoError(t, err)
			defer doms.Close()
			require.NoError(t, doms.InitBlockOverlay(tx))

			header := &types.Header{Number: *uint256.NewInt(2), GasLimit: 30_000_000, BaseFee: uint256.NewInt(1_000_000_000)}
			accumulator := notifications.NewAccumulator()
			accumulator.StartChange(header, [][]byte{{1}}, false)
			account.Nonce = 2
			account.Balance.SetUint64(75)
			accumulator.ChangeAccount(address, 0, accounts.SerialiseV3(&account))
			slot := common.Hash{2}
			accumulator.ChangeStorage(address, 0, slot, []byte{3})
			recorded := accumulator.Changes()[0].ToProto()

			header.Number.SetUint64(100)
			require.NoError(t, rawdb.WriteHeader(doms.BlockOverlay(), header))
			require.NoError(t, rawdb.WriteHeadHeaderHash(doms.BlockOverlay(), header.Hash()))
			rawdb.WriteHeadBlockHash(doms.BlockOverlay(), header.Hash())
			require.NoError(t, stages.SaveStageProgress(doms.BlockOverlay(), stages.Finish, 100))
			accumulator.StartChange(header, nil, false)
			accumulator.MarkIncomplete()
			account.Nonce = 3
			account.Balance.SetUint64(25)
			want := accounts.SerialiseV3(&account)
			if name == "deleted" {
				want = nil
				require.NoError(t, doms.DomainDel(kv.AccountsDomain, tx, address[:], 2, nil))
			} else {
				require.NoError(t, doms.DomainPut(kv.AccountsDomain, tx, address[:], want, 2, nil))
			}
			value, _, err := tx.GetLatest(kv.AccountsDomain, address[:], kv.GetLatestOptions{})
			require.NoError(t, err)
			require.Equal(t, committed, value, "the final account is still uncommitted")

			capture := new(stateChangesCapture)
			dispatcher := NewDispatcher(chain.AllProtocolChanges, nil, capture, log.New())
			module := &ExecModule{
				pipelineExecutor: &PipelineExecutor{dispatcher: dispatcher},
				accum:            &Accumulation{Accumulator: accumulator},
				backgroundCtx:    ctx,
				logger:           log.New(),
			}
			published, err := module.dispatchNotificationsFromOverlay(ctx, doms, 1, nil)
			require.NoError(t, err)
			require.True(t, published)
			require.Equal(t, recorded, capture.batch.ChangeBatch[0], "recorded transactions and state changes must be preserved")
			require.True(t, capture.batch.StateChangesIncomplete)
			latest := kvcache.NewLatestBatchCache()
			latest.OnNewBlock(capture.batch)
			_, err = latest.View(ctx, tx)
			require.ErrorIs(t, err, kvcache.ErrExecutionViewUnavailable)

			require.NoError(t, doms.Flush(ctx, tx))
			stateVersion, err := rawdb.GetStateVersion(tx)
			require.NoError(t, err)
			require.Equal(t, stateVersion, capture.batch.StateVersionId)
			view, err := latest.View(ctx, tx)
			require.NoError(t, err)
			value, err = view.Get(address[:])
			require.NoError(t, err)
			require.True(t, bytes.Equal(want, value))
			coherent := kvcache.New(kvcache.DefaultCoherentConfig)
			coherent.OnNewBlock(capture.batch)
			view, err = coherent.View(ctx, tx)
			require.NoError(t, err)
			value, err = view.Get(address[:])
			require.NoError(t, err)
			require.True(t, bytes.Equal(want, value), "the coherent cache must agree with the committed state")
		})
	}
}

func TestDispatcherNotifiesAfterUnwindToGenesis(t *testing.T) {
	_, tx := temporaltest.NewTestTx(t)
	header := &types.Header{Number: *uint256.NewInt(1)}
	require.NoError(t, rawdb.WriteHeader(tx, header))
	require.NoError(t, rawdb.WriteCanonicalHash(tx, header.Hash(), 1))
	events := shards.NewEvents()
	headers, unsubscribe := events.AddHeaderSubscription()
	t.Cleanup(unsubscribe)
	dispatcher := NewDispatcher(chain.AllProtocolChanges, events, nil, log.New())
	unwindPoint := uint64(0)

	require.NoError(t, dispatcher.Dispatch(t.Context(), tx, 0, nil, nil, 1, 1, &unwindPoint))

	select {
	case got := <-headers:
		require.Len(t, got, 1)
	default:
		t.Fatal("header after genesis unwind was not announced")
	}
}

func TestDispatcherCapsHeadersAfterDeepUnwind(t *testing.T) {
	_, tx := temporaltest.NewTestTx(t)
	for blockNum := uint64(1); blockNum <= 1_500; blockNum++ {
		header := &types.Header{Number: *uint256.NewInt(blockNum)}
		require.NoError(t, rawdb.WriteHeader(tx, header))
		require.NoError(t, rawdb.WriteCanonicalHash(tx, header.Hash(), blockNum))
	}
	events := shards.NewEvents()
	headers, unsubscribe := events.AddHeaderSubscription()
	t.Cleanup(unsubscribe)
	dispatcher := NewDispatcher(chain.AllProtocolChanges, events, nil, log.New())
	unwindPoint := uint64(0)

	require.NoError(t, dispatcher.Dispatch(t.Context(), tx, 0, nil, nil, 2_000, 1_500, &unwindPoint))

	select {
	case got := <-headers:
		require.Len(t, got, 1_024)
		var first, last types.Header
		require.NoError(t, rlp.DecodeBytes(got[0], &first))
		require.NoError(t, rlp.DecodeBytes(got[len(got)-1], &last))
		require.Equal(t, uint64(477), first.Number.Uint64())
		require.Equal(t, uint64(1_500), last.Number.Uint64())
	default:
		t.Fatal("headers after deep unwind were not announced")
	}
}

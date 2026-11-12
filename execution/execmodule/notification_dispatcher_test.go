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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
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

func TestStateChangesSnapshotMissingExecutedHeader(t *testing.T) {
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return stages.SaveStageProgress(tx, stages.Execution, 1)
	}))
	dispatcher := NewDispatcher(chain.AllProtocolChanges, freezeblocks.NewBlockReader(nil), nil, nil, nil, log.New())
	_, err := dispatcher.StateChangesSnapshot(t.Context(), db)
	require.ErrorContains(t, err, "executed header 1")

	header := &types.Header{Number: *uint256.NewInt(1), GasLimit: 30_000_000, BaseFee: uint256.NewInt(1_000_000_000)}
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		if writeErr := rawdb.WriteHeader(tx, header); writeErr != nil {
			return writeErr
		}
		return rawdb.WriteCanonicalHash(tx, header.Hash(), 1)
	}))
	batch, err := dispatcher.StateChangesSnapshot(t.Context(), db)
	require.NoError(t, err)
	require.Len(t, batch.ChangeBatch, 1)
	require.Equal(t, uint64(1), batch.ChangeBatch[0].BlockHeight)
}

func TestStateChangesSnapshotUsesExecutionProgress(t *testing.T) {
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	finished := &types.Header{Number: *uint256.NewInt(1), GasLimit: 30_000_000, BaseFee: uint256.NewInt(1_000_000_000)}
	executed := types.CopyHeader(finished)
	executed.Number.SetUint64(2)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		for _, header := range []*types.Header{finished, executed} {
			if err := rawdb.WriteHeader(tx, header); err != nil {
				return err
			}
			if err := rawdb.WriteCanonicalHash(tx, header.Hash(), header.Number.Uint64()); err != nil {
				return err
			}
		}
		if err := stages.SaveStageProgress(tx, stages.Finish, 1); err != nil {
			return err
		}
		return stages.SaveStageProgress(tx, stages.Execution, 2)
	}))
	dispatcher := NewDispatcher(chain.AllProtocolChanges, freezeblocks.NewBlockReader(nil), nil, nil, nil, log.New())
	batch, err := dispatcher.StateChangesSnapshot(t.Context(), db)
	require.NoError(t, err)
	require.Len(t, batch.ChangeBatch, 1)
	require.Equal(t, gointerfaces.ConvertHashToH256(executed.Hash()), batch.ChangeBatch[0].BlockHash)
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
	require.NoError(t, rawdb.WriteCanonicalHash(tx, header.Hash(), header.Number.Uint64()))
	require.NoError(t, rawdb.WriteHeadHeaderHash(tx, header.Hash()))

	accumulator := notifications.NewAccumulator()
	accumulator.StartChange(header, nil, false)
	capture := new(stateChangesCapture)
	dispatcher := NewDispatcher(chain.AllProtocolChanges, freezeblocks.NewBlockReader(nil), nil, capture, nil, log.New())

	const projectedStateVersion = uint64(7)
	require.NoError(t, dispatcher.Dispatch(
		t.Context(),
		tx,
		tx,
		projectedStateVersion,
		accumulator,
		nil,
		0,
		1,
		nil,
		nil,
	))
	require.NotNil(t, capture.batch)
	require.Equal(t, projectedStateVersion, capture.batch.StateVersionId)
}

func TestDispatcherUsesExecutedHead(t *testing.T) {
	_, tx := temporaltest.NewTestTx(t)
	executed := &types.Header{
		Number:                *uint256.NewInt(90),
		Time:                  1_080,
		GasLimit:              30_000_000,
		GasUsed:               15_000_000,
		BaseFee:               uint256.NewInt(1_000_000_000),
		WithdrawalsHash:       new(common.Hash),
		BlobGasUsed:           common.NewUint64(0),
		ExcessBlobGas:         common.NewUint64(0),
		ParentBeaconBlockRoot: new(common.Hash),
		RequestsHash:          new(common.Hash),
		BlockAccessListHash:   new(common.Hash),
		SlotNumber:            common.NewUint64(90),
	}
	ahead := types.CopyHeader(executed)
	ahead.Number.SetUint64(100)
	ahead.Time = 1_200
	ahead.GasLimit = 40_000_000
	ahead.GasUsed = 20_000_000
	ahead.BaseFee.SetUint64(2_000_000_000)
	*ahead.ExcessBlobGas = 100_000_000
	*ahead.SlotNumber = 100
	for _, header := range []*types.Header{executed, ahead} {
		require.NoError(t, rawdb.WriteHeader(tx, header))
		require.NoError(t, rawdb.WriteCanonicalHash(tx, header.Hash(), header.Number.Uint64()))
	}
	require.NoError(t, rawdb.WriteHeadHeaderHash(tx, ahead.Hash()))
	rawdb.WriteHeadBlockHash(tx, ahead.Hash())
	require.NoError(t, stages.SaveStageProgress(tx, stages.Finish, executed.Number.Uint64()))
	accumulator := notifications.NewAccumulator()
	accumulator.StartChange(executed, nil, false)
	capture := new(stateChangesCapture)
	dispatcher := NewDispatcher(chain.AllProtocolChanges, freezeblocks.NewBlockReader(nil), nil, capture, nil, log.New())

	require.NoError(t, dispatcher.Dispatch(t.Context(), tx, tx, 7, accumulator, nil, 89, 90, nil, nil))
	require.NotNil(t, capture.batch)
	require.Len(t, capture.batch.ChangeBatch, 1)
	head := capture.batch.ChangeBatch[0]
	require.Equal(t, executed.Number.Uint64(), head.BlockHeight)
	require.Equal(t, gointerfaces.ConvertHashToH256(executed.Hash()), head.BlockHash)
	require.Equal(t, executed.Time, head.BlockTime)
	require.Equal(t, executed.SlotNumber, head.SlotNumber)
	require.Equal(t, executed.BaseFee.Uint64(), capture.batch.PendingBlockBaseFee)
	require.Equal(t, chain.AllProtocolChanges.GetMinBlobGasPrice(), capture.batch.PendingBlobFeePerGas)
	require.Equal(t, executed.GasLimit, capture.batch.BlockGasLimit)
}

type notificationHeaderReader struct {
	dbservices.HeaderReader
	header *types.Header
}

func (r notificationHeaderReader) HeaderByNumber(context.Context, kv.Getter, uint64) (*types.Header, error) {
	return r.header, nil
}

func TestDispatcherClearsFrozenHeadNotifications(t *testing.T) {
	_, tx := temporaltest.NewTestTx(t)
	header := &types.Header{Number: *uint256.NewInt(1), GasLimit: 30_000_000, BaseFee: uint256.NewInt(1_000_000_000)}
	require.Nil(t, rawdb.ReadHeaderByNumber(tx, 1))
	accumulator := notifications.NewAccumulator()
	accumulator.StartChange(header, nil, false)
	capture := new(stateChangesCapture)
	dispatcher := NewDispatcher(chain.AllProtocolChanges, notificationHeaderReader{header: header}, nil, capture, nil, log.New())
	require.NoError(t, dispatcher.Dispatch(t.Context(), tx, tx, 1, accumulator, nil, 0, 1, nil, nil))
	require.NotNil(t, capture.batch, "snapshot-only headers must still produce a state batch")
	require.Equal(t, gointerfaces.ConvertHashToH256(header.Hash()), capture.batch.ChangeBatch[0].BlockHash)
	require.False(t, accumulator.HasPendingChanges(), "delivered changes must not trigger another dispatch at the same head")
}

func TestDispatcherRefreshesIncompleteAccountChanges(t *testing.T) {
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
			require.NoError(t, rawdb.WriteCanonicalHash(doms.BlockOverlay(), header.Hash(), header.Number.Uint64()))
			require.NoError(t, rawdb.WriteHeadHeaderHash(doms.BlockOverlay(), header.Hash()))
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
			dispatcher := NewDispatcher(chain.AllProtocolChanges, freezeblocks.NewBlockReader(nil), nil, capture, nil, log.New())
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
			view, err := latest.View(ctx, tx)
			require.NoError(t, err)
			value, err = view.Get(address[:])
			require.NoError(t, err)
			require.Equal(t, want, value, "the pool must see the announced account before commit")

			require.NoError(t, doms.Flush(ctx, tx))
			stateVersion, err := rawdb.GetStateVersion(tx)
			require.NoError(t, err)
			require.Equal(t, stateVersion, capture.batch.StateVersionId)
			coherent := kvcache.New(kvcache.DefaultCoherentConfig)
			coherent.OnNewBlock(capture.batch)
			view, err = coherent.View(ctx, tx)
			require.NoError(t, err)
			value, err = view.Get(address[:])
			require.NoError(t, err)
			require.Equal(t, want, value, "the coherent cache must agree with the committed state")
		})
	}
}

type failingNotificationState struct {
	kv.TemporalGetter
	err error
}

func (s failingNotificationState) GetLatest(kv.Domain, []byte, kv.GetLatestOptions) ([]byte, kv.Step, error) {
	return nil, 0, s.err
}

func TestDispatcherDoesNotPublishBeforeAccountRefresh(t *testing.T) {
	_, tx := temporaltest.NewTestTx(t)
	header := &types.Header{Number: *uint256.NewInt(1), GasLimit: 30_000_000, BaseFee: uint256.NewInt(1_000_000_000)}
	require.NoError(t, rawdb.WriteHeader(tx, header))
	require.NoError(t, rawdb.WriteCanonicalHash(tx, header.Hash(), 1))
	require.NoError(t, rawdb.WriteHeadHeaderHash(tx, header.Hash()))
	accumulator := notifications.NewAccumulator()
	accumulator.StartChange(&types.Header{}, nil, false)
	accumulator.ChangeAccount(common.Address{1}, 0, nil)
	accumulator.MarkIncomplete()
	recorded := append([]notifications.StateChange(nil), accumulator.Changes()...)
	events := shards.NewEvents()
	headers, unsubscribe := events.AddHeaderSubscription()
	t.Cleanup(unsubscribe)
	capture := new(stateChangesCapture)
	dispatcher := NewDispatcher(chain.AllProtocolChanges, freezeblocks.NewBlockReader(nil), events, capture, nil, log.New())
	readErr := errors.New("account read failed")
	state := failingNotificationState{TemporalGetter: tx, err: readErr}

	require.ErrorIs(t, dispatcher.Dispatch(t.Context(), tx, state, 1, accumulator, nil, 0, 1, nil, nil), readErr)
	select {
	case <-headers:
		t.Fatal("a failed account refresh must not publish headers")
	default:
	}
	require.Nil(t, capture.batch)
	require.Equal(t, recorded, accumulator.Changes(), "failed preparation must leave recorded changes untouched")
}

func TestDispatcherNotifiesAfterUnwindToGenesis(t *testing.T) {
	_, tx := temporaltest.NewTestTx(t)
	header := &types.Header{Number: *uint256.NewInt(1)}
	require.NoError(t, rawdb.WriteHeader(tx, header))
	require.NoError(t, rawdb.WriteCanonicalHash(tx, header.Hash(), 1))
	events := shards.NewEvents()
	headers, unsubscribe := events.AddHeaderSubscription()
	t.Cleanup(unsubscribe)
	dispatcher := NewDispatcher(chain.AllProtocolChanges, freezeblocks.NewBlockReader(nil), events, nil, nil, log.New())
	unwindPoint := uint64(0)

	require.NoError(t, dispatcher.Dispatch(t.Context(), tx, tx, 0, nil, nil, 1, 1, &unwindPoint, nil))

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
	dispatcher := NewDispatcher(chain.AllProtocolChanges, freezeblocks.NewBlockReader(nil), events, nil, nil, log.New())
	unwindPoint := uint64(0)

	require.NoError(t, dispatcher.Dispatch(t.Context(), tx, tx, 0, nil, nil, 2_000, 1_500, &unwindPoint, nil))

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

func TestDispatcherNoOpDoesNotRepeatHeader(t *testing.T) {
	_, tx := temporaltest.NewTestTx(t)
	header := &types.Header{Number: *uint256.NewInt(1), GasLimit: 30_000_000, BaseFee: uint256.NewInt(1_000_000_000)}
	require.NoError(t, rawdb.WriteHeader(tx, header))
	require.NoError(t, rawdb.WriteCanonicalHash(tx, header.Hash(), 1))
	require.NoError(t, rawdb.WriteHeadHeaderHash(tx, header.Hash()))
	events := shards.NewEvents()
	headers, unsubscribe := events.AddHeaderSubscription()
	t.Cleanup(unsubscribe)
	capture := new(stateChangesCapture)
	dispatcher := NewDispatcher(chain.AllProtocolChanges, freezeblocks.NewBlockReader(nil), events, capture, nil, log.New())
	require.NoError(t, dispatcher.Dispatch(t.Context(), tx, tx, 1, notifications.NewAccumulator(), nil, 1, 1, nil, nil))
	require.NotNil(t, capture.batch, "the pool still needs the head notification")
	require.Empty(t, headers, "unchanged finish height must not reannounce the current header")
}

func oneLogBlockReceipts(blockNum uint64) notifications.BlockReceipts {
	header := &types.Header{Number: *uint256.NewInt(blockNum)}
	l := &types.Log{BlockNumber: hexutil.Uint64(blockNum)}
	receipt := &types.Receipt{BlockNumber: uint256.NewInt(blockNum), Logs: types.Logs{l}}
	return notifications.BlockReceipts{Receipts: types.Receipts{receipt}, Txs: []types.Transaction{types.NewTransaction(blockNum, common.Address{}, uint256.NewInt(0), 0, uint256.NewInt(0), nil)}, Header: header}
}

func loadOneLogBlockReceipts(_ context.Context, _ kv.TemporalTx, blockNum uint64) (notifications.BlockReceipts, error) {
	return oneLogBlockReceipts(blockNum), nil
}

func writeHeadersAndFillCache(t *testing.T, tx kv.RwTx, recent *notifications.RecentReceipts, blocks uint64) {
	t.Helper()
	for blockNum := uint64(1); blockNum <= blocks; blockNum++ {
		b := oneLogBlockReceipts(blockNum)
		require.NoError(t, rawdb.WriteHeader(tx, b.Header))
		require.NoError(t, rawdb.WriteCanonicalHash(tx, b.Header.Hash(), blockNum))
		recent.Add(b.Receipts, b.Txs, b.Header)
	}
}

func TestDispatcherDeliversLogsOfBlocksMissingFromTheCache(t *testing.T) {
	const blocks = notifications.MaxNotifiedBlocks
	_, tx := temporaltest.NewTestTx(t)
	recent := notifications.NewRecentReceipts(notifications.MaxNotifiedBlocks)
	writeHeadersAndFillCache(t, tx, recent, blocks)
	recent.Clear()
	events := shards.NewEvents()
	logs, unsubscribe := events.AddLogsSubscription()
	t.Cleanup(unsubscribe)
	events.EmptyLogSubscription(false)
	dispatcher := NewDispatcher(chain.AllProtocolChanges, freezeblocks.NewBlockReader(nil), events, nil, loadOneLogBlockReceipts, log.New())

	require.NoError(t, dispatcher.Dispatch(t.Context(), tx, tx, 0, nil, recent, 0, blocks, nil, tx))

	select {
	case got := <-logs:
		require.Len(t, got, blocks, "every block's log is delivered")
		for i, l := range got {
			require.Equal(t, uint64(i+1), uint64(l.BlockNumber), "in block order")
		}
	default:
		t.Fatal("no logs delivered")
	}
}

func TestDispatcherDeliversReceiptsOfBlocksMissingFromTheCache(t *testing.T) {
	const blocks = notifications.MaxNotifiedBlocks
	_, tx := temporaltest.NewTestTx(t)
	recent := notifications.NewRecentReceipts(notifications.MaxNotifiedBlocks)
	writeHeadersAndFillCache(t, tx, recent, blocks)
	recent.Clear()
	events := shards.NewEvents()
	receipts, unsubscribe := events.AddReceiptsSubscription()
	t.Cleanup(unsubscribe)
	events.EmptyReceiptSubscription(false)
	dispatcher := NewDispatcher(chain.AllProtocolChanges, freezeblocks.NewBlockReader(nil), events, nil, loadOneLogBlockReceipts, log.New())

	require.NoError(t, dispatcher.Dispatch(t.Context(), tx, tx, 0, nil, recent, 0, blocks, nil, tx))

	select {
	case got := <-receipts:
		require.Len(t, got, blocks, "every block's receipt is delivered")
		for i, r := range got {
			blockNum := uint64(i + 1)
			require.Equal(t, blockNum, r.Receipt.BlockNumber.Uint64(), "in block order")
			require.Equal(t, blockNum, r.Header.Number.Uint64(), "with the block's header")
			require.Equal(t, blockNum, r.Tx.GetNonce(), "with the block's transaction")
		}
	default:
		t.Fatal("no receipts delivered")
	}
}

func TestDispatcherSendsRemovedLogsOfBlocksLoadedAfterACacheMiss(t *testing.T) {
	const blocks = 10
	_, tx := temporaltest.NewTestTx(t)
	recent := notifications.NewRecentReceipts(notifications.MaxNotifiedBlocks)
	writeHeadersAndFillCache(t, tx, recent, blocks)
	recent.Clear()
	events := shards.NewEvents()
	logs, unsubscribe := events.AddLogsSubscription()
	t.Cleanup(unsubscribe)
	events.EmptyLogSubscription(false)
	dispatcher := NewDispatcher(chain.AllProtocolChanges, freezeblocks.NewBlockReader(nil), events, nil, loadOneLogBlockReceipts, log.New())
	require.NoError(t, dispatcher.Dispatch(t.Context(), tx, tx, 0, nil, recent, 0, blocks, nil, tx))
	require.Len(t, <-logs, blocks)

	recent.Unwind(0)
	require.NoError(t, dispatcher.Dispatch(t.Context(), tx, tx, 0, nil, recent, blocks, blocks, nil, nil))

	select {
	case got := <-logs:
		require.Len(t, got, blocks, "every loaded block's log is removed")
		for _, l := range got {
			require.True(t, l.Removed)
		}
	default:
		t.Fatal("no removed logs delivered")
	}
}

// The Engine API path has no committed tx, so the cache alone must cover the longest notify span.
func TestDispatcherForkchoicePathDeliversLogsOfTheWholeNotifySpan(t *testing.T) {
	const blocks = 1_500
	_, tx := temporaltest.NewTestTx(t)
	recent := shards.NewNotifications(nil).RecentReceipts
	writeHeadersAndFillCache(t, tx, recent, blocks)
	events := shards.NewEvents()
	logs, unsubscribe := events.AddLogsSubscription()
	t.Cleanup(unsubscribe)
	events.EmptyLogSubscription(false)
	dispatcher := NewDispatcher(chain.AllProtocolChanges, freezeblocks.NewBlockReader(nil), events, nil, nil, log.New())

	require.NoError(t, dispatcher.Dispatch(t.Context(), tx, tx, 0, nil, recent, 0, blocks, nil, nil))

	select {
	case got := <-logs:
		require.Len(t, got, 1_024, "the whole notify span is delivered")
		require.Equal(t, uint64(blocks-1_024+1), uint64(got[0].BlockNumber))
		require.Equal(t, uint64(blocks), uint64(got[len(got)-1].BlockNumber))
	default:
		t.Fatal("no logs delivered")
	}
}

func TestDispatcherKeepsBlocksAboveFinalizedWithoutSubscribers(t *testing.T) {
	const blocks = 3
	_, tx := temporaltest.NewTestTx(t)
	recent := notifications.NewRecentReceipts(notifications.MaxNotifiedBlocks)
	writeHeadersAndFillCache(t, tx, recent, blocks)
	rawdb.WriteForkchoiceFinalized(tx, oneLogBlockReceipts(2).Header.Hash())
	dispatcher := NewDispatcher(chain.AllProtocolChanges, freezeblocks.NewBlockReader(nil), shards.NewEvents(), nil, nil, log.New())

	require.NoError(t, dispatcher.Dispatch(t.Context(), tx, tx, 0, nil, recent, 0, blocks, nil, nil))

	require.Equal(t, []uint64{1, 2}, recent.Missing(1, blocks+1))
}

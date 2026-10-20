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

package das

import (
	"context"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	blob_storage_mock_services "github.com/erigontech/erigon/cl/persistence/blob_storage/mock_services"
	"github.com/erigontech/erigon/cl/rpc"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/node/gointerfaces/sentinelproto"
)

// fuluColumnBlock is a Fulu block with one blob commitment at slot.
func fuluColumnBlock(t *testing.T, cfg *clparams.BeaconChainConfig, slot uint64) (*cltypes.SignedBeaconBlock, common.Hash) {
	t.Helper()
	block := cltypes.NewSignedBeaconBlock(cfg, clparams.FuluVersion)
	block.Block.Slot = slot
	block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	return block, root
}

// queuedRoot queues a root that is due as soon as it is not in a round and past its backoff.
func queuedRoot(t *testing.T) (*deferredColumnSyncs, common.Hash) {
	cfg := clparams.MainnetBeaconConfig
	block, root := fuluColumnBlock(t, &cfg, 1)
	queue := &deferredColumnSyncs{}
	queue.add(root, block, time.Time{})
	return queue, root
}

func isDue(queue *deferredColumnSyncs, root common.Hash, now time.Time) bool {
	roots, _ := queue.due(now, 0)
	return slices.Contains(roots, root)
}

func queued(queue *deferredColumnSyncs, root common.Hash) bool {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	_, ok := queue.entries[root]
	return ok
}

func attempts(queue *deferredColumnSyncs, root common.Hash) int {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if e := queue.entries[root]; e != nil {
		return e.attempts
	}
	return 0
}

// The backoff grows by a slot per failed round and stays at an epoch once it gets there; the
// queue reports that point, and the worker then gives up only a root that is no longer canonical.
func TestDeferredColumnSyncQueueCapsTheBackoffAtAnEpoch(t *testing.T) {
	const slotsPerEpoch = 8
	queue, root := queuedRoot(t)
	now := time.Unix(1000, 0)
	slot := 12 * time.Second

	require.True(t, isDue(queue, root, now))
	queue.start([]common.Hash{root})
	require.False(t, isDue(queue, root, now.Add(time.Hour)), "a root in a round is not picked again")

	for attempt := 1; attempt <= slotsPerEpoch+2; attempt++ {
		require.Equal(t, attempt >= slotsPerEpoch, queue.failed(root, now, slot, slotsPerEpoch), "attempt %d", attempt)
		wait := slot * time.Duration(min(attempt, slotsPerEpoch))
		require.False(t, isDue(queue, root, now.Add(wait-time.Millisecond)), "attempt %d", attempt)
		require.True(t, isDue(queue, root, now.Add(wait)), "attempt %d", attempt)
	}
}

// A storage error says nothing about peers serving the root, so it waits a slot without
// counting as an attempt.
func TestDeferredColumnSyncQueueRetriesAStorageErrorWithoutAnAttempt(t *testing.T) {
	queue, root := queuedRoot(t)
	now := time.Unix(1000, 0)
	slot := 12 * time.Second
	require.True(t, isDue(queue, root, now))
	for range 3 {
		queue.retryLater(root, now, slot)
		require.False(t, isDue(queue, root, now.Add(slot-time.Millisecond)))
		require.True(t, isDue(queue, root, now.Add(slot)))
	}
	require.False(t, queue.failed(root, now, slot, 2), "storage errors did not count as attempts")
}

// The canonical index covers the head chain only, so a slot above its head is undecided and the
// root is kept; a root is given up only when its slot names another block or none below the head.
func TestStillCanonicalReadsTheBeaconIndices(t *testing.T) {
	root, other := common.Hash{1}, common.Hash{2}
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		if err := beacon_indicies.MarkRootCanonical(tx, 5, root); err != nil {
			return err
		}
		return beacon_indicies.MarkRootCanonical(tx, 7, common.Hash{3})
	}))
	d := &peerdas{indiciesDB: db}
	require.True(t, d.stillCanonical(t.Context(), 5, root))
	require.False(t, d.stillCanonical(t.Context(), 5, other), "an orphaned root at a canonical slot")
	require.False(t, d.stillCanonical(t.Context(), 6, other), "a root at a slot the canonical chain skipped")
	require.True(t, d.stillCanonical(t.Context(), 8, other), "a root above the canonical head")
	require.True(t, (&peerdas{}).stillCanonical(t.Context(), 5, other), "without indices the root is kept")

	db.Close()
	require.True(t, d.stillCanonical(t.Context(), 5, other), "an index read error keeps the root")
}

// A storage error for a root dropped while in a round must not bring the root back.
func TestDeferredColumnSyncQueueRetryLaterAfterForgetLeavesNoEntry(t *testing.T) {
	queue, root := queuedRoot(t)
	queue.start([]common.Hash{root})
	queue.forget(root)

	queue.retryLater(root, time.Unix(1000, 0), 12*time.Second)

	require.Empty(t, queue.entries)
}

// A round that ends after its root was dropped must not bring the root back, and the due scan
// does not either.
func TestDeferredColumnSyncQueueFailedAfterForgetLeavesNoEntry(t *testing.T) {
	queue, root := queuedRoot(t)
	now := time.Unix(1000, 0)
	queue.start([]common.Hash{root})
	queue.forget(root)

	queue.failed(root, now, 12*time.Second, 32)

	require.False(t, isDue(queue, root, now))
	require.Empty(t, queue.entries)
}

// preGloasCustodyGrace is half the time to the attestation deadline: 1999.5 ms on 12 s slots,
// where ATTESTATION_DUE_BPS puts the deadline at 3999 ms.
func preGloasCustodyGrace(cfg *clparams.BeaconChainConfig) time.Duration {
	return time.Duration(cfg.AttestationDueMs(false)) * time.Millisecond / 2
}

// A custody node gives gossip half the time to the attestation deadline before Gloas, and half the
// window from the payload deadline to the PTC deadline from Gloas; an archive node gives it a full
// slot.
func TestDeferredColumnSyncDelay(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	fulu := cltypes.NewSignedBeaconBlock(&cfg, clparams.FuluVersion)
	gloas := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	custody := &peerdas{beaconConfig: &cfg, caplinConfig: &clparams.CaplinConfig{}}
	require.Equal(t, preGloasCustodyGrace(&cfg), custody.deferredColumnSyncDelay(fulu))
	require.Equal(t, 1500*time.Millisecond, custody.deferredColumnSyncDelay(gloas))
	archive := &peerdas{beaconConfig: &cfg, caplinConfig: &clparams.CaplinConfig{ArchiveBlobs: true}}
	require.Equal(t, 12*time.Second, archive.deferredColumnSyncDelay(gloas))
}

// The tick is a quarter of the shortest custody grace, so a due root is picked up before most of
// the fetch half of the window has passed, also with short slots.
func TestDeferredColumnSyncTick(t *testing.T) {
	mainnet := clparams.MainnetBeaconConfig
	require.Equal(t, 375*time.Millisecond, (&peerdas{beaconConfig: &mainnet}).deferredColumnSyncTick())

	fiveSecondSlots := clparams.MainnetBeaconConfig
	fiveSecondSlots.SecondsPerSlot = 5
	d := &peerdas{beaconConfig: &fiveSecondSlots}
	require.Equal(t, 833*time.Millisecond, d.custodyGrace(false))
	require.Equal(t, 625*time.Millisecond, d.custodyGrace(true))
	require.Equal(t, 156250*time.Microsecond, d.deferredColumnSyncTick())
}

// The first attempt is due one grace after the later of the slot start and the enqueue: a Gloas
// root is queued mid-slot, when its columns are still arriving. Queuing the root again keeps it.
func TestSyncColumnDataLaterSchedulesTheFirstAttempt(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.FuluForkEpoch = 0
	cfg.InitializeForkSchedule()
	for _, tc := range []struct {
		name      string
		slotStart time.Duration
	}{
		{name: "slot started before the enqueue", slotStart: -5 * time.Second},
		{name: "slot starts after the enqueue", slotStart: 5 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			block, root := fuluColumnBlock(t, &cfg, 100)
			slotStart := time.Now().Add(tc.slotStart)
			clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
			clock.EXPECT().GetSlotTime(uint64(100)).Return(slotStart).AnyTimes()
			d := &peerdas{beaconConfig: &cfg, ethClock: clock, caplinConfig: &clparams.CaplinConfig{}}

			before := time.Now()
			require.NoError(t, d.SyncColumnDataLater(block))
			after := time.Now()
			first := d.blocksToCheckSync.entries[root].nextAttempt
			grace := preGloasCustodyGrace(&cfg)
			if tc.slotStart > 0 {
				require.Equal(t, slotStart.Add(grace), first)
			} else {
				require.WithinRange(t, first, before.Add(grace), after.Add(grace))
			}

			require.NoError(t, d.SyncColumnDataLater(block))
			require.Equal(t, first, d.blocksToCheckSync.entries[root].nextAttempt)
		})
	}
}

func TestSyncColumnDataWorkerDropsRootsThatLeftTheServeRange(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.FuluForkEpoch = 0
	cfg.InitializeForkSchedule()
	block, root := fuluColumnBlock(t, &cfg, 1)
	clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
	currentSlot := (cfg.MinEpochsForDataColumnSidecarsRequests + 1) * cfg.SlotsPerEpoch
	clock.EXPECT().GetCurrentSlot().Return(currentSlot).AnyTimes()
	// A root still in range would wait for its grace and stay queued.
	clock.EXPECT().GetSlotTime(gomock.Any()).Return(time.Now().Add(time.Hour)).AnyTimes()
	d := &peerdas{beaconConfig: &cfg, ethClock: clock, caplinConfig: &clparams.CaplinConfig{}}
	require.NoError(t, d.SyncColumnDataLater(block))

	go d.syncColumnDataWorker(t.Context())
	require.Eventually(t, func() bool {
		return !queued(&d.blocksToCheckSync, root)
	}, 5*time.Second, 10*time.Millisecond)
}

func TestSyncColumnDataWorkerForgetsAnAvailableRoot(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.FuluForkEpoch = 0
	cfg.SecondsPerSlot = 1 // an archive node gives gossip one slot from enqueue
	cfg.InitializeForkSchedule()
	block, root := fuluColumnBlock(t, &cfg, 1)
	ctrl := gomock.NewController(t)
	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetCurrentSlot().Return(uint64(2)).AnyTimes()
	clock.EXPECT().GetSlotTime(gomock.Any()).Return(time.Now().Add(-time.Hour)).AnyTimes()
	columnStorage := blob_storage_mock_services.NewMockDataColumnStorage(ctrl)
	columnStorage.EXPECT().GetSavedColumnIndex(gomock.Any(), uint64(1), root).Return(nil, nil).AnyTimes()
	blobStorage := blob_storage_mock_services.NewMockBlobStorage(ctrl)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), root).Return(uint32(1), nil).AnyTimes()
	d := &peerdas{
		beaconConfig:  &cfg,
		ethClock:      clock,
		caplinConfig:  &clparams.CaplinConfig{ArchiveBlobs: true},
		columnStorage: columnStorage,
		blobStorage:   blobStorage,
	}
	require.NoError(t, d.SyncColumnDataLater(block))

	go d.syncColumnDataWorker(t.Context())
	require.Eventually(t, func() bool {
		return !queued(&d.blocksToCheckSync, root)
	}, 5*time.Second, 10*time.Millisecond)
}

// peerCountSentinel reports active peers that serve no columns: before Fulu the RPC's column-peer
// refresher stays idle.
type peerCountSentinel struct {
	sentinelproto.SentinelClient
	active uint64
	calls  atomic.Int32
}

func (s *peerCountSentinel) GetPeers(context.Context, *sentinelproto.EmptyMessage, ...grpc.CallOption) (*sentinelproto.PeerCount, error) {
	s.calls.Add(1)
	return &sentinelproto.PeerCount{Active: s.active}, nil
}

// Without peers no download can run, so the worker does not scan the stored columns either.
func TestSyncColumnDataWorkerSkipsTheAvailabilityScanWithoutPeers(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.FuluForkEpoch = 0
	cfg.SecondsPerSlot = 1
	cfg.InitializeForkSchedule()
	block, _ := fuluColumnBlock(t, &cfg, 1)
	ctrl := gomock.NewController(t)
	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetCurrentSlot().Return(uint64(2)).AnyTimes()
	// The RPC's column-peer refresher stays idle before Fulu.
	clock.EXPECT().GetCurrentEpoch().Return(uint64(0)).AnyTimes()
	clock.EXPECT().StateVersionByEpoch(gomock.Any()).Return(clparams.DenebVersion).AnyTimes()
	clock.EXPECT().GetSlotTime(gomock.Any()).Return(time.Now().Add(-time.Hour)).AnyTimes()
	var scans atomic.Int32
	sentinel := &peerCountSentinel{}
	columnStorage := blob_storage_mock_services.NewMockDataColumnStorage(ctrl)
	columnStorage.EXPECT().GetSavedColumnIndex(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, uint64, common.Hash) ([]uint64, error) {
			scans.Add(1)
			return nil, nil
		}).AnyTimes()
	blobStorage := blob_storage_mock_services.NewMockBlobStorage(ctrl)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(0), nil).AnyTimes()
	d := &peerdas{
		beaconConfig:  &cfg,
		ethClock:      clock,
		caplinConfig:  &clparams.CaplinConfig{ArchiveBlobs: true},
		columnStorage: columnStorage,
		blobStorage:   blobStorage,
		rpc:           rpc.NewBeaconRpcP2P(t.Context(), sentinel, &cfg, clock, nil),
	}
	require.NoError(t, d.SyncColumnDataLater(block))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go d.syncColumnDataWorker(ctx)
	// The worker asks for peers only once the root is due; let it find none a few times.
	require.Eventually(t, func() bool { return sentinel.calls.Load() >= 3 }, 10*time.Second, 10*time.Millisecond)
	require.Zero(t, scans.Load())
}

// unservedRootWorker queues a Fulu root at slot 1 that no peer serves, on an archive node whose
// first failed round reaches the backoff cap, with canonicalAtSlot marked canonical at slot 1.
func unservedRootWorker(t *testing.T, canonicalAtSlot func(root common.Hash) common.Hash) (*peerdas, common.Hash) {
	t.Helper()
	cfg := clparams.MainnetBeaconConfig
	cfg.FuluForkEpoch = 0
	cfg.SecondsPerSlot = 1
	cfg.SlotsPerEpoch = 1 // the first failed round reaches the backoff cap
	cfg.InitializeForkSchedule()
	block, root := fuluColumnBlock(t, &cfg, 1)
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.MarkRootCanonical(tx, 1, canonicalAtSlot(root))
	}))
	ctrl := gomock.NewController(t)
	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetCurrentSlot().Return(uint64(2)).AnyTimes()
	clock.EXPECT().GetCurrentEpoch().Return(uint64(0)).AnyTimes()
	clock.EXPECT().StateVersionByEpoch(gomock.Any()).Return(clparams.DenebVersion).AnyTimes()
	clock.EXPECT().GetSlotTime(gomock.Any()).Return(time.Now().Add(-time.Hour)).AnyTimes()
	columnStorage := blob_storage_mock_services.NewMockDataColumnStorage(ctrl)
	columnStorage.EXPECT().GetSavedColumnIndex(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	blobStorage := blob_storage_mock_services.NewMockBlobStorage(ctrl)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(0), nil).AnyTimes()
	d := &peerdas{
		beaconConfig:  &cfg,
		ethClock:      clock,
		caplinConfig:  &clparams.CaplinConfig{ArchiveBlobs: true},
		columnStorage: columnStorage,
		blobStorage:   blobStorage,
		indiciesDB:    db,
		rpc:           rpc.NewBeaconRpcP2P(t.Context(), &peerCountSentinel{active: 1}, &cfg, clock, nil),
	}
	require.NoError(t, d.SyncColumnDataLater(block))
	return d, root
}

// A round that leaves columns missing ends at its deadline. Once the backoff reaches its cap, the
// canonical check must still read the indices, so an orphaned root nobody serves is given up.
func TestSyncColumnDataWorkerGivesUpAnOrphanAfterATimedOutRound(t *testing.T) {
	d, root := unservedRootWorker(t, func(common.Hash) common.Hash { return common.Hash{0xaa} })

	go d.syncColumnDataWorker(t.Context())
	require.Eventually(t, func() bool {
		return !queued(&d.blocksToCheckSync, root)
	}, 8*time.Second, 50*time.Millisecond)
}

// A canonical root at the backoff cap is kept and tried again. failed counts an attempt before the
// canonical check, so only a second attempt shows that the root was kept.
func TestSyncColumnDataWorkerKeepsACanonicalRootAtTheBackoffCap(t *testing.T) {
	d, root := unservedRootWorker(t, func(root common.Hash) common.Hash { return root })

	go d.syncColumnDataWorker(t.Context())
	require.Eventually(t, func() bool {
		return attempts(&d.blocksToCheckSync, root) >= 2
	}, 10*time.Second, 50*time.Millisecond)
}

// With five-second slots the worker picks a due root up within its tick, well before a one-second
// tick would.
func TestSyncColumnDataWorkerPicksADueRootUpWithinTheTick(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.FuluForkEpoch = 0
	cfg.SecondsPerSlot = 5
	cfg.InitializeForkSchedule()
	block, root := fuluColumnBlock(t, &cfg, 1)
	ctrl := gomock.NewController(t)
	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetCurrentSlot().Return(uint64(2)).AnyTimes()
	scanned := make(chan struct{}, 1)
	columnStorage := blob_storage_mock_services.NewMockDataColumnStorage(ctrl)
	columnStorage.EXPECT().GetSavedColumnIndex(gomock.Any(), uint64(1), root).DoAndReturn(
		func(context.Context, uint64, common.Hash) ([]uint64, error) {
			select {
			case scanned <- struct{}{}:
			default:
			}
			// Every column is stored, so no download round starts.
			columns := make([]uint64, cfg.NumberOfColumns)
			for i := range columns {
				columns[i] = uint64(i)
			}
			return columns, nil
		}).AnyTimes()
	blobStorage := blob_storage_mock_services.NewMockBlobStorage(ctrl)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), root).Return(uint32(1), nil).AnyTimes()
	d := &peerdas{
		beaconConfig:  &cfg,
		ethClock:      clock,
		caplinConfig:  &clparams.CaplinConfig{ArchiveBlobs: true},
		columnStorage: columnStorage,
		blobStorage:   blobStorage,
	}
	d.blocksToCheckSync.add(root, block, time.Now())

	start := time.Now()
	go d.syncColumnDataWorker(t.Context())
	select {
	case <-scanned:
		require.Less(t, time.Since(start), 600*time.Millisecond)
	case <-time.After(5 * time.Second):
		t.Fatal("the due root was not picked up")
	}
}

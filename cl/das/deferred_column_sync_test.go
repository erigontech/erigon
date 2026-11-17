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
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	peerdasstate "github.com/erigontech/erigon/cl/das/state"
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
	return fuluColumnBlockWithBlobs(t, cfg, slot, 1)
}

func fuluColumnBlockWithBlobs(t *testing.T, cfg *clparams.BeaconChainConfig, slot uint64, blobs int) (*cltypes.SignedBeaconBlock, common.Hash) {
	t.Helper()
	block := cltypes.NewSignedBeaconBlock(cfg, clparams.FuluVersion)
	block.Block.Slot = slot
	for range blobs {
		block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	}
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

// pristineRoot queues a root with blobs commitments whose first attempt is an hour away, and a wake channel.
func pristineRoot(t *testing.T, blobs int) (*deferredColumnSyncs, common.Hash) {
	t.Helper()
	cfg := clparams.MainnetBeaconConfig
	block, root := fuluColumnBlockWithBlobs(t, &cfg, 1, blobs)
	queue := &deferredColumnSyncs{wake: make(chan struct{}, 1)}
	queue.add(root, block, time.Now().Add(time.Hour))
	return queue, root
}

func nextAttempt(queue *deferredColumnSyncs, root common.Hash) time.Time {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	return queue.entries[root].nextAttempt
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
		rpc:           rpc.NewBeaconRpcP2P(t.Context(), sentinel, &cfg, clock),
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
		rpc:           rpc.NewBeaconRpcP2P(t.Context(), &peerCountSentinel{active: 1}, &cfg, clock),
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

// A recovery holds for a queued root only when FK20 would be slower than a by-root round, which it is from nine blobs.
func TestDeferredColumnSyncPrioritiseFirstAttemptNeedsAQueuedRootWithNineBlobs(t *testing.T) {
	now := time.Unix(1000, 0)
	t.Run("absent root", func(t *testing.T) {
		queue, _ := pristineRoot(t, 9)

		hold, moved, _ := queue.prioritiseFirstAttempt(common.Hash{0xff}, now)

		require.False(t, hold)
		require.False(t, moved)
		require.Empty(t, queue.wake)
	})
	t.Run("8 blobs", func(t *testing.T) {
		queue, root := pristineRoot(t, 8)
		first := nextAttempt(queue, root)

		hold, moved, _ := queue.prioritiseFirstAttempt(root, now)

		require.False(t, hold)
		require.False(t, moved)
		require.False(t, isDue(queue, root, now))
		require.Empty(t, queue.wake)
		require.Equal(t, first, nextAttempt(queue, root), "the first attempt stays where it was")
	})
}

// The by-root round publishes nothing for a backfilled root, so holding for it would only delay FK20.
func TestDeferredColumnSyncPrioritiseFirstAttemptDoesNotHoldForABackfilledRoot(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	block, root := fuluColumnBlockWithBlobs(t, &cfg, 1, 9)
	backfilled := &backfillColumnBlock{version: clparams.FuluVersion, slot: 1, root: root, commitments: block.GetBlobKzgCommitments()}
	queue := &deferredColumnSyncs{wake: make(chan struct{}, 1)}
	firstAttempt := time.Now().Add(time.Hour)
	queue.add(root, backfilled, firstAttempt)

	hold, moved, _ := queue.prioritiseFirstAttempt(root, time.Unix(1000, 0))

	require.False(t, hold)
	require.False(t, moved)
	require.Empty(t, queue.wake)
	require.Equal(t, firstAttempt, nextAttempt(queue, root))
	require.False(t, queue.entries[root].prioritised)
}

// A first attempt that has not started is moved to the decision time and the worker is woken, so the hold ends at the
// by-root round and not at the grace.
func TestDeferredColumnSyncPrioritiseFirstAttemptMovesAPristineAttemptToNow(t *testing.T) {
	queue, root := pristineRoot(t, 9)
	now := time.Unix(1000, 0)
	require.False(t, isDue(queue, root, now))

	hold, moved, _ := queue.prioritiseFirstAttempt(root, now)

	require.True(t, hold)
	require.True(t, moved)
	require.Equal(t, now, nextAttempt(queue, root))
	require.True(t, isDue(queue, root, now))
	require.Len(t, queue.wake, 1)
}

// An attempt that is due already stays where it is, also when the worker has returned it as due but not started it yet.
// The worker is woken, in case it gave up that pass.
func TestDeferredColumnSyncPrioritiseFirstAttemptHoldsForAnAttemptThatIsDueButNotStarted(t *testing.T) {
	queue, root := pristineRoot(t, 9)
	queue.entries[root].nextAttempt = time.Unix(500, 0)
	now := time.Unix(1000, 0)
	selected, _ := queue.due(now, 0)
	require.Equal(t, []common.Hash{root}, selected)

	hold, moved, _ := queue.prioritiseFirstAttempt(root, now)

	require.True(t, hold)
	require.False(t, moved)
	require.Equal(t, time.Unix(500, 0), nextAttempt(queue, root))
	require.Len(t, queue.wake, 1)
}

// A first attempt in flight cannot be sped up: the recovery holds for it and the attempt is neither moved nor woken.
func TestDeferredColumnSyncPrioritiseFirstAttemptHoldsForAnAttemptInFlight(t *testing.T) {
	queue, root := pristineRoot(t, 9)
	queue.entries[root].nextAttempt = time.Unix(500, 0)
	queue.start([]common.Hash{root})

	hold, moved, blobs := queue.prioritiseFirstAttempt(root, time.Unix(1000, 0))

	require.True(t, hold)
	require.False(t, moved)
	require.Equal(t, 9, blobs)
	require.Equal(t, time.Unix(500, 0), nextAttempt(queue, root))
	require.Empty(t, queue.wake)
	require.True(t, queue.entries[root].inFlight)
}

// A backoff, whether from a storage error or a failed attempt, is not the by-root round the recovery would hold for.
func TestDeferredColumnSyncPrioritiseFirstAttemptLeavesABackoffAlone(t *testing.T) {
	now := time.Unix(1000, 0)
	slot := 12 * time.Second
	for _, tc := range []struct {
		name    string
		backoff func(queue *deferredColumnSyncs, root common.Hash)
	}{
		{"storage error", func(queue *deferredColumnSyncs, root common.Hash) { queue.retryLater(root, now, slot) }},
		{"failed attempt", func(queue *deferredColumnSyncs, root common.Hash) {
			queue.start([]common.Hash{root})
			queue.failed(root, now, slot, 32)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			queue, root := pristineRoot(t, 9)
			tc.backoff(queue, root)
			backedOff := nextAttempt(queue, root)

			hold, moved, _ := queue.prioritiseFirstAttempt(root, now)

			require.False(t, hold)
			require.False(t, moved)
			require.Equal(t, backedOff, nextAttempt(queue, root))
			require.Empty(t, queue.wake)
			require.False(t, queue.entries[root].prioritised)
		})
	}
}

// The hold is the root's, not the recovery's: it happens once while the root is queued, however many recoveries follow,
// and again only for a root queued anew.
func TestDeferredColumnSyncPrioritiseFirstAttemptHoldsOncePerQueuedRoot(t *testing.T) {
	queue, root := pristineRoot(t, 9)
	now := time.Unix(1000, 0)

	hold, _, _ := queue.prioritiseFirstAttempt(root, now)
	require.True(t, hold)
	require.Len(t, queue.wake, 1)
	<-queue.wake
	hold, moved, _ := queue.prioritiseFirstAttempt(root, now)
	require.False(t, hold, "a second recovery of the same root")
	require.False(t, moved)
	require.Empty(t, queue.wake)

	queue.forget(root)
	block, _ := fuluColumnBlockWithBlobs(t, &clparams.MainnetBeaconConfig, 1, 9)
	queue.add(root, block, now.Add(time.Hour))
	hold, _, _ = queue.prioritiseFirstAttempt(root, now)
	require.True(t, hold, "a root queued anew")
}

// A prioritised root in flight holds once, whatever the round does.
func TestDeferredColumnSyncPrioritiseFirstAttemptHoldsOnceWhileInFlight(t *testing.T) {
	queue, root := pristineRoot(t, 9)
	queue.start([]common.Hash{root})
	hold, _, _ := queue.prioritiseFirstAttempt(root, time.Unix(1000, 0))
	require.True(t, hold)

	hold, _, _ = queue.prioritiseFirstAttempt(root, time.Unix(1000, 0))

	require.False(t, hold)
}

// prioritisedRootWorker is a custody node with a root of nine blobs queued an hour ahead and no column stored, so the
// worker starts a round for the root when it scans it. scanned reports each scan.
func prioritisedRootWorker(t *testing.T, cfg *clparams.BeaconChainConfig) (d *peerdas, root common.Hash, scanned <-chan struct{}) {
	t.Helper()
	block, root := fuluColumnBlockWithBlobs(t, cfg, 1, 9)
	ctrl := gomock.NewController(t)
	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetCurrentSlot().Return(uint64(2)).AnyTimes()
	// The RPC's column-peer refresher stays idle before Fulu.
	clock.EXPECT().GetCurrentEpoch().Return(uint64(0)).AnyTimes()
	clock.EXPECT().StateVersionByEpoch(gomock.Any()).Return(clparams.DenebVersion).AnyTimes()
	scans := make(chan struct{}, 1)
	columnStorage := blob_storage_mock_services.NewMockDataColumnStorage(ctrl)
	columnStorage.EXPECT().GetSavedColumnIndex(gomock.Any(), uint64(1), root).DoAndReturn(
		func(context.Context, uint64, common.Hash) ([]uint64, error) {
			select {
			case scans <- struct{}{}:
			default:
			}
			return nil, nil
		}).AnyTimes()
	d = &peerdas{
		beaconConfig:  cfg,
		ethClock:      clock,
		caplinConfig:  &clparams.CaplinConfig{},
		state:         peerdasstate.NewPeerDasState(cfg, &clparams.NetworkConfig{}),
		columnStorage: columnStorage,
	}
	d.blocksToCheckSync.wake = make(chan struct{}, 1)
	d.blocksToCheckSync.add(root, block, time.Now().Add(time.Hour))
	return d, root, scans
}

// A prioritised root is scanned at once, not at the next tick of the worker.
func TestSyncColumnDataWorkerPicksAPrioritisedRootUpWithoutWaitingForTheTick(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.FuluForkEpoch = 0
	cfg.SecondsPerSlot = 120
	cfg.InitializeForkSchedule()
	d, root, scanned := prioritisedRootWorker(t, &cfg)
	require.Equal(t, time.Second, d.deferredColumnSyncTick())
	go d.syncColumnDataWorker(t.Context())
	time.Sleep(100 * time.Millisecond)

	hold, _, _ := d.blocksToCheckSync.prioritiseFirstAttempt(root, time.Now())
	prioritised := time.Now()

	require.True(t, hold)
	select {
	case <-scanned:
		require.Less(t, time.Since(prioritised), 500*time.Millisecond)
	case <-time.After(5 * time.Second):
		t.Fatal("the prioritised root was not scanned")
	}
}

// failingPeerSentinel fails the first peer count, finds no peers for the second and has one peer from then on.
type failingPeerSentinel struct {
	sentinelproto.SentinelClient
	calls atomic.Int32
}

func (s *failingPeerSentinel) GetPeers(context.Context, *sentinelproto.EmptyMessage, ...grpc.CallOption) (*sentinelproto.PeerCount, error) {
	switch s.calls.Add(1) {
	case 1:
		return nil, errors.New("sentinel unavailable")
	case 2:
		return &sentinelproto.PeerCount{}, nil
	}
	return &sentinelproto.PeerCount{Active: 1}, nil
}

// The worker that gives up on a prioritised root for want of peers leaves it due, so the next pass starts it.
func TestSyncColumnDataWorkerStartsAPrioritisedRootAfterThePeerCheckFailsOrFindsNoPeers(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.FuluForkEpoch = 0
	cfg.SecondsPerSlot = 1
	cfg.InitializeForkSchedule()
	d, root, _ := prioritisedRootWorker(t, &cfg)
	sentinel := &failingPeerSentinel{}
	d.rpc = rpc.NewBeaconRpcP2P(t.Context(), sentinel, &cfg, d.ethClock)
	hold, _, _ := d.blocksToCheckSync.prioritiseFirstAttempt(root, time.Now())
	require.True(t, hold)

	go d.syncColumnDataWorker(t.Context())

	require.Eventually(t, func() bool { return attempts(&d.blocksToCheckSync, root) > 0 }, 10*time.Second, 10*time.Millisecond,
		"the prioritised root was not started")
	require.GreaterOrEqual(t, sentinel.calls.Load(), int32(3))
}

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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	blob_storage_mock_services "github.com/erigontech/erigon/cl/persistence/blob_storage/mock_services"
	"github.com/erigontech/erigon/cl/rpc"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/node/gointerfaces/sentinelproto"
)

func TestDeferredColumnSyncDue(t *testing.T) {
	slotStart := time.Unix(1000, 0)
	delay := 2 * time.Second
	require.False(t, deferredColumnSyncDue(slotStart, slotStart, slotStart, delay))
	require.False(t, deferredColumnSyncDue(slotStart.Add(1999*time.Millisecond), slotStart, slotStart, delay))
	require.True(t, deferredColumnSyncDue(slotStart.Add(2*time.Second), slotStart, slotStart, delay))
	require.True(t, deferredColumnSyncDue(slotStart.Add(time.Minute), slotStart, slotStart, delay))
}

// The backoff grows by a slot per failed round; once it would reach an epoch the root is given
// up, which also releases a block that was orphaned and is never served.
func TestDeferredColumnSyncQueueGivesUpOnceTheBackoffReachesAnEpoch(t *testing.T) {
	const slotsPerEpoch = 8
	queue := newDeferredColumnSyncQueue()
	root := common.Hash{1}
	now := time.Unix(1000, 0)
	slot := 12 * time.Second

	require.True(t, queue.ready(root, now))
	queue.start([]common.Hash{root})
	require.False(t, queue.ready(root, now.Add(time.Hour)), "a root in a round is not picked again")

	for attempt := 1; attempt < slotsPerEpoch; attempt++ {
		require.False(t, queue.failed(root, now, slot, slotsPerEpoch), "attempt %d", attempt)
		wait := slot * time.Duration(attempt)
		require.False(t, queue.ready(root, now.Add(wait-time.Millisecond)), "attempt %d", attempt)
		require.True(t, queue.ready(root, now.Add(wait)), "attempt %d", attempt)
	}
	require.True(t, queue.failed(root, now, slot, slotsPerEpoch))
}

// A round that ends after its root was dropped must not bring the root back.
func TestDeferredColumnSyncQueueFailedAfterDoneLeavesNoEntry(t *testing.T) {
	queue := newDeferredColumnSyncQueue()
	root := common.Hash{1}
	now := time.Unix(1000, 0)
	queue.start([]common.Hash{root})
	queue.done(root)

	queue.failed(root, now, 12*time.Second, 32)

	require.Empty(t, queue.entries)
	require.True(t, queue.ready(root, now))
}

// Gossip gets the grace period from the later of the block's slot start and the moment the
// root was queued: a Gloas root is queued mid-slot, when its columns are still arriving.
func TestDeferredColumnSyncDueCountsTheGraceFromEnqueue(t *testing.T) {
	slotStart := time.Unix(1000, 0)
	queuedAt := slotStart.Add(5 * time.Second)
	delay := 2 * time.Second
	require.False(t, deferredColumnSyncDue(slotStart.Add(3*time.Second), slotStart, queuedAt, delay))
	require.False(t, deferredColumnSyncDue(queuedAt.Add(delay-time.Millisecond), slotStart, queuedAt, delay))
	require.True(t, deferredColumnSyncDue(queuedAt.Add(delay), slotStart, queuedAt, delay))
	// A root queued before its slot started waits from the slot start.
	require.True(t, deferredColumnSyncDue(slotStart.Add(delay), slotStart, slotStart.Add(-time.Second), delay))
}

func TestSyncColumnDataLaterRecordsTheEnqueueTime(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.FuluForkEpoch = 0
	cfg.InitializeForkSchedule()
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.FuluVersion)
	block.Block.Slot = 100
	block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	d := &peerdas{beaconConfig: &cfg}

	before := time.Now()
	require.NoError(t, d.SyncColumnDataLater(block))
	after := time.Now()
	queued, ok := d.blocksToCheckSync.Load(common.Hash(root))
	require.True(t, ok)
	require.WithinRange(t, queued.(deferredColumnSync).queuedAt, before, after)

	// Queuing the root again keeps the first enqueue time.
	require.NoError(t, d.SyncColumnDataLater(block))
	requeued, _ := d.blocksToCheckSync.Load(common.Hash(root))
	require.Equal(t, queued.(deferredColumnSync).queuedAt, requeued.(deferredColumnSync).queuedAt)
}

func TestSyncColumnDataWorkerDropsRootsThatLeftTheServeRange(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.FuluForkEpoch = 0
	cfg.InitializeForkSchedule()
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.FuluVersion)
	block.Block.Slot = 1
	block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
	currentSlot := (cfg.MinEpochsForDataColumnSidecarsRequests + 1) * cfg.SlotsPerEpoch
	clock.EXPECT().GetCurrentSlot().Return(currentSlot).AnyTimes()
	// A root still in range would wait for its grace and stay queued.
	clock.EXPECT().GetSlotTime(gomock.Any()).Return(time.Now().Add(time.Hour)).AnyTimes()
	d := &peerdas{beaconConfig: &cfg, ethClock: clock, caplinConfig: &clparams.CaplinConfig{}}
	require.NoError(t, d.SyncColumnDataLater(block))

	go d.syncColumnDataWorker(t.Context())
	require.Eventually(t, func() bool {
		_, queued := d.blocksToCheckSync.Load(common.Hash(root))
		return !queued
	}, 5*time.Second, 10*time.Millisecond)
}

type retryRecordingForkChoice struct {
	BlockGetter
	retried chan common.Hash
}

func (f retryRecordingForkChoice) RetryPendingExecutionPayloadEnvelope(_ context.Context, root common.Hash) {
	f.retried <- root
}

func TestSyncColumnDataWorkerRetriesTheEnvelopeOfAnAvailableRoot(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.FuluForkEpoch = 0
	cfg.SecondsPerSlot = 1 // an archive node gives gossip one slot from enqueue
	cfg.InitializeForkSchedule()
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.FuluVersion)
	block.Block.Slot = 1
	block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	ctrl := gomock.NewController(t)
	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetCurrentSlot().Return(uint64(2)).AnyTimes()
	clock.EXPECT().GetSlotTime(gomock.Any()).Return(time.Now().Add(-time.Hour)).AnyTimes()
	columnStorage := blob_storage_mock_services.NewMockDataColumnStorage(ctrl)
	columnStorage.EXPECT().GetSavedColumnIndex(gomock.Any(), uint64(1), common.Hash(root)).Return(nil, nil).AnyTimes()
	blobStorage := blob_storage_mock_services.NewMockBlobStorage(ctrl)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), common.Hash(root)).Return(uint32(1), nil).AnyTimes()
	forkChoice := retryRecordingForkChoice{retried: make(chan common.Hash, 1)}
	d := &peerdas{
		beaconConfig:  &cfg,
		ethClock:      clock,
		caplinConfig:  &clparams.CaplinConfig{ArchiveBlobs: true},
		columnStorage: columnStorage,
		blobStorage:   blobStorage,
		forkChoice:    forkChoice,
	}
	require.NoError(t, d.SyncColumnDataLater(block))

	go d.syncColumnDataWorker(t.Context())
	select {
	case retried := <-forkChoice.retried:
		require.Equal(t, common.Hash(root), retried)
	case <-time.After(5 * time.Second):
		t.Fatal("the envelope of an available root was not retried")
	}
	_, queued := d.blocksToCheckSync.Load(common.Hash(root))
	require.False(t, queued)
}

type peerlessSentinel struct {
	sentinelproto.SentinelClient
}

func (peerlessSentinel) GetPeers(context.Context, *sentinelproto.EmptyMessage, ...grpc.CallOption) (*sentinelproto.PeerCount, error) {
	return &sentinelproto.PeerCount{}, nil
}

// Without peers no download can run, so the worker does not scan the stored columns either.
func TestSyncColumnDataWorkerSkipsTheAvailabilityScanWithoutPeers(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.FuluForkEpoch = 0
	cfg.SecondsPerSlot = 1
	cfg.InitializeForkSchedule()
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.FuluVersion)
	block.Block.Slot = 1
	block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	ctrl := gomock.NewController(t)
	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetCurrentSlot().Return(uint64(2)).AnyTimes()
	// The RPC's column-peer refresher stays idle before Fulu.
	clock.EXPECT().GetCurrentEpoch().Return(uint64(0)).AnyTimes()
	clock.EXPECT().StateVersionByEpoch(gomock.Any()).Return(clparams.DenebVersion).AnyTimes()
	clock.EXPECT().GetSlotTime(gomock.Any()).Return(time.Now().Add(-time.Hour)).AnyTimes()
	var scans atomic.Int32
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
		rpc:           rpc.NewBeaconRpcP2P(t.Context(), peerlessSentinel{}, &cfg, clock, nil),
	}
	require.NoError(t, d.SyncColumnDataLater(block))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go d.syncColumnDataWorker(ctx)
	// The root is due after one slot; two more ticks pass without peers.
	time.Sleep(3 * time.Second)
	require.Zero(t, scans.Load())
}

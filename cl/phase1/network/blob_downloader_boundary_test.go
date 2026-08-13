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

package network

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
)

func TestBlobHistoryDownloaderProcessesFirstUnfrozenSlot(t *testing.T) {
	const firstUnfrozenSlot = uint64(100)
	wantErr := errors.New("first unfrozen slot visited")
	reader := &boundaryBlockReader{err: wantErr}
	downloader := newBoundaryDownloader(t, firstUnfrozenSlot, firstUnfrozenSlot, firstUnfrozenSlot, reader)

	require.ErrorIs(t, downloader.downloadOnce(false), wantErr)
	require.Equal(t, []uint64{firstUnfrozenSlot}, reader.slots)
}

func TestBlobHistoryDownloaderBatchStopsAtFrozenBoundary(t *testing.T) {
	const firstUnfrozenSlot = uint64(100)
	reader := &boundaryBlockReader{}
	downloader := newBoundaryDownloader(t, firstUnfrozenSlot+1, firstUnfrozenSlot, 0, reader)

	require.NoError(t, downloader.downloadOnce(false))
	require.Equal(t, []uint64{firstUnfrozenSlot + 1, firstUnfrozenSlot}, reader.slots)
}

func TestBlobHistoryDownloaderRefreshesFrozenBoundaryBetweenBatches(t *testing.T) {
	snapshot := &boundaryMutableSnapshot{}
	reader := &boundaryBlockReader{onRead: func(slot uint64) {
		if slot == 13 {
			snapshot.frozen.Store(13)
		}
	}}
	downloader := newBoundaryDownloader(t, 20, 0, 0, reader)
	downloader.sn = snapshot

	require.NoError(t, downloader.downloadOnce(false))
	require.Equal(t, []uint64{20, 19, 18, 17, 16, 15, 14, 13}, reader.slots)
}

func TestBlobHistoryDownloaderRunsWithSinglePeer(t *testing.T) {
	const slot = uint64(100)
	wantErr := errors.New("single peer admitted")
	reader := &boundaryBlockReader{err: wantErr}
	downloader := newBoundaryDownloader(t, slot, 0, slot, reader)
	downloader.rpc = boundaryPeerCounter(1)

	require.ErrorIs(t, downloader.downloadOnce(false), wantErr)
}

func TestBlobHistoryDownloaderWaitsWithoutPeers(t *testing.T) {
	reader := &boundaryBlockReader{}
	downloader := newBoundaryDownloader(t, 100, 0, 100, reader)
	downloader.rpc = boundaryPeerCounter(0)

	require.NoError(t, downloader.downloadOnce(false))
	require.Empty(t, reader.slots)
}

func TestBlobHistoryDownloaderStopsWhenPeersDisappear(t *testing.T) {
	reader := &boundaryBlockReader{}
	downloader := newBoundaryDownloader(t, 20, 0, 0, reader)
	downloader.rpc = &boundarySequencePeerCounter{counts: []uint64{1, 1, 0}}
	notified := false
	downloader.SetNotifyBlobBackfilled(func() { notified = true })

	require.NoError(t, downloader.downloadOnce(false))
	require.Equal(t, []uint64{20, 19, 18, 17, 16, 15, 14, 13}, reader.slots)
	require.False(t, notified)
	require.Zero(t, downloader.nextBackfillTargetSlot)
}

func TestBlobHistoryDownloaderUnsyncedWaitObservesCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	entered := make(chan struct{})
	downloader := newBoundaryDownloader(t, 20, 0, 0, &boundaryBlockReader{})
	downloader.ctx = ctx
	downloader.syncedChecker = boundarySyncedCheckerFunc(func() bool {
		select {
		case <-entered:
		default:
			close(entered)
		}
		return false
	})
	done := make(chan error, 1)
	go func() { done <- downloader.downloadOnce(false) }()

	<-entered
	cancel()
	require.NoError(t, <-done)
}

func TestBlobHistoryDownloaderKeepsRetryTargetAtDenebStart(t *testing.T) {
	const denebStart = uint64(100)
	downloader := newBoundaryDownloader(t, denebStart+10, 0, denebStart, &boundaryBlockReader{})

	require.NoError(t, downloader.downloadOnce(false))
	require.Equal(t, denebStart, downloader.nextBackfillTargetSlot)
}

func TestBlobHistoryDownloaderSecondCompletedPassScansOnlyRecentWindow(t *testing.T) {
	const head = uint64(1_000)
	reader := &boundaryBlockReader{}
	downloader := newBoundaryDownloader(t, head, 0, 0, reader)

	require.NoError(t, downloader.downloadOnce(false))
	reader.slots = nil
	require.NoError(t, downloader.downloadOnce(false))

	wantFloor := head - clparams.MainnetBeaconConfig.SlotsPerEpoch*2
	require.Equal(t, wantFloor, downloader.nextBackfillTargetSlot)
	require.Equal(t, head-wantFloor+1, uint64(len(reader.slots)))
	require.Equal(t, wantFloor, reader.slots[len(reader.slots)-1])
}

func newBoundaryDownloader(t *testing.T, headSlot, frozenBlobs, targetSlot uint64, reader freezeblocks.BeaconSnapshotReader) *BlobHistoryDownloader {
	t.Helper()
	downloader := &BlobHistoryDownloader{
		ctx:                    t.Context(),
		beaconCfg:              &clparams.MainnetBeaconConfig,
		rpc:                    boundaryPeerCounter(1),
		indiciesDB:             memdb.NewTestDB(t, dbcfg.ChainDB),
		blockReader:            reader,
		sn:                     boundarySnapshot(frozenBlobs),
		syncedChecker:          boundarySyncedChecker(true),
		nextBackfillTargetSlot: targetSlot,
		denebStartSlot:         targetSlot,
		archiveBlobs:           true,
		logger:                 log.New(),
	}
	downloader.headSlot.Store(headSlot)
	return downloader
}

type boundaryBlockReader struct {
	freezeblocks.BeaconSnapshotReader
	slots  []uint64
	err    error
	block  *cltypes.SignedBeaconBlock
	onRead func(uint64)
}

func (r *boundaryBlockReader) ReadBeaconBlockBodyBySlot(_ context.Context, _ kv.Tx, slot uint64) (*cltypes.SignedBeaconBlock, error) {
	r.slots = append(r.slots, slot)
	if r.onRead != nil {
		r.onRead(slot)
	}
	return r.block, r.err
}

type boundaryPeerCounter uint64

func (p boundaryPeerCounter) Peers() (uint64, error) { return uint64(p), nil }

func (p boundaryPeerCounter) SendBlobsSidecarByIdentifierReq(context.Context, *solid.ListSSZ[*cltypes.BlobIdentifier]) ([]*cltypes.BlobSidecar, string, error) {
	return nil, "", nil
}

type boundarySequencePeerCounter struct {
	counts []uint64
	calls  int
}

func (p *boundarySequencePeerCounter) Peers() (uint64, error) {
	index := min(p.calls, len(p.counts)-1)
	p.calls++
	return p.counts[index], nil
}

func (p *boundarySequencePeerCounter) SendBlobsSidecarByIdentifierReq(context.Context, *solid.ListSSZ[*cltypes.BlobIdentifier]) ([]*cltypes.BlobSidecar, string, error) {
	return nil, "", nil
}

type boundarySnapshot uint64

func (s boundarySnapshot) FrozenBlobs() uint64 { return uint64(s) }

type boundaryMutableSnapshot struct {
	frozen atomic.Uint64
}

func (s *boundaryMutableSnapshot) FrozenBlobs() uint64 { return s.frozen.Load() }

type boundarySyncedChecker bool

func (s boundarySyncedChecker) Synced() bool { return bool(s) }

type boundarySyncedCheckerFunc func() bool

func (f boundarySyncedCheckerFunc) Synced() bool { return f() }

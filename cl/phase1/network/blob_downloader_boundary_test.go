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
	"math"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/common"
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
	downloader := newBoundaryDownloader(t, firstUnfrozenSlot, firstUnfrozenSlot, firstUnfrozenSlot, 1, reader)

	require.ErrorIs(t, downloader.downloadOnce(false), wantErr)
	require.Equal(t, []uint64{firstUnfrozenSlot}, reader.slots)
}

func TestBlobHistoryDownloaderBatchStopsAtFrozenBoundary(t *testing.T) {
	const firstUnfrozenSlot = uint64(100)
	reader := &boundaryBlockReader{}
	downloader := newBoundaryDownloader(t, firstUnfrozenSlot+1, firstUnfrozenSlot, 0, 1, reader)

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
	downloader := newBoundaryDownloader(t, 20, 0, 0, 1, reader)
	downloader.sn = snapshot

	require.NoError(t, downloader.downloadOnce(false))
	require.Equal(t, []uint64{20, 19, 18, 17, 16, 15, 14, 13}, reader.slots)
}

func TestBlobHistoryDownloaderRunsWithAvailablePeer(t *testing.T) {
	const slot = uint64(100)
	wantErr := errors.New("available peer used")
	reader := &boundaryBlockReader{err: wantErr}
	downloader := newBoundaryDownloader(t, slot, 0, slot, 1, reader)

	require.ErrorIs(t, downloader.downloadOnce(false), wantErr)
}

func TestBlobHistoryDownloaderWaitsWithoutPeers(t *testing.T) {
	reader := &boundaryBlockReader{}
	downloader := newBoundaryDownloader(t, 100, 0, 100, 0, reader)

	require.NoError(t, downloader.downloadOnce(false))
	require.Empty(t, reader.slots)
}

func TestBlobHistoryDownloaderErrorsWhenCanonicalBodyIsUnavailable(t *testing.T) {
	const slot = uint64(100)
	downloader := newBoundaryDownloader(t, slot, 0, slot, 1, &boundaryBlockReader{})
	tx, err := downloader.indiciesDB.(kv.RwDB).BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, beacon_indicies.MarkRootCanonical(t.Context(), tx, slot, common.HexToHash("0x01")))
	require.NoError(t, tx.Commit())

	require.ErrorContains(t, downloader.downloadOnce(false), "canonical block body is unavailable")
}

func TestBlobHistoryDownloaderTreatsZeroCanonicalRootAsEmptySlot(t *testing.T) {
	const slot = uint64(100)
	downloader := newBoundaryDownloader(t, slot, 0, slot, 1, &boundaryBlockReader{})

	require.NoError(t, downloader.downloadOnce(false))
	require.False(t, downloader.BlobBackfillPending(slot))
}

func TestBlobHistoryDownloaderRejectsBlockWithoutMessage(t *testing.T) {
	const slot = uint64(100)
	downloader := newBoundaryDownloader(t, slot, 0, slot, 1, &boundaryBlockReader{
		block: &cltypes.SignedBeaconBlock{},
	})

	require.ErrorContains(t, downloader.downloadOnce(false), "canonical block is incomplete")
}

func TestBlobHistoryDownloaderRejectsBlockWithoutBody(t *testing.T) {
	const slot = uint64(100)
	downloader := newBoundaryDownloader(t, slot, 0, slot, 1, &boundaryBlockReader{
		block: &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: slot}},
	})

	require.ErrorContains(t, downloader.downloadOnce(false), "canonical block body is incomplete")
}

func newBoundaryDownloader(t *testing.T, headSlot, frozenBlobs, targetSlot, peers uint64, reader freezeblocks.BeaconSnapshotReader) *BlobHistoryDownloader {
	t.Helper()
	beaconCfg := clparams.MainnetBeaconConfig
	beaconCfg.DenebForkEpoch = 0
	beaconCfg.FuluForkEpoch = math.MaxUint64
	downloader := &BlobHistoryDownloader{
		ctx:           t.Context(),
		beaconCfg:     &beaconCfg,
		rpc:           boundaryPeerClient(peers),
		indiciesDB:    memdb.NewTestDB(t, dbcfg.ChainDB),
		blockReader:   reader,
		sn:            boundarySnapshot(frozenBlobs),
		syncedChecker: boundarySyncedChecker(true),
		targetSlot:    targetSlot,
		archiveBlobs:  true,
		logger:        log.New(),
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

type boundaryPeerClient uint64

func (p boundaryPeerClient) Peers() (uint64, error) { return uint64(p), nil }

func (boundaryPeerClient) SendBlobsSidecarByIdentifierReq(context.Context, *solid.ListSSZ[*cltypes.BlobIdentifier]) ([]*cltypes.BlobSidecar, string, error) {
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

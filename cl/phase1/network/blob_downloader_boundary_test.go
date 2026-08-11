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
	slots []uint64
	err   error
}

func (r *boundaryBlockReader) ReadBeaconBlockBodyBySlot(_ context.Context, _ kv.Tx, slot uint64) (*cltypes.SignedBeaconBlock, error) {
	r.slots = append(r.slots, slot)
	return nil, r.err
}

type boundaryPeerClient uint64

func (p boundaryPeerClient) Peers() (uint64, error) { return uint64(p), nil }

func (boundaryPeerClient) SendBlobsSidecarByIdentifierReq(context.Context, *solid.ListSSZ[*cltypes.BlobIdentifier]) ([]*cltypes.BlobSidecar, string, error) {
	return nil, "", nil
}

type boundarySnapshot uint64

func (s boundarySnapshot) FrozenBlobs() uint64 { return uint64(s) }

type boundarySyncedChecker bool

func (s boundarySyncedChecker) Synced() bool { return bool(s) }

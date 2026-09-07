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

package stages

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	das_mock_services "github.com/erigontech/erigon/cl/das/mock_services"
	blob_mock_services "github.com/erigontech/erigon/cl/persistence/blob_storage/mock_services"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
)

func TestFloorFor(t *testing.T) {
	tests := []struct {
		name       string
		head, keep uint64
		want       uint64
	}{
		{name: "head below keep", head: 10, keep: 11, want: 0},
		{name: "head equals keep", head: 11, keep: 11, want: 0},
		{name: "keep forever", head: 11, keep: ^uint64(0), want: 0},
		{name: "normal window", head: 100, keep: 30, want: 70},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, floorFor(test.head, test.keep))
		})
	}
}

type pruningLogHandler struct {
	records []*log.Record
}

func (h *pruningLogHandler) Log(record *log.Record) error {
	h.records = append(h.records, record)
	return nil
}

func (h *pruningLogHandler) Enabled(_ context.Context, _ log.Lvl) bool {
	return true
}

func TestCleanupAndPruningLogsPruneErrors(t *testing.T) {
	ctrl := gomock.NewController(t)
	blobStore := blob_mock_services.NewMockBlobStorage(ctrl)
	peerDas := das_mock_services.NewMockPeerDas(ctrl)
	clock := eth_clock.NewMockEthereumClock(ctrl)
	blobErr := errors.New("blob prune failed")
	columnErr := errors.New("column prune failed")

	clock.EXPECT().GetCurrentSlot().Return(uint64(200_000))
	blobStore.EXPECT().PruneBelow(uint64(71_400)).Return(blobErr)
	peerDas.EXPECT().PruneBelow(uint64(199_900)).Return(columnErr)

	handler := &pruningLogHandler{}
	logger := log.New()
	logger.SetHandler(handler)
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	cfg := &Cfg{
		indiciesDB: db,
		ethClock:   clock,
		beaconCfg:  &clparams.MainnetBeaconConfig,
		blobStore:  blobStore,
		peerDas:    peerDas,
		caplinConfig: clparams.CaplinConfig{
			ColumnKeepSlots: 100,
		},
	}

	require.NoError(t, cleanupAndPruning(t.Context(), logger, cfg, Args{}))
	require.Len(t, handler.records, 2)
	require.Equal(t, "failed to prune blob sidecars", handler.records[0].Msg)
	require.Equal(t, "failed to prune data column sidecars", handler.records[1].Msg)
	require.Contains(t, handler.records[0].Ctx, blobErr)
	require.Contains(t, handler.records[1].Ctx, columnErr)
}

func TestCleanupAndPruningKeepsEveryBlobUnderArchiveFlags(t *testing.T) {
	for _, test := range []struct {
		name   string
		caplin clparams.CaplinConfig
	}{
		{name: "blobs-archive", caplin: clparams.CaplinConfig{ArchiveBlobs: true, ColumnKeepSlots: 100}},
		{name: "blobs-no-pruning", caplin: clparams.CaplinConfig{BlobPruningDisabled: true, ColumnKeepSlots: 100}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			blobStore := blob_mock_services.NewMockBlobStorage(ctrl)
			peerDas := das_mock_services.NewMockPeerDas(ctrl)
			clock := eth_clock.NewMockEthereumClock(ctrl)

			clock.EXPECT().GetCurrentSlot().Return(uint64(200_000))
			// A zero floor is what makes PruneBelow a no-op; any other value deletes the
			// archive these flags exist to keep.
			blobStore.EXPECT().PruneBelow(uint64(0)).Return(nil)
			peerDas.EXPECT().PruneBelow(uint64(199_900)).Return(nil)

			cfg := &Cfg{
				indiciesDB:   mdbxtest.NewTestDB(t, dbcfg.ChainDB),
				ethClock:     clock,
				beaconCfg:    &clparams.MainnetBeaconConfig,
				blobStore:    blobStore,
				peerDas:      peerDas,
				caplinConfig: test.caplin,
			}

			require.NoError(t, cleanupAndPruning(t.Context(), log.New(), cfg, Args{}))
		})
	}
}

func TestCleanupAndPruningResolvesZeroColumnKeepSlotsToTheSpecWindow(t *testing.T) {
	ctrl := gomock.NewController(t)
	blobStore := blob_mock_services.NewMockBlobStorage(ctrl)
	peerDas := das_mock_services.NewMockPeerDas(ctrl)
	clock := eth_clock.NewMockEthereumClock(ctrl)

	const head = 200_000
	cfg := &Cfg{
		indiciesDB:   mdbxtest.NewTestDB(t, dbcfg.ChainDB),
		ethClock:     clock,
		beaconCfg:    &clparams.MainnetBeaconConfig,
		blobStore:    blobStore,
		peerDas:      peerDas,
		caplinConfig: clparams.CaplinConfig{},
	}
	// Zero means the spec window, not "keep nothing"; the standalone cmd/caplin binary
	// leaves the field unset.
	specWindow := cfg.beaconCfg.MinEpochsForDataColumnSidecarsRequests * cfg.beaconCfg.SlotsPerEpoch

	clock.EXPECT().GetCurrentSlot().Return(uint64(head))
	blobStore.EXPECT().PruneBelow(uint64(head - 128600)).Return(nil)
	peerDas.EXPECT().PruneBelow(head - specWindow).Return(nil)

	require.NoError(t, cleanupAndPruning(t.Context(), log.New(), cfg, Args{}))
}

// The serving window is epoch-based: the earliest required column starts at the first slot
// of current_epoch - MIN_EPOCHS, so subtracting a slot count from a head that sits inside
// an epoch cuts above the boundary and deletes data the node must still serve.
func TestCleanupAndPruningColumnFloorLandsOnTheEpochBoundary(t *testing.T) {
	ctrl := gomock.NewController(t)
	blobStore := blob_mock_services.NewMockBlobStorage(ctrl)
	peerDas := das_mock_services.NewMockPeerDas(ctrl)
	clock := eth_clock.NewMockEthereumClock(ctrl)

	beaconCfg := clparams.MainnetBeaconConfig
	beaconCfg.SlotsPerEpoch = 12
	beaconCfg.MinEpochsForDataColumnSidecarsRequests = 4096

	// 59159 is 11 slots into epoch 4929, so the slot-distance floor overshoots by 11.
	const head = 59_159
	const wantFloor = 9_996 // (4929 - 4096) * 12

	cfg := &Cfg{
		indiciesDB:   mdbxtest.NewTestDB(t, dbcfg.ChainDB),
		ethClock:     clock,
		beaconCfg:    &beaconCfg,
		blobStore:    blobStore,
		peerDas:      peerDas,
		caplinConfig: clparams.CaplinConfig{},
	}

	clock.EXPECT().GetCurrentSlot().Return(uint64(head))
	blobStore.EXPECT().PruneBelow(uint64(0)).Return(nil)
	peerDas.EXPECT().PruneBelow(uint64(wantFloor)).Return(nil)

	require.NoError(t, cleanupAndPruning(t.Context(), log.New(), cfg, Args{}))
}

// An explicit --caplin.columns-keep-slots is a slot count, not an epoch window, so it must
// keep cutting at that exact distance from the head.
func TestCleanupAndPruningKeepsExplicitColumnSlotsUnaligned(t *testing.T) {
	ctrl := gomock.NewController(t)
	blobStore := blob_mock_services.NewMockBlobStorage(ctrl)
	peerDas := das_mock_services.NewMockPeerDas(ctrl)
	clock := eth_clock.NewMockEthereumClock(ctrl)

	beaconCfg := clparams.MainnetBeaconConfig
	beaconCfg.SlotsPerEpoch = 12

	const head = 59_159
	cfg := &Cfg{
		indiciesDB:   mdbxtest.NewTestDB(t, dbcfg.ChainDB),
		ethClock:     clock,
		beaconCfg:    &beaconCfg,
		blobStore:    blobStore,
		peerDas:      peerDas,
		caplinConfig: clparams.CaplinConfig{ColumnKeepSlots: 1_000},
	}

	clock.EXPECT().GetCurrentSlot().Return(uint64(head))
	blobStore.EXPECT().PruneBelow(uint64(0)).Return(nil)
	peerDas.EXPECT().PruneBelow(uint64(head - 1_000)).Return(nil)

	require.NoError(t, cleanupAndPruning(t.Context(), log.New(), cfg, Args{}))
}

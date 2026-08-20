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

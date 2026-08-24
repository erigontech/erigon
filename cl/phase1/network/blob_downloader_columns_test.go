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
	"testing"
	"time"

	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/das/mock_services"
	"github.com/erigontech/erigon/common/log/v3"
)

// neverRecovers returns a PeerDas whose column download only ends when its per-attempt
// context expires, so the time recoverFuluColumns spends is the timeout it chose.
func neverRecovers(t *testing.T) *mock_services.MockPeerDas {
	t.Helper()
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	peerDas := mock_services.NewMockPeerDas(ctrl)
	peerDas.EXPECT().
		DownloadColumnsAndRecoverBlobs(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ []cltypes.ColumnSyncableSignedBlock) error {
			<-ctx.Done()
			return ctx.Err()
		}).
		AnyTimes()
	return peerDas
}

func columnDownloader(t *testing.T, headSlot uint64) *BlobHistoryDownloader {
	t.Helper()
	cfg := clparams.MainnetBeaconConfig
	b := &BlobHistoryDownloader{
		ctx:                              context.Background(),
		beaconCfg:                        &cfg,
		peerDasGetter:                    staticPeerDasGetter{pd: neverRecovers(t)},
		columnBackfillTimeout:            2 * time.Second,
		columnBackfillOutOfWindowTimeout: 50 * time.Millisecond,
		logger:                           log.New(),
	}
	b.headSlot.Store(headSlot)
	return b
}

func fuluBlockAt(slot uint64) *cltypes.SignedBeaconBlock {
	blk := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.FuluVersion)
	blk.Block.Slot = slot
	return blk
}

// Peers are only required to serve data columns inside
// MIN_EPOCHS_FOR_DATA_COLUMN_SIDECARS_REQUESTS. Spending the full per-block timeout on
// older blocks costs an archive backfill days for attempts that cannot succeed.
func TestFuluColumnRecoveryUsesShortTimeoutOutsideTheWindow(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	window := cfg.MinSlotsForDataColumnSidecarsRequest()
	head := window + 100_000

	b := columnDownloader(t, head)

	start := time.Now()
	b.recoverFuluColumns([]*cltypes.SignedBeaconBlock{fuluBlockAt(head - window - 1)})
	elapsed := time.Since(start)

	if elapsed > time.Second {
		t.Fatalf("out-of-window block used the full timeout: took %s", elapsed)
	}
}

// A block still inside the window must keep the full timeout: those columns are
// genuinely fetchable and a short deadline would abandon recoverable blobs.
func TestFuluColumnRecoveryKeepsFullTimeoutInsideTheWindow(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	window := cfg.MinSlotsForDataColumnSidecarsRequest()
	head := window + 100_000

	b := columnDownloader(t, head)
	b.columnBackfillTimeout = 300 * time.Millisecond

	start := time.Now()
	b.recoverFuluColumns([]*cltypes.SignedBeaconBlock{fuluBlockAt(head - 10)})
	elapsed := time.Since(start)

	if elapsed < 250*time.Millisecond {
		t.Fatalf("in-window block was cut short: took %s", elapsed)
	}
}

func TestMinSlotsForDataColumnSidecarsRequest(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	want := cfg.MinEpochsForDataColumnSidecarsRequests * cfg.SlotsPerEpoch
	if got := cfg.MinSlotsForDataColumnSidecarsRequest(); got != want {
		t.Fatalf("got %d want %d", got, want)
	}
}

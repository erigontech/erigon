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

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/blob_storage/mock_services"
	"github.com/erigontech/erigon/common/log/v3"
)

// blockWithCommitments builds a Deneb block at slot carrying n kzg commitments.
func blockWithCommitments(t *testing.T, slot uint64, n int) *cltypes.SignedBeaconBlock {
	t.Helper()
	blk := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	blk.Block.Slot = slot
	for range n {
		blk.Block.Body.BlobKzgCommitments.Append(&cltypes.KZGCommitment{})
	}
	return blk
}

func gapDownloader(t *testing.T, store *mock_services.MockBlobStorage) *BlobHistoryDownloader {
	t.Helper()
	cfg := clparams.MainnetBeaconConfig
	return &BlobHistoryDownloader{
		ctx:         context.Background(),
		beaconCfg:   &cfg,
		blobStorage: store,
		logger:      log.New(),
	}
}

// A block whose sidecars are still absent after the fetch attempt is a gap the pass has
// to report: declaring the backfill finished while it stands is what lets the antiquary
// walk into a range it cannot dump.
func TestIncompleteAfterAttemptReportsStillMissingSlots(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	store := mock_services.NewMockBlobStorage(ctrl)
	store.EXPECT().
		KzgCommitmentsCount(gomock.Any(), gomock.Any()).
		Return(uint32(0), nil).
		AnyTimes()

	b := gapDownloader(t, store)

	got, err := b.incompleteAfterAttempt([]*cltypes.SignedBeaconBlock{
		blockWithCommitments(t, 100, 1),
		blockWithCommitments(t, 200, 2),
	})

	require.NoError(t, err)
	require.Equal(t, []uint64{100, 200}, got)
}

// A block the attempt did complete must not be reported, or every pass would claim gaps
// it had just filled.
func TestIncompleteAfterAttemptIgnoresRecoveredSlots(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	store := mock_services.NewMockBlobStorage(ctrl)
	store.EXPECT().
		KzgCommitmentsCount(gomock.Any(), gomock.Any()).
		Return(uint32(1), nil).
		AnyTimes()

	b := gapDownloader(t, store)

	got, err := b.incompleteAfterAttempt([]*cltypes.SignedBeaconBlock{
		blockWithCommitments(t, 100, 1),
	})

	require.NoError(t, err)
	require.Empty(t, got)
}

// Partial recovery is still a gap: a block wanting two sidecars with one stored cannot
// be dumped.
func TestIncompleteAfterAttemptCountsPartialRecoveryAsAGap(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	store := mock_services.NewMockBlobStorage(ctrl)
	store.EXPECT().
		KzgCommitmentsCount(gomock.Any(), gomock.Any()).
		Return(uint32(1), nil).
		AnyTimes()

	b := gapDownloader(t, store)

	got, err := b.incompleteAfterAttempt([]*cltypes.SignedBeaconBlock{
		blockWithCommitments(t, 300, 2),
	})

	require.NoError(t, err)
	require.Equal(t, []uint64{300}, got)
}

func TestBlobGapsAccumulateAcrossBatchesAndReportRange(t *testing.T) {
	b := &BlobHistoryDownloader{logger: log.New()}

	b.recordBlobGaps([]uint64{500, 100})
	b.recordBlobGaps([]uint64{300})

	count, lowest, highest := b.blobGapSummary()
	require.Equal(t, 3, count)
	require.Equal(t, uint64(100), lowest)
	require.Equal(t, uint64(500), highest)
}

// Each pass rebuilds the set from what it actually observed, so a slot repaired out of
// band does not linger in the summary forever.
func TestBlobGapsResetAtTheStartOfAPass(t *testing.T) {
	b := &BlobHistoryDownloader{logger: log.New()}
	b.recordBlobGaps([]uint64{100, 200})

	b.resetBlobGaps()

	count, _, _ := b.blobGapSummary()
	require.Zero(t, count)
}

func TestBlobGapSlotsAreSortedForStableReporting(t *testing.T) {
	b := &BlobHistoryDownloader{logger: log.New()}
	b.recordBlobGaps([]uint64{300, 100, 200})

	require.Equal(t, []uint64{100, 200, 300}, b.BlobGapSlots())
}

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

// Gaps must be discoverable without the column walk. That walk exists to fetch columns
// no peer will serve, so on an archive node it crawls — measured at ~1.6 slots/sec across
// the 345k slots between the frozen frontier and head, i.e. days before it has even seen
// every gap. Reading the store answers the same question in seconds.
func TestScanRangeForGapsReportsEveryIncompleteSlot(t *testing.T) {
	want := map[uint64]int{100: 1, 101: 0, 102: 2, 103: 0}
	have := map[uint64]int{100: 0, 101: 0, 102: 1, 103: 0}

	got := scanRangeForGaps(100, 104, func(slot uint64) (stored, commitments int, err error) {
		return have[slot], want[slot], nil
	})

	require.Equal(t, []uint64{100, 102}, got)
}

// A slot the scan cannot read is skipped rather than reported: claiming a gap it could
// not observe would send the drain fetching sidecars that may already be present.
func TestScanRangeForGapsSkipsSlotsItCannotRead(t *testing.T) {
	got := scanRangeForGaps(200, 203, func(slot uint64) (int, int, error) {
		if slot == 201 {
			return 0, 0, errors.New("unreadable")
		}
		return 0, 1, nil
	})

	require.Equal(t, []uint64{200, 202}, got)
}

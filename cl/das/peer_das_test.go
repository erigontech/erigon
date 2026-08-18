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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	storagemock "github.com/erigontech/erigon/cl/persistence/blob_storage/mock_services"
	"github.com/erigontech/erigon/cl/sentinel/httpreqresp"
	"github.com/erigontech/erigon/common"
)

func fuluAtEpochTwoConfig() clparams.BeaconChainConfig {
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 2
	cfg.InitializeForkSchedule()
	return cfg
}

func TestSyncColumnDataLaterStoresCompactFuluBlock(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	d := &peerdas{}
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.FuluVersion)
	block.Block.Body.BlobKzgCommitments.Append(&cltypes.KZGCommitment{})
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)

	require.NoError(t, d.SyncColumnDataLater(block))

	value, ok := d.blocksToCheckSync[common.Hash(blockRoot)]
	require.True(t, ok)
	queued, compact := value.block.(*deferredColumnSyncBlock)
	require.True(t, compact)
	require.Equal(t, block.Block.Slot, queued.slot)
	require.Equal(t, common.Hash(blockRoot), queued.root)
	require.Equal(t, block.Block.Body.BlobKzgCommitments.Len(), queued.commitments.Len())
}

func TestSyncColumnDataLaterBoundsQueue(t *testing.T) {
	const queueLimit = maxDeferredColumnSyncBlocks
	cfg := clparams.MainnetBeaconConfig
	d := &peerdas{}
	for slot := uint64(1); slot <= queueLimit+1; slot++ {
		block := cltypes.NewSignedBeaconBlock(&cfg, clparams.FuluVersion)
		block.Block.Slot = slot
		block.Block.Body.BlobKzgCommitments.Append(&cltypes.KZGCommitment{})
		require.NoError(t, d.SyncColumnDataLater(block))
	}

	require.LessOrEqual(t, len(d.blocksToCheckSync), queueLimit)
}

func TestSyncColumnDataLaterUsesConfiguredFork(t *testing.T) {
	cfg := fuluAtEpochTwoConfig()

	tests := []struct {
		name           string
		blockSlot      uint64
		decodedVersion clparams.StateVersion
		wantQueued     bool
	}{
		{name: "Fulu slot decoded as Electra", blockSlot: 2 * cfg.SlotsPerEpoch, decodedVersion: clparams.ElectraVersion, wantQueued: true},
		{name: "Electra slot decoded as Fulu", blockSlot: cfg.SlotsPerEpoch, decodedVersion: clparams.FuluVersion},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &peerdas{beaconConfig: &cfg}
			block := cltypes.NewSignedBeaconBlock(&cfg, tt.decodedVersion)
			block.Block.Slot = tt.blockSlot
			block.Block.Body.BlobKzgCommitments.Append(&cltypes.KZGCommitment{})
			blockRoot, err := block.Block.HashSSZ()
			require.NoError(t, err)

			require.NoError(t, d.SyncColumnDataLater(block))
			job, queued := d.blocksToCheckSync[common.Hash(blockRoot)]
			require.Equal(t, tt.wantQueued, queued)
			if tt.wantQueued {
				require.Equal(t, clparams.FuluVersion, job.block.Version())
			}
		})
	}
}

func TestInitializeDownloadRequestUsesConfiguredFork(t *testing.T) {
	cfg := fuluAtEpochTwoConfig()

	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.ElectraVersion)
	block.Block.Slot = cfg.FuluForkEpoch * cfg.SlotsPerEpoch
	block.Block.Body.BlobKzgCommitments.Append(&cltypes.KZGCommitment{})
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)

	storage := storagemock.NewMockDataColumnStorage(gomock.NewController(t))
	storage.EXPECT().GetSavedColumnIndex(gomock.Any(), block.Block.Slot, common.Hash(blockRoot)).Return(nil, nil)
	req, err := initializeDownloadRequest(
		[]cltypes.ColumnSyncableSignedBlock{block},
		&cfg,
		storage,
		map[cltypes.CustodyIndex]bool{0: true},
	)
	require.NoError(t, err)
	require.Equal(t, 1, req.remainingEntriesCount())
}

func TestDownloadOnlyCustodyColumnsWithoutRPC(t *testing.T) {
	d := &peerdas{}
	require.Error(t, d.DownloadOnlyCustodyColumns(context.Background(), nil))
}

func TestDownloadColumnsAndRecoverBlobsWithoutRPC(t *testing.T) {
	d := &peerdas{}
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.FuluVersion)
	block.Block.Body.BlobKzgCommitments.Append(&cltypes.KZGCommitment{})
	require.Error(t, d.DownloadColumnsAndRecoverBlobs(context.Background(), []cltypes.ColumnSyncableSignedBlock{block}))
}

func TestSyncDeferredColumnDataPrunesExpiredJobsWithoutRPC(t *testing.T) {
	now := time.Now()
	expiredRoot := common.Hash{1}
	activeRoot := common.Hash{2}
	d := &peerdas{blocksToCheckSync: map[common.Hash]deferredColumnSyncJob{
		expiredRoot: {block: &deferredColumnSyncBlock{}, addedAt: now.Add(-deferredColumnSyncTTL - time.Second)},
		activeRoot:  {block: &deferredColumnSyncBlock{}, addedAt: now},
	}}

	d.syncDeferredColumnData(t.Context())

	jobs := d.deferredColumnSyncJobs()
	require.NotContains(t, jobs, expiredRoot)
	require.Contains(t, jobs, activeRoot)
}

// initTestBeaconConfig installs cfg as the global config if no test has done so
// yet. InitGlobalStaticConfig panics on a second call, so tests in this package
// must agree on every global-only field; they may differ only in fork epochs,
// which each test reads from its own local config.
func initTestBeaconConfig(cfg *clparams.BeaconChainConfig) {
	if clparams.GetBeaconConfig() == nil {
		clparams.InitGlobalStaticConfig(cfg, &clparams.CaplinConfig{})
	}
}

func TestIsExpectedColumnDownloadMiss(t *testing.T) {
	require.False(t, isExpectedColumnDownloadMiss(nil))
	require.True(t, isExpectedColumnDownloadMiss(&httpreqresp.PeerResponseError{
		Code: httpreqresp.ResponseCodeResourceUnavailable,
	}))
	require.True(t, isExpectedColumnDownloadMiss(fmt.Errorf("column miss: %w", &httpreqresp.PeerResponseError{
		Code: httpreqresp.ResponseCodeResourceUnavailable,
	})))
	require.False(t, isExpectedColumnDownloadMiss(&httpreqresp.PeerResponseError{
		Code:    httpreqresp.ResponseCodeServerError,
		Message: "broken",
	}))
	require.False(t, isExpectedColumnDownloadMiss(&httpreqresp.HTTPError{
		StatusCode: 400,
		Body:       "Read Code: EOF",
	}))
	require.False(t, isExpectedColumnDownloadMiss(errors.New("peer error code: 2 (server error). Error message: broken")))
}

func TestResolveColumnSidecarSlotAndRoot(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 1
	cfg.GloasForkEpoch = 2
	cfg.InitializeForkSchedule()
	initTestBeaconConfig(&cfg)
	d := &peerdas{beaconConfig: &cfg}
	spe := cfg.SlotsPerEpoch

	t.Run("rejects Gloas schema carrying a pre-Gloas slot", func(t *testing.T) {
		// A peer selects the Gloas decode schema (no SignedBlockHeader) via the
		// response fork-digest, then claims slot 0, which maps to a pre-Gloas
		// fork. The pre-Gloas branch must not dereference the absent header.
		sidecar := cltypes.NewDataColumnSidecarWithVersion(clparams.GloasVersion)
		sidecar.Slot = 0
		require.Nil(t, sidecar.SignedBlockHeader)
		_, _, ok := d.resolveColumnSidecarSlotAndRoot(sidecar)
		require.False(t, ok)
	})

	t.Run("rejects pre-Gloas schema with nil signed block header", func(t *testing.T) {
		sidecar := cltypes.NewDataColumnSidecarWithVersion(clparams.FuluVersion)
		sidecar.SignedBlockHeader = nil
		_, _, ok := d.resolveColumnSidecarSlotAndRoot(sidecar)
		require.False(t, ok)
	})

	t.Run("accepts a consistent Fulu sidecar", func(t *testing.T) {
		sidecar := cltypes.NewDataColumnSidecarWithVersion(clparams.FuluVersion)
		require.NotNil(t, sidecar.SignedBlockHeader)
		sidecar.SignedBlockHeader.Header.Slot = spe // epoch 1 => Fulu
		slot, blockRoot, ok := d.resolveColumnSidecarSlotAndRoot(sidecar)
		require.True(t, ok)
		require.Equal(t, spe, slot)
		want, err := sidecar.SignedBlockHeader.Header.HashSSZ()
		require.NoError(t, err)
		require.Equal(t, common.Hash(want), blockRoot)
	})

	t.Run("accepts a consistent Gloas sidecar", func(t *testing.T) {
		sidecar := cltypes.NewDataColumnSidecarWithVersion(clparams.GloasVersion)
		sidecar.Slot = 2 * spe // epoch 2 => Gloas
		sidecar.BeaconBlockRoot = common.HexToHash("0xabc")
		slot, blockRoot, ok := d.resolveColumnSidecarSlotAndRoot(sidecar)
		require.True(t, ok)
		require.Equal(t, 2*spe, slot)
		require.Equal(t, common.HexToHash("0xabc"), blockRoot)
	})
}

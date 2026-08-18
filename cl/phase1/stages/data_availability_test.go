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
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	dasmock "github.com/erigontech/erigon/cl/das/mock_services"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
)

func TestAcquireBlockDataAvailability(t *testing.T) {
	tests := []struct {
		name          string
		afterDownload bool
		wantErr       error
	}{
		{name: "available after download", afterDownload: true},
		{name: "still unavailable", wantErr: forkchoice.ErrEIP7594ColumnDataNotAvailable},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.FuluVersion)
			block.Block.Slot = 1
			block.Block.Body.BlobKzgCommitments.Append(&cltypes.KZGCommitment{})
			blockRoot, err := block.Block.HashSSZ()
			require.NoError(t, err)

			ctrl := gomock.NewController(t)
			peerDas := dasmock.NewMockPeerDas(ctrl)
			peerDas.EXPECT().IsDataAvailable(block.Block.Slot, common.Hash(blockRoot)).Return(false, nil)
			peerDas.EXPECT().IsArchivedMode().Return(false)
			peerDas.EXPECT().DownloadOnlyCustodyColumns(gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, blocks []cltypes.ColumnSyncableSignedBlock) error {
					require.Equal(t, []cltypes.ColumnSyncableSignedBlock{block}, blocks)
					return nil
				},
			)
			peerDas.EXPECT().IsDataAvailable(block.Block.Slot, common.Hash(blockRoot)).Return(tt.afterDownload, nil)

			err = acquireBlockDataAvailability(t.Context(), peerDas, block)
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestAcquireBlockDataAvailabilityInArchiveMode(t *testing.T) {
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.FuluVersion)
	block.Block.Slot = 1
	block.Block.Body.BlobKzgCommitments.Append(&cltypes.KZGCommitment{})
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	peerDas := dasmock.NewMockPeerDas(ctrl)
	peerDas.EXPECT().IsDataAvailable(block.Block.Slot, common.Hash(blockRoot)).Return(false, nil)
	peerDas.EXPECT().IsArchivedMode().Return(true)
	peerDas.EXPECT().DownloadColumnsAndRecoverBlobs(gomock.Any(), []cltypes.ColumnSyncableSignedBlock{block}).Return(nil)
	peerDas.EXPECT().IsDataAvailable(block.Block.Slot, common.Hash(blockRoot)).Return(true, nil)

	require.NoError(t, acquireBlockDataAvailability(t.Context(), peerDas, block))
}

func TestAcquireRecentBlocksDataAvailabilityBatchesDownloads(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.InitializeForkSchedule()

	blocks := make([]*cltypes.SignedBeaconBlock, 2)
	roots := make([]common.Hash, len(blocks))
	for i := range blocks {
		block := cltypes.NewSignedBeaconBlock(&cfg, clparams.FuluVersion)
		block.Block.Slot = uint64(i + 1)
		block.Block.Body.BlobKzgCommitments.Append(&cltypes.KZGCommitment{})
		root, err := block.Block.HashSSZ()
		require.NoError(t, err)
		blocks[i] = block
		roots[i] = common.Hash(root)
	}

	ctrl := gomock.NewController(t)
	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetCurrentSlot().Return(blocks[len(blocks)-1].Block.Slot).AnyTimes()
	peerDas := dasmock.NewMockPeerDas(ctrl)
	for i, block := range blocks {
		peerDas.EXPECT().IsDataAvailable(block.Block.Slot, roots[i]).Return(false, nil)
	}
	peerDas.EXPECT().IsArchivedMode().Return(false)
	peerDas.EXPECT().DownloadOnlyCustodyColumns(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, downloaded []cltypes.ColumnSyncableSignedBlock) error {
			require.Len(t, downloaded, len(blocks))
			for i, block := range blocks {
				require.Same(t, block, downloaded[i])
			}
			return nil
		},
	)
	for i, block := range blocks {
		peerDas.EXPECT().IsDataAvailable(block.Block.Slot, roots[i]).Return(true, nil)
	}

	errs := acquireRecentBlocksDataAvailability(t.Context(), &Cfg{
		beaconCfg: &cfg,
		ethClock:  clock,
		peerDas:   peerDas,
	}, blocks)
	require.Len(t, errs, len(blocks))
	for _, err := range errs {
		require.NoError(t, err)
	}
}

func TestRequiresRecentBlockDataAvailabilityUsesConfiguredFork(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 2
	cfg.InitializeForkSchedule()

	tests := []struct {
		name           string
		blockSlot      uint64
		decodedVersion clparams.StateVersion
		want           bool
	}{
		{name: "Fulu slot decoded as Electra", blockSlot: 2 * cfg.SlotsPerEpoch, decodedVersion: clparams.ElectraVersion, want: true},
		{name: "Electra slot decoded as Fulu", blockSlot: cfg.SlotsPerEpoch, decodedVersion: clparams.FuluVersion},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
			clock.EXPECT().GetCurrentSlot().Return(tt.blockSlot)
			block := cltypes.NewSignedBeaconBlock(&cfg, tt.decodedVersion)
			block.Block.Slot = tt.blockSlot

			require.Equal(t, tt.want, requiresRecentBlockDataAvailability(&Cfg{
				beaconCfg: &cfg,
				ethClock:  clock,
			}, block))
		})
	}
}

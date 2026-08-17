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

package forkchoice

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	dasmock "github.com/erigontech/erigon/cl/das/mock_services"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/common"
)

func TestOnBlockChecksDataAvailabilityWithoutNewPayload(t *testing.T) {
	store := buildExAnteStore(t)
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 1
	cfg.InitializeForkSchedule()
	require.Equal(t, clparams.FuluVersion, cfg.GetCurrentStateVersion(cfg.FuluForkEpoch))
	store.beaconCfg = &cfg

	parentRoot, _, err := store.GetHead(nil)
	require.NoError(t, err)
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.FuluVersion)
	block.Block.Slot = cfg.SlotsPerEpoch
	block.Block.ParentRoot = parentRoot
	block.Block.Body.BlobKzgCommitments.Append(&cltypes.KZGCommitment{})
	store.OnTick(store.genesisTime + block.Block.Slot*cfg.SecondsPerSlot)
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	store.engine = execution_client.NewMockExecutionEngine(ctrl)

	peerDas := dasmock.NewMockPeerDas(ctrl)
	peerDas.EXPECT().IsArchivedMode().Return(false)
	peerDas.EXPECT().IsDataAvailable(block.Block.Slot, common.Hash(blockRoot)).Return(false, nil)
	peerDas.EXPECT().SyncColumnDataLater(block).Return(nil)
	store.InitPeerDas(peerDas)

	err = store.OnBlock(context.Background(), block, false, false, true)
	require.ErrorIs(t, err, ErrEIP7594ColumnDataNotAvailable)
}

func TestHasNonEmptyBlobsAndProofsForAllCommitments(t *testing.T) {
	tests := []struct {
		name          string
		blobs         [][]byte
		proofs        [][][]byte
		expectedCount int
		want          bool
	}{
		{
			name:          "missing blob",
			blobs:         make([][]byte, 1),
			proofs:        [][][]byte{{{1}}},
			expectedCount: 1,
		},
		{
			name:          "missing proofs",
			blobs:         [][]byte{{1}},
			proofs:        make([][][]byte, 1),
			expectedCount: 1,
		},
		{
			name:          "empty proof",
			blobs:         [][]byte{{1}},
			proofs:        [][][]byte{{nil}},
			expectedCount: 1,
		},
		{
			name:          "unexpected count",
			blobs:         [][]byte{{1}},
			proofs:        [][][]byte{{{2}}},
			expectedCount: 2,
		},
		{
			name:          "non-empty entries",
			blobs:         [][]byte{{1}, {2}},
			proofs:        [][][]byte{{{3}}, {{4}}},
			expectedCount: 2,
			want:          true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, hasNonEmptyBlobsAndProofsForAllCommitments(tt.blobs, tt.proofs, tt.expectedCount))
		})
	}
}

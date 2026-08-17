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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	dasmock "github.com/erigontech/erigon/cl/das/mock_services"
	"github.com/erigontech/erigon/common"
)

func TestOnBlockRequiresRecentFuluDataAvailability(t *testing.T) {
	for _, checkDataAvailability := range []bool{false, true} {
		t.Run(fmt.Sprintf("caller_check_%t", checkDataAvailability), func(t *testing.T) {
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
			peerDas := dasmock.NewMockPeerDas(ctrl)
			peerDas.EXPECT().IsDataAvailable(block.Block.Slot, common.Hash(blockRoot)).Return(false, nil)
			peerDas.EXPECT().SyncColumnDataLater(block).Return(nil)
			store.InitPeerDas(peerDas)

			err = store.OnBlock(context.Background(), block, false, false, checkDataAvailability)
			require.ErrorIs(t, err, ErrEIP7594ColumnDataNotAvailable)
		})
	}
}

func TestHasCompleteBlobDataForAllCommitments(t *testing.T) {
	blob := make([]byte, cltypes.BYTES_PER_BLOB)
	proof := make([]byte, cltypes.BYTES_KZG_PROOF)
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
			proofs:        [][][]byte{{proof}},
			expectedCount: 1,
		},
		{
			name:          "missing proofs",
			blobs:         [][]byte{blob},
			proofs:        make([][][]byte, 1),
			expectedCount: 1,
		},
		{
			name:          "truncated blob",
			blobs:         [][]byte{{1}},
			proofs:        [][][]byte{{proof}},
			expectedCount: 1,
		},
		{
			name:          "truncated proof",
			blobs:         [][]byte{blob},
			proofs:        [][][]byte{{{1}}},
			expectedCount: 1,
		},
		{
			name:          "multiple proofs",
			blobs:         [][]byte{blob},
			proofs:        [][][]byte{{proof, proof}},
			expectedCount: 1,
		},
		{
			name:          "unexpected count",
			blobs:         [][]byte{blob},
			proofs:        [][][]byte{{proof}},
			expectedCount: 2,
		},
		{
			name:          "complete entries",
			blobs:         [][]byte{blob, blob},
			proofs:        [][][]byte{{proof}, {proof}},
			expectedCount: 2,
			want:          true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, hasCompleteBlobDataForAllCommitments(tt.blobs, tt.proofs, tt.expectedCount))
		})
	}
}

func TestKzgCommitmentsToVersionedHashesPreservesEmptyList(t *testing.T) {
	commitments := solid.NewStaticListSSZ[*cltypes.KZGCommitment](1, 48)
	require.NotNil(t, kzgCommitmentsToVersionedHashes(commitments))
	require.Empty(t, kzgCommitmentsToVersionedHashes(commitments))
}

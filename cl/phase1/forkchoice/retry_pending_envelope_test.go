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
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/cltypes"
	das_mock "github.com/erigontech/erigon/cl/das/mock_services"
	"github.com/erigontech/erigon/common"
	"github.com/hashicorp/golang-lru/v2"
)

type retryPendingForkGraph struct {
	payloadVoteForkGraph
	block *cltypes.SignedBeaconBlock
}

func (g retryPendingForkGraph) GetBlock(common.Hash) (*cltypes.SignedBeaconBlock, bool) {
	return g.block, g.block != nil
}

func newRetryPendingStore(t *testing.T, peerDas *das_mock.MockPeerDas) (*ForkChoiceStore, *lru.Cache[common.Hash, *cltypes.SignedExecutionPayloadEnvelope]) {
	t.Helper()
	pending, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](2)
	require.NoError(t, err)
	local, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](2)
	require.NoError(t, err)
	block := &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 7}}
	f := &ForkChoiceStore{
		forkGraph:                      retryPendingForkGraph{block: block},
		pendingEnvelopes:               pending,
		pendingLocalSelfBuildEnvelopes: local,
		peerDas:                        peerDas,
	}
	return f, pending
}

func TestRetryPendingExecutionPayloadEnvelopeIgnoresRootsWithoutPendingEnvelope(t *testing.T) {
	peerDas := das_mock.NewMockPeerDas(gomock.NewController(t))
	f, _ := newRetryPendingStore(t, peerDas)

	f.RetryPendingExecutionPayloadEnvelope(t.Context(), common.HexToHash("0x1"))
}

func TestRetryPendingExecutionPayloadEnvelopeWaitsForColumnData(t *testing.T) {
	root := common.HexToHash("0x1")
	peerDas := das_mock.NewMockPeerDas(gomock.NewController(t))
	peerDas.EXPECT().IsDataAvailable(uint64(7), root).Return(false, nil)
	f, pending := newRetryPendingStore(t, peerDas)
	pending.Add(root, &cltypes.SignedExecutionPayloadEnvelope{})

	f.RetryPendingExecutionPayloadEnvelope(t.Context(), root)

	require.True(t, pending.Contains(root))
}

func TestRetryPendingExecutionPayloadEnvelopeAppliesOnceColumnDataIsAvailable(t *testing.T) {
	root := common.HexToHash("0x1")
	peerDas := das_mock.NewMockPeerDas(gomock.NewController(t))
	peerDas.EXPECT().IsDataAvailable(uint64(7), root).Return(true, nil)
	f, pending := newRetryPendingStore(t, peerDas)
	pending.Add(root, &cltypes.SignedExecutionPayloadEnvelope{})

	f.RetryPendingExecutionPayloadEnvelope(t.Context(), root)

	require.False(t, pending.Contains(root))
}

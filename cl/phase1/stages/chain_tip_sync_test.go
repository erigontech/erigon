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
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	dasmock "github.com/erigontech/erigon/cl/das/mock_services"
	"github.com/erigontech/erigon/cl/sentinel/peers"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

func TestSendFetchErrorReturnsAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	done := make(chan struct{})
	go func() {
		sendFetchError(ctx, make(chan error), errors.New("fetch failed"))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("error sender blocked after cancellation")
	}
}

func TestChainTipBlockResponseQueueHoldsOneBatch(t *testing.T) {
	responses := newChainTipBlockResponseChannel()
	first := &peers.PeeredObject[[]*cltypes.SignedBeaconBlock]{}
	second := &peers.PeeredObject[[]*cltypes.SignedBeaconBlock]{}

	select {
	case responses <- first:
	default:
		t.Fatal("first block batch was not buffered")
	}
	select {
	case responses <- second:
		t.Fatal("more than one block batch was buffered")
	default:
	}
}

func TestChainTipSyncChecksFuluDataAvailability(t *testing.T) {
	stageCfg, block, blockRoot := newSignedFuluForwardSyncFixture(t)

	ctrl := gomock.NewController(t)
	peerDas := dasmock.NewMockPeerDas(ctrl)
	peerDas.EXPECT().IsDataAvailable(block.Block.Slot, blockRoot).Return(false, nil)
	peerDas.EXPECT().IsArchivedMode().Return(false)
	peerDas.EXPECT().DownloadOnlyCustodyColumns(gomock.Any(), gomock.Any()).Return(nil)
	peerDas.EXPECT().IsDataAvailable(block.Block.Slot, blockRoot).Return(false, nil)
	stageCfg.peerDas = peerDas
	stageCfg.forkChoice.InitPeerDas(peerDas)

	respCh := make(chan *peers.PeeredObject[[]*cltypes.SignedBeaconBlock], 1)
	respCh <- &peers.PeeredObject[[]*cltypes.SignedBeaconBlock]{Data: []*cltypes.SignedBeaconBlock{block}}
	errCh := make(chan error)
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	err := listenToIncomingBlocksUntilANewBlockIsReceived(ctx, log.Root(), stageCfg, Args{targetSlot: block.Block.Slot}, respCh, errCh)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	_, imported := stageCfg.forkChoice.GetHeader(blockRoot)
	require.False(t, imported)
}

func TestChainTipSyncValidatesBlockBeforeAcquiringColumns(t *testing.T) {
	stageCfg, block, _ := newSignedFuluForwardSyncFixture(t)
	block.Signature = common.Bytes96{}

	respCh := make(chan *peers.PeeredObject[[]*cltypes.SignedBeaconBlock], 1)
	respCh <- &peers.PeeredObject[[]*cltypes.SignedBeaconBlock]{Data: []*cltypes.SignedBeaconBlock{block}}
	err := listenToIncomingBlocksUntilANewBlockIsReceived(
		t.Context(),
		log.Root(),
		stageCfg,
		Args{targetSlot: block.Block.Slot},
		respCh,
		make(chan error),
	)
	require.ErrorContains(t, err, "signature")
}

func TestRequiresRecentBlockDataAvailabilityByFork(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	clparams.ApplyMinimalPreset(&cfg)
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 1
	cfg.GloasForkEpoch = 2
	cfg.InitializeForkSchedule()

	ctrl := gomock.NewController(t)
	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetCurrentSlot().Return(2 * cfg.SlotsPerEpoch).AnyTimes()
	stageCfg := &Cfg{beaconCfg: &cfg, ethClock: clock}

	tests := []struct {
		name    string
		slot    uint64
		version clparams.StateVersion
		want    bool
	}{
		{name: "Electra", slot: 0, version: clparams.ElectraVersion},
		{name: "Fulu", slot: cfg.SlotsPerEpoch, version: clparams.FuluVersion, want: true},
		{name: "Gloas", slot: 2 * cfg.SlotsPerEpoch, version: clparams.GloasVersion},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			block := cltypes.NewSignedBeaconBlock(&cfg, tt.version)
			block.Block.Slot = tt.slot
			require.Equal(t, tt.want, requiresRecentBlockDataAvailability(stageCfg, block))
		})
	}
}

func TestChainTipSyncBatchesFuluDataAvailability(t *testing.T) {
	stageCfg, blocks, roots := newSignedFuluSyncFixture(t, 1, 2)
	first, second := blocks[0], blocks[1]
	firstRoot, secondRoot := roots[0], roots[1]

	ctrl := gomock.NewController(t)
	peerDas := dasmock.NewMockPeerDas(ctrl)
	peerDas.EXPECT().IsDataAvailable(first.Block.Slot, firstRoot).Return(false, nil)
	peerDas.EXPECT().IsDataAvailable(second.Block.Slot, secondRoot).Return(false, nil)
	peerDas.EXPECT().IsArchivedMode().Return(false)
	peerDas.EXPECT().DownloadOnlyCustodyColumns(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, downloaded []cltypes.ColumnSyncableSignedBlock) error {
			require.Len(t, downloaded, len(blocks))
			return nil
		},
	)
	peerDas.EXPECT().IsDataAvailable(first.Block.Slot, firstRoot).Return(false, nil)
	peerDas.EXPECT().IsDataAvailable(second.Block.Slot, secondRoot).Return(false, nil)
	stageCfg.peerDas = peerDas
	stageCfg.forkChoice.InitPeerDas(peerDas)

	respCh := make(chan *peers.PeeredObject[[]*cltypes.SignedBeaconBlock], 1)
	respCh <- &peers.PeeredObject[[]*cltypes.SignedBeaconBlock]{Data: blocks}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	err := listenToIncomingBlocksUntilANewBlockIsReceived(ctx, log.Root(), stageCfg, Args{targetSlot: second.Block.Slot}, respCh, make(chan error))
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestChainTipSyncRetriesLocalDataAvailabilityErrors(t *testing.T) {
	stageCfg, block, blockRoot := newSignedFuluForwardSyncFixture(t)

	ctrl := gomock.NewController(t)
	peerDas := dasmock.NewMockPeerDas(ctrl)
	peerDas.EXPECT().IsDataAvailable(block.Block.Slot, blockRoot).Return(false, errors.New("storage temporarily unavailable")).Times(2)
	stageCfg.peerDas = peerDas
	stageCfg.forkChoice.InitPeerDas(peerDas)

	respCh := make(chan *peers.PeeredObject[[]*cltypes.SignedBeaconBlock], 1)
	respCh <- &peers.PeeredObject[[]*cltypes.SignedBeaconBlock]{Data: []*cltypes.SignedBeaconBlock{block}}
	go func() {
		timer := time.NewTimer(1100 * time.Millisecond)
		defer timer.Stop()
		<-timer.C
		respCh <- &peers.PeeredObject[[]*cltypes.SignedBeaconBlock]{Data: []*cltypes.SignedBeaconBlock{block}}
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 1250*time.Millisecond)
	defer cancel()

	err := listenToIncomingBlocksUntilANewBlockIsReceived(ctx, log.Root(), stageCfg, Args{targetSlot: block.Block.Slot}, respCh, make(chan error))
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func newFuluChainTipTestBlock(cfg *clparams.BeaconChainConfig, slot uint64, parentRoot common.Hash) *cltypes.SignedBeaconBlock {
	block := cltypes.NewSignedBeaconBlock(cfg, clparams.FuluVersion)
	block.Block.Slot = slot
	block.Block.ParentRoot = parentRoot
	block.Block.Body.BlobKzgCommitments.Append(&cltypes.KZGCommitment{})
	return block
}

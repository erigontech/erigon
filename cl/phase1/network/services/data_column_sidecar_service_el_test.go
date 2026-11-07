package services

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	forkchoicemock "github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	gossipmock "github.com/erigontech/erigon/cl/phase1/network/gossip/mock_services"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
)

var (
	elTestParentRoot = common.Hash{0x11}
	elTestFinalized  = solid.Checkpoint{Epoch: 1, Root: common.Hash{0xf1}}
)

func elBuiltSidecar(slot, index uint64) *cltypes.DataColumnSidecar {
	sidecar := cltypes.NewDataColumnSidecarWithVersion(clparams.FuluVersion)
	sidecar.Index = index
	sidecar.SignedBlockHeader = &cltypes.SignedBeaconBlockHeader{Header: &cltypes.BeaconBlockHeader{Slot: slot, ProposerIndex: 7, ParentRoot: elTestParentRoot}}
	return sidecar
}

// newELPublishTestService returns a column service whose finalized checkpoint (epoch 1) is an ancestor of
// elTestParentRoot.
func newELPublishTestService(t *testing.T) (*dataColumnSidecarService, *gossipmock.MockGossip, *eth_clock.MockEthereumClock, *forkchoicemock.ForkChoiceStorageMock) {
	t.Helper()
	cfg := clparams.MainnetBeaconConfig
	ctrl := gomock.NewController(t)
	publisher := gossipmock.NewMockGossip(ctrl)
	clock := eth_clock.NewMockEthereumClock(ctrl)
	fcu := forkchoicemock.NewForkChoiceStorageMock(t)
	fcu.FinalizedCheckpointVal = elTestFinalized
	fcu.Ancestors[elTestFinalized.Epoch*cfg.SlotsPerEpoch] = forkchoice.ForkChoiceNode{Root: elTestFinalized.Root}
	service := NewDataColumnSidecarService(t.Context(), &cfg, clock, fcu, nil, nil, nil, publisher).(*dataColumnSidecarService)
	return service, publisher, clock, fcu
}

func TestPublishColumnsFromELPublishesUnseenColumnsAndMarksThemSeen(t *testing.T) {
	service, publisher, clock, _ := newELPublishTestService(t)
	defer func(f func(uint64) uint64) { computeSubnetForDataColumnSidecar = f }(computeSubnetForDataColumnSidecar)
	computeSubnetForDataColumnSidecar = func(index uint64) uint64 { return index % service.cfg.DataColumnSidecarSubnetCount }
	fresh, seen := elBuiltSidecar(100, 3), elBuiltSidecar(100, 70)
	// A sidecar for (slot, proposer, index) already arrived on gossip.
	service.seenSidecar.Add(seenSidecarKey{slot: 100, proposerIndex: 7, index: 70}, struct{}{})
	encoded, err := fresh.EncodeSSZ(nil)
	require.NoError(t, err)
	// The digest comes from the sidecar's epoch, not the current one.
	digest := common.Bytes4{1, 2, 3, 4}
	clock.EXPECT().ComputeForkDigest(uint64(100)/service.cfg.SlotsPerEpoch).Return(digest, nil).AnyTimes()
	publisher.EXPECT().PublishToForkDigest(gomock.Any(), digest, gossip.TopicNameDataColumnSidecar(3), encoded).Return(nil).Times(1)

	service.PublishColumnsFromEL(t.Context(), []*cltypes.DataColumnSidecar{fresh, seen})

	_, ok := service.seenSidecar.Get(seenSidecarKey{slot: 100, proposerIndex: 7, index: 3})
	require.True(t, ok)
	// A second import of the same columns publishes nothing.
	service.PublishColumnsFromEL(t.Context(), []*cltypes.DataColumnSidecar{fresh, seen})
}

func TestPublishColumnsFromELSkipsColumnsFinalityNoLongerAllows(t *testing.T) {
	for name, tc := range map[string]struct {
		slot     uint64
		ancestor common.Hash
	}{
		"finalized checkpoint is not an ancestor": {slot: 100, ancestor: common.Hash{0xbb}},
		"slot at the finalized epoch start":       {slot: 32, ancestor: elTestFinalized.Root},
	} {
		t.Run(name, func(t *testing.T) {
			service, _, _, fcu := newELPublishTestService(t)
			fcu.Ancestors[elTestFinalized.Epoch*service.cfg.SlotsPerEpoch] = forkchoice.ForkChoiceNode{Root: tc.ancestor}

			service.PublishColumnsFromEL(t.Context(), []*cltypes.DataColumnSidecar{elBuiltSidecar(tc.slot, 3)})

			_, ok := service.seenSidecar.Get(seenSidecarKey{slot: tc.slot, proposerIndex: 7, index: 3})
			require.False(t, ok)
		})
	}
}

// A column that could not be published stays unclaimed, so a copy from gossip is still processed and relayed.
func TestPublishColumnsFromELReleasesTheClaimOfAFailedPublish(t *testing.T) {
	service, publisher, clock, _ := newELPublishTestService(t)
	defer func(f func(uint64) uint64) { computeSubnetForDataColumnSidecar = f }(computeSubnetForDataColumnSidecar)
	computeSubnetForDataColumnSidecar = func(index uint64) uint64 { return index % service.cfg.DataColumnSidecarSubnetCount }
	clock.EXPECT().ComputeForkDigest(gomock.Any()).Return(common.Bytes4{1, 2, 3, 4}, nil).AnyTimes()
	publisher.EXPECT().PublishToForkDigest(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("topic not found")).Times(1)

	service.PublishColumnsFromEL(t.Context(), []*cltypes.DataColumnSidecar{elBuiltSidecar(100, 3)})

	_, ok := service.seenSidecar.Get(seenSidecarKey{slot: 100, proposerIndex: 7, index: 3})
	require.False(t, ok)
}

package services

import (
	"context"
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
	elTestBlockRoot  = common.Hash{0x22}
	elTestDigest     = common.Bytes4{1, 2, 3, 4}
)

func elBuiltSidecar(slot, index uint64) *cltypes.DataColumnSidecar {
	sidecar := cltypes.NewDataColumnSidecarWithVersion(clparams.FuluVersion)
	sidecar.Index = index
	sidecar.SignedBlockHeader = &cltypes.SignedBeaconBlockHeader{Header: &cltypes.BeaconBlockHeader{Slot: slot, ProposerIndex: 7, ParentRoot: elTestParentRoot}}
	return sidecar
}

func elBuiltGloasSidecar(slot, index uint64) *cltypes.DataColumnSidecar {
	sidecar := cltypes.NewDataColumnSidecarWithVersion(clparams.GloasVersion)
	sidecar.Slot, sidecar.Index, sidecar.BeaconBlockRoot = slot, index, elTestBlockRoot
	return sidecar
}

func gloasSeenKey(sidecar *cltypes.DataColumnSidecar) seenGloasSidecarKey {
	return seenGloasSidecarKey{beaconBlockRoot: sidecar.BeaconBlockRoot, index: sidecar.Index}
}

// newGloasClaimTestService returns an EL publish test service that maps column indices to subnets modulo the subnet
// count and answers every fork digest with elTestDigest.
func newGloasClaimTestService(t *testing.T) (*dataColumnSidecarService, *gossipmock.MockGossip, *eth_clock.MockEthereumClock) {
	t.Helper()
	service, publisher, clock, _ := newELPublishTestService(t)
	original := computeSubnetForDataColumnSidecar
	t.Cleanup(func() { computeSubnetForDataColumnSidecar = original })
	computeSubnetForDataColumnSidecar = func(index uint64) uint64 { return index % service.cfg.DataColumnSidecarSubnetCount }
	clock.EXPECT().ComputeForkDigest(gomock.Any()).Return(elTestDigest, nil).AnyTimes()
	return service, publisher, clock
}

type publishedColumn struct {
	topic   string
	payload []byte
}

// recordPublishes records every column published to elTestDigest and answers the nth publish with outcomes[n], or nil
// once the outcomes run out.
func recordPublishes(publisher *gossipmock.MockGossip, outcomes ...error) *[]publishedColumn {
	published := &[]publishedColumn{}
	publisher.EXPECT().PublishToForkDigest(gomock.Any(), elTestDigest, gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ common.Bytes4, topic string, payload []byte) error {
			var outcome error
			if len(*published) < len(outcomes) {
				outcome = outcomes[len(*published)]
			}
			*published = append(*published, publishedColumn{topic: topic, payload: payload})
			return outcome
		}).AnyTimes()
	return published
}

func requirePublished(t *testing.T, published []publishedColumn, sidecars ...*cltypes.DataColumnSidecar) {
	t.Helper()
	expected := make([]publishedColumn, len(sidecars))
	for i, sidecar := range sidecars {
		payload, err := sidecar.EncodeSSZ(nil)
		require.NoError(t, err)
		expected[i] = publishedColumn{topic: gossip.TopicNameDataColumnSidecar(computeSubnetForDataColumnSidecar(sidecar.Index)), payload: payload}
	}
	require.Equal(t, expected, published)
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

func TestPublishColumnsPublishesUnseenColumnsAndMarksThemSeen(t *testing.T) {
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

	service.PublishColumns(t.Context(), []*cltypes.DataColumnSidecar{fresh, seen})

	_, ok := service.seenSidecar.Get(seenSidecarKey{slot: 100, proposerIndex: 7, index: 3})
	require.True(t, ok)
	// A second import of the same columns publishes nothing.
	service.PublishColumns(t.Context(), []*cltypes.DataColumnSidecar{fresh, seen})
}

func TestPublishColumnsSkipsColumnsFinalityNoLongerAllows(t *testing.T) {
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

			service.PublishColumns(t.Context(), []*cltypes.DataColumnSidecar{elBuiltSidecar(tc.slot, 3)})

			_, ok := service.seenSidecar.Get(seenSidecarKey{slot: tc.slot, proposerIndex: 7, index: 3})
			require.False(t, ok)
		})
	}
}

// A column that could not be published stays unclaimed, so a copy from gossip is still processed and relayed.
func TestPublishColumnsReleasesTheClaimOfAFailedPublish(t *testing.T) {
	service, publisher, clock, _ := newELPublishTestService(t)
	defer func(f func(uint64) uint64) { computeSubnetForDataColumnSidecar = f }(computeSubnetForDataColumnSidecar)
	computeSubnetForDataColumnSidecar = func(index uint64) uint64 { return index % service.cfg.DataColumnSidecarSubnetCount }
	clock.EXPECT().ComputeForkDigest(gomock.Any()).Return(common.Bytes4{1, 2, 3, 4}, nil).AnyTimes()
	publisher.EXPECT().PublishToForkDigest(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("topic not found")).Times(1)

	service.PublishColumns(t.Context(), []*cltypes.DataColumnSidecar{elBuiltSidecar(100, 3)})

	_, ok := service.seenSidecar.Get(seenSidecarKey{slot: 100, proposerIndex: 7, index: 3})
	require.False(t, ok)
}

func TestPublishColumnsReleasesTheClaimOfAFailedGloasPublish(t *testing.T) {
	service, publisher, clock, _ := newELPublishTestService(t)
	defer func(f func(uint64) uint64) { computeSubnetForDataColumnSidecar = f }(computeSubnetForDataColumnSidecar)
	computeSubnetForDataColumnSidecar = func(index uint64) uint64 { return index % service.cfg.DataColumnSidecarSubnetCount }
	clock.EXPECT().ComputeForkDigest(gomock.Any()).Return(common.Bytes4{1, 2, 3, 4}, nil).AnyTimes()
	publisher.EXPECT().PublishToForkDigest(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("topic not found")).Times(1)
	sidecar := cltypes.NewDataColumnSidecarWithVersion(clparams.GloasVersion)
	sidecar.Slot, sidecar.Index, sidecar.BeaconBlockRoot = 100, 3, common.Hash{0x22}

	service.PublishColumns(t.Context(), []*cltypes.DataColumnSidecar{sidecar})

	ok := service.seenGloas.contains(seenGloasSidecarKey{beaconBlockRoot: sidecar.BeaconBlockRoot, index: 3})
	require.False(t, ok)
}

func TestSeenGloasReleaseKeepsANewerMark(t *testing.T) {
	seen := newSeenGloasSidecars("testSeenGloas", 1)
	key := seenGloasSidecarKey{beaconBlockRoot: common.Hash{0x31}, index: 1}
	evictor := seenGloasSidecarKey{beaconBlockRoot: common.Hash{0x31}, index: 2}
	staleRelease, claimed := seen.claim(key)
	require.True(t, claimed)
	_, claimed = seen.claim(evictor)
	require.True(t, claimed)
	require.False(t, seen.contains(key))
	_, claimed = seen.claim(key)
	require.True(t, claimed)

	staleRelease()

	require.True(t, seen.contains(key))
}

func TestClaimColumnsMarksWithoutPublishing(t *testing.T) {
	service, _, _ := newGloasClaimTestService(t)
	sidecars := []*cltypes.DataColumnSidecar{elBuiltGloasSidecar(100, 3), elBuiltGloasSidecar(100, 5)}

	service.ClaimColumns(sidecars)

	for _, sidecar := range sidecars {
		require.True(t, service.seenGloas.contains(gloasSeenKey(sidecar)))
	}
}

func TestClaimColumnsPublishPublishesTheClaimedColumns(t *testing.T) {
	service, publisher, _ := newGloasClaimTestService(t)
	published := recordPublishes(publisher)
	sidecars := []*cltypes.DataColumnSidecar{elBuiltGloasSidecar(100, 3), elBuiltGloasSidecar(100, 5)}

	publish := service.ClaimColumns(sidecars)
	require.Empty(t, *published)
	publish(t.Context())

	requirePublished(t, *published, sidecars...)
}

func TestClaimColumnsPublishReleasesOnlyAFailedColumn(t *testing.T) {
	failure := errors.New("topic not found")
	for name, outcomes := range map[string][]error{
		"first fails then second succeeds": {failure, nil},
		"first succeeds then second fails": {nil, failure},
	} {
		t.Run(name, func(t *testing.T) {
			service, publisher, _ := newGloasClaimTestService(t)
			published := recordPublishes(publisher, outcomes...)
			sidecars := []*cltypes.DataColumnSidecar{elBuiltGloasSidecar(100, 3), elBuiltGloasSidecar(100, 5)}

			publish := service.ClaimColumns(sidecars)
			publish(t.Context())

			requirePublished(t, *published, sidecars...)
			for i, sidecar := range sidecars {
				require.Equal(t, outcomes[i] == nil, service.seenGloas.contains(gloasSeenKey(sidecar)), "column %d", sidecar.Index)
			}
		})
	}
}

func TestClaimColumnsPublishFailureKeepsAMarkTakenAfterItsClaimWasReleasedAndRetaken(t *testing.T) {
	service, publisher, _ := newGloasClaimTestService(t)
	recordPublishes(publisher, errors.New("topic not found"))
	sidecar := elBuiltGloasSidecar(100, 3)
	publish := service.ClaimColumns([]*cltypes.DataColumnSidecar{sidecar})
	service.seenGloas.cache.Remove(gloasSeenKey(sidecar))
	_, retaken := service.seenGloas.claim(gloasSeenKey(sidecar))
	require.True(t, retaken)

	publish(t.Context())

	require.True(t, service.seenGloas.contains(gloasSeenKey(sidecar)))
}

func TestClaimColumnsSkipsColumnsSeenBefore(t *testing.T) {
	service, publisher, _ := newGloasClaimTestService(t)
	published := recordPublishes(publisher)
	seen, fresh := elBuiltGloasSidecar(100, 3), elBuiltGloasSidecar(100, 5)
	_, claimedBefore := service.seenGloas.claim(gloasSeenKey(seen))
	require.True(t, claimedBefore)

	service.ClaimColumns([]*cltypes.DataColumnSidecar{seen, fresh})(t.Context())

	requirePublished(t, *published, fresh)
	require.True(t, service.seenGloas.contains(gloasSeenKey(seen)))
}

func TestClaimColumnsWithAForkDigestErrorMarksNothing(t *testing.T) {
	service, publisher, clock, _ := newELPublishTestService(t)
	clock.EXPECT().ComputeForkDigest(gomock.Any()).Return(common.Bytes4{}, errors.New("no fork digest")).Times(1)
	published := recordPublishes(publisher)
	sidecar := elBuiltGloasSidecar(100, 3)

	service.ClaimColumns([]*cltypes.DataColumnSidecar{sidecar})(t.Context())

	require.Empty(t, *published)
	require.False(t, service.seenGloas.contains(gloasSeenKey(sidecar)))
}

func TestClaimColumnsForANonImportableBlockClaimsNothing(t *testing.T) {
	service, publisher, _, _ := newELPublishTestService(t)
	published := recordPublishes(publisher)
	sidecar := elBuiltGloasSidecar(elTestFinalized.Epoch*service.cfg.SlotsPerEpoch, 3)

	service.ClaimColumns([]*cltypes.DataColumnSidecar{sidecar})(t.Context())

	require.Empty(t, *published)
	require.False(t, service.seenGloas.contains(gloasSeenKey(sidecar)))
}

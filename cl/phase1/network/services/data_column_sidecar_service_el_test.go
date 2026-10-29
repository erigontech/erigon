package services

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/gossip"
	gossipmock "github.com/erigontech/erigon/cl/phase1/network/gossip/mock_services"
)

func elBuiltSidecar(index uint64) *cltypes.DataColumnSidecar {
	sidecar := cltypes.NewDataColumnSidecarWithVersion(clparams.FuluVersion)
	sidecar.Index = index
	sidecar.SignedBlockHeader = &cltypes.SignedBeaconBlockHeader{Header: &cltypes.BeaconBlockHeader{Slot: 100, ProposerIndex: 7}}
	return sidecar
}

func TestPublishColumnsFromELPublishesUnseenColumnsAndMarksThemSeen(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	defer func(f func(uint64) uint64) { computeSubnetForDataColumnSidecar = f }(computeSubnetForDataColumnSidecar)
	computeSubnetForDataColumnSidecar = func(index uint64) uint64 { return index % cfg.DataColumnSidecarSubnetCount }
	ctrl := gomock.NewController(t)
	publisher := gossipmock.NewMockGossip(ctrl)
	service := NewDataColumnSidecarService(t.Context(), &cfg, nil, nil, nil, nil, nil, publisher).(*dataColumnSidecarService)
	fresh, seen := elBuiltSidecar(3), elBuiltSidecar(70)
	// A sidecar for (slot, proposer, index) already arrived on gossip.
	service.seenSidecar.Add(seenSidecarKey{slot: 100, proposerIndex: 7, index: 70}, struct{}{})
	encoded, err := fresh.EncodeSSZ(nil)
	require.NoError(t, err)
	publisher.EXPECT().Publish(gomock.Any(), gossip.TopicNameDataColumnSidecar(3), encoded).Return(nil).Times(1)

	service.PublishColumnsFromEL(t.Context(), []*cltypes.DataColumnSidecar{fresh, seen})

	_, ok := service.seenSidecar.Get(seenSidecarKey{slot: 100, proposerIndex: 7, index: 3})
	require.True(t, ok)
	// A second import of the same columns publishes nothing.
	service.PublishColumnsFromEL(t.Context(), []*cltypes.DataColumnSidecar{fresh, seen})
}

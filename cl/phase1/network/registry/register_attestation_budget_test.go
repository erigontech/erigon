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

package registry

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/metrics"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	clgossip "github.com/erigontech/erigon/cl/gossip"
	p2p_mock "github.com/erigontech/erigon/cl/p2p/mock_services"
	forkchoice_mock "github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/phase1/network/gossip"
	"github.com/erigontech/erigon/cl/phase1/network/services"
	service_mock "github.com/erigontech/erigon/cl/phase1/network/services/mock_services"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

func TestAttestationGossipBudgetScalesWithExpectedTraffic(t *testing.T) {
	tests := []struct {
		name             string
		beaconConfig     clparams.BeaconChainConfig
		networkConfig    clparams.NetworkConfig
		activeValidators uint64
		budget           int
	}{
		{
			name:             "mainnet",
			beaconConfig:     clparams.BeaconConfigs[chainspec.MainnetChainID],
			networkConfig:    clparams.NetworkConfigs[chainspec.MainnetChainID],
			activeValidators: 262_144,
			budget:           384,
		},
		{
			name:             "gnosis",
			beaconConfig:     clparams.BeaconConfigs[chainspec.GnosisChainID],
			networkConfig:    clparams.NetworkConfigs[chainspec.GnosisChainID],
			activeValidators: 65_536,
			budget:           384,
		},
		{
			name:          "no head state",
			beaconConfig:  clparams.BeaconConfigs[chainspec.MainnetChainID],
			networkConfig: clparams.NetworkConfigs[chainspec.MainnetChainID],
			budget:        250,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			digest := common.Bytes4{0x11, 0x22, 0x33, 0x44}

			h, err := libp2p.New(libp2p.NoListenAddrs)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, h.Close()) })
			ps, err := pubsub.NewGossipSub(t.Context(), h)
			require.NoError(t, err)

			clock := eth_clock.NewMockEthereumClock(ctrl)
			clock.EXPECT().CurrentForkDigest().Return(digest, nil).AnyTimes()
			clock.EXPECT().StateVersionByForkDigest(digest).Return(clparams.Phase0Version, nil).AnyTimes()
			clock.EXPECT().GetCurrentEpoch().Return(uint64(0)).AnyTimes()
			clock.EXPECT().GetCurrentSlot().Return(uint64(0)).AnyTimes()
			clock.EXPECT().GetEpochAtSlot(gomock.Any()).Return(uint64(0)).AnyTimes()
			clock.EXPECT().ComputeForkDigest(gomock.Any()).Return(digest, nil).AnyTimes()

			p2pManager := p2p_mock.NewMockP2PManager(ctrl)
			p2pManager.EXPECT().Pubsub().Return(ps).AnyTimes()
			p2pManager.EXPECT().Host().Return(nil).AnyTimes()
			p2pManager.EXPECT().BandwidthCounter().Return(metrics.NewBandwidthCounter()).AnyTimes()

			gm := gossip.NewGossipManager(
				t.Context(), p2pManager, &tt.beaconConfig, &tt.networkConfig, clock, false, 0,
				datasize.MB, datasize.MB, false,
			)
			t.Cleanup(func() { require.NoError(t, gm.Close()) })

			blockService := service_mock.NewMockBlockService(ctrl)
			syncContributionService := service_mock.NewMockSyncContributionService(ctrl)
			aggregateAndProofService := service_mock.NewMockAggregateAndProofService(ctrl)
			syncCommitteeMessagesService := service_mock.NewMockSyncCommitteeMessagesService(ctrl)
			attesterSlashingService := service_mock.NewMockAttesterSlashingService(ctrl)
			voluntaryExitService := service_mock.NewMockVoluntaryExitService(ctrl)
			blsToExecutionChangeService := service_mock.NewMockBLSToExecutionChangeService(ctrl)
			proposerSlashingService := service_mock.NewMockProposerSlashingService(ctrl)
			attestationService := service_mock.NewMockAttestationService(ctrl)
			blobService := service_mock.NewMockBlobSidecarsService(ctrl)
			dataColumnSidecarService := service_mock.NewMockDataColumnSidecarService(ctrl)
			executionPayloadService := service_mock.NewMockExecutionPayloadService(ctrl)
			payloadAttestationService := service_mock.NewMockPayloadAttestationService(ctrl)
			proposerPreferencesService := service_mock.NewMockProposerPreferencesService(ctrl)
			executionPayloadBidService := service_mock.NewMockExecutionPayloadBidService(ctrl)

			blockService.EXPECT().Names().Return(nil)
			syncContributionService.EXPECT().Names().Return(nil)
			aggregateAndProofService.EXPECT().Names().Return(nil)
			syncCommitteeMessagesService.EXPECT().Names().Return(nil)
			attesterSlashingService.EXPECT().Names().Return(nil)
			voluntaryExitService.EXPECT().Names().Return(nil)
			blsToExecutionChangeService.EXPECT().Names().Return(nil)
			proposerSlashingService.EXPECT().Names().Return(nil)
			blobService.EXPECT().Names().Return(nil)
			dataColumnSidecarService.EXPECT().Names().Return(nil)
			executionPayloadService.EXPECT().Names().Return(nil)
			payloadAttestationService.EXPECT().Names().Return(nil)
			proposerPreferencesService.EXPECT().Names().Return(nil)
			executionPayloadBidService.EXPECT().Names().Return(nil)

			attestationNames := make([]string, 0, tt.networkConfig.AttestationSubnetCount)
			for subnet := range tt.networkConfig.AttestationSubnetCount {
				attestationNames = append(attestationNames, clgossip.TopicNameBeaconAttestation(subnet))
			}
			attestationService.EXPECT().Names().Return(attestationNames)
			var decoded atomic.Int64
			attestationService.EXPECT().DecodeGossipMessage(gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ peer.ID, _ []byte, _ clparams.StateVersion) (*services.AttestationForGossip, error) {
					decoded.Add(1)
					return new(services.AttestationForGossip), nil
				}).AnyTimes()
			attestationService.EXPECT().ProcessMessage(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

			forkChoice := forkchoice_mock.NewForkChoiceStorageMock(t)
			RegisterGossipServices(
				gm, forkChoice, clock, &tt.beaconConfig, func() uint64 { return tt.activeValidators }, blockService,
				attesterSlashingService, blobService, dataColumnSidecarService, syncCommitteeMessagesService,
				syncContributionService, aggregateAndProofService, attestationService, voluntaryExitService,
				blsToExecutionChangeService, proposerSlashingService, executionPayloadService,
				payloadAttestationService, proposerPreferencesService, executionPayloadBidService,
			)

			expiry := time.Now().Add(time.Minute)
			require.NoError(t, gm.SubscribeWithExpiry(attestationNames[0], expiry))
			require.NoError(t, gm.SubscribeWithExpiry(attestationNames[1], expiry))

			published := 0
			publishThrough := func(total int) {
				t.Helper()
				for published < total {
					message := []byte(fmt.Sprintf("attestation-%d", published))
					if err := gm.Publish(context.Background(), attestationNames[published%2], message); err != nil {
						require.EqualError(t, err, pubsub.RejectValidationIgnored)
					}
					published++
				}
			}

			firstBatch := min(256, tt.budget)
			publishThrough(firstBatch)
			require.Equal(t, int64(firstBatch), decoded.Load())

			publishThrough(tt.budget + 100)
			require.Equal(t, int64(tt.budget), decoded.Load())
		})
	}
}

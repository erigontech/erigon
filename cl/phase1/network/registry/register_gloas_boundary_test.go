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
	"github.com/erigontech/erigon/cl/cltypes"
	clgossip "github.com/erigontech/erigon/cl/gossip"
	p2p_mock "github.com/erigontech/erigon/cl/p2p/mock_services"
	forkchoice_mock "github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/phase1/network/gossip"
	service_mock "github.com/erigontech/erigon/cl/phase1/network/services/mock_services"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
)

func TestRegisterGossipServicesProcessesGloasPreferencesOneEpochEarly(t *testing.T) {
	tests := []struct {
		name          string
		topicDigest   common.Bytes4
		topicVersion  clparams.StateVersion
		wantProcessed bool
	}{
		{
			name:          "Gloas digest",
			topicDigest:   common.Bytes4{0xaa, 0xbb, 0xcc, 0xdd},
			topicVersion:  clparams.GloasVersion,
			wantProcessed: true,
		},
		{
			name:         "Fulu digest",
			topicDigest:  common.Bytes4{0x11, 0x22, 0x33, 0x44},
			topicVersion: clparams.FuluVersion,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			cfg := clparams.MainnetBeaconConfig
			cfg.AltairForkEpoch = 0
			cfg.BellatrixForkEpoch = 0
			cfg.CapellaForkEpoch = 0
			cfg.DenebForkEpoch = 0
			cfg.ElectraForkEpoch = 0
			cfg.FuluForkEpoch = 0
			cfg.GloasForkEpoch = 3
			cfg.InitializeForkSchedule()

			h, err := libp2p.New(libp2p.NoListenAddrs)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, h.Close()) })
			ps, err := pubsub.NewGossipSub(t.Context(), h, pubsub.WithPeerScore(
				&pubsub.PeerScoreParams{
					Topics:            make(map[string]*pubsub.TopicScoreParams),
					AppSpecificScore:  func(peer.ID) float64 { return 0 },
					AppSpecificWeight: 1,
					DecayInterval:     time.Second,
					DecayToZero:       0.01,
					RetainScore:       time.Minute,
				},
				&pubsub.PeerScoreThresholds{},
			))
			require.NoError(t, err)

			clock := eth_clock.NewMockEthereumClock(ctrl)
			clock.EXPECT().CurrentForkDigest().Return(tt.topicDigest, nil).AnyTimes()
			clock.EXPECT().StateVersionByForkDigest(tt.topicDigest).Return(tt.topicVersion, nil).AnyTimes()
			clock.EXPECT().GetCurrentEpoch().Return(uint64(2)).AnyTimes()
			clock.EXPECT().GetCurrentSlot().Return(uint64(64)).AnyTimes()
			clock.EXPECT().GetEpochAtSlot(gomock.Any()).DoAndReturn(func(slot uint64) uint64 {
				return slot / cfg.SlotsPerEpoch
			}).AnyTimes()
			clock.EXPECT().ComputeForkDigest(gomock.Any()).Return(common.Bytes4{0xaa, 0xbb, 0xcc, 0xdd}, nil).AnyTimes()
			p2pManager := p2p_mock.NewMockP2PManager(ctrl)
			p2pManager.EXPECT().Pubsub().Return(ps).AnyTimes()
			p2pManager.EXPECT().Host().Return(nil).AnyTimes()
			p2pManager.EXPECT().BandwidthCounter().Return(metrics.NewBandwidthCounter()).AnyTimes()
			gm := gossip.NewGossipManager(
				t.Context(), p2pManager, &cfg, &clparams.NetworkConfig{}, clock, false, 0,
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
			attestationService.EXPECT().Names().Return(nil)
			blobService.EXPECT().Names().Return(nil)
			dataColumnSidecarService.EXPECT().Names().Return(nil)
			executionPayloadService.EXPECT().Names().Return(nil)
			payloadAttestationService.EXPECT().Names().Return(nil)
			proposerPreferencesService.EXPECT().Names().Return([]string{clgossip.TopicNameProposerPreferences})
			executionPayloadBidService.EXPECT().Names().Return(nil)

			preference := &cltypes.SignedProposerPreferences{Message: &cltypes.ProposerPreferences{ProposalSlot: 96}}
			proposerPreferencesService.EXPECT().DecodeGossipMessage(gomock.Any(), []byte("preference"), tt.topicVersion).Return(preference, nil).MaxTimes(1)
			processed := make(chan struct{})
			processCall := proposerPreferencesService.EXPECT().ProcessMessage(gomock.Any(), gomock.Nil(), preference)
			if tt.wantProcessed {
				processCall.DoAndReturn(func(context.Context, *uint64, *cltypes.SignedProposerPreferences) error {
					close(processed)
					return nil
				})
			} else {
				processCall.Times(0)
			}

			forkChoice := forkchoice_mock.NewForkChoiceStorageMock(t)
			forkChoice.HighestSeenVal = 64
			RegisterGossipServices(
				gm, forkChoice, clock, blockService, attesterSlashingService, blobService, dataColumnSidecarService,
				syncCommitteeMessagesService, syncContributionService, aggregateAndProofService, attestationService,
				voluntaryExitService, blsToExecutionChangeService, proposerSlashingService, executionPayloadService,
				payloadAttestationService, proposerPreferencesService, executionPayloadBidService,
			)

			err = gm.Publish(t.Context(), clgossip.TopicNameProposerPreferences, []byte("preference"))
			if !tt.wantProcessed {
				require.EqualError(t, err, pubsub.RejectValidationIgnored)
				return
			}
			require.NoError(t, err)
			select {
			case <-processed:
			case <-time.After(time.Second):
				t.Fatal("proposer preferences message was not processed")
			}
		})
	}
}

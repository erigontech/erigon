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
	"fmt"
	"testing"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
)

// PTC votes are due at one point in the slot, so a single peer can forward the
// whole committee's votes in one burst.
func TestPayloadAttestationRateLimiterAdmitsFullCommitteeBurstFromOnePeer(t *testing.T) {
	condition := withPayloadAttestationRateLimiter()
	pid := peer.ID("ptc-peer")
	for i := range int(clparams.MaxPtcSize) {
		require.True(t, condition(pid, nil, clparams.GloasVersion), "vote %d", i)
	}
}

func TestProposerPreferencesRateLimiterAdmitsLookaheadWindowFromOnePeer(t *testing.T) {
	minimal := clparams.MainnetBeaconConfig
	clparams.ApplyMinimalPreset(&minimal)
	longLookahead := clparams.MainnetBeaconConfig
	longLookahead.MinSeedLookahead = 2
	for _, test := range []struct {
		name   string
		cfg    clparams.BeaconChainConfig
		window int
	}{
		{name: "mainnet", cfg: clparams.MainnetBeaconConfig, window: 64},
		{name: "minimal", cfg: minimal, window: 16},
		{name: "long lookahead", cfg: longLookahead, window: 96},
	} {
		t.Run(test.name, func(t *testing.T) {
			condition := withProposerPreferencesRateLimiter(&test.cfg)
			pid := peer.ID("relaying-peer")
			for i := range test.window {
				require.True(t, condition(pid, nil, clparams.GloasVersion), "preference %d", i)
			}
			require.False(t, condition(pid, nil, clparams.GloasVersion))
		})
	}
}

// Aggregates for a slot are published at one point in the slot, so a single peer can forward
// every committee's aggregators in one burst.
func TestAggregateAndProofRateLimiterAdmitsSlotBurstFromOnePeer(t *testing.T) {
	minimal := clparams.MainnetBeaconConfig
	clparams.ApplyMinimalPreset(&minimal)
	for _, test := range []struct {
		name  string
		cfg   clparams.BeaconChainConfig
		burst int
	}{
		{name: "mainnet", cfg: clparams.MainnetBeaconConfig, burst: 1024},
		{name: "minimal", cfg: minimal, burst: 64},
	} {
		t.Run(test.name, func(t *testing.T) {
			condition := withAggregateAndProofRateLimiter(&test.cfg)
			pid := peer.ID("aggregate-peer")
			for i := range test.burst {
				require.True(t, condition(pid, nil, clparams.FuluVersion), "aggregate %d", i)
			}
		})
	}
}

func syncCommitteeGossipMessage(subnet uint64) *pubsub.Message {
	topic := fmt.Sprintf("/eth2/00000000/sync_committee_%d/ssz_snappy", subnet)
	return &pubsub.Message{Message: &pb.Message{Topic: &topic}}
}

// A peer may relay every message of each subnet it serves in one burst, but no more than a subnet carries in a slot.
func TestSyncCommitteeMessageRateLimiterAdmitsEachSubnetsSlotFromOnePeer(t *testing.T) {
	minimal := clparams.MainnetBeaconConfig
	clparams.ApplyMinimalPreset(&minimal)
	for _, test := range []struct {
		name      string
		cfg       clparams.BeaconChainConfig
		perSubnet int
	}{
		{name: "mainnet", cfg: clparams.MainnetBeaconConfig, perSubnet: 128},
		{name: "minimal", cfg: minimal, perSubnet: 8},
	} {
		t.Run(test.name, func(t *testing.T) {
			condition := withSyncCommitteeMessageRateLimiter(&test.cfg)
			pid := peer.ID("sync-committee-peer")
			for subnet := range test.cfg.SyncCommitteeSubnetCount {
				for i := range test.perSubnet {
					require.True(t, condition(pid, syncCommitteeGossipMessage(subnet), clparams.FuluVersion), "subnet %d message %d", subnet, i)
				}
			}
			require.False(t, condition(pid, syncCommitteeGossipMessage(0), clparams.FuluVersion), "a subnet's next message in the same slot")
		})
	}
}

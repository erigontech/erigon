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

package p2p

import (
	"bytes"
	"context"
	"encoding/hex"
	"io"
	"testing"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/libp2p/go-msgio"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/libp2p/go-libp2p/core/peer"
)

type rpcReceiveTracer struct {
	received chan struct{}
}

func (tracer *rpcReceiveTracer) Trace(event *pb.TraceEvent) {
	if event.GetType() == pb.TraceEvent_RECV_RPC {
		select {
		case tracer.received <- struct{}{}:
		default:
		}
	}
}

// sendRawRPC writes rpc to a Caplin gossipsub peer and reports whether the peer passed
// it to the pubsub event loop (true) or reset the stream (false).
func sendRawRPC(t *testing.T, rpc []byte) bool {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	mn := mocknet.New()
	t.Cleanup(func() { require.NoError(t, mn.Close()) })
	target, err := mn.GenPeer()
	require.NoError(t, err)
	sender, err := mn.GenPeer()
	require.NoError(t, err)
	sender.SetStreamHandler(pubsub.GossipSubID_v11, func(s network.Stream) {
		defer s.Close()
		_, _ = io.Copy(io.Discard, s)
	})

	networkConfig, beaconConfig, _, err := clparams.GetConfigsByNetworkName("mainnet")
	require.NoError(t, err)
	manager := &p2pManager{cfg: &P2PConfig{NetworkConfig: networkConfig, BeaconConfig: beaconConfig}, ethClock: eth_clock.NewEthereumClock(0, common.Hash{}, beaconConfig)}
	tracer := &rpcReceiveTracer{received: make(chan struct{}, 1)}
	opts, err := manager.pubsubOptions(beaconConfig)
	require.NoError(t, err)
	opts = append(opts, pubsub.WithEventTracer(tracer))
	_, err = pubsub.NewGossipSub(ctx, target, opts...)
	require.NoError(t, err)
	require.NoError(t, mn.LinkAll())
	require.NoError(t, mn.ConnectAllButSelf())

	s, err := sender.NewStream(ctx, target.ID(), pubsub.GossipSubID_v11)
	require.NoError(t, err)
	defer s.Close()
	if err := msgio.NewVarintWriter(s).WriteMsg(rpc); err != nil {
		require.ErrorIs(t, err, network.ErrReset)
		return false
	}
	reset := make(chan error, 1)
	go func() {
		_, err := s.Read(make([]byte, 1))
		reset <- err
	}()

	select {
	case err := <-reset:
		require.ErrorIs(t, err, network.ErrReset)
		return false
	case <-tracer.received:
		return true
	case <-ctx.Done():
		t.Fatal("the RPC was neither delivered nor rejected")
		return false
	}
}

// TestPubsubRejectsOversizedIHave pins Caplin's control-message bound: an IHAVE
// frame under the message-size cap but over the default control-message budget is
// rejected (stream reset) instead of being delivered to the pubsub event loop.
func TestPubsubRejectsOversizedIHave(t *testing.T) {
	ids := bytes.Repeat([]byte{0x12, 0x00}, (512<<10)/2+1)
	control := protowire.AppendTag(nil, 1, protowire.BytesType)
	control = protowire.AppendBytes(control, ids)
	rpc := protowire.AppendTag(nil, 3, protowire.BytesType)
	rpc = protowire.AppendBytes(rpc, control)
	networkConfig := clparams.NetworkConfigs[chainspec.MainnetChainID]
	require.Less(t, uint64(len(rpc)), networkConfig.MaxGossipMessageSize())

	require.False(t, sendRawRPC(t, rpc), "oversized IHAVE was decoded and passed to the pubsub event loop")
}

func TestPubsubFrameLimitFollowsMaxGossipMessageSize(t *testing.T) {
	networkConfig := clparams.NetworkConfigs[chainspec.MainnetChainID]
	limit := int(networkConfig.MaxGossipMessageSize())
	const topic = "/eth2/00000000/beacon_block/ssz_snappy"
	publishRPC := func(size int) []byte {
		msgLen := func(data int) int {
			return protowire.SizeTag(4) + protowire.SizeBytes(len(topic)) + protowire.SizeTag(2) + protowire.SizeBytes(data)
		}
		data := size
		for protowire.SizeTag(2)+protowire.SizeBytes(msgLen(data)) != size {
			data--
		}
		msg := protowire.AppendTag(nil, 4, protowire.BytesType)
		msg = protowire.AppendString(msg, topic)
		msg = protowire.AppendTag(msg, 2, protowire.BytesType)
		msg = protowire.AppendBytes(msg, make([]byte, data))
		rpc := protowire.AppendTag(nil, 2, protowire.BytesType)
		rpc = protowire.AppendBytes(rpc, msg)
		require.Len(t, rpc, size)
		return rpc
	}

	require.True(t, sendRawRPC(t, publishRPC(limit)), "an RPC at max_message_size must be accepted")
	require.False(t, sendRawRPC(t, publishRPC(limit+1)), "an RPC above max_message_size must reset the stream")
}

func TestGossipSubscriptionFilter(t *testing.T) {
	t.Parallel()
	networkConfig, beaconConfig, _, err := clparams.GetConfigsByNetworkName("mainnet")
	require.NoError(t, err)
	filter := gossipSubscriptionFilter([]string{"4a26c58b"}, gossipTopicNames(networkConfig, beaconConfig))
	require.True(t, filter.CanSubscribe("/eth2/4a26c58b/beacon_block/ssz_snappy"))
	require.True(t, filter.CanSubscribe("/eth2/4a26c58b/data_column_sidecar_127/ssz_snappy"))
	require.True(t, filter.CanSubscribe("/eth2/4a26c58b/beacon_attestation_63/ssz_snappy"))
	require.True(t, filter.CanSubscribe("/eth2/4a26c58b/sync_committee_contribution_and_proof/ssz_snappy"))
	require.True(t, filter.CanSubscribe("/eth2/4a26c58b/proposer_preferences/ssz_snappy"))
	require.False(t, filter.CanSubscribe("/eth2/deadbeef/beacon_block/ssz_snappy"), "a digest outside the schedule")
	require.False(t, filter.CanSubscribe("/eth2/4a26c58b/made_up_topic/ssz_snappy"), "a name that is not a gossip topic")
	require.False(t, filter.CanSubscribe("/eth2/4a26c58b/beacon_attestation_64/ssz_snappy"), "a subnet past the count")
	require.False(t, filter.CanSubscribe("/eth2/4a26c58b/beacon_attestation_007/ssz_snappy"), "a zero-padded subnet")
	require.False(t, filter.CanSubscribe("/eth2/4a26c58b/beacon_block/ssz"))
	require.False(t, filter.CanSubscribe("/junk/topic"))

	subscribe, topic := true, "/eth2/4a26c58b/beacon_block/ssz_snappy"
	subs := make([]*pb.RPC_SubOpts, maxSubscriptionsPerRPC+1)
	for i := range subs {
		subs[i] = &pb.RPC_SubOpts{Subscribe: &subscribe, Topicid: &topic}
	}
	_, err = filter.FilterIncomingSubscriptions(peer.ID("p"), subs)
	require.ErrorIs(t, err, pubsub.ErrTooManySubscriptions)
}

func TestPubsubOptionsRefuseNonGossipTopics(t *testing.T) {
	t.Parallel()
	mn := mocknet.New()
	t.Cleanup(func() { require.NoError(t, mn.Close()) })
	host, err := mn.GenPeer()
	require.NoError(t, err)
	networkConfig, beaconConfig, _, err := clparams.GetConfigsByNetworkName("mainnet")
	require.NoError(t, err)
	manager := &p2pManager{cfg: &P2PConfig{NetworkConfig: networkConfig, BeaconConfig: beaconConfig}, ethClock: eth_clock.NewEthereumClock(0, common.Hash{}, beaconConfig)}
	psOpts, err := manager.pubsubOptions(beaconConfig)
	require.NoError(t, err)
	ps, err := pubsub.NewGossipSub(t.Context(), host, psOpts...)
	require.NoError(t, err)

	_, err = ps.Join("/junk/topic")
	require.ErrorContains(t, err, "subscription filter")
}

func TestScheduledForkDigestsFollowForksAndBlobParameters(t *testing.T) {
	t.Parallel()
	_, beaconConfig, _, err := clparams.GetConfigsByNetworkName("mainnet")
	require.NoError(t, err)
	clock := eth_clock.NewEthereumClock(0, common.Hash{}, beaconConfig)
	manager := &p2pManager{ethClock: clock}

	digests, err := manager.scheduledForkDigests(beaconConfig)
	require.NoError(t, err)

	epochs := []uint64{0, beaconConfig.AltairForkEpoch, beaconConfig.ElectraForkEpoch, beaconConfig.FuluForkEpoch}
	for _, blobParameters := range beaconConfig.BlobSchedule {
		epochs = append(epochs, blobParameters.Epoch)
	}
	for _, epoch := range epochs {
		digest, err := clock.ComputeForkDigest(epoch)
		require.NoError(t, err)
		require.Contains(t, digests, hex.EncodeToString(digest[:]), "epoch %d", epoch)
	}
}

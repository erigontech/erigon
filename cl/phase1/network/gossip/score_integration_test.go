package gossip

import (
	"context"
	"io"
	"testing"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/libp2p/go-msgio"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	"github.com/erigontech/erigon/cl/clparams"
	gossipnames "github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/p2p/mock_services"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

type gossipScoreTracer struct {
	added      chan struct{}
	rejections chan string
}

func (tracer *gossipScoreTracer) Trace(event *pb.TraceEvent) {
	switch event.GetType() {
	case pb.TraceEvent_ON_NEW_OUTBOUND_STREAM:
		select {
		case tracer.added <- struct{}{}:
		default:
		}
	case pb.TraceEvent_REJECT_MESSAGE:
		tracer.rejections <- event.GetRejectMessage().GetReason()
	}
}

func TestGossipValidationGraylistsPeer(t *testing.T) {
	for _, tc := range []struct {
		topic            string
		activeValidators uint64
	}{
		{gossipnames.TopicNameBeaconAggregateAndProof, 64},
		{gossipnames.TopicNameBeaconAttestation(0), 64},
		{gossipnames.TopicNameSyncCommittee(0), 0},
	} {
		t.Run(tc.topic, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			mn := mocknet.New()
			t.Cleanup(func() { require.NoError(t, mn.Close()) })
			receiver, err := mn.GenPeer()
			require.NoError(t, err)
			sender, err := mn.GenPeer()
			require.NoError(t, err)
			sender.SetStreamHandler(pubsub.GossipSubID_v11, func(stream network.Stream) {
				defer stream.Close()
				_, _ = io.Copy(io.Discard, stream)
			})

			networkConfig, beaconConfig := clparams.GetConfigsByNetwork(chainspec.MainnetChainID)
			clparams.ApplyMinimalPreset(beaconConfig)
			controller := gomock.NewController(t)
			mockP2P := mock_services.NewMockP2PManager(controller)
			mockP2P.EXPECT().Host().Return(receiver).AnyTimes()
			clock := eth_clock.NewMockEthereumClock(controller)
			clock.EXPECT().GetCurrentEpoch().Return(uint64(0)).AnyTimes()
			clock.EXPECT().StateVersionByForkDigest(gomock.Any()).Return(clparams.Phase0Version, nil).AnyTimes()
			manager := &GossipManager{
				p2p:            mockP2P,
				beaconConfig:   beaconConfig,
				networkConfig:  networkConfig,
				ethClock:       clock,
				activeIndicies: tc.activeValidators,
				stats:          newGossipMessageStats(),
			}
			params := manager.topicScoreParams(tc.topic)
			require.NotNil(t, params)
			topicName := composeTopic(common.Bytes4{}, tc.topic)
			tracer := &gossipScoreTracer{added: make(chan struct{}, 1), rejections: make(chan string, 32)}
			router := pubsub.DefaultGossipSubRouter(receiver)
			ps, err := pubsub.NewGossipSubWithRouter(ctx, receiver, router,
				pubsub.WithMessageSignaturePolicy(pubsub.StrictNoSign),
				pubsub.WithNoAuthor(),
				pubsub.WithMessageIdFn(func(msg *pb.Message) string { return string(msg.Data) }),
				pubsub.WithPeerScore(&pubsub.PeerScoreParams{
					Topics:           make(map[string]*pubsub.TopicScoreParams),
					AppSpecificScore: func(peer.ID) float64 { return 0 },
					DecayInterval:    manager.oneSlotDuration(),
					DecayToZero:      gossipnames.DecayToZero,
					RetainScore:      manager.oneEpochDuration(),
				}, &pubsub.PeerScoreThresholds{GossipThreshold: -4000, PublishThreshold: -8000, GraylistThreshold: -16000}),
				pubsub.WithEventTracer(tracer),
			)
			require.NoError(t, err)
			validator := manager.newPubsubValidator(&mockService{}, func(_ peer.ID, msg *pubsub.Message, _ clparams.StateVersion) bool {
				return msg.Data[0] != 0
			})
			require.NoError(t, ps.RegisterTopicValidator(topicName, validator))
			topic, err := ps.Join(topicName)
			require.NoError(t, err)
			require.NoError(t, topic.SetScoreParams(params))
			subscription, err := topic.Subscribe()
			require.NoError(t, err)
			t.Cleanup(subscription.Cancel)
			require.NoError(t, mn.LinkAll())
			require.NoError(t, mn.ConnectAllButSelf())
			stream, err := sender.NewStream(ctx, receiver.ID(), pubsub.GossipSubID_v11)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, stream.Close()) })
			select {
			case <-tracer.added:
			case <-ctx.Done():
				t.Fatal("peer was not added to the router")
			}
			writer := msgio.NewVarintWriter(stream)
			sendAndAwaitRejections := func(prefix byte, reason string) {
				messages := make([]*pb.Message, 16)
				for i := range messages {
					messages[i] = &pb.Message{Data: []byte{prefix, byte(i + 1)}, Topic: &topicName}
				}
				encoded, err := proto.Marshal(&pb.RPC{Publish: messages})
				require.NoError(t, err)
				require.NoError(t, writer.WriteMsg(encoded))
				for range messages {
					select {
					case got := <-tracer.rejections:
						require.Equal(t, reason, got)
					case <-ctx.Done():
						t.Fatal("gossip validation did not finish")
					}
				}
			}

			require.Equal(t, pubsub.AcceptAll, router.AcceptFrom(sender.ID()))
			sendAndAwaitRejections(0, pubsub.RejectValidationIgnored)
			require.Equal(t, pubsub.AcceptAll, router.AcceptFrom(sender.ID()))
			sendAndAwaitRejections(0x80, pubsub.RejectValidationFailed)
			require.Equal(t, pubsub.AcceptNone, router.AcceptFrom(sender.ID()))
		})
	}
}

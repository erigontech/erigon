package sentinel

import (
	"encoding/hex"
	"reflect"
	"time"

	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/proto"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/proto/p2p"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/proto/ssz_snappy"
	"github.com/ledgerwatch/log/v3"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

func (s *Sentinel) startGossip() error {
	prefix := "/eth2/4a26c58b"
	gsub, err := pubsub.NewGossipSub(s.ctx, s.host, s.pubsubOptions()...)
	if err != nil {
		return err
	}
	err = subscribeGossipTopic(s, gsub, prefix+"/beacon_block/ssz_snappy", ssz_snappy.NewSubCodec, s.handleBeaconBlockSubscription)
	if err != nil {
		return err
	}
	err = subscribeGossipTopic(s, gsub, prefix+"/light_client_finality_update/ssz_snappy", ssz_snappy.NewSubCodec, s.handleFinalitySubscription)
	if err != nil {
		return err
	}
	err = subscribeGossipTopic(s, gsub, prefix+"/light_client_optimistic_update/ssz_snappy", ssz_snappy.NewSubCodec, s.handleOptimisticSubscription)
	if err != nil {
		return err
	}
	return nil
}

func subscribeGossipTopic[T proto.Packet](
	s *Sentinel,
	gsub *pubsub.PubSub,
	topic string,
	newcodec func(*pubsub.Subscription) proto.SubCodec,
	fn func(ctx *proto.SubContext, v T) error,
) error {

	top, err := gsub.Join(topic)
	if err != nil {
		return err
	}
	sub_sub, err := top.Subscribe(pubsub.WithBufferSize(32))
	if err != nil {
		return err
	}
	go func() {
		for {
			log.Info("[Gossip] Topic Peers", "topic", topic, "peers", len(top.ListPeers()))
			time.Sleep(30 * time.Second)
		}
	}()
	go runSubscriptionHandler(s, sub_sub, newcodec, fn)
	return nil
}

// currySubHandler uses a func(ctx *proto.StreamContext, dat proto.Packet) error to handle a *pubsub.Subscription
// this allows us to write encoding non specific type safe handler without performance overhead
func runSubscriptionHandler[T proto.Packet](
	s *Sentinel,
	sub *pubsub.Subscription,
	newcodec func(*pubsub.Subscription) proto.SubCodec,
	fn func(ctx *proto.SubContext, v T) error,
) {
	sd := newcodec(sub)
	for {
		val := (*new(T)).Clone().(T)
		ctx, err := sd.Decode(s.ctx, val)
		if err != nil {
			log.Warn("fail to decode gossip packet", "err", err, "topic", ctx.Topic, "pkt", reflect.TypeOf(val))
			continue
		}
		if ctx.Msg.ReceivedFrom == s.host.ID() {
			continue
		}
		err = fn(ctx, val)
		if err != nil {
			log.Warn("failed handling gossip ", "err", err, "topic", ctx.Topic, "pkt", reflect.TypeOf(val))
			continue
		}
		log.Info("[Gossip] Received Subscription", "topic", ctx.Topic)
	}
}

func (s *Sentinel) handleFinalitySubscription(
	ctx *proto.SubContext,
	u *p2p.LightClientFinalityUpdate) error {
	s.state.AddFinalityUpdateEvent(u)
	return nil
}
func (s *Sentinel) handleOptimisticSubscription(
	ctx *proto.SubContext,
	u *p2p.LightClientOptimisticUpdate) error {
	s.state.AddOptimisticUpdateEvent(u)
	return nil
}
func (s *Sentinel) handleBeaconBlockSubscription(
	ctx *proto.SubContext,
	u *p2p.SignedBeaconBlockBellatrix) error {
	log.Info("[Gossip] beacon_block",
		"Slot", u.Block.Slot,
		"Signature", hex.EncodeToString(u.Signature[:]),
		"graffiti", string(u.Block.Body.Graffiti[:]),
		"eth1_blockhash", hex.EncodeToString(u.Block.Body.Eth1Data.BlockHash[:]),
		"stateRoot", hex.EncodeToString(u.Block.StateRoot[:]),
		"parentRoot", hex.EncodeToString(u.Block.ParentRoot[:]),
		"proposerIdx", u.Block.ProposerIndex,
	)
	return nil
}

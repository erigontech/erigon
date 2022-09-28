package sentinel

import (
	"reflect"

	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/proto"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/proto/p2p"
	"github.com/ledgerwatch/log/v3"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

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
		}
		log.Info("[Gossip] Received message", "topic", ctx.Topic, "msg", val)
	}
}

func (s *Sentinel) handleFinalitySubscription(ctx *proto.SubContext, u *p2p.LightClientFinalityUpdate) error {
	s.state.AddFinalityUpdateEvent(u)
	return nil
}
func (s *Sentinel) handleOptimisticSubscription(ctx *proto.SubContext, u *p2p.LightClientOptimisticUpdate) error {
	s.state.AddOptimisticUpdateEvent(u)
	return nil
}
func (s *Sentinel) handleBeaconBlockSubscription(ctx *proto.SubContext, u *proto.EmptyPacket) error {
	log.Info("got beacon block", "raw", ctx.Msg)
	return nil
}

package sentinel

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/proto"
	"github.com/ledgerwatch/log/v3"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

// GossipSubscription abstracts a gossip subscription to write decoded structs.
type GossipSubscription struct {
	p     proto.GossipCodec
	topic *pubsub.Topic
	sub   *pubsub.Subscription

	cf context.CancelFunc
	rf pubsub.RelayCancelFunc
}

// construct a gossip subsubscription
func NewGossipSubscription[T proto.Packet](
	s *Sentinel, // the sentinel to connect with
	topic string, // the topic
	ch chan *proto.GossipContext, // the channel to deliver packets to
	newcodec func(*pubsub.Subscription, *pubsub.Topic) proto.GossipCodec,
	opts ...pubsub.TopicOpt,
) (sub *GossipSubscription, err error) {
	sub = &GossipSubscription{}
	sub.topic, err = s.pubsub.Join(topic, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to begin topic %s subscription, err=%w", topic, err)
	}
	sub.sub, err = sub.topic.Subscribe()
	if err != nil {
		return nil, fmt.Errorf("failed to begin topic %s subscription, err=%w", topic, err)
	}
	sub.p = newcodec(sub.sub, sub.topic)

	sub.rf, err = sub.topic.Relay()
	if err != nil {
		return nil, fmt.Errorf("failed to begin relaying %s subcription, err=%w", topic, err)
	}

	var ctx context.Context
	ctx, sub.cf = context.WithCancel(s.ctx)
	go runGossipSubscription[T](ctx, s.host.ID(), sub, ch)
	return sub, nil
}

// calls the cancel func for the subscriber and closes the topic and sub
func (s *GossipSubscription) Close() {
	if s.cf != nil {
		s.cf()
	}
	if s.rf != nil {
		s.rf()
	}
	if s.sub != nil {
		s.sub.Cancel()
		s.sub = nil
	}
	if s.topic != nil {
		s.topic.Close()
		s.topic = nil
	}
}

// this is a helper to begin running the gossip subscription.
// function should not be used outside of the constructor for gossip subscription
func runGossipSubscription[T proto.Packet](
	ctx context.Context,
	host peer.ID,
	s *GossipSubscription,
	ch chan *proto.GossipContext,
) {
	var t T
	n := func() T {
		return t.Clone().(T)
	}
	for {
		select {
		case <-ctx.Done():
			return
		default:
			val := n()
			pctx, err := s.p.Decode(ctx, val)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				log.Warn("fail to decode gossip packet", "err", err, "topic", pctx.Topic, "pkt", reflect.TypeOf(t))
				continue
			}
			if pctx.Msg.GetFrom() == host {
				continue
			}
			ch <- pctx
		}
	}
}

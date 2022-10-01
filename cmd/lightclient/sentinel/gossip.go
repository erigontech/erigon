package sentinel

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/proto"
	"github.com/ledgerwatch/log/v3"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

// GossipSubscription abstracts a gossip subscription to write decoded structs.
type GossipSubscription struct {
	p     proto.SubCodec
	topic *pubsub.Topic
	sub   *pubsub.Subscription

	cf context.CancelFunc
}

// construct a gossip subsubscription
func NewGossipSubscription[T proto.Packet](
	s *Sentinel, // the sentinel to connect with
	topic string, // the topic
	ch chan *proto.SubContext, // the channel to deliver packets to
	newcodec func(*pubsub.Subscription) proto.SubCodec,
	opts ...pubsub.TopicOpt,
) (sub *GossipSubscription, err error) {
	sub = &GossipSubscription{}
	sub.topic, err = s.pubsub.Join(topic, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to begin topic %s subscription, err=%s", topic, err)
	}
	sub.sub, err = sub.topic.Subscribe()
	if err != nil {
		return nil, fmt.Errorf("failed to begin topic %s subscription, err=%s", topic, err)
	}
	sub.p = newcodec(sub.sub)
	s.host = s.host
	var ctx context.Context
	ctx, sub.cf = context.WithCancel(s.ctx)
	go runGossipSubscription[T](ctx, sub, ch)
	return sub, nil
}

// calls the cancel func for the subscriber and closes the topic and sub
func (s *GossipSubscription) Close() {
	if s.cf != nil {
		s.cf()
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
	s *GossipSubscription,
	ch chan *proto.SubContext,
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
			ch <- pctx
		}
	}
}

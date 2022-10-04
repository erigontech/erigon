package sentinel

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/proto"
	"github.com/ledgerwatch/log/v3"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

// GossipSubscription abstracts a gossip subscription to write decoded structs.
type GossipSubscription struct {
	gossip_topic GossipTopic
	host         peer.ID
	ch           chan *proto.GossipContext
	ctx          context.Context

	p     proto.GossipCodec
	topic *pubsub.Topic
	sub   *pubsub.Subscription

	cf context.CancelFunc
	rf pubsub.RelayCancelFunc

	setup sync.Once
}

func (sub *GossipSubscription) Listen() (err error) {
	sub.setup.Do(func() {
		sub.sub, err = sub.topic.Subscribe()
		if err != nil {
			err = fmt.Errorf("failed to begin topic %s subscription, err=%w", sub.topic.String(), err)
			return
		}
		sub.p = sub.gossip_topic.Codec(sub.sub, sub.topic)
		var sctx context.Context
		sctx, sub.cf = context.WithCancel(sub.ctx)
		go sub.run(sctx)
	})
	return nil
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
func (s *GossipSubscription) run(ctx context.Context) {
	for {
		s.do(ctx)
	}
}

func (s *GossipSubscription) do(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("[Gossip] Message Handler Crashed", "err", r)
		}
	}()
	select {
	case <-ctx.Done():
		return
	default:
		val := s.gossip_topic.Typ.Clone()
		pctx, err := s.p.Decode(ctx, val)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Warn("fail to decode gossip packet", "err", err, "topic", pctx.Topic, "pkt", reflect.TypeOf(val))
			return
		}
		if pctx.Msg.GetFrom() == s.host {
			return
		}
		s.ch <- pctx
	}
}

/*
   Copyright 2022 Erigon-Lightclient contributors
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package sentinel

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/log/v3"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

// GossipSubscription abstracts a gossip subscription to write decoded structs.
type GossipSubscription struct {
	gossip_topic GossipTopic
	host         peer.ID
	ch           chan *pubsub.Message
	ctx          context.Context

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
		var sctx context.Context
		sctx, sub.cf = context.WithCancel(sub.ctx)
		go sub.run(sctx, sub.sub, sub.sub.Topic())
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
func (s *GossipSubscription) run(ctx context.Context, sub *pubsub.Subscription, topic string) {
	for {
		s.do(ctx, sub, topic)
	}
}

func (s *GossipSubscription) do(ctx context.Context, sub *pubsub.Subscription, topic string) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("[Sentinel Gossip] Message Handler Crashed", "err", r)
		}
	}()
	select {
	case <-ctx.Done():
		return
	default:
		msg, err := sub.Next(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Warn("[Sentinel] fail to decode gossip packet", "err", err, "topic", topic)
			return
		}
		if msg.GetFrom() == s.host {
			return
		}
		if err := s.topic.Publish(ctx, common.CopyBytes(msg.Data)); err != nil {
			return
		}
		s.ch <- msg
	}
}

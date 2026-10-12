// Copyright 2022 The Erigon Authors
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

package sentinel

import (
	"context"
	"sync"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

type GossipTopic struct {
	Name     string
	CodecStr string
}

type GossipManager struct {
	ch            chan *GossipMessage
	subscriptions sync.Map // map from topic string to *GossipSubscription
}

const maxIncomingGossipMessages = 1 << 16

// construct a new gossip manager that will handle packets with the given handlerfunc
func NewGossipManager(
	ctx context.Context,
) *GossipManager {
	g := &GossipManager{
		ch:            make(chan *GossipMessage, maxIncomingGossipMessages),
		subscriptions: sync.Map{},
	}
	return g
}

func (g *GossipManager) Recv() <-chan *GossipMessage {
	return g.ch
}

func (s *Sentinel) SubscribeGossip(topic GossipTopic, expiration time.Time, opts ...pubsub.TopicOpt) (sub *GossipSubscription, err error) {
	panic("do not call this")
}

func (s *Sentinel) Unsubscribe(topic GossipTopic, opts ...pubsub.TopicOpt) (err error) {
	panic("do not call this")
}

func (g *GossipManager) Close() {
	g.subscriptions.Range(func(key, value any) bool {
		if value != nil {
			value.(*GossipSubscription).Close()
		}
		return true
	})
}

// GossipSubscription abstracts a gossip subscription to write decoded structs.
type GossipSubscription struct {
	topic *pubsub.Topic
	sub   *pubsub.Subscription

	cf context.CancelFunc
	rf pubsub.RelayCancelFunc

	stopCh    chan struct{}
	closeOnce sync.Once
	lock      sync.Mutex
}

func (g *GossipSubscription) OverwriteSubscriptionExpiry(expiry time.Time) {
	panic("do not call this")
}

// calls the cancel func for the subscriber and closes the topic and sub
func (g *GossipSubscription) Close() {
	g.lock.Lock()
	defer g.lock.Unlock()
	g.closeOnce.Do(func() {
		if g.stopCh != nil {
			close(g.stopCh)
		}
		if g.cf != nil {
			g.cf()
		}
		if g.rf != nil {
			g.rf()
		}
		if g.sub != nil {
			g.sub.Cancel()
			g.sub = nil
		}
		if g.topic != nil {
			g.topic.Close()
			g.topic = nil
		}
	})
}

type GossipMessage struct {
	From      peer.ID
	TopicName string
	Data      []byte
}

func (g *GossipSubscription) Publish(data []byte) error {
	panic("do not call this")
}

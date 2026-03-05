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
	"sync/atomic"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

const SSZSnappyCodec = "ssz_snappy"

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

func (s *GossipManager) Recv() <-chan *GossipMessage {
	return s.ch
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
	gossip_topic GossipTopic
	host         peer.ID
	ch           chan *GossipMessage
	ctx          context.Context
	expiration   atomic.Value // Unix nano for how much we should listen to this topic
	subscribed   atomic.Bool

	topic *pubsub.Topic
	sub   *pubsub.Subscription

	cf context.CancelFunc
	rf pubsub.RelayCancelFunc

	s *Sentinel

	stopCh    chan struct{}
	closeOnce sync.Once
	lock      sync.Mutex
}

func (sub *GossipSubscription) OverwriteSubscriptionExpiry(expiry time.Time) {
	panic("do not call this")
}

// calls the cancel func for the subscriber and closes the topic and sub
func (s *GossipSubscription) Close() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.closeOnce.Do(func() {
		if s.stopCh != nil {
			close(s.stopCh)
		}
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

package sentinel

import (
	"fmt"
	"time"

	"github.com/ledgerwatch/log/v3"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

const (
	// overlay parameters
	gossipSubD   = 8  // topic stable mesh target count
	gossipSubDlo = 6  // topic stable mesh low watermark
	gossipSubDhi = 12 // topic stable mesh high watermark

	// gossip parameters
	gossipSubMcacheLen    = 6   // number of windows to retain full messages in cache for `IWANT` responses
	gossipSubMcacheGossip = 3   // number of windows to gossip about
	gossipSubSeenTTL      = 550 // number of heartbeat intervals to retain message IDs

	// fanout ttl
	gossipSubFanoutTTL = 60000000000 // TTL for fanout maps for topics we are not subscribed to but have published to, in nano seconds

	// heartbeat interval
	gossipSubHeartbeatInterval = 700 * time.Millisecond // frequency of heartbeat, milliseconds

	// misc
	rSubD = 8 // random gossip target
)

// Specifies the prefix for any pubsub topic.
const gossipTopicPrefix = "/eth2/"
const blockSubnetTopicFormat = "/eth2/%x/beacon_block"

// begings the topic subscription
func (s *Sentinel) BeginSubscription(topic string, opt ...pubsub.SubOpt) error {
	s.runningSubscriptionsLock.Lock()
	defer s.runningSubscriptionsLock.Unlock()

	topicHandle, err := s.AddToTopic(topic)
	if err != nil {
		return fmt.Errorf("failed to begin topic %s subscription, err=%s", topic, err)
	}
	if topicHandle == nil {
		return fmt.Errorf("failed to get topic handle while subscribing")
	}

	subscription, err := topicHandle.Subscribe()
	if err != nil {
		return fmt.Errorf("failed to begin topic %s subscription, err=%s", topic, err)
	}

	if _, ok := s.runningSubscriptions[subscription.Topic()]; !ok {
		s.runningSubscriptions[subscription.Topic()] = subscription
		log.Info("[Gossip] began subscription", "topic", subscription.Topic())
		go s.beginTopicListening(*subscription)
	}

	return nil
}

// Add the topics to the sentinel
func (s *Sentinel) AddToTopic(topic string, opts ...pubsub.TopicOpt) (topicHandle *pubsub.Topic, err error) {
	s.subscribedTopicLock.Lock()
	defer s.subscribedTopicLock.Unlock()

	// Join topics if we have alredy not joined them
	if _, ok := s.subscribedTopics[topic]; !ok {
		topicHandle, err = s.pubsub.Join(topic, opts...)
		if err != nil {
			return nil, fmt.Errorf("failed to join topic %s, error=%s", topic, err)
		}

		s.subscribedTopics[topic] = topicHandle
		log.Info("[Gossip] joined", "topic", topic)
	} else {
		topicHandle = s.subscribedTopics[topic]
	}

	return topicHandle, nil
}

// unsubscribes from topics
func (s *Sentinel) UnsubscribeToTopic(topic string) error {
	s.subscribedTopicLock.Lock()
	defer s.subscribedTopicLock.Unlock()

	s.runningSubscriptionsLock.Lock()
	defer s.runningSubscriptionsLock.Unlock()

	// unsubscribe from topic of we are subscribe to it
	if topicHandler, ok := s.subscribedTopics[topic]; ok {
		if err := topicHandler.Close(); err != nil {
			return fmt.Errorf("failed to unsubscribe from topic %s, error=%s", topic, err)
		}
		delete(s.runningSubscriptions, topic)
		delete(s.subscribedTopics, topic)
	}

	return nil
}

func (s *Sentinel) beginTopicListening(subscription pubsub.Subscription) {
	log.Info("[Gossip] began listening to subscription", "topic", subscription.Topic())
	for _, ok := s.runningSubscriptions[subscription.Topic()]; ok; _, ok = s.runningSubscriptions[subscription.Topic()] {
		select {
		case <-s.ctx.Done():
			break
		default:
		}
		msg, err := subscription.Next(s.ctx)
		if err != nil {
			log.Warn("Failed to read message", "topic", subscription.Topic(), "err", err)
		}

		log.Info("[Gossip] received message", "topic", subscription.Topic(), "message", msg)

	}
}

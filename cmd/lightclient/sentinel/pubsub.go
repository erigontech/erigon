package sentinel

import (
	"fmt"
	"time"

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

	topicHandle, err := s.SubscribeToTopic(topic)
	if err != nil {
		return fmt.Errorf("failed to begin topic %s subscription, err=%s", topic, err)
	}

	subscription, err := topicHandle.Subscribe(opt...)
	if err != nil {
		return fmt.Errorf("failed to begin topic %s subscription, err=%s", topic, err)
	}

	s.runningSubscriptions[subscription.Topic()] = subscription
	return nil
}

// subscribes to topics that the sentinel is not subscribe to
func (s *Sentinel) SubscribeToTopic(topic string, opts ...pubsub.TopicOpt) (topicHandle *pubsub.Topic, err error) {
	s.subscribedTopicLock.Lock()
	defer s.subscribedTopicLock.Unlock()

	// Join topics if we have alredy not joined them
	if _, ok := s.subscribedTopics[topic]; !ok {
		topicHandle, err = s.pubsub.Join(topic, opts...)
		if err != nil {
			return nil, fmt.Errorf("failed to join topic %s, error=%s", topic, err)
		}

		s.subscribedTopics[topic] = topicHandle
	} else {
		topicHandle = s.subscribedTopics[topic]
	}

	return nil, nil
}

// unsubscribes from topics
func (s *Sentinel) UnsubscribeToTopic(topic string) error {
	s.subscribedTopicLock.Lock()
	defer s.subscribedTopicLock.Unlock()

	// unsubscribe from topic of we are subscribe to it
	if topicHandler, ok := s.subscribedTopics[topic]; ok {
		if err := topicHandler.Close(); err != nil {
			return fmt.Errorf("failed to unsubscribe from topic %s, error=%s", topic, err)
		}
		delete(s.subscribedTopics, topic)
	}

	return nil
}

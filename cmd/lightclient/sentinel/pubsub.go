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

func (s *Sentinel) beginSubscriptions() error {
	return nil
}

func (s *Sentinel) subscribeToTopic(topic string, opts ...pubsub.TopicOpt) error {
	s.subscribedTopicLock.Lock()
	defer s.subscribedTopicLock.Unlock()

	// Join topics if we have alredy not joined them
	if _, ok := s.subscribedTopics[topic]; !ok {
		topicHandle, err := s.pubsub.Join(topic, opts...)
		if err != nil {
			return fmt.Errorf("failed to join topic %s, error=%s", topic, err)
		}

		s.subscribedTopics[topic] = topicHandle
	}

	return nil
}

func (s *Sentinel) unsubscribeToTopic(topic string) error {
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

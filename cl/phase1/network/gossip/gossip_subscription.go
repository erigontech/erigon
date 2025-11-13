package gossip

import (
	"errors"
	"sync"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

type TopicSubscription struct {
	topic      *pubsub.Topic
	sub        *pubsub.Subscription
	expiration time.Time
	validator  pubsub.ValidatorEx
}

type TopicSubscriptions struct {
	subs  map[string]*TopicSubscription
	mutex sync.RWMutex
}

func NewTopicSubscriptions() *TopicSubscriptions {
	s := &TopicSubscriptions{
		subs:  make(map[string]*TopicSubscription),
		mutex: sync.RWMutex{},
	}
	go s.expireCheck()
	return s
}

func (t *TopicSubscriptions) AllTopics() []string {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	topics := make([]string, 0, len(t.subs))
	for topic := range t.subs {
		topics = append(topics, topic)
	}
	return topics
}

func (t *TopicSubscriptions) Get(topic string) *TopicSubscription {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.subs[topic]
}

func (t *TopicSubscriptions) Add(topic string, topicHandle *pubsub.Topic, validator pubsub.ValidatorEx) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if _, ok := t.subs[topic]; ok {
		return errors.New("topic already exists")
	}
	t.subs[topic] = &TopicSubscription{
		topic:      topicHandle,
		sub:        nil,
		validator:  validator,
		expiration: time.Unix(0, 0),
	}
	return nil
}

func (t *TopicSubscriptions) Remove(topic string) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	sub, ok := t.subs[topic]
	if !ok {
		return errors.New("topic not found")
	}
	if sub.sub != nil {
		sub.sub.Cancel()
		sub.sub = nil
	}
	sub.topic.Close()
	sub.topic = nil
	delete(t.subs, topic)
	return nil
}

func (t *TopicSubscriptions) Unsubscribe(topic string) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	sub, ok := t.subs[topic]
	if !ok {
		return errors.New("topic not found")
	}
	if sub.sub != nil {
		sub.sub.Cancel()
		sub.sub = nil
	}
	sub.expiration = time.Unix(0, 0) // reset
	return nil
}

func (t *TopicSubscriptions) SubscribeWithExpiry(topic string, expiration time.Time) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	sub, ok := t.subs[topic]
	if !ok {
		return errors.New("topic not found")
	}
	if time.Now().Before(expiration) {
		if sub.sub == nil {
			// subscribe the topic
			s, err := sub.topic.Subscribe()
			if err != nil {
				return err
			}
			sub.sub = s
		}
	} else {
		if sub.sub != nil {
			// unsubscribe the topic
			sub.sub.Cancel()
			sub.sub = nil
		}
	}
	sub.expiration = expiration
	return nil
}

func (t *TopicSubscriptions) expireCheck() {
	ticker := time.NewTicker(12 * time.Second)
	for range ticker.C {
		t.mutex.Lock()
		for _, sub := range t.subs {
			if time.Now().After(sub.expiration) && sub.sub != nil {
				sub.sub.Cancel()
				sub.sub = nil
			}
		}
		t.mutex.Unlock()
	}
}

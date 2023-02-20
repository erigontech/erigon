package sentinel

import (
	"testing"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/stretchr/testify/require"
)

func NewMockGossipManager() *GossipManager {
	subs := map[string]*GossipSubscription{
		"topic1": {
			host: "host1",
		},
		"topic3": {
			host: "host2",
		},
		"topic2": {
			host: "host3",
		},
	}

	return &GossipManager{
		ch:            make(chan *pubsub.Message, 1),
		subscriptions: subs,
	}
}

func TestCloseTopic(t *testing.T) {
	gm := NewMockGossipManager()

	testCases := []struct {
		topic string
	}{
		{"topic1"},
		{"topic2"},
	}

	for _, testCase := range testCases {
		// check that it has the topic before it closes it
		if _, ok := gm.subscriptions[testCase.topic]; !ok {
			t.Errorf("attempted to close invalid topic")
		}

		gm.CloseTopic(testCase.topic)

		// check that topic has been deleted after closing
		if _, ok := gm.subscriptions[testCase.topic]; ok {
			t.Errorf("closed topic but topic still exists")
		}
	}
}

func TestGetSubscription(t *testing.T) {
	gm := NewMockGossipManager()

	testCases := []struct {
		topic        string
		expectedSub  *GossipSubscription
		expectedBool bool
	}{
		{
			topic:        "topic1",
			expectedSub:  gm.subscriptions["topic1"],
			expectedBool: true,
		},
		{
			topic:        "topic2",
			expectedSub:  gm.subscriptions["topic2"],
			expectedBool: true,
		},
		{
			topic:        "topic4",
			expectedSub:  nil,
			expectedBool: false,
		},
	}

	for _, testCase := range testCases {
		subscription, ok := gm.GetSubscription(testCase.topic)
		require.EqualValues(t, subscription, testCase.expectedSub)
		require.EqualValues(t, ok, testCase.expectedBool)
	}
}

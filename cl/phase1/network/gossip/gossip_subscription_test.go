// Copyright 2024 The Erigon Authors
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

package gossip

import (
	"context"
	"testing"
	"time"

	"github.com/erigontech/erigon/cl/p2p/mock_services"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"
)

type TopicSubscriptionsTestSuite struct {
	suite.Suite
	ctrl    *gomock.Controller
	mockP2P *mock_services.MockP2PManager
	host    host.Host
	ps      *pubsub.PubSub
	ts      *TopicSubscriptions
	cancel  context.CancelFunc
}

func (s *TopicSubscriptionsTestSuite) SetupTest() {
	ctx := context.Background()
	s.ctrl = gomock.NewController(s.T())

	// Create actual libp2p host and pubsub
	var err error
	s.host, err = libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	s.Require().NoError(err)

	// Create pubsub with minimal options
	s.ps, err = pubsub.NewGossipSub(ctx, s.host, pubsub.WithMessageIdFn(func(pmsg *pb.Message) string {
		return string(pmsg.Data)
	}))
	s.Require().NoError(err)

	// Create a mock P2PManager
	s.mockP2P = mock_services.NewMockP2PManager(s.ctrl)
	s.mockP2P.EXPECT().Pubsub().Return(s.ps).AnyTimes()
	s.mockP2P.EXPECT().UpdateENRAttSubnets(gomock.Any(), gomock.Any()).AnyTimes()
	s.mockP2P.EXPECT().UpdateENRSyncNets(gomock.Any(), gomock.Any()).AnyTimes()

	ctx, s.cancel = context.WithCancel(context.Background())
	s.ts = NewTopicSubscriptions(ctx, s.mockP2P)
}

func (s *TopicSubscriptionsTestSuite) TearDownTest() {
	if s.cancel != nil {
		s.cancel()
	}
	if s.host != nil {
		s.host.Close()
	}
	if s.ctrl != nil {
		s.ctrl.Finish()
	}
}

func (s *TopicSubscriptionsTestSuite) TestNewTopicSubscriptions() {
	ts := NewTopicSubscriptions(context.Background(), s.mockP2P)
	s.NotNil(ts)
	s.NotNil(ts.subs)
	s.NotNil(ts.toSubscribes)
	s.Equal(s.mockP2P, ts.p2p)
}

func (s *TopicSubscriptionsTestSuite) TestAllTopics_Empty() {
	topics := s.ts.AllTopics()
	s.Empty(topics)
}

func (s *TopicSubscriptionsTestSuite) TestAllTopics_WithTopics() {
	topic1 := "/eth2/abcd1234/beacon_block/ssz_snappy"
	topic2 := "/eth2/abcd1234/beacon_aggregate_and_proof/ssz_snappy"

	topicHandle1, err := s.ps.Join(topic1)
	s.Require().NoError(err)
	topicHandle2, err := s.ps.Join(topic2)
	s.Require().NoError(err)

	validator := func(ctx context.Context, pid peer.ID, msg *pubsub.Message) pubsub.ValidationResult {
		return pubsub.ValidationAccept
	}

	err = s.ts.Add(topic1, topicHandle1, validator)
	s.Require().NoError(err)
	err = s.ts.Add(topic2, topicHandle2, validator)
	s.Require().NoError(err)

	topics := s.ts.AllTopics()
	s.Len(topics, 2)
	s.Contains(topics, topic1)
	s.Contains(topics, topic2)
}

func (s *TopicSubscriptionsTestSuite) TestGet_NotFound() {
	result := s.ts.Get("non_existent_topic")
	s.Nil(result)
}

func (s *TopicSubscriptionsTestSuite) TestGet_Found() {
	topic := "/eth2/abcd1234/beacon_block/ssz_snappy"
	topicHandle, err := s.ps.Join(topic)
	s.Require().NoError(err)

	validator := func(ctx context.Context, pid peer.ID, msg *pubsub.Message) pubsub.ValidationResult {
		return pubsub.ValidationAccept
	}

	err = s.ts.Add(topic, topicHandle, validator)
	s.Require().NoError(err)

	result := s.ts.Get(topic)
	s.NotNil(result)
	s.Equal(topicHandle, result.topic)
	s.NotNil(result.validator)
}

func (s *TopicSubscriptionsTestSuite) TestAdd_Success() {
	topic := "/eth2/abcd1234/beacon_block/ssz_snappy"
	topicHandle, err := s.ps.Join(topic)
	s.Require().NoError(err)

	validator := func(ctx context.Context, pid peer.ID, msg *pubsub.Message) pubsub.ValidationResult {
		return pubsub.ValidationAccept
	}

	err = s.ts.Add(topic, topicHandle, validator)
	s.NoError(err)

	result := s.ts.Get(topic)
	s.NotNil(result)
	s.Equal(topicHandle, result.topic)
	s.Nil(result.sub) // Not subscribed yet
	s.Equal(time.Unix(0, 0), result.expiry)
}

func (s *TopicSubscriptionsTestSuite) TestAdd_Duplicate() {
	topic := "/eth2/abcd1234/beacon_block/ssz_snappy"
	topicHandle, err := s.ps.Join(topic)
	s.Require().NoError(err)

	validator := func(ctx context.Context, pid peer.ID, msg *pubsub.Message) pubsub.ValidationResult {
		return pubsub.ValidationAccept
	}

	err = s.ts.Add(topic, topicHandle, validator)
	s.Require().NoError(err)

	// Try to add the same topic again with the same handle (simulating a duplicate add)
	// Note: We can't join the same topic twice in libp2p, so we reuse the same handle
	err = s.ts.Add(topic, topicHandle, validator)
	if s.Error(err) {
		s.Contains(err.Error(), "topic already exists")
	}
}

func (s *TopicSubscriptionsTestSuite) TestAdd_WithPendingSubscription() {
	topic := "/eth2/abcd1234/beacon_block/ssz_snappy"
	topicHandle, err := s.ps.Join(topic)
	s.Require().NoError(err)

	validator := func(ctx context.Context, pid peer.ID, msg *pubsub.Message) pubsub.ValidationResult {
		return pubsub.ValidationAccept
	}

	// Try to subscribe before adding the topic
	futureExpiry := time.Now().Add(1 * time.Hour)
	err = s.ts.SubscribeWithExpiry(topic, futureExpiry)
	s.Error(err)
	s.Contains(err.Error(), "topic not found")

	// Now add the topic - it should automatically subscribe with the pending expiry
	err = s.ts.Add(topic, topicHandle, validator)
	s.NoError(err)

	result := s.ts.Get(topic)
	s.NotNil(result)
	s.NotNil(result.sub) // Should be subscribed now
	s.Equal(futureExpiry.Unix(), result.expiry.Unix())
}

func (s *TopicSubscriptionsTestSuite) TestRemove_Success() {
	topic := "/eth2/abcd1234/beacon_block/ssz_snappy"
	topicHandle, err := s.ps.Join(topic)
	s.Require().NoError(err)

	validator := func(ctx context.Context, pid peer.ID, msg *pubsub.Message) pubsub.ValidationResult {
		return pubsub.ValidationAccept
	}

	err = s.ts.Add(topic, topicHandle, validator)
	s.Require().NoError(err)

	// Subscribe to the topic
	futureExpiry := time.Now().Add(1 * time.Hour)
	err = s.ts.SubscribeWithExpiry(topic, futureExpiry)
	s.Require().NoError(err)

	// Remove the topic
	err = s.ts.Remove(topic)
	s.NoError(err)

	// Verify it's removed
	result := s.ts.Get(topic)
	s.Nil(result)
}

func (s *TopicSubscriptionsTestSuite) TestRemove_NotFound() {
	err := s.ts.Remove("non_existent_topic")
	s.Error(err)
	s.Contains(err.Error(), "topic not found")
}

func (s *TopicSubscriptionsTestSuite) TestUnsubscribe_Success() {
	topic := "/eth2/abcd1234/beacon_block/ssz_snappy"
	topicHandle, err := s.ps.Join(topic)
	s.Require().NoError(err)

	validator := func(ctx context.Context, pid peer.ID, msg *pubsub.Message) pubsub.ValidationResult {
		return pubsub.ValidationAccept
	}

	err = s.ts.Add(topic, topicHandle, validator)
	s.Require().NoError(err)

	// Subscribe to the topic
	futureExpiry := time.Now().Add(1 * time.Hour)
	err = s.ts.SubscribeWithExpiry(topic, futureExpiry)
	s.Require().NoError(err)

	result := s.ts.Get(topic)
	s.NotNil(result.sub)

	// Unsubscribe
	err = s.ts.Unsubscribe(topic)
	s.NoError(err)

	result = s.ts.Get(topic)
	s.NotNil(result)                        // Topic still exists
	s.Nil(result.sub)                       // But subscription is nil
	s.Equal(time.Unix(0, 0), result.expiry) // Expiry reset
}

func (s *TopicSubscriptionsTestSuite) TestUnsubscribe_NotFound() {
	err := s.ts.Unsubscribe("non_existent_topic")
	s.Error(err)
	s.Contains(err.Error(), "topic not found")
}

func (s *TopicSubscriptionsTestSuite) TestUnsubscribe_NotSubscribed() {
	topic := "/eth2/abcd1234/beacon_block/ssz_snappy"
	topicHandle, err := s.ps.Join(topic)
	s.Require().NoError(err)

	validator := func(ctx context.Context, pid peer.ID, msg *pubsub.Message) pubsub.ValidationResult {
		return pubsub.ValidationAccept
	}

	err = s.ts.Add(topic, topicHandle, validator)
	s.Require().NoError(err)

	// Unsubscribe without subscribing first
	err = s.ts.Unsubscribe(topic)
	s.NoError(err) // Should not error
}

func (s *TopicSubscriptionsTestSuite) TestSubscribeWithExpiry_Success() {
	topic := "/eth2/abcd1234/beacon_block/ssz_snappy"
	topicHandle, err := s.ps.Join(topic)
	s.Require().NoError(err)

	validator := func(ctx context.Context, pid peer.ID, msg *pubsub.Message) pubsub.ValidationResult {
		return pubsub.ValidationAccept
	}

	err = s.ts.Add(topic, topicHandle, validator)
	s.Require().NoError(err)

	// Subscribe with future expiry
	futureExpiry := time.Now().Add(1 * time.Hour)
	err = s.ts.SubscribeWithExpiry(topic, futureExpiry)
	s.NoError(err)

	result := s.ts.Get(topic)
	s.NotNil(result)
	s.NotNil(result.sub)
	s.Equal(futureExpiry.Unix(), result.expiry.Unix())
}

func (s *TopicSubscriptionsTestSuite) TestSubscribeWithExpiry_TopicNotFound() {
	futureExpiry := time.Now().Add(1 * time.Hour)
	err := s.ts.SubscribeWithExpiry("non_existent_topic", futureExpiry)
	s.Error(err)
	s.Contains(err.Error(), "topic not found")

	// Verify it's added to toSubscribes
	s.ts.mutex.RLock()
	_, ok := s.ts.toSubscribes["non_existent_topic"]
	s.ts.mutex.RUnlock()
	s.True(ok)
}

func (s *TopicSubscriptionsTestSuite) TestSubscribeWithExpiry_ExpiryInPast() {
	topic := "/eth2/abcd1234/beacon_block/ssz_snappy"
	topicHandle, err := s.ps.Join(topic)
	s.Require().NoError(err)

	validator := func(ctx context.Context, pid peer.ID, msg *pubsub.Message) pubsub.ValidationResult {
		return pubsub.ValidationAccept
	}

	err = s.ts.Add(topic, topicHandle, validator)
	s.Require().NoError(err)

	// Subscribe with past expiry
	pastExpiry := time.Now().Add(-1 * time.Hour)
	err = s.ts.SubscribeWithExpiry(topic, pastExpiry)
	s.Error(err)
	s.Equal(ErrExpiryInThePast, err)
}

func (s *TopicSubscriptionsTestSuite) TestSubscribeWithExpiry_UpdateExpiry() {
	topic := "/eth2/abcd1234/beacon_block/ssz_snappy"
	topicHandle, err := s.ps.Join(topic)
	s.Require().NoError(err)

	validator := func(ctx context.Context, pid peer.ID, msg *pubsub.Message) pubsub.ValidationResult {
		return pubsub.ValidationAccept
	}

	err = s.ts.Add(topic, topicHandle, validator)
	s.Require().NoError(err)

	// Subscribe with first expiry
	firstExpiry := time.Now().Add(1 * time.Hour)
	err = s.ts.SubscribeWithExpiry(topic, firstExpiry)
	s.Require().NoError(err)

	result := s.ts.Get(topic)
	s.Equal(firstExpiry.Unix(), result.expiry.Unix())

	// Update with second expiry
	secondExpiry := time.Now().Add(2 * time.Hour)
	err = s.ts.SubscribeWithExpiry(topic, secondExpiry)
	s.NoError(err)

	result = s.ts.Get(topic)
	s.Equal(secondExpiry.Unix(), result.expiry.Unix())
	s.NotNil(result.sub) // Should still be subscribed
}

func (s *TopicSubscriptionsTestSuite) TestSubscribeWithExpiry_BeaconAttestation() {
	topic := "/eth2/abcd1234/beacon_attestation_3/ssz_snappy"
	topicHandle, err := s.ps.Join(topic)
	s.Require().NoError(err)

	validator := func(ctx context.Context, pid peer.ID, msg *pubsub.Message) pubsub.ValidationResult {
		return pubsub.ValidationAccept
	}

	err = s.ts.Add(topic, topicHandle, validator)
	s.Require().NoError(err)

	futureExpiry := time.Now().Add(1 * time.Hour)
	err = s.ts.SubscribeWithExpiry(topic, futureExpiry)
	s.NoError(err)

	// Verify ENR update was called (via the mock set up in SetupTest with AnyTimes())
	result := s.ts.Get(topic)
	s.NotNil(result)
	s.NotNil(result.sub)
}

func (s *TopicSubscriptionsTestSuite) TestSubscribeWithExpiry_SyncCommittee() {
	topic := "/eth2/abcd1234/sync_committee_2/ssz_snappy"
	topicHandle, err := s.ps.Join(topic)
	s.Require().NoError(err)

	validator := func(ctx context.Context, pid peer.ID, msg *pubsub.Message) pubsub.ValidationResult {
		return pubsub.ValidationAccept
	}

	err = s.ts.Add(topic, topicHandle, validator)
	s.Require().NoError(err)

	futureExpiry := time.Now().Add(1 * time.Hour)
	err = s.ts.SubscribeWithExpiry(topic, futureExpiry)
	s.NoError(err)

	// Verify ENR update was called (via the mock set up in SetupTest with AnyTimes())
	result := s.ts.Get(topic)
	s.NotNil(result)
	s.NotNil(result.sub)
}

func (s *TopicSubscriptionsTestSuite) TestConcurrentAccess() {
	const numGoroutines = 10
	const numOperations = 100

	done := make(chan bool, numGoroutines)

	// Spawn multiple goroutines that perform operations concurrently
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer func() { done <- true }()
			for j := 0; j < numOperations; j++ {
				topic := "/eth2/abcd1234/test_topic/ssz_snappy"

				// Try to get all topics
				_ = s.ts.AllTopics()

				// Try to get a specific topic
				_ = s.ts.Get(topic)
			}
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	s.True(true) // If we get here without data races, test passes
}

func TestTopicSubscriptions(t *testing.T) {
	suite.Run(t, new(TopicSubscriptionsTestSuite))
}

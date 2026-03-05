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
	"errors"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/p2p/mock_services"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/metrics"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"
)

// mockService is a mock implementation of serviceintf.Service[any]
type mockService struct {
	decodeFunc  func(peer.ID, []byte, clparams.StateVersion) (any, error)
	processFunc func(context.Context, *uint64, any) error
	namesFunc   func() []string
}

func (m *mockService) Names() []string {
	if m.namesFunc != nil {
		return m.namesFunc()
	}
	return []string{"test_topic"}
}

func (m *mockService) DecodeGossipMessage(pid peer.ID, data []byte, version clparams.StateVersion) (any, error) {
	if m.decodeFunc != nil {
		return m.decodeFunc(pid, data, version)
	}
	return "decoded_message", nil
}

func (m *mockService) ProcessMessage(ctx context.Context, subnet *uint64, msg any) error {
	if m.processFunc != nil {
		return m.processFunc(ctx, subnet, msg)
	}
	return nil
}

// createMockMessage creates a mock pubsub.Message
func createMockMessage(topic string, data []byte) *pubsub.Message {
	return &pubsub.Message{
		Message: &pb.Message{
			Topic: &topic,
			Data:  data,
		},
	}
}

type newPubsubValidatorTestSuite struct {
	suite.Suite
	gm        *GossipManager
	ctrl      *gomock.Controller
	mockP2P   *mock_services.MockP2PManager
	mockClock *eth_clock.MockEthereumClock
}

// SetupTest is called before each test method
func (s *newPubsubValidatorTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())
	s.mockP2P = mock_services.NewMockP2PManager(s.ctrl)
	s.mockClock = eth_clock.NewMockEthereumClock(s.ctrl)

	beaconConfig := &clparams.BeaconChainConfig{
		SlotsPerEpoch:  32,
		SecondsPerSlot: 12,
	}
	networkConfig := &clparams.NetworkConfig{}

	// Setup mock expectations
	s.mockClock.EXPECT().GetCurrentEpoch().Return(uint64(10)).AnyTimes()
	s.mockClock.EXPECT().GetCurrentSlot().Return(uint64(320)).AnyTimes()
	s.mockClock.EXPECT().CurrentForkDigest().Return(common.Bytes4{0xab, 0xcd, 0x12, 0x34}, nil).AnyTimes()
	s.mockClock.EXPECT().GetEpochAtSlot(gomock.Any()).DoAndReturn(func(slot uint64) uint64 {
		return slot / beaconConfig.SlotsPerEpoch
	}).AnyTimes()
	s.mockClock.EXPECT().ComputeForkDigest(gomock.Any()).Return(common.Bytes4{0xab, 0xcd, 0x12, 0x34}, nil).AnyTimes()
	s.mockP2P.EXPECT().BandwidthCounter().Return(nil).AnyTimes()
	s.mockP2P.EXPECT().Host().Return(nil).AnyTimes()

	s.gm = NewGossipManager(
		context.Background(),
		s.mockP2P,
		beaconConfig,
		networkConfig,
		s.mockClock,
		false,                        // subscribeAll
		0,                            // activeIndicies
		datasize.ByteSize(1024*1024), // maxInboundTrafficPerPeer
		datasize.ByteSize(1024*1024), // maxOutboundTrafficPerPeer
		false,                        // adaptableTrafficRequirements
	)
}

// TearDownTest is called after each test method
func (s *newPubsubValidatorTestSuite) TearDownTest() {
	if s.gm != nil {
		s.gm.Close()
	}
	if s.ctrl != nil {
		s.ctrl.Finish()
	}
}

func (s *newPubsubValidatorTestSuite) TestNewPubsubValidator_EmptyTopic() {
	service := &mockService{}
	validator := s.gm.newPubsubValidator(service)

	ctx := context.Background()
	pid := peer.ID("test-peer")
	msg := createMockMessage("", nil)

	result := validator(ctx, pid, msg)
	s.Equal(pubsub.ValidationReject, result)
}

func (s *newPubsubValidatorTestSuite) TestNewPubsubValidator_InvalidTopicName() {
	service := &mockService{}
	validator := s.gm.newPubsubValidator(service)

	ctx := context.Background()
	pid := peer.ID("test-peer")
	// Invalid topic format (not 5 parts)
	msg := createMockMessage("/eth2/abcd", nil)

	result := validator(ctx, pid, msg)
	s.Equal(pubsub.ValidationReject, result)
}

func (s *newPubsubValidatorTestSuite) TestNewPubsubValidator_ValidTopicFormat() {
	service := &mockService{}
	validator := s.gm.newPubsubValidator(service)

	ctx := context.Background()
	pid := peer.ID("test-peer")
	// Valid topic format: /eth2/[fork_digest]/[topic]/ssz_snappy
	topic := "/eth2/abcd1234/beacon_block/ssz_snappy"
	testData := []byte("test data")
	compressedData := utils.CompressSnappy(testData)
	msg := createMockMessage(topic, compressedData)

	result := validator(ctx, pid, msg)
	s.Equal(pubsub.ValidationAccept, result)
}

func (s *newPubsubValidatorTestSuite) TestNewPubsubValidator_ConditionFails() {
	service := &mockService{}
	condition := func(pid peer.ID, msg *pubsub.Message, version clparams.StateVersion) bool {
		return false // Condition fails
	}
	validator := s.gm.newPubsubValidator(service, condition)

	ctx := context.Background()
	pid := peer.ID("test-peer")
	topic := "/eth2/abcd1234/beacon_block/ssz_snappy"
	testData := []byte("test data")
	compressedData := utils.CompressSnappy(testData)
	msg := createMockMessage(topic, compressedData)

	result := validator(ctx, pid, msg)
	s.Equal(pubsub.ValidationIgnore, result)
}

func (s *newPubsubValidatorTestSuite) TestNewPubsubValidator_ConditionPasses() {
	service := &mockService{}
	condition := func(pid peer.ID, msg *pubsub.Message, version clparams.StateVersion) bool {
		return true // Condition passes
	}
	validator := s.gm.newPubsubValidator(service, condition)

	ctx := context.Background()
	pid := peer.ID("test-peer")
	topic := "/eth2/abcd1234/beacon_block/ssz_snappy"
	testData := []byte("test data")
	compressedData := utils.CompressSnappy(testData)
	msg := createMockMessage(topic, compressedData)

	result := validator(ctx, pid, msg)
	s.Equal(pubsub.ValidationAccept, result)
}

func (s *newPubsubValidatorTestSuite) TestNewPubsubValidator_NilMessageData() {
	service := &mockService{}
	validator := s.gm.newPubsubValidator(service)

	ctx := context.Background()
	pid := peer.ID("test-peer")
	topic := "/eth2/abcd1234/beacon_block/ssz_snappy"
	msg := createMockMessage(topic, nil)

	result := validator(ctx, pid, msg)
	s.Equal(pubsub.ValidationReject, result)
}

func (s *newPubsubValidatorTestSuite) TestNewPubsubValidator_DecompressionError() {
	service := &mockService{}
	validator := s.gm.newPubsubValidator(service)

	ctx := context.Background()
	pid := peer.ID("test-peer")
	topic := "/eth2/abcd1234/beacon_block/ssz_snappy"
	// Invalid compressed data
	invalidData := []byte("not valid snappy compressed data")
	msg := createMockMessage(topic, invalidData)

	result := validator(ctx, pid, msg)
	s.Equal(pubsub.ValidationReject, result)
}

func (s *newPubsubValidatorTestSuite) TestNewPubsubValidator_DecodeError() {
	service := &mockService{
		decodeFunc: func(pid peer.ID, data []byte, version clparams.StateVersion) (any, error) {
			return nil, errors.New("decode error")
		},
	}
	validator := s.gm.newPubsubValidator(service)

	ctx := context.Background()
	pid := peer.ID("test-peer")
	topic := "/eth2/abcd1234/beacon_block/ssz_snappy"
	testData := []byte("test data")
	compressedData := utils.CompressSnappy(testData)
	msg := createMockMessage(topic, compressedData)

	result := validator(ctx, pid, msg)
	s.Equal(pubsub.ValidationReject, result)
}

func (s *newPubsubValidatorTestSuite) TestNewPubsubValidator_ProcessMessageError() {
	service := &mockService{
		processFunc: func(ctx context.Context, subnet *uint64, msg any) error {
			return errors.New("process error")
		},
	}
	validator := s.gm.newPubsubValidator(service)

	ctx := context.Background()
	pid := peer.ID("test-peer")
	topic := "/eth2/abcd1234/beacon_block/ssz_snappy"
	testData := []byte("test data")
	compressedData := utils.CompressSnappy(testData)
	msg := createMockMessage(topic, compressedData)

	result := validator(ctx, pid, msg)
	s.Equal(pubsub.ValidationReject, result)
}

func (s *newPubsubValidatorTestSuite) TestNewPubsubValidator_ProcessMessageErrNotSynced() {
	service := &mockService{
		processFunc: func(ctx context.Context, subnet *uint64, msg any) error {
			return synced_data.ErrNotSynced
		},
	}
	validator := s.gm.newPubsubValidator(service)

	ctx := context.Background()
	pid := peer.ID("test-peer")
	topic := "/eth2/abcd1234/beacon_block/ssz_snappy"
	testData := []byte("test data")
	compressedData := utils.CompressSnappy(testData)
	msg := createMockMessage(topic, compressedData)

	result := validator(ctx, pid, msg)
	s.Equal(pubsub.ValidationIgnore, result)
}

func (s *newPubsubValidatorTestSuite) TestNewPubsubValidator_ProcessMessageErrIgnore() {
	service := &mockService{
		processFunc: func(ctx context.Context, subnet *uint64, msg any) error {
			return errors.New("ignore this message")
		},
	}
	validator := s.gm.newPubsubValidator(service)

	ctx := context.Background()
	pid := peer.ID("test-peer")
	topic := "/eth2/abcd1234/beacon_block/ssz_snappy"
	testData := []byte("test data")
	compressedData := utils.CompressSnappy(testData)
	msg := createMockMessage(topic, compressedData)

	result := validator(ctx, pid, msg)
	s.Equal(pubsub.ValidationIgnore, result)
}

func (s *newPubsubValidatorTestSuite) TestNewPubsubValidator_WithSubnet() {
	service := &mockService{}
	validator := s.gm.newPubsubValidator(service)

	ctx := context.Background()
	pid := peer.ID("test-peer")
	// Topic with subnet: beacon_attestation_3
	topic := "/eth2/abcd1234/beacon_attestation_3/ssz_snappy"
	testData := []byte("test data")
	compressedData := utils.CompressSnappy(testData)
	msg := createMockMessage(topic, compressedData)

	result := validator(ctx, pid, msg)
	s.Equal(pubsub.ValidationAccept, result)
}

func (s *newPubsubValidatorTestSuite) TestNewPubsubValidator_WithInvalidSubnet() {
	service := &mockService{}
	validator := s.gm.newPubsubValidator(service)

	ctx := context.Background()
	pid := peer.ID("test-peer")
	// Topic with invalid subnet (no number at the end)
	topic := "/eth2/abcd1234/beacon_attestation_invalid/ssz_snappy"
	testData := []byte("test data")
	compressedData := utils.CompressSnappy(testData)
	msg := createMockMessage(topic, compressedData)

	result := validator(ctx, pid, msg)
	s.Equal(pubsub.ValidationReject, result)
}

func (s *newPubsubValidatorTestSuite) TestNewPubsubValidator_Success() {
	service := &mockService{
		decodeFunc: func(pid peer.ID, data []byte, version clparams.StateVersion) (any, error) {
			return "decoded_message", nil
		},
		processFunc: func(ctx context.Context, subnet *uint64, msg any) error {
			return nil
		},
	}
	validator := s.gm.newPubsubValidator(service)

	ctx := context.Background()
	pid := peer.ID("test-peer")
	topic := "/eth2/abcd1234/beacon_block/ssz_snappy"
	testData := []byte("test data")
	compressedData := utils.CompressSnappy(testData)
	msg := createMockMessage(topic, compressedData)

	result := validator(ctx, pid, msg)
	s.Equal(pubsub.ValidationAccept, result)
}

type subscribeUpcomingTopicsTestSuite struct {
	suite.Suite
	gm        *GossipManager
	ctrl      *gomock.Controller
	mockClock *eth_clock.MockEthereumClock
	host      host.Host
}

// SetupTest is called before each test method
func (s *subscribeUpcomingTopicsTestSuite) SetupTest() {
	ctx := context.Background()
	s.ctrl = gomock.NewController(s.T())
	s.mockClock = eth_clock.NewMockEthereumClock(s.ctrl)

	beaconConfig := &clparams.BeaconChainConfig{
		SlotsPerEpoch:  32,
		SecondsPerSlot: 12,
	}
	networkConfig := &clparams.NetworkConfig{}

	// Setup mock expectations
	s.mockClock.EXPECT().GetCurrentEpoch().Return(uint64(10)).AnyTimes()
	s.mockClock.EXPECT().GetCurrentSlot().Return(uint64(320)).AnyTimes()
	s.mockClock.EXPECT().CurrentForkDigest().Return(common.Bytes4{0xab, 0xcd, 0x12, 0x34}, nil).AnyTimes()
	s.mockClock.EXPECT().GetEpochAtSlot(gomock.Any()).DoAndReturn(func(slot uint64) uint64 {
		return slot / beaconConfig.SlotsPerEpoch
	}).AnyTimes()
	s.mockClock.EXPECT().ComputeForkDigest(gomock.Any()).Return(common.Bytes4{0xab, 0xcd, 0x12, 0x34}, nil).AnyTimes()

	// Create actual libp2p host and pubsub
	var err error
	s.host, err = libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	s.Require().NoError(err)

	// Create pubsub with minimal options (no peer scoring for simplicity in tests)
	ps, err := pubsub.NewGossipSub(ctx, s.host, pubsub.WithMessageIdFn(func(pmsg *pb.Message) string {
		return string(pmsg.Data)
	}))
	s.Require().NoError(err)

	// Create a mock P2PManager that returns the actual pubsub
	mockP2P := mock_services.NewMockP2PManager(s.ctrl)
	mockP2P.EXPECT().Pubsub().Return(ps).AnyTimes()
	mockP2P.EXPECT().Host().Return(s.host).AnyTimes()
	mockP2P.EXPECT().BandwidthCounter().Return(metrics.NewBandwidthCounter()).AnyTimes()

	s.gm = NewGossipManager(
		context.Background(),
		mockP2P,
		beaconConfig,
		networkConfig,
		s.mockClock,
		false,                        // subscribeAll
		0,                            // activeIndicies
		datasize.ByteSize(1024*1024), // maxInboundTrafficPerPeer
		datasize.ByteSize(1024*1024), // maxOutboundTrafficPerPeer
		false,                        // adaptableTrafficRequirements
	)
}

// TearDownTest is called after each test method
func (s *subscribeUpcomingTopicsTestSuite) TearDownTest() {
	if s.gm != nil {
		s.gm.Close()
	}
	if s.host != nil {
		s.host.Close()
	}
	if s.ctrl != nil {
		s.ctrl.Finish()
	}
}

func (s *subscribeUpcomingTopicsTestSuite) TestSubscribeUpcomingTopics_NoTopics() {
	newForkDigest := common.Bytes4{0x12, 0x34, 0x56, 0x78}
	err := s.gm.subscribeUpcomingTopics(newForkDigest)
	s.NoError(err)
}

func (s *subscribeUpcomingTopicsTestSuite) TestSubscribeUpcomingTopics_WithTopics() {
	oldForkDigest := common.Bytes4{0xab, 0xcd, 0x12, 0x34}
	newForkDigest := common.Bytes4{0x12, 0x34, 0x56, 0x78}

	// Add an initial topic to subscriptions
	// Use a topic that doesn't match any score params case (returns nil)
	topicName := "unknown_topic"
	oldTopic := composeTopic(oldForkDigest, topicName)

	// Join the old topic to create a real topic handle
	oldTopicHandle, err := s.gm.p2p.Pubsub().Join(oldTopic)
	s.Require().NoError(err)

	// Create a validator
	validator := func(ctx context.Context, pid peer.ID, msg *pubsub.Message) pubsub.ValidationResult {
		return pubsub.ValidationAccept
	}

	// Add the old topic to subscriptions
	err = s.gm.subscriptions.Add(oldTopic, oldTopicHandle, validator)
	s.Require().NoError(err)

	// Set a future expiry for the old topic
	futureExpiry := time.Now().Add(1 * time.Hour)
	err = s.gm.subscriptions.SubscribeWithExpiry(oldTopic, futureExpiry)
	s.Require().NoError(err)

	// Verify old topic exists
	allTopics := s.gm.subscriptions.AllTopics()
	s.Contains(allTopics, oldTopic)

	// Subscribe to upcoming topics with new fork digest
	err = s.gm.subscribeUpcomingTopics(newForkDigest)
	s.NoError(err)

	// Verify new topic was created
	newTopic := composeTopic(newForkDigest, topicName)
	allTopicsAfter := s.gm.subscriptions.AllTopics()
	s.Contains(allTopicsAfter, newTopic)

	// Verify old topic still exists (it will be removed later by the cleanup goroutine)
	s.Contains(allTopicsAfter, oldTopic)
}

func (s *subscribeUpcomingTopicsTestSuite) TestSubscribeUpcomingTopics_SameForkDigest() {
	forkDigest := common.Bytes4{0xab, 0xcd, 0x12, 0x34}
	// Use a topic that doesn't match any score params case (returns nil)
	topicName := "unknown_topic"
	topic := composeTopic(forkDigest, topicName)

	// Add a topic
	topicHandle, err := s.gm.p2p.Pubsub().Join(topic)
	s.Require().NoError(err)

	validator := func(ctx context.Context, pid peer.ID, msg *pubsub.Message) pubsub.ValidationResult {
		return pubsub.ValidationAccept
	}

	err = s.gm.subscriptions.Add(topic, topicHandle, validator)
	s.Require().NoError(err)

	// Set a future expiry for the topic
	futureExpiry := time.Now().Add(1 * time.Hour)
	err = s.gm.subscriptions.SubscribeWithExpiry(topic, futureExpiry)
	s.Require().NoError(err)

	// Try to subscribe with the same fork digest
	err = s.gm.subscribeUpcomingTopics(forkDigest)
	s.NoError(err)

	// Verify topic count hasn't changed (same topic, so no new one added)
	allTopics := s.gm.subscriptions.AllTopics()
	s.Len(allTopics, 1)
	s.Contains(allTopics, topic)
}

func (s *subscribeUpcomingTopicsTestSuite) TestSubscribeUpcomingTopics_MultipleTopics() {
	oldForkDigest := common.Bytes4{0xab, 0xcd, 0x12, 0x34}
	newForkDigest := common.Bytes4{0x12, 0x34, 0x56, 0x78}

	// Use topics that don't match any score params case (returns nil)
	topicNames := []string{"unknown_topic_1", "unknown_topic_2", "unknown_topic_3"}
	validator := func(ctx context.Context, pid peer.ID, msg *pubsub.Message) pubsub.ValidationResult {
		return pubsub.ValidationAccept
	}

	// Add multiple topics
	futureExpiry := time.Now().Add(1 * time.Hour)
	for _, topicName := range topicNames {
		oldTopic := composeTopic(oldForkDigest, topicName)
		topicHandle, err := s.gm.p2p.Pubsub().Join(oldTopic)
		s.Require().NoError(err)
		err = s.gm.subscriptions.Add(oldTopic, topicHandle, validator)
		s.Require().NoError(err)
		// Set a future expiry for each topic
		err = s.gm.subscriptions.SubscribeWithExpiry(oldTopic, futureExpiry)
		s.Require().NoError(err)
	}

	// Verify all old topics exist
	allTopics := s.gm.subscriptions.AllTopics()
	s.Len(allTopics, len(topicNames))

	// Subscribe to upcoming topics
	err := s.gm.subscribeUpcomingTopics(newForkDigest)
	s.NoError(err)

	// Verify all new topics were created
	allTopicsAfter := s.gm.subscriptions.AllTopics()
	s.GreaterOrEqual(len(allTopicsAfter), len(topicNames)*2) // Old + new topics

	for _, topicName := range topicNames {
		newTopic := composeTopic(newForkDigest, topicName)
		s.Contains(allTopicsAfter, newTopic)
	}
}

func TestGossipManager(t *testing.T) {
	suite.Run(t, new(subscribeUpcomingTopicsTestSuite))
	suite.Run(t, new(newPubsubValidatorTestSuite))
}

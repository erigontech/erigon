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
	"github.com/libp2p/go-libp2p/core/metrics"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
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

// setupTestGossipManager creates a test GossipManager with mocks
func setupTestGossipManager(t *testing.T) (*GossipManager, *gomock.Controller, *mock_services.MockP2PManager, *eth_clock.MockEthereumClock) {
	ctrl := gomock.NewController(t)
	mockP2P := mock_services.NewMockP2PManager(ctrl)
	mockClock := eth_clock.NewMockEthereumClock(ctrl)

	beaconConfig := &clparams.BeaconChainConfig{
		SlotsPerEpoch:  32,
		SecondsPerSlot: 12,
	}
	networkConfig := &clparams.NetworkConfig{}

	// Setup mock expectations
	mockClock.EXPECT().GetCurrentEpoch().Return(uint64(10)).AnyTimes()
	mockClock.EXPECT().GetCurrentSlot().Return(uint64(320)).AnyTimes()
	mockClock.EXPECT().CurrentForkDigest().Return(common.Bytes4{0xab, 0xcd, 0x12, 0x34}, nil).AnyTimes()
	mockClock.EXPECT().GetEpochAtSlot(gomock.Any()).DoAndReturn(func(slot uint64) uint64 {
		return slot / beaconConfig.SlotsPerEpoch
	}).AnyTimes()
	mockClock.EXPECT().ComputeForkDigest(gomock.Any()).Return(common.Bytes4{0xab, 0xcd, 0x12, 0x34}, nil).AnyTimes()

	gm := NewGossipManager(
		mockP2P,
		beaconConfig,
		networkConfig,
		mockClock,
		false,                        // subscribeAll
		0,                            // activeIndicies
		datasize.ByteSize(1024*1024), // maxInboundTrafficPerPeer
		datasize.ByteSize(1024*1024), // maxOutboundTrafficPerPeer
		false,                        // adaptableTrafficRequirements
	)

	return gm, ctrl, mockP2P, mockClock
}

func TestNewPubsubValidator_EmptyTopic(t *testing.T) {
	gm, ctrl, _, _ := setupTestGossipManager(t)
	defer ctrl.Finish()

	service := &mockService{}
	validator := gm.newPubsubValidator(service)

	ctx := context.Background()
	pid := peer.ID("test-peer")
	msg := createMockMessage("", nil)

	result := validator(ctx, pid, msg)
	require.Equal(t, pubsub.ValidationReject, result)
}

func TestNewPubsubValidator_InvalidTopicName(t *testing.T) {
	gm, ctrl, _, _ := setupTestGossipManager(t)
	defer ctrl.Finish()

	service := &mockService{}
	validator := gm.newPubsubValidator(service)

	ctx := context.Background()
	pid := peer.ID("test-peer")
	// Invalid topic format (not 5 parts)
	msg := createMockMessage("/eth2/abcd", nil)

	result := validator(ctx, pid, msg)
	require.Equal(t, pubsub.ValidationReject, result)
}

func TestNewPubsubValidator_ValidTopicFormat(t *testing.T) {
	gm, ctrl, _, _ := setupTestGossipManager(t)
	defer ctrl.Finish()

	service := &mockService{}
	validator := gm.newPubsubValidator(service)

	ctx := context.Background()
	pid := peer.ID("test-peer")
	// Valid topic format: /eth2/[fork_digest]/[topic]/ssz_snappy
	topic := "/eth2/abcd1234/beacon_block/ssz_snappy"
	testData := []byte("test data")
	compressedData := utils.CompressSnappy(testData)
	msg := createMockMessage(topic, compressedData)

	result := validator(ctx, pid, msg)
	require.Equal(t, pubsub.ValidationAccept, result)
}

func TestNewPubsubValidator_ConditionFails(t *testing.T) {
	gm, ctrl, _, _ := setupTestGossipManager(t)
	defer ctrl.Finish()

	service := &mockService{}
	condition := func(pid peer.ID, msg *pubsub.Message, version clparams.StateVersion) bool {
		return false // Condition fails
	}
	validator := gm.newPubsubValidator(service, condition)

	ctx := context.Background()
	pid := peer.ID("test-peer")
	topic := "/eth2/abcd1234/beacon_block/ssz_snappy"
	testData := []byte("test data")
	compressedData := utils.CompressSnappy(testData)
	msg := createMockMessage(topic, compressedData)

	result := validator(ctx, pid, msg)
	require.Equal(t, pubsub.ValidationIgnore, result)
}

func TestNewPubsubValidator_ConditionPasses(t *testing.T) {
	gm, ctrl, _, _ := setupTestGossipManager(t)
	defer ctrl.Finish()

	service := &mockService{}
	condition := func(pid peer.ID, msg *pubsub.Message, version clparams.StateVersion) bool {
		return true // Condition passes
	}
	validator := gm.newPubsubValidator(service, condition)

	ctx := context.Background()
	pid := peer.ID("test-peer")
	topic := "/eth2/abcd1234/beacon_block/ssz_snappy"
	testData := []byte("test data")
	compressedData := utils.CompressSnappy(testData)
	msg := createMockMessage(topic, compressedData)

	result := validator(ctx, pid, msg)
	require.Equal(t, pubsub.ValidationAccept, result)
}

func TestNewPubsubValidator_NilMessageData(t *testing.T) {
	gm, ctrl, _, _ := setupTestGossipManager(t)
	defer ctrl.Finish()

	service := &mockService{}
	validator := gm.newPubsubValidator(service)

	ctx := context.Background()
	pid := peer.ID("test-peer")
	topic := "/eth2/abcd1234/beacon_block/ssz_snappy"
	msg := createMockMessage(topic, nil)

	result := validator(ctx, pid, msg)
	require.Equal(t, pubsub.ValidationReject, result)
}

func TestNewPubsubValidator_DecompressionError(t *testing.T) {
	gm, ctrl, _, _ := setupTestGossipManager(t)
	defer ctrl.Finish()

	service := &mockService{}
	validator := gm.newPubsubValidator(service)

	ctx := context.Background()
	pid := peer.ID("test-peer")
	topic := "/eth2/abcd1234/beacon_block/ssz_snappy"
	// Invalid compressed data
	invalidData := []byte("not valid snappy compressed data")
	msg := createMockMessage(topic, invalidData)

	result := validator(ctx, pid, msg)
	require.Equal(t, pubsub.ValidationReject, result)
}

func TestNewPubsubValidator_DecodeError(t *testing.T) {
	gm, ctrl, _, _ := setupTestGossipManager(t)
	defer ctrl.Finish()

	service := &mockService{
		decodeFunc: func(pid peer.ID, data []byte, version clparams.StateVersion) (any, error) {
			return nil, errors.New("decode error")
		},
	}
	validator := gm.newPubsubValidator(service)

	ctx := context.Background()
	pid := peer.ID("test-peer")
	topic := "/eth2/abcd1234/beacon_block/ssz_snappy"
	testData := []byte("test data")
	compressedData := utils.CompressSnappy(testData)
	msg := createMockMessage(topic, compressedData)

	result := validator(ctx, pid, msg)
	require.Equal(t, pubsub.ValidationReject, result)
}

func TestNewPubsubValidator_ProcessMessageError(t *testing.T) {
	gm, ctrl, _, _ := setupTestGossipManager(t)
	defer ctrl.Finish()

	service := &mockService{
		processFunc: func(ctx context.Context, subnet *uint64, msg any) error {
			return errors.New("process error")
		},
	}
	validator := gm.newPubsubValidator(service)

	ctx := context.Background()
	pid := peer.ID("test-peer")
	topic := "/eth2/abcd1234/beacon_block/ssz_snappy"
	testData := []byte("test data")
	compressedData := utils.CompressSnappy(testData)
	msg := createMockMessage(topic, compressedData)

	result := validator(ctx, pid, msg)
	require.Equal(t, pubsub.ValidationReject, result)
}

func TestNewPubsubValidator_ProcessMessageErrNotSynced(t *testing.T) {
	gm, ctrl, _, _ := setupTestGossipManager(t)
	defer ctrl.Finish()

	service := &mockService{
		processFunc: func(ctx context.Context, subnet *uint64, msg any) error {
			return synced_data.ErrNotSynced
		},
	}
	validator := gm.newPubsubValidator(service)

	ctx := context.Background()
	pid := peer.ID("test-peer")
	topic := "/eth2/abcd1234/beacon_block/ssz_snappy"
	testData := []byte("test data")
	compressedData := utils.CompressSnappy(testData)
	msg := createMockMessage(topic, compressedData)

	result := validator(ctx, pid, msg)
	require.Equal(t, pubsub.ValidationIgnore, result)
}

func TestNewPubsubValidator_ProcessMessageErrIgnore(t *testing.T) {
	gm, ctrl, _, _ := setupTestGossipManager(t)
	defer ctrl.Finish()

	service := &mockService{
		processFunc: func(ctx context.Context, subnet *uint64, msg any) error {
			return errors.New("ignore this message")
		},
	}
	validator := gm.newPubsubValidator(service)

	ctx := context.Background()
	pid := peer.ID("test-peer")
	topic := "/eth2/abcd1234/beacon_block/ssz_snappy"
	testData := []byte("test data")
	compressedData := utils.CompressSnappy(testData)
	msg := createMockMessage(topic, compressedData)

	result := validator(ctx, pid, msg)
	require.Equal(t, pubsub.ValidationIgnore, result)
}

func TestNewPubsubValidator_WithSubnet(t *testing.T) {
	gm, ctrl, _, _ := setupTestGossipManager(t)
	defer ctrl.Finish()

	service := &mockService{}
	validator := gm.newPubsubValidator(service)

	ctx := context.Background()
	pid := peer.ID("test-peer")
	// Topic with subnet: beacon_attestation_3
	topic := "/eth2/abcd1234/beacon_attestation_3/ssz_snappy"
	testData := []byte("test data")
	compressedData := utils.CompressSnappy(testData)
	msg := createMockMessage(topic, compressedData)

	result := validator(ctx, pid, msg)
	require.Equal(t, pubsub.ValidationAccept, result)
}

func TestNewPubsubValidator_WithInvalidSubnet(t *testing.T) {
	gm, ctrl, _, _ := setupTestGossipManager(t)
	defer ctrl.Finish()

	service := &mockService{}
	validator := gm.newPubsubValidator(service)

	ctx := context.Background()
	pid := peer.ID("test-peer")
	// Topic with invalid subnet (no number at the end)
	topic := "/eth2/abcd1234/beacon_attestation_invalid/ssz_snappy"
	testData := []byte("test data")
	compressedData := utils.CompressSnappy(testData)
	msg := createMockMessage(topic, compressedData)

	result := validator(ctx, pid, msg)
	require.Equal(t, pubsub.ValidationReject, result)
}

func TestNewPubsubValidator_Success(t *testing.T) {
	gm, ctrl, _, _ := setupTestGossipManager(t)
	defer ctrl.Finish()

	service := &mockService{
		decodeFunc: func(pid peer.ID, data []byte, version clparams.StateVersion) (any, error) {
			return "decoded_message", nil
		},
		processFunc: func(ctx context.Context, subnet *uint64, msg any) error {
			return nil
		},
	}
	validator := gm.newPubsubValidator(service)

	ctx := context.Background()
	pid := peer.ID("test-peer")
	topic := "/eth2/abcd1234/beacon_block/ssz_snappy"
	testData := []byte("test data")
	compressedData := utils.CompressSnappy(testData)
	msg := createMockMessage(topic, compressedData)

	result := validator(ctx, pid, msg)
	require.Equal(t, pubsub.ValidationAccept, result)
}

// setupTestGossipManagerWithPubsub creates a GossipManager with actual libp2p pubsub
func setupTestGossipManagerWithPubsub(t *testing.T) (*GossipManager, *gomock.Controller, *eth_clock.MockEthereumClock, func()) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockClock := eth_clock.NewMockEthereumClock(ctrl)

	beaconConfig := &clparams.BeaconChainConfig{
		SlotsPerEpoch:  32,
		SecondsPerSlot: 12,
	}
	networkConfig := &clparams.NetworkConfig{}

	// Setup mock expectations
	mockClock.EXPECT().GetCurrentEpoch().Return(uint64(10)).AnyTimes()
	mockClock.EXPECT().GetCurrentSlot().Return(uint64(320)).AnyTimes()
	mockClock.EXPECT().CurrentForkDigest().Return(common.Bytes4{0xab, 0xcd, 0x12, 0x34}, nil).AnyTimes()
	mockClock.EXPECT().GetEpochAtSlot(gomock.Any()).DoAndReturn(func(slot uint64) uint64 {
		return slot / beaconConfig.SlotsPerEpoch
	}).AnyTimes()
	mockClock.EXPECT().ComputeForkDigest(gomock.Any()).Return(common.Bytes4{0xab, 0xcd, 0x12, 0x34}, nil).AnyTimes()

	// Create actual libp2p host and pubsub
	host, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)

	// Create pubsub with minimal options (no peer scoring for simplicity in tests)
	ps, err := pubsub.NewGossipSub(ctx, host, pubsub.WithMessageIdFn(func(pmsg *pb.Message) string {
		return string(pmsg.Data)
	}))
	require.NoError(t, err)

	// Create a mock P2PManager that returns the actual pubsub
	mockP2P := mock_services.NewMockP2PManager(ctrl)
	mockP2P.EXPECT().Pubsub().Return(ps).AnyTimes()
	mockP2P.EXPECT().Host().Return(host).AnyTimes()
	mockP2P.EXPECT().BandwidthCounter().Return(metrics.NewBandwidthCounter()).AnyTimes()

	gm := NewGossipManager(
		mockP2P,
		beaconConfig,
		networkConfig,
		mockClock,
		false,                        // subscribeAll
		0,                            // activeIndicies
		datasize.ByteSize(1024*1024), // maxInboundTrafficPerPeer
		datasize.ByteSize(1024*1024), // maxOutboundTrafficPerPeer
		false,                        // adaptableTrafficRequirements
	)

	cleanup := func() {
		host.Close()
		ctrl.Finish()
	}

	return gm, ctrl, mockClock, cleanup
}

func TestSubscribeUpcomingTopics_NoTopics(t *testing.T) {
	gm, _, _, cleanup := setupTestGossipManagerWithPubsub(t)
	defer cleanup()

	newForkDigest := common.Bytes4{0x12, 0x34, 0x56, 0x78}
	err := gm.subscribeUpcomingTopics(newForkDigest)
	require.NoError(t, err)
}

func TestSubscribeUpcomingTopics_WithTopics(t *testing.T) {
	gm, _, _, cleanup := setupTestGossipManagerWithPubsub(t)
	defer cleanup()

	oldForkDigest := common.Bytes4{0xab, 0xcd, 0x12, 0x34}
	newForkDigest := common.Bytes4{0x12, 0x34, 0x56, 0x78}

	// Add an initial topic to subscriptions
	// Use a topic that doesn't match any score params case (returns nil)
	topicName := "unknown_topic"
	oldTopic := composeTopic(oldForkDigest, topicName)

	// Join the old topic to create a real topic handle
	oldTopicHandle, err := gm.p2p.Pubsub().Join(oldTopic)
	require.NoError(t, err)

	// Create a validator
	validator := func(ctx context.Context, pid peer.ID, msg *pubsub.Message) pubsub.ValidationResult {
		return pubsub.ValidationAccept
	}

	// Add the old topic to subscriptions
	err = gm.subscriptions.Add(oldTopic, oldTopicHandle, validator)
	require.NoError(t, err)

	// Set a future expiry for the old topic
	futureExpiry := time.Now().Add(1 * time.Hour)
	err = gm.subscriptions.SubscribeWithExpiry(oldTopic, futureExpiry)
	require.NoError(t, err)

	// Verify old topic exists
	allTopics := gm.subscriptions.AllTopics()
	require.Contains(t, allTopics, oldTopic)

	// Subscribe to upcoming topics with new fork digest
	err = gm.subscribeUpcomingTopics(newForkDigest)
	require.NoError(t, err)

	// Verify new topic was created
	newTopic := composeTopic(newForkDigest, topicName)
	allTopicsAfter := gm.subscriptions.AllTopics()
	require.Contains(t, allTopicsAfter, newTopic)

	// Verify old topic still exists (it will be removed later by the cleanup goroutine)
	require.Contains(t, allTopicsAfter, oldTopic)
}

func TestSubscribeUpcomingTopics_SameForkDigest(t *testing.T) {
	gm, _, _, cleanup := setupTestGossipManagerWithPubsub(t)
	defer cleanup()

	forkDigest := common.Bytes4{0xab, 0xcd, 0x12, 0x34}
	// Use a topic that doesn't match any score params case (returns nil)
	topicName := "unknown_topic"
	topic := composeTopic(forkDigest, topicName)

	// Add a topic
	topicHandle, err := gm.p2p.Pubsub().Join(topic)
	require.NoError(t, err)

	validator := func(ctx context.Context, pid peer.ID, msg *pubsub.Message) pubsub.ValidationResult {
		return pubsub.ValidationAccept
	}

	err = gm.subscriptions.Add(topic, topicHandle, validator)
	require.NoError(t, err)

	// Set a future expiry for the topic
	futureExpiry := time.Now().Add(1 * time.Hour)
	err = gm.subscriptions.SubscribeWithExpiry(topic, futureExpiry)
	require.NoError(t, err)

	// Try to subscribe with the same fork digest
	err = gm.subscribeUpcomingTopics(forkDigest)
	require.NoError(t, err)

	// Verify topic count hasn't changed (same topic, so no new one added)
	allTopics := gm.subscriptions.AllTopics()
	require.Len(t, allTopics, 1)
	require.Contains(t, allTopics, topic)
}

func TestSubscribeUpcomingTopics_MultipleTopics(t *testing.T) {
	gm, _, _, cleanup := setupTestGossipManagerWithPubsub(t)
	defer cleanup()

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
		topicHandle, err := gm.p2p.Pubsub().Join(oldTopic)
		require.NoError(t, err)
		err = gm.subscriptions.Add(oldTopic, topicHandle, validator)
		require.NoError(t, err)
		// Set a future expiry for each topic
		err = gm.subscriptions.SubscribeWithExpiry(oldTopic, futureExpiry)
		require.NoError(t, err)
	}

	// Verify all old topics exist
	allTopics := gm.subscriptions.AllTopics()
	require.Len(t, allTopics, len(topicNames))

	// Subscribe to upcoming topics
	err := gm.subscribeUpcomingTopics(newForkDigest)
	require.NoError(t, err)

	// Verify all new topics were created
	allTopicsAfter := gm.subscriptions.AllTopics()
	require.GreaterOrEqual(t, len(allTopicsAfter), len(topicNames)*2) // Old + new topics

	for _, topicName := range topicNames {
		newTopic := composeTopic(newForkDigest, topicName)
		require.Contains(t, allTopicsAfter, newTopic)
	}
}

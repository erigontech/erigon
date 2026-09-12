// Copyright 2026 The Erigon Authors
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

package epbs

import (
	"context"
	"errors"
	"math"
	"math/big"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/builder/epbs/eladapter"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/gossip"
	clutils "github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
)

type coordinatorAssembler struct {
	parameters *builder.Parameters
	payloadID  uint64
	payload    *eladapter.AssembledPayload
	calls      int
	started    chan struct{}
	release    chan struct{}
}

func (a *coordinatorAssembler) AssemblePayload(ctx context.Context, parameters *builder.Parameters) (uint64, error) {
	a.calls++
	a.parameters = parameters
	if a.started != nil {
		close(a.started)
	}
	if a.release != nil {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-a.release:
		}
	}
	return a.payloadID, nil
}

func (a *coordinatorAssembler) GetPayload(context.Context, uint64) (*eladapter.AssembledPayload, error) {
	return a.payload, nil
}

type coordinatorPublisher struct {
	topic string
	data  []byte
	err   error
	calls int
}

type discardCoordinatorPublisher struct{}

func (discardCoordinatorPublisher) Publish(context.Context, string, []byte) error { return nil }

func (p *coordinatorPublisher) Publish(_ context.Context, topic string, data []byte) error {
	p.calls++
	p.topic = topic
	p.data = append([]byte(nil), data...)
	return p.err
}

type blockingCoordinatorPublisher struct {
	started chan struct{}
	release chan struct{}
}

type controlledCoordinatorAssembler struct {
	mu      sync.Mutex
	calls   int
	started chan struct{}
	release chan struct{}
	payload *eladapter.AssembledPayload
}

type concurrentSlotAssembler struct {
	mu       sync.Mutex
	arrived  int
	ready    chan struct{}
	payloads map[uint64]*eladapter.AssembledPayload
}

func (a *concurrentSlotAssembler) AssemblePayload(ctx context.Context, parameters *builder.Parameters) (uint64, error) {
	a.mu.Lock()
	a.arrived++
	if a.arrived == len(a.payloads) {
		close(a.ready)
	}
	a.mu.Unlock()
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-a.ready:
		return *parameters.SlotNumber, nil
	}
}

func (a *concurrentSlotAssembler) GetPayload(_ context.Context, payloadID uint64) (*eladapter.AssembledPayload, error) {
	return a.payloads[payloadID], nil
}

func (a *controlledCoordinatorAssembler) AssemblePayload(ctx context.Context, _ *builder.Parameters) (uint64, error) {
	a.mu.Lock()
	a.calls++
	calls := a.calls
	a.mu.Unlock()
	if calls != 1 {
		return 0, errors.New("unexpected duplicate assembly")
	}
	close(a.started)
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-a.release:
		return 1, nil
	}
}

func (a *controlledCoordinatorAssembler) GetPayload(context.Context, uint64) (*eladapter.AssembledPayload, error) {
	return a.payload, nil
}

func (a *controlledCoordinatorAssembler) Calls() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.calls
}

func (p *blockingCoordinatorPublisher) Publish(ctx context.Context, _ string, _ []byte) error {
	close(p.started)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.release:
		return nil
	}
}

type coordinatorSigner struct {
	mu   sync.Mutex
	root common.Hash
}

func (s *coordinatorSigner) Pubkey() common.Bytes48 { return common.Bytes48{0: 1} }

func (s *coordinatorSigner) SignBid(_ context.Context, root common.Hash) (common.Bytes96, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.root = root
	return common.Bytes96{0: 2}, nil
}

func (s *coordinatorSigner) SignEnvelope(context.Context, common.Hash) (common.Bytes96, error) {
	return common.Bytes96{}, nil
}

func TestCoordinatorRunSlotBuildsPublishesAndRetainsBid(t *testing.T) {
	config := gloasCoordinatorConfig()
	input := validCoordinatorSlotInput(config)
	assembled := validCoordinatorPayload(&config, input, big.NewInt(1_234_567_890_999))
	assembler := &coordinatorAssembler{payloadID: 7, payload: assembled}
	publisher := new(coordinatorPublisher)
	signer := new(coordinatorSigner)
	coordinator := NewCoordinator(&config, signer, FixedMarginStrategy{Margin: 1}, assembler, publisher, 2)

	signedBid, err := coordinator.RunSlot(t.Context(), input)
	require.NoError(t, err)
	require.NotNil(t, signedBid)
	require.Equal(t, gossip.TopicNameExecutionPayloadBid, publisher.topic)
	require.NotEmpty(t, publisher.data)
	decodedBid := new(cltypes.SignedExecutionPayloadBid)
	require.NoError(t, decodedBid.DecodeSSZStrict(publisher.data, int(clparams.GloasVersion)))
	wantBidRoot, err := signedBid.Message.HashSSZ()
	require.NoError(t, err)
	gotBidRoot, err := decodedBid.Message.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, wantBidRoot, gotBidRoot)
	require.Equal(t, signedBid.Signature, decodedBid.Signature)
	require.Equal(t, uint64(1_234), signedBid.Message.Value)
	require.Equal(t, input.Slot, signedBid.Message.Slot)
	require.Equal(t, input.ParentBlockRoot, signedBid.Message.ParentBlockRoot)
	require.Equal(t, input.ParentBlockHash, signedBid.Message.ParentBlockHash)
	require.Equal(t, assembled.Eth1Block.BlockHash, signedBid.Message.BlockHash)
	require.Equal(t, input.PrevRandao, signedBid.Message.PrevRandao)
	require.Equal(t, input.ValidatedPreferences.Message.FeeRecipient, signedBid.Message.FeeRecipient)
	require.Equal(t, input.BuilderIndex, signedBid.Message.BuilderIndex)
	require.Zero(t, signedBid.Message.ExecutionPayment)

	stateVersion := config.GetCurrentStateVersion(input.Slot / config.SlotsPerEpoch)
	forkVersion := clutils.Uint32ToBytes4(config.GetForkVersionByVersion(stateVersion))
	domain, err := fork.ComputeDomain(config.DomainBeaconBuilder[:], forkVersion, input.GenesisValidatorsRoot)
	require.NoError(t, err)
	wantSigningRoot, err := fork.ComputeSigningRoot(signedBid.Message, domain)
	require.NoError(t, err)
	require.Equal(t, common.Hash(wantSigningRoot), signer.root)

	require.Equal(t, input.ParentBlockHash, assembler.parameters.ParentHash)
	require.Equal(t, input.Timestamp, assembler.parameters.Timestamp)
	require.Equal(t, input.PrevRandao, assembler.parameters.PrevRandao)
	require.Equal(t, input.BuilderExecutionAddress, assembler.parameters.SuggestedFeeRecipient)
	require.Equal(t, input.ParentBlockRoot, *assembler.parameters.ParentBeaconBlockRoot)
	require.Equal(t, input.Slot, *assembler.parameters.SlotNumber)
	require.Equal(t, input.ValidatedPreferences.Message.TargetGasLimit, *assembler.parameters.TargetGasLimit)

	identity := PayloadIdentity{
		Slot:            input.Slot,
		ParentBlockHash: input.ParentBlockHash,
		ParentBlockRoot: input.ParentBlockRoot,
		BlockHash:       assembled.Eth1Block.BlockHash,
	}
	retained, ok, err := coordinator.Payload(identity)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(7), retained.PayloadID)
	require.Equal(t, input.BuilderIndex, retained.BuilderIndex)
	require.Equal(t, input.GenesisValidatorsRoot, retained.GenesisRoot)
	signedBidRoot, err := signedBid.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, common.Hash(signedBidRoot), retained.SignedBidRoot)
	require.True(t, coordinator.MatchesPayload(identity, input.BuilderIndex, common.Hash(signedBidRoot)))
	require.False(t, coordinator.MatchesPayload(identity, input.BuilderIndex+1, common.Hash(signedBidRoot)))
	require.False(t, coordinator.MatchesPayload(identity, input.BuilderIndex, common.HexToHash("0xff")))
	require.NotSame(t, assembled, retained.Assembled)
	require.True(t, coordinator.DropPayload(identity))
	_, ok, err = coordinator.Payload(identity)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestCoordinatorRoutesPayloadFeesToBuilderAndBidPaymentToProposer(t *testing.T) {
	config := gloasCoordinatorConfig()
	input := validCoordinatorSlotInput(config)
	builderExecutionAddress := common.HexToAddress("0x21")
	input.BuilderExecutionAddress = builderExecutionAddress
	assembled := validCoordinatorPayload(&config, input, big.NewInt(2_000_000_000))
	assembled.Eth1Block.FeeRecipient = builderExecutionAddress
	assembler := &coordinatorAssembler{payload: assembled}
	coordinator := NewCoordinator(
		&config, new(coordinatorSigner), FixedMarginStrategy{Margin: 1}, assembler, new(coordinatorPublisher), 1,
	)

	bid, err := coordinator.RunSlot(t.Context(), input)
	require.NoError(t, err)
	require.Equal(t, builderExecutionAddress, assembler.parameters.SuggestedFeeRecipient)
	require.Equal(t, input.ValidatedPreferences.Message.FeeRecipient, bid.Message.FeeRecipient)

	wrongPayload := validCoordinatorPayload(&config, input, big.NewInt(2_000_000_000))
	wrongPayload.Eth1Block.FeeRecipient = input.ValidatedPreferences.Message.FeeRecipient
	wrongCoordinator := NewCoordinator(
		&config, new(coordinatorSigner), FixedMarginStrategy{Margin: 1},
		&coordinatorAssembler{payload: wrongPayload}, new(coordinatorPublisher), 1,
	)
	_, err = wrongCoordinator.RunSlot(t.Context(), input)
	require.ErrorContains(t, err, "execution payload fee recipient mismatch")
}

func TestCoordinatorExposesPayloadBeforeBidPublication(t *testing.T) {
	config := gloasCoordinatorConfig()
	input := validCoordinatorSlotInput(config)
	assembled := validCoordinatorPayload(&config, input, big.NewInt(2_000_000_000))
	publisher := &blockingCoordinatorPublisher{started: make(chan struct{}), release: make(chan struct{})}
	coordinator := NewCoordinator(
		&config,
		new(coordinatorSigner),
		FixedMarginStrategy{Margin: 1},
		&coordinatorAssembler{payloadID: 8, payload: assembled},
		publisher,
		1,
	)
	identity := PayloadIdentity{
		Slot: input.Slot, ParentBlockHash: input.ParentBlockHash,
		ParentBlockRoot: input.ParentBlockRoot, BlockHash: assembled.Eth1Block.BlockHash,
	}
	result := make(chan error, 1)
	go func() {
		_, err := coordinator.RunSlot(t.Context(), input)
		result <- err
	}()
	<-publisher.started

	_, ok, lookupErr := coordinator.Payload(identity)
	require.NoError(t, lookupErr)
	require.True(t, ok)
	close(publisher.release)
	require.NoError(t, <-result)
	_, ok, lookupErr = coordinator.Payload(identity)
	require.NoError(t, lookupErr)
	require.True(t, ok)
}

func TestCoordinatorKeepsPayloadWhenPublisherReturnsErrorAfterDelivery(t *testing.T) {
	config := gloasCoordinatorConfig()
	input := validCoordinatorSlotInput(config)
	assembled := validCoordinatorPayload(&config, input, big.NewInt(2_000_000_000))
	assembler := &coordinatorAssembler{payloadID: 1, payload: assembled}
	publisher := &coordinatorPublisher{err: errors.New("delivery outcome unknown")}
	coordinator := NewCoordinator(&config, new(coordinatorSigner), FixedMarginStrategy{Margin: 1}, assembler, publisher, 1)

	signedBid, err := coordinator.RunSlot(t.Context(), input)
	require.ErrorContains(t, err, "delivery outcome unknown")
	require.NotNil(t, signedBid)
	retained, ok, lookupErr := coordinator.Payload(payloadIdentity(input, assembled))
	require.NoError(t, lookupErr)
	require.True(t, ok)
	require.NotNil(t, retained)
}

func TestCoordinatorRejectsBidWhenRetentionIsFull(t *testing.T) {
	config := gloasCoordinatorConfig()
	input := validCoordinatorSlotInput(config)
	assembler := &coordinatorAssembler{payloadID: 1, payload: validCoordinatorPayload(&config, input, big.NewInt(2_000_000_000))}
	publisher := new(coordinatorPublisher)
	coordinator := NewCoordinator(&config, new(coordinatorSigner), FixedMarginStrategy{Margin: 1}, assembler, publisher, 1)
	_, err := coordinator.RunSlot(t.Context(), input)
	require.NoError(t, err)

	next := coordinatorInputAtSlot(input, input.Slot+1)
	assembler.payloadID = 2
	assembler.payload = validCoordinatorPayload(&config, next, big.NewInt(2_000_000_000))
	_, err = coordinator.RunSlot(t.Context(), next)
	require.ErrorIs(t, err, ErrRetainedPayloadCapacity)
	require.Equal(t, 1, assembler.calls)
	require.Equal(t, 1, publisher.calls)
}

func TestCoordinatorRejectsDuplicateAuctionBeforeAssembly(t *testing.T) {
	config := gloasCoordinatorConfig()
	input := validCoordinatorSlotInput(config)
	assembler := &controlledCoordinatorAssembler{
		started: make(chan struct{}), release: make(chan struct{}),
		payload: validCoordinatorPayload(&config, input, big.NewInt(2_000_000_000)),
	}
	coordinator := NewCoordinator(&config, new(coordinatorSigner), FixedMarginStrategy{Margin: 1}, assembler, discardCoordinatorPublisher{}, 2)
	first := make(chan error, 1)
	go func() {
		_, err := coordinator.RunSlot(t.Context(), input)
		first <- err
	}()
	<-assembler.started

	_, err := coordinator.RunSlot(t.Context(), input)
	require.ErrorIs(t, err, ErrAuctionAlreadyTracked)
	require.Equal(t, 1, assembler.Calls())
	close(assembler.release)
	require.NoError(t, <-first)
}

func TestCoordinatorSafeExpiryPreventsInflightOldSlotFromPublishing(t *testing.T) {
	config := gloasCoordinatorConfig()
	input := validCoordinatorSlotInput(config)
	assembled := validCoordinatorPayload(&config, input, big.NewInt(2_000_000_000))
	assembler := &controlledCoordinatorAssembler{
		started: make(chan struct{}), release: make(chan struct{}), payload: assembled,
	}
	publisher := new(coordinatorPublisher)
	coordinator := NewCoordinator(&config, new(coordinatorSigner), FixedMarginStrategy{Margin: 1}, assembler, publisher, 2)
	result := make(chan error, 1)
	go func() {
		_, err := coordinator.RunSlot(t.Context(), input)
		result <- err
	}()
	<-assembler.started
	coordinator.PruneExpiredBeforeSlot(input.Slot + 1)
	close(assembler.release)
	require.ErrorIs(t, <-result, ErrSlotExpired)
	require.Zero(t, publisher.calls)

	regularAssembler := &coordinatorAssembler{payload: assembled}
	coordinator.assembler = regularAssembler
	_, err := coordinator.RunSlot(t.Context(), input)
	require.ErrorIs(t, err, ErrSlotExpired)
	require.Zero(t, regularAssembler.calls)
}

func TestCoordinatorDropDuringPublishIsNotReversed(t *testing.T) {
	config := gloasCoordinatorConfig()
	input := validCoordinatorSlotInput(config)
	assembled := validCoordinatorPayload(&config, input, big.NewInt(2_000_000_000))
	publisher := &blockingCoordinatorPublisher{started: make(chan struct{}), release: make(chan struct{})}
	coordinator := NewCoordinator(
		&config, new(coordinatorSigner), FixedMarginStrategy{Margin: 1},
		&coordinatorAssembler{payload: assembled}, publisher, 1,
	)
	result := make(chan error, 1)
	go func() {
		_, err := coordinator.RunSlot(t.Context(), input)
		result <- err
	}()
	<-publisher.started
	identity := payloadIdentity(input, assembled)
	require.False(t, coordinator.DropPayload(identity))
	_, ok, lookupErr := coordinator.Payload(identity)
	require.NoError(t, lookupErr)
	require.True(t, ok)
	close(publisher.release)
	require.NoError(t, <-result)
	_, ok, lookupErr = coordinator.Payload(identity)
	require.NoError(t, lookupErr)
	require.True(t, ok)
}

func TestCoordinatorSafeExpiryDuringPublishCleansAfterPublish(t *testing.T) {
	config := gloasCoordinatorConfig()
	input := validCoordinatorSlotInput(config)
	assembled := validCoordinatorPayload(&config, input, big.NewInt(2_000_000_000))
	publisher := &blockingCoordinatorPublisher{started: make(chan struct{}), release: make(chan struct{})}
	assembler := &coordinatorAssembler{payload: assembled}
	coordinator := NewCoordinator(
		&config, new(coordinatorSigner), FixedMarginStrategy{Margin: 1}, assembler, publisher, 1,
	)
	result := make(chan error, 1)
	go func() {
		_, err := coordinator.RunSlot(t.Context(), input)
		result <- err
	}()
	<-publisher.started
	identity := payloadIdentity(input, assembled)

	require.Zero(t, coordinator.PruneExpiredBeforeSlot(input.Slot+1))
	_, ok, lookupErr := coordinator.Payload(identity)
	require.NoError(t, lookupErr)
	require.True(t, ok)
	close(publisher.release)
	require.NoError(t, <-result)
	_, ok, lookupErr = coordinator.Payload(identity)
	require.NoError(t, lookupErr)
	require.False(t, ok)

	_, err := coordinator.RunSlot(t.Context(), input)
	require.ErrorIs(t, err, ErrSlotExpired)
	require.Equal(t, 1, assembler.calls)
}

func TestCoordinatorCapsAggregateOutstandingBids(t *testing.T) {
	config := gloasCoordinatorConfig()
	input := validCoordinatorSlotInput(config)
	input.AvailableBidValueGwei = 3
	firstPayload := validCoordinatorPayload(&config, input, big.NewInt(2_000_000_000))
	assembler := &coordinatorAssembler{payload: firstPayload}
	coordinator := NewCoordinator(&config, new(coordinatorSigner), FixedMarginStrategy{Margin: 1}, assembler, new(coordinatorPublisher), 2)
	firstBid, err := coordinator.RunSlot(t.Context(), input)
	require.NoError(t, err)
	require.Equal(t, uint64(2), firstBid.Message.Value)
	require.True(t, coordinator.DropPayload(payloadIdentity(input, firstPayload)))

	next := coordinatorInputAtSlot(input, input.Slot+1)
	secondPayload := validCoordinatorPayload(&config, next, big.NewInt(2_000_000_000))
	assembler.payload = secondPayload
	secondBid, err := coordinator.RunSlot(t.Context(), next)
	require.NoError(t, err)
	require.Equal(t, uint64(1), secondBid.Message.Value)
	require.LessOrEqual(t, firstBid.Message.Value+secondBid.Message.Value, input.AvailableBidValueGwei)
}

func TestCoordinatorDoesNotOvercommitConcurrentSlots(t *testing.T) {
	config := gloasCoordinatorConfig()
	firstInput := validCoordinatorSlotInput(config)
	firstInput.AvailableBidValueGwei = 3
	secondInput := coordinatorInputAtSlot(firstInput, firstInput.Slot+1)
	assembler := &concurrentSlotAssembler{
		ready: make(chan struct{}),
		payloads: map[uint64]*eladapter.AssembledPayload{
			firstInput.Slot:  validCoordinatorPayload(&config, firstInput, big.NewInt(2_000_000_000)),
			secondInput.Slot: validCoordinatorPayload(&config, secondInput, big.NewInt(2_000_000_000)),
		},
	}
	coordinator := NewCoordinator(&config, new(coordinatorSigner), FixedMarginStrategy{Margin: 1}, assembler, discardCoordinatorPublisher{}, 2)
	type result struct {
		bid *cltypes.SignedExecutionPayloadBid
		err error
	}
	results := make(chan result, 2)
	for _, input := range []SlotInput{firstInput, secondInput} {
		go func() {
			bid, err := coordinator.RunSlot(t.Context(), input)
			results <- result{bid: bid, err: err}
		}()
	}
	first := <-results
	second := <-results
	require.NoError(t, first.err)
	require.NoError(t, second.err)
	require.NotNil(t, first.bid)
	require.NotNil(t, second.bid)
	require.LessOrEqual(t, first.bid.Message.Value+second.bid.Message.Value, firstInput.AvailableBidValueGwei)
}

func TestCoordinatorPruneExpiredBeforeSlotRemovesOnlyOlderPayloads(t *testing.T) {
	config := gloasCoordinatorConfig()
	input := validCoordinatorSlotInput(config)
	first := validCoordinatorPayload(&config, input, big.NewInt(2_000_000_000))
	assembler := &coordinatorAssembler{payloadID: 1, payload: first}
	coordinator := NewCoordinator(&config, new(coordinatorSigner), FixedMarginStrategy{Margin: 1}, assembler, new(coordinatorPublisher), 2)
	_, err := coordinator.RunSlot(t.Context(), input)
	require.NoError(t, err)

	next := coordinatorInputAtSlot(input, input.Slot+1)
	second := validCoordinatorPayload(&config, next, big.NewInt(2_000_000_000))
	assembler.payloadID = 2
	assembler.payload = second
	_, err = coordinator.RunSlot(t.Context(), next)
	require.NoError(t, err)

	require.Equal(t, 1, coordinator.PruneExpiredBeforeSlot(next.Slot))
	_, ok, lookupErr := coordinator.Payload(payloadIdentity(input, first))
	require.NoError(t, lookupErr)
	require.False(t, ok)
	_, ok, lookupErr = coordinator.Payload(payloadIdentity(next, second))
	require.NoError(t, lookupErr)
	require.True(t, ok)
}

func TestCoordinatorRejectsStaleBuilderStatus(t *testing.T) {
	config := gloasCoordinatorConfig()
	input := validCoordinatorSlotInput(config)
	input.BuilderStatusSlot--
	assembler := &coordinatorAssembler{payload: validCoordinatorPayload(&config, input, big.NewInt(2_000_000_000))}
	coordinator := NewCoordinator(&config, new(coordinatorSigner), FixedMarginStrategy{Margin: 1}, assembler, new(coordinatorPublisher), 1)

	_, err := coordinator.RunSlot(t.Context(), input)
	require.ErrorContains(t, err, "builder status slot")
	require.Zero(t, assembler.calls)
}

func TestCoordinatorRejectsBuilderStatusFromAnotherParent(t *testing.T) {
	config := gloasCoordinatorConfig()
	input := validCoordinatorSlotInput(config)
	input.BuilderStatusParentRoot[0] ^= 1
	assembler := &coordinatorAssembler{payload: validCoordinatorPayload(&config, input, big.NewInt(2_000_000_000))}
	coordinator := NewCoordinator(&config, new(coordinatorSigner), FixedMarginStrategy{Margin: 1}, assembler, new(coordinatorPublisher), 1)

	_, err := coordinator.RunSlot(t.Context(), input)
	require.ErrorContains(t, err, "builder status parent root")
	require.Zero(t, assembler.calls)
}

func TestCoordinatorRejectsBuilderIndexForAnotherPubkey(t *testing.T) {
	config := gloasCoordinatorConfig()
	input := validCoordinatorSlotInput(config)
	input.BuilderPubkey[0] ^= 1
	assembler := &coordinatorAssembler{payload: validCoordinatorPayload(&config, input, big.NewInt(2_000_000_000))}
	coordinator := NewCoordinator(&config, new(coordinatorSigner), FixedMarginStrategy{Margin: 1}, assembler, new(coordinatorPublisher), 1)

	_, err := coordinator.RunSlot(t.Context(), input)
	require.ErrorContains(t, err, "builder pubkey")
	require.Zero(t, assembler.calls)
}

func TestCoordinatorRejectsBuilderStatusForAnotherIndex(t *testing.T) {
	config := gloasCoordinatorConfig()
	input := validCoordinatorSlotInput(config)
	input.BuilderIndex++
	assembler := &coordinatorAssembler{payload: validCoordinatorPayload(&config, input, big.NewInt(2_000_000_000))}
	coordinator := NewCoordinator(&config, new(coordinatorSigner), FixedMarginStrategy{Margin: 1}, assembler, new(coordinatorPublisher), 1)

	_, err := coordinator.RunSlot(t.Context(), input)
	require.ErrorContains(t, err, "builder status index")
	require.Zero(t, assembler.calls)
}

func TestCoordinatorRejectsMismatchedBuiltPayload(t *testing.T) {
	for name, mutate := range map[string]func(*cltypes.Eth1Block){
		"parent hash":   func(payload *cltypes.Eth1Block) { payload.ParentHash[0] ^= 1 },
		"prev randao":   func(payload *cltypes.Eth1Block) { payload.PrevRandao[0] ^= 1 },
		"timestamp":     func(payload *cltypes.Eth1Block) { payload.Time++ },
		"slot":          func(payload *cltypes.Eth1Block) { payload.SlotNumber++ },
		"fee recipient": func(payload *cltypes.Eth1Block) { payload.FeeRecipient[0] ^= 1 },
		"gas limit":     func(payload *cltypes.Eth1Block) { payload.GasLimit++ },
	} {
		t.Run(name, func(t *testing.T) {
			config := gloasCoordinatorConfig()
			input := validCoordinatorSlotInput(config)
			assembled := validCoordinatorPayload(&config, input, big.NewInt(2_000_000_000))
			mutate(assembled.Eth1Block)
			publisher := new(coordinatorPublisher)
			coordinator := NewCoordinator(
				&config, new(coordinatorSigner), FixedMarginStrategy{Margin: 1},
				&coordinatorAssembler{payload: assembled}, publisher, 1,
			)

			_, err := coordinator.RunSlot(t.Context(), input)
			require.Error(t, err)
			require.Zero(t, publisher.calls)
		})
	}
}

func TestCoordinatorRejectsBuiltPayloadReusingParentBlockHash(t *testing.T) {
	config := gloasCoordinatorConfig()
	input := validCoordinatorSlotInput(config)
	assembled := validCoordinatorPayload(&config, input, big.NewInt(2_000_000_000))
	assembled.Eth1Block.BlockHash = input.ParentBlockHash
	publisher := new(coordinatorPublisher)
	signer := new(coordinatorSigner)
	coordinator := NewCoordinator(
		&config, signer, FixedMarginStrategy{Margin: 1},
		&coordinatorAssembler{payload: assembled}, publisher, 1,
	)

	bid, err := coordinator.RunSlot(t.Context(), input)
	require.Error(t, err)
	require.Nil(t, bid)
	require.Zero(t, publisher.calls)
	require.Zero(t, signer.root)
}

func TestCoordinatorSkipsInactiveBuilderBeforeAssembly(t *testing.T) {
	for name, mutate := range map[string]func(*SlotInput){
		"inactive":        func(input *SlotInput) { input.BuilderActive = false },
		"no excess funds": func(input *SlotInput) { input.AvailableBidValueGwei = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			config := gloasCoordinatorConfig()
			input := validCoordinatorSlotInput(config)
			mutate(&input)
			assembler := &coordinatorAssembler{payload: validCoordinatorPayload(&config, input, big.NewInt(2_000_000_000))}
			coordinator := NewCoordinator(&config, new(coordinatorSigner), FixedMarginStrategy{Margin: 1}, assembler, new(coordinatorPublisher), 1)

			bid, err := coordinator.RunSlot(t.Context(), input)
			require.NoError(t, err)
			require.Nil(t, bid)
			require.Zero(t, assembler.calls)
		})
	}
}

func TestCoordinatorCapsFlooredBidAtAvailableBalance(t *testing.T) {
	config := gloasCoordinatorConfig()
	input := validCoordinatorSlotInput(config)
	input.AvailableBidValueGwei = 1_000
	assembled := validCoordinatorPayload(&config, input, big.NewInt(1_234_567_890_999))
	coordinator := NewCoordinator(
		&config, new(coordinatorSigner), FixedMarginStrategy{Margin: 1},
		&coordinatorAssembler{payload: assembled}, new(coordinatorPublisher), 1,
	)

	bid, err := coordinator.RunSlot(t.Context(), input)
	require.NoError(t, err)
	require.Equal(t, uint64(1_000), bid.Message.Value)
}

func TestCoordinatorRejectsNilBlobBundle(t *testing.T) {
	config := gloasCoordinatorConfig()
	input := validCoordinatorSlotInput(config)
	assembled := validCoordinatorPayload(&config, input, big.NewInt(2_000_000_000))
	assembled.BlobsBundle = nil
	publisher := new(coordinatorPublisher)
	coordinator := NewCoordinator(
		&config, new(coordinatorSigner), FixedMarginStrategy{Margin: 1},
		&coordinatorAssembler{payload: assembled}, publisher, 1,
	)

	_, err := coordinator.RunSlot(t.Context(), input)
	require.ErrorContains(t, err, "blob bundle")
	require.Zero(t, publisher.calls)
}

func TestCoordinatorRejectsMalformedBlobBundle(t *testing.T) {
	config := gloasCoordinatorConfig()
	input := validCoordinatorSlotInput(config)
	assembled := validCoordinatorPayload(&config, input, big.NewInt(2_000_000_000))
	assembled.BlobsBundle = &eladapter.BlobsBundle{
		Commitments: [][]byte{make([]byte, len(cltypes.KZGCommitment{})-1)},
		Proofs:      [][]byte{make([]byte, len(cltypes.KZGProof{})), make([]byte, len(cltypes.KZGProof{}))},
		Blobs:       [][]byte{make([]byte, cltypes.BytesPerBlob)},
	}
	publisher := new(coordinatorPublisher)
	coordinator := NewCoordinator(
		&config, new(coordinatorSigner), FixedMarginStrategy{Margin: 1},
		&coordinatorAssembler{payload: assembled}, publisher, 1,
	)

	_, err := coordinator.RunSlot(t.Context(), input)
	require.ErrorContains(t, err, "commitment 0 has length")
	require.Zero(t, publisher.calls)
}

func TestCoordinatorAcceptsPeerDASProofsForEveryColumn(t *testing.T) {
	config := gloasCoordinatorConfig()
	input := validCoordinatorSlotInput(config)
	assembled := validCoordinatorPayload(&config, input, big.NewInt(2_000_000_000))
	assembled.BlobsBundle = validCoordinatorBlobsBundle(config, 1)
	coordinator := NewCoordinator(
		&config, new(coordinatorSigner), FixedMarginStrategy{Margin: 1},
		&coordinatorAssembler{payload: assembled}, new(coordinatorPublisher), 1,
	)

	bid, err := coordinator.RunSlot(t.Context(), input)
	require.NoError(t, err)
	require.Equal(t, 1, bid.Message.BlobKzgCommitments.Len())
}

func TestCoordinatorRejectsMissingPeerDASColumnProof(t *testing.T) {
	config := gloasCoordinatorConfig()
	input := validCoordinatorSlotInput(config)
	assembled := validCoordinatorPayload(&config, input, big.NewInt(2_000_000_000))
	assembled.BlobsBundle = validCoordinatorBlobsBundle(config, 1)
	assembled.BlobsBundle.Proofs = assembled.BlobsBundle.Proofs[:len(assembled.BlobsBundle.Proofs)-1]
	publisher := new(coordinatorPublisher)
	coordinator := NewCoordinator(
		&config, new(coordinatorSigner), FixedMarginStrategy{Margin: 1},
		&coordinatorAssembler{payload: assembled}, publisher, 1,
	)

	_, err := coordinator.RunSlot(t.Context(), input)
	require.ErrorContains(t, err, "proofs")
	require.Zero(t, publisher.calls)
}

func TestCoordinatorRejectsMalformedPeerDASColumnProof(t *testing.T) {
	config := gloasCoordinatorConfig()
	input := validCoordinatorSlotInput(config)
	assembled := validCoordinatorPayload(&config, input, big.NewInt(2_000_000_000))
	assembled.BlobsBundle = validCoordinatorBlobsBundle(config, 1)
	assembled.BlobsBundle.Proofs[1] = assembled.BlobsBundle.Proofs[1][:len(cltypes.KZGProof{})-1]
	publisher := new(coordinatorPublisher)
	coordinator := NewCoordinator(
		&config, new(coordinatorSigner), FixedMarginStrategy{Margin: 1},
		&coordinatorAssembler{payload: assembled}, publisher, 1,
	)

	_, err := coordinator.RunSlot(t.Context(), input)
	require.ErrorContains(t, err, "proof 1 has length")
	require.Zero(t, publisher.calls)
}

func TestCoordinatorRejectsPreferenceForAnotherIdentity(t *testing.T) {
	for name, mutate := range map[string]func(*cltypes.ProposerPreferences){
		"slot":           func(preferences *cltypes.ProposerPreferences) { preferences.ProposalSlot++ },
		"dependent root": func(preferences *cltypes.ProposerPreferences) { preferences.DependentRoot[0] ^= 1 },
	} {
		t.Run(name, func(t *testing.T) {
			config := gloasCoordinatorConfig()
			input := validCoordinatorSlotInput(config)
			mutate(input.ValidatedPreferences.Message)
			assembler := &coordinatorAssembler{payload: validCoordinatorPayload(&config, input, big.NewInt(2_000_000_000))}
			coordinator := NewCoordinator(&config, new(coordinatorSigner), FixedMarginStrategy{Margin: 1}, assembler, new(coordinatorPublisher), 1)

			_, err := coordinator.RunSlot(t.Context(), input)
			require.Error(t, err)
			require.Zero(t, assembler.calls)
		})
	}
}

func TestCoordinatorUsesEIP1559GasLimitTransition(t *testing.T) {
	config := gloasCoordinatorConfig()
	input := validCoordinatorSlotInput(config)
	input.ValidatedPreferences.Message.TargetGasLimit = input.ParentGasLimit + 1_000_000
	wantGasLimit := input.ParentGasLimit + input.ParentGasLimit/1024 - 1
	assembled := validCoordinatorPayload(&config, input, big.NewInt(2_000_000_000))
	assembled.Eth1Block.GasLimit = wantGasLimit
	coordinator := NewCoordinator(
		&config, new(coordinatorSigner), FixedMarginStrategy{Margin: 1},
		&coordinatorAssembler{payload: assembled}, new(coordinatorPublisher), 1,
	)

	bid, err := coordinator.RunSlot(t.Context(), input)
	require.NoError(t, err)
	require.Equal(t, wantGasLimit, bid.Message.GasLimit)
}

func TestCoordinatorAcceptsZeroGasTargetAtEIP1559Minimum(t *testing.T) {
	config := gloasCoordinatorConfig()
	input := validCoordinatorSlotInput(config)
	input.ValidatedPreferences.Message.TargetGasLimit = 0
	wantGasLimit := input.ParentGasLimit - (input.ParentGasLimit/1024 - 1)
	assembled := validCoordinatorPayload(&config, input, big.NewInt(2_000_000_000))
	assembled.Eth1Block.GasLimit = wantGasLimit
	coordinator := NewCoordinator(
		&config, new(coordinatorSigner), FixedMarginStrategy{Margin: 1},
		&coordinatorAssembler{payload: assembled}, new(coordinatorPublisher), 1,
	)

	bid, err := coordinator.RunSlot(t.Context(), input)
	require.NoError(t, err)
	require.Equal(t, wantGasLimit, bid.Message.GasLimit)
}

func TestCoordinatorRejectsSlotBeforeGloas(t *testing.T) {
	config := gloasCoordinatorConfig()
	input := validCoordinatorSlotInput(config)
	config.GloasForkEpoch = input.Slot/config.SlotsPerEpoch + 1
	assembler := &coordinatorAssembler{payload: validCoordinatorPayload(&config, input, big.NewInt(2_000_000_000))}
	coordinator := NewCoordinator(&config, new(coordinatorSigner), FixedMarginStrategy{Margin: 1}, assembler, new(coordinatorPublisher), 1)

	_, err := coordinator.RunSlot(t.Context(), input)
	require.ErrorContains(t, err, "before gloas")
	require.Zero(t, assembler.calls)
}

func TestCoordinatorRejectsGenesisSlot(t *testing.T) {
	config := gloasCoordinatorConfig()
	input := validCoordinatorSlotInput(config)
	config.GenesisSlot = input.Slot
	assembler := &coordinatorAssembler{payload: validCoordinatorPayload(&config, input, big.NewInt(2_000_000_000))}
	coordinator := NewCoordinator(&config, new(coordinatorSigner), FixedMarginStrategy{Margin: 1}, assembler, new(coordinatorPublisher), 1)

	_, err := coordinator.RunSlot(t.Context(), input)
	require.ErrorContains(t, err, "genesis")
	require.Zero(t, assembler.calls)
}

func TestCoordinatorRejectsBidThatOverflowsGwei(t *testing.T) {
	config := gloasCoordinatorConfig()
	input := validCoordinatorSlotInput(config)
	overflowGwei := new(big.Int).Add(new(big.Int).SetUint64(math.MaxUint64), big.NewInt(1))
	blockValue := new(big.Int).Mul(overflowGwei, big.NewInt(weiPerGwei))
	publisher := new(coordinatorPublisher)
	coordinator := NewCoordinator(
		&config, new(coordinatorSigner), FixedMarginStrategy{Margin: 1},
		&coordinatorAssembler{payload: validCoordinatorPayload(&config, input, blockValue)}, publisher, 1,
	)

	_, err := coordinator.RunSlot(t.Context(), input)
	require.ErrorContains(t, err, "exceeds uint64 gwei")
	require.Zero(t, publisher.calls)
}

func TestCoordinatorRejectsMalformedExecutionRequests(t *testing.T) {
	config := gloasCoordinatorConfig()
	input := validCoordinatorSlotInput(config)
	assembled := validCoordinatorPayload(&config, input, big.NewInt(2_000_000_000))
	assembled.RequestsBundle.Requests = [][]byte{{0xff, 1}}
	publisher := new(coordinatorPublisher)
	coordinator := NewCoordinator(
		&config, new(coordinatorSigner), FixedMarginStrategy{Margin: 1},
		&coordinatorAssembler{payload: assembled}, publisher, 1,
	)

	_, err := coordinator.RunSlot(t.Context(), input)
	require.ErrorContains(t, err, "unknown execution request type")
	require.Zero(t, publisher.calls)
}

func TestCoordinatorRejectsTypedNilDependency(t *testing.T) {
	config := gloasCoordinatorConfig()
	input := validCoordinatorSlotInput(config)
	var assembler *coordinatorAssembler
	coordinator := NewCoordinator(&config, new(coordinatorSigner), FixedMarginStrategy{Margin: 1}, assembler, new(coordinatorPublisher), 1)

	require.NotPanics(t, func() {
		_, err := coordinator.RunSlot(t.Context(), input)
		require.ErrorContains(t, err, "missing dependency")
	})
}

func TestCoordinatorRejectsNilContext(t *testing.T) {
	config := gloasCoordinatorConfig()
	input := validCoordinatorSlotInput(config)
	coordinator := NewCoordinator(
		&config, new(coordinatorSigner), FixedMarginStrategy{Margin: 1},
		&coordinatorAssembler{payload: validCoordinatorPayload(&config, input, big.NewInt(2_000_000_000))},
		new(coordinatorPublisher), 1,
	)

	require.NotPanics(t, func() {
		var ctx context.Context
		_, err := coordinator.RunSlot(ctx, input)
		require.ErrorContains(t, err, "nil context")
	})
}

func TestCoordinatorPayloadLookupSurvivesRevealRetry(t *testing.T) {
	config := gloasCoordinatorConfig()
	input := validCoordinatorSlotInput(config)
	assembled := validCoordinatorPayload(&config, input, big.NewInt(2_000_000_000))
	coordinator := NewCoordinator(
		&config, new(coordinatorSigner), FixedMarginStrategy{Margin: 1},
		&coordinatorAssembler{payload: assembled}, new(coordinatorPublisher), 1,
	)
	_, err := coordinator.RunSlot(t.Context(), input)
	require.NoError(t, err)
	identity := payloadIdentity(input, assembled)

	first, ok, err := coordinator.Payload(identity)
	require.NoError(t, err)
	require.True(t, ok)
	second, ok, err := coordinator.Payload(identity)
	require.NoError(t, err)
	require.True(t, ok)
	require.NotSame(t, first, second)
	require.True(t, coordinator.DropPayload(identity))
	_, ok, err = coordinator.Payload(identity)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestCoordinatorPayloadLookupReturnsOwnedDefensiveCopy(t *testing.T) {
	config := gloasCoordinatorConfig()
	input := validCoordinatorSlotInput(config)
	assembled := validCoordinatorPayload(&config, input, big.NewInt(2_000_000_000))
	assembled.BlobsBundle = validCoordinatorBlobsBundle(config, 1)
	wantBlockHash := assembled.Eth1Block.BlockHash
	wantBlockValue := new(big.Int).Set(assembled.BlockValue)
	wantBlobByte := assembled.BlobsBundle.Blobs[0][0]
	identity := payloadIdentity(input, assembled)
	coordinator := NewCoordinator(
		&config, new(coordinatorSigner), FixedMarginStrategy{Margin: 1},
		&coordinatorAssembler{payload: assembled}, new(coordinatorPublisher), 1,
	)
	_, err := coordinator.RunSlot(t.Context(), input)
	require.NoError(t, err)

	assembled.Eth1Block.BlockHash[0] ^= 1
	assembled.BlockValue.SetInt64(1)
	assembled.BlobsBundle.Blobs[0][0] ^= 1
	first, ok, err := coordinator.Payload(identity)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, wantBlockHash, first.Assembled.Eth1Block.BlockHash)
	require.Equal(t, wantBlockValue, first.Assembled.BlockValue)
	require.Equal(t, wantBlobByte, first.Assembled.BlobsBundle.Blobs[0][0])

	first.Assembled.Eth1Block.BlockHash[0] ^= 1
	first.Assembled.BlockValue.SetInt64(2)
	first.Assembled.BlobsBundle.Blobs[0][0] ^= 1
	second, ok, err := coordinator.Payload(identity)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, wantBlockHash, second.Assembled.Eth1Block.BlockHash)
	require.Equal(t, wantBlockValue, second.Assembled.BlockValue)
	require.Equal(t, wantBlobByte, second.Assembled.BlobsBundle.Blobs[0][0])
}

func validCoordinatorSlotInput(config clparams.BeaconChainConfig) SlotInput { //nolint:gocritic // Tests use isolated config values.
	slot := uint64(64)
	dependentRoot := common.HexToHash("0x10")
	return SlotInput{
		ValidatedPreferences: &cltypes.SignedProposerPreferences{
			Message: &cltypes.ProposerPreferences{
				DependentRoot:  dependentRoot,
				ProposalSlot:   slot,
				FeeRecipient:   common.HexToAddress("0x20"),
				TargetGasLimit: 30_000_000,
			},
			Signature: common.Bytes96{0: 1},
		},
		Slot:                    slot,
		DependentRoot:           dependentRoot,
		ParentBlockRoot:         common.HexToHash("0x30"),
		ParentBlockHash:         common.HexToHash("0x40"),
		ParentGasLimit:          30_000_000,
		PrevRandao:              common.HexToHash("0x50"),
		Timestamp:               1_800_000_000,
		Withdrawals:             []*types.Withdrawal{},
		BuilderIndex:            3,
		BuilderStatusIndex:      3,
		BuilderStatusSlot:       slot,
		BuilderStatusParentRoot: common.HexToHash("0x30"),
		BuilderPubkey:           common.Bytes48{0: 1},
		BuilderExecutionAddress: common.HexToAddress("0x21"),
		GenesisValidatorsRoot:   common.HexToHash("0x60"),
		BuilderActive:           true,
		AvailableBidValueGwei:   2_000,
	}
}

func gloasCoordinatorConfig() clparams.BeaconChainConfig {
	config := clparams.MainnetBeaconConfig
	config.AltairForkEpoch = 0
	config.BellatrixForkEpoch = 0
	config.CapellaForkEpoch = 0
	config.DenebForkEpoch = 0
	config.ElectraForkEpoch = 0
	config.FuluForkEpoch = 0
	config.GloasForkEpoch = 0
	config.NumberOfColumns = 2
	return config
}

func coordinatorInputAtSlot(input SlotInput, slot uint64) SlotInput {
	input.ValidatedPreferences = input.ValidatedPreferences.Clone().(*cltypes.SignedProposerPreferences)
	input.ValidatedPreferences.Message.ProposalSlot = slot
	input.Slot = slot
	input.BuilderStatusSlot = slot
	input.Timestamp += slot - 64
	return input
}

func payloadIdentity(input SlotInput, payload *eladapter.AssembledPayload) PayloadIdentity {
	return PayloadIdentity{
		Slot: input.Slot, ParentBlockHash: input.ParentBlockHash,
		ParentBlockRoot: input.ParentBlockRoot, BlockHash: payload.Eth1Block.BlockHash,
	}
}

func validCoordinatorPayload(config *clparams.BeaconChainConfig, input SlotInput, blockValue *big.Int) *eladapter.AssembledPayload {
	payload := cltypes.NewEth1Block(clparams.GloasVersion, config)
	payload.ParentHash = input.ParentBlockHash
	payload.BlockHash = common.HexToHash("0x70")
	payload.PrevRandao = input.PrevRandao
	payload.FeeRecipient = input.BuilderExecutionAddress
	payload.GasLimit = input.ValidatedPreferences.Message.TargetGasLimit
	payload.Time = input.Timestamp
	payload.SlotNumber = input.Slot
	payload.Extra = solid.NewExtraData()
	payload.Transactions = solid.NewTransactionsSSZWithLimits(config.MaxTransactionsPerPayload, config.MaxBytesPerTransaction)
	payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(config.MaxWithdrawalsPerPayload), 44)
	return &eladapter.AssembledPayload{
		Eth1Block:      payload,
		RequestsBundle: &typesproto.RequestsBundle{Requests: [][]byte{}},
		BlobsBundle: &eladapter.BlobsBundle{
			Commitments: [][]byte{}, Proofs: [][]byte{}, Blobs: [][]byte{},
		},
		BlockValue: blockValue,
	}
}

func validCoordinatorBlobsBundle(config clparams.BeaconChainConfig, blobs int) *eladapter.BlobsBundle { //nolint:gocritic // Tests use isolated config values.
	bundle := &eladapter.BlobsBundle{
		Commitments: make([][]byte, blobs),
		Proofs:      make([][]byte, blobs*int(config.NumberOfColumns)),
		Blobs:       make([][]byte, blobs),
	}
	for i := range bundle.Commitments {
		bundle.Commitments[i] = make([]byte, len(cltypes.KZGCommitment{}))
		bundle.Blobs[i] = make([]byte, cltypes.BytesPerBlob)
	}
	for i := range bundle.Proofs {
		bundle.Proofs[i] = make([]byte, len(cltypes.KZGProof{}))
	}
	return bundle
}

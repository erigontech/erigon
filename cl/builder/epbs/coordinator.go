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
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"sync"

	"github.com/erigontech/erigon/cl/builder/epbs/eladapter"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/gossip"
	clservices "github.com/erigontech/erigon/cl/phase1/network/services"
	clutils "github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
)

const weiPerGwei = 1_000_000_000

var (
	ErrPayloadNotReady         = errors.New("assembled payload is not ready")
	ErrRetainedPayloadCapacity = errors.New("retained payload capacity reached")
	ErrAuctionAlreadyTracked   = errors.New("auction already tracked")
	ErrSlotExpired             = errors.New("slot expired")
)

type PayloadAssembler interface {
	AssemblePayload(context.Context, *builder.Parameters) (uint64, error)
	// GetPayload transfers exclusive ownership of the returned payload to the caller.
	GetPayload(context.Context, uint64) (*eladapter.AssembledPayload, error)
}

type GossipPublisher interface {
	Publish(context.Context, string, []byte) error
}

type SlotInput struct {
	ValidatedPreferences    *cltypes.SignedProposerPreferences
	Slot                    uint64
	DependentRoot           common.Hash
	ParentBlockRoot         common.Hash
	ParentBlockHash         common.Hash
	ParentGasLimit          uint64
	PrevRandao              common.Hash
	Timestamp               uint64
	Withdrawals             []*types.Withdrawal
	BuilderIndex            uint64
	BuilderStatusIndex      uint64
	BuilderStatusSlot       uint64
	BuilderStatusParentRoot common.Hash
	BuilderPubkey           common.Bytes48
	GenesisValidatorsRoot   common.Hash
	BuilderActive           bool
	AvailableBidValueGwei   uint64
	freshness               slotInputFreshnessToken
}

type slotInputFreshnessToken struct {
	preferenceRoot common.Hash
	headStateSlot  uint64
	headBlockSlot  uint64
	buildOnFull    bool
}

type PayloadIdentity struct {
	Slot            uint64
	ParentBlockHash common.Hash
	ParentBlockRoot common.Hash
	BlockHash       common.Hash
}

type RetainedPayload struct {
	PayloadID         uint64
	Assembled         *eladapter.AssembledPayload
	ExecutionRequests *cltypes.ExecutionRequests
	BidValue          uint64
}

type Coordinator struct {
	beaconCfg   *clparams.BeaconChainConfig
	signer      Signer
	strategy    BidStrategy
	assembler   PayloadAssembler
	publisher   GossipPublisher
	maxRetained int

	mu        sync.Mutex
	slotFloor uint64
	auctions  map[auctionKey]*auctionEntry
	retained  map[PayloadIdentity]*retainedPayloadEntry
}

type auctionKey struct {
	slot            uint64
	parentBlockHash common.Hash
	parentBlockRoot common.Hash
	builderIndex    uint64
}

type auctionEntry struct {
	key      auctionKey
	bidValue uint64
	phase    auctionPhase
	identity PayloadIdentity
}

type auctionPhase uint8

const (
	auctionPhaseAdmitted auctionPhase = iota
	auctionPhasePublishing
	auctionPhaseRetained
)

type retainedPayloadEntry struct {
	payload *RetainedPayload
	owner   *auctionEntry
}

func NewCoordinator(
	beaconCfg *clparams.BeaconChainConfig,
	signer Signer,
	strategy BidStrategy,
	assembler PayloadAssembler,
	publisher GossipPublisher,
	maxRetained int,
) *Coordinator {
	return &Coordinator{
		beaconCfg: beaconCfg, signer: signer, strategy: strategy, assembler: assembler,
		publisher: publisher, maxRetained: maxRetained,
		auctions: make(map[auctionKey]*auctionEntry), retained: make(map[PayloadIdentity]*retainedPayloadEntry),
	}
}

func (c *Coordinator) RunSlot(ctx context.Context, input SlotInput) (*cltypes.SignedExecutionPayloadBid, error) {
	return c.runSlotGuarded(ctx, input, nil)
}

func (c *Coordinator) runSlotGuarded(
	ctx context.Context,
	input SlotInput,
	freshness SlotInputFreshness,
) (*cltypes.SignedExecutionPayloadBid, error) {
	if err := c.validateInput(ctx, input); err != nil {
		return nil, err
	}
	if !input.BuilderActive || input.AvailableBidValueGwei == 0 {
		return nil, nil
	}
	auction, err := c.admitAuction(input)
	if err != nil {
		return nil, err
	}
	keepAuction := false
	defer func() {
		if !keepAuction {
			c.releaseAuction(auction)
		}
	}()
	if err := validateSlotInputFreshness(ctx, input, freshness); err != nil {
		return nil, err
	}

	preferences := input.ValidatedPreferences.Clone().(*cltypes.SignedProposerPreferences)
	parameters := buildParameters(input, preferences.Message)
	payloadID, err := c.assembler.AssemblePayload(ctx, parameters)
	if err != nil {
		return nil, fmt.Errorf("epbs/coordinator: assemble payload: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	assembled, err := c.assembler.GetPayload(ctx, payloadID)
	if err != nil {
		return nil, fmt.Errorf("epbs/coordinator: get payload: %w", err)
	}
	if assembled == nil {
		return nil, ErrPayloadNotReady
	}
	if err := validateBuiltPayload(c.beaconCfg, input, preferences.Message, assembled); err != nil {
		return nil, fmt.Errorf("epbs/coordinator: %w", err)
	}
	if err := validateSlotInputFreshness(ctx, input, freshness); err != nil {
		return nil, err
	}

	candidateBidValue, ok, err := c.strategyBidValue(input.Slot, assembled.BlockValue)
	if err != nil || !ok {
		return nil, err
	}
	executionRequests, requestsRoot, err := decodeExecutionRequests(c.beaconCfg, assembled.RequestsBundle)
	if err != nil {
		return nil, fmt.Errorf("epbs/coordinator: execution requests: %w", err)
	}
	commitments, err := buildBlobCommitments(c.beaconCfg, input.Slot, assembled.BlobsBundle)
	if err != nil {
		return nil, fmt.Errorf("epbs/coordinator: blob bundle: %w", err)
	}
	bidValue, ok, err := c.reserveBid(auction, candidateBidValue, input.AvailableBidValueGwei)
	if err != nil || !ok {
		return nil, err
	}

	payload := assembled.Eth1Block
	bid := &cltypes.ExecutionPayloadBid{
		ParentBlockHash:       input.ParentBlockHash,
		ParentBlockRoot:       input.ParentBlockRoot,
		BlockHash:             payload.BlockHash,
		PrevRandao:            input.PrevRandao,
		FeeRecipient:          preferences.Message.FeeRecipient,
		GasLimit:              payload.GasLimit,
		BuilderIndex:          input.BuilderIndex,
		Slot:                  input.Slot,
		Value:                 bidValue,
		ExecutionPayment:      0,
		BlobKzgCommitments:    *commitments,
		ExecutionRequestsRoot: requestsRoot,
	}
	stateVersion := c.beaconCfg.GetCurrentStateVersion(input.Slot / c.beaconCfg.SlotsPerEpoch)
	forkVersion := clutils.Uint32ToBytes4(c.beaconCfg.GetForkVersionByVersion(stateVersion))
	domain, err := fork.ComputeDomain(c.beaconCfg.DomainBeaconBuilder[:], forkVersion, input.GenesisValidatorsRoot)
	if err != nil {
		return nil, fmt.Errorf("epbs/coordinator: builder domain: %w", err)
	}
	signingRoot, err := fork.ComputeSigningRoot(bid, domain)
	if err != nil {
		return nil, fmt.Errorf("epbs/coordinator: bid signing root: %w", err)
	}
	signature, err := c.signer.SignBid(ctx, common.Hash(signingRoot))
	if err != nil {
		return nil, fmt.Errorf("epbs/coordinator: sign bid: %w", err)
	}
	if signature == (common.Bytes96{}) {
		return nil, errors.New("epbs/coordinator: signer returned an empty signature")
	}
	signedBid := &cltypes.SignedExecutionPayloadBid{Message: bid, Signature: signature}
	encoded, err := signedBid.EncodeSSZ(nil)
	if err != nil {
		return nil, fmt.Errorf("epbs/coordinator: encode bid: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	identity := PayloadIdentity{
		Slot: input.Slot, ParentBlockHash: input.ParentBlockHash,
		ParentBlockRoot: input.ParentBlockRoot, BlockHash: payload.BlockHash,
	}
	retained := &RetainedPayload{
		PayloadID: payloadID, Assembled: assembled, ExecutionRequests: executionRequests, BidValue: bidValue,
	}
	owned, err := cloneRetainedPayload(c.beaconCfg, retained)
	if err != nil {
		return nil, fmt.Errorf("epbs/coordinator: retain payload snapshot: %w", err)
	}
	if err := validateSlotInputFreshness(ctx, input, freshness); err != nil {
		return nil, err
	}
	if err := c.retainForPublish(auction, identity, owned); err != nil {
		return nil, err
	}
	keepAuction = true
	publishErr := c.publisher.Publish(ctx, gossip.TopicNameExecutionPayloadBid, encoded)
	c.finishPublish(auction)
	if publishErr != nil {
		return signedBid, fmt.Errorf("epbs/coordinator: publish bid: %w", publishErr)
	}
	return signedBid, nil
}

func validateSlotInputFreshness(ctx context.Context, input SlotInput, freshness SlotInputFreshness) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if freshness == nil {
		return nil
	}
	if err := freshness.ValidateCurrent(ctx, input); err != nil {
		return fmt.Errorf("epbs/coordinator: slot input is stale: %w", err)
	}
	return ctx.Err()
}

func (c *Coordinator) Payload(identity PayloadIdentity) (*RetainedPayload, bool, error) {
	if c == nil {
		return nil, false, nil
	}
	c.mu.Lock()
	entry := c.retained[identity]
	c.mu.Unlock()
	if entry == nil {
		return nil, false, nil
	}
	cloned, err := cloneRetainedPayload(c.beaconCfg, entry.payload)
	if err != nil {
		return nil, false, fmt.Errorf("epbs/coordinator: clone retained payload: %w", err)
	}
	return cloned, true, nil
}

func (c *Coordinator) DropPayload(identity PayloadIdentity) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := c.retained[identity]
	if entry == nil || entry.owner.phase == auctionPhasePublishing {
		return false
	}
	delete(c.retained, identity)
	return true
}

// PruneExpiredBeforeSlot removes slots whose bid acceptance and payload reveal windows are known to have closed.
func (c *Coordinator) PruneExpiredBeforeSlot(slot uint64) int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if slot > c.slotFloor {
		c.slotFloor = slot
	}
	pruned := 0
	for key, entry := range c.auctions {
		if key.slot >= c.slotFloor || entry.phase == auctionPhasePublishing {
			continue
		}
		if retained := c.retained[entry.identity]; retained != nil && retained.owner == entry {
			delete(c.retained, entry.identity)
			pruned++
		}
		delete(c.auctions, key)
	}
	return pruned
}

func (c *Coordinator) validateInput(ctx context.Context, input SlotInput) error {
	if ctx == nil {
		return errors.New("epbs/coordinator: nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if c == nil || c.beaconCfg == nil || isNilDependency(c.signer) || isNilDependency(c.strategy) || isNilDependency(c.assembler) || isNilDependency(c.publisher) {
		return errors.New("epbs/coordinator: missing dependency")
	}
	if c.maxRetained <= 0 {
		return errors.New("epbs/coordinator: retained payload capacity must be positive")
	}
	if c.beaconCfg.SlotsPerEpoch == 0 {
		return errors.New("epbs/coordinator: slots per epoch must be positive")
	}
	if input.Slot <= c.beaconCfg.GenesisSlot {
		return fmt.Errorf("epbs/coordinator: slot %d is not after genesis slot %d", input.Slot, c.beaconCfg.GenesisSlot)
	}
	if c.beaconCfg.GetCurrentStateVersion(input.Slot/c.beaconCfg.SlotsPerEpoch) < clparams.GloasVersion {
		return errors.New("epbs/coordinator: slot is before gloas")
	}
	preferences := input.ValidatedPreferences
	if preferences == nil || preferences.Message == nil {
		return errors.New("epbs/coordinator: missing validated proposer preferences")
	}
	if preferences.Signature == (common.Bytes96{}) {
		return errors.New("epbs/coordinator: proposer preferences have an empty signature")
	}
	if input.Timestamp == 0 || input.ParentGasLimit == 0 {
		return errors.New("epbs/coordinator: incomplete slot context")
	}
	if input.DependentRoot == (common.Hash{}) || input.ParentBlockRoot == (common.Hash{}) || input.ParentBlockHash == (common.Hash{}) || input.GenesisValidatorsRoot == (common.Hash{}) {
		return errors.New("epbs/coordinator: incomplete root context")
	}
	if input.Withdrawals == nil {
		return errors.New("epbs/coordinator: withdrawals are unavailable")
	}
	for i, withdrawal := range input.Withdrawals {
		if withdrawal == nil {
			return fmt.Errorf("epbs/coordinator: nil withdrawal at index %d", i)
		}
	}
	if preferences.Message.ProposalSlot != input.Slot {
		return fmt.Errorf("epbs/coordinator: proposer preference slot %d does not match %d", preferences.Message.ProposalSlot, input.Slot)
	}
	if preferences.Message.DependentRoot != input.DependentRoot {
		return errors.New("epbs/coordinator: proposer preference dependent root mismatch")
	}
	if input.BuilderStatusSlot != input.Slot {
		return fmt.Errorf("epbs/coordinator: builder status slot %d does not match %d", input.BuilderStatusSlot, input.Slot)
	}
	if input.BuilderStatusIndex != input.BuilderIndex {
		return fmt.Errorf("epbs/coordinator: builder status index %d does not match %d", input.BuilderStatusIndex, input.BuilderIndex)
	}
	if input.BuilderStatusParentRoot != input.ParentBlockRoot {
		return errors.New("epbs/coordinator: builder status parent root mismatch")
	}
	if input.BuilderPubkey != c.signer.Pubkey() {
		return errors.New("epbs/coordinator: builder pubkey does not match signer")
	}
	return nil
}

func isNilDependency(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

func buildParameters(input SlotInput, preferences *cltypes.ProposerPreferences) *builder.Parameters {
	parentRoot := input.ParentBlockRoot
	slot := input.Slot
	targetGasLimit := preferences.TargetGasLimit
	withdrawals := make([]*types.Withdrawal, len(input.Withdrawals))
	for i, withdrawal := range input.Withdrawals {
		copied := *withdrawal
		withdrawals[i] = &copied
	}
	return &builder.Parameters{
		ParentHash: input.ParentBlockHash, Timestamp: input.Timestamp, PrevRandao: input.PrevRandao,
		SuggestedFeeRecipient: preferences.FeeRecipient, Withdrawals: withdrawals,
		ParentBeaconBlockRoot: &parentRoot, SlotNumber: &slot, TargetGasLimit: &targetGasLimit,
	}
}

func validateBuiltPayload(
	beaconCfg *clparams.BeaconChainConfig,
	input SlotInput,
	preferences *cltypes.ProposerPreferences,
	assembled *eladapter.AssembledPayload,
) error {
	if assembled == nil || assembled.Eth1Block == nil {
		return errors.New("missing execution payload")
	}
	payload := assembled.Eth1Block
	if payload.Version() != clparams.GloasVersion {
		return fmt.Errorf("execution payload version %s is not gloas", payload.Version())
	}
	if payload.BlockHash == (common.Hash{}) {
		return errors.New("execution payload block hash is zero")
	}
	if payload.BlockHash == input.ParentBlockHash {
		return errors.New("execution payload block hash equals parent block hash")
	}
	if payload.ParentHash != input.ParentBlockHash {
		return errors.New("execution payload parent hash mismatch")
	}
	if payload.PrevRandao != input.PrevRandao {
		return errors.New("execution payload prev_randao mismatch")
	}
	if payload.Time != input.Timestamp {
		return errors.New("execution payload timestamp mismatch")
	}
	if payload.SlotNumber != input.Slot {
		return errors.New("execution payload slot number mismatch")
	}
	if payload.FeeRecipient != preferences.FeeRecipient {
		return errors.New("execution payload fee recipient mismatch")
	}
	if !clservices.IsGasLimitTargetCompatible(input.ParentGasLimit, payload.GasLimit, preferences.TargetGasLimit) {
		return errors.New("execution payload gas limit is incompatible with proposer target")
	}
	if payload.Extra == nil || payload.Transactions == nil || payload.Withdrawals == nil || payload.BlockAccessList == nil {
		return errors.New("execution payload has uninitialized gloas fields")
	}
	if !withdrawalsMatch(payload.Withdrawals, input.Withdrawals) {
		return errors.New("execution payload withdrawals mismatch")
	}
	if assembled.BlockValue == nil || assembled.BlockValue.Sign() < 0 {
		return errors.New("invalid block value")
	}
	if assembled.RequestsBundle == nil || assembled.RequestsBundle.Requests == nil {
		return errors.New("execution requests are unavailable")
	}
	if assembled.BlobsBundle == nil {
		return errors.New("blob bundle is unavailable")
	}
	if uint64(payload.Withdrawals.Len()) > beaconCfg.MaxWithdrawalsPerPayload {
		return errors.New("execution payload has too many withdrawals")
	}
	return nil
}

func withdrawalsMatch(actual *solid.ListSSZ[*cltypes.Withdrawal], expected []*types.Withdrawal) bool {
	if actual.Len() != len(expected) {
		return false
	}
	for i, want := range expected {
		got := actual.Get(i)
		if got == nil || want == nil || got.Index != want.Index || got.Validator != want.Validator || got.Address != want.Address || got.Amount != want.Amount {
			return false
		}
	}
	return true
}

func (c *Coordinator) strategyBidValue(slot uint64, blockValue *big.Int) (uint64, bool, error) {
	amount := c.strategy.Decide(slot, new(big.Int).Set(blockValue))
	if amount == nil {
		return 0, false, nil
	}
	if amount.Sign() < 0 || amount.Cmp(blockValue) > 0 {
		return 0, false, errors.New("epbs/coordinator: strategy returned an invalid bid")
	}
	gwei := new(big.Int).Quo(new(big.Int).Set(amount), big.NewInt(weiPerGwei))
	if !gwei.IsUint64() {
		return 0, false, errors.New("epbs/coordinator: bid exceeds uint64 gwei")
	}
	value := gwei.Uint64()
	if value == 0 {
		return 0, false, nil
	}
	return value, true, nil
}

func decodeExecutionRequests(beaconCfg *clparams.BeaconChainConfig, bundle *typesproto.RequestsBundle) (*cltypes.ExecutionRequests, common.Hash, error) {
	requests := make([]hexutil.Bytes, len(bundle.Requests))
	for i := range bundle.Requests {
		requests[i] = bundle.Requests[i]
	}
	decoded, err := cltypes.DecodeExecutionRequestsList(beaconCfg, requests, clparams.GloasVersion)
	if err != nil {
		return nil, common.Hash{}, err
	}
	root, err := decoded.HashSSZ()
	if err != nil {
		return nil, common.Hash{}, err
	}
	return decoded, common.Hash(root), nil
}

func cloneRetainedPayload(beaconCfg *clparams.BeaconChainConfig, source *RetainedPayload) (*RetainedPayload, error) {
	if source == nil || source.Assembled == nil || source.Assembled.Eth1Block == nil || source.Assembled.BlockValue == nil || source.Assembled.RequestsBundle == nil || source.ExecutionRequests == nil {
		return nil, errors.New("retained payload is incomplete")
	}
	encodedPayload, err := source.Assembled.Eth1Block.EncodeSSZ(nil)
	if err != nil {
		return nil, err
	}
	executionPayload := cltypes.NewEth1Block(clparams.GloasVersion, beaconCfg)
	if err := executionPayload.DecodeSSZStrict(encodedPayload, int(clparams.GloasVersion)); err != nil {
		return nil, err
	}
	assembled := &eladapter.AssembledPayload{
		Eth1Block: executionPayload,
		RequestsBundle: &typesproto.RequestsBundle{
			Requests: cloneByteSlices(source.Assembled.RequestsBundle.Requests),
		},
		BlockValue: new(big.Int).Set(source.Assembled.BlockValue),
	}
	if source.Assembled.BlobsBundle != nil {
		assembled.BlobsBundle = &eladapter.BlobsBundle{
			Commitments: cloneByteSlices(source.Assembled.BlobsBundle.Commitments),
			Proofs:      cloneByteSlices(source.Assembled.BlobsBundle.Proofs),
			Blobs:       cloneByteSlices(source.Assembled.BlobsBundle.Blobs),
		}
	}
	return &RetainedPayload{
		PayloadID: source.PayloadID, Assembled: assembled,
		ExecutionRequests: source.ExecutionRequests.Clone().(*cltypes.ExecutionRequests),
		BidValue:          source.BidValue,
	}, nil
}

func cloneByteSlices(source [][]byte) [][]byte {
	if source == nil {
		return nil
	}
	cloned := make([][]byte, len(source))
	for i := range source {
		cloned[i] = bytes.Clone(source[i])
	}
	return cloned
}

func buildBlobCommitments(beaconCfg *clparams.BeaconChainConfig, slot uint64, bundle *eladapter.BlobsBundle) (*solid.ListSSZ[*cltypes.KZGCommitment], error) {
	maxBlobs := beaconCfg.GetBlobParameters(slot / beaconCfg.SlotsPerEpoch).MaxBlobsPerBlock
	commitments := solid.NewStaticProgressiveListSSZ[*cltypes.KZGCommitment](cltypes.MaxBlobsCommittmentsPerBlock, len(cltypes.KZGCommitment{}))
	if bundle == nil {
		return commitments, nil
	}
	if len(bundle.Commitments) != len(bundle.Blobs) {
		return nil, errors.New("commitments and blobs have different lengths")
	}
	if beaconCfg.NumberOfColumns == 0 || uint64(len(bundle.Blobs)) > math.MaxUint64/beaconCfg.NumberOfColumns {
		return nil, errors.New("invalid PeerDAS column count")
	}
	wantProofs := uint64(len(bundle.Blobs)) * beaconCfg.NumberOfColumns
	if uint64(len(bundle.Proofs)) != wantProofs {
		return nil, fmt.Errorf("%d proofs do not match %d blobs across %d columns", len(bundle.Proofs), len(bundle.Blobs), beaconCfg.NumberOfColumns)
	}
	if uint64(len(bundle.Commitments)) > maxBlobs {
		return nil, fmt.Errorf("%d blobs exceed maximum %d", len(bundle.Commitments), maxBlobs)
	}
	for i := range bundle.Proofs {
		if len(bundle.Proofs[i]) != len(cltypes.KZGProof{}) {
			return nil, fmt.Errorf("proof %d has length %d", i, len(bundle.Proofs[i]))
		}
	}
	for i := range bundle.Commitments {
		if len(bundle.Commitments[i]) != len(cltypes.KZGCommitment{}) {
			return nil, fmt.Errorf("commitment %d has length %d", i, len(bundle.Commitments[i]))
		}
		if len(bundle.Blobs[i]) != cltypes.BytesPerBlob {
			return nil, fmt.Errorf("blob %d has length %d", i, len(bundle.Blobs[i]))
		}
		commitment := new(cltypes.KZGCommitment)
		copy(commitment[:], bundle.Commitments[i])
		commitments.Append(commitment)
	}
	return commitments, nil
}

func (c *Coordinator) admitAuction(input SlotInput) (*auctionEntry, error) {
	key := auctionKey{
		slot: input.Slot, parentBlockHash: input.ParentBlockHash,
		parentBlockRoot: input.ParentBlockRoot, builderIndex: input.BuilderIndex,
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if input.Slot < c.slotFloor {
		return nil, ErrSlotExpired
	}
	if c.auctions[key] != nil {
		return nil, ErrAuctionAlreadyTracked
	}
	if len(c.auctions) >= c.maxRetained {
		return nil, ErrRetainedPayloadCapacity
	}
	entry := &auctionEntry{key: key}
	c.auctions[key] = entry
	return entry, nil
}

func (c *Coordinator) releaseAuction(entry *auctionEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.auctions[entry.key] == entry {
		delete(c.auctions, entry.key)
	}
}

func (c *Coordinator) reserveBid(entry *auctionEntry, candidate, available uint64) (uint64, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry.key.slot < c.slotFloor || c.auctions[entry.key] != entry {
		return 0, false, ErrSlotExpired
	}
	var outstanding uint64
	for _, tracked := range c.auctions {
		if tracked == entry {
			continue
		}
		if math.MaxUint64-outstanding < tracked.bidValue {
			outstanding = math.MaxUint64
			break
		}
		outstanding += tracked.bidValue
	}
	if outstanding >= available {
		return 0, false, nil
	}
	value := min(candidate, available-outstanding)
	if value == 0 {
		return 0, false, nil
	}
	entry.bidValue = value
	return value, true, nil
}

func (c *Coordinator) retainForPublish(entry *auctionEntry, identity PayloadIdentity, payload *RetainedPayload) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry.key.slot < c.slotFloor || c.auctions[entry.key] != entry {
		return ErrSlotExpired
	}
	if c.retained[identity] != nil {
		return errors.New("epbs/coordinator: payload identity already retained")
	}
	entry.phase = auctionPhasePublishing
	entry.identity = identity
	c.retained[identity] = &retainedPayloadEntry{payload: payload, owner: entry}
	return nil
}

func (c *Coordinator) finishPublish(entry *auctionEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.auctions[entry.key] != entry || entry.phase != auctionPhasePublishing {
		return
	}
	if entry.key.slot < c.slotFloor {
		if retained := c.retained[entry.identity]; retained != nil && retained.owner == entry {
			delete(c.retained, entry.identity)
		}
		delete(c.auctions, entry.key)
		return
	}
	entry.phase = auctionPhaseRetained
}

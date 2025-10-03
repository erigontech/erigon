// Copyright 2025 The Erigon Authors
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

package executiontests

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/jinzhu/copier"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/consensus/merge"
	"github.com/erigontech/erigon/execution/engineapi"
	enginetypes "github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/types"
)

type MockClOption func(*MockCl)

func WithMockClState(state *MockClState) MockClOption {
	return func(cl *MockCl) {
		cl.state = state
	}
}

type MockCl struct {
	logger                log.Logger
	engineApiClient       *engineapi.JsonRpcClient
	suggestedFeeRecipient common.Address
	genesis               common.Hash
	state                 *MockClState
}

func NewMockCl(logger log.Logger, elClient *engineapi.JsonRpcClient, genesis *types.Block, opts ...MockClOption) *MockCl {
	mcl := &MockCl{
		logger:                logger,
		engineApiClient:       elClient,
		suggestedFeeRecipient: genesis.Coinbase(),
		genesis:               genesis.Hash(),
		state: &MockClState{
			ParentElBlock:     genesis.Hash(),
			ParentElTimestamp: genesis.Time(),
			ParentClBlockRoot: big.NewInt(999_999_999),
			ParentRandao:      big.NewInt(0),
		},
	}
	for _, opt := range opts {
		opt(mcl)
	}
	return mcl
}

// BuildCanonicalBlock builds a new block and sets it as canonical.
func (cl *MockCl) BuildCanonicalBlock(ctx context.Context, opts ...BlockBuildingOption) (*MockClPayload, error) {
	clPayload, err := cl.BuildNewPayload(ctx, opts...)
	if err != nil {
		return nil, err
	}
	status, err := cl.InsertNewPayload(ctx, clPayload)
	if err != nil {
		return nil, err
	}
	if status.Status != enginetypes.ValidStatus {
		return nil, fmt.Errorf("unexpected status when inserting payload for canonical block: %s", status.Status)
	}
	err = cl.UpdateForkChoice(ctx, clPayload)
	if err != nil {
		return nil, err
	}
	return clPayload, nil
}

// BuildNewPayload builds a new payload on top of the lastNode canonical block. To help with testing forking, the parent
// block can be overridden by passing an option.
func (cl *MockCl) BuildNewPayload(ctx context.Context, opts ...BlockBuildingOption) (*MockClPayload, error) {
	options := cl.applyBlockBuildingOptions(opts...)
	forkChoiceState := enginetypes.ForkChoiceState{
		HeadHash:           cl.state.ParentElBlock,
		SafeBlockHash:      cl.genesis,
		FinalizedBlockHash: cl.genesis,
	}
	var timestamp uint64
	if options.timestamp != nil {
		timestamp = *options.timestamp
	} else {
		timestamp = cl.state.ParentElTimestamp + 1
	}
	if options.waitUntilTimestamp {
		waitDuration := time.Until(time.Unix(int64(timestamp), 0)) + 100*time.Millisecond
		cl.logger.Debug("[mock-cl] waiting until", "time", timestamp, "duration", waitDuration)
		err := common.Sleep(ctx, waitDuration)
		if err != nil {
			return nil, err
		}
	}
	parentBeaconBlockRoot := common.BigToHash(cl.state.ParentClBlockRoot)
	payloadAttributes := enginetypes.PayloadAttributes{
		Timestamp:             hexutil.Uint64(timestamp),
		PrevRandao:            common.BigToHash(cl.state.ParentRandao),
		SuggestedFeeRecipient: cl.suggestedFeeRecipient,
		Withdrawals:           make([]*types.Withdrawal, 0),
		ParentBeaconBlockRoot: &parentBeaconBlockRoot,
	}
	cl.logger.Debug("[mock-cl] building block", "timestamp", timestamp)
	// start the block building process
	fcuRes, err := retryEngineSyncing(ctx, func() (*enginetypes.ForkChoiceUpdatedResponse, enginetypes.EngineStatus, error) {
		r, err := cl.engineApiClient.ForkchoiceUpdatedV3(ctx, &forkChoiceState, &payloadAttributes)
		if err != nil {
			return nil, "", err
		}
		return r, r.PayloadStatus.Status, err
	})
	if err != nil {
		return nil, err
	}
	if fcuRes.PayloadStatus.Status != enginetypes.ValidStatus {
		return nil, fmt.Errorf("payload status of block building fcu is not valid: %s", fcuRes.PayloadStatus.Status)
	}
	// get the newly built block
	newPayload, err := cl.engineApiClient.GetPayloadV4(ctx, *fcuRes.PayloadId)
	if err != nil {
		return nil, err
	}
	return &MockClPayload{newPayload, payloadAttributes.ParentBeaconBlockRoot}, nil
}

// InsertNewPayload validates a new payload and inserts it into the engine. Note it does not update the fork choice.
func (cl *MockCl) InsertNewPayload(ctx context.Context, p *MockClPayload) (*enginetypes.PayloadStatus, error) {
	elPayload := p.ExecutionPayload
	clParentBlockRoot := p.ParentBeaconBlockRoot
	return retryEngineSyncing(ctx, func() (*enginetypes.PayloadStatus, enginetypes.EngineStatus, error) {
		r, err := cl.engineApiClient.NewPayloadV4(ctx, elPayload, []common.Hash{}, clParentBlockRoot, []hexutil.Bytes{})
		if err != nil {
			return nil, "", err
		}
		return r, r.Status, err
	})
}

// UpdateForkChoice updates the fork choice to the given block. Genesis is always set as safe and finalised.
func (cl *MockCl) UpdateForkChoice(ctx context.Context, p *MockClPayload) error {
	head := p.ExecutionPayload.BlockHash
	forkChoiceState := enginetypes.ForkChoiceState{
		HeadHash:           head,
		SafeBlockHash:      cl.genesis,
		FinalizedBlockHash: cl.genesis,
	}
	fcuRes, err := retryEngineSyncing(ctx, func() (*enginetypes.ForkChoiceUpdatedResponse, enginetypes.EngineStatus, error) {
		r, err := cl.engineApiClient.ForkchoiceUpdatedV3(ctx, &forkChoiceState, nil)
		if err != nil {
			return nil, "", err
		}
		return r, r.PayloadStatus.Status, err
	})
	if err != nil {
		return err
	}
	if fcuRes.PayloadStatus.Status != enginetypes.ValidStatus {
		return fmt.Errorf("payload status of fcu is not valid: %s", fcuRes.PayloadStatus.Status)
	}
	// move forward
	cl.state.ParentElBlock = head
	cl.state.ParentElTimestamp = p.ExecutionPayload.Timestamp.Uint64()
	cl.state.ParentClBlockRoot = new(big.Int).Add(p.ParentBeaconBlockRoot.Big(), big.NewInt(1))
	cl.state.ParentRandao = new(big.Int).Add(p.ExecutionPayload.PrevRandao.Big(), big.NewInt(1))
	return nil
}

func (cl *MockCl) State() *MockClState {
	return cl.state
}

func (cl *MockCl) applyBlockBuildingOptions(opts ...BlockBuildingOption) blockBuildingOptions {
	defaultOptions := blockBuildingOptions{}
	for _, opt := range opts {
		opt(&defaultOptions)
	}
	return defaultOptions
}

type BlockBuildingOption func(*blockBuildingOptions)

func WithTimestamp(timestamp uint64) BlockBuildingOption {
	return func(opts *blockBuildingOptions) {
		opts.timestamp = &timestamp
	}
}

func WithWaitUntilTimestamp() BlockBuildingOption {
	return func(opts *blockBuildingOptions) {
		opts.waitUntilTimestamp = true
	}
}

type blockBuildingOptions struct {
	timestamp          *uint64
	waitUntilTimestamp bool
}

func retryEngineSyncing[T any](ctx context.Context, f func() (*T, enginetypes.EngineStatus, error)) (*T, error) {
	operation := func() (*T, error) {
		res, status, err := f()
		if err != nil {
			return nil, backoff.Permanent(err) // do not retry
		}
		if status == enginetypes.SyncingStatus {
			return nil, errors.New("engine is syncing") // retry
		}
		return res, nil
	}
	// don't retry for too long
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	var backOff backoff.BackOff
	backOff = backoff.NewConstantBackOff(50 * time.Millisecond)
	backOff = backoff.WithContext(backOff, ctx)
	return backoff.RetryWithData(operation, backOff)
}

type MockClPayload struct {
	*enginetypes.GetPayloadResponse
	ParentBeaconBlockRoot *common.Hash
}

type MockClState struct {
	ParentElBlock     common.Hash
	ParentElTimestamp uint64
	ParentClBlockRoot *big.Int
	ParentRandao      *big.Int
}

func TamperMockClPayloadStateRoot(p *MockClPayload, stateRoot common.Hash) *MockClPayload {
	var pCopy MockClPayload
	err := copier.CopyWithOption(&pCopy, p, copier.Option{DeepCopy: true})
	if err != nil {
		panic(fmt.Sprintf("could not copy mock cl payload when trying to tamper it: %s", err))
	}
	pCopy.ExecutionPayload.StateRoot = stateRoot
	h := MockClPayloadToHeader(&pCopy)
	pCopy.ExecutionPayload.BlockHash = h.Hash()
	return &pCopy
}

func MockClPayloadToHeader(p *MockClPayload) *types.Header {
	elPayload := p.GetPayloadResponse.ExecutionPayload
	var bloom types.Bloom
	copy(bloom[:], elPayload.LogsBloom)
	txns := make(types.BinaryTransactions, len(elPayload.Transactions))
	for i, txn := range elPayload.Transactions {
		txns[i] = txn
	}
	header := &types.Header{
		ParentHash:            elPayload.ParentHash,
		Coinbase:              elPayload.FeeRecipient,
		Root:                  elPayload.StateRoot,
		Bloom:                 bloom,
		BaseFee:               (*big.Int)(elPayload.BaseFeePerGas),
		Extra:                 elPayload.ExtraData,
		Number:                big.NewInt(0).SetUint64(elPayload.BlockNumber.Uint64()),
		GasUsed:               uint64(elPayload.GasUsed),
		GasLimit:              uint64(elPayload.GasLimit),
		Time:                  uint64(elPayload.Timestamp),
		MixDigest:             elPayload.PrevRandao,
		UncleHash:             empty.UncleHash,
		Difficulty:            merge.ProofOfStakeDifficulty,
		Nonce:                 merge.ProofOfStakeNonce,
		ReceiptHash:           elPayload.ReceiptsRoot,
		TxHash:                types.DeriveSha(txns),
		BlobGasUsed:           (*uint64)(elPayload.BlobGasUsed),
		ExcessBlobGas:         (*uint64)(elPayload.ExcessBlobGas),
		ParentBeaconBlockRoot: p.ParentBeaconBlockRoot,
	}
	if elPayload.Withdrawals != nil {
		wh := types.DeriveSha(types.Withdrawals(elPayload.Withdrawals))
		header.WithdrawalsHash = &wh
	}
	requests := make(types.FlatRequests, 0)
	for _, r := range p.ExecutionRequests {
		requests = append(requests, types.FlatRequest{Type: r[0], RequestData: r[1:]})
	}
	header.RequestsHash = requests.Hash()
	return header
}

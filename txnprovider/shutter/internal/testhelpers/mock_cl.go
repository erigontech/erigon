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

package testhelpers

import (
	"context"
	"fmt"
	"math/big"
	"time"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/turbo/engineapi"
	enginetypes "github.com/erigontech/erigon/turbo/engineapi/engine_types"
	"github.com/erigontech/erigon/txnprovider/shutter"
)

type MockCl struct {
	slotCalculator        shutter.SlotCalculator
	engineApiClient       *engineapi.JsonRpcClient
	suggestedFeeRecipient libcommon.Address
	prevBlockHash         libcommon.Hash
	prevRandao            *big.Int
	prevBeaconBlockRoot   *big.Int
}

func NewMockCl(sc shutter.SlotCalculator, elClient *engineapi.JsonRpcClient, feeRecipient libcommon.Address, elGenesis *types.Block) *MockCl {
	return &MockCl{
		slotCalculator:        sc,
		engineApiClient:       elClient,
		suggestedFeeRecipient: feeRecipient,
		prevBlockHash:         elGenesis.Hash(),
		prevRandao:            big.NewInt(0),
		prevBeaconBlockRoot:   big.NewInt(10_000),
	}
}

func (cl *MockCl) BuildBlock(ctx context.Context, opts ...BlockBuildingOption) (*enginetypes.ExecutionPayload, error) {
	options := cl.applyBlockBuildingOptions(opts...)
	timestamp := cl.slotCalculator.CalcSlotStartTimestamp(options.slot)
	forkChoiceState := enginetypes.ForkChoiceState{
		FinalizedBlockHash: cl.prevBlockHash,
		SafeBlockHash:      cl.prevBlockHash,
		HeadHash:           cl.prevBlockHash,
	}

	parentBeaconBlockRoot := libcommon.BigToHash(cl.prevBeaconBlockRoot)
	payloadAttributes := enginetypes.PayloadAttributes{
		Timestamp:             hexutil.Uint64(timestamp),
		PrevRandao:            libcommon.BigToHash(cl.prevRandao),
		SuggestedFeeRecipient: cl.suggestedFeeRecipient,
		Withdrawals:           make([]*types.Withdrawal, 0),
		ParentBeaconBlockRoot: &parentBeaconBlockRoot,
	}

	// start block building process
	fcuRes, err := cl.engineApiClient.ForkchoiceUpdatedV3(ctx, &forkChoiceState, &payloadAttributes)
	if err != nil {
		return nil, err
	}
	if fcuRes.PayloadStatus.Status != enginetypes.ValidStatus {
		return nil, fmt.Errorf("payload status is not valid: %s", fcuRes.PayloadStatus.Status)
	}

	// give block builder time to build a block
	err = libcommon.Sleep(ctx, time.Duration(cl.slotCalculator.SecondsPerSlot())*time.Second)
	if err != nil {
		return nil, err
	}

	// get the newly built block
	payloadRes, err := cl.engineApiClient.GetPayloadV4(ctx, *fcuRes.PayloadId)
	if err != nil {
		return nil, err
	}

	// insert the newly built block
	payloadStatus, err := cl.engineApiClient.NewPayloadV4(ctx, payloadRes.ExecutionPayload, []libcommon.Hash{}, &parentBeaconBlockRoot, []hexutil.Bytes{})
	if err != nil {
		return nil, err
	}
	if payloadStatus.Status != enginetypes.ValidStatus {
		return nil, fmt.Errorf("payload status is not valid: %s", payloadStatus.Status)
	}

	// set the newly built block as canonical
	newHash := payloadRes.ExecutionPayload.BlockHash
	forkChoiceState = enginetypes.ForkChoiceState{
		FinalizedBlockHash: newHash,
		SafeBlockHash:      newHash,
		HeadHash:           newHash,
	}
	fcuRes, err = cl.engineApiClient.ForkchoiceUpdatedV3(ctx, &forkChoiceState, nil)
	if err != nil {
		return nil, err
	}
	if fcuRes.PayloadStatus.Status != enginetypes.ValidStatus {
		return nil, fmt.Errorf("payload status is not valid: %s", fcuRes.PayloadStatus.Status)
	}

	cl.prevBlockHash = newHash
	cl.prevRandao.Add(cl.prevRandao, big.NewInt(1))
	cl.prevBeaconBlockRoot.Add(cl.prevBeaconBlockRoot, big.NewInt(1))
	return payloadRes.ExecutionPayload, nil
}

func (cl *MockCl) applyBlockBuildingOptions(opts ...BlockBuildingOption) blockBuildingOptions {
	defaultOptions := blockBuildingOptions{
		slot: cl.slotCalculator.CalcCurrentSlot(),
	}
	for _, opt := range opts {
		opt(&defaultOptions)
	}
	return defaultOptions
}

type BlockBuildingOption func(*blockBuildingOptions)

func WithBlockBuildingSlot(slot uint64) BlockBuildingOption {
	return func(opts *blockBuildingOptions) {
		opts.slot = slot
	}
}

type blockBuildingOptions struct {
	slot uint64
}

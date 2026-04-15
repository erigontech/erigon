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

package engineapi

// testing_api.go implements the testing_ RPC namespace, specifically testing_buildBlockV1.
// Enable via --http.api=...,testing (e.g. --http.api eth,erigon,testing).
// This namespace MUST NOT be enabled on production networks.

import (
	"context"
	"errors"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/rpc"
)

// TestingAPI is the interface for the testing_ RPC namespace.
type TestingAPI interface {
	// BuildBlockV1 synchronously builds and returns an execution payload given a parent hash,
	// payload attributes, an optional transaction list, and optional extra data.
	// Unlike the two-phase forkchoiceUpdated+getPayload flow this call blocks until the block
	// is fully assembled and returns the result in a single response.
	//
	// transactions: nil  → draw from mempool (normal builder behaviour)
	//               []   → build an empty block (mempool bypassed, no txs)
	//               [...] → TODO: explicit tx list not yet supported; returns error
	//
	// NOTE: overriding extraData post-assembly means the BlockHash in the returned payload
	// will NOT match a block header that includes that extraData. Callers should treat the
	// result as a template when extraData is overridden.
	BuildBlockV1(ctx context.Context, parentHash common.Hash, payloadAttributes *engine_types.PayloadAttributes, transactions *[]hexutil.Bytes, extraData *hexutil.Bytes) (*engine_types.GetPayloadResponse, error)
}

// testingImpl is the concrete implementation of TestingAPI.
type testingImpl struct {
	server *EngineServer
}

// NewTestingImpl returns a new TestingAPI implementation wrapping the given EngineServer.
func NewTestingImpl(server *EngineServer) TestingAPI {
	return &testingImpl{server: server}
}

// NewTestingRPCEntry returns the rpc.API descriptor for the testing_ namespace.
func NewTestingRPCEntry(server *EngineServer) rpc.API {
	return rpc.API{
		Namespace: "testing",
		Public:    false,
		Service:   TestingAPI(NewTestingImpl(server)),
		Version:   "1.0",
	}
}

// BuildBlockV1 implements TestingAPI.
func (t *testingImpl) BuildBlockV1(
	ctx context.Context,
	parentHash common.Hash,
	payloadAttributes *engine_types.PayloadAttributes,
	transactions *[]hexutil.Bytes,
	extraData *hexutil.Bytes,
) (*engine_types.GetPayloadResponse, error) {
	if payloadAttributes == nil {
		return nil, &rpc.InvalidParamsError{Message: "payloadAttributes must not be null"}
	}

	// Explicit transaction list is not yet supported (requires proto extension).
	// TODO: implement forced_transactions via AssembleBlockRequest proto extension.
	if transactions != nil && len(*transactions) > 0 {
		return nil, &rpc.InvalidParamsError{Message: "explicit transaction list not yet supported in testing_buildBlockV1; use null for mempool or [] for empty block"}
	}

	// Validate parent block exists.
	parentHeader := t.server.chainRW.GetHeaderByHash(ctx, parentHash)
	if parentHeader == nil {
		return nil, &rpc.InvalidParamsError{Message: "unknown parent hash"}
	}

	timestamp := uint64(payloadAttributes.Timestamp)

	// Timestamp must be strictly greater than parent.
	if parentHeader.Time >= timestamp {
		return nil, &rpc.InvalidParamsError{Message: "payload timestamp must be greater than parent block timestamp"}
	}

	// Validate withdrawals presence.
	if err := t.server.checkWithdrawalsPresence(timestamp, payloadAttributes.Withdrawals); err != nil {
		return nil, err
	}

	// Determine version from timestamp for proper fork handling.
	version := clparams.BellatrixVersion
	switch {
	case t.server.config.IsAmsterdam(timestamp):
		version = clparams.GloasVersion
	case t.server.config.IsOsaka(timestamp):
		version = clparams.FuluVersion
	case t.server.config.IsPrague(timestamp):
		version = clparams.ElectraVersion
	case t.server.config.IsCancun(timestamp):
		version = clparams.DenebVersion
	case t.server.config.IsShanghai(timestamp):
		version = clparams.CapellaVersion
	}

	// Validate parentBeaconBlockRoot presence for Cancun+.
	if version >= clparams.DenebVersion && payloadAttributes.ParentBeaconBlockRoot == nil {
		return nil, &rpc.InvalidParamsError{Message: "parentBeaconBlockRoot required for Cancun and later forks"}
	}
	if version < clparams.DenebVersion && payloadAttributes.ParentBeaconBlockRoot != nil {
		return nil, &rpc.InvalidParamsError{Message: "parentBeaconBlockRoot not supported before Cancun"}
	}

	// Build the AssembleBlock parameters (mirrors forkchoiceUpdated logic).
	assembleParams := &builder.Parameters{
		ParentHash:            parentHash,
		Timestamp:             timestamp,
		PrevRandao:            payloadAttributes.PrevRandao,
		SuggestedFeeRecipient: payloadAttributes.SuggestedFeeRecipient,
		SlotNumber:            (*uint64)(payloadAttributes.SlotNumber),
	}
	if version >= clparams.CapellaVersion {
		assembleParams.Withdrawals = payloadAttributes.Withdrawals
	}
	if version >= clparams.DenebVersion {
		assembleParams.ParentBeaconBlockRoot = payloadAttributes.ParentBeaconBlockRoot
	}

	// Both steps share a single slot-duration budget so the total wall-clock
	// time of BuildBlockV1 is bounded to one slot (e.g. 12 s), not two.
	// Each step acquires the lock independently, matching production behaviour
	// where ForkChoiceUpdated and GetPayload are separate RPC calls.
	deadline := time.Now().Add(time.Duration(t.server.config.SecondsPerSlot()) * time.Second)

	// Step 1: AssembleBlock (locked scope).
	var payloadID uint64
	execBusy, err := func() (bool, error) {
		t.server.lock.Lock()
		defer t.server.lock.Unlock()

		var assembled execmodule.AssembleBlockResult
		var err error
		busy, err := waitForResponse(time.Until(deadline), func() (bool, error) {
			assembled, err = t.server.executionService.AssembleBlock(ctx, assembleParams)
			if err != nil {
				return false, err
			}
			return assembled.Busy, nil
		})
		payloadID = assembled.PayloadID
		return busy, err
	}()
	if err != nil {
		return nil, err
	}
	if execBusy {
		return nil, errors.New("execution service is busy, cannot build block")
	}

	// Step 2: GetAssembledBlock (separate locked scope).
	var assembled execmodule.AssembledBlockResult
	execBusy, err = func() (bool, error) {
		t.server.lock.Lock()
		defer t.server.lock.Unlock()

		var err error
		busy, err := waitForResponse(time.Until(deadline), func() (bool, error) {
			assembled, err = t.server.executionService.GetAssembledBlock(ctx, payloadID)
			if err != nil {
				return false, err
			}
			return assembled.Busy, nil
		})
		return busy, err
	}()
	if err != nil {
		return nil, err
	}
	if execBusy {
		return nil, errors.New("execution service is busy retrieving assembled block")
	}
	if assembled.Block == nil {
		return nil, errors.New("no assembled block data available for payload ID")
	}

	response, err := assembledBlockToPayloadResponse(assembled.Block, assembled.BlockValue, version)
	if err != nil {
		return nil, err
	}
	response.ShouldOverrideBuilder = false

	// Override extra data if provided. Note: the BlockHash in ExecutionPayload reflects the
	// originally built block; overriding ExtraData here means BlockHash will NOT match.
	if extraData != nil {
		response.ExecutionPayload.ExtraData = *extraData
	}

	return response, nil
}

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

// testing_api.go implements the testing_ RPC namespace (testing_buildBlockV1,
// testing_commitBlockV1).
// Enable via --http.api=...,testing (e.g. --http.api eth,erigon,testing).
// This namespace MUST NOT be enabled on production networks.

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/engineapi/engine_helpers"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/txnprovider"
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
	//               [...] → include every supplied transaction in order, or return an error
	BuildBlockV1(ctx context.Context, parentHash common.Hash, payloadAttributes *engine_types.PayloadAttributes, transactions *[]hexutil.Bytes, extraData *hexutil.Bytes) (*engine_types.GetPayloadResponse, error)

	// CommitBlockV1 builds a block on top of the current canonical head, inserts it into the
	// chain, and sets it as the new head — the equivalent of BuildBlockV1 followed by
	// engine_newPayload + engine_forkchoiceUpdated in a single call. Returns the hash of the
	// committed block. On any failure the canonical head is left unchanged, with one
	// exception: the fork choice update runs asynchronously in the execution module, so a
	// busy error returned when the slot budget expires mid fork choice does not stop the
	// head from advancing afterwards.
	// The transactions parameter follows the same semantics as BuildBlockV1.
	CommitBlockV1(ctx context.Context, payloadAttributes *engine_types.PayloadAttributes, transactions *[]hexutil.Bytes, extraData *hexutil.Bytes) (common.Hash, error)
}

// testingImpl is the concrete implementation of TestingAPI.
type testingImpl struct {
	server *EngineServer
}

// NewTestingImpl returns a new TestingAPI implementation wrapping the given EngineServer.
func NewTestingImpl(server *EngineServer) TestingAPI {
	return &testingImpl{server: server}
}

// decodeTxnProvider decodes explicit transactions, returning nil for the mempool path.
// Nonces are checked during execution: earlier calls can advance contract nonces.
func (t *testingImpl) decodeTxnProvider(transactions *[]hexutil.Bytes, blockNumber, timestamp uint64) (*staticTxnProvider, error) {
	if transactions == nil {
		return nil, nil
	}

	decoded := make([]types.Transaction, 0, len(*transactions))
	signer := types.MakeSigner(t.server.config, blockNumber, timestamp)
	for i, rawTx := range *transactions {
		tx, err := types.DecodeTransaction(rawTx)
		if err != nil {
			return nil, &rpc.InvalidParamsError{Message: fmt.Sprintf("transaction %d: decode error: %v", i, err)}
		}
		sender, err := signer.Sender(tx)
		if err != nil {
			return nil, &rpc.InvalidParamsError{Message: fmt.Sprintf("transaction %d: cannot recover sender: %v", i, err)}
		}
		tx.SetSender(sender)
		decoded = append(decoded, tx)
	}
	return &staticTxnProvider{txns: decoded}, nil
}

// NewTestingRPCEntry returns the rpc.API descriptor for the testing_ namespace.
func NewTestingRPCEntry(server *EngineServer) rpc.API {
	return rpc.API{
		Namespace: "testing",
		Public:    false,
		Service:   NewTestingImpl(server),
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

	// Validate parent block exists.
	parentHeader := t.server.chainRW.GetHeaderByHash(ctx, parentHash)
	if parentHeader == nil {
		return nil, &rpc.InvalidParamsError{Message: "unknown parent hash"}
	}

	assembled, version, err := t.assembleTestingBlock(ctx, parentHash, parentHeader, payloadAttributes, transactions, extraData, t.slotDeadline(ctx))
	if err != nil {
		return nil, err
	}

	response, err := assembledBlockToPayloadResponse(assembled.Block, assembled.BlockValue, version)
	if err != nil {
		return nil, err
	}
	response.ShouldOverrideBuilder = false

	return response, nil
}

// CommitBlockV1 implements TestingAPI.
func (t *testingImpl) CommitBlockV1(
	ctx context.Context,
	payloadAttributes *engine_types.PayloadAttributes,
	transactions *[]hexutil.Bytes,
	extraData *hexutil.Bytes,
) (common.Hash, error) {
	if payloadAttributes == nil {
		return common.Hash{}, &rpc.InvalidParamsError{Message: "payloadAttributes must not be null"}
	}

	deadline := t.slotDeadline(ctx)

	parentHeader := t.server.chainRW.CurrentHeader(ctx)
	if parentHeader == nil {
		return common.Hash{}, errors.New("no canonical head available")
	}

	// Preserve the current safe and finalized hashes: committing only advances the head.
	_, finalizedHash, safeHash, err := t.server.chainRW.GetForkChoice(ctx)
	if err != nil {
		return common.Hash{}, err
	}

	assembled, _, err := t.assembleTestingBlock(ctx, parentHeader.Hash(), parentHeader, payloadAttributes, transactions, extraData, deadline)
	if err != nil {
		return common.Hash{}, err
	}

	block := assembled.Block.Block
	blockHash := block.Hash()
	transactionsForValidation := slices.Clone(block.Transactions())
	for i := range transactionsForValidation {
		transactionsForValidation[i] = transactionsForValidation[i].Unwrap()
	}
	validationBlock := types.NewBlockFromNetwork(block.HeaderNoCopy(), &types.Body{
		Transactions: transactionsForValidation,
		Uncles:       block.Uncles(),
		Withdrawals:  block.Withdrawals(),
	}, block.BlockAccessListSidecar())

	err = func() error {
		t.server.lock.Lock()
		defer t.server.lock.Unlock()
		return t.server.chainRW.InsertBlock(ctx, block)
	}()
	if err != nil {
		return common.Hash{}, err
	}

	status, validationErr, busy, err := t.lockedStatusPoll(ctx, deadline, func() (execmodule.ExecutionStatus, *string, error) {
		s, v, _, err := t.server.chainRW.ValidateChain(ctx, validationBlock)
		return s, v, err
	})
	if err != nil {
		return common.Hash{}, err
	}
	if busy {
		return common.Hash{}, errors.New("execution service is busy, cannot validate block")
	}
	if status != execmodule.ExecutionStatusSuccess {
		return common.Hash{}, commitStatusError("block validation failed", status, validationErr)
	}

	status, validationErr, busy, err = t.lockedStatusPoll(ctx, deadline, func() (execmodule.ExecutionStatus, *string, error) {
		s, v, _, err := t.server.chainRW.UpdateForkChoice(ctx, blockHash, safeHash, finalizedHash)
		return s, v, err
	})
	if err != nil {
		return common.Hash{}, err
	}
	if busy {
		return common.Hash{}, errors.New("execution service is busy, cannot update fork choice")
	}
	switch status {
	case execmodule.ExecutionStatusSuccess:
	case execmodule.ExecutionStatusInvalidForkchoice:
		return common.Hash{}, &engine_helpers.InvalidForkchoiceStateErr
	case execmodule.ExecutionStatusReorgTooDeep:
		return common.Hash{}, &engine_helpers.ReorgTooDeepErr
	default:
		return common.Hash{}, commitStatusError("fork choice update failed", status, validationErr)
	}

	return blockHash, nil
}

// lockedPoll runs call under the engine lock, retrying while the execution
// service reports busy, until the deadline expires.
func (t *testingImpl) lockedPoll(ctx context.Context, deadline time.Time, call func() (bool, error)) (bool, error) {
	t.server.lock.Lock()
	defer t.server.lock.Unlock()
	return waitForResponse(ctx, time.Until(deadline), call)
}

func (t *testingImpl) lockedStatusPoll(ctx context.Context, deadline time.Time, call func() (execmodule.ExecutionStatus, *string, error)) (status execmodule.ExecutionStatus, validationErr *string, busy bool, err error) {
	busy, err = t.lockedPoll(ctx, deadline, func() (bool, error) {
		var callErr error
		status, validationErr, callErr = call()
		if callErr != nil {
			return false, callErr
		}
		return status == execmodule.ExecutionStatusBusy, nil
	})
	return status, validationErr, busy, err
}

func commitStatusError(what string, status execmodule.ExecutionStatus, validationErr *string) error {
	msg := fmt.Sprintf("%s: %s", what, status)
	if validationErr != nil {
		msg = fmt.Sprintf("%s: %s", msg, *validationErr)
	}
	return &rpc.CustomError{Code: rpc.ErrCodeDefault, Message: msg}
}

// slotDeadline bounds the total wall-clock time of a testing_ call to one slot
// (e.g. 12 s), honouring a shorter caller-supplied context deadline.
func (t *testingImpl) slotDeadline(ctx context.Context) time.Time {
	deadline := time.Now().Add(time.Duration(t.server.config.SecondsPerSlot()) * time.Second)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	return deadline
}

// assembleTestingBlock validates the payload attributes against the given parent and
// synchronously assembles a block on top of it. Each build step acquires the engine
// lock independently, matching production behaviour where ForkChoiceUpdated and
// GetPayload are separate RPC calls; both share the supplied deadline.
func (t *testingImpl) assembleTestingBlock(
	ctx context.Context,
	parentHash common.Hash,
	parentHeader *types.Header,
	payloadAttributes *engine_types.PayloadAttributes,
	transactions *[]hexutil.Bytes,
	extraData *hexutil.Bytes,
	deadline time.Time,
) (*execmodule.AssembledBlockResult, clparams.StateVersion, error) {
	// The polling timeout cannot interrupt a blocking execution call.
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	timestamp := uint64(payloadAttributes.Timestamp)

	// Timestamp must be strictly greater than parent.
	if parentHeader.Time >= timestamp {
		return nil, 0, &rpc.InvalidParamsError{Message: "payload timestamp must be greater than parent block timestamp"}
	}

	if extraData != nil && uint64(len(*extraData)) > params.MaximumExtraDataSize {
		return nil, 0, &rpc.InvalidParamsError{Message: fmt.Sprintf("extraData longer than %d bytes (%d)", params.MaximumExtraDataSize, len(*extraData))}
	}

	if err := t.server.checkWithdrawals(timestamp, payloadAttributes.Withdrawals); err != nil {
		return nil, 0, err
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
		return nil, 0, &rpc.InvalidParamsError{Message: "parentBeaconBlockRoot required for Cancun and later forks"}
	}
	if version < clparams.DenebVersion && payloadAttributes.ParentBeaconBlockRoot != nil {
		return nil, 0, &rpc.InvalidParamsError{Message: "parentBeaconBlockRoot not supported before Cancun"}
	}

	// Validate slotNumber presence for Glamsterdam+.
	if version >= clparams.GloasVersion && payloadAttributes.SlotNumber == nil {
		return nil, 0, &rpc.InvalidParamsError{Message: "slotNumber required for Glamsterdam and later forks"}
	}
	if version < clparams.GloasVersion && payloadAttributes.SlotNumber != nil {
		return nil, 0, &rpc.InvalidParamsError{Message: "slotNumber not supported before Glamsterdam"}
	}

	// Validate targetGasLimit presence for Glamsterdam+.
	if version >= clparams.GloasVersion && payloadAttributes.TargetGasLimit == nil {
		return nil, 0, &rpc.InvalidParamsError{Message: "targetGasLimit required for Glamsterdam and later forks"}
	}
	if version < clparams.GloasVersion && payloadAttributes.TargetGasLimit != nil {
		return nil, 0, &rpc.InvalidParamsError{Message: "targetGasLimit not supported before Glamsterdam"}
	}

	customProvider, err := t.decodeTxnProvider(transactions, parentHeader.Number.Uint64()+1, timestamp)
	if err != nil {
		return nil, 0, err
	}

	// Build the AssembleBlock parameters (mirrors forkchoiceUpdated logic).
	// ExtraData always overrides the builder's configured default (which varies by
	// erigon version) to keep testing_ block hashes deterministic, matching geth.
	assembleParams := &builder.Parameters{
		ParentHash:            parentHash,
		Timestamp:             timestamp,
		PrevRandao:            payloadAttributes.PrevRandao,
		SuggestedFeeRecipient: payloadAttributes.SuggestedFeeRecipient,
		ExtraData:             []byte{},
	}
	if customProvider != nil {
		assembleParams.CustomTxnProvider = customProvider
		assembleParams.RequireAllTransactions = true
	}
	if extraData != nil {
		assembleParams.ExtraData = *extraData
	}
	if version >= clparams.CapellaVersion {
		assembleParams.Withdrawals = payloadAttributes.Withdrawals
	}
	if version >= clparams.DenebVersion {
		assembleParams.ParentBeaconBlockRoot = payloadAttributes.ParentBeaconBlockRoot
	}
	if version >= clparams.GloasVersion {
		assembleParams.SlotNumber = (*uint64)(payloadAttributes.SlotNumber)
		assembleParams.TargetGasLimit = (*uint64)(payloadAttributes.TargetGasLimit)
	}

	var payloadID uint64
	execBusy, err := t.lockedPoll(ctx, deadline, func() (bool, error) {
		assembled, err := t.server.executionService.AssembleBlock(ctx, assembleParams)
		if err != nil {
			return false, err
		}
		payloadID = assembled.PayloadID
		return assembled.Busy, nil
	})
	if err != nil {
		return nil, 0, err
	}
	if execBusy {
		return nil, 0, errors.New("execution service is busy, cannot build block")
	}

	var assembled execmodule.AssembledBlockResult
	execBusy, err = t.lockedPoll(ctx, deadline, func() (bool, error) {
		var err error
		assembled, err = t.server.executionService.GetAssembledBlock(ctx, payloadID)
		if err != nil {
			return false, err
		}
		return assembled.Busy, nil
	})
	if err != nil {
		return nil, 0, err
	}
	if execBusy {
		return nil, 0, errors.New("execution service is busy retrieving assembled block")
	}
	// Discarding an expired build may make retrieval report an unknown payload.
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, 0, ctxErr
	}
	if assembled.Block == nil {
		return nil, 0, errors.New("no assembled block data available for payload ID")
	}
	if customProvider != nil {
		included := assembled.Block.Block.Transactions()
		for i, expected := range customProvider.txns {
			if i >= len(included) || included[i].Hash() != expected.Hash() {
				return nil, 0, fmt.Errorf("transaction %d (%s) was not included at its requested position", i, expected.Hash())
			}
		}
		if len(included) != len(customProvider.txns) {
			return nil, 0, errors.New("assembled block contains unrequested transactions")
		}
	}

	return &assembled, version, nil
}

// staticTxnProvider is consumed by one build goroutine. It serves every list
// entry once, including duplicates, so execution can report the first invalid entry.
// The original list is kept for the final check against the assembled block.
type staticTxnProvider struct {
	txns []types.Transaction
	next int
}

func (s *staticTxnProvider) ProvideTxns(_ context.Context, opts ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	o := txnprovider.ApplyProvideOptions(opts...)
	if s.next == len(s.txns) || o.Amount <= 0 {
		return nil, nil
	}
	start := s.next
	s.next += min(o.Amount, len(s.txns)-s.next)
	return s.txns[start:s.next:s.next], nil
}

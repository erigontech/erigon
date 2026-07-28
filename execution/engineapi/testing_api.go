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
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	execctx "github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/engineapi/engine_helpers"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
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
	//               [...] → build a block containing exactly these transactions (strict nonce check)
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
	logger log.Logger
	db     kv.TemporalRoDB
}

// NewTestingImpl returns a new TestingAPI implementation wrapping the given EngineServer.
func NewTestingImpl(server *EngineServer, logger log.Logger, db kv.TemporalRoDB) TestingAPI {
	return &testingImpl{server: server, logger: logger, db: db}
}

// decodeTxnProvider decodes raw transactions into a TxnProvider.
// Returns nil if transactions is nil (mempool path).
// Opens a single temporal DB transaction for all nonce lookups to avoid per-sender overhead.
func (t *testingImpl) decodeTxnProvider(ctx context.Context, transactions *[]hexutil.Bytes, blockNumber, timestamp uint64) (txnprovider.TxnProvider, error) {
	if transactions == nil {
		return nil, nil
	}

	var reader *state.ReaderV3
	if t.db != nil {
		dbTx, err := t.db.BeginTemporalRo(ctx)
		if err != nil {
			return nil, fmt.Errorf("could not begin temporal transaction: %w", err)
		}
		defer dbTx.Rollback()
		sd, err := execctx.NewSharedDomains(ctx, dbTx, t.logger, execctx.WithoutDeferredBranchUpdates())
		if err != nil {
			return nil, fmt.Errorf("NewSharedDomains error: %w", err)
		}
		defer sd.Close()
		reader = state.NewReaderV3(sd.AsGetter(dbTx))
	}

	decoded := make([]types.Transaction, 0, len(*transactions))
	signer := types.MakeSigner(t.server.config, blockNumber, timestamp)
	expectedNonce := make(map[accounts.Address]uint64, len(*transactions))
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
		if _, seen := expectedNonce[sender]; !seen {
			var stateNonce uint64
			if reader != nil {
				acc, err := reader.ReadAccountData(accounts.InternAddress(sender.Value()))
				if err != nil {
					return nil, fmt.Errorf("ReadAccountData error: %w", err)
				}
				if acc != nil {
					stateNonce = acc.Nonce
				}
			}
			expectedNonce[sender] = stateNonce
		}
		want := expectedNonce[sender]
		got := tx.GetNonce()
		if got > want {
			return nil, &rpc.CustomError{Code: rpc.ErrCodeDefault, Message: fmt.Sprintf("nonce too high: address %v, tx: %d state: %d", sender.Value(), got, want)}
		}
		if got < want {
			return nil, &rpc.CustomError{Code: rpc.ErrCodeDefault, Message: fmt.Sprintf("nonce too low: address %v, tx: %d state: %d", sender.Value(), got, want)}
		}
		expectedNonce[sender]++
		decoded = append(decoded, tx)
	}
	return &staticTxnProvider{txns: decoded}, nil
}

// NewTestingRPCEntry returns the rpc.API descriptor for the testing_ namespace.
func NewTestingRPCEntry(server *EngineServer, logger log.Logger, db kv.TemporalRoDB) rpc.API {
	return rpc.API{
		Namespace: "testing",
		Public:    false,
		Service:   TestingAPI(NewTestingImpl(server, logger, db)),
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
	blockNumber := block.NumberU64()

	var encodedBAL []byte
	if assembled.Block.BlockAccessList != nil {
		if encodedBAL, err = types.EncodeBlockAccessListBytes(assembled.Block.BlockAccessList); err != nil {
			return common.Hash{}, err
		}
	}

	err = func() error {
		t.server.lock.Lock()
		defer t.server.lock.Unlock()
		return t.server.chainRW.InsertBlock(ctx, block, encodedBAL)
	}()
	if err != nil {
		return common.Hash{}, err
	}

	status, validationErr, busy, err := t.lockedStatusPoll(deadline, func() (execmodule.ExecutionStatus, *string, error) {
		s, v, _, err := t.server.chainRW.ValidateChain(ctx, blockHash, blockNumber)
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

	status, validationErr, busy, err = t.lockedStatusPoll(deadline, func() (execmodule.ExecutionStatus, *string, error) {
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

// lockedStatusPoll runs call under the engine lock, retrying while the execution
// service reports busy, until the deadline expires.
func (t *testingImpl) lockedStatusPoll(deadline time.Time, call func() (execmodule.ExecutionStatus, *string, error)) (status execmodule.ExecutionStatus, validationErr *string, busy bool, err error) {
	t.server.lock.Lock()
	defer t.server.lock.Unlock()
	busy, err = waitForResponse(time.Until(deadline), func() (bool, error) {
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
	timestamp := uint64(payloadAttributes.Timestamp)

	// Timestamp must be strictly greater than parent.
	if parentHeader.Time >= timestamp {
		return nil, 0, &rpc.InvalidParamsError{Message: "payload timestamp must be greater than parent block timestamp"}
	}

	if extraData != nil && uint64(len(*extraData)) > params.MaximumExtraDataSize {
		return nil, 0, &rpc.InvalidParamsError{Message: fmt.Sprintf("extraData longer than %d bytes (%d)", params.MaximumExtraDataSize, len(*extraData))}
	}

	// Validate withdrawals presence.
	if err := t.server.checkWithdrawalsPresence(timestamp, payloadAttributes.Withdrawals); err != nil {
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

	customProvider, err := t.decodeTxnProvider(ctx, transactions, parentHeader.Number.Uint64()+1, timestamp)
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
		CustomTxnProvider:     customProvider,
		ExtraData:             []byte{},
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
		return nil, 0, err
	}
	if execBusy {
		return nil, 0, errors.New("execution service is busy, cannot build block")
	}

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
		return nil, 0, err
	}
	if execBusy {
		return nil, 0, errors.New("execution service is busy retrieving assembled block")
	}
	if assembled.Block == nil {
		return nil, 0, errors.New("no assembled block data available for payload ID")
	}

	return &assembled, version, nil
}

// staticTxnProvider is a TxnProvider that yields a fixed transaction list exactly once,
// then returns nil on every subsequent call. Used only by the testing_ namespace.
type staticTxnProvider struct {
	txns []types.Transaction
	done atomic.Bool
}

func (s *staticTxnProvider) ProvideTxns(_ context.Context, _ ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	if !s.done.CompareAndSwap(false, true) {
		return nil, nil
	}
	txns := s.txns
	s.txns = nil // release for GC after handing off
	return txns, nil
}

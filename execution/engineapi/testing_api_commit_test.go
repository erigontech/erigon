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

package engineapi

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
)

// commitRecorder captures the chain mutations performed by CommitBlockV1 so
// tests can assert on what was inserted and which fork choice was applied.
type commitRecorder struct {
	inserted  []*types.RawBlock
	validated []common.Hash
	fcuCalled bool
	fcuHead   common.Hash
	fcuSafe   common.Hash
	fcuFinal  common.Hash

	// stub responses
	safeHash    common.Hash
	finalHash   common.Hash
	validateRes execmodule.ValidationResult
	fcuRes      execmodule.ForkChoiceResult
	insertRes   execmodule.ExecutionStatus
	insertErr   error
}

// newCommitStub wires a stubExecutionModule for the full CommitBlockV1 flow:
// current head lookup, block assembly, insertion, validation and fork choice.
func newCommitStub(parentHdr *types.Header, assembled *types.BlockWithReceipts, rec *commitRecorder) *stubExecutionModule {
	return &stubExecutionModule{
		currentHeaderFunc: func(_ context.Context) (*types.Header, error) {
			return parentHdr, nil
		},
		assembleBlockFunc: func(_ context.Context, _ *builder.Parameters) (execmodule.AssembleBlockResult, error) {
			return execmodule.AssembleBlockResult{PayloadID: 7, Busy: false}, nil
		},
		getAssembledBlockFunc: func(_ context.Context, _ uint64) (execmodule.AssembledBlockResult, error) {
			return execmodule.AssembledBlockResult{Block: assembled, BlockValue: uint256.NewInt(0), Busy: false}, nil
		},
		insertBlocksFunc: func(_ context.Context, blocks []*types.RawBlock) (execmodule.ExecutionStatus, error) {
			rec.inserted = append(rec.inserted, blocks...)
			return rec.insertRes, rec.insertErr
		},
		validateChainFunc: func(_ context.Context, hash common.Hash, _ uint64) (execmodule.ValidationResult, error) {
			rec.validated = append(rec.validated, hash)
			return rec.validateRes, nil
		},
		getForkChoiceFunc: func(_ context.Context) (execmodule.ForkChoiceState, error) {
			return execmodule.ForkChoiceState{
				HeadHash:      parentHdr.Hash(),
				SafeHash:      rec.safeHash,
				FinalizedHash: rec.finalHash,
			}, nil
		},
		updateForkChoiceFunc: func(_ context.Context, head, safe, finalized common.Hash) (execmodule.ForkChoiceResult, error) {
			rec.fcuCalled = true
			rec.fcuHead = head
			rec.fcuSafe = safe
			rec.fcuFinal = finalized
			return rec.fcuRes, nil
		},
	}
}

func successRecorder() *commitRecorder {
	return &commitRecorder{
		safeHash:    common.Hash{0x5a},
		finalHash:   common.Hash{0xf1},
		validateRes: execmodule.ValidationResult{ValidationStatus: execmodule.ExecutionStatusSuccess},
		fcuRes:      execmodule.ForkChoiceResult{Status: execmodule.ExecutionStatusSuccess},
		insertRes:   execmodule.ExecutionStatusSuccess,
	}
}

func TestCommitBlockV1(t *testing.T) {
	t.Parallel()

	const parentTimestamp = 1000
	parentHdr := makeParentHeader(parentTimestamp)
	parentHash := parentHdr.Hash()
	blockHash := common.HexToHash("0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd")

	defaultAssembled := func() *types.BlockWithReceipts {
		return makeAssembledBlock(blockHash, parentHash, common.Hash{0xcc}, 101, parentTimestamp+1, nil, 30_000_000, 0, 0, 0)
	}
	// newEnv wires the default happy-path stack; tests mutate the returned
	// recorder or stub before calling CommitBlockV1 to vary behaviour.
	newEnv := func(assembled *types.BlockWithReceipts) (TestingAPI, *commitRecorder, *stubExecutionModule) {
		rec := successRecorder()
		stub := newCommitStub(parentHdr, assembled, rec)
		return newTestingAPI(allForksChainConfig(), stub), rec, stub
	}

	t.Run("nil payloadAttributes", func(t *testing.T) {
		t.Parallel()
		api := newTestingAPI(allForksChainConfig(), &stubExecutionModule{})
		hash, err := api.CommitBlockV1(context.Background(), nil, nil, nil)
		require.Error(t, err)
		assert.Equal(t, common.Hash{}, hash)
		var rpcErr *rpc.InvalidParamsError
		require.ErrorAs(t, err, &rpcErr)
		assert.Contains(t, rpcErr.Message, "payloadAttributes must not be null")
	})

	t.Run("no canonical head", func(t *testing.T) {
		t.Parallel()
		// Default stub CurrentHeader returns nil.
		api := newTestingAPI(allForksChainConfig(), &stubExecutionModule{})
		hash, err := api.CommitBlockV1(context.Background(), validPayloadAttrs(parentTimestamp), nil, nil)
		require.Error(t, err)
		assert.Equal(t, common.Hash{}, hash)
		assert.Contains(t, err.Error(), "no canonical head")
	})

	t.Run("timestamp not greater than head", func(t *testing.T) {
		t.Parallel()
		api, rec, _ := newEnv(nil)
		attrs := validPayloadAttrs(parentTimestamp)
		attrs.Timestamp = hexutil.Uint64(parentTimestamp)
		hash, err := api.CommitBlockV1(context.Background(), attrs, nil, nil)
		require.Error(t, err)
		assert.Equal(t, common.Hash{}, hash)
		var rpcErr *rpc.InvalidParamsError
		require.ErrorAs(t, err, &rpcErr)
		assert.Contains(t, rpcErr.Message, "payload timestamp must be greater than parent")
		assert.Empty(t, rec.inserted)
		assert.False(t, rec.fcuCalled)
	})

	t.Run("invalid transaction bytes", func(t *testing.T) {
		t.Parallel()
		api, rec, _ := newEnv(nil)
		txs := []hexutil.Bytes{{0x01, 0x02}}
		hash, err := api.CommitBlockV1(context.Background(), validPayloadAttrs(parentTimestamp), &txs, nil)
		require.Error(t, err)
		assert.Equal(t, common.Hash{}, hash)
		var rpcErr *rpc.InvalidParamsError
		require.ErrorAs(t, err, &rpcErr)
		assert.Contains(t, rpcErr.Message, "decode error")
		assert.Empty(t, rec.inserted)
		assert.False(t, rec.fcuCalled)
	})

	t.Run("happy path commits block and sets head", func(t *testing.T) {
		t.Parallel()
		api, rec, _ := newEnv(defaultAssembled())

		hash, err := api.CommitBlockV1(context.Background(), validPayloadAttrs(parentTimestamp), nil, nil)
		require.NoError(t, err)
		assert.Equal(t, blockHash, hash)

		require.Len(t, rec.inserted, 1)
		require.Equal(t, []common.Hash{blockHash}, rec.validated)
		require.True(t, rec.fcuCalled)
		assert.Equal(t, blockHash, rec.fcuHead)
		assert.Equal(t, rec.safeHash, rec.fcuSafe, "safe hash must be preserved")
		assert.Equal(t, rec.finalHash, rec.fcuFinal, "finalized hash must be preserved")
	})

	t.Run("extraData forwarded to the block builder", func(t *testing.T) {
		t.Parallel()
		api, rec, stub := newEnv(defaultAssembled())
		var captured *builder.Parameters
		stub.assembleBlockFunc = func(_ context.Context, params *builder.Parameters) (execmodule.AssembleBlockResult, error) {
			captured = params
			return execmodule.AssembleBlockResult{PayloadID: 7, Busy: false}, nil
		}

		override := hexutil.Bytes("overridden-extra")
		hash, err := api.CommitBlockV1(context.Background(), validPayloadAttrs(parentTimestamp), nil, &override)
		require.NoError(t, err)
		assert.Equal(t, blockHash, hash)
		require.NotNil(t, captured)
		assert.Equal(t, []byte("overridden-extra"), captured.ExtraData)
		require.True(t, rec.fcuCalled)
		assert.Equal(t, blockHash, rec.fcuHead)
	})

	t.Run("insert failure leaves head untouched", func(t *testing.T) {
		t.Parallel()
		api, rec, _ := newEnv(defaultAssembled())
		rec.insertRes = execmodule.ExecutionStatusBadBlock

		hash, err := api.CommitBlockV1(context.Background(), validPayloadAttrs(parentTimestamp), nil, nil)
		require.Error(t, err)
		assert.Equal(t, common.Hash{}, hash)
		assert.Empty(t, rec.validated)
		assert.False(t, rec.fcuCalled)
	})

	t.Run("insert error leaves head untouched", func(t *testing.T) {
		t.Parallel()
		api, rec, _ := newEnv(defaultAssembled())
		rec.insertErr = errors.New("disk full")

		hash, err := api.CommitBlockV1(context.Background(), validPayloadAttrs(parentTimestamp), nil, nil)
		require.Error(t, err)
		assert.Equal(t, common.Hash{}, hash)
		assert.False(t, rec.fcuCalled)
	})

	t.Run("bad block on validation leaves head untouched", func(t *testing.T) {
		t.Parallel()
		api, rec, _ := newEnv(defaultAssembled())
		rec.validateRes = execmodule.ValidationResult{
			ValidationStatus: execmodule.ExecutionStatusBadBlock,
			ValidationError:  "intrinsic gas too low",
		}

		hash, err := api.CommitBlockV1(context.Background(), validPayloadAttrs(parentTimestamp), nil, nil)
		require.Error(t, err)
		assert.Equal(t, common.Hash{}, hash)
		var rpcErr *rpc.CustomError
		require.ErrorAs(t, err, &rpcErr)
		assert.Equal(t, rpc.ErrCodeDefault, rpcErr.Code)
		assert.Contains(t, rpcErr.Message, "intrinsic gas too low")
		assert.False(t, rec.fcuCalled, "fork choice must not run after a bad block")
	})

	t.Run("invalid fork choice returns engine error code", func(t *testing.T) {
		t.Parallel()
		api, rec, _ := newEnv(defaultAssembled())
		rec.fcuRes = execmodule.ForkChoiceResult{Status: execmodule.ExecutionStatusInvalidForkchoice}

		hash, err := api.CommitBlockV1(context.Background(), validPayloadAttrs(parentTimestamp), nil, nil)
		require.Error(t, err)
		assert.Equal(t, common.Hash{}, hash)
		require.Equal(t, -38002, err.(rpc.Error).ErrorCode())
	})

	t.Run("reorg too deep returns engine error code", func(t *testing.T) {
		t.Parallel()
		api, rec, _ := newEnv(defaultAssembled())
		rec.fcuRes = execmodule.ForkChoiceResult{Status: execmodule.ExecutionStatusReorgTooDeep}

		hash, err := api.CommitBlockV1(context.Background(), validPayloadAttrs(parentTimestamp), nil, nil)
		require.Error(t, err)
		assert.Equal(t, common.Hash{}, hash)
		require.Equal(t, -38006, err.(rpc.Error).ErrorCode())
	})

	t.Run("bad block on fork choice returns error", func(t *testing.T) {
		t.Parallel()
		api, rec, _ := newEnv(defaultAssembled())
		rec.fcuRes = execmodule.ForkChoiceResult{
			Status:          execmodule.ExecutionStatusBadBlock,
			ValidationError: "invalid chain after execution",
		}

		hash, err := api.CommitBlockV1(context.Background(), validPayloadAttrs(parentTimestamp), nil, nil)
		require.Error(t, err)
		assert.Equal(t, common.Hash{}, hash)
		var rpcErr *rpc.CustomError
		require.ErrorAs(t, err, &rpcErr)
		assert.Equal(t, rpc.ErrCodeDefault, rpcErr.Code)
		assert.Contains(t, rpcErr.Message, "invalid chain after execution")
	})

	t.Run("nil transactions uses mempool", func(t *testing.T) {
		t.Parallel()
		api, _, stub := newEnv(defaultAssembled())
		var captured *builder.Parameters
		stub.assembleBlockFunc = func(_ context.Context, params *builder.Parameters) (execmodule.AssembleBlockResult, error) {
			captured = params
			return execmodule.AssembleBlockResult{PayloadID: 7, Busy: false}, nil
		}

		_, err := api.CommitBlockV1(context.Background(), validPayloadAttrs(parentTimestamp), nil, nil)
		require.NoError(t, err)
		require.NotNil(t, captured)
		assert.Nil(t, captured.CustomTxnProvider, "nil transactions must draw from the mempool")
	})

	t.Run("nonce too high rejected before insertion", func(t *testing.T) {
		t.Parallel()
		key, err := crypto.GenerateKey()
		require.NoError(t, err)
		// State nonce is 0 (no DB in tests), tx nonce 1 → too high.
		rawTx := makeSignedRawTx(t, key, allForksChainConfig(), 1, 101, parentTimestamp+1)
		api, rec, _ := newEnv(nil)

		txs := []hexutil.Bytes{rawTx}
		hash, err := api.CommitBlockV1(context.Background(), validPayloadAttrs(parentTimestamp), &txs, nil)
		require.Error(t, err)
		assert.Equal(t, common.Hash{}, hash)
		var rpcErr *rpc.CustomError
		require.ErrorAs(t, err, &rpcErr)
		assert.Contains(t, rpcErr.Message, "nonce too high")
		assert.Empty(t, rec.inserted)
		assert.False(t, rec.fcuCalled)
	})

	t.Run("missing parentBeaconBlockRoot for Cancun+", func(t *testing.T) {
		t.Parallel()
		api, rec, _ := newEnv(nil)
		attrs := validPayloadAttrs(parentTimestamp)
		attrs.ParentBeaconBlockRoot = nil
		hash, err := api.CommitBlockV1(context.Background(), attrs, nil, nil)
		require.Error(t, err)
		assert.Equal(t, common.Hash{}, hash)
		var rpcErr *rpc.InvalidParamsError
		require.ErrorAs(t, err, &rpcErr)
		assert.Contains(t, rpcErr.Message, "parentBeaconBlockRoot required for Cancun")
		assert.Empty(t, rec.inserted)
	})

	t.Run("missing withdrawals for Shanghai", func(t *testing.T) {
		t.Parallel()
		rec := successRecorder()
		stub := newCommitStub(parentHdr, nil, rec)
		api := newTestingAPI(preCancunChainConfig(), stub)
		attrs := &engine_types.PayloadAttributes{
			Timestamp:             hexutil.Uint64(parentTimestamp + 1),
			PrevRandao:            common.Hash{0xaa},
			SuggestedFeeRecipient: common.HexToAddress("0x1111111111111111111111111111111111111111"),
			Withdrawals:           nil,
		}
		hash, err := api.CommitBlockV1(context.Background(), attrs, nil, nil)
		require.Error(t, err)
		assert.Equal(t, common.Hash{}, hash)
		var rpcErr *rpc.InvalidParamsError
		require.ErrorAs(t, err, &rpcErr)
		assert.Contains(t, rpcErr.Message, "missing withdrawals")
		assert.Empty(t, rec.inserted)
	})

	t.Run("no assembled block data", func(t *testing.T) {
		t.Parallel()
		api, rec, _ := newEnv(nil) // getAssembledBlock returns nil block

		hash, err := api.CommitBlockV1(context.Background(), validPayloadAttrs(parentTimestamp), nil, nil)
		require.Error(t, err)
		assert.Equal(t, common.Hash{}, hash)
		assert.Contains(t, err.Error(), "no assembled block data")
		assert.Empty(t, rec.inserted)
		assert.False(t, rec.fcuCalled)
	})

	t.Run("block access list propagated to insertion", func(t *testing.T) {
		t.Parallel()
		assembled := defaultAssembled()
		assembled.BlockAccessList = types.BlockAccessList{}
		api, rec, _ := newEnv(assembled)

		_, err := api.CommitBlockV1(context.Background(), validPayloadAttrs(parentTimestamp), nil, nil)
		require.NoError(t, err)
		require.Len(t, rec.inserted, 1)
		assert.NotEmpty(t, rec.inserted[0].BlockAccessList, "encoded BAL must reach the inserted raw block")
	})

	t.Run("busy on validation returns error without fork choice", func(t *testing.T) {
		t.Parallel()
		api, rec, stub := newEnv(defaultAssembled())
		stub.validateChainFunc = func(_ context.Context, _ common.Hash, _ uint64) (execmodule.ValidationResult, error) {
			return execmodule.ValidationResult{ValidationStatus: execmodule.ExecutionStatusBusy}, nil
		}

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		hash, err := api.CommitBlockV1(ctx, validPayloadAttrs(parentTimestamp), nil, nil)
		require.Error(t, err)
		assert.Equal(t, common.Hash{}, hash)
		assert.Contains(t, err.Error(), "busy, cannot validate block")
		assert.False(t, rec.fcuCalled)
	})

	t.Run("busy on fork choice returns error", func(t *testing.T) {
		t.Parallel()
		api, _, stub := newEnv(defaultAssembled())
		stub.updateForkChoiceFunc = func(_ context.Context, _, _, _ common.Hash) (execmodule.ForkChoiceResult, error) {
			return execmodule.ForkChoiceResult{Status: execmodule.ExecutionStatusBusy}, nil
		}

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		hash, err := api.CommitBlockV1(ctx, validPayloadAttrs(parentTimestamp), nil, nil)
		require.Error(t, err)
		assert.Equal(t, common.Hash{}, hash)
		assert.Contains(t, err.Error(), "busy, cannot update fork choice")
	})

	t.Run("valid transaction list is committed", func(t *testing.T) {
		t.Parallel()
		key, err := crypto.GenerateKey()
		require.NoError(t, err)
		cfg := allForksChainConfig()
		rawTx0 := makeSignedRawTx(t, key, cfg, 0, 101, parentTimestamp+1)
		rawTx1 := makeSignedRawTx(t, key, cfg, 1, 101, parentTimestamp+1)
		api, rec, stub := newEnv(defaultAssembled())
		var captured *builder.Parameters
		stub.assembleBlockFunc = func(_ context.Context, params *builder.Parameters) (execmodule.AssembleBlockResult, error) {
			captured = params
			return execmodule.AssembleBlockResult{PayloadID: 7, Busy: false}, nil
		}

		txs := []hexutil.Bytes{rawTx0, rawTx1}
		hash, err := api.CommitBlockV1(context.Background(), validPayloadAttrs(parentTimestamp), &txs, nil)
		require.NoError(t, err)
		assert.Equal(t, blockHash, hash)
		require.NotNil(t, captured)
		require.NotNil(t, captured.CustomTxnProvider)
		provided, err := captured.CustomTxnProvider.ProvideTxns(context.Background())
		require.NoError(t, err)
		assert.Len(t, provided, 2)
		require.True(t, rec.fcuCalled)
	})

	t.Run("empty transaction list builds empty block via custom provider", func(t *testing.T) {
		t.Parallel()
		api, _, stub := newEnv(defaultAssembled())
		var captured *builder.Parameters
		stub.assembleBlockFunc = func(_ context.Context, params *builder.Parameters) (execmodule.AssembleBlockResult, error) {
			captured = params
			return execmodule.AssembleBlockResult{PayloadID: 7, Busy: false}, nil
		}

		txs := []hexutil.Bytes{}
		_, err := api.CommitBlockV1(context.Background(), validPayloadAttrs(parentTimestamp), &txs, nil)
		require.NoError(t, err)
		require.NotNil(t, captured)
		assert.NotNil(t, captured.CustomTxnProvider, "empty tx list must bypass the mempool")
		assert.Equal(t, parentHash, captured.ParentHash, "block must be built on the canonical head")
	})
}

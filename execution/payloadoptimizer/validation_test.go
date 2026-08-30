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

package payloadoptimizer_test

import (
	"context"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/payloadoptimizer"
	"github.com/erigontech/erigon/execution/protocol/misc"
	protocolparams "github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types"
)

func validColdResult(params *builder.Parameters, requests types.FlatRequests, value uint64) execmodule.AssembledBlockResult {
	requestsHash := requests.Hash()
	gasLimit := baseParentGasLimit
	if params.TargetGasLimit != nil {
		gasLimit = misc.CalcGasLimit(baseParentGasLimit, *params.TargetGasLimit)
	}
	header := &types.Header{
		ParentHash:            params.ParentHash,
		Number:                *uint256.NewInt(2),
		GasLimit:              gasLimit,
		Time:                  params.Timestamp,
		MixDigest:             params.PrevRandao,
		Coinbase:              params.SuggestedFeeRecipient,
		ParentBeaconBlockRoot: params.ParentBeaconBlockRoot,
		SlotNumber:            params.SlotNumber,
		Extra:                 params.ExtraData,
		RequestsHash:          requestsHash,
	}
	return execmodule.AssembledBlockResult{
		Block: &types.BlockWithReceipts{
			Block:    types.NewBlock(header, nil, nil, nil, params.Withdrawals, nil),
			Requests: requests,
		},
		BlockValue: uint256.NewInt(value),
	}
}

func applyColdResult(t *testing.T, buildCtx payloadoptimizer.BuildContext, result execmodule.AssembledBlockResult) error {
	t.Helper()
	backend := &optimizerBackend{
		assemble: func(context.Context, *builder.Parameters) (execmodule.AssembleBlockResult, error) {
			return execmodule.AssembleBlockResult{PayloadID: 1}, nil
		},
		get: func(context.Context, uint64) (execmodule.AssembledBlockResult, error) { return result, nil },
	}
	session, err := payloadoptimizer.New(backend).Open(t.Context(), buildCtx)
	require.NoError(t, err)
	update, err := payloadoptimizer.NewOrderflowUpdate(nil)
	require.NoError(t, err)
	_, err = session.Apply(t.Context(), update)
	return err
}

func TestCandidateRejectsIncompleteOrInconsistentResultGraph(t *testing.T) {
	params, fork, requests := baseBuildContextInput()
	buildCtx, err := payloadoptimizer.NewBuildContext(params, fork, requests, baseParentGasLimit)
	require.NoError(t, err)
	transaction := &types.LegacyTx{CommonTx: types.CommonTx{GasLimit: 21_000}}
	receipt := &types.Receipt{BlockNumber: uint256.NewInt(2), GasUsed: 21_000, CumulativeGasUsed: 21_000}

	tests := map[string]func(*execmodule.AssembledBlockResult){
		"nil block value": func(result *execmodule.AssembledBlockResult) { result.BlockValue = nil },
		"request hash": func(result *execmodule.AssembledBlockResult) {
			result.Block.Block.HeaderNoCopy().RequestsHash = new(common.Hash)
		},
		"receipt cardinality": func(result *execmodule.AssembledBlockResult) {
			result.Block.Block = types.NewBlock(result.Block.Block.Header(), []types.Transaction{transaction}, nil, nil, params.Withdrawals, nil)
		},
		"nil receipt": func(result *execmodule.AssembledBlockResult) {
			result.Block.Block = types.NewBlock(result.Block.Block.Header(), []types.Transaction{transaction}, nil, []*types.Receipt{receipt}, params.Withdrawals, nil)
			result.Block.Receipts = types.Receipts{nil}
		},
		"nil receipt block number": func(result *execmodule.AssembledBlockResult) {
			result.Block.Block = types.NewBlock(result.Block.Block.Header(), []types.Transaction{transaction}, nil, []*types.Receipt{receipt}, params.Withdrawals, nil)
			result.Block.Receipts = types.Receipts{{GasUsed: 21_000, CumulativeGasUsed: 21_000}}
		},
		"receipt root": func(result *execmodule.AssembledBlockResult) {
			result.Block.Block = types.NewBlock(result.Block.Block.Header(), []types.Transaction{transaction}, nil, []*types.Receipt{receipt}, params.Withdrawals, nil)
			result.Block.Receipts = types.Receipts{{BlockNumber: uint256.NewInt(2), GasUsed: 20_000, CumulativeGasUsed: 20_000}}
		},
		"nil transaction": func(result *execmodule.AssembledBlockResult) {
			header := result.Block.Block.Header()
			result.Block.Block = types.NewBlockFromStorage(common.Hash{}, header, types.Transactions{nil}, nil, params.Withdrawals, nil)
			result.Block.Receipts = types.Receipts{{BlockNumber: uint256.NewInt(2)}}
		},
		"nil withdrawal": func(result *execmodule.AssembledBlockResult) {
			header := result.Block.Block.Header()
			result.Block.Block = types.NewBlockFromStorage(common.Hash{}, header, nil, nil, types.Withdrawals{nil}, nil)
		},
		"BAL sidecar": func(result *execmodule.AssembledBlockResult) {
			bal := types.BlockAccessList{}
			hash := common.Hash{0xff}
			header := result.Block.Block.Header()
			header.BlockAccessListHash = &hash
			result.Block.Block = types.NewBlock(header, nil, nil, nil, params.Withdrawals, types.NewBlockAccessListSidecar(bal))
			result.Block.BlockAccessList = bal
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			result := validColdResult(params, requests, 1)
			mutate(&result)
			require.NotPanics(t, func() {
				err := applyColdResult(t, buildCtx, result)
				require.Error(t, err)
			})
		})
	}
}

func TestNilExpectedRequestsAcceptsGeneratedCandidateRequests(t *testing.T) {
	params, fork, requests := baseBuildContextInput()
	buildCtx, err := payloadoptimizer.NewBuildContext(params, fork, nil, baseParentGasLimit)
	require.NoError(t, err)

	require.NoError(t, applyColdResult(t, buildCtx, validColdResult(params, requests, 1)))
}

func TestCandidateValidationCoversBuildContextFields(t *testing.T) {
	params, fork, requests := baseBuildContextInput()
	buildCtx, err := payloadoptimizer.NewBuildContext(params, fork, requests, baseParentGasLimit)
	require.NoError(t, err)
	otherRoot := common.Hash{0xff}
	otherSlot := uint64(99)

	tests := map[string]func(*execmodule.AssembledBlockResult){
		"parent":        func(r *execmodule.AssembledBlockResult) { r.Block.Block.HeaderNoCopy().ParentHash[0]++ },
		"timestamp":     func(r *execmodule.AssembledBlockResult) { r.Block.Block.HeaderNoCopy().Time++ },
		"randao":        func(r *execmodule.AssembledBlockResult) { r.Block.Block.HeaderNoCopy().MixDigest[0]++ },
		"fee recipient": func(r *execmodule.AssembledBlockResult) { r.Block.Block.HeaderNoCopy().Coinbase[0]++ },
		"parent root": func(r *execmodule.AssembledBlockResult) {
			r.Block.Block.HeaderNoCopy().ParentBeaconBlockRoot = &otherRoot
		},
		"slot":       func(r *execmodule.AssembledBlockResult) { r.Block.Block.HeaderNoCopy().SlotNumber = &otherSlot },
		"zero gas":   func(r *execmodule.AssembledBlockResult) { r.Block.Block.HeaderNoCopy().GasLimit = 0 },
		"extra data": func(r *execmodule.AssembledBlockResult) { r.Block.Block.HeaderNoCopy().Extra[0]++ },
		"withdrawals": func(r *execmodule.AssembledBlockResult) {
			withdrawals := []*types.Withdrawal{{Index: 8, Validator: 9, Amount: 99, Address: common.Address{0x0b}}}
			r.Block.Block = types.NewBlock(r.Block.Block.Header(), nil, nil, nil, withdrawals, nil)
		},
		"requests": func(r *execmodule.AssembledBlockResult) { r.Block.Requests[0].RequestData[0]++ },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			result := validColdResult(params, requests, 1)
			mutate(&result)
			backend := &optimizerBackend{
				assemble: func(context.Context, *builder.Parameters) (execmodule.AssembleBlockResult, error) {
					return execmodule.AssembleBlockResult{PayloadID: 1}, nil
				},
				get: func(context.Context, uint64) (execmodule.AssembledBlockResult, error) { return result, nil },
			}
			session, err := payloadoptimizer.New(backend).Open(t.Context(), buildCtx)
			require.NoError(t, err)
			update, err := payloadoptimizer.NewOrderflowUpdate(nil)
			require.NoError(t, err)

			_, err = session.Apply(t.Context(), update)
			require.ErrorIs(t, err, payloadoptimizer.ErrCandidateContextMismatch)
		})
	}
}

func TestCandidateValidationAcceptsAProtocolAdjustedGasLimit(t *testing.T) {
	params, fork, requests := baseBuildContextInput()
	buildCtx, err := payloadoptimizer.NewBuildContext(params, fork, requests, baseParentGasLimit)
	require.NoError(t, err)
	result := validColdResult(params, requests, 1)
	require.NotEqual(t, *params.TargetGasLimit, result.Block.Block.GasLimit())
	backend := &optimizerBackend{
		assemble: func(context.Context, *builder.Parameters) (execmodule.AssembleBlockResult, error) {
			return execmodule.AssembleBlockResult{PayloadID: 1}, nil
		},
		get: func(context.Context, uint64) (execmodule.AssembledBlockResult, error) { return result, nil },
	}
	session, err := payloadoptimizer.New(backend).Open(t.Context(), buildCtx)
	require.NoError(t, err)
	update, err := payloadoptimizer.NewOrderflowUpdate(nil)
	require.NoError(t, err)

	_, err = session.Apply(t.Context(), update)
	require.NoError(t, err)
}

func TestCandidateValidationEnforcesResolvedBlobCap(t *testing.T) {
	params, fork, requests := baseBuildContextInput()
	maxBlobs := uint64(1)
	params.MaxBlobsPerBlock = &maxBlobs
	buildCtx, err := payloadoptimizer.NewBuildContext(params, fork, requests, baseParentGasLimit)
	require.NoError(t, err)
	to := common.Address{0x01}
	blobTransaction := func(nonce uint64) types.Transaction {
		return &types.BlobTx{
			DynamicFeeTransaction: types.DynamicFeeTransaction{
				CommonTx: types.CommonTx{Nonce: nonce, GasLimit: 21_000, To: &to},
				ChainID:  *uint256.NewInt(1),
				TipCap:   *uint256.NewInt(1),
				FeeCap:   *uint256.NewInt(1),
			},
			MaxFeePerBlobGas:    *uint256.NewInt(1),
			BlobVersionedHashes: []common.Hash{{0x01, byte(nonce)}},
		}
	}
	resultWith := func(blobCount int) execmodule.AssembledBlockResult {
		result := validColdResult(params, requests, 1)
		transactions := make(types.Transactions, blobCount)
		receipts := make(types.Receipts, blobCount)
		for i := range blobCount {
			transactions[i] = blobTransaction(uint64(i))
			receipts[i] = &types.Receipt{Type: types.BlobTxType, BlockNumber: uint256.NewInt(2), GasUsed: 21_000, CumulativeGasUsed: uint64(i+1) * 21_000}
		}
		blobGasUsed := uint64(blobCount) * protocolparams.GasPerBlob
		header := result.Block.Block.Header()
		header.BlobGasUsed = &blobGasUsed
		result.Block.Block = types.NewBlock(header, transactions, nil, receipts, params.Withdrawals, nil)
		result.Block.Receipts = receipts
		return result
	}

	require.NoError(t, applyColdResult(t, buildCtx, resultWith(1)))
	require.ErrorIs(t, applyColdResult(t, buildCtx, resultWith(2)), payloadoptimizer.ErrCandidateContextMismatch)
}

func TestCandidateValidationUsesCanonicalParentAndTargetGasLimit(t *testing.T) {
	const parentGasLimit = uint64(30_000_000)
	for name, target := range map[string]uint64{
		"decrease": 20_000_000,
		"exact":    parentGasLimit,
		"increase": 40_000_000,
	} {
		t.Run(name, func(t *testing.T) {
			params, fork, requests := baseBuildContextInput()
			params.TargetGasLimit = &target
			buildCtx, err := payloadoptimizer.NewBuildContext(params, fork, requests, parentGasLimit)
			require.NoError(t, err)
			want := misc.CalcGasLimit(parentGasLimit, target)
			result := validColdResult(params, requests, 1)
			result.Block.Block.HeaderNoCopy().GasLimit = want
			require.NoError(t, applyColdResult(t, buildCtx, result))

			result = validColdResult(params, requests, 1)
			result.Block.Block.HeaderNoCopy().GasLimit = want + 1
			require.ErrorIs(t, applyColdResult(t, buildCtx, result), payloadoptimizer.ErrCandidateContextMismatch)
		})
	}
}

func TestCandidateValidationUsesParentGasLimitWithoutTarget(t *testing.T) {
	params, fork, requests := baseBuildContextInput()
	params.TargetGasLimit = nil
	buildCtx, err := payloadoptimizer.NewBuildContext(params, fork, requests, baseParentGasLimit)
	require.NoError(t, err)
	result := validColdResult(params, requests, 1)
	result.Block.Block.HeaderNoCopy().GasLimit = baseParentGasLimit
	require.NoError(t, applyColdResult(t, buildCtx, result))

	result = validColdResult(params, requests, 1)
	result.Block.Block.HeaderNoCopy().GasLimit = baseParentGasLimit + 1
	require.ErrorIs(t, applyColdResult(t, buildCtx, result), payloadoptimizer.ErrCandidateContextMismatch)
}

func TestCandidateValidationAcceptsPreAmsterdamComputedBALWithoutHeaderCommitment(t *testing.T) {
	params, fork, requests := baseBuildContextInput()
	buildCtx, err := payloadoptimizer.NewBuildContext(params, fork, requests, baseParentGasLimit)
	require.NoError(t, err)
	result := validColdResult(params, requests, 1)
	result.Block.BlockAccessList = types.BlockAccessList{}

	require.NoError(t, applyColdResult(t, buildCtx, result))
}

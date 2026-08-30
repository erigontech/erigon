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
	"github.com/erigontech/erigon/execution/types"
)

func validColdResult(params *builder.Parameters, requests types.FlatRequests, value uint64) execmodule.AssembledBlockResult {
	header := &types.Header{
		ParentHash:            params.ParentHash,
		Number:                *uint256.NewInt(2),
		GasLimit:              *params.TargetGasLimit + 1,
		Time:                  params.Timestamp,
		MixDigest:             params.PrevRandao,
		Coinbase:              params.SuggestedFeeRecipient,
		ParentBeaconBlockRoot: params.ParentBeaconBlockRoot,
		SlotNumber:            params.SlotNumber,
		Extra:                 params.ExtraData,
	}
	return execmodule.AssembledBlockResult{
		Block: &types.BlockWithReceipts{
			Block:    types.NewBlock(header, nil, nil, nil, params.Withdrawals, nil),
			Requests: requests,
		},
		BlockValue: uint256.NewInt(value),
	}
}

func TestCandidateValidationCoversBuildContextFields(t *testing.T) {
	params, fork, requests := baseBuildContextInput()
	buildCtx, err := payloadoptimizer.NewBuildContext(params, fork, requests)
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
	buildCtx, err := payloadoptimizer.NewBuildContext(params, fork, requests)
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

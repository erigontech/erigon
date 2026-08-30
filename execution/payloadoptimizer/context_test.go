package payloadoptimizer_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/payloadoptimizer"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/txnprovider"
)

type contextTxnProvider struct{}

func (contextTxnProvider) ProvideTxns(context.Context, ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	return nil, nil
}

func baseBuildContextInput() (*builder.Parameters, [4]byte, types.FlatRequests) {
	root := common.Hash{0x05}
	slot := uint64(6)
	gasLimit := uint64(7)
	return &builder.Parameters{
		ParentHash:            common.Hash{0x01},
		Timestamp:             2,
		PrevRandao:            common.Hash{0x03},
		SuggestedFeeRecipient: common.Address{0x04},
		Withdrawals:           []*types.Withdrawal{{Index: 8, Validator: 9, Amount: 10, Address: common.Address{0x0b}}},
		ParentBeaconBlockRoot: &root,
		SlotNumber:            &slot,
		TargetGasLimit:        &gasLimit,
		ExtraData:             []byte{0x0c},
	}, [4]byte{0x0d}, types.FlatRequests{{Type: types.DepositRequestType, RequestData: []byte{0x0e}}}
}

func TestBuildContextOwnsItsInputs(t *testing.T) {
	parentRoot := common.Hash{0x04}
	slot := uint64(5)
	gasLimit := uint64(6)
	params := &builder.Parameters{
		PayloadId:             99,
		ParentHash:            common.Hash{0x01},
		Timestamp:             2,
		PrevRandao:            common.Hash{0x03},
		SuggestedFeeRecipient: common.Address{0x04},
		Withdrawals:           []*types.Withdrawal{{Index: 7, Validator: 8, Amount: 9, Address: common.Address{0x0a}}},
		ParentBeaconBlockRoot: &parentRoot,
		SlotNumber:            &slot,
		TargetGasLimit:        &gasLimit,
		ExtraData:             []byte{0x0b},
	}
	requests := types.FlatRequests{{Type: types.DepositRequestType, RequestData: []byte{0x0c}}}

	ctx, err := payloadoptimizer.NewBuildContext(params, [4]byte{0x0d}, requests)
	require.NoError(t, err)

	params.ParentHash[0] = 0xff
	params.Withdrawals[0].Amount = 100
	*params.ParentBeaconBlockRoot = common.Hash{0xff}
	*params.SlotNumber = 100
	*params.TargetGasLimit = 100
	params.ExtraData[0] = 0xff
	requests[0].RequestData[0] = 0xff

	owned := ctx.Parameters()
	require.Equal(t, common.Hash{0x01}, owned.ParentHash)
	require.Equal(t, uint64(9), owned.Withdrawals[0].Amount)
	require.Equal(t, common.Hash{0x04}, *owned.ParentBeaconBlockRoot)
	require.Equal(t, uint64(5), *owned.SlotNumber)
	require.Equal(t, uint64(6), *owned.TargetGasLimit)
	require.Equal(t, []byte{0x0b}, owned.ExtraData)
	require.Zero(t, owned.PayloadId)
	require.Nil(t, owned.CustomTxnProvider)
	require.Equal(t, []byte{0x0c}, ctx.ExecutionRequests()[0].RequestData)

	owned.ParentHash[0] = 0xee
	owned.Withdrawals[0].Amount = 200
	owned.ExtraData[0] = 0xee
	returnedRequests := ctx.ExecutionRequests()
	returnedRequests[0].RequestData[0] = 0xee

	require.Equal(t, common.Hash{0x01}, ctx.Parameters().ParentHash)
	require.Equal(t, uint64(9), ctx.Parameters().Withdrawals[0].Amount)
	require.Equal(t, []byte{0x0b}, ctx.Parameters().ExtraData)
	require.Equal(t, []byte{0x0c}, ctx.ExecutionRequests()[0].RequestData)
}

func TestBuildContextEqualityCoversEveryExecutionFieldInBothDirections(t *testing.T) {
	baseParams, baseFork, baseRequests := baseBuildContextInput()
	base, err := payloadoptimizer.NewBuildContext(baseParams, baseFork, baseRequests)
	require.NoError(t, err)
	require.True(t, base.Equal(base)) //nolint:gocritic

	tests := map[string]func(*builder.Parameters, *[4]byte, *types.FlatRequests){
		"parent hash":      func(p *builder.Parameters, _ *[4]byte, _ *types.FlatRequests) { p.ParentHash[0]++ },
		"timestamp":        func(p *builder.Parameters, _ *[4]byte, _ *types.FlatRequests) { p.Timestamp++ },
		"prev randao":      func(p *builder.Parameters, _ *[4]byte, _ *types.FlatRequests) { p.PrevRandao[0]++ },
		"fee recipient":    func(p *builder.Parameters, _ *[4]byte, _ *types.FlatRequests) { p.SuggestedFeeRecipient[0]++ },
		"withdrawals":      func(p *builder.Parameters, _ *[4]byte, _ *types.FlatRequests) { p.Withdrawals[0].Amount++ },
		"withdrawal index": func(p *builder.Parameters, _ *[4]byte, _ *types.FlatRequests) { p.Withdrawals[0].Index++ },
		"withdrawal validator": func(p *builder.Parameters, _ *[4]byte, _ *types.FlatRequests) {
			p.Withdrawals[0].Validator++
		},
		"withdrawal address": func(p *builder.Parameters, _ *[4]byte, _ *types.FlatRequests) { p.Withdrawals[0].Address[0]++ },
		"withdrawals nil":    func(p *builder.Parameters, _ *[4]byte, _ *types.FlatRequests) { p.Withdrawals = nil },
		"withdrawals empty":  func(p *builder.Parameters, _ *[4]byte, _ *types.FlatRequests) { p.Withdrawals = []*types.Withdrawal{} },
		"parent beacon root": func(p *builder.Parameters, _ *[4]byte, _ *types.FlatRequests) { (*p.ParentBeaconBlockRoot)[0]++ },
		"parent root nil":    func(p *builder.Parameters, _ *[4]byte, _ *types.FlatRequests) { p.ParentBeaconBlockRoot = nil },
		"slot number":        func(p *builder.Parameters, _ *[4]byte, _ *types.FlatRequests) { *p.SlotNumber++ },
		"slot number nil":    func(p *builder.Parameters, _ *[4]byte, _ *types.FlatRequests) { p.SlotNumber = nil },
		"target gas limit":   func(p *builder.Parameters, _ *[4]byte, _ *types.FlatRequests) { *p.TargetGasLimit++ },
		"gas limit nil":      func(p *builder.Parameters, _ *[4]byte, _ *types.FlatRequests) { p.TargetGasLimit = nil },
		"extra data":         func(p *builder.Parameters, _ *[4]byte, _ *types.FlatRequests) { p.ExtraData[0]++ },
		"extra data nil":     func(p *builder.Parameters, _ *[4]byte, _ *types.FlatRequests) { p.ExtraData = nil },
		"fork version":       func(_ *builder.Parameters, f *[4]byte, _ *types.FlatRequests) { f[0]++ },
		"execution requests": func(_ *builder.Parameters, _ *[4]byte, r *types.FlatRequests) { (*r)[0].RequestData[0]++ },
		"request type":       func(_ *builder.Parameters, _ *[4]byte, r *types.FlatRequests) { (*r)[0].Type++ },
		"requests nil":       func(_ *builder.Parameters, _ *[4]byte, r *types.FlatRequests) { *r = nil },
		"requests empty":     func(_ *builder.Parameters, _ *[4]byte, r *types.FlatRequests) { *r = types.FlatRequests{} },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			params, fork, requests := baseBuildContextInput()
			mutate(params, &fork, &requests)
			other, err := payloadoptimizer.NewBuildContext(params, fork, requests)
			require.NoError(t, err)
			require.False(t, base.Equal(other))
			require.False(t, other.Equal(base))
		})
	}

	sameParams, sameFork, sameRequests := baseBuildContextInput()
	sameParams.PayloadId = 123
	same, err := payloadoptimizer.NewBuildContext(sameParams, sameFork, sameRequests)
	require.NoError(t, err)
	require.True(t, base.Equal(same))
	require.Equal(t, 11, reflect.TypeFor[builder.Parameters]().NumField())
}

func TestBuildContextRejectsInvalidInputs(t *testing.T) {
	_, err := payloadoptimizer.NewBuildContext(nil, [4]byte{}, nil)
	require.Error(t, err)
	params, fork, requests := baseBuildContextInput()
	params.CustomTxnProvider = contextTxnProvider{}

	_, err = payloadoptimizer.NewBuildContext(params, fork, requests)
	require.ErrorIs(t, err, payloadoptimizer.ErrCustomTxnProvider)
}

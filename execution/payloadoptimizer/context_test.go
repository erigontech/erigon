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
	"math"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/payloadoptimizer"
	protocolparams "github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/txnprovider"
)

type contextTxnProvider struct{}

const baseParentGasLimit = uint64(30_000_000)

func (contextTxnProvider) ProvideTxns(context.Context, ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	return nil, nil
}

func baseBuildContextInput() (*builder.Parameters, [4]byte, types.FlatRequests) {
	root := common.Hash{0x05}
	gasLimit := uint64(31_000_000)
	return &builder.Parameters{
		ParentHash:            common.Hash{0x01},
		Timestamp:             2,
		PrevRandao:            common.Hash{0x03},
		SuggestedFeeRecipient: common.Address{0x04},
		Withdrawals:           []*types.Withdrawal{{Index: 8, Validator: 9, Amount: 10, Address: common.Address{0x0b}}},
		ParentBeaconBlockRoot: &root,
		TargetGasLimit:        &gasLimit,
		ExtraData:             []byte{0x0c},
	}, [4]byte{0x0d}, types.FlatRequests{{Type: types.DepositRequestType, RequestData: []byte{0x0e}}}
}

func TestBuildContextOwnsItsInputs(t *testing.T) {
	parentRoot := common.Hash{0x04}
	slot := uint64(5)
	gasLimit := uint64(protocolparams.MinBlockGasLimit)
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

	ctx, err := payloadoptimizer.NewBuildContext(params, [4]byte{0x0d}, requests, baseParentGasLimit)
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
	require.Equal(t, uint64(protocolparams.MinBlockGasLimit), *owned.TargetGasLimit)
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

func TestBuildContextResolvesBuilderDefaultsAndOverrides(t *testing.T) {
	defaultGas, defaultBlobs := uint64(31_000_000), uint64(2)
	defaults := payloadoptimizer.BuildDefaults{
		TargetGasLimit:   &defaultGas,
		ExtraData:        []byte{0xaa},
		MaxBlobsPerBlock: &defaultBlobs,
	}
	params, fork, requests := baseBuildContextInput()
	params.TargetGasLimit = nil
	params.ExtraData = nil

	resolved, err := payloadoptimizer.NewBuildContext(params, fork, requests, baseParentGasLimit, defaults)
	require.NoError(t, err)
	require.Equal(t, defaultGas, *resolved.Parameters().TargetGasLimit)
	require.Equal(t, []byte{0xaa}, resolved.Parameters().ExtraData)
	require.Equal(t, defaultBlobs, *resolved.Parameters().MaxBlobsPerBlock)
	explicitParams := params.Copy()
	explicitParams.TargetGasLimit = &defaultGas
	explicitParams.ExtraData = []byte{0xaa}
	explicitParams.MaxBlobsPerBlock = &defaultBlobs
	explicit, err := payloadoptimizer.NewBuildContext(explicitParams, fork, requests, baseParentGasLimit, defaults)
	require.NoError(t, err)
	require.True(t, resolved.Equal(explicit))
	require.True(t, explicit.Equal(resolved))

	overrideGas, overrideBlobs := uint64(32_000_000), uint64(1)
	params.TargetGasLimit = &overrideGas
	params.ExtraData = []byte{0xbb}
	params.MaxBlobsPerBlock = &overrideBlobs
	overridden, err := payloadoptimizer.NewBuildContext(params, fork, requests, baseParentGasLimit, defaults)
	require.NoError(t, err)
	require.Equal(t, overrideGas, *overridden.Parameters().TargetGasLimit)
	require.Equal(t, []byte{0xbb}, overridden.Parameters().ExtraData)
	require.Equal(t, overrideBlobs, *overridden.Parameters().MaxBlobsPerBlock)
	require.False(t, resolved.Equal(overridden))
	require.False(t, overridden.Equal(resolved))

	defaults.TargetGasLimit = nil
	defaults.ExtraData = nil
	defaults.MaxBlobsPerBlock = nil
	params.TargetGasLimit = nil
	params.ExtraData = nil
	params.MaxBlobsPerBlock = nil
	fallback, err := payloadoptimizer.NewBuildContext(params, fork, requests, baseParentGasLimit, defaults)
	require.NoError(t, err)
	require.Equal(t, baseParentGasLimit, *fallback.Parameters().TargetGasLimit)
	require.NotNil(t, fallback.Parameters().ExtraData)
	require.Empty(t, fallback.Parameters().ExtraData)
	require.Equal(t, uint64(^uint64(0)), *fallback.Parameters().MaxBlobsPerBlock)
}

func TestBuildContextEqualityCoversEveryExecutionFieldInBothDirections(t *testing.T) {
	baseParams, baseFork, baseRequests := baseBuildContextInput()
	base, err := payloadoptimizer.NewBuildContext(baseParams, baseFork, baseRequests, baseParentGasLimit)
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
		"slot number": func(p *builder.Parameters, _ *[4]byte, _ *types.FlatRequests) {
			slot := uint64(6)
			p.SlotNumber = &slot
		},
		"target gas limit": func(p *builder.Parameters, _ *[4]byte, _ *types.FlatRequests) { *p.TargetGasLimit++ },
		"gas limit nil":    func(p *builder.Parameters, _ *[4]byte, _ *types.FlatRequests) { p.TargetGasLimit = nil },
		"max blobs": func(p *builder.Parameters, _ *[4]byte, _ *types.FlatRequests) {
			maxBlobs := uint64(1)
			p.MaxBlobsPerBlock = &maxBlobs
		},
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
			other, err := payloadoptimizer.NewBuildContext(params, fork, requests, baseParentGasLimit)
			require.NoError(t, err)
			require.False(t, base.Equal(other))
			require.False(t, other.Equal(base))
		})
	}

	sameParams, sameFork, sameRequests := baseBuildContextInput()
	sameParams.PayloadId = 123
	same, err := payloadoptimizer.NewBuildContext(sameParams, sameFork, sameRequests, baseParentGasLimit)
	require.NoError(t, err)
	require.True(t, base.Equal(same))
	differentParentGas, err := payloadoptimizer.NewBuildContext(sameParams, sameFork, sameRequests, baseParentGasLimit+1)
	require.NoError(t, err)
	require.False(t, base.Equal(differentParentGas))
	require.Equal(t, baseParentGasLimit, base.ParentGasLimit())
	require.Equal(t, 13, reflect.TypeFor[builder.Parameters]().NumField())
}

func TestBuildContextRejectsInvalidInputs(t *testing.T) {
	_, err := payloadoptimizer.NewBuildContext(nil, [4]byte{}, nil, baseParentGasLimit)
	require.Error(t, err)
	params, fork, requests := baseBuildContextInput()
	_, err = payloadoptimizer.NewBuildContext(params, fork, requests, protocolparams.MinBlockGasLimit-1)
	require.Error(t, err)
	_, err = payloadoptimizer.NewBuildContext(params, fork, requests, protocolparams.MinBlockGasLimit)
	require.NoError(t, err)
	_, err = payloadoptimizer.NewBuildContext(params, fork, requests, protocolparams.MaxBlockGasLimit+1)
	require.Error(t, err)
	params, fork, requests = baseBuildContextInput()
	params.CustomTxnProvider = contextTxnProvider{}

	_, err = payloadoptimizer.NewBuildContext(params, fork, requests, baseParentGasLimit)
	require.ErrorIs(t, err, payloadoptimizer.ErrCustomTxnProvider)

	params, fork, requests = baseBuildContextInput()
	params.Withdrawals = append(params.Withdrawals, nil)
	require.NotPanics(t, func() {
		_, err = payloadoptimizer.NewBuildContext(params, fork, requests, baseParentGasLimit)
	})
	require.Error(t, err)
}

func TestBuildContextPreservesGloasTargetGasPreference(t *testing.T) {
	for name, target := range map[string]uint64{
		"zero":           0,
		"below minimum":  protocolparams.MinBlockGasLimit - 1,
		"minimum":        protocolparams.MinBlockGasLimit,
		"maximum":        protocolparams.MaxBlockGasLimit,
		"above maximum":  protocolparams.MaxBlockGasLimit + 1,
		"uint64 maximum": math.MaxUint64,
	} {
		t.Run("explicit gas "+name, func(t *testing.T) {
			params, fork, requests := baseBuildContextInput()
			params.TargetGasLimit = &target
			buildContext, err := payloadoptimizer.NewBuildContext(params, fork, requests, baseParentGasLimit)
			require.NoError(t, err)
			require.Equal(t, target, *buildContext.Parameters().TargetGasLimit)
		})
		t.Run("default gas "+name, func(t *testing.T) {
			params, fork, requests := baseBuildContextInput()
			params.TargetGasLimit = nil
			buildContext, err := payloadoptimizer.NewBuildContext(params, fork, requests, baseParentGasLimit, payloadoptimizer.BuildDefaults{TargetGasLimit: &target})
			require.NoError(t, err)
			require.Equal(t, target, *buildContext.Parameters().TargetGasLimit)
		})
	}
}

func TestBuildContextEnforcesResolvedConsensusBounds(t *testing.T) {
	tooLong := make([]byte, protocolparams.MaximumExtraDataSize+1)
	t.Run("explicit extra above maximum", func(t *testing.T) {
		params, fork, requests := baseBuildContextInput()
		params.ExtraData = tooLong
		_, err := payloadoptimizer.NewBuildContext(params, fork, requests, baseParentGasLimit)
		require.Error(t, err)
	})
	t.Run("default extra above maximum", func(t *testing.T) {
		params, fork, requests := baseBuildContextInput()
		params.ExtraData = nil
		_, err := payloadoptimizer.NewBuildContext(params, fork, requests, baseParentGasLimit, payloadoptimizer.BuildDefaults{ExtraData: tooLong})
		require.Error(t, err)
	})
	for name, extra := range map[string][]byte{
		"empty":   {},
		"maximum": make([]byte, protocolparams.MaximumExtraDataSize),
	} {
		t.Run("valid extra "+name, func(t *testing.T) {
			params, fork, requests := baseBuildContextInput()
			params.ExtraData = extra
			_, err := payloadoptimizer.NewBuildContext(params, fork, requests, baseParentGasLimit)
			require.NoError(t, err)
		})
	}
}

func TestZeroBuildContextAccessorsAreSafe(t *testing.T) {
	var buildCtx payloadoptimizer.BuildContext
	require.NotPanics(t, func() {
		require.Nil(t, buildCtx.Parameters())
		require.Nil(t, buildCtx.ExecutionRequests())
		require.False(t, buildCtx.Equal(payloadoptimizer.BuildContext{}))
	})
}

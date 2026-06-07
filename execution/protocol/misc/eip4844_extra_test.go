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

package misc

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types"
)

func TestVerifyPresenceOfCancunHeaderFields(t *testing.T) {
	t.Parallel()
	full := &types.Header{
		BlobGasUsed:           common.NewUint64(0),
		ExcessBlobGas:         common.NewUint64(0),
		ParentBeaconBlockRoot: &common.Hash{},
	}
	require.NoError(t, VerifyPresenceOfCancunHeaderFields(full))

	tests := map[string]func(*types.Header){
		"missing blobGasUsed":           func(h *types.Header) { h.BlobGasUsed = nil },
		"missing excessBlobGas":         func(h *types.Header) { h.ExcessBlobGas = nil },
		"missing parentBeaconBlockRoot": func(h *types.Header) { h.ParentBeaconBlockRoot = nil },
	}
	for name, drop := range tests {
		drop := drop
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h := &types.Header{
				BlobGasUsed:           common.NewUint64(0),
				ExcessBlobGas:         common.NewUint64(0),
				ParentBeaconBlockRoot: &common.Hash{},
			}
			drop(h)
			require.Error(t, VerifyPresenceOfCancunHeaderFields(h))
		})
	}
}

func TestVerifyAbsenceOfCancunHeaderFields(t *testing.T) {
	t.Parallel()
	require.NoError(t, VerifyAbsenceOfCancunHeaderFields(&types.Header{}))

	tests := map[string]func(*types.Header){
		"has blobGasUsed":           func(h *types.Header) { h.BlobGasUsed = common.NewUint64(0) },
		"has excessBlobGas":         func(h *types.Header) { h.ExcessBlobGas = common.NewUint64(0) },
		"has parentBeaconBlockRoot": func(h *types.Header) { h.ParentBeaconBlockRoot = &common.Hash{} },
	}
	for name, set := range tests {
		set := set
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h := &types.Header{}
			set(h)
			require.Error(t, VerifyAbsenceOfCancunHeaderFields(h))
		})
	}
}

func TestGetBlobGasUsed(t *testing.T) {
	t.Parallel()
	require.Equal(t, uint64(0), GetBlobGasUsed(0))
	require.Equal(t, params.GasPerBlob, GetBlobGasUsed(1))
	require.Equal(t, 3*params.GasPerBlob, GetBlobGasUsed(3))
}

func TestGetBlobGasPrice_MinAtZeroExcess(t *testing.T) {
	t.Parallel()
	cfg := chain.AllProtocolChanges
	price, err := GetBlobGasPrice(cfg, 0, 0)
	require.NoError(t, err)
	require.Equal(t, cfg.GetMinBlobGasPrice(), price.Uint64())
}

func TestCalcExcessBlobGas_BelowTargetIsZero(t *testing.T) {
	t.Parallel()
	cfg := chain.AllProtocolChanges
	parent := &types.Header{
		ExcessBlobGas: common.NewUint64(0),
		BlobGasUsed:   common.NewUint64(0),
	}
	require.Equal(t, uint64(0), CalcExcessBlobGas(cfg, parent, 0))
}

func TestCalcExcessBlobGas_AboveTarget(t *testing.T) {
	t.Parallel()
	cfg := copyConfig(chain.AllProtocolChanges)
	cfg.OsakaTime = nil // keep the pre-Osaka subtraction branch

	target := cfg.GetTargetBlobsPerBlock(0)
	targetBlobGas := target * params.GasPerBlob

	parent := &types.Header{
		ExcessBlobGas: common.NewUint64(2 * targetBlobGas),
		BlobGasUsed:   common.NewUint64(0),
	}
	require.Equal(t, 2*targetBlobGas-targetBlobGas, CalcExcessBlobGas(cfg, parent, 0))
}

func TestFakeExponential_Overflow(t *testing.T) {
	t.Parallel()
	factor := new(uint256.Int).SetAllOne()
	denom := uint256.NewInt(2)
	_, err := FakeExponential(factor, denom, 1)
	require.ErrorContains(t, err, "overflow")
}

func TestValidateBlobs(t *testing.T) {
	t.Parallel()
	noTxns := &[]types.Transaction{}

	require.ErrorIs(t, ValidateBlobs(0, 0, 6, nil, noTxns), ErrNilBlobHashes)

	require.NoError(t, ValidateBlobs(0, 0, 6, []common.Hash{}, noTxns))

	require.ErrorIs(t,
		ValidateBlobs(0, 0, 6, []common.Hash{common.HexToHash("0x01")}, noTxns),
		ErrMismatchBlobHashes)
}

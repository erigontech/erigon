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

package txtype_test

import (
	"testing"

	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
	"github.com/erigontech/erigon/txnprovider/txtype"
	"github.com/stretchr/testify/require"
)

func TestGlobalRegistryHasAllKnownTypes(t *testing.T) {
	knownTypes := []byte{
		types.LegacyTxType,
		types.AccessListTxType,
		types.DynamicFeeTxType,
		types.BlobTxType,
		types.SetCodeTxType,
		types.AccountAbstractionTxType,
	}
	for _, typeByte := range knownTypes {
		h, ok := txtype.Global.Get(typeByte)
		require.True(t, ok, "handler missing for type %d", typeByte)
		require.Equal(t, typeByte, h.TypeByte())
		require.NotEmpty(t, h.Name())
	}
}

func TestTypeNameFallback(t *testing.T) {
	name := txtype.TypeName(0xff)
	require.Contains(t, name, "255")
}

func TestDefaultHandlerDefaults(t *testing.T) {
	cfg := txpoolcfg.DefaultConfig
	forks := txtype.ForkState{IsCancun: true, IsPrague: true}

	// Legacy is always active regardless of forks.
	h, _ := txtype.Global.Get(types.LegacyTxType)
	require.True(t, h.ForkRequired(forks))
	require.True(t, h.CanCreate())
	require.True(t, h.CanReplace(types.BlobTxType))
	require.Equal(t, cfg.PriceBump, h.PriceBumpPercent(&cfg))
	require.Equal(t, cfg.AccountSlots, h.AccountLimit(&cfg))
	require.False(t, h.EvictOnNonceGap())
	require.True(t, h.PromotionCheck(0, 0))
}

func TestBlobHandlerPolicy(t *testing.T) {
	cfg := txpoolcfg.DefaultConfig
	h, ok := txtype.Global.Get(types.BlobTxType)
	require.True(t, ok)

	// Fork gating
	require.False(t, h.ForkRequired(txtype.ForkState{}))
	require.True(t, h.ForkRequired(txtype.ForkState{IsCancun: true}))

	// Creation prohibited
	require.False(t, h.CanCreate())

	// Can only replace blob→blob
	require.True(t, h.CanReplace(types.BlobTxType))
	require.False(t, h.CanReplace(types.LegacyTxType))
	require.False(t, h.CanReplace(types.DynamicFeeTxType))

	// Price bump is the blob-specific bump
	require.Equal(t, cfg.BlobPriceBump, h.PriceBumpPercent(&cfg))

	// Per-sender limit is BlobSlots
	require.Equal(t, cfg.BlobSlots, h.AccountLimit(&cfg))

	// Strict nonce ordering
	require.True(t, h.EvictOnNonceGap())

	// Promotion gated on blob fee cap
	require.False(t, h.PromotionCheck(5, 10))
	require.True(t, h.PromotionCheck(10, 10))
	require.True(t, h.PromotionCheck(15, 10))
}

func TestSetCodeHandlerPolicy(t *testing.T) {
	h, ok := txtype.Global.Get(types.SetCodeTxType)
	require.True(t, ok)

	require.False(t, h.ForkRequired(txtype.ForkState{}))
	require.True(t, h.ForkRequired(txtype.ForkState{IsPrague: true}))
	require.False(t, h.CanCreate())
}

func TestAAHandlerPolicy(t *testing.T) {
	h, ok := txtype.Global.Get(types.AccountAbstractionTxType)
	require.True(t, ok)

	require.False(t, h.ForkRequired(txtype.ForkState{}))
	require.True(t, h.ForkRequired(txtype.ForkState{AllowAA: true}))
	require.False(t, h.CanCreate())
	require.True(t, h.IntrinsicGasFlags().IsAATxn)
}

func TestDuplicateRegistrationPanics(t *testing.T) {
	r := txtype.NewRegistry()
	r.Register(txtype.LegacyHandler{})
	require.Panics(t, func() { r.Register(txtype.LegacyHandler{}) })
}
